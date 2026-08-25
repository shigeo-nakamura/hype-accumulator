use crate::{
    ledger::ReplayState,
    pacing::{PacingError, PacingLimits, PacingState},
    signal::{CoreHealth, SignalSnapshot},
    workflow::{HypeAtoms, WorkflowStage, WorkflowState},
};
use chrono::{DateTime, NaiveDate, Utc};
use serde::Serialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
};
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HaltReason {
    ManualReview,
    StuckWorkflow,
    StaleSignal,
}

impl HaltReason {
    const fn label(self) -> &'static str {
        match self {
            Self::ManualReview => "manual_review",
            Self::StuckWorkflow => "stuck_workflow",
            Self::StaleSignal => "stale_signal",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct HorizonMetrics {
    pub horizon: NaiveDate,
    pub days_remaining: u64,
    pub purchase_slots_remaining: u64,
    pub residual_usdc: f64,
    pub required_pace_usdc: Option<f64>,
    pub infeasible: bool,
}

/// Identifier-free workflow projection safe for status and metrics output.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowObservation {
    workflow_key: String,
    stage: WorkflowStage,
    last_transition_at: DateTime<Utc>,
    last_fill_at: Option<DateTime<Utc>>,
    staking_eligible_hype: HypeAtoms,
    staking_target_hype: HypeAtoms,
    delegated_hype: HypeAtoms,
}

impl From<&WorkflowState> for WorkflowObservation {
    fn from(state: &WorkflowState) -> Self {
        Self {
            workflow_key: state.workflow_id().to_owned(),
            stage: state.stage(),
            last_transition_at: state.last_transition_at(),
            last_fill_at: state.last_fill_at(),
            staking_eligible_hype: state.staking_eligibility().eligible_hype,
            staking_target_hype: state.staking_target_hype(),
            delegated_hype: state.delegated_hype(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct MetricsSnapshot {
    pub observed_at: DateTime<Utc>,
    pub confirmed_deposits_usdc: f64,
    pub admitted_deposits_usdc: f64,
    pub unallocated_deposits_usdc: f64,
    pub deployable_usdc: f64,
    pub uninvested_usdc: f64,
    pub committed_usdc: f64,
    pub spent_usdc: f64,
    pub last_capital_event_at: Option<DateTime<Utc>>,
    pub last_decision_at: Option<DateTime<Utc>>,
    pub last_fill_at: Option<DateTime<Utc>>,
    pub unstaked_hype_atoms: u64,
    pub delegated_hype_atoms: u64,
    pub pending_workflows: u64,
    pub manual_review_workflows: u64,
    pub pending_workflow_age_seconds: Option<u64>,
    pub workflow_stuck: bool,
    pub api_errors_total: u64,
    pub stale_signal_events_total: u64,
    pub dry_run_actions_total: u64,
    pub stale_signal: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub halt_reason: Option<HaltReason>,
    pub horizons: Vec<HorizonMetrics>,
}

#[derive(Debug, Error)]
pub enum MetricsError {
    #[error("stuck workflow threshold must be positive")]
    InvalidStuckThreshold,
    #[error("workflow observation is after metrics observation time")]
    FutureWorkflowObservation,
    #[error("capital projections disagree between pacing and ledger state")]
    InconsistentCapitalState,
    #[error("metrics aggregation overflowed")]
    ArithmeticOverflow,
    #[error("duplicate workflow observation")]
    DuplicateWorkflow,
    #[error("pacing projection failed: {0}")]
    Pacing(#[from] PacingError),
}

impl MetricsSnapshot {
    /// Builds one immutable, identifier-free operational projection.
    ///
    /// # Errors
    ///
    /// Fails closed on time regression, arithmetic overflow, invalid pacing
    /// bounds, or disagreement between the pacing and durable-ledger totals.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub fn from_runtime(
        observed_at: DateTime<Utc>,
        pacing: &PacingState,
        limits: &PacingLimits,
        ledger: &ReplayState,
        workflows: &[WorkflowObservation],
        signal: Option<&SignalSnapshot>,
        api_errors_total: u64,
        stale_signal_events_total: u64,
        dry_run_actions_total: u64,
        stuck_after_seconds: u64,
    ) -> Result<Self, MetricsError> {
        if stuck_after_seconds == 0 {
            return Err(MetricsError::InvalidStuckThreshold);
        }
        let admitted = checked_sum(
            pacing
                .deposits()
                .values()
                .map(|tranche| tranche.admitted_usdc.as_micros()),
        )?;
        let committed = checked_sum(
            pacing
                .deposits()
                .values()
                .map(|tranche| tranche.committed_usdc.as_micros()),
        )?;
        let spent = checked_sum(
            pacing
                .deposits()
                .values()
                .map(|tranche| tranche.invested_usdc.as_micros()),
        )?;
        let uninvested = checked_sum(
            pacing
                .deposits()
                .values()
                .map(|tranche| tranche.residual_usdc().as_micros()),
        )?;
        let confirmed = ledger
            .authoritative_deposits_usdc()
            .ok_or(MetricsError::ArithmeticOverflow)?
            .as_micros();
        let unallocated = confirmed
            .checked_sub(admitted)
            .ok_or(MetricsError::InconsistentCapitalState)?;
        let deployable = ledger.deployable_usdc().as_micros();
        if admitted != ledger.admitted_usdc().as_micros()
            || committed != ledger.committed_usdc().as_micros()
            || spent != ledger.spent_usdc().as_micros()
            || uninvested != deployable
        {
            return Err(MetricsError::InconsistentCapitalState);
        }

        let mut horizon_residuals = BTreeMap::<NaiveDate, u64>::new();
        for tranche in pacing.deposits().values() {
            let residual = tranche.residual_usdc().as_micros();
            if residual == 0 {
                continue;
            }
            let entry = horizon_residuals.entry(tranche.target_horizon).or_default();
            *entry = entry
                .checked_add(residual)
                .ok_or(MetricsError::ArithmeticOverflow)?;
        }
        let mut horizons = Vec::with_capacity(horizon_residuals.len());
        for (horizon, residual) in horizon_residuals {
            let signed_days_remaining = horizon
                .signed_duration_since(observed_at.date_naive())
                .num_days()
                .saturating_add(1)
                .max(0);
            let days_remaining = u64::try_from(signed_days_remaining)
                .map_err(|_| MetricsError::ArithmeticOverflow)?;
            let slots = limits.remaining_purchase_slots(observed_at.date_naive(), horizon)?;
            let required_micros = (slots > 0).then(|| residual.div_ceil(slots));
            horizons.push(HorizonMetrics {
                horizon,
                days_remaining,
                purchase_slots_remaining: slots,
                residual_usdc: as_usdc(residual),
                required_pace_usdc: required_micros.map(as_usdc),
                infeasible: required_micros
                    .is_none_or(|required| required > limits.max_daily_notional_usdc.as_micros()),
            });
        }

        let last_capital_event_at = pacing
            .deposits()
            .values()
            .map(|tranche| tranche.received_at)
            .chain(
                pacing
                    .withdrawals()
                    .values()
                    .map(|withdrawal| withdrawal.event.occurred_at),
            )
            .max();
        let last_decision_at = pacing
            .decisions()
            .values()
            .map(|decision| decision.decided_at)
            .max();

        let mut last_fill_at = None;
        let mut unstaked_hype_atoms = 0_u64;
        let mut delegated_hype_atoms = 0_u64;
        let mut pending_workflows = 0_u64;
        let mut manual_review_workflows = 0_u64;
        let mut pending_workflow_age_seconds = None;
        let mut workflow_stuck = false;
        let mut workflow_keys = BTreeSet::new();
        for workflow in workflows {
            if !workflow_keys.insert(&workflow.workflow_key) {
                return Err(MetricsError::DuplicateWorkflow);
            }
            if workflow.last_transition_at > observed_at
                || workflow.last_fill_at.is_some_and(|fill| fill > observed_at)
            {
                return Err(MetricsError::FutureWorkflowObservation);
            }
            last_fill_at = last_fill_at.max(workflow.last_fill_at);
            delegated_hype_atoms = delegated_hype_atoms
                .checked_add(workflow.delegated_hype.as_atoms())
                .ok_or(MetricsError::ArithmeticOverflow)?;
            unstaked_hype_atoms = unstaked_hype_atoms
                .checked_add(workflow.unstaked_hype_atoms()?)
                .ok_or(MetricsError::ArithmeticOverflow)?;
            if workflow.stage != WorkflowStage::Complete {
                pending_workflows = pending_workflows
                    .checked_add(1)
                    .ok_or(MetricsError::ArithmeticOverflow)?;
                let age = u64::try_from(
                    observed_at
                        .signed_duration_since(workflow.last_transition_at)
                        .num_seconds(),
                )
                .map_err(|_| MetricsError::FutureWorkflowObservation)?;
                pending_workflow_age_seconds =
                    Some(pending_workflow_age_seconds.map_or(age, |oldest: u64| oldest.max(age)));
                if workflow.stage == WorkflowStage::ManualReview {
                    manual_review_workflows = manual_review_workflows
                        .checked_add(1)
                        .ok_or(MetricsError::ArithmeticOverflow)?;
                } else if age >= stuck_after_seconds {
                    workflow_stuck = true;
                }
            }
        }

        let stale_signal = signal
            .is_none_or(|snapshot| !matches!(snapshot.core_health(), CoreHealth::Healthy { .. }));
        let halt_reason = if manual_review_workflows > 0 {
            Some(HaltReason::ManualReview)
        } else if workflow_stuck {
            Some(HaltReason::StuckWorkflow)
        } else if stale_signal {
            Some(HaltReason::StaleSignal)
        } else {
            None
        };

        Ok(Self {
            observed_at,
            confirmed_deposits_usdc: as_usdc(confirmed),
            admitted_deposits_usdc: as_usdc(admitted),
            unallocated_deposits_usdc: as_usdc(unallocated),
            deployable_usdc: as_usdc(deployable),
            uninvested_usdc: as_usdc(uninvested),
            committed_usdc: as_usdc(committed),
            spent_usdc: as_usdc(spent),
            last_capital_event_at,
            last_decision_at,
            last_fill_at,
            unstaked_hype_atoms,
            delegated_hype_atoms,
            pending_workflows,
            manual_review_workflows,
            pending_workflow_age_seconds,
            workflow_stuck,
            api_errors_total,
            stale_signal_events_total,
            dry_run_actions_total,
            stale_signal,
            halt_reason,
            horizons,
        })
    }

    /// Renders Prometheus text without identifiers or secret data.
    #[must_use]
    #[allow(clippy::too_many_lines)]
    pub fn to_prometheus(&self) -> String {
        let mut output = String::new();
        for (name, help, value) in [
            (
                "hype_accumulator_confirmed_deposits_usdc",
                "Authoritative confirmed deposits.",
                self.confirmed_deposits_usdc.to_string(),
            ),
            (
                "hype_accumulator_admitted_deposits_usdc",
                "Deposits admitted for pacing.",
                self.admitted_deposits_usdc.to_string(),
            ),
            (
                "hype_accumulator_unallocated_deposits_usdc",
                "Confirmed deposits not admitted.",
                self.unallocated_deposits_usdc.to_string(),
            ),
            (
                "hype_accumulator_deployable_usdc",
                "Capital available for a new commitment.",
                self.deployable_usdc.to_string(),
            ),
            (
                "hype_accumulator_uninvested_usdc",
                "Admitted capital not invested or committed.",
                self.uninvested_usdc.to_string(),
            ),
            (
                "hype_accumulator_committed_usdc",
                "Capital committed to unsettled decisions.",
                self.committed_usdc.to_string(),
            ),
            (
                "hype_accumulator_spent_usdc",
                "Authoritatively settled cash debit.",
                self.spent_usdc.to_string(),
            ),
            (
                "hype_accumulator_unstaked_hype_atoms",
                "Decision-attributed HYPE not confirmed in staking.",
                self.unstaked_hype_atoms.to_string(),
            ),
            (
                "hype_accumulator_delegated_hype_atoms",
                "Decision-attributed HYPE confirmed delegated.",
                self.delegated_hype_atoms.to_string(),
            ),
            (
                "hype_accumulator_pending_workflows",
                "Incomplete daily workflows.",
                self.pending_workflows.to_string(),
            ),
            (
                "hype_accumulator_manual_review_workflows",
                "Workflows halted for manual review.",
                self.manual_review_workflows.to_string(),
            ),
            (
                "hype_accumulator_pending_workflow_age_seconds",
                "Age of the oldest incomplete workflow.",
                self.pending_workflow_age_seconds.unwrap_or(0).to_string(),
            ),
            (
                "hype_accumulator_workflow_stuck",
                "Whether a nonterminal workflow exceeded its age threshold.",
                u8::from(self.workflow_stuck).to_string(),
            ),
            (
                "hype_accumulator_stale_signal",
                "Whether the core signal is absent or stale.",
                u8::from(self.stale_signal).to_string(),
            ),
        ] {
            writeln!(output, "# HELP {name} {help}").expect("write string");
            writeln!(output, "# TYPE {name} gauge").expect("write string");
            writeln!(output, "{name} {value}").expect("write string");
        }
        for (name, help, value) in [
            (
                "hype_accumulator_api_errors_total",
                "Read-side API errors.",
                self.api_errors_total,
            ),
            (
                "hype_accumulator_stale_signal_events_total",
                "Observed stale-signal events.",
                self.stale_signal_events_total,
            ),
            (
                "hype_accumulator_dry_run_actions_total",
                "External actions suppressed by DRY_RUN.",
                self.dry_run_actions_total,
            ),
        ] {
            writeln!(output, "# HELP {name} {help}").expect("write string");
            writeln!(output, "# TYPE {name} counter").expect("write string");
            writeln!(output, "{name} {value}").expect("write string");
        }
        writeln!(
            output,
            "# HELP hype_accumulator_horizon_residual_usdc Residual admitted capital by horizon."
        )
        .expect("write string");
        writeln!(
            output,
            "# TYPE hype_accumulator_horizon_residual_usdc gauge"
        )
        .expect("write string");
        writeln!(output, "# HELP hype_accumulator_horizon_purchase_slots_remaining Purchase slots remaining by horizon.").expect("write string");
        writeln!(
            output,
            "# TYPE hype_accumulator_horizon_purchase_slots_remaining gauge"
        )
        .expect("write string");
        writeln!(output, "# HELP hype_accumulator_horizon_infeasible Whether required pace exceeds the daily cap.").expect("write string");
        writeln!(output, "# TYPE hype_accumulator_horizon_infeasible gauge").expect("write string");
        for horizon in &self.horizons {
            writeln!(
                output,
                "hype_accumulator_horizon_residual_usdc{{horizon=\"{}\"}} {}",
                horizon.horizon, horizon.residual_usdc
            )
            .expect("write string");
            writeln!(
                output,
                "hype_accumulator_horizon_purchase_slots_remaining{{horizon=\"{}\"}} {}",
                horizon.horizon, horizon.purchase_slots_remaining
            )
            .expect("write string");
            writeln!(
                output,
                "hype_accumulator_horizon_infeasible{{horizon=\"{}\"}} {}",
                horizon.horizon,
                u8::from(horizon.infeasible)
            )
            .expect("write string");
        }
        writeln!(
            output,
            "# HELP hype_accumulator_halt Whether operations are halted, by stable reason."
        )
        .expect("write string");
        writeln!(output, "# TYPE hype_accumulator_halt gauge").expect("write string");
        if let Some(reason) = self.halt_reason {
            writeln!(
                output,
                "hype_accumulator_halt{{reason=\"{}\"}} 1",
                reason.label()
            )
            .expect("write string");
        }
        output
    }
}

impl WorkflowObservation {
    fn unstaked_hype_atoms(&self) -> Result<u64, MetricsError> {
        let staking_confirmed = self.staking_target_hype.as_atoms() > 0
            && matches!(
                self.stage,
                WorkflowStage::StakingBalanceConfirmed
                    | WorkflowStage::DelegationSubmitted
                    | WorkflowStage::DelegatedConfirmed
                    | WorkflowStage::Complete
            );
        if staking_confirmed {
            self.staking_eligible_hype
                .as_atoms()
                .checked_sub(self.staking_target_hype.as_atoms())
                .ok_or(MetricsError::InconsistentCapitalState)
        } else {
            Ok(self.staking_eligible_hype.as_atoms())
        }
    }
}

fn checked_sum(values: impl IntoIterator<Item = u64>) -> Result<u64, MetricsError> {
    values.into_iter().try_fold(0_u64, |total, value| {
        total
            .checked_add(value)
            .ok_or(MetricsError::ArithmeticOverflow)
    })
}

#[allow(clippy::cast_precision_loss)]
fn as_usdc(micros: u64) -> f64 {
    micros as f64 / 1_000_000.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::CarryOverPolicy,
        ledger::{
            DurableLedger, LedgerEvent, LedgerEventKind, ProtectedAnchorStore, ProtectedHeadAnchor,
        },
        pacing::{CapitalEvent, DepositEvent, UsdcMicros},
        status::{AccumulatorStatus, DashboardStatus},
        status_io::write_metrics_atomic,
    };
    use chrono::TimeZone;
    use std::{
        collections::BTreeSet,
        sync::{Arc, Mutex},
    };

    #[derive(Default)]
    struct MemoryAnchor(Mutex<Option<ProtectedHeadAnchor>>);

    impl ProtectedAnchorStore for MemoryAnchor {
        fn load(&self) -> Result<Option<ProtectedHeadAnchor>, String> {
            self.0
                .lock()
                .map(|value| value.clone())
                .map_err(|_| "anchor lock poisoned".into())
        }

        fn compare_and_swap(
            &self,
            expected: Option<&ProtectedHeadAnchor>,
            next: &ProtectedHeadAnchor,
        ) -> Result<bool, String> {
            let mut value = self
                .0
                .lock()
                .map_err(|_| "anchor lock poisoned".to_owned())?;
            if value.as_ref() != expected {
                return Ok(false);
            }
            *value = Some(next.clone());
            Ok(true)
        }
    }

    fn at(hour: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 25, hour, 0, 0)
            .single()
            .expect("valid fixture time")
    }

    fn usd(value: u64) -> UsdcMicros {
        UsdcMicros::checked_from_whole_usdc(value).expect("small fixture amount")
    }

    fn limits() -> PacingLimits {
        PacingLimits {
            min_deposit_confirmations: 1,
            max_automatically_admitted_usdc: usd(1_000),
            yearly_admission_cap_usdc: usd(1_000),
            cumulative_admission_cap_usdc: usd(2_000),
            deposit_cooldown_seconds: 1,
            min_order_usdc: usd(1),
            max_daily_notional_usdc: usd(100),
            fixed_reserve_usdc: usd(1),
            fee_spread_reserve_bps: 10,
            final_catch_up_days: 7,
            carry_over_policy: CarryOverPolicy::HoldForApproval,
            utc_hour: 12,
            utc_minute: 0,
            weekdays: BTreeSet::from([1, 2, 3, 4, 5]),
        }
    }

    fn stuck_observation() -> WorkflowObservation {
        WorkflowObservation {
            workflow_key: "workflow-1".into(),
            stage: WorkflowStage::Decided,
            last_transition_at: at(10),
            last_fill_at: None,
            staking_eligible_hype: HypeAtoms::from_atoms(0),
            staking_target_hype: HypeAtoms::from_atoms(0),
            delegated_hype: HypeAtoms::from_atoms(0),
        }
    }

    #[test]
    fn synthetic_stuck_workflow_sets_status_and_prometheus_alert_inputs() {
        let snapshot = MetricsSnapshot::from_runtime(
            at(12),
            &PacingState::default(),
            &limits(),
            &ReplayState::default(),
            &[stuck_observation()],
            None,
            2,
            3,
            4,
            3_600,
        )
        .expect("consistent empty capital projection");

        assert!(snapshot.workflow_stuck);
        assert_eq!(snapshot.pending_workflows, 1);
        assert_eq!(snapshot.pending_workflow_age_seconds, Some(7_200));
        assert_eq!(snapshot.halt_reason, Some(HaltReason::StuckWorkflow));

        let prometheus = snapshot.to_prometheus();
        assert!(prometheus.contains("hype_accumulator_workflow_stuck 1"));
        assert!(prometheus.contains("hype_accumulator_halt{reason=\"stuck_workflow\"} 1"));
        assert!(prometheus.contains("hype_accumulator_api_errors_total 2"));
        for forbidden in [
            "wallet",
            "address",
            "signature",
            "signed_payload",
            "action_id",
        ] {
            assert!(!prometheus.contains(forbidden));
        }
    }

    #[test]
    fn operations_are_optional_and_attach_to_dashboard_status() {
        let snapshot = MetricsSnapshot::from_runtime(
            at(12),
            &PacingState::default(),
            &limits(),
            &ReplayState::default(),
            &[],
            None,
            0,
            1,
            0,
            3_600,
        )
        .expect("empty snapshot");
        let accumulator = AccumulatorStatus::new(10.0, 0.0, 40.0, at(12), None, "daily", None)
            .expect("balance snapshot");
        let status = DashboardStatus::new(at(12), at(10), true, accumulator)
            .with_operations(snapshot.clone())
            .expect("same-time operations");
        let json = status.to_json().expect("dashboard JSON");
        let value: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");

        assert_eq!(value["schema_version"], 1);
        assert_eq!(value["operations"]["stale_signal"], true);
        assert_eq!(value["operations"]["halt_reason"], "stale_signal");
        assert!(!json.contains("account"));
        assert!(!json.contains("signature"));

        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("metrics.prom");
        write_metrics_atomic(&path, &snapshot).expect("atomic metrics write");
        let written = std::fs::read_to_string(path).expect("metrics payload");
        assert!(written.ends_with('\n'));
        assert!(written.contains("hype_accumulator_stale_signal 1"));
    }

    #[test]
    fn admitted_capital_and_horizon_are_projected_from_consistent_states() {
        let limits = limits();
        let deposit = DepositEvent {
            event_id: "deposit-1".into(),
            amount_usdc: usd(100),
            received_at: at(8),
            confirmed_at: Some(at(8)),
            confirmation_count: 1,
            admission_approved_at: Some(at(8)),
        };
        let mut pacing = PacingState::default();
        pacing
            .reconcile_capital(&[CapitalEvent::Deposit(deposit)], at(10), &limits)
            .expect("capital admitted");

        let directory = tempfile::tempdir().expect("temporary ledger");
        let mut ledger = DurableLedger::open(directory.path(), Arc::new(MemoryAnchor::default()))
            .expect("ledger opens");
        ledger
            .append(LedgerEvent {
                event_id: "deposit-1".into(),
                occurred_at: at(8),
                kind: LedgerEventKind::AuthoritativeDeposit {
                    amount_usdc: usd(100),
                },
            })
            .expect("deposit recorded");
        ledger
            .append(LedgerEvent {
                event_id: "admission-1".into(),
                occurred_at: at(10),
                kind: LedgerEventKind::DepositAdmission {
                    deposit_event_id: "deposit-1".into(),
                    amount_usdc: usd(100),
                },
            })
            .expect("admission recorded");

        let snapshot = MetricsSnapshot::from_runtime(
            at(12),
            &pacing,
            &limits,
            ledger.state(),
            &[],
            None,
            0,
            0,
            0,
            3_600,
        )
        .expect("consistent capital projection");

        assert!((snapshot.confirmed_deposits_usdc - 100.0).abs() < f64::EPSILON);
        assert!((snapshot.admitted_deposits_usdc - 100.0).abs() < f64::EPSILON);
        assert!(snapshot.unallocated_deposits_usdc.abs() < f64::EPSILON);
        assert!((snapshot.deployable_usdc - 100.0).abs() < f64::EPSILON);
        assert_eq!(snapshot.last_capital_event_at, Some(at(8)));
        assert_eq!(snapshot.horizons.len(), 1);
        assert_eq!(
            snapshot.horizons[0].horizon,
            NaiveDate::from_ymd_opt(2026, 12, 31).expect("valid horizon")
        );
        assert!(snapshot.horizons[0].purchase_slots_remaining > 0);
        assert!(snapshot.horizons[0].required_pace_usdc.is_some());
    }

    #[test]
    fn invalid_threshold_and_future_workflow_fail_closed() {
        assert!(matches!(
            MetricsSnapshot::from_runtime(
                at(12),
                &PacingState::default(),
                &limits(),
                &ReplayState::default(),
                &[],
                None,
                0,
                0,
                0,
                0,
            ),
            Err(MetricsError::InvalidStuckThreshold)
        ));

        let duplicate = stuck_observation();
        assert!(matches!(
            MetricsSnapshot::from_runtime(
                at(12),
                &PacingState::default(),
                &limits(),
                &ReplayState::default(),
                &[duplicate.clone(), duplicate],
                None,
                0,
                0,
                0,
                3_600,
            ),
            Err(MetricsError::DuplicateWorkflow)
        ));

        let mut future = stuck_observation();
        future.last_transition_at = at(13);
        assert!(matches!(
            MetricsSnapshot::from_runtime(
                at(12),
                &PacingState::default(),
                &limits(),
                &ReplayState::default(),
                &[future],
                None,
                0,
                0,
                0,
                3_600,
            ),
            Err(MetricsError::FutureWorkflowObservation)
        ));
    }
}
