//! Deterministic, offline-only capital admission and fixed-DCA planning.
//!
//! This module never constructs a signer or submits an exchange action. A new
//! non-zero [`DailyDecision`] is an idempotent economic intent that must be
//! persisted before a separate execution layer may act on it. Replaying the
//! same UTC day returns [`DecisionResult::Existing`], which is audit-only and
//! must never be submitted again.

use crate::config::{CarryOverPolicy, Config};
use chrono::{DateTime, Datelike, Days, NaiveDate, TimeDelta, Timelike, Utc, Weekday};
use rust_decimal::{
    prelude::{FromPrimitive, ToPrimitive},
    Decimal,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
};
use thiserror::Error;

const STATE_SCHEMA_VERSION: u8 = 1;
const BPS_DENOMINATOR: u64 = 10_000;
const USDC_MICROS_PER_UNIT: u64 = 1_000_000;
type DueRow = (NaiveDate, DateTime<Utc>, String, UsdcMicros, UsdcMicros);

/// Exact USDC microunits. Integer accounting avoids float-dependent pacing or
/// restart drift and matches USDC's six-decimal precision.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct UsdcMicros(u64);

impl UsdcMicros {
    #[must_use]
    pub const fn from_micros(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn checked_from_whole_usdc(value: u64) -> Option<Self> {
        match value.checked_mul(USDC_MICROS_PER_UNIT) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    #[must_use]
    pub const fn as_micros(self) -> u64 {
        self.0
    }

    #[must_use]
    pub const fn is_zero(self) -> bool {
        self.0 == 0
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PacingLimits {
    pub min_deposit_confirmations: u32,
    pub max_automatically_admitted_usdc: UsdcMicros,
    pub yearly_admission_cap_usdc: UsdcMicros,
    pub cumulative_admission_cap_usdc: UsdcMicros,
    pub deposit_cooldown_seconds: u64,
    pub min_order_usdc: UsdcMicros,
    pub max_daily_notional_usdc: UsdcMicros,
    pub fee_spread_reserve_bps: u16,
    pub final_catch_up_days: u32,
    pub carry_over_policy: CarryOverPolicy,
    pub utc_hour: u8,
    pub utc_minute: u8,
    pub weekdays: BTreeSet<u8>,
}

impl PacingLimits {
    /// Converts validated application config to exact scheduler limits.
    ///
    /// # Errors
    ///
    /// Returns [`PacingError::InvalidLimits`] when a monetary value cannot be
    /// represented exactly enough as non-negative USDC microunits or when an
    /// invariant is inconsistent. Call [`Config::validate`] first for the full
    /// startup validation path.
    pub fn from_config(config: &Config) -> Result<Self, PacingError> {
        let limits = Self {
            min_deposit_confirmations: config.capital.min_deposit_confirmations,
            max_automatically_admitted_usdc: money_from_f64(
                config.capital.max_automatically_deployable_usdc,
            )?,
            yearly_admission_cap_usdc: money_from_f64(config.capital.yearly_deployment_cap_usdc)?,
            cumulative_admission_cap_usdc: money_from_f64(
                config.capital.cumulative_deployment_cap_usdc,
            )?,
            deposit_cooldown_seconds: config.pacing.deposit_cooldown_seconds,
            min_order_usdc: money_from_f64(config.pacing.min_order_usdc)?,
            max_daily_notional_usdc: money_from_f64(
                config
                    .pacing
                    .max_order_usdc
                    .min(config.execution.max_order_usdc),
            )?,
            fee_spread_reserve_bps: config.pacing.fee_spread_reserve_bps,
            final_catch_up_days: config.pacing.final_catch_up_days,
            carry_over_policy: config.pacing.carry_over_policy,
            utc_hour: config.schedule.utc_hour,
            utc_minute: config.schedule.utc_minute,
            weekdays: config.schedule.weekdays.iter().copied().collect(),
        };
        limits.validate()?;
        Ok(limits)
    }

    /// Validates scheduler-local admission and execution invariants.
    ///
    /// # Errors
    ///
    /// Returns [`PacingError::InvalidLimits`] for unsafe or inconsistent caps.
    pub fn validate(&self) -> Result<(), PacingError> {
        if self.min_deposit_confirmations == 0
            || self.deposit_cooldown_seconds == 0
            || self.min_order_usdc.is_zero()
            || self.max_daily_notional_usdc.is_zero()
            || self.min_order_usdc > self.max_daily_notional_usdc
            || self.max_automatically_admitted_usdc.is_zero()
            || self.yearly_admission_cap_usdc.is_zero()
            || self.cumulative_admission_cap_usdc.is_zero()
            || self.max_automatically_admitted_usdc > self.yearly_admission_cap_usdc
            || self.yearly_admission_cap_usdc > self.cumulative_admission_cap_usdc
            || self.fee_spread_reserve_bps >= 10_000
            || self.final_catch_up_days == 0
            || self.utc_hour > 23
            || self.utc_minute > 59
            || self.weekdays.is_empty()
            || self.weekdays.iter().any(|day| !(1..=7).contains(day))
        {
            return Err(PacingError::InvalidLimits);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum CapitalEvent {
    Deposit(DepositEvent),
    Withdrawal(WithdrawalEvent),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DepositEvent {
    pub event_id: String,
    pub amount_usdc: UsdcMicros,
    pub received_at: DateTime<Utc>,
    pub confirmed_at: Option<DateTime<Utc>>,
    pub confirmation_count: u32,
    pub admission_approved_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WithdrawalEvent {
    pub event_id: String,
    pub amount_usdc: UsdcMicros,
    pub occurred_at: DateTime<Utc>,
    pub reconciled_at: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdmissionStatus {
    AwaitingConfirmation,
    AwaitingApproval,
    CoolingDown,
    Admitted,
    PartiallyAdmitted,
    HeldByAdmissionCap,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DepositTranche {
    pub event_id: String,
    pub source_amount_usdc: UsdcMicros,
    pub received_at: DateTime<Utc>,
    pub confirmed_at: Option<DateTime<Utc>>,
    pub confirmation_count: u32,
    pub admission_approved_at: Option<DateTime<Utc>>,
    pub first_usable_at: Option<DateTime<Utc>>,
    pub target_horizon: NaiveDate,
    pub status: AdmissionStatus,
    pub admitted_usdc: UsdcMicros,
    pub invested_usdc: UsdcMicros,
    pub committed_usdc: UsdcMicros,
    pub withdrawn_usdc: UsdcMicros,
}

impl DepositTranche {
    #[must_use]
    pub fn residual_usdc(&self) -> UsdcMicros {
        UsdcMicros(
            self.admitted_usdc
                .0
                .saturating_sub(self.invested_usdc.0)
                .saturating_sub(self.committed_usdc.0)
                .saturating_sub(self.withdrawn_usdc.0),
        )
    }

    #[must_use]
    pub fn unadmitted_usdc(&self) -> UsdcMicros {
        UsdcMicros(
            self.source_amount_usdc
                .0
                .saturating_sub(self.admitted_usdc.0),
        )
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CapitalAllocation {
    pub tranche_id: String,
    pub amount_usdc: UsdcMicros,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WithdrawalRecord {
    pub event: WithdrawalEvent,
    pub applied: bool,
    pub allocations: Vec<CapitalAllocation>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionReason {
    Planned,
    ManualPause,
    MissingCapitalHistory,
    PriorDecisionUnsettled,
    NoAdmittedCapital,
    HorizonInfeasible,
    BelowExchangeMinimum,
    ReserveBelowMinimum,
    InsufficientObservedBalance,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PacingAlert {
    UnadmittedCapital {
        amount_usdc: UsdcMicros,
    },
    HorizonInfeasible {
        horizon: NaiveDate,
        residual_usdc: UsdcMicros,
        remaining_capacity_usdc: UsdcMicros,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PacingExplanation {
    pub admitted_unspent_usdc: UsdcMicros,
    pub unadmitted_usdc: UsdcMicros,
    pub fixed_required_usdc: UsdcMicros,
    pub observed_budget_after_reserve_usdc: UsdcMicros,
    pub admitted_budget_after_reserve_usdc: UsdcMicros,
    pub exchange_minimum_usdc: UsdcMicros,
    pub daily_cap_usdc: UsdcMicros,
    pub fee_spread_reserve_bps: u16,
    pub final_catch_up_active: bool,
    pub active_tranches: usize,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DecisionAllocation {
    pub tranche_id: String,
    pub committed_usdc: UsdcMicros,
    pub filled_usdc: UsdcMicros,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DailyDecision {
    pub decision_id: String,
    pub decision_date: NaiveDate,
    pub decided_at: DateTime<Utc>,
    pub capital_snapshot_hash: String,
    pub input_snapshot_hash: String,
    pub planned_usdc: UsdcMicros,
    pub filled_usdc: UsdcMicros,
    pub settled: bool,
    pub reason: DecisionReason,
    pub allocations: Vec<DecisionAllocation>,
    pub alerts: Vec<PacingAlert>,
    pub explanation: PacingExplanation,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DecisionInput {
    pub at: DateTime<Utc>,
    pub observed_spot_usdc: UsdcMicros,
    pub capital_history_complete: bool,
    pub manual_pause: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DecisionResult {
    New(DailyDecision),
    Existing(DailyDecision),
}

impl DecisionResult {
    #[must_use]
    pub const fn is_new(&self) -> bool {
        matches!(self, Self::New(_))
    }

    #[must_use]
    pub const fn decision(&self) -> &DailyDecision {
        match self {
            Self::New(decision) | Self::Existing(decision) => decision,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PacingState {
    schema_version: u8,
    deposits: BTreeMap<String, DepositTranche>,
    withdrawals: BTreeMap<String, WithdrawalRecord>,
    decisions: BTreeMap<NaiveDate, DailyDecision>,
}

impl Default for PacingState {
    fn default() -> Self {
        Self {
            schema_version: STATE_SCHEMA_VERSION,
            deposits: BTreeMap::new(),
            withdrawals: BTreeMap::new(),
            decisions: BTreeMap::new(),
        }
    }
}

impl PacingState {
    #[must_use]
    pub const fn deposits(&self) -> &BTreeMap<String, DepositTranche> {
        &self.deposits
    }

    #[must_use]
    pub const fn withdrawals(&self) -> &BTreeMap<String, WithdrawalRecord> {
        &self.withdrawals
    }

    #[must_use]
    pub const fn decisions(&self) -> &BTreeMap<NaiveDate, DailyDecision> {
        &self.decisions
    }

    /// Reconciles authoritative capital events transactionally. Account balance
    /// changes without one of these events never create deployable capital.
    ///
    /// Deposit admission requires confirmations, cooldown, explicit approval,
    /// and available automatic/yearly/cumulative capacity. A reconciled
    /// withdrawal consumes only free tranche residual and never creates a HYPE
    /// sale intent.
    ///
    /// # Errors
    ///
    /// Returns [`PacingError`] for malformed/conflicting events, insufficient
    /// free capital for a withdrawal, arithmetic overflow, or corrupt state.
    pub fn reconcile_capital(
        &mut self,
        events: &[CapitalEvent],
        at: DateTime<Utc>,
        limits: &PacingLimits,
    ) -> Result<(), PacingError> {
        limits.validate()?;
        self.validate_invariants()?;
        let mut next = self.clone();
        let mut ordered = events.iter().collect::<Vec<_>>();
        ordered.sort_by_key(|event| match event {
            CapitalEvent::Deposit(deposit) => (deposit.received_at, 0_u8, &deposit.event_id),
            CapitalEvent::Withdrawal(withdrawal) => {
                (withdrawal.occurred_at, 1_u8, &withdrawal.event_id)
            }
        });
        for event in ordered {
            match event {
                CapitalEvent::Deposit(deposit) => next.upsert_deposit(deposit, limits)?,
                CapitalEvent::Withdrawal(withdrawal) => next.upsert_withdrawal(withdrawal)?,
            }
        }
        next.apply_ready_capital(at, limits)?;
        next.validate_invariants()?;
        *self = next;
        Ok(())
    }

    /// Creates one durable fixed-DCA decision for an eligible UTC date.
    ///
    /// The planner uses admitted tranche residual only, commits a newly planned
    /// amount before returning it, and returns `Existing` on any same-day replay.
    /// It never reads signals and never produces a signed action.
    ///
    /// # Errors
    ///
    /// Returns [`PacingError::DecisionNotDue`] before the configured UTC time or
    /// on an ineligible date, and other errors for corrupt state or overflow.
    pub fn decide(
        &mut self,
        input: &DecisionInput,
        limits: &PacingLimits,
    ) -> Result<DecisionResult, PacingError> {
        limits.validate()?;
        self.validate_invariants()?;
        let date = input.at.date_naive();
        if let Some(existing) = self.decisions.get(&date) {
            return Ok(DecisionResult::Existing(existing.clone()));
        }
        if !self.is_decision_due(input.at, limits) {
            return Err(PacingError::DecisionNotDue);
        }

        let mut next = self.clone();
        let decision = next.build_decision(input, limits)?;
        next.decisions.insert(date, decision.clone());
        next.validate_invariants()?;
        *self = next;
        Ok(DecisionResult::New(decision))
    }

    /// Finalizes a planned offline decision with its cumulative economic fill.
    /// Unfilled commitment is released to the original tranches. Repeating the
    /// exact settlement is idempotent; a conflicting replay fails closed.
    ///
    /// # Errors
    ///
    /// Returns [`PacingError`] for unknown/skip decisions, overfills,
    /// conflicting settlement, corrupt state, or arithmetic overflow.
    pub fn settle_decision(
        &mut self,
        decision_id: &str,
        final_filled_usdc: UsdcMicros,
    ) -> Result<(), PacingError> {
        self.validate_invariants()?;
        let date = self
            .decisions
            .iter()
            .find_map(|(date, decision)| (decision.decision_id == decision_id).then_some(*date))
            .ok_or(PacingError::UnknownDecision)?;
        let existing = self
            .decisions
            .get(&date)
            .ok_or(PacingError::UnknownDecision)?;
        if existing.planned_usdc.is_zero() {
            return Err(PacingError::NotEconomicDecision);
        }
        if existing.settled {
            return if existing.filled_usdc == final_filled_usdc {
                Ok(())
            } else {
                Err(PacingError::ConflictingSettlement)
            };
        }
        if final_filled_usdc > existing.planned_usdc {
            return Err(PacingError::FillExceedsCommitment);
        }

        let mut next = self.clone();
        let allocations = next
            .decisions
            .get(&date)
            .ok_or(PacingError::UnknownDecision)?
            .allocations
            .clone();
        let mut fill_remaining = final_filled_usdc.0;
        let mut filled_by_tranche = Vec::with_capacity(allocations.len());
        for allocation in &allocations {
            let filled = allocation.committed_usdc.0.min(fill_remaining);
            fill_remaining -= filled;
            filled_by_tranche.push(filled);
            let tranche = next
                .deposits
                .get_mut(&allocation.tranche_id)
                .ok_or(PacingError::CorruptState)?;
            tranche.committed_usdc =
                checked_sub(tranche.committed_usdc, allocation.committed_usdc)?;
            tranche.invested_usdc = checked_add(tranche.invested_usdc, UsdcMicros(filled))?;
        }
        if fill_remaining != 0 {
            return Err(PacingError::CorruptState);
        }
        let decision = next
            .decisions
            .get_mut(&date)
            .ok_or(PacingError::UnknownDecision)?;
        for (allocation, filled) in decision.allocations.iter_mut().zip(filled_by_tranche) {
            allocation.filled_usdc = UsdcMicros(filled);
        }
        decision.filled_usdc = final_filled_usdc;
        decision.settled = true;
        next.validate_invariants()?;
        *self = next;
        Ok(())
    }

    /// Checks persisted-state conservation before planning or reconciliation.
    ///
    /// # Errors
    ///
    /// Returns [`PacingError::CorruptState`] for schema, identity, attribution,
    /// or money-conservation violations.
    pub fn validate_invariants(&self) -> Result<(), PacingError> {
        if self.schema_version != STATE_SCHEMA_VERSION {
            return Err(PacingError::CorruptState);
        }
        self.validate_capital_records()?;
        self.validate_decision_attribution()
    }

    fn validate_capital_records(&self) -> Result<(), PacingError> {
        for (id, tranche) in &self.deposits {
            if id != &tranche.event_id
                || invalid_id(id)
                || self.withdrawals.contains_key(id)
                || tranche.source_amount_usdc.is_zero()
                || tranche.admitted_usdc > tranche.source_amount_usdc
            {
                return Err(PacingError::CorruptState);
            }
            let used = checked_sum([
                tranche.invested_usdc,
                tranche.committed_usdc,
                tranche.withdrawn_usdc,
            ])?;
            if used > tranche.admitted_usdc {
                return Err(PacingError::CorruptState);
            }
        }
        for (id, withdrawal) in &self.withdrawals {
            if id != &withdrawal.event.event_id
                || invalid_id(id)
                || withdrawal.event.amount_usdc.is_zero()
            {
                return Err(PacingError::CorruptState);
            }
            let allocated = checked_sum(
                withdrawal
                    .allocations
                    .iter()
                    .map(|allocation| allocation.amount_usdc),
            )?;
            if (withdrawal.applied && allocated != withdrawal.event.amount_usdc)
                || (!withdrawal.applied && !allocated.is_zero())
            {
                return Err(PacingError::CorruptState);
            }
        }
        Ok(())
    }

    fn validate_decision_attribution(&self) -> Result<(), PacingError> {
        let mut decision_ids = BTreeSet::new();
        let mut expected_committed = BTreeMap::<&str, UsdcMicros>::new();
        let mut expected_invested = BTreeMap::<&str, UsdcMicros>::new();
        for (date, decision) in &self.decisions {
            let committed = checked_sum(
                decision
                    .allocations
                    .iter()
                    .map(|allocation| allocation.committed_usdc),
            )?;
            let filled = checked_sum(
                decision
                    .allocations
                    .iter()
                    .map(|allocation| allocation.filled_usdc),
            )?;
            if date != &decision.decision_date
                || decision.decision_id != format!("fixed-dca:{date}")
                || !decision_ids.insert(&decision.decision_id)
                || committed != decision.planned_usdc
                || filled != decision.filled_usdc
                || filled > committed
                || (!decision.settled && !filled.is_zero())
                || (decision.planned_usdc.is_zero() && !decision.allocations.is_empty())
            {
                return Err(PacingError::CorruptState);
            }
            for allocation in &decision.allocations {
                if !self.deposits.contains_key(&allocation.tranche_id)
                    || allocation.filled_usdc > allocation.committed_usdc
                {
                    return Err(PacingError::CorruptState);
                }
                let target = if decision.settled {
                    &mut expected_invested
                } else {
                    &mut expected_committed
                };
                let amount = if decision.settled {
                    allocation.filled_usdc
                } else {
                    allocation.committed_usdc
                };
                let current = target
                    .get(allocation.tranche_id.as_str())
                    .copied()
                    .unwrap_or_default();
                target.insert(
                    allocation.tranche_id.as_str(),
                    checked_add(current, amount)?,
                );
            }
        }
        let mut expected_withdrawn = BTreeMap::<&str, UsdcMicros>::new();
        for withdrawal in self.withdrawals.values().filter(|row| row.applied) {
            for allocation in &withdrawal.allocations {
                if !self.deposits.contains_key(&allocation.tranche_id) {
                    return Err(PacingError::CorruptState);
                }
                let current = expected_withdrawn
                    .get(allocation.tranche_id.as_str())
                    .copied()
                    .unwrap_or_default();
                expected_withdrawn.insert(
                    allocation.tranche_id.as_str(),
                    checked_add(current, allocation.amount_usdc)?,
                );
            }
        }
        for (id, tranche) in &self.deposits {
            if tranche.committed_usdc
                != expected_committed
                    .get(id.as_str())
                    .copied()
                    .unwrap_or_default()
                || tranche.invested_usdc
                    != expected_invested
                        .get(id.as_str())
                        .copied()
                        .unwrap_or_default()
                || tranche.withdrawn_usdc
                    != expected_withdrawn
                        .get(id.as_str())
                        .copied()
                        .unwrap_or_default()
            {
                return Err(PacingError::CorruptState);
            }
        }
        Ok(())
    }

    fn upsert_deposit(
        &mut self,
        event: &DepositEvent,
        limits: &PacingLimits,
    ) -> Result<(), PacingError> {
        validate_deposit(event)?;
        if let Some(existing) = self.deposits.get_mut(&event.event_id) {
            if existing.source_amount_usdc != event.amount_usdc
                || existing.received_at != event.received_at
                || event.confirmation_count < existing.confirmation_count
                || !monotonic_timestamp(existing.confirmed_at, event.confirmed_at)
                || !monotonic_timestamp(existing.admission_approved_at, event.admission_approved_at)
            {
                return Err(PacingError::ConflictingCapitalEvent(event.event_id.clone()));
            }
            existing.confirmation_count = event.confirmation_count;
            existing.confirmed_at = event.confirmed_at;
            existing.admission_approved_at = event.admission_approved_at;
            existing.first_usable_at = first_usable_at(event, limits)?;
            return Ok(());
        }
        if self.withdrawals.contains_key(&event.event_id) {
            return Err(PacingError::ConflictingCapitalEvent(event.event_id.clone()));
        }
        let horizon = NaiveDate::from_ymd_opt(event.received_at.year(), 12, 31)
            .ok_or(PacingError::InvalidCapitalEvent(event.event_id.clone()))?;
        self.deposits.insert(
            event.event_id.clone(),
            DepositTranche {
                event_id: event.event_id.clone(),
                source_amount_usdc: event.amount_usdc,
                received_at: event.received_at,
                confirmed_at: event.confirmed_at,
                confirmation_count: event.confirmation_count,
                admission_approved_at: event.admission_approved_at,
                first_usable_at: first_usable_at(event, limits)?,
                target_horizon: horizon,
                status: initial_status(event, limits),
                admitted_usdc: UsdcMicros::default(),
                invested_usdc: UsdcMicros::default(),
                committed_usdc: UsdcMicros::default(),
                withdrawn_usdc: UsdcMicros::default(),
            },
        );
        Ok(())
    }

    fn upsert_withdrawal(&mut self, event: &WithdrawalEvent) -> Result<(), PacingError> {
        validate_withdrawal(event)?;
        if self.deposits.contains_key(&event.event_id) {
            return Err(PacingError::ConflictingCapitalEvent(event.event_id.clone()));
        }
        if let Some(existing) = self.withdrawals.get(&event.event_id) {
            if &existing.event != event {
                return Err(PacingError::ConflictingCapitalEvent(event.event_id.clone()));
            }
            return Ok(());
        }
        self.withdrawals.insert(
            event.event_id.clone(),
            WithdrawalRecord {
                event: event.clone(),
                applied: false,
                allocations: Vec::new(),
            },
        );
        Ok(())
    }

    fn apply_ready_capital(
        &mut self,
        at: DateTime<Utc>,
        limits: &PacingLimits,
    ) -> Result<(), PacingError> {
        let mut timeline = Vec::<(DateTime<Utc>, u8, String)>::new();
        for tranche in self.deposits.values_mut() {
            if let Some(usable_at) = tranche.first_usable_at.filter(|value| *value <= at) {
                if !tranche.unadmitted_usdc().is_zero() {
                    timeline.push((usable_at, 0, tranche.event_id.clone()));
                }
            } else {
                tranche.status = status_before_admission(tranche, limits, at);
            }
        }
        timeline.extend(
            self.withdrawals
                .values()
                .filter(|record| !record.applied && record.event.reconciled_at <= at)
                .map(|record| (record.event.occurred_at, 1, record.event.event_id.clone())),
        );
        timeline.sort();
        for (_, kind, id) in timeline {
            if kind == 0 {
                self.admit_ready_deposit(&id, limits)?;
            } else {
                self.apply_ready_withdrawal(&id)?;
            }
        }
        Ok(())
    }

    fn admit_ready_deposit(&mut self, id: &str, limits: &PacingLimits) -> Result<(), PacingError> {
        let admission_year = self
            .deposits
            .get(id)
            .ok_or(PacingError::CorruptState)?
            .received_at
            .year();
        let (admitted_total, admitted_year) = self.admitted_totals(admission_year)?;
        let tranche = self.deposits.get_mut(id).ok_or(PacingError::CorruptState)?;
        let unadmitted = tranche.unadmitted_usdc();
        let capacity = [
            checked_sub_floor(limits.max_automatically_admitted_usdc, admitted_total),
            checked_sub_floor(limits.cumulative_admission_cap_usdc, admitted_total),
            checked_sub_floor(limits.yearly_admission_cap_usdc, admitted_year),
        ]
        .into_iter()
        .min()
        .ok_or(PacingError::CorruptState)?;
        let newly_admitted = UsdcMicros(unadmitted.0.min(capacity.0));
        tranche.admitted_usdc = checked_add(tranche.admitted_usdc, newly_admitted)?;
        tranche.status = if tranche.admitted_usdc == tranche.source_amount_usdc {
            AdmissionStatus::Admitted
        } else if tranche.admitted_usdc.is_zero() {
            AdmissionStatus::HeldByAdmissionCap
        } else {
            AdmissionStatus::PartiallyAdmitted
        };
        Ok(())
    }

    fn admitted_totals(&self, year: i32) -> Result<(UsdcMicros, UsdcMicros), PacingError> {
        let admitted_total =
            checked_sum(self.deposits.values().map(|tranche| tranche.admitted_usdc))?;
        let admitted_year = checked_sum(
            self.deposits
                .values()
                .filter(|tranche| tranche.received_at.year() == year)
                .map(|tranche| tranche.admitted_usdc),
        )?;
        Ok((admitted_total, admitted_year))
    }

    fn apply_ready_withdrawal(&mut self, id: &str) -> Result<(), PacingError> {
        let record = self.withdrawals.get(id).ok_or(PacingError::CorruptState)?;
        let free = checked_sum(self.deposits.values().map(DepositTranche::residual_usdc))?;
        if record.event.amount_usdc > free {
            return Err(PacingError::WithdrawalExceedsFreeCapital(id.to_owned()));
        }
        let target = record.event.amount_usdc;
        let mut tranche_ids = self.deposits.keys().cloned().collect::<Vec<_>>();
        tranche_ids.sort_by_key(|tranche_id| {
            let tranche = &self.deposits[tranche_id];
            (
                tranche.target_horizon,
                tranche.received_at,
                tranche.event_id.clone(),
            )
        });
        let mut remaining = target.0;
        let mut allocations = Vec::new();
        for tranche_id in tranche_ids {
            if remaining == 0 {
                break;
            }
            let tranche = self
                .deposits
                .get_mut(&tranche_id)
                .ok_or(PacingError::CorruptState)?;
            let amount = tranche.residual_usdc().0.min(remaining);
            if amount == 0 {
                continue;
            }
            tranche.withdrawn_usdc = checked_add(tranche.withdrawn_usdc, UsdcMicros(amount))?;
            remaining -= amount;
            allocations.push(CapitalAllocation {
                tranche_id,
                amount_usdc: UsdcMicros(amount),
            });
        }
        if remaining != 0 {
            return Err(PacingError::CorruptState);
        }
        let record = self
            .withdrawals
            .get_mut(id)
            .ok_or(PacingError::CorruptState)?;
        record.applied = true;
        record.allocations = allocations;
        Ok(())
    }

    fn is_decision_due(&self, at: DateTime<Utc>, limits: &PacingLimits) -> bool {
        if at.hour() < u32::from(limits.utc_hour)
            || (at.hour() == u32::from(limits.utc_hour)
                && at.minute() < u32::from(limits.utc_minute))
        {
            return false;
        }
        let date = at.date_naive();
        if limits.weekdays.contains(&weekday_number(at.weekday())) {
            return true;
        }
        self.deposits.values().any(|tranche| {
            !tranche.residual_usdc().is_zero()
                && date <= tranche.target_horizon
                && days_inclusive(date, tranche.target_horizon)
                    .is_some_and(|days| days <= u64::from(limits.final_catch_up_days))
        })
    }

    fn build_decision(
        &mut self,
        input: &DecisionInput,
        limits: &PacingLimits,
    ) -> Result<DailyDecision, PacingError> {
        let date = input.at.date_naive();
        let capital_snapshot_hash = capital_snapshot_hash(self)?;
        let input_snapshot_hash = snapshot_hash(&(input, limits))?;
        let admitted_unspent =
            checked_sum(self.deposits.values().map(DepositTranche::residual_usdc))?;
        let unadmitted = checked_sum(self.deposits.values().map(DepositTranche::unadmitted_usdc))?;
        let observed_budget =
            apply_reserve(input.observed_spot_usdc, limits.fee_spread_reserve_bps)?;
        let mut alerts = Vec::new();
        if !unadmitted.is_zero() {
            alerts.push(PacingAlert::UnadmittedCapital {
                amount_usdc: unadmitted,
            });
        }

        let (due_rows, expired_residual, final_catch_up_active) =
            self.collect_due_rows(date, limits)?;
        let fixed_required = checked_sum(due_rows.iter().map(|row| row.4))?;
        Self::append_infeasibility_alerts(date, limits, &due_rows, &mut alerts)?;
        if !expired_residual.is_zero() {
            alerts.push(PacingAlert::HorizonInfeasible {
                horizon: date.checked_sub_days(Days::new(1)).unwrap_or(date),
                residual_usdc: expired_residual,
                remaining_capacity_usdc: UsdcMicros::default(),
            });
        }

        let mut reason = DecisionReason::Planned;
        let mut planned = UsdcMicros::default();
        let prior_unsettled = self.decisions.values().any(|decision| {
            decision.decision_date < date && !decision.planned_usdc.is_zero() && !decision.settled
        });
        if input.manual_pause {
            reason = DecisionReason::ManualPause;
        } else if !input.capital_history_complete {
            reason = DecisionReason::MissingCapitalHistory;
        } else if prior_unsettled {
            reason = DecisionReason::PriorDecisionUnsettled;
        } else if due_rows.is_empty() {
            reason = if expired_residual.is_zero() {
                DecisionReason::NoAdmittedCapital
            } else {
                DecisionReason::HorizonInfeasible
            };
        } else {
            let active_residual = checked_sum(due_rows.iter().map(|row| row.3))?;
            let admitted_budget = apply_reserve(active_residual, limits.fee_spread_reserve_bps)?;
            if active_residual < limits.min_order_usdc {
                reason = DecisionReason::BelowExchangeMinimum;
            } else if admitted_budget < limits.min_order_usdc {
                reason = DecisionReason::ReserveBelowMinimum;
            } else if observed_budget < limits.min_order_usdc {
                reason = DecisionReason::InsufficientObservedBalance;
            } else {
                let desired = UsdcMicros(fixed_required.0.max(limits.min_order_usdc.0));
                planned = [
                    desired,
                    admitted_budget,
                    observed_budget,
                    limits.max_daily_notional_usdc,
                ]
                .into_iter()
                .min()
                .ok_or(PacingError::CorruptState)?;
                if planned < limits.min_order_usdc {
                    planned = UsdcMicros::default();
                    reason = DecisionReason::BelowExchangeMinimum;
                }
            }
        }

        let allocations = self.commit_plan(planned, &due_rows)?;
        let explanation = PacingExplanation {
            admitted_unspent_usdc: admitted_unspent,
            unadmitted_usdc: unadmitted,
            fixed_required_usdc: fixed_required,
            observed_budget_after_reserve_usdc: observed_budget,
            admitted_budget_after_reserve_usdc: apply_reserve(
                admitted_unspent,
                limits.fee_spread_reserve_bps,
            )?,
            exchange_minimum_usdc: limits.min_order_usdc,
            daily_cap_usdc: limits.max_daily_notional_usdc,
            fee_spread_reserve_bps: limits.fee_spread_reserve_bps,
            final_catch_up_active,
            active_tranches: due_rows.len(),
        };
        Ok(DailyDecision {
            decision_id: format!("fixed-dca:{date}"),
            decision_date: date,
            decided_at: input.at,
            capital_snapshot_hash,
            input_snapshot_hash,
            planned_usdc: planned,
            filled_usdc: UsdcMicros::default(),
            settled: false,
            reason,
            allocations,
            alerts,
            explanation,
        })
    }

    fn collect_due_rows(
        &self,
        date: NaiveDate,
        limits: &PacingLimits,
    ) -> Result<(Vec<DueRow>, UsdcMicros, bool), PacingError> {
        let mut due_rows = Vec::new();
        let mut expired_residual = UsdcMicros::default();
        let mut final_catch_up_active = false;
        for tranche in self.deposits.values() {
            let residual = tranche.residual_usdc();
            if residual.is_zero() {
                continue;
            }
            if tranche.target_horizon < date {
                expired_residual = checked_add(expired_residual, residual)?;
                continue;
            }
            let slots = eligible_days(date, tranche.target_horizon, limits)?;
            if slots == 0 {
                expired_residual = checked_add(expired_residual, residual)?;
                continue;
            }
            let required = UsdcMicros(div_ceil(residual.0, slots));
            let catch_up = days_inclusive(date, tranche.target_horizon)
                .is_some_and(|days| days <= u64::from(limits.final_catch_up_days));
            final_catch_up_active |= catch_up;
            due_rows.push((
                tranche.target_horizon,
                tranche.received_at,
                tranche.event_id.clone(),
                residual,
                required,
            ));
        }
        due_rows.sort_by_key(|row| (row.0, row.1, row.2.clone()));
        Ok((due_rows, expired_residual, final_catch_up_active))
    }

    fn append_infeasibility_alerts(
        date: NaiveDate,
        limits: &PacingLimits,
        due_rows: &[DueRow],
        alerts: &mut Vec<PacingAlert>,
    ) -> Result<(), PacingError> {
        let horizons = due_rows.iter().map(|row| row.0).collect::<BTreeSet<_>>();
        for horizon in horizons {
            let required = checked_sum(
                due_rows
                    .iter()
                    .filter(|row| row.0 <= horizon)
                    .map(|row| row.3),
            )?;
            let slots = eligible_days(date, horizon, limits)?;
            let capacity = UsdcMicros(
                limits
                    .max_daily_notional_usdc
                    .0
                    .checked_mul(slots)
                    .ok_or(PacingError::ArithmeticOverflow)?,
            );
            if required > capacity {
                alerts.push(PacingAlert::HorizonInfeasible {
                    horizon,
                    residual_usdc: required,
                    remaining_capacity_usdc: capacity,
                });
            }
        }
        Ok(())
    }

    fn commit_plan(
        &mut self,
        planned: UsdcMicros,
        due_rows: &[DueRow],
    ) -> Result<Vec<DecisionAllocation>, PacingError> {
        if planned.is_zero() {
            return Ok(Vec::new());
        }
        let mut amounts = BTreeMap::<String, u64>::new();
        let mut remaining = planned.0;
        for row in due_rows {
            if remaining == 0 {
                break;
            }
            let amount = row.4 .0.min(row.3 .0).min(remaining);
            if amount != 0 {
                amounts.insert(row.2.clone(), amount);
                remaining -= amount;
            }
        }
        if remaining != 0 {
            for row in due_rows {
                if remaining == 0 {
                    break;
                }
                let already = amounts.get(&row.2).copied().unwrap_or_default();
                let extra = row.3 .0.saturating_sub(already).min(remaining);
                if extra != 0 {
                    *amounts.entry(row.2.clone()).or_default() += extra;
                    remaining -= extra;
                }
            }
        }
        if remaining != 0 {
            return Err(PacingError::CorruptState);
        }
        let mut allocations = Vec::with_capacity(amounts.len());
        for row in due_rows {
            let Some(amount) = amounts.remove(&row.2) else {
                continue;
            };
            let tranche = self
                .deposits
                .get_mut(&row.2)
                .ok_or(PacingError::CorruptState)?;
            tranche.committed_usdc = checked_add(tranche.committed_usdc, UsdcMicros(amount))?;
            allocations.push(DecisionAllocation {
                tranche_id: row.2.clone(),
                committed_usdc: UsdcMicros(amount),
                filled_usdc: UsdcMicros::default(),
            });
        }
        Ok(allocations)
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum PacingError {
    #[error("invalid pacing limits")]
    InvalidLimits,
    #[error("invalid capital event: {0}")]
    InvalidCapitalEvent(String),
    #[error("capital event conflicts with durable state: {0}")]
    ConflictingCapitalEvent(String),
    #[error("withdrawal exceeds admitted uncommitted capital: {0}")]
    WithdrawalExceedsFreeCapital(String),
    #[error("daily decision is not due")]
    DecisionNotDue,
    #[error("unknown decision")]
    UnknownDecision,
    #[error("decision has no economic commitment")]
    NotEconomicDecision,
    #[error("fill exceeds decision commitment")]
    FillExceedsCommitment,
    #[error("settlement conflicts with durable state")]
    ConflictingSettlement,
    #[error("persisted pacing state violates an invariant")]
    CorruptState,
    #[error("pacing arithmetic overflow")]
    ArithmeticOverflow,
    #[error("snapshot serialization failed: {0}")]
    Snapshot(String),
}

fn validate_deposit(event: &DepositEvent) -> Result<(), PacingError> {
    if invalid_id(&event.event_id)
        || event.amount_usdc.is_zero()
        || event
            .confirmed_at
            .is_some_and(|confirmed| confirmed < event.received_at)
    {
        return Err(PacingError::InvalidCapitalEvent(event.event_id.clone()));
    }
    Ok(())
}

fn validate_withdrawal(event: &WithdrawalEvent) -> Result<(), PacingError> {
    if invalid_id(&event.event_id)
        || event.amount_usdc.is_zero()
        || event.reconciled_at < event.occurred_at
    {
        return Err(PacingError::InvalidCapitalEvent(event.event_id.clone()));
    }
    Ok(())
}

fn invalid_id(id: &str) -> bool {
    id.is_empty() || id.trim() != id
}

fn monotonic_timestamp(old: Option<DateTime<Utc>>, new: Option<DateTime<Utc>>) -> bool {
    old.is_none_or(|old| new == Some(old))
}

fn first_usable_at(
    event: &DepositEvent,
    limits: &PacingLimits,
) -> Result<Option<DateTime<Utc>>, PacingError> {
    if event.confirmation_count < limits.min_deposit_confirmations
        || event.confirmed_at.is_none()
        || event.admission_approved_at.is_none()
    {
        return Ok(None);
    }
    let seconds = i64::try_from(limits.deposit_cooldown_seconds)
        .map_err(|_| PacingError::ArithmeticOverflow)?;
    let cooldown_at = event
        .received_at
        .checked_add_signed(TimeDelta::seconds(seconds))
        .ok_or(PacingError::ArithmeticOverflow)?;
    Ok([
        Some(cooldown_at),
        event.confirmed_at,
        event.admission_approved_at,
    ]
    .into_iter()
    .flatten()
    .max())
}

fn initial_status(event: &DepositEvent, limits: &PacingLimits) -> AdmissionStatus {
    if event.confirmation_count < limits.min_deposit_confirmations || event.confirmed_at.is_none() {
        AdmissionStatus::AwaitingConfirmation
    } else if event.admission_approved_at.is_none() {
        AdmissionStatus::AwaitingApproval
    } else {
        AdmissionStatus::CoolingDown
    }
}

fn status_before_admission(
    tranche: &DepositTranche,
    limits: &PacingLimits,
    at: DateTime<Utc>,
) -> AdmissionStatus {
    if tranche.confirmation_count < limits.min_deposit_confirmations
        || tranche.confirmed_at.is_none()
    {
        AdmissionStatus::AwaitingConfirmation
    } else if tranche.admission_approved_at.is_none() {
        AdmissionStatus::AwaitingApproval
    } else if tranche
        .first_usable_at
        .is_some_and(|usable_at| usable_at > at)
    {
        AdmissionStatus::CoolingDown
    } else {
        AdmissionStatus::HeldByAdmissionCap
    }
}

fn eligible_days(
    start: NaiveDate,
    horizon: NaiveDate,
    limits: &PacingLimits,
) -> Result<u64, PacingError> {
    if start > horizon {
        return Ok(0);
    }
    let total_days = days_inclusive(start, horizon).ok_or(PacingError::ArithmeticOverflow)?;
    let mut eligible = 0_u64;
    for offset in 0..total_days {
        let date = start
            .checked_add_days(Days::new(offset))
            .ok_or(PacingError::ArithmeticOverflow)?;
        let catch_up = total_days - offset <= u64::from(limits.final_catch_up_days);
        if catch_up || limits.weekdays.contains(&weekday_number(date.weekday())) {
            eligible = eligible
                .checked_add(1)
                .ok_or(PacingError::ArithmeticOverflow)?;
        }
    }
    Ok(eligible)
}

fn days_inclusive(start: NaiveDate, end: NaiveDate) -> Option<u64> {
    let days = end.signed_duration_since(start).num_days();
    u64::try_from(days).ok()?.checked_add(1)
}

fn apply_reserve(value: UsdcMicros, reserve_bps: u16) -> Result<UsdcMicros, PacingError> {
    let multiplier = BPS_DENOMINATOR
        .checked_sub(u64::from(reserve_bps))
        .ok_or(PacingError::ArithmeticOverflow)?;
    let value = u128::from(value.0)
        .checked_mul(u128::from(multiplier))
        .ok_or(PacingError::ArithmeticOverflow)?
        / u128::from(BPS_DENOMINATOR);
    Ok(UsdcMicros(
        u64::try_from(value).map_err(|_| PacingError::ArithmeticOverflow)?,
    ))
}

fn div_ceil(value: u64, divisor: u64) -> u64 {
    value / divisor + u64::from(value % divisor != 0)
}

fn checked_add(left: UsdcMicros, right: UsdcMicros) -> Result<UsdcMicros, PacingError> {
    left.0
        .checked_add(right.0)
        .map(UsdcMicros)
        .ok_or(PacingError::ArithmeticOverflow)
}

fn checked_sub(left: UsdcMicros, right: UsdcMicros) -> Result<UsdcMicros, PacingError> {
    left.0
        .checked_sub(right.0)
        .map(UsdcMicros)
        .ok_or(PacingError::CorruptState)
}

const fn checked_sub_floor(left: UsdcMicros, right: UsdcMicros) -> UsdcMicros {
    UsdcMicros(left.0.saturating_sub(right.0))
}

fn checked_sum(values: impl IntoIterator<Item = UsdcMicros>) -> Result<UsdcMicros, PacingError> {
    values
        .into_iter()
        .try_fold(UsdcMicros::default(), checked_add)
}

fn money_from_f64(value: f64) -> Result<UsdcMicros, PacingError> {
    if !value.is_finite() || value <= 0.0 {
        return Err(PacingError::InvalidLimits);
    }
    let decimal = Decimal::from_f64(value).ok_or(PacingError::InvalidLimits)?;
    let scaled = decimal * Decimal::from(USDC_MICROS_PER_UNIT);
    if !scaled.fract().is_zero() {
        return Err(PacingError::InvalidLimits);
    }
    scaled
        .to_u64()
        .map(UsdcMicros)
        .ok_or(PacingError::InvalidLimits)
}

const fn weekday_number(weekday: Weekday) -> u8 {
    match weekday {
        Weekday::Mon => 1,
        Weekday::Tue => 2,
        Weekday::Wed => 3,
        Weekday::Thu => 4,
        Weekday::Fri => 5,
        Weekday::Sat => 6,
        Weekday::Sun => 7,
    }
}

fn capital_snapshot_hash(state: &PacingState) -> Result<String, PacingError> {
    snapshot_hash(&(state.schema_version, &state.deposits, &state.withdrawals))
}

fn snapshot_hash<T: Serialize>(value: &T) -> Result<String, PacingError> {
    let bytes =
        serde_json::to_vec(value).map_err(|error| PacingError::Snapshot(error.to_string()))?;
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(64);
    for byte in digest {
        write!(&mut output, "{byte:02x}")
            .map_err(|error| PacingError::Snapshot(error.to_string()))?;
    }
    Ok(output)
}
