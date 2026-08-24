//! Crash-safe, signer-free orchestration for one daily HYPE allocation.
//!
//! The journal is single-writer and append-only. Every external action is
//! durably prepared and fsynced before it can be returned to a caller. After a
//! restart, a prepared action is reconciliation-only and is never returned as
//! a new submission.

use crate::pacing::{DailyDecision, DecisionReason, UsdcMicros};
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
};
use thiserror::Error;

const JOURNAL_SCHEMA_VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct HypeAtoms(u64);

impl HypeAtoms {
    #[must_use]
    pub const fn from_atoms(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn as_atoms(self) -> u64 {
        self.0
    }

    #[must_use]
    pub const fn is_zero(self) -> bool {
        self.0 == 0
    }

    fn checked_sub(self, other: Self) -> Option<Self> {
        self.0.checked_sub(other.0).map(Self)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InventoryBaseline {
    pub spot_hype_atoms: HypeAtoms,
    pub staking_hype_atoms: HypeAtoms,
    pub delegated_hype_atoms: HypeAtoms,
    pub configured_residual_hype_atoms: HypeAtoms,
}

impl InventoryBaseline {
    fn residual_hype_deficit(&self) -> HypeAtoms {
        self.configured_residual_hype_atoms
            .checked_sub(self.spot_hype_atoms)
            .unwrap_or_default()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CapitalCommitment {
    pub event_id: String,
    pub planned_usdc: UsdcMicros,
    pub committed_usdc: UsdcMicros,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DecisionBinding {
    pub decision_id: String,
    pub decision_date: NaiveDate,
    pub decided_at: DateTime<Utc>,
    pub capital_snapshot_hash: String,
    pub input_snapshot_hash: String,
    pub planned_usdc: UsdcMicros,
    pub committed_usdc: UsdcMicros,
    pub capital_commitments: Vec<CapitalCommitment>,
    pub inventory_before: InventoryBaseline,
}

impl DecisionBinding {
    /// Copies the immutable pacing decision into the execution workflow.
    ///
    /// This deliberately copies the exact committed tranche set and snapshot
    /// hashes. Later deposits cannot resize this binding.
    ///
    /// # Errors
    ///
    /// Returns an error unless the pacing decision is a new, non-zero planned
    /// decision whose committed allocations sum exactly to the daily amount.
    pub fn from_pacing_decision(
        decision: &DailyDecision,
        inventory_before: InventoryBaseline,
    ) -> Result<Self, WorkflowError> {
        if decision.settled
            || decision.planned_usdc.is_zero()
            || decision.filled_usdc != UsdcMicros::from_micros(0)
            || decision.debited_usdc != UsdcMicros::from_micros(0)
            || decision.reason != DecisionReason::Planned
            || decision.allocations.iter().any(|allocation| {
                !allocation.filled_usdc.is_zero() || !allocation.debited_usdc.is_zero()
            })
        {
            return Err(WorkflowError::InvalidBinding(
                "pacing decision is not an unsettled planned purchase".into(),
            ));
        }
        let mut commitments = decision
            .allocations
            .iter()
            .map(|allocation| CapitalCommitment {
                event_id: allocation.tranche_id.trim().to_owned(),
                planned_usdc: allocation.planned_usdc,
                committed_usdc: allocation.committed_usdc,
            })
            .collect::<Vec<_>>();
        commitments.sort_by(|left, right| left.event_id.cmp(&right.event_id));
        let binding = Self {
            decision_id: decision.decision_id.trim().to_owned(),
            decision_date: decision.decision_date,
            decided_at: decision.decided_at,
            capital_snapshot_hash: decision.capital_snapshot_hash.trim().to_owned(),
            input_snapshot_hash: decision.input_snapshot_hash.trim().to_owned(),
            planned_usdc: decision.planned_usdc,
            committed_usdc: decision.committed_usdc,
            capital_commitments: commitments,
            inventory_before,
        };
        binding.validate()?;
        Ok(binding)
    }

    fn validate(&self) -> Result<(), WorkflowError> {
        if self.decision_id.is_empty()
            || self.capital_snapshot_hash.is_empty()
            || self.input_snapshot_hash.is_empty()
            || self.planned_usdc.is_zero()
            || self.committed_usdc < self.planned_usdc
            || self.capital_commitments.is_empty()
            || self.decided_at.date_naive() != self.decision_date
        {
            return Err(WorkflowError::InvalidBinding(
                "decision identity, snapshots, date, and capital must be complete".into(),
            ));
        }
        let mut ids = BTreeSet::new();
        let mut planned_total = 0_u64;
        let mut committed_total = 0_u64;
        for commitment in &self.capital_commitments {
            if commitment.event_id.is_empty()
                || commitment.planned_usdc.is_zero()
                || commitment.committed_usdc < commitment.planned_usdc
                || !ids.insert(commitment.event_id.as_str())
            {
                return Err(WorkflowError::InvalidBinding(
                    "capital commitments must have unique non-empty IDs and amounts".into(),
                ));
            }
            planned_total = planned_total
                .checked_add(commitment.planned_usdc.as_micros())
                .ok_or_else(|| {
                    WorkflowError::InvalidBinding("planned capital overflowed".into())
                })?;
            committed_total = committed_total
                .checked_add(commitment.committed_usdc.as_micros())
                .ok_or_else(|| {
                    WorkflowError::InvalidBinding("committed capital overflowed".into())
                })?;
        }
        if planned_total != self.planned_usdc.as_micros()
            || committed_total != self.committed_usdc.as_micros()
        {
            return Err(WorkflowError::InvalidBinding(
                "capital allocations do not sum to planned and committed USDC".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WorkflowStage {
    Decided,
    OrderSubmitted,
    PartiallyFilled,
    Filled,
    OrderFinalized,
    StakingEligibilityRecorded,
    StakingDepositSubmitted,
    StakingBalanceConfirmed,
    DelegationSubmitted,
    DelegatedConfirmed,
    Complete,
    ManualReview,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionKind {
    SubmitOrder,
    DepositToStaking,
    Delegate,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ExternalAction {
    SubmitOrder {
        action_id: String,
        client_order_id: String,
        notional_usdc: UsdcMicros,
        max_debit_usdc: UsdcMicros,
    },
    DepositToStaking {
        action_id: String,
        amount_hype: HypeAtoms,
    },
    Delegate {
        action_id: String,
        amount_hype: HypeAtoms,
    },
}

impl ExternalAction {
    #[must_use]
    pub fn action_id(&self) -> &str {
        match self {
            Self::SubmitOrder { action_id, .. }
            | Self::DepositToStaking { action_id, .. }
            | Self::Delegate { action_id, .. } => action_id,
        }
    }

    #[must_use]
    pub const fn kind(&self) -> ActionKind {
        match self {
            Self::SubmitOrder { .. } => ActionKind::SubmitOrder,
            Self::DepositToStaking { .. } => ActionKind::DepositToStaking,
            Self::Delegate { .. } => ActionKind::Delegate,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PrepareOutcome {
    Ready(ExternalAction),
    ReconcileOnly { action_id: String, kind: ActionKind },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExternalReceipt {
    Confirmed(String),
    Ambiguous,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderFinality {
    Filled,
    Canceled,
    Expired,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StakingEligibility {
    pub residual_hype: HypeAtoms,
    pub eligible_hype: HypeAtoms,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WorkflowTransition {
    DecisionRecorded {
        workflow_id: String,
        binding: DecisionBinding,
    },
    ActionPrepared {
        action: ExternalAction,
    },
    OrderSubmissionObserved {
        action_id: String,
        exchange_order_id: String,
    },
    OrderSubmissionAbsent {
        action_id: String,
        observation_id: String,
    },
    OrderFillObserved {
        observation_id: String,
        cumulative_hype: HypeAtoms,
        cumulative_filled_usdc: UsdcMicros,
        cumulative_debited_usdc: UsdcMicros,
        fully_filled: bool,
    },
    OrderFinalized {
        action_id: String,
        cumulative_hype: HypeAtoms,
        cumulative_filled_usdc: UsdcMicros,
        cumulative_debited_usdc: UsdcMicros,
        finality: OrderFinality,
    },
    StakingEligibilityRecorded {
        residual_hype: HypeAtoms,
        eligible_hype: HypeAtoms,
    },
    StakingDepositObserved {
        action_id: String,
        receipt: ExternalReceipt,
    },
    StakingBalanceConfirmed {
        observation_id: String,
        attributable_hype: HypeAtoms,
    },
    DelegationObserved {
        action_id: String,
        receipt: ExternalReceipt,
    },
    DelegatedBalanceConfirmed {
        observation_id: String,
        attributable_hype: HypeAtoms,
    },
    Completed,
    ManualReview {
        reason: String,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkflowEvent {
    pub event_id: String,
    pub at: DateTime<Utc>,
    pub transition: WorkflowTransition,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkflowState {
    workflow_id: String,
    binding: DecisionBinding,
    stage: WorkflowStage,
    pending_action: Option<ExternalAction>,
    exchange_order_id: Option<String>,
    purchased_hype: HypeAtoms,
    filled_usdc: UsdcMicros,
    debited_usdc: UsdcMicros,
    residual_hype: HypeAtoms,
    staking_eligible_hype: HypeAtoms,
    staking_target_hype: HypeAtoms,
    delegated_hype: HypeAtoms,
    staking_submitted_at: Option<DateTime<Utc>>,
    delegation_submitted_at: Option<DateTime<Utc>>,
    manual_review_reason: Option<String>,
    last_transition_at: DateTime<Utc>,
}

impl WorkflowState {
    #[must_use]
    pub fn workflow_id(&self) -> &str {
        &self.workflow_id
    }

    #[must_use]
    pub const fn binding(&self) -> &DecisionBinding {
        &self.binding
    }

    #[must_use]
    pub const fn stage(&self) -> WorkflowStage {
        self.stage
    }

    #[must_use]
    pub const fn pending_action(&self) -> Option<&ExternalAction> {
        self.pending_action.as_ref()
    }

    #[must_use]
    pub const fn purchased_hype(&self) -> HypeAtoms {
        self.purchased_hype
    }

    #[must_use]
    pub const fn filled_usdc(&self) -> UsdcMicros {
        self.filled_usdc
    }

    #[must_use]
    pub const fn debited_usdc(&self) -> UsdcMicros {
        self.debited_usdc
    }

    #[must_use]
    pub const fn staking_eligibility(&self) -> StakingEligibility {
        StakingEligibility {
            residual_hype: self.residual_hype,
            eligible_hype: self.staking_eligible_hype,
        }
    }

    #[must_use]
    pub const fn staking_target_hype(&self) -> HypeAtoms {
        self.staking_target_hype
    }

    #[must_use]
    pub const fn delegated_hype(&self) -> HypeAtoms {
        self.delegated_hype
    }

    #[must_use]
    pub fn manual_review_reason(&self) -> Option<&str> {
        self.manual_review_reason.as_deref()
    }

    fn replay(events: &[WorkflowEvent]) -> Result<Self, WorkflowError> {
        let first = events.first().ok_or(WorkflowError::EmptyJournal)?;
        let WorkflowTransition::DecisionRecorded {
            workflow_id,
            binding,
        } = &first.transition
        else {
            return Err(WorkflowError::CorruptJournal(
                "first event is not a decision".into(),
            ));
        };
        binding.validate()?;
        if first.event_id != event_id_for_decision(workflow_id) {
            return Err(WorkflowError::CorruptJournal(
                "decision event ID is not deterministic".into(),
            ));
        }
        let expected_workflow_id = workflow_id_for(binding)?;
        if workflow_id != &expected_workflow_id || first.at != binding.decided_at {
            return Err(WorkflowError::CorruptJournal(
                "decision binding does not match workflow identity".into(),
            ));
        }
        let mut state = Self {
            workflow_id: workflow_id.clone(),
            binding: binding.clone(),
            stage: WorkflowStage::Decided,
            pending_action: None,
            exchange_order_id: None,
            purchased_hype: HypeAtoms::default(),
            filled_usdc: UsdcMicros::from_micros(0),
            debited_usdc: UsdcMicros::from_micros(0),
            residual_hype: HypeAtoms::default(),
            staking_eligible_hype: HypeAtoms::default(),
            staking_target_hype: HypeAtoms::default(),
            delegated_hype: HypeAtoms::default(),
            staking_submitted_at: None,
            delegation_submitted_at: None,
            manual_review_reason: None,
            last_transition_at: first.at,
        };
        for event in &events[1..] {
            state.apply(event)?;
        }
        Ok(state)
    }

    // A centralized exhaustive match makes the persisted transition table
    // auditable and prevents a new event variant from bypassing replay checks.
    #[allow(clippy::too_many_lines)]
    fn apply(&mut self, event: &WorkflowEvent) -> Result<(), WorkflowError> {
        if event.at < self.last_transition_at {
            return Err(WorkflowError::InvalidTransition(
                "transition timestamp regressed".into(),
            ));
        }
        validate_event_id(&self.workflow_id, event)?;
        match &event.transition {
            WorkflowTransition::DecisionRecorded { .. } => {
                return Err(WorkflowError::InvalidTransition(
                    "decision was recorded twice".into(),
                ));
            }
            WorkflowTransition::ActionPrepared { action } => {
                if self.pending_action.is_some() {
                    return Err(WorkflowError::InvalidTransition(
                        "another external action is already pending reconciliation".into(),
                    ));
                }
                self.validate_prepared_action(action)?;
                self.pending_action = Some(action.clone());
                match action {
                    ExternalAction::DepositToStaking { amount_hype, .. } => {
                        self.staking_target_hype = *amount_hype;
                    }
                    ExternalAction::SubmitOrder { .. } | ExternalAction::Delegate { .. } => {}
                }
            }
            WorkflowTransition::OrderSubmissionObserved {
                action_id,
                exchange_order_id,
            } => {
                self.require_pending(ActionKind::SubmitOrder, action_id)?;
                if self.stage != WorkflowStage::Decided || exchange_order_id.trim().is_empty() {
                    return Err(WorkflowError::InvalidTransition(
                        "order submission response is invalid for current state".into(),
                    ));
                }
                self.exchange_order_id = Some(exchange_order_id.trim().to_owned());
                self.pending_action = None;
                self.stage = WorkflowStage::OrderSubmitted;
            }
            WorkflowTransition::OrderSubmissionAbsent {
                action_id,
                observation_id,
            } => {
                self.require_pending(ActionKind::SubmitOrder, action_id)?;
                if self.stage != WorkflowStage::Decided || observation_id.trim().is_empty() {
                    return Err(WorkflowError::InvalidTransition(
                        "absent order submission evidence is invalid for current state".into(),
                    ));
                }
                self.pending_action = None;
                self.stage = WorkflowStage::OrderFinalized;
            }
            WorkflowTransition::OrderFillObserved {
                cumulative_hype,
                cumulative_filled_usdc,
                cumulative_debited_usdc,
                fully_filled,
                ..
            } => {
                if !matches!(
                    self.stage,
                    WorkflowStage::OrderSubmitted
                        | WorkflowStage::PartiallyFilled
                        | WorkflowStage::Filled
                ) {
                    return Err(WorkflowError::InvalidTransition(
                        "fill is invalid for current state".into(),
                    ));
                }
                self.validate_cumulative_fill(
                    *cumulative_hype,
                    *cumulative_filled_usdc,
                    *cumulative_debited_usdc,
                    false,
                )?;
                if self.stage == WorkflowStage::Filled && !fully_filled {
                    return Err(WorkflowError::ContradictoryObservation(
                        "a fully filled order regressed to partial".into(),
                    ));
                }
                self.purchased_hype = *cumulative_hype;
                self.filled_usdc = *cumulative_filled_usdc;
                self.debited_usdc = *cumulative_debited_usdc;
                self.stage = if *fully_filled {
                    WorkflowStage::Filled
                } else {
                    WorkflowStage::PartiallyFilled
                };
            }
            WorkflowTransition::OrderFinalized {
                action_id,
                cumulative_hype,
                cumulative_filled_usdc,
                cumulative_debited_usdc,
                finality,
            } => {
                if action_id != &action_id_for(&self.workflow_id, ActionKind::SubmitOrder)
                    || !matches!(
                        self.stage,
                        WorkflowStage::OrderSubmitted
                            | WorkflowStage::PartiallyFilled
                            | WorkflowStage::Filled
                    )
                {
                    return Err(WorkflowError::InvalidTransition(
                        "order finalization is invalid for current state".into(),
                    ));
                }
                self.validate_cumulative_fill(
                    *cumulative_hype,
                    *cumulative_filled_usdc,
                    *cumulative_debited_usdc,
                    matches!(finality, OrderFinality::Canceled | OrderFinality::Expired),
                )?;
                self.purchased_hype = *cumulative_hype;
                self.filled_usdc = *cumulative_filled_usdc;
                self.debited_usdc = *cumulative_debited_usdc;
                self.stage = WorkflowStage::OrderFinalized;
            }
            WorkflowTransition::StakingEligibilityRecorded {
                residual_hype,
                eligible_hype,
            } => {
                if self.stage != WorkflowStage::OrderFinalized {
                    return Err(WorkflowError::InvalidTransition(
                        "staking eligibility is invalid for current state".into(),
                    ));
                }
                let expected_residual = self
                    .purchased_hype
                    .min(self.binding.inventory_before.residual_hype_deficit());
                let expected_eligible = self
                    .purchased_hype
                    .checked_sub(expected_residual)
                    .ok_or_else(|| {
                        WorkflowError::CorruptJournal("HYPE split underflowed".into())
                    })?;
                if *residual_hype != expected_residual || *eligible_hype != expected_eligible {
                    return Err(WorkflowError::ContradictoryObservation(
                        "unsigned staking eligibility does not conserve purchased HYPE".into(),
                    ));
                }
                self.residual_hype = *residual_hype;
                self.staking_eligible_hype = *eligible_hype;
                self.stage = WorkflowStage::StakingEligibilityRecorded;
            }
            WorkflowTransition::StakingDepositObserved { action_id, receipt } => {
                self.require_pending(ActionKind::DepositToStaking, action_id)?;
                if self.stage != WorkflowStage::OrderFinalized {
                    return Err(WorkflowError::InvalidTransition(
                        "staking response is invalid for current state".into(),
                    ));
                }
                validate_receipt(receipt)?;
                self.pending_action = None;
                self.staking_submitted_at = Some(event.at);
                self.stage = WorkflowStage::StakingDepositSubmitted;
            }
            WorkflowTransition::StakingBalanceConfirmed {
                attributable_hype, ..
            } => {
                if self.stage != WorkflowStage::StakingDepositSubmitted
                    || *attributable_hype != self.staking_target_hype
                {
                    return Err(WorkflowError::ContradictoryObservation(
                        "staking balance does not match decision-attributed HYPE".into(),
                    ));
                }
                self.stage = WorkflowStage::StakingBalanceConfirmed;
            }
            WorkflowTransition::DelegationObserved { action_id, receipt } => {
                self.require_pending(ActionKind::Delegate, action_id)?;
                if self.stage != WorkflowStage::StakingBalanceConfirmed {
                    return Err(WorkflowError::InvalidTransition(
                        "delegation response is invalid for current state".into(),
                    ));
                }
                validate_receipt(receipt)?;
                self.pending_action = None;
                self.delegation_submitted_at = Some(event.at);
                self.stage = WorkflowStage::DelegationSubmitted;
            }
            WorkflowTransition::DelegatedBalanceConfirmed {
                attributable_hype, ..
            } => {
                if self.stage != WorkflowStage::DelegationSubmitted
                    || *attributable_hype != self.staking_target_hype
                {
                    return Err(WorkflowError::ContradictoryObservation(
                        "delegated balance does not match decision-attributed HYPE".into(),
                    ));
                }
                self.delegated_hype = *attributable_hype;
                self.stage = WorkflowStage::DelegatedConfirmed;
            }
            WorkflowTransition::Completed => {
                let unsigned_eligibility_complete =
                    self.stage == WorkflowStage::StakingEligibilityRecorded;
                let future_staking_complete = self.stage == WorkflowStage::DelegatedConfirmed
                    && self.delegated_hype == self.staking_target_hype;
                if !unsigned_eligibility_complete && !future_staking_complete {
                    return Err(WorkflowError::InvalidTransition(
                        "workflow cannot complete before terminal eligibility reconciliation"
                            .into(),
                    ));
                }
                self.stage = WorkflowStage::Complete;
            }
            WorkflowTransition::ManualReview { reason } => {
                if reason.trim().is_empty() {
                    return Err(WorkflowError::InvalidTransition(
                        "manual review transition is invalid".into(),
                    ));
                }
                self.pending_action = None;
                self.manual_review_reason = Some(reason.trim().to_owned());
                self.stage = WorkflowStage::ManualReview;
            }
        }
        self.last_transition_at = event.at;
        Ok(())
    }

    fn validate_prepared_action(&self, action: &ExternalAction) -> Result<(), WorkflowError> {
        let expected_id = action_id_for(&self.workflow_id, action.kind());
        if action.action_id() != expected_id {
            return Err(WorkflowError::InvalidTransition(
                "external action ID is not deterministic".into(),
            ));
        }
        match action {
            ExternalAction::SubmitOrder {
                client_order_id,
                notional_usdc,
                max_debit_usdc,
                ..
            } if self.stage == WorkflowStage::Decided
                && client_order_id == &client_order_id_for(&self.workflow_id)
                && *notional_usdc == self.binding.planned_usdc
                && *max_debit_usdc == self.binding.committed_usdc =>
            {
                Ok(())
            }
            ExternalAction::DepositToStaking { amount_hype, .. }
                if self.stage == WorkflowStage::OrderFinalized
                    && !amount_hype.is_zero()
                    && Some(*amount_hype)
                        == self
                            .purchased_hype
                            .checked_sub(self.binding.inventory_before.residual_hype_deficit()) =>
            {
                Ok(())
            }
            ExternalAction::Delegate { amount_hype, .. }
                if self.stage == WorkflowStage::StakingBalanceConfirmed
                    && *amount_hype == self.staking_target_hype
                    && !amount_hype.is_zero() =>
            {
                Ok(())
            }
            _ => Err(WorkflowError::InvalidTransition(
                "external action payload is invalid for current state".into(),
            )),
        }
    }

    fn validate_cumulative_fill(
        &self,
        cumulative_hype: HypeAtoms,
        cumulative_filled_usdc: UsdcMicros,
        cumulative_debited_usdc: UsdcMicros,
        allow_zero: bool,
    ) -> Result<(), WorkflowError> {
        if (cumulative_hype.is_zero() && !allow_zero)
            || cumulative_hype.is_zero() != cumulative_filled_usdc.is_zero()
            || cumulative_hype < self.purchased_hype
            || cumulative_filled_usdc < self.filled_usdc
            || cumulative_debited_usdc < self.debited_usdc
            || cumulative_debited_usdc < cumulative_filled_usdc
            || cumulative_filled_usdc > self.binding.planned_usdc
            || cumulative_debited_usdc > self.binding.committed_usdc
        {
            return Err(WorkflowError::ContradictoryObservation(
                "cumulative fill/debit regressed, violated its cap, or had zero HYPE".into(),
            ));
        }
        Ok(())
    }

    fn require_pending(&self, kind: ActionKind, action_id: &str) -> Result<(), WorkflowError> {
        match self.pending_action.as_ref() {
            Some(action) if action.kind() == kind && action.action_id() == action_id => Ok(()),
            _ => Err(WorkflowError::InvalidTransition(
                "response does not match the pending write-ahead action".into(),
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AppendOutcome {
    Appended,
    Duplicate,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct JournalRecord {
    schema_version: u8,
    sequence: u64,
    previous_hash: String,
    event: WorkflowEvent,
    record_hash: String,
}

#[derive(Serialize)]
struct RecordHashInput<'a> {
    schema_version: u8,
    sequence: u64,
    previous_hash: &'a str,
    event: &'a WorkflowEvent,
}

pub struct DurableWorkflow {
    path: PathBuf,
    records: Vec<JournalRecord>,
    events_by_id: BTreeMap<String, WorkflowEvent>,
    state: WorkflowState,
    file_len: u64,
}

impl DurableWorkflow {
    /// Opens or creates one append-only workflow journal.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed bindings, truncated/hash-invalid
    /// journals, a binding mismatch, or a durable write failure.
    pub fn open_or_create(
        path: impl AsRef<Path>,
        binding: &DecisionBinding,
    ) -> Result<Self, WorkflowError> {
        binding.validate()?;
        let path = path.as_ref().to_path_buf();
        let records = load_records(&path)?;
        if records.is_empty() {
            let workflow_id = workflow_id_for(binding)?;
            let initial = WorkflowEvent {
                event_id: event_id_for_decision(&workflow_id),
                at: binding.decided_at,
                transition: WorkflowTransition::DecisionRecorded {
                    workflow_id,
                    binding: binding.clone(),
                },
            };
            let state = WorkflowState::replay(std::slice::from_ref(&initial))?;
            let mut workflow = Self {
                path,
                records: Vec::new(),
                events_by_id: BTreeMap::new(),
                state,
                file_len: 0,
            };
            workflow.append(initial)?;
            Ok(workflow)
        } else {
            let events = records
                .iter()
                .map(|record| record.event.clone())
                .collect::<Vec<_>>();
            let state = WorkflowState::replay(&events)?;
            if state.binding != *binding {
                return Err(WorkflowError::BindingMismatch);
            }
            let mut events_by_id = BTreeMap::new();
            for event in events {
                if events_by_id.insert(event.event_id.clone(), event).is_some() {
                    return Err(WorkflowError::CorruptJournal(
                        "duplicate event ID in journal".into(),
                    ));
                }
            }
            let file_len = fs::metadata(&path).map_err(WorkflowError::io)?.len();
            Ok(Self {
                path,
                records,
                events_by_id,
                state,
                file_len,
            })
        }
    }

    #[must_use]
    pub const fn state(&self) -> &WorkflowState {
        &self.state
    }

    #[must_use]
    pub fn record_count(&self) -> usize {
        self.records.len()
    }

    /// Persists the order intent before returning it as externally actionable.
    ///
    /// On restart, the same call returns reconciliation-only rather than a
    /// second actionable request.
    ///
    /// # Errors
    ///
    /// Returns an error when the stage is invalid or the intent cannot be
    /// durably appended.
    pub fn prepare_order(&mut self, at: DateTime<Utc>) -> Result<PrepareOutcome, WorkflowError> {
        self.prepare_action(ActionKind::SubmitOrder, at)
    }

    /// Records the reconciled exchange order identity.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid response, stage, replay, or write.
    pub fn observe_order_submission(
        &mut self,
        exchange_order_id: impl Into<String>,
        at: DateTime<Utc>,
    ) -> Result<AppendOutcome, WorkflowError> {
        let action_id = action_id_for(self.state.workflow_id(), ActionKind::SubmitOrder);
        self.append_observation(
            stable_id(
                "event/order_submission/v1",
                &[self.state.workflow_id(), &action_id],
            ),
            at,
            WorkflowTransition::OrderSubmissionObserved {
                action_id,
                exchange_order_id: exchange_order_id.into(),
            },
        )
    }

    /// Records that authoritative post-expiry CLOID reconciliation found no
    /// accepted order, releasing the prepared intent as a zero-fill terminal
    /// outcome without allowing another submission.
    ///
    /// # Errors
    ///
    /// Returns an error for empty evidence, an invalid stage, replay conflict,
    /// or write failure.
    pub fn record_order_submission_absent(
        &mut self,
        observation_id: impl Into<String>,
        at: DateTime<Utc>,
    ) -> Result<AppendOutcome, WorkflowError> {
        let action_id = action_id_for(self.state.workflow_id(), ActionKind::SubmitOrder);
        let observation_id = observation_id.into();
        self.append_observation(
            stable_id(
                "event/order_submission_absent/v1",
                &[self.state.workflow_id(), &action_id, &observation_id],
            ),
            at,
            WorkflowTransition::OrderSubmissionAbsent {
                action_id,
                observation_id,
            },
        )
    }

    /// Records cumulative fill and authoritative debit observations.
    ///
    /// # Errors
    ///
    /// Returns an error when cumulative values regress, exceed their immutable
    /// caps, conflict with replay, or cannot be persisted.
    pub fn observe_order_fill(
        &mut self,
        observation_id: impl Into<String>,
        cumulative_hype: HypeAtoms,
        cumulative_filled_usdc: UsdcMicros,
        cumulative_debited_usdc: UsdcMicros,
        fully_filled: bool,
        at: DateTime<Utc>,
    ) -> Result<AppendOutcome, WorkflowError> {
        let observation_id = observation_id.into();
        self.append_observation(
            stable_id(
                "event/order_fill/v1",
                &[self.state.workflow_id(), &observation_id],
            ),
            at,
            WorkflowTransition::OrderFillObserved {
                observation_id,
                cumulative_hype,
                cumulative_filled_usdc,
                cumulative_debited_usdc,
                fully_filled,
            },
        )
    }

    /// Finalizes the order from authoritative cumulative reconciliation.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid cumulative values, stage, replay, or write.
    pub fn finalize_order(
        &mut self,
        cumulative_hype: HypeAtoms,
        cumulative_filled_usdc: UsdcMicros,
        cumulative_debited_usdc: UsdcMicros,
        finality: OrderFinality,
        at: DateTime<Utc>,
    ) -> Result<AppendOutcome, WorkflowError> {
        let action_id = action_id_for(self.state.workflow_id(), ActionKind::SubmitOrder);
        self.append_observation(
            stable_id(
                "event/order_finalized/v1",
                &[self.state.workflow_id(), &action_id],
            ),
            at,
            WorkflowTransition::OrderFinalized {
                action_id,
                cumulative_hype,
                cumulative_filled_usdc,
                cumulative_debited_usdc,
                finality,
            },
        )
    }

    /// Records the unsigned residual/eligible split without creating an action.
    ///
    /// Automatic staking remains unavailable under the current custody policy;
    /// an eligible amount is audit information only and stays in spot.
    ///
    /// # Errors
    ///
    /// Returns an error unless the order is terminal or the audit event cannot
    /// be persisted.
    pub fn record_staking_eligibility(
        &mut self,
        at: DateTime<Utc>,
    ) -> Result<StakingEligibility, WorkflowError> {
        if self.state.stage == WorkflowStage::StakingEligibilityRecorded {
            return Ok(self.state.staking_eligibility());
        }
        if self.state.stage != WorkflowStage::OrderFinalized {
            return Err(WorkflowError::InvalidTransition(
                "staking eligibility requires a terminal order".into(),
            ));
        }
        let residual_hype = self
            .state
            .purchased_hype
            .min(self.state.binding.inventory_before.residual_hype_deficit());
        let eligible_hype = self
            .state
            .purchased_hype
            .checked_sub(residual_hype)
            .ok_or_else(|| WorkflowError::CorruptJournal("HYPE split underflowed".into()))?;
        let eligibility = StakingEligibility {
            residual_hype,
            eligible_hype,
        };
        self.append_transition(
            stable_id(
                "event/staking_eligibility/v1",
                &[self.state.workflow_id(), "disabled"],
            ),
            at,
            WorkflowTransition::StakingEligibilityRecorded {
                residual_hype,
                eligible_hype,
            },
        )?;
        Ok(eligibility)
    }

    /// Rejects automatic staking under the mandatory disabled policy.
    ///
    /// # Errors
    ///
    /// Always returns [`WorkflowError::AutomaticStakingDisabled`].
    pub fn prepare_staking_deposit(
        &mut self,
        _at: DateTime<Utc>,
    ) -> Result<PrepareOutcome, WorkflowError> {
        Err(WorkflowError::AutomaticStakingDisabled)
    }

    /// Records the staking submission response, including ambiguity.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid response, stage, replay, or write.
    pub fn observe_staking_deposit(
        &mut self,
        receipt: ExternalReceipt,
        at: DateTime<Utc>,
    ) -> Result<AppendOutcome, WorkflowError> {
        let action_id = action_id_for(self.state.workflow_id(), ActionKind::DepositToStaking);
        self.append_observation(
            stable_id(
                "event/staking_submitted/v1",
                &[self.state.workflow_id(), &action_id],
            ),
            at,
            WorkflowTransition::StakingDepositObserved { action_id, receipt },
        )
    }

    /// Confirms the decision-attributed staking balance after submission.
    ///
    /// # Errors
    ///
    /// Returns an error for stale or contradictory evidence, invalid stage,
    /// replay conflict, or write failure.
    pub fn confirm_staking_balance(
        &mut self,
        observation_id: impl Into<String>,
        attributable_hype: HypeAtoms,
        observed_at: DateTime<Utc>,
    ) -> Result<AppendOutcome, WorkflowError> {
        let submitted_at = self.state.staking_submitted_at.ok_or_else(|| {
            WorkflowError::InvalidTransition("staking action is not submitted".into())
        })?;
        if observed_at < submitted_at {
            return Err(WorkflowError::StaleObservation);
        }
        if attributable_hype != self.state.staking_target_hype {
            let reason = "staking balance contradicted the decision-attributed amount".to_owned();
            self.mark_manual_review(reason.clone(), observed_at)?;
            return Err(WorkflowError::ContradictoryObservation(reason));
        }
        let observation_id = observation_id.into();
        self.append_observation(
            stable_id(
                "event/staking_balance/v1",
                &[self.state.workflow_id(), &observation_id],
            ),
            observed_at,
            WorkflowTransition::StakingBalanceConfirmed {
                observation_id,
                attributable_hype,
            },
        )
    }

    /// Rejects automatic delegation under the mandatory disabled policy.
    ///
    /// # Errors
    ///
    /// Always returns [`WorkflowError::AutomaticStakingDisabled`].
    pub fn prepare_delegation(
        &mut self,
        _at: DateTime<Utc>,
    ) -> Result<PrepareOutcome, WorkflowError> {
        Err(WorkflowError::AutomaticStakingDisabled)
    }

    /// Records the delegation submission response, including ambiguity.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid response, stage, replay, or write.
    pub fn observe_delegation(
        &mut self,
        receipt: ExternalReceipt,
        at: DateTime<Utc>,
    ) -> Result<AppendOutcome, WorkflowError> {
        let action_id = action_id_for(self.state.workflow_id(), ActionKind::Delegate);
        self.append_observation(
            stable_id(
                "event/delegation_submitted/v1",
                &[self.state.workflow_id(), &action_id],
            ),
            at,
            WorkflowTransition::DelegationObserved { action_id, receipt },
        )
    }

    /// Confirms the exact decision-attributed delegated balance.
    ///
    /// # Errors
    ///
    /// Returns an error for stale or contradictory evidence, invalid stage,
    /// replay conflict, or write failure.
    pub fn confirm_delegated_balance(
        &mut self,
        observation_id: impl Into<String>,
        attributable_hype: HypeAtoms,
        observed_at: DateTime<Utc>,
    ) -> Result<AppendOutcome, WorkflowError> {
        let submitted_at = self.state.delegation_submitted_at.ok_or_else(|| {
            WorkflowError::InvalidTransition("delegation action is not submitted".into())
        })?;
        if observed_at < submitted_at {
            return Err(WorkflowError::StaleObservation);
        }
        if attributable_hype != self.state.staking_target_hype {
            let reason = "delegated balance contradicted the decision-attributed amount".to_owned();
            self.mark_manual_review(reason.clone(), observed_at)?;
            return Err(WorkflowError::ContradictoryObservation(reason));
        }
        let observation_id = observation_id.into();
        self.append_observation(
            stable_id(
                "event/delegated_balance/v1",
                &[self.state.workflow_id(), &observation_id],
            ),
            observed_at,
            WorkflowTransition::DelegatedBalanceConfirmed {
                observation_id,
                attributable_hype,
            },
        )
    }

    /// Marks a fully reconciled workflow complete.
    ///
    /// # Errors
    ///
    /// Returns an error unless exact delegation is confirmed or the event
    /// cannot be persisted.
    pub fn complete(&mut self, at: DateTime<Utc>) -> Result<AppendOutcome, WorkflowError> {
        self.append_transition(
            stable_id("event/complete/v1", &[self.state.workflow_id(), "complete"]),
            at,
            WorkflowTransition::Completed,
        )
    }

    /// Stops automation in a durable manual-review state.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty reason, replay conflict, or write failure.
    pub fn mark_manual_review(
        &mut self,
        reason: impl Into<String>,
        at: DateTime<Utc>,
    ) -> Result<AppendOutcome, WorkflowError> {
        let reason = reason.into();
        self.append_transition(
            stable_id(
                "event/manual_review/v1",
                &[self.state.workflow_id(), reason.trim()],
            ),
            at,
            WorkflowTransition::ManualReview { reason },
        )
    }

    fn prepare_action(
        &mut self,
        kind: ActionKind,
        at: DateTime<Utc>,
    ) -> Result<PrepareOutcome, WorkflowError> {
        if let Some(action) = self.state.pending_action.as_ref() {
            if action.kind() == kind {
                return Ok(PrepareOutcome::ReconcileOnly {
                    action_id: action.action_id().to_owned(),
                    kind,
                });
            }
            return Err(WorkflowError::InvalidTransition(
                "a different action is pending reconciliation".into(),
            ));
        }
        let action = external_action_for(&self.state, kind)?;
        let event_id = stable_id(
            "event/action_prepared/v1",
            &[self.state.workflow_id(), action.action_id()],
        );
        self.append_transition(
            event_id,
            at,
            WorkflowTransition::ActionPrepared {
                action: action.clone(),
            },
        )?;
        Ok(PrepareOutcome::Ready(action))
    }

    fn append_transition(
        &mut self,
        event_id: String,
        at: DateTime<Utc>,
        transition: WorkflowTransition,
    ) -> Result<AppendOutcome, WorkflowError> {
        self.append(WorkflowEvent {
            event_id,
            at,
            transition,
        })
    }

    fn append_observation(
        &mut self,
        event_id: String,
        at: DateTime<Utc>,
        transition: WorkflowTransition,
    ) -> Result<AppendOutcome, WorkflowError> {
        let result = self.append_transition(event_id, at, transition);
        let reason = match &result {
            Err(WorkflowError::ContradictoryObservation(reason)) => Some(reason.clone()),
            Err(WorkflowError::EventCollision(event_id)) => {
                Some(format!("conflicting replay for event {event_id}"))
            }
            _ => None,
        };
        if let Some(reason) = reason {
            if self.state.stage != WorkflowStage::ManualReview {
                let detected_at = at.max(self.state.last_transition_at);
                self.mark_manual_review(reason, detected_at)?;
            }
        }
        result
    }

    fn append(&mut self, event: WorkflowEvent) -> Result<AppendOutcome, WorkflowError> {
        if let Some(existing) = self.events_by_id.get(&event.event_id) {
            return if existing.transition == event.transition {
                Ok(AppendOutcome::Duplicate)
            } else {
                Err(WorkflowError::EventCollision(event.event_id))
            };
        }
        let mut events = self
            .records
            .iter()
            .map(|record| record.event.clone())
            .collect::<Vec<_>>();
        events.push(event.clone());
        let next_state = WorkflowState::replay(&events)?;
        let sequence = u64::try_from(self.records.len())
            .map_err(|_| WorkflowError::CorruptJournal("sequence overflowed".into()))?;
        let previous_hash = self
            .records
            .last()
            .map_or_else(String::new, |record| record.record_hash.clone());
        let record_hash = record_hash(sequence, &previous_hash, &event)?;
        let record = JournalRecord {
            schema_version: JOURNAL_SCHEMA_VERSION,
            sequence,
            previous_hash,
            event,
            record_hash,
        };
        self.write_record(&record)?;
        self.events_by_id
            .insert(record.event.event_id.clone(), record.event.clone());
        self.records.push(record);
        self.state = next_state;
        Ok(AppendOutcome::Appended)
    }

    fn write_record(&mut self, record: &JournalRecord) -> Result<(), WorkflowError> {
        let parent = normalized_parent(&self.path);
        fs::create_dir_all(parent).map_err(WorkflowError::io)?;
        let created = !self.path.exists();
        let current_len = fs::metadata(&self.path)
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        if current_len != self.file_len {
            return Err(WorkflowError::ConcurrentModification);
        }
        let mut line = serde_json::to_vec(record).map_err(WorkflowError::json)?;
        line.push(b'\n');
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(WorkflowError::io)?;
        file.write_all(&line).map_err(WorkflowError::io)?;
        file.sync_all().map_err(WorkflowError::io)?;
        if created {
            File::open(parent)
                .and_then(|directory| directory.sync_all())
                .map_err(WorkflowError::io)?;
        }
        self.file_len = self
            .file_len
            .checked_add(
                u64::try_from(line.len())
                    .map_err(|_| WorkflowError::CorruptJournal("file length overflowed".into()))?,
            )
            .ok_or_else(|| WorkflowError::CorruptJournal("file length overflowed".into()))?;
        Ok(())
    }
}

fn external_action_for(
    state: &WorkflowState,
    kind: ActionKind,
) -> Result<ExternalAction, WorkflowError> {
    let action_id = action_id_for(state.workflow_id(), kind);
    match kind {
        ActionKind::SubmitOrder if state.stage == WorkflowStage::Decided => {
            Ok(ExternalAction::SubmitOrder {
                action_id,
                client_order_id: client_order_id_for(state.workflow_id()),
                notional_usdc: state.binding.planned_usdc,
                max_debit_usdc: state.binding.committed_usdc,
            })
        }
        ActionKind::DepositToStaking if state.stage == WorkflowStage::OrderFinalized => {
            let amount_hype = state
                .purchased_hype
                .checked_sub(
                    state
                        .binding
                        .inventory_before
                        .configured_residual_hype_atoms,
                )
                .filter(|amount| !amount.is_zero())
                .ok_or_else(|| {
                    WorkflowError::InvalidTransition(
                        "reconciled HYPE does not exceed the residual buffer".into(),
                    )
                })?;
            Ok(ExternalAction::DepositToStaking {
                action_id,
                amount_hype,
            })
        }
        ActionKind::Delegate if state.stage == WorkflowStage::StakingBalanceConfirmed => {
            Ok(ExternalAction::Delegate {
                action_id,
                amount_hype: state.staking_target_hype,
            })
        }
        _ => Err(WorkflowError::InvalidTransition(
            "external action is invalid for current stage".into(),
        )),
    }
}

fn validate_receipt(receipt: &ExternalReceipt) -> Result<(), WorkflowError> {
    if matches!(receipt, ExternalReceipt::Confirmed(reference) if reference.trim().is_empty()) {
        return Err(WorkflowError::ContradictoryObservation(
            "confirmed receipt has an empty reference".into(),
        ));
    }
    Ok(())
}

fn workflow_id_for(binding: &DecisionBinding) -> Result<String, WorkflowError> {
    let encoded = serde_json::to_vec(binding).map_err(WorkflowError::json)?;
    Ok(format!("wf_{}", digest_hex(&encoded)))
}

fn action_id_for(workflow_id: &str, kind: ActionKind) -> String {
    let kind = match kind {
        ActionKind::SubmitOrder => "submit_order",
        ActionKind::DepositToStaking => "deposit_to_staking",
        ActionKind::Delegate => "delegate",
    };
    stable_id("action/v1", &[workflow_id, kind])
}

fn client_order_id_for(workflow_id: &str) -> String {
    let digest = stable_id("cloid/v1", &[workflow_id]);
    format!("0x{}", &digest[..32])
}

fn event_id_for_decision(workflow_id: &str) -> String {
    stable_id("event/decision/v1", &[workflow_id])
}

fn validate_event_id(workflow_id: &str, event: &WorkflowEvent) -> Result<(), WorkflowError> {
    let expected = match &event.transition {
        WorkflowTransition::DecisionRecorded { workflow_id, .. } => {
            event_id_for_decision(workflow_id)
        }
        WorkflowTransition::ActionPrepared { action } => stable_id(
            "event/action_prepared/v1",
            &[workflow_id, action.action_id()],
        ),
        WorkflowTransition::OrderSubmissionObserved { action_id, .. } => {
            stable_id("event/order_submission/v1", &[workflow_id, action_id])
        }
        WorkflowTransition::OrderSubmissionAbsent {
            action_id,
            observation_id,
        } => stable_id(
            "event/order_submission_absent/v1",
            &[workflow_id, action_id, observation_id],
        ),
        WorkflowTransition::OrderFillObserved { observation_id, .. } => {
            stable_id("event/order_fill/v1", &[workflow_id, observation_id])
        }
        WorkflowTransition::OrderFinalized { action_id, .. } => {
            stable_id("event/order_finalized/v1", &[workflow_id, action_id])
        }
        WorkflowTransition::StakingEligibilityRecorded { .. } => {
            stable_id("event/staking_eligibility/v1", &[workflow_id, "disabled"])
        }
        WorkflowTransition::StakingDepositObserved { action_id, .. } => {
            stable_id("event/staking_submitted/v1", &[workflow_id, action_id])
        }
        WorkflowTransition::StakingBalanceConfirmed { observation_id, .. } => {
            stable_id("event/staking_balance/v1", &[workflow_id, observation_id])
        }
        WorkflowTransition::DelegationObserved { action_id, .. } => {
            stable_id("event/delegation_submitted/v1", &[workflow_id, action_id])
        }
        WorkflowTransition::DelegatedBalanceConfirmed { observation_id, .. } => {
            stable_id("event/delegated_balance/v1", &[workflow_id, observation_id])
        }
        WorkflowTransition::Completed => stable_id("event/complete/v1", &[workflow_id, "complete"]),
        WorkflowTransition::ManualReview { reason } => {
            stable_id("event/manual_review/v1", &[workflow_id, reason.trim()])
        }
    };
    if event.event_id == expected {
        Ok(())
    } else {
        Err(WorkflowError::CorruptJournal(
            "event ID is not deterministic".into(),
        ))
    }
}

fn stable_id(domain: &str, parts: &[&str]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(domain.as_bytes());
    for part in parts {
        hasher.update([0]);
        hasher.update(part.as_bytes());
    }
    digest_to_hex(&hasher.finalize())
}

fn record_hash(
    sequence: u64,
    previous_hash: &str,
    event: &WorkflowEvent,
) -> Result<String, WorkflowError> {
    let encoded = serde_json::to_vec(&RecordHashInput {
        schema_version: JOURNAL_SCHEMA_VERSION,
        sequence,
        previous_hash,
        event,
    })
    .map_err(WorkflowError::json)?;
    Ok(digest_hex(&encoded))
}

fn digest_hex(bytes: &[u8]) -> String {
    digest_to_hex(&Sha256::digest(bytes))
}

fn digest_to_hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

fn load_records(path: &Path) -> Result<Vec<JournalRecord>, WorkflowError> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let payload = fs::read(path).map_err(WorkflowError::io)?;
    if payload.is_empty() {
        return Ok(Vec::new());
    }
    if payload.last() != Some(&b'\n') {
        return Err(WorkflowError::TruncatedJournal);
    }
    let mut records = Vec::new();
    let mut expected_previous_hash = String::new();
    for line in payload
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let record: JournalRecord = serde_json::from_slice(line).map_err(WorkflowError::json)?;
        let expected_sequence = u64::try_from(records.len())
            .map_err(|_| WorkflowError::CorruptJournal("sequence overflowed".into()))?;
        if record.schema_version != JOURNAL_SCHEMA_VERSION
            || record.sequence != expected_sequence
            || record.previous_hash != expected_previous_hash
            || record.record_hash
                != record_hash(record.sequence, &record.previous_hash, &record.event)?
        {
            return Err(WorkflowError::CorruptJournal(
                "record sequence or hash chain is invalid".into(),
            ));
        }
        expected_previous_hash.clone_from(&record.record_hash);
        records.push(record);
    }
    Ok(records)
}

fn normalized_parent(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

#[derive(Debug, Error)]
pub enum WorkflowError {
    #[error("invalid decision binding: {0}")]
    InvalidBinding(String),
    #[error("journal belongs to a different decision binding")]
    BindingMismatch,
    #[error("workflow journal is empty")]
    EmptyJournal,
    #[error("workflow journal is truncated")]
    TruncatedJournal,
    #[error("workflow journal is corrupt: {0}")]
    CorruptJournal(String),
    #[error("event ID collision: {0}")]
    EventCollision(String),
    #[error("workflow transition is invalid: {0}")]
    InvalidTransition(String),
    #[error("observation is contradictory: {0}")]
    ContradictoryObservation(String),
    #[error("observation predates the corresponding external action")]
    StaleObservation,
    #[error("automatic staking and delegation are disabled by custody policy")]
    AutomaticStakingDisabled,
    #[error("journal changed since it was opened")]
    ConcurrentModification,
    #[error("journal I/O failed: {0}")]
    Io(String),
    #[error("journal serialization failed: {0}")]
    Json(String),
}

impl WorkflowError {
    #[allow(clippy::needless_pass_by_value)]
    fn io(error: io::Error) -> Self {
        Self::Io(error.to_string())
    }

    #[allow(clippy::needless_pass_by_value)]
    fn json(error: serde_json::Error) -> Self {
        Self::Json(error.to_string())
    }
}
