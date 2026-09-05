//! Crash-safe, signer-free orchestration for one daily HYPE allocation.
//!
//! The journal is single-writer and append-only. Its fsynced `<journal>.head`
//! checkpoint closes local crash windows, while a caller-supplied monotonic
//! anchor in an independent rollback domain detects restoration of both local
//! files to an otherwise valid historical prefix. Every external action is
//! durably prepared and anchored before it can be returned to a caller. After a
//! restart, a prepared action is reconciliation-only and is never returned as a
//! new submission. A separately shared append-only owner store prevents one
//! stable venue order identity from settling more than one decision workflow.

use crate::{
    fs_safety::{normal_absolute_path, reject_linked_file, reject_multiple_links},
    pacing::{DailyDecision, DecisionReason, UsdcMicros},
};
use chrono::{DateTime, NaiveDate, TimeDelta, Utc};
use fs2::FileExt as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};
use thiserror::Error;

const JOURNAL_SCHEMA_VERSION: u8 = 1;
const CHECKPOINT_SCHEMA_VERSION: u8 = 1;
const PROTECTED_HEAD_SCHEMA_VERSION: u8 = 1;
const EXCHANGE_ORDER_OWNER_SCHEMA_VERSION: u8 = 1;
const EXCHANGE_FILL_OWNER_SCHEMA_VERSION: u8 = 1;
const PENDING_APPEND_SCHEMA_VERSION: u8 = 1;
const EXCHANGE_ORDER_OWNER_CONFLICT_REASON: &str =
    "exchange order ID is already owned by another workflow";
const EXCHANGE_FILL_OWNER_CONFLICT_REASON: &str =
    "exchange fill ID is already owned by another workflow";

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
    /// Opaque digest of the master account, subaccount, or vault being traded.
    pub execution_identity_hash: String,
    pub spot_hype_atoms: HypeAtoms,
    pub staking_hype_atoms: HypeAtoms,
    pub delegated_hype_atoms: HypeAtoms,
    pub configured_residual_hype_atoms: HypeAtoms,
    /// Reconciled, unconsumed quantity currently assigned to `residual_spot`.
    pub unconsumed_residual_spot_hype_atoms: HypeAtoms,
}

impl InventoryBaseline {
    fn residual_hype_deficit(&self) -> HypeAtoms {
        self.configured_residual_hype_atoms
            .checked_sub(self.unconsumed_residual_spot_hype_atoms)
            .unwrap_or_default()
    }
}

/// Latest time through which every signer-side authorization input is valid.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AuthorizationInputFreshness {
    pub decision_valid_through_at: DateTime<Utc>,
    pub signal_evidence_valid_through_at: DateTime<Utc>,
    pub book_evidence_valid_through_at: DateTime<Utc>,
    pub account_evidence_valid_through_at: DateTime<Utc>,
    pub fee_schedule_valid_through_at: DateTime<Utc>,
    pub policy_acknowledgement_valid_through_at: DateTime<Utc>,
}

impl AuthorizationInputFreshness {
    /// Earliest of the six freshness horizons. `pub(crate)` so envelope
    /// assembly (`order_envelope.rs`) can compute the same effective-expiry
    /// cap this struct's own validation (`valid_expiry_binding`) enforces,
    /// without hand-duplicating the field list.
    pub(crate) fn earliest_deadline(&self) -> DateTime<Utc> {
        [
            self.decision_valid_through_at,
            self.signal_evidence_valid_through_at,
            self.book_evidence_valid_through_at,
            self.account_evidence_valid_through_at,
            self.fee_schedule_valid_through_at,
            self.policy_acknowledgement_valid_through_at,
        ]
        .into_iter()
        .min()
        .expect("authorization freshness has a fixed non-empty field set")
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EligibilityPolicyBinding {
    pub policy_version: String,
    pub fill_registration_deadline_seconds: u64,
    pub lot_eligibility_max_age_seconds: u64,
}

/// Immutable, signer-free capability used only by the offline staking
/// simulation feature. Production builds reject bindings that contain it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OfflineStakingCapabilityBinding {
    pub capability_version: String,
    pub execution_identity_hash: String,
    pub validator_address: String,
    pub validator_summary_evidence_hash: String,
    pub policy_version: String,
    pub policy_acknowledgement_hash: String,
}

const OFFLINE_STAKING_CAPABILITY_VERSION: &str = "offline-staking-simulation/v1";

/// Immutable fields of the signer-authorized IOC order envelope.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OrderEnvelopeBinding {
    /// Opaque digest of the dedicated API-wallet signer authorized for this order.
    pub signer_identity_hash: String,
    pub original_quantity_hype: HypeAtoms,
    pub hype_atoms_per_hype: u64,
    pub market_metadata_digest: String,
    pub limit_price_usdc_per_hype: UsdcMicros,
    pub l1_nonce: u64,
    pub signed_expiry_at: DateTime<Utc>,
    pub effective_expiry_at: DateTime<Utc>,
    pub venue_clock_evidence_at: DateTime<Utc>,
    pub venue_clock_evidence_valid_through_at: DateTime<Utc>,
    pub venue_clock_evidence_digest: String,
    pub max_venue_clock_lag_ms: u64,
    pub input_freshness: AuthorizationInputFreshness,
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
    pub order_envelope: OrderEnvelopeBinding,
    pub eligibility_policy: EligibilityPolicyBinding,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offline_staking_capability: Option<OfflineStakingCapabilityBinding>,
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
        order_envelope: OrderEnvelopeBinding,
        eligibility_policy: EligibilityPolicyBinding,
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
            order_envelope,
            eligibility_policy,
            offline_staking_capability: None,
        };
        binding.validate()?;
        Ok(binding)
    }

    /// Binds signer-free staking/delegation mechanics to an offline-only
    /// capability before the durable workflow identity is derived.
    ///
    /// # Errors
    ///
    /// Returns an error unless the capability is canonical, account-bound,
    /// policy-bound, and this crate was built with the explicit simulation
    /// feature.
    #[cfg(feature = "offline-staking-simulation")]
    pub fn with_offline_staking_capability(
        mut self,
        capability: OfflineStakingCapabilityBinding,
    ) -> Result<Self, WorkflowError> {
        self.offline_staking_capability = Some(capability);
        self.validate()?;
        Ok(self)
    }

    fn validate(&self) -> Result<(), WorkflowError> {
        let max_order_notional_usdc = max_order_notional_usdc(&self.order_envelope);
        if self.decision_id.trim().is_empty()
            || self.decision_id != self.decision_id.trim()
            || self.capital_snapshot_hash.is_empty()
            || self.input_snapshot_hash.is_empty()
            || self
                .inventory_before
                .execution_identity_hash
                .trim()
                .is_empty()
            || self.inventory_before.execution_identity_hash
                != self.inventory_before.execution_identity_hash.trim()
            || self.order_envelope.signer_identity_hash.trim().is_empty()
            || self.order_envelope.signer_identity_hash
                != self.order_envelope.signer_identity_hash.trim()
            || self.inventory_before.unconsumed_residual_spot_hype_atoms
                > self.inventory_before.spot_hype_atoms
            || self.inventory_before.unconsumed_residual_spot_hype_atoms
                > self.inventory_before.configured_residual_hype_atoms
            || self.planned_usdc.is_zero()
            || self.committed_usdc < self.planned_usdc
            || self.order_envelope.original_quantity_hype.is_zero()
            || self.order_envelope.hype_atoms_per_hype == 0
            || self.order_envelope.market_metadata_digest.trim().is_empty()
            || self.eligibility_policy.policy_version.trim().is_empty()
            || policy_time_delta(self.eligibility_policy.fill_registration_deadline_seconds)
                .is_none()
            || policy_time_delta(self.eligibility_policy.lot_eligibility_max_age_seconds).is_none()
            || self.order_envelope.limit_price_usdc_per_hype.is_zero()
            || max_order_notional_usdc.is_none_or(|notional| {
                notional > self.planned_usdc || notional > self.committed_usdc
            })
            || !valid_expiry_binding(&self.order_envelope, self.decided_at)
            || self.capital_commitments.is_empty()
            || self.decided_at.date_naive() != self.decision_date
        {
            return Err(WorkflowError::InvalidBinding(
                "decision identity, snapshots, date, and capital must be complete".into(),
            ));
        }
        if let Some(capability) = &self.offline_staking_capability {
            if !cfg!(feature = "offline-staking-simulation") {
                return Err(WorkflowError::InvalidBinding(
                    "offline staking capability is unavailable in this build".into(),
                ));
            }
            capability.validate(self)?;
        }
        let mut ids = BTreeSet::new();
        let mut planned_total = 0_u64;
        let mut committed_total = 0_u64;
        for commitment in &self.capital_commitments {
            if commitment.event_id.is_empty()
                || commitment.event_id != commitment.event_id.trim()
                || commitment.planned_usdc.is_zero()
                || commitment.committed_usdc < commitment.planned_usdc
                || !ids.insert(commitment.event_id.as_str())
            {
                return Err(WorkflowError::InvalidBinding(
                    "capital commitments must have unique canonical IDs and amounts".into(),
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

impl OfflineStakingCapabilityBinding {
    fn validate(&self, binding: &DecisionBinding) -> Result<(), WorkflowError> {
        if self.capability_version != OFFLINE_STAKING_CAPABILITY_VERSION
            || self.execution_identity_hash != binding.inventory_before.execution_identity_hash
            || self.policy_version != binding.eligibility_policy.policy_version
            || !canonical_ethereum_address(&self.validator_address)
            || !lower_hex_digest(&self.validator_summary_evidence_hash)
            || !lower_hex_digest(&self.policy_acknowledgement_hash)
        {
            return Err(WorkflowError::InvalidBinding(
                "offline staking capability is not canonical or account/policy bound".into(),
            ));
        }
        Ok(())
    }

    fn digest(&self) -> Result<String, WorkflowError> {
        serde_json::to_vec(self)
            .map(|encoded| digest_hex(&encoded))
            .map_err(WorkflowError::json)
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
        execution_identity_hash: String,
        signer_identity_hash: String,
        notional_usdc: UsdcMicros,
        max_debit_usdc: UsdcMicros,
        original_quantity_hype: HypeAtoms,
        hype_atoms_per_hype: u64,
        market_metadata_digest: String,
        limit_price_usdc_per_hype: UsdcMicros,
        l1_nonce: u64,
        signed_expiry_at: DateTime<Utc>,
    },
    DepositToStaking {
        action_id: String,
        eligibility_workflow_id: String,
        capability_binding_hash: String,
        amount_hype: HypeAtoms,
    },
    Delegate {
        action_id: String,
        eligibility_workflow_id: String,
        capability_binding_hash: String,
        validator_address: String,
        validator_summary_evidence_hash: String,
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
    Confirmed { transaction_hash: String },
    Ambiguous,
}

impl ExternalReceipt {
    fn confirmed_transaction_hash(&self) -> Option<&str> {
        match self {
            Self::Confirmed { transaction_hash } => Some(transaction_hash),
            Self::Ambiguous => None,
        }
    }

    /// Returns the canonical digest used to bind later confirmation evidence.
    ///
    /// # Errors
    ///
    /// Returns [`WorkflowError`] if the receipt cannot be serialized.
    pub fn canonical_digest(&self) -> Result<String, WorkflowError> {
        serde_json::to_vec(self)
            .map(|encoded| digest_hex(&encoded))
            .map_err(WorkflowError::json)
    }
}

/// Authoritative, account-bound evidence that the exact simulated staking
/// deposit is reflected in staking state after submission.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StakingBalanceConfirmation {
    pub observation_id: String,
    pub action_id: String,
    pub execution_identity_hash: String,
    pub eligibility_workflow_id: String,
    pub submission_receipt_hash: String,
    pub baseline_history_hash: String,
    pub baseline_captured_at: DateTime<Utc>,
    pub current_history_hash: String,
    pub current_captured_at: DateTime<Utc>,
    pub matched_transaction_hash: String,
    pub attributable_hype: HypeAtoms,
}

/// Authoritative, validator-bound evidence that the exact simulated
/// delegation is reflected in account delegation state after submission.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DelegatedBalanceConfirmation {
    pub observation_id: String,
    pub action_id: String,
    pub execution_identity_hash: String,
    pub eligibility_workflow_id: String,
    pub validator_address: String,
    pub validator_summary_evidence_hash: String,
    pub submission_receipt_hash: String,
    pub baseline_history_hash: String,
    pub baseline_captured_at: DateTime<Utc>,
    pub current_history_hash: String,
    pub current_captured_at: DateTime<Utc>,
    pub matched_transaction_hash: String,
    pub attributable_hype: HypeAtoms,
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

/// One immutable fill included by the independent eligibility reconciler.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BoundFillEvidence {
    pub fill_id: String,
    pub authorization_id: String,
    pub authorization_record_hash: String,
    pub execution_identity_hash: String,
    pub client_order_id: String,
    pub order_id: String,
    pub purchased_hype: HypeAtoms,
    pub executed_notional_usdc: UsdcMicros,
    pub executed_at: DateTime<Utc>,
    pub first_observed_at: DateTime<Utc>,
    pub registration_record_id: String,
    pub registration_record_hash: String,
    pub registration_cursor: u64,
    pub registered_at: DateTime<Utc>,
    pub registration_deadline_at: DateTime<Utc>,
}

/// One authoritative movement consuming quantity attributed to this workflow.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BoundMovementEvidence {
    pub movement_id: String,
    pub consumed_hype: HypeAtoms,
    pub occurred_at: DateTime<Utc>,
}

/// Signer-authorized terminal evidence for an accepted order.
///
/// This structure is deliberately complete and content-addressed when
/// eligibility is recorded. It does not contain signing material.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct OrderBoundEligibilityEvidence {
    pub authorization_id: String,
    pub authorization_record_hash: String,
    pub decision_id: String,
    pub execution_identity_hash: String,
    pub signer_identity_hash: String,
    pub client_order_id: String,
    pub canonical_order_envelope_hash: String,
    pub authorized_planned_usdc: UsdcMicros,
    pub authorized_max_debit_usdc: UsdcMicros,
    pub original_quantity_hype: HypeAtoms,
    pub hype_atoms_per_hype: u64,
    pub market_metadata_digest: String,
    pub limit_price_usdc_per_hype: UsdcMicros,
    pub l1_nonce: u64,
    pub signed_expiry_at: DateTime<Utc>,
    pub authorized_at: DateTime<Utc>,
    pub order_id: String,
    pub accepted_at: DateTime<Utc>,
    pub order_bound_at: DateTime<Utc>,
    pub effective_expiry_at: DateTime<Utc>,
    pub residual_reservation_hype: HypeAtoms,
    pub policy_version: String,
    pub fill_history: GapFreeHistoryWatermark,
    pub movement_history: GapFreeHistoryWatermark,
    pub movements: Vec<BoundMovementEvidence>,
    pub fills: Vec<BoundFillEvidence>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryDomain {
    Order,
    Fill,
    Movement,
}

/// Gap-free authoritative history coverage used for conclusive absence.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GapFreeHistoryWatermark {
    pub domain: HistoryDomain,
    pub watermark_id: String,
    pub cursor: u64,
    pub gap_free_from_at: DateTime<Utc>,
    pub through_at: DateTime<Utc>,
    pub evidence_hash: String,
}

/// Evidence that an ambiguous claimed order can no longer appear or fill.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ConclusiveAbsenceEvidence {
    pub observation_id: String,
    pub execution_identity_hash: String,
    pub client_order_id: String,
    pub effective_expiry_at: DateTime<Utc>,
    pub order_history: GapFreeHistoryWatermark,
    pub fill_history: GapFreeHistoryWatermark,
}

/// Authenticated, account-scoped venue evidence for one accepted IOC order.
///
/// The caller must populate this from an exact-CLOID lookup on the authorized
/// execution account. The provenance hashes identify the authenticated account
/// snapshot and full venue envelope returned by that lookup.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AuthenticatedOrderSubmission {
    pub observation_id: String,
    pub account_scope_evidence_hash: String,
    pub order_envelope_evidence_hash: String,
    pub execution_identity_hash: String,
    pub signer_identity_hash: String,
    pub decision_id: String,
    pub client_order_id: String,
    pub exchange_order_id: String,
    pub canonical_order_envelope_hash: String,
    pub planned_usdc: UsdcMicros,
    pub max_debit_usdc: UsdcMicros,
    pub original_quantity_hype: HypeAtoms,
    pub hype_atoms_per_hype: u64,
    pub market_metadata_digest: String,
    pub limit_price_usdc_per_hype: UsdcMicros,
    pub l1_nonce: u64,
    pub signed_expiry_at: DateTime<Utc>,
    pub effective_expiry_at: DateTime<Utc>,
    pub market: String,
    pub side: String,
    pub time_in_force: String,
    pub accepted_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WorkflowTransition {
    DecisionRecorded {
        workflow_id: String,
        binding: Box<DecisionBinding>,
    },
    ActionPrepared {
        action: ExternalAction,
    },
    OrderSubmissionObserved {
        action_id: String,
        evidence: Box<AuthenticatedOrderSubmission>,
    },
    OrderSubmissionAbsent {
        action_id: String,
        evidence: ConclusiveAbsenceEvidence,
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
        eligibility_workflow_id: String,
        evidence: Option<Box<OrderBoundEligibilityEvidence>>,
        residual_hype: HypeAtoms,
        eligible_hype: HypeAtoms,
    },
    StakingDepositObserved {
        action_id: String,
        receipt: ExternalReceipt,
    },
    StakingBalanceConfirmed {
        evidence: Box<StakingBalanceConfirmation>,
    },
    DelegationObserved {
        action_id: String,
        receipt: ExternalReceipt,
    },
    DelegatedBalanceConfirmed {
        evidence: Box<DelegatedBalanceConfirmation>,
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
    order_prepared_at: Option<DateTime<Utc>>,
    exchange_order_id: Option<String>,
    order_accepted_at: Option<DateTime<Utc>>,
    purchased_hype: HypeAtoms,
    filled_usdc: UsdcMicros,
    debited_usdc: UsdcMicros,
    residual_hype: HypeAtoms,
    staking_eligible_hype: HypeAtoms,
    eligibility_workflow_id: Option<String>,
    staking_target_hype: HypeAtoms,
    #[serde(default)]
    staking_confirmed_hype: HypeAtoms,
    delegated_hype: HypeAtoms,
    #[serde(default)]
    staking_prepared_at: Option<DateTime<Utc>>,
    staking_submitted_at: Option<DateTime<Utc>>,
    #[serde(default)]
    staking_submission_receipt_hash: Option<String>,
    #[serde(default)]
    staking_confirmed_transaction_hash: Option<String>,
    #[serde(default)]
    delegation_prepared_at: Option<DateTime<Utc>>,
    delegation_submitted_at: Option<DateTime<Utc>>,
    #[serde(default)]
    delegation_submission_receipt_hash: Option<String>,
    #[serde(default)]
    delegation_confirmed_transaction_hash: Option<String>,
    #[serde(default)]
    last_fill_at: Option<DateTime<Utc>>,
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
    pub fn client_order_id(&self) -> String {
        client_order_id_for(&self.workflow_id)
    }

    #[must_use]
    pub fn exchange_order_id(&self) -> Option<&str> {
        self.exchange_order_id.as_deref()
    }

    #[must_use]
    pub const fn order_accepted_at(&self) -> Option<DateTime<Utc>> {
        self.order_accepted_at
    }

    /// Returns the signer-side canonical IOC order envelope digest.
    ///
    /// # Errors
    ///
    /// Returns an error only if the immutable envelope cannot be serialized.
    pub fn canonical_order_envelope_hash(&self) -> Result<String, WorkflowError> {
        canonical_order_envelope_hash(self)
    }

    #[must_use]
    pub fn eligibility_workflow_id(&self) -> Option<&str> {
        self.eligibility_workflow_id.as_deref()
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
    pub const fn staking_confirmed_hype(&self) -> HypeAtoms {
        self.staking_confirmed_hype
    }

    #[must_use]
    pub const fn delegated_hype(&self) -> HypeAtoms {
        self.delegated_hype
    }

    #[must_use]
    pub const fn last_fill_at(&self) -> Option<DateTime<Utc>> {
        self.last_fill_at
    }

    #[must_use]
    pub const fn last_transition_at(&self) -> DateTime<Utc> {
        self.last_transition_at
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
        let binding = binding.as_ref();
        binding.validate()?;
        if first.event_id != event_id_for_decision(workflow_id) {
            return Err(WorkflowError::CorruptJournal(
                "decision event ID is not deterministic".into(),
            ));
        }
        let expected_workflow_id = workflow_id_for(binding);
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
            order_prepared_at: None,
            exchange_order_id: None,
            order_accepted_at: None,
            purchased_hype: HypeAtoms::default(),
            filled_usdc: UsdcMicros::from_micros(0),
            debited_usdc: UsdcMicros::from_micros(0),
            residual_hype: HypeAtoms::default(),
            staking_eligible_hype: HypeAtoms::default(),
            eligibility_workflow_id: None,
            staking_target_hype: HypeAtoms::default(),
            staking_confirmed_hype: HypeAtoms::default(),
            delegated_hype: HypeAtoms::default(),
            staking_prepared_at: None,
            staking_submitted_at: None,
            staking_submission_receipt_hash: None,
            staking_confirmed_transaction_hash: None,
            delegation_prepared_at: None,
            delegation_submitted_at: None,
            delegation_submission_receipt_hash: None,
            delegation_confirmed_transaction_hash: None,
            last_fill_at: None,
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
                    ExternalAction::SubmitOrder { .. } => {
                        self.order_prepared_at = Some(event.at);
                    }
                    ExternalAction::DepositToStaking { amount_hype, .. } => {
                        self.staking_prepared_at = Some(event.at);
                        self.staking_target_hype = *amount_hype;
                    }
                    ExternalAction::Delegate { .. } => {
                        self.delegation_prepared_at = Some(event.at);
                    }
                }
            }
            WorkflowTransition::OrderSubmissionObserved {
                action_id,
                evidence,
            } => {
                if self.order_is_terminal() {
                    return Err(WorkflowError::ContradictoryObservation(
                        "accepted order appeared after terminal reconciliation".into(),
                    ));
                }
                self.require_pending(ActionKind::SubmitOrder, action_id)?;
                if self.stage != WorkflowStage::Decided {
                    return Err(WorkflowError::InvalidTransition(
                        "order submission response is invalid for current state".into(),
                    ));
                }
                self.validate_order_submission_evidence(evidence, event.at)?;
                self.exchange_order_id = Some(evidence.exchange_order_id.clone());
                self.order_accepted_at = Some(evidence.accepted_at);
                self.pending_action = None;
                self.stage = WorkflowStage::OrderSubmitted;
            }
            WorkflowTransition::OrderSubmissionAbsent {
                action_id,
                evidence,
            } => {
                if self.exchange_order_id.is_some() {
                    return Err(WorkflowError::ContradictoryObservation(
                        "order absence contradicted an accepted order".into(),
                    ));
                }
                self.require_pending(ActionKind::SubmitOrder, action_id)?;
                if self.stage != WorkflowStage::Decided {
                    return Err(WorkflowError::InvalidTransition(
                        "absent order submission evidence is invalid for current state".into(),
                    ));
                }
                self.validate_conclusive_absence(evidence, event.at)?;
                self.pending_action = None;
                self.stage = WorkflowStage::OrderFinalized;
            }
            WorkflowTransition::OrderFillObserved {
                observation_id,
                cumulative_hype,
                cumulative_filled_usdc,
                cumulative_debited_usdc,
                fully_filled,
            } => {
                if observation_id.trim().is_empty() {
                    return Err(WorkflowError::InvalidTransition(
                        "fill observation ID is empty".into(),
                    ));
                }
                if self.order_is_terminal() {
                    return Err(WorkflowError::ContradictoryObservation(
                        "new fill appeared after terminal reconciliation".into(),
                    ));
                }
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
                self.validate_observed_fill(
                    *cumulative_hype,
                    *cumulative_filled_usdc,
                    *cumulative_debited_usdc,
                    *fully_filled,
                )?;
                if self.stage == WorkflowStage::Filled && !fully_filled {
                    return Err(WorkflowError::ContradictoryObservation(
                        "a fully filled order regressed to partial".into(),
                    ));
                }
                let new_fill_observed = *cumulative_hype > self.purchased_hype;
                self.purchased_hype = *cumulative_hype;
                self.filled_usdc = *cumulative_filled_usdc;
                self.debited_usdc = *cumulative_debited_usdc;
                if new_fill_observed {
                    self.last_fill_at = Some(event.at);
                }
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
                let new_fill_observed = *cumulative_hype > self.purchased_hype;
                self.validate_order_finalization(
                    *cumulative_hype,
                    *cumulative_filled_usdc,
                    *cumulative_debited_usdc,
                    *finality,
                )?;
                self.purchased_hype = *cumulative_hype;
                self.filled_usdc = *cumulative_filled_usdc;
                self.debited_usdc = *cumulative_debited_usdc;
                if new_fill_observed {
                    self.last_fill_at = Some(event.at);
                }
                self.stage = WorkflowStage::OrderFinalized;
            }
            WorkflowTransition::StakingEligibilityRecorded {
                eligibility_workflow_id,
                evidence,
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
                self.validate_eligibility_evidence(evidence.as_deref(), event.at)?;
                let expected_workflow_id = eligibility_workflow_id_for(
                    &self.workflow_id,
                    evidence.as_deref(),
                    *residual_hype,
                    *eligible_hype,
                )?;
                if eligibility_workflow_id != &expected_workflow_id {
                    return Err(WorkflowError::ContradictoryObservation(
                        "eligibility workflow identity does not match its complete evidence".into(),
                    ));
                }
                self.residual_hype = *residual_hype;
                self.staking_eligible_hype = *eligible_hype;
                self.eligibility_workflow_id = Some(eligibility_workflow_id.clone());
                self.stage = WorkflowStage::StakingEligibilityRecorded;
            }
            WorkflowTransition::StakingDepositObserved { action_id, receipt } => {
                self.require_pending(ActionKind::DepositToStaking, action_id)?;
                if self.stage != WorkflowStage::StakingEligibilityRecorded {
                    return Err(WorkflowError::InvalidTransition(
                        "staking response requires recorded eligibility".into(),
                    ));
                }
                validate_receipt(receipt)?;
                self.pending_action = None;
                self.staking_submission_receipt_hash = Some(receipt.canonical_digest()?);
                self.staking_confirmed_transaction_hash =
                    receipt.confirmed_transaction_hash().map(str::to_owned);
                self.staking_submitted_at = Some(event.at);
                self.stage = WorkflowStage::StakingDepositSubmitted;
            }
            WorkflowTransition::StakingBalanceConfirmed { evidence } => {
                if self.stage != WorkflowStage::StakingDepositSubmitted {
                    return Err(WorkflowError::InvalidTransition(
                        "staking confirmation is invalid for current state".into(),
                    ));
                }
                self.validate_staking_balance_confirmation(evidence, event.at)?;
                self.staking_confirmed_hype = evidence.attributable_hype;
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
                self.delegation_submission_receipt_hash = Some(receipt.canonical_digest()?);
                self.delegation_confirmed_transaction_hash =
                    receipt.confirmed_transaction_hash().map(str::to_owned);
                self.delegation_submitted_at = Some(event.at);
                self.stage = WorkflowStage::DelegationSubmitted;
            }
            WorkflowTransition::DelegatedBalanceConfirmed { evidence } => {
                if self.stage != WorkflowStage::DelegationSubmitted {
                    return Err(WorkflowError::InvalidTransition(
                        "delegation confirmation is invalid for current state".into(),
                    ));
                }
                self.validate_delegated_balance_confirmation(evidence, event.at)?;
                self.delegated_hype = evidence.attributable_hype;
                self.stage = WorkflowStage::DelegatedConfirmed;
            }
            WorkflowTransition::Completed => {
                let eligibility_only_complete = self.binding.offline_staking_capability.is_none()
                    && self.stage == WorkflowStage::StakingEligibilityRecorded;
                let zero_staking_complete = self.staking_eligible_hype.is_zero()
                    && self.stage == WorkflowStage::StakingEligibilityRecorded;
                let simulated_staking_complete = self.binding.offline_staking_capability.is_some()
                    && self.stage == WorkflowStage::DelegatedConfirmed
                    && self.delegated_hype == self.staking_target_hype;
                if !eligibility_only_complete
                    && !zero_staking_complete
                    && !simulated_staking_complete
                {
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
        let expected_id = action_id_for_state(self, action.kind())?;
        if action.action_id() != expected_id {
            return Err(WorkflowError::InvalidTransition(
                "external action ID is not deterministic".into(),
            ));
        }
        match action {
            ExternalAction::SubmitOrder {
                client_order_id,
                execution_identity_hash,
                signer_identity_hash,
                notional_usdc,
                max_debit_usdc,
                original_quantity_hype,
                hype_atoms_per_hype,
                market_metadata_digest,
                limit_price_usdc_per_hype,
                l1_nonce,
                signed_expiry_at,
                ..
            } if self.stage == WorkflowStage::Decided
                && client_order_id == &client_order_id_for(&self.workflow_id)
                && execution_identity_hash
                    == &self.binding.inventory_before.execution_identity_hash
                && signer_identity_hash == &self.binding.order_envelope.signer_identity_hash
                && *notional_usdc == self.binding.planned_usdc
                && *max_debit_usdc == self.binding.committed_usdc
                && *original_quantity_hype
                    == self.binding.order_envelope.original_quantity_hype
                && *hype_atoms_per_hype == self.binding.order_envelope.hype_atoms_per_hype
                && market_metadata_digest
                    == &self.binding.order_envelope.market_metadata_digest
                && *limit_price_usdc_per_hype
                    == self.binding.order_envelope.limit_price_usdc_per_hype
                && *l1_nonce == self.binding.order_envelope.l1_nonce
                && *signed_expiry_at == self.binding.order_envelope.signed_expiry_at =>
            {
                Ok(())
            }
            ExternalAction::DepositToStaking {
                eligibility_workflow_id,
                capability_binding_hash,
                amount_hype,
                ..
            } if self.stage == WorkflowStage::StakingEligibilityRecorded
                && !amount_hype.is_zero()
                && *amount_hype == self.staking_eligible_hype
                && self.eligibility_workflow_id() == Some(eligibility_workflow_id)
                && self.offline_staking_capability_hash()?.as_str() == capability_binding_hash =>
            {
                Ok(())
            }
            ExternalAction::Delegate {
                eligibility_workflow_id,
                capability_binding_hash,
                validator_address,
                validator_summary_evidence_hash,
                amount_hype,
                ..
            } if self.stage == WorkflowStage::StakingBalanceConfirmed
                && *amount_hype == self.staking_target_hype
                && !amount_hype.is_zero()
                && self.eligibility_workflow_id() == Some(eligibility_workflow_id)
                && self.offline_staking_capability_hash()?.as_str() == capability_binding_hash
                && self.offline_staking_capability()?.validator_address == *validator_address
                && self
                    .offline_staking_capability()?
                    .validator_summary_evidence_hash
                    == *validator_summary_evidence_hash =>
            {
                Ok(())
            }
            _ => Err(WorkflowError::InvalidTransition(
                "external action payload is invalid for current state".into(),
            )),
        }
    }

    fn offline_staking_capability(
        &self,
    ) -> Result<&OfflineStakingCapabilityBinding, WorkflowError> {
        self.binding
            .offline_staking_capability
            .as_ref()
            .ok_or(WorkflowError::AutomaticStakingDisabled)
    }

    fn offline_staking_capability_hash(&self) -> Result<String, WorkflowError> {
        self.offline_staking_capability()?.digest()
    }

    fn validate_staking_balance_confirmation(
        &self,
        evidence: &StakingBalanceConfirmation,
        recorded_at: DateTime<Utc>,
    ) -> Result<(), WorkflowError> {
        let capability = self.offline_staking_capability()?;
        let prepared_at = self.staking_prepared_at.ok_or_else(|| {
            WorkflowError::CorruptJournal("staking preparation timestamp is missing".into())
        })?;
        let submitted_at = self.staking_submitted_at.ok_or_else(|| {
            WorkflowError::CorruptJournal("staking submission timestamp is missing".into())
        })?;
        let receipt_hash = self
            .staking_submission_receipt_hash
            .as_deref()
            .ok_or_else(|| {
                WorkflowError::CorruptJournal("staking submission receipt is missing".into())
            })?;
        let eligibility_workflow_id = self.eligibility_workflow_id().ok_or_else(|| {
            WorkflowError::CorruptJournal("staking eligibility identity is missing".into())
        })?;
        if !canonical_nonempty(&evidence.observation_id)
            || evidence.action_id != action_id_for_state(self, ActionKind::DepositToStaking)?
            || evidence.execution_identity_hash != capability.execution_identity_hash
            || evidence.eligibility_workflow_id != eligibility_workflow_id
            || evidence.submission_receipt_hash != receipt_hash
            || !lower_hex_digest(&evidence.baseline_history_hash)
            || evidence.baseline_captured_at >= prepared_at
            || !lower_hex_digest(&evidence.current_history_hash)
            || evidence.current_captured_at < submitted_at
            || evidence.current_captured_at <= evidence.baseline_captured_at
            || evidence.current_captured_at > recorded_at
            || evidence.baseline_history_hash == evidence.current_history_hash
            || !confirmation_transaction_matches(
                receipt_hash,
                self.staking_confirmed_transaction_hash.as_deref(),
                &evidence.matched_transaction_hash,
            )?
            || evidence.attributable_hype != self.staking_target_hype
            || evidence.attributable_hype.is_zero()
        {
            return Err(WorkflowError::ContradictoryObservation(
                "staking confirmation is not uniquely account/action/eligibility bound".into(),
            ));
        }
        Ok(())
    }

    fn validate_delegated_balance_confirmation(
        &self,
        evidence: &DelegatedBalanceConfirmation,
        recorded_at: DateTime<Utc>,
    ) -> Result<(), WorkflowError> {
        let capability = self.offline_staking_capability()?;
        let prepared_at = self.delegation_prepared_at.ok_or_else(|| {
            WorkflowError::CorruptJournal("delegation preparation timestamp is missing".into())
        })?;
        let submitted_at = self.delegation_submitted_at.ok_or_else(|| {
            WorkflowError::CorruptJournal("delegation submission timestamp is missing".into())
        })?;
        let receipt_hash = self
            .delegation_submission_receipt_hash
            .as_deref()
            .ok_or_else(|| {
                WorkflowError::CorruptJournal("delegation submission receipt is missing".into())
            })?;
        let eligibility_workflow_id = self.eligibility_workflow_id().ok_or_else(|| {
            WorkflowError::CorruptJournal("staking eligibility identity is missing".into())
        })?;
        if !canonical_nonempty(&evidence.observation_id)
            || evidence.action_id != action_id_for_state(self, ActionKind::Delegate)?
            || evidence.execution_identity_hash != capability.execution_identity_hash
            || evidence.eligibility_workflow_id != eligibility_workflow_id
            || evidence.validator_address != capability.validator_address
            || evidence.validator_summary_evidence_hash
                != capability.validator_summary_evidence_hash
            || evidence.submission_receipt_hash != receipt_hash
            || !lower_hex_digest(&evidence.baseline_history_hash)
            || evidence.baseline_captured_at >= prepared_at
            || !lower_hex_digest(&evidence.current_history_hash)
            || evidence.current_captured_at < submitted_at
            || evidence.current_captured_at <= evidence.baseline_captured_at
            || evidence.current_captured_at > recorded_at
            || evidence.baseline_history_hash == evidence.current_history_hash
            || !confirmation_transaction_matches(
                receipt_hash,
                self.delegation_confirmed_transaction_hash.as_deref(),
                &evidence.matched_transaction_hash,
            )?
            || evidence.attributable_hype != self.staking_target_hype
            || evidence.attributable_hype.is_zero()
        {
            return Err(WorkflowError::ContradictoryObservation(
                "delegation confirmation is not uniquely account/action/validator bound".into(),
            ));
        }
        Ok(())
    }

    fn validate_conclusive_absence(
        &self,
        evidence: &ConclusiveAbsenceEvidence,
        recorded_at: DateTime<Utc>,
    ) -> Result<(), WorkflowError> {
        let effective_expiry_at = self.binding.order_envelope.effective_expiry_at;
        if evidence.observation_id.trim().is_empty()
            || evidence.execution_identity_hash
                != self.binding.inventory_before.execution_identity_hash
            || evidence.client_order_id != client_order_id_for(&self.workflow_id)
            || evidence.effective_expiry_at != effective_expiry_at
            || recorded_at <= effective_expiry_at
            || !independent_history_watermarks(&evidence.order_history, &evidence.fill_history)
            || !valid_gap_free_watermark(
                &evidence.order_history,
                HistoryDomain::Order,
                self.binding.decided_at,
                effective_expiry_at,
                recorded_at,
            )
            || !valid_gap_free_watermark(
                &evidence.fill_history,
                HistoryDomain::Fill,
                self.binding.decided_at,
                effective_expiry_at,
                recorded_at,
            )
        {
            return Err(WorkflowError::ContradictoryObservation(
                "conclusive absence lacks post-expiry gap-free order/fill history".into(),
            ));
        }
        Ok(())
    }

    fn validate_eligibility_evidence(
        &self,
        evidence: Option<&OrderBoundEligibilityEvidence>,
        recorded_at: DateTime<Utc>,
    ) -> Result<(), WorkflowError> {
        let Some(order_id) = self.exchange_order_id.as_deref() else {
            if evidence.is_none() && self.purchased_hype.is_zero() {
                return Ok(());
            }
            return Err(WorkflowError::ContradictoryObservation(
                "eligibility evidence claimed an order after authoritative absence".into(),
            ));
        };
        let evidence = evidence.ok_or_else(|| {
            WorkflowError::ContradictoryObservation(
                "accepted order lacks signer-side order_bound authorization evidence".into(),
            )
        })?;
        let accepted_at = self.order_accepted_at.ok_or_else(|| {
            WorkflowError::CorruptJournal("accepted order timestamp is missing".into())
        })?;
        let prepared_at = self.order_prepared_at.ok_or_else(|| {
            WorkflowError::CorruptJournal("accepted order preparation timestamp is missing".into())
        })?;
        if evidence.authorization_id.trim().is_empty()
            || evidence.authorization_record_hash.trim().is_empty()
            || evidence.policy_version.trim().is_empty()
            || evidence.policy_version != self.binding.eligibility_policy.policy_version
            || evidence.decision_id != self.binding.decision_id
            || evidence.execution_identity_hash
                != self.binding.inventory_before.execution_identity_hash
            || evidence.signer_identity_hash != self.binding.order_envelope.signer_identity_hash
            || evidence.client_order_id != client_order_id_for(&self.workflow_id)
            || evidence.canonical_order_envelope_hash != canonical_order_envelope_hash(self)?
            || evidence.authorized_planned_usdc != self.binding.planned_usdc
            || evidence.authorized_max_debit_usdc != self.binding.committed_usdc
            || evidence.original_quantity_hype != self.binding.order_envelope.original_quantity_hype
            || evidence.hype_atoms_per_hype != self.binding.order_envelope.hype_atoms_per_hype
            || evidence.market_metadata_digest != self.binding.order_envelope.market_metadata_digest
            || evidence.limit_price_usdc_per_hype
                != self.binding.order_envelope.limit_price_usdc_per_hype
            || evidence.l1_nonce != self.binding.order_envelope.l1_nonce
            || evidence.signed_expiry_at != self.binding.order_envelope.signed_expiry_at
            || evidence.effective_expiry_at != self.binding.order_envelope.effective_expiry_at
            || evidence.order_id != order_id
            || evidence.accepted_at != accepted_at
            || evidence.authorized_at < self.binding.decided_at
            || evidence.authorized_at >= prepared_at
            || evidence.order_bound_at < accepted_at
            || evidence.order_bound_at >= evidence.effective_expiry_at
            || evidence.order_bound_at > recorded_at
            || evidence.effective_expiry_at <= accepted_at
            || evidence.effective_expiry_at <= evidence.signed_expiry_at
            || evidence.residual_reservation_hype
                != self.binding.inventory_before.residual_hype_deficit()
        {
            return Err(WorkflowError::ContradictoryObservation(
                "order_bound authorization does not exactly match the immutable order binding"
                    .into(),
            ));
        }
        if !valid_eligibility_history_watermark(
            &evidence.fill_history,
            HistoryDomain::Fill,
            self.binding.decided_at,
            recorded_at,
        ) || !valid_eligibility_history_watermark(
            &evidence.movement_history,
            HistoryDomain::Movement,
            self.binding.decided_at,
            recorded_at,
        ) || !independent_history_watermarks(&evidence.fill_history, &evidence.movement_history)
        {
            return Err(WorkflowError::ContradictoryObservation(
                "eligibility lacks a fresh common fill/movement watermark".into(),
            ));
        }
        self.validate_bound_fills(evidence, accepted_at, recorded_at)?;
        Self::validate_bound_movements(evidence, recorded_at)
    }

    fn validate_bound_fills(
        &self,
        evidence: &OrderBoundEligibilityEvidence,
        accepted_at: DateTime<Utc>,
        recorded_at: DateTime<Utc>,
    ) -> Result<(), WorkflowError> {
        let (registration_window, lot_max_age) =
            eligibility_policy_windows(&self.binding.eligibility_policy)?;
        let mut fill_ids = BTreeSet::new();
        let mut registration_record_ids = BTreeSet::new();
        let mut registration_cursors = BTreeSet::new();
        let mut purchased = 0_u64;
        let mut executed_notional = 0_u64;
        let mut residual_remaining = evidence.residual_reservation_hype.as_atoms();
        let mut previous: Option<(DateTime<Utc>, &str)> = None;
        for fill in &evidence.fills {
            let key = (fill.executed_at, fill.fill_id.as_str());
            let registration_deadline_at = fill
                .first_observed_at
                .checked_add_signed(registration_window)
                .ok_or_else(|| {
                    WorkflowError::ContradictoryObservation(
                        "fill registration deadline overflowed".into(),
                    )
                })?;
            let residual_for_fill = residual_remaining.min(fill.purchased_hype.as_atoms());
            let fill_notional_cap =
                max_fill_notional_usdc(fill.purchased_hype, &self.binding.order_envelope)
                    .ok_or_else(|| {
                        WorkflowError::ContradictoryObservation(
                            "fill quantity-at-limit notional overflowed".into(),
                        )
                    })?;
            let has_eligible_allocation = residual_for_fill < fill.purchased_hype.as_atoms();
            let eligibility_expired = if has_eligible_allocation {
                let eligibility_expires_at = fill
                    .executed_at
                    .checked_add_signed(lot_max_age)
                    .ok_or_else(|| {
                        WorkflowError::ContradictoryObservation(
                            "lot eligibility deadline overflowed".into(),
                        )
                    })?;
                recorded_at >= eligibility_expires_at
            } else {
                false
            };
            if fill.fill_id.trim().is_empty()
                || fill.fill_id.trim() != fill.fill_id
                || fill.authorization_id != evidence.authorization_id
                || fill.authorization_record_hash != evidence.authorization_record_hash
                || fill.execution_identity_hash != evidence.execution_identity_hash
                || fill.client_order_id != evidence.client_order_id
                || fill.order_id != evidence.order_id
                || fill.purchased_hype.is_zero()
                || fill.executed_notional_usdc.is_zero()
                || fill.executed_notional_usdc > fill_notional_cap
                || !fill_ids.insert(fill.fill_id.as_str())
                || previous.is_some_and(|previous| previous >= key)
                || fill.executed_at < accepted_at
                || fill.executed_at >= evidence.effective_expiry_at
                || fill.first_observed_at < fill.executed_at
                || fill.registration_record_id.trim().is_empty()
                || !registration_record_ids.insert(fill.registration_record_id.as_str())
                || fill.registration_record_hash.trim().is_empty()
                || fill.registration_cursor == 0
                || !registration_cursors.insert(fill.registration_cursor)
                || fill.registration_cursor > evidence.fill_history.cursor
                || fill.registered_at < fill.first_observed_at
                || fill.registered_at >= fill.registration_deadline_at
                || fill.registered_at > recorded_at
                || fill.registration_deadline_at != registration_deadline_at
                || fill.registration_deadline_at < fill.first_observed_at
                || recorded_at < fill.first_observed_at
                || eligibility_expired
            {
                return Err(WorkflowError::ContradictoryObservation(
                    "fill evidence is late, duplicated, unsorted, or outside the authorized order window"
                        .into(),
                ));
            }
            (purchased, executed_notional) =
                checked_fill_totals(purchased, executed_notional, fill)?;
            residual_remaining = residual_remaining.saturating_sub(fill.purchased_hype.as_atoms());
            previous = Some(key);
        }
        if purchased != self.purchased_hype.as_atoms()
            || executed_notional != self.filled_usdc.as_micros()
        {
            return Err(WorkflowError::ContradictoryObservation(
                "authorized fill set does not equal the terminal quantity and execution notional"
                    .into(),
            ));
        }
        Ok(())
    }

    fn validate_bound_movements(
        evidence: &OrderBoundEligibilityEvidence,
        recorded_at: DateTime<Utc>,
    ) -> Result<(), WorkflowError> {
        let mut movement_ids = BTreeSet::new();
        let mut consumed = 0_u64;
        let mut previous: Option<(DateTime<Utc>, &str)> = None;
        for movement in &evidence.movements {
            let key = (movement.occurred_at, movement.movement_id.as_str());
            consumed = consumed
                .checked_add(movement.consumed_hype.as_atoms())
                .ok_or_else(|| {
                    WorkflowError::ContradictoryObservation(
                        "movement consumption overflowed".into(),
                    )
                })?;
            let residual_available = residual_hype_available_before(
                &evidence.fills,
                evidence.residual_reservation_hype,
                movement.occurred_at,
            )
            .ok_or_else(|| {
                WorkflowError::ContradictoryObservation(
                    "residual movement availability overflowed".into(),
                )
            })?;
            if !canonical_nonempty(&movement.movement_id)
                || movement.consumed_hype.is_zero()
                || !movement_ids.insert(movement.movement_id.as_str())
                || previous.is_some_and(|previous| previous >= key)
                || movement.occurred_at > recorded_at
                || movement.occurred_at > evidence.movement_history.through_at
                || consumed > residual_available
            {
                return Err(WorkflowError::ContradictoryObservation(
                    "movement evidence is invalid or consumes an eligible allocation".into(),
                ));
            }
            previous = Some(key);
        }
        Ok(())
    }

    fn validate_observed_fill(
        &self,
        cumulative_hype: HypeAtoms,
        cumulative_filled_usdc: UsdcMicros,
        cumulative_debited_usdc: UsdcMicros,
        fully_filled: bool,
    ) -> Result<(), WorkflowError> {
        self.validate_cumulative_fill(
            cumulative_hype,
            cumulative_filled_usdc,
            cumulative_debited_usdc,
            false,
        )?;
        if fully_filled && cumulative_hype != self.binding.order_envelope.original_quantity_hype {
            return Err(WorkflowError::ContradictoryObservation(
                "fully-filled observation did not reconcile the full signed quantity".into(),
            ));
        }
        Ok(())
    }

    fn validate_order_submission_evidence(
        &self,
        evidence: &AuthenticatedOrderSubmission,
        recorded_at: DateTime<Utc>,
    ) -> Result<(), WorkflowError> {
        let envelope = &self.binding.order_envelope;
        let execution_identity_hash = &self.binding.inventory_before.execution_identity_hash;
        if evidence.observation_id.trim().is_empty()
            || evidence.observation_id != evidence.observation_id.trim()
            || evidence.account_scope_evidence_hash.trim().is_empty()
            || evidence.account_scope_evidence_hash != evidence.account_scope_evidence_hash.trim()
            || evidence.order_envelope_evidence_hash.trim().is_empty()
            || evidence.order_envelope_evidence_hash != evidence.order_envelope_evidence_hash.trim()
            || evidence.exchange_order_id.trim().is_empty()
            || evidence.exchange_order_id != evidence.exchange_order_id.trim()
            || evidence.execution_identity_hash != *execution_identity_hash
            || evidence.signer_identity_hash != envelope.signer_identity_hash
            || evidence.decision_id != self.binding.decision_id
            || evidence.client_order_id != self.client_order_id()
            || evidence.canonical_order_envelope_hash != canonical_order_envelope_hash(self)?
            || evidence.planned_usdc != self.binding.planned_usdc
            || evidence.max_debit_usdc != self.binding.committed_usdc
            || evidence.original_quantity_hype != envelope.original_quantity_hype
            || evidence.hype_atoms_per_hype != envelope.hype_atoms_per_hype
            || evidence.market_metadata_digest != envelope.market_metadata_digest
            || evidence.limit_price_usdc_per_hype != envelope.limit_price_usdc_per_hype
            || evidence.l1_nonce != envelope.l1_nonce
            || evidence.signed_expiry_at != envelope.signed_expiry_at
            || evidence.effective_expiry_at != envelope.effective_expiry_at
            || evidence.market != "HYPE/USDC"
            || evidence.side != "buy"
            || evidence.time_in_force != "IOC"
            || evidence.accepted_at < self.last_transition_at
            || evidence.accepted_at >= envelope.signed_expiry_at
            || evidence.accepted_at > recorded_at
        {
            return Err(WorkflowError::ContradictoryObservation(
                "authenticated venue order does not exactly match the authorized envelope".into(),
            ));
        }
        Ok(())
    }

    fn validate_order_finalization(
        &self,
        cumulative_hype: HypeAtoms,
        cumulative_filled_usdc: UsdcMicros,
        cumulative_debited_usdc: UsdcMicros,
        finality: OrderFinality,
    ) -> Result<(), WorkflowError> {
        self.validate_cumulative_fill(
            cumulative_hype,
            cumulative_filled_usdc,
            cumulative_debited_usdc,
            matches!(finality, OrderFinality::Canceled | OrderFinality::Expired),
        )?;
        if finality == OrderFinality::Filled
            && cumulative_hype != self.binding.order_envelope.original_quantity_hype
        {
            return Err(WorkflowError::ContradictoryObservation(
                "filled finality did not reconcile the full signed quantity".into(),
            ));
        }
        Ok(())
    }

    fn validate_cumulative_fill(
        &self,
        cumulative_hype: HypeAtoms,
        cumulative_filled_usdc: UsdcMicros,
        cumulative_debited_usdc: UsdcMicros,
        allow_zero: bool,
    ) -> Result<(), WorkflowError> {
        let proportional_fill_cap =
            max_fill_notional_usdc(cumulative_hype, &self.binding.order_envelope).ok_or_else(
                || {
                    WorkflowError::ContradictoryObservation(
                        "cumulative quantity-at-limit notional overflowed".into(),
                    )
                },
            )?;
        if (cumulative_hype.is_zero() && !allow_zero)
            || cumulative_hype.is_zero() != cumulative_filled_usdc.is_zero()
            || cumulative_hype < self.purchased_hype
            || cumulative_filled_usdc < self.filled_usdc
            || cumulative_debited_usdc < self.debited_usdc
            || cumulative_debited_usdc < cumulative_filled_usdc
            || cumulative_filled_usdc > self.binding.planned_usdc
            || cumulative_filled_usdc > proportional_fill_cap
            || cumulative_debited_usdc > self.binding.committed_usdc
            || cumulative_hype > self.binding.order_envelope.original_quantity_hype
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

    fn order_is_terminal(&self) -> bool {
        matches!(
            self.stage,
            WorkflowStage::OrderFinalized
                | WorkflowStage::StakingEligibilityRecorded
                | WorkflowStage::StakingDepositSubmitted
                | WorkflowStage::StakingBalanceConfirmed
                | WorkflowStage::DelegationSubmitted
                | WorkflowStage::DelegatedConfirmed
                | WorkflowStage::Complete
        )
    }

    fn invalid_transition_contradiction_reason(
        &self,
        transition: &WorkflowTransition,
        at: DateTime<Utc>,
    ) -> Option<String> {
        match transition {
            WorkflowTransition::OrderSubmissionObserved { evidence, .. }
                if evidence.accepted_at < self.last_transition_at =>
            {
                Some("accepted order predates the durable order preparation".into())
            }
            WorkflowTransition::OrderSubmissionObserved { evidence, .. }
                if evidence.accepted_at >= self.binding.order_envelope.signed_expiry_at =>
            {
                Some("order acceptance reached its signed expiry horizon".into())
            }
            WorkflowTransition::OrderSubmissionObserved { .. } if self.order_is_terminal() => {
                Some("accepted order appeared after terminal reconciliation".into())
            }
            WorkflowTransition::OrderSubmissionAbsent { .. }
                if self.exchange_order_id.is_some() =>
            {
                Some("order absence contradicted an accepted order".into())
            }
            WorkflowTransition::OrderSubmissionAbsent { evidence, .. }
                if self.stage == WorkflowStage::Decided =>
            {
                if at < self.last_transition_at {
                    return Some("absence evidence predates the prepared order".into());
                }
                match self.validate_conclusive_absence(evidence, at) {
                    Err(WorkflowError::ContradictoryObservation(reason)) => Some(reason),
                    _ => None,
                }
            }
            WorkflowTransition::OrderFillObserved { .. } if self.order_is_terminal() => {
                Some("new fill appeared after terminal reconciliation".into())
            }
            WorkflowTransition::OrderFinalized { .. } if self.order_is_terminal() => {
                Some("terminal order appeared after terminal reconciliation".into())
            }
            WorkflowTransition::OrderFillObserved {
                cumulative_hype,
                cumulative_filled_usdc,
                cumulative_debited_usdc,
                fully_filled,
                ..
            } if matches!(
                self.stage,
                WorkflowStage::OrderSubmitted
                    | WorkflowStage::PartiallyFilled
                    | WorkflowStage::Filled
            ) =>
            {
                if let Err(WorkflowError::ContradictoryObservation(reason)) = self
                    .validate_observed_fill(
                        *cumulative_hype,
                        *cumulative_filled_usdc,
                        *cumulative_debited_usdc,
                        *fully_filled,
                    )
                {
                    return Some(reason);
                }
                (self.stage == WorkflowStage::Filled && !fully_filled)
                    .then(|| "a fully filled order regressed to partial".into())
            }
            WorkflowTransition::OrderFinalized {
                cumulative_hype,
                cumulative_filled_usdc,
                cumulative_debited_usdc,
                finality,
                ..
            } if matches!(
                self.stage,
                WorkflowStage::OrderSubmitted
                    | WorkflowStage::PartiallyFilled
                    | WorkflowStage::Filled
            ) =>
            {
                match self.validate_order_finalization(
                    *cumulative_hype,
                    *cumulative_filled_usdc,
                    *cumulative_debited_usdc,
                    *finality,
                ) {
                    Err(WorkflowError::ContradictoryObservation(reason)) => Some(reason),
                    _ => None,
                }
            }
            WorkflowTransition::StakingEligibilityRecorded { evidence, .. }
                if self.stage == WorkflowStage::OrderFinalized =>
            {
                if at < self.last_transition_at {
                    return Some(
                        "eligibility evidence predates terminal order reconciliation".into(),
                    );
                }
                match self.validate_eligibility_evidence(evidence.as_deref(), at) {
                    Err(WorkflowError::ContradictoryObservation(reason)) => Some(reason),
                    _ => None,
                }
            }
            _ => None,
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct JournalCheckpoint {
    schema_version: u8,
    workflow_id: String,
    sequence: Option<u64>,
    record_hash: String,
    journal_len: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct PendingWorkflowAppend {
    schema_version: u8,
    workflow_id: String,
    prior_head: Option<ProtectedWorkflowHead>,
    prior_journal_len: u64,
    record: JournalRecord,
    next_head: ProtectedWorkflowHead,
}

/// Latest workflow journal head retained outside the journal rollback domain.
///
/// Implementations of [`ProtectedWorkflowHeadStore`] must durably persist this
/// value independently of the journal and its adjacent checkpoint.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ProtectedWorkflowHead {
    pub schema_version: u8,
    pub workflow_id: String,
    pub sequence: u64,
    pub record_hash: String,
    pub journal_len: u64,
}

/// Independently durable, monotonic storage for one stable decision's head.
///
/// A store instance must be scoped by the stable decision identity, never by a
/// journal path or a hash of mutable execution inputs. Every attempted binding
/// for one decision must therefore observe the same protected ownership head.
/// `compare_and_swap` must atomically and durably replace `expected` with
/// `next`, returning `false` when the currently protected value differs from
/// `expected`.
pub trait ProtectedWorkflowHeadStore: Send + Sync {
    /// Loads the currently protected head.
    ///
    /// # Errors
    ///
    /// Returns an implementation-specific availability or integrity error.
    fn load(&self) -> Result<Option<ProtectedWorkflowHead>, String>;

    /// Durably advances the protected head if its current value is `expected`.
    ///
    /// # Errors
    ///
    /// Returns an implementation-specific availability or persistence error.
    fn compare_and_swap(
        &self,
        expected: Option<&ProtectedWorkflowHead>,
        next: &ProtectedWorkflowHead,
    ) -> Result<bool, String>;
}

/// File-backed [`ProtectedWorkflowHeadStore`], one instance per stable
/// decision identity (construct with a path derived from that identity, e.g.
/// `<state_dir>/workflow-heads/<decision_id>.json`; never share one instance
/// or path across decisions). Mirrors the ledger's
/// `FileProtectedAnchorStore` durability pattern: an exclusive lock file
/// serializes readers/writers, symlinks and hard-linked paths are refused,
/// and every write is an atomic create-temp-then-rename with a directory
/// fsync, so a crash between write and rename never leaves a partial file
/// and a crash after rename is indistinguishable from a completed swap.
pub struct FileProtectedWorkflowHeadStore {
    path: PathBuf,
    lock_path: PathBuf,
}

impl FileProtectedWorkflowHeadStore {
    /// Constructs a store without reading or creating the protected head.
    ///
    /// # Errors
    ///
    /// Returns an error if the path does not name a normal absolute file.
    pub fn new(path: impl Into<PathBuf>) -> Result<Self, String> {
        let path = path.into();
        if !normal_absolute_path(&path) {
            return Err("protected workflow head path must name a normal absolute file".into());
        }
        let mut lock_name = path
            .file_name()
            .ok_or_else(|| "protected workflow head has no file name".to_owned())?
            .to_os_string();
        lock_name.push(".lock");
        let lock_path = path
            .parent()
            .ok_or_else(|| "protected workflow head has no parent".to_owned())?
            .join(lock_name);
        Ok(Self { path, lock_path })
    }

    fn read(&self) -> Result<Option<ProtectedWorkflowHead>, String> {
        reject_linked_file(&self.path)
            .map_err(|error| format!("unsafe protected workflow head: {error}"))?;
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW);
        }
        match options.open(&self.path) {
            Ok(mut file) => {
                reject_multiple_links(&file)
                    .map_err(|error| format!("unsafe protected workflow head: {error}"))?;
                let mut payload = String::new();
                file.read_to_string(&mut payload)
                    .map_err(|error| format!("protected workflow head read failed: {error}"))?;
                serde_json::from_str(&payload)
                    .map(Some)
                    .map_err(|error| format!("invalid protected workflow head: {error}"))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(format!("protected workflow head read failed: {error}")),
        }
    }

    fn lock(&self) -> Result<File, String> {
        let parent = self
            .lock_path
            .parent()
            .ok_or_else(|| "protected workflow head lock has no parent".to_owned())?;
        fs::create_dir_all(parent)
            .map_err(|error| format!("protected workflow head directory create failed: {error}"))?;
        reject_linked_file(&self.lock_path)
            .map_err(|error| format!("unsafe protected workflow head lock: {error}"))?;
        let mut options = OpenOptions::new();
        options.create(true).read(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
        }
        let lock = options
            .open(&self.lock_path)
            .map_err(|error| format!("protected workflow head lock open failed: {error}"))?;
        reject_multiple_links(&lock)
            .map_err(|error| format!("unsafe protected workflow head lock: {error}"))?;
        lock.lock_exclusive()
            .map_err(|error| format!("protected workflow head lock failed: {error}"))?;
        Ok(lock)
    }
}

impl ProtectedWorkflowHeadStore for FileProtectedWorkflowHeadStore {
    fn load(&self) -> Result<Option<ProtectedWorkflowHead>, String> {
        self.read()
    }

    fn compare_and_swap(
        &self,
        expected: Option<&ProtectedWorkflowHead>,
        next: &ProtectedWorkflowHead,
    ) -> Result<bool, String> {
        let lock = self.lock()?;
        let current = self.read()?;
        if current.as_ref() != expected {
            fs2::FileExt::unlock(&lock)
                .map_err(|error| format!("protected workflow head unlock failed: {error}"))?;
            return Ok(false);
        }
        crate::status_io::write_private_json_atomic(&self.path, next)
            .map_err(|error| format!("protected workflow head write failed: {error}"))?;
        fs2::FileExt::unlock(&lock)
            .map_err(|error| format!("protected workflow head unlock failed: {error}"))?;
        Ok(true)
    }
}

/// Immutable ownership of one stable venue order identity.
///
/// The store key is `(execution_identity_hash, exchange_order_id)`. The
/// remaining fields bind that venue order to exactly one decision workflow and
/// its signer-authorized envelope.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ExchangeOrderOwner {
    pub schema_version: u8,
    pub execution_identity_hash: String,
    pub exchange_order_id: String,
    pub decision_id: String,
    pub workflow_id: String,
    pub client_order_id: String,
    pub canonical_order_envelope_hash: String,
}

/// Immutable ownership of one stable venue fill identity.
///
/// The key is `(execution_identity_hash, fill_id)`. The remaining fields bind
/// that fill to exactly one signer authorization, venue order, and decision.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ExchangeFillOwner {
    pub schema_version: u8,
    pub execution_identity_hash: String,
    pub fill_id: String,
    pub decision_id: String,
    pub workflow_id: String,
    pub authorization_id: String,
    pub authorization_record_hash: String,
    pub exchange_order_id: String,
    pub client_order_id: String,
    pub canonical_order_envelope_hash: String,
}

/// Shared durable ownership storage for exchange order and fill identities.
///
/// One store must cover every workflow for the execution identities it serves;
/// it must not be scoped to a decision or journal path. `claim` must atomically
/// and durably insert a missing key, return `true` for an exact existing owner,
/// and return `false` without mutation when the key belongs to another owner.
/// Commit-coupled claims use recoverable pre-commit intents as documented below;
/// once committed, ownership remains immutable and append-only.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OwnershipCommitOutcome {
    Committed,
    Conflict,
    CommitRejected,
    CommitAmbiguous,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JournalCommitStatus {
    Committed,
    Rejected,
    Ambiguous,
}

pub trait ExchangeOrderOwnerStore: Send + Sync {
    /// Claims an exchange order identity for exactly one decision workflow.
    ///
    /// # Errors
    ///
    /// Returns an implementation-specific availability or persistence error.
    fn claim(&self, owner: &ExchangeOrderOwner) -> Result<bool, String>;

    /// Serializes an order claim with its durable workflow commit.
    ///
    /// The store must hold one execution-identity-wide serialization boundary
    /// while invoking `commit`. Before invoking it, the store must atomically
    /// and durably insert a missing owner as a recoverable intent. If `commit`
    /// returns [`JournalCommitStatus::Rejected`], the store must atomically
    /// remove only an intent inserted by this call before returning. It must
    /// retain the intent for `Committed` and `Ambiguous`. An exact intent left by
    /// process failure is retained and allows the same workflow to retry, while
    /// conflicting owners remain excluded throughout the commit crash window.
    ///
    /// # Errors
    ///
    /// Returns an implementation-specific availability or persistence error.
    fn claim_and_commit(
        &self,
        owner: &ExchangeOrderOwner,
        commit: &mut dyn FnMut() -> JournalCommitStatus,
    ) -> Result<OwnershipCommitOutcome, String>;

    /// Atomically claims a venue fill bundle for exactly one authorization and order.
    ///
    /// A conflict anywhere in the bundle must return `false` without inserting
    /// any missing owner from that bundle.
    ///
    /// # Errors
    ///
    /// Returns an implementation-specific availability or persistence error.
    fn claim_fills(&self, owners: &[ExchangeFillOwner]) -> Result<bool, String>;

    /// Serializes an atomic fill-bundle claim with its durable workflow commit.
    ///
    /// Before invoking `commit`, the store must atomically and durably insert
    /// every missing owner in the bundle as one recoverable intent. If `commit`
    /// returns [`JournalCommitStatus::Rejected`], it must atomically remove only
    /// the owners inserted by this call. It must retain the complete bundle for
    /// `Committed` and `Ambiguous`. A process failure may leave that exact bundle
    /// for the same workflow to retry, but must never expose a partial bundle.
    ///
    /// # Errors
    ///
    /// Returns an implementation-specific availability or persistence error.
    fn claim_fills_and_commit(
        &self,
        owners: &[ExchangeFillOwner],
        commit: &mut dyn FnMut() -> JournalCommitStatus,
    ) -> Result<OwnershipCommitOutcome, String>;
}

const EXCHANGE_OWNER_FILE_SCHEMA_VERSION: u8 = 1;

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct ExchangeOwnerFile {
    #[serde(default)]
    schema_version: u8,
    #[serde(default)]
    order_owners: Vec<ExchangeOrderOwner>,
    #[serde(default)]
    fill_owners: Vec<ExchangeFillOwner>,
}

/// File-backed [`ExchangeOrderOwnerStore`]. Unlike
/// [`FileProtectedWorkflowHeadStore`], exactly one instance must be shared by
/// every workflow for the execution identities it serves — never scope it to
/// a decision or journal path. Ownership records are logically append-only
/// (an owner, once committed, is never overwritten or removed); the on-disk
/// representation is a single JSON document holding both owner lists,
/// rewritten atomically on every change. One exclusive lock file serializes
/// every claim across all execution identities this store instance serves,
/// which satisfies the trait's required execution-identity-wide boundary
/// (a coarser, whole-store boundary is a strict superset of a per-identity
/// one) at the cost of not letting unrelated identities claim concurrently —
/// an acceptable tradeoff for this single-process bot's claim volume.
pub struct FileExchangeOrderOwnerStore {
    path: PathBuf,
    lock_path: PathBuf,
}

impl FileExchangeOrderOwnerStore {
    /// Constructs a store without reading or creating its backing file.
    ///
    /// # Errors
    ///
    /// Returns an error if the path does not name a normal absolute file.
    pub fn new(path: impl Into<PathBuf>) -> Result<Self, String> {
        let path = path.into();
        if !normal_absolute_path(&path) {
            return Err("exchange order owner path must name a normal absolute file".into());
        }
        let mut lock_name = path
            .file_name()
            .ok_or_else(|| "exchange order owner store has no file name".to_owned())?
            .to_os_string();
        lock_name.push(".lock");
        let lock_path = path
            .parent()
            .ok_or_else(|| "exchange order owner store has no parent".to_owned())?
            .join(lock_name);
        Ok(Self { path, lock_path })
    }

    fn read(&self) -> Result<ExchangeOwnerFile, String> {
        reject_linked_file(&self.path)
            .map_err(|error| format!("unsafe exchange order owner store: {error}"))?;
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW);
        }
        match options.open(&self.path) {
            Ok(mut file) => {
                reject_multiple_links(&file)
                    .map_err(|error| format!("unsafe exchange order owner store: {error}"))?;
                let mut payload = String::new();
                file.read_to_string(&mut payload)
                    .map_err(|error| format!("exchange order owner store read failed: {error}"))?;
                let data: ExchangeOwnerFile = serde_json::from_str(&payload)
                    .map_err(|error| format!("invalid exchange order owner store: {error}"))?;
                if data.schema_version != EXCHANGE_OWNER_FILE_SCHEMA_VERSION {
                    return Err(format!(
                        "unsupported exchange order owner store schema version {}",
                        data.schema_version
                    ));
                }
                Ok(data)
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(ExchangeOwnerFile {
                schema_version: EXCHANGE_OWNER_FILE_SCHEMA_VERSION,
                ..ExchangeOwnerFile::default()
            }),
            Err(error) => Err(format!("exchange order owner store read failed: {error}")),
        }
    }

    fn write(&self, data: &ExchangeOwnerFile) -> Result<(), String> {
        crate::status_io::write_private_json_atomic(&self.path, data)
            .map_err(|error| format!("exchange order owner store write failed: {error}"))
    }

    fn lock(&self) -> Result<File, String> {
        let parent = self
            .lock_path
            .parent()
            .ok_or_else(|| "exchange order owner store lock has no parent".to_owned())?;
        fs::create_dir_all(parent).map_err(|error| {
            format!("exchange order owner store directory create failed: {error}")
        })?;
        reject_linked_file(&self.lock_path)
            .map_err(|error| format!("unsafe exchange order owner store lock: {error}"))?;
        let mut options = OpenOptions::new();
        options.create(true).read(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
        }
        let lock = options
            .open(&self.lock_path)
            .map_err(|error| format!("exchange order owner store lock open failed: {error}"))?;
        reject_multiple_links(&lock)
            .map_err(|error| format!("unsafe exchange order owner store lock: {error}"))?;
        lock.lock_exclusive()
            .map_err(|error| format!("exchange order owner store lock failed: {error}"))?;
        Ok(lock)
    }
}

fn order_owner_key(owner: &ExchangeOrderOwner) -> (&str, &str) {
    (&owner.execution_identity_hash, &owner.exchange_order_id)
}

fn fill_owner_key(owner: &ExchangeFillOwner) -> (&str, &str) {
    (&owner.execution_identity_hash, &owner.fill_id)
}

fn find_fill_owner<'a>(
    data: &'a ExchangeOwnerFile,
    key: (&str, &str),
) -> Option<&'a ExchangeFillOwner> {
    data.fill_owners
        .iter()
        .find(|candidate| fill_owner_key(candidate) == key)
}

/// Groups a fill bundle by key, rejecting an internally inconsistent bundle
/// (the same key claimed twice with different values) the same way the
/// in-memory reference store does.
fn fill_bundle_claims(
    owners: &[ExchangeFillOwner],
) -> Option<BTreeMap<(String, String), ExchangeFillOwner>> {
    let mut claims = BTreeMap::new();
    for owner in owners {
        let key = (owner.execution_identity_hash.clone(), owner.fill_id.clone());
        if claims
            .insert(key, owner.clone())
            .is_some_and(|existing| existing != *owner)
        {
            return None;
        }
    }
    Some(claims)
}

impl ExchangeOrderOwnerStore for FileExchangeOrderOwnerStore {
    fn claim(&self, owner: &ExchangeOrderOwner) -> Result<bool, String> {
        let lock = self.lock()?;
        let mut data = self.read()?;
        let key = order_owner_key(owner);
        let result = if let Some(existing) = data
            .order_owners
            .iter()
            .find(|candidate| order_owner_key(candidate) == key)
        {
            existing == owner
        } else {
            data.order_owners.push(owner.clone());
            self.write(&data)?;
            true
        };
        fs2::FileExt::unlock(&lock)
            .map_err(|error| format!("exchange order owner store unlock failed: {error}"))?;
        Ok(result)
    }

    fn claim_and_commit(
        &self,
        owner: &ExchangeOrderOwner,
        commit: &mut dyn FnMut() -> JournalCommitStatus,
    ) -> Result<OwnershipCommitOutcome, String> {
        let lock = self.lock()?;
        let mut data = self.read()?;
        let key = order_owner_key(owner);
        let existing = data
            .order_owners
            .iter()
            .find(|candidate| order_owner_key(candidate) == key);
        if existing.is_some_and(|candidate| candidate != owner) {
            fs2::FileExt::unlock(&lock)
                .map_err(|error| format!("exchange order owner store unlock failed: {error}"))?;
            return Ok(OwnershipCommitOutcome::Conflict);
        }
        let inserted = existing.is_none();
        if inserted {
            data.order_owners.push(owner.clone());
            self.write(&data)?;
        }
        let outcome = match commit() {
            JournalCommitStatus::Committed => OwnershipCommitOutcome::Committed,
            JournalCommitStatus::Ambiguous => OwnershipCommitOutcome::CommitAmbiguous,
            JournalCommitStatus::Rejected => {
                if inserted {
                    data.order_owners
                        .retain(|candidate| order_owner_key(candidate) != key);
                    self.write(&data)?;
                }
                OwnershipCommitOutcome::CommitRejected
            }
        };
        fs2::FileExt::unlock(&lock)
            .map_err(|error| format!("exchange order owner store unlock failed: {error}"))?;
        Ok(outcome)
    }

    fn claim_fills(&self, owners: &[ExchangeFillOwner]) -> Result<bool, String> {
        let Some(claims) = fill_bundle_claims(owners) else {
            return Ok(false);
        };
        let lock = self.lock()?;
        let mut data = self.read()?;
        let conflict = claims.iter().any(|(key, owner)| {
            find_fill_owner(&data, (key.0.as_str(), key.1.as_str())).is_some_and(|e| e != owner)
        });
        if conflict {
            fs2::FileExt::unlock(&lock)
                .map_err(|error| format!("exchange order owner store unlock failed: {error}"))?;
            return Ok(false);
        }
        let inserted_keys = claims
            .iter()
            .filter(|(key, _)| find_fill_owner(&data, (key.0.as_str(), key.1.as_str())).is_none())
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        if !inserted_keys.is_empty() {
            for (key, owner) in &claims {
                if inserted_keys.contains(key) {
                    data.fill_owners.push(owner.clone());
                }
            }
            self.write(&data)?;
        }
        fs2::FileExt::unlock(&lock)
            .map_err(|error| format!("exchange order owner store unlock failed: {error}"))?;
        Ok(true)
    }

    fn claim_fills_and_commit(
        &self,
        owners: &[ExchangeFillOwner],
        commit: &mut dyn FnMut() -> JournalCommitStatus,
    ) -> Result<OwnershipCommitOutcome, String> {
        let Some(claims) = fill_bundle_claims(owners) else {
            return Ok(OwnershipCommitOutcome::Conflict);
        };
        let lock = self.lock()?;
        let mut data = self.read()?;
        let conflict = claims.iter().any(|(key, owner)| {
            find_fill_owner(&data, (key.0.as_str(), key.1.as_str())).is_some_and(|e| e != owner)
        });
        if conflict {
            fs2::FileExt::unlock(&lock)
                .map_err(|error| format!("exchange order owner store unlock failed: {error}"))?;
            return Ok(OwnershipCommitOutcome::Conflict);
        }
        let inserted_keys = claims
            .iter()
            .filter(|(key, _)| find_fill_owner(&data, (key.0.as_str(), key.1.as_str())).is_none())
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        if !inserted_keys.is_empty() {
            for (key, owner) in &claims {
                if inserted_keys.contains(key) {
                    data.fill_owners.push(owner.clone());
                }
            }
            self.write(&data)?;
        }
        let outcome = match commit() {
            JournalCommitStatus::Committed => OwnershipCommitOutcome::Committed,
            JournalCommitStatus::Ambiguous => OwnershipCommitOutcome::CommitAmbiguous,
            JournalCommitStatus::Rejected => {
                if !inserted_keys.is_empty() {
                    data.fill_owners.retain(|candidate| {
                        !inserted_keys.iter().any(|key| {
                            fill_owner_key(candidate) == (key.0.as_str(), key.1.as_str())
                        })
                    });
                    self.write(&data)?;
                }
                OwnershipCommitOutcome::CommitRejected
            }
        };
        fs2::FileExt::unlock(&lock)
            .map_err(|error| format!("exchange order owner store unlock failed: {error}"))?;
        Ok(outcome)
    }
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
    protected_head_store: Arc<dyn ProtectedWorkflowHeadStore>,
    protected_head: Option<ProtectedWorkflowHead>,
    exchange_order_owner_store: Arc<dyn ExchangeOrderOwnerStore>,
}

impl DurableWorkflow {
    /// Reads the decision binding already durably committed to this
    /// journal, if any, without a lock or a caller-supplied binding to
    /// validate against.
    ///
    /// A caller whose binding recomputes time-varying inputs (price, nonce,
    /// expiry) on every call must use this first and, when it returns
    /// `Some`, pass that exact binding to [`Self::open_or_create`] instead
    /// of a freshly recomputed one. A crash between the initial commit and
    /// a later append (e.g. before [`Self::prepare_order`] runs) otherwise
    /// leaves the journal durably bound to the *first* attempt's values;
    /// every retry that recomputes fresh values would then permanently fail
    /// `open_or_create`'s binding-match check.
    ///
    /// # Errors
    ///
    /// Returns an error for a truncated or hash-invalid journal. Returns
    /// `Ok(None)` when the journal does not yet exist or is empty.
    pub fn peek_committed_binding(
        path: impl AsRef<Path>,
    ) -> Result<Option<DecisionBinding>, WorkflowError> {
        let records = load_records(path.as_ref())?;
        if records.is_empty() {
            return Ok(None);
        }
        let events = records
            .iter()
            .map(|record| record.event.clone())
            .collect::<Vec<_>>();
        let state = WorkflowState::replay(&events)?;
        Ok(Some(state.binding))
    }

    /// Opens or creates one append-only workflow journal.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed bindings, truncated/hash-invalid or
    /// rolled-back journals, a binding mismatch, or a durable write failure.
    pub fn open_or_create(
        path: impl AsRef<Path>,
        binding: &DecisionBinding,
        protected_head_store: Arc<dyn ProtectedWorkflowHeadStore>,
        exchange_order_owner_store: Arc<dyn ExchangeOrderOwnerStore>,
    ) -> Result<Self, WorkflowError> {
        binding.validate()?;
        let path = path.as_ref().to_path_buf();
        let workflow_id = workflow_id_for(binding);
        let append_lock = acquire_journal_append_lock(&path)?;
        recover_pending_append(&path, &workflow_id, protected_head_store.as_ref())?;
        let records = load_records(&path)?;
        if records.is_empty() {
            if protected_head_store
                .load()
                .map_err(WorkflowError::protected_head)?
                .is_some()
            {
                return Err(WorkflowError::RollbackDetected(
                    "protected head exists but the local journal is empty".into(),
                ));
            }
            if checkpoint_path(&path).exists() {
                verify_bootstrap_checkpoint(&path, &workflow_id)?;
            } else {
                write_checkpoint(&path, &bootstrap_checkpoint(&workflow_id))?;
            }
            let initial = WorkflowEvent {
                event_id: event_id_for_decision(&workflow_id),
                at: binding.decided_at,
                transition: WorkflowTransition::DecisionRecorded {
                    workflow_id,
                    binding: Box::new(binding.clone()),
                },
            };
            let state = WorkflowState::replay(std::slice::from_ref(&initial))?;
            let mut workflow = Self {
                path,
                records: Vec::new(),
                events_by_id: BTreeMap::new(),
                state,
                file_len: 0,
                protected_head_store,
                protected_head: None,
                exchange_order_owner_store,
            };
            workflow.append_while_locked(initial, &append_lock)?;
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
            let last = records.last().ok_or_else(|| {
                WorkflowError::CorruptJournal("non-empty journal has no final record".into())
            })?;
            let expected_protected_head = protected_head_for(last, &state.workflow_id, file_len);
            let protected_head = protected_head_store
                .load()
                .map_err(WorkflowError::protected_head)?;
            if protected_head.as_ref() != Some(&expected_protected_head) {
                return Err(WorkflowError::RollbackDetected(
                    "local journal does not match its independently protected head".into(),
                ));
            }
            verify_or_advance_checkpoint(&path, &records, &state.workflow_id, file_len)?;
            let mut workflow = Self {
                path,
                records,
                events_by_id,
                state,
                file_len,
                protected_head_store,
                protected_head,
                exchange_order_owner_store,
            };
            drop(append_lock);
            workflow.reconcile_exchange_order_owner()?;
            workflow.reconcile_exchange_fill_owners()?;
            Ok(workflow)
        }
    }

    #[must_use]
    pub const fn state(&self) -> &WorkflowState {
        &self.state
    }

    /// Returns the currently pending order action only after revalidating it
    /// against the replayed durable workflow state.
    ///
    /// # Errors
    ///
    /// Returns an error unless the journal's current pending action is the
    /// deterministic order envelope authorized by this workflow binding.
    pub fn pending_prepared_order(&self) -> Result<&ExternalAction, WorkflowError> {
        let Some(action @ ExternalAction::SubmitOrder { .. }) = self.state.pending_action.as_ref()
        else {
            return Err(WorkflowError::InvalidTransition(
                "no journal-backed order action is pending".into(),
            ));
        };
        self.state.validate_prepared_action(action)?;
        Ok(action)
    }

    #[must_use]
    pub fn record_count(&self) -> usize {
        self.records.len()
    }

    fn exchange_order_owner(
        &self,
        exchange_order_id: &str,
    ) -> Result<ExchangeOrderOwner, WorkflowError> {
        Ok(ExchangeOrderOwner {
            schema_version: EXCHANGE_ORDER_OWNER_SCHEMA_VERSION,
            execution_identity_hash: self
                .state
                .binding
                .inventory_before
                .execution_identity_hash
                .clone(),
            exchange_order_id: exchange_order_id.to_owned(),
            decision_id: self.state.binding.decision_id.clone(),
            workflow_id: self.state.workflow_id().to_owned(),
            client_order_id: self.state.client_order_id(),
            canonical_order_envelope_hash: self.state.canonical_order_envelope_hash()?,
        })
    }

    fn claim_exchange_order(&self, exchange_order_id: &str) -> Result<bool, WorkflowError> {
        let owner = self.exchange_order_owner(exchange_order_id)?;
        self.exchange_order_owner_store
            .claim(&owner)
            .map_err(WorkflowError::exchange_order_owner)
    }

    fn exchange_fill_owner(
        &self,
        evidence: &OrderBoundEligibilityEvidence,
        fill: &BoundFillEvidence,
    ) -> ExchangeFillOwner {
        ExchangeFillOwner {
            schema_version: EXCHANGE_FILL_OWNER_SCHEMA_VERSION,
            execution_identity_hash: fill.execution_identity_hash.clone(),
            fill_id: fill.fill_id.clone(),
            decision_id: self.state.binding.decision_id.clone(),
            workflow_id: self.state.workflow_id().to_owned(),
            authorization_id: fill.authorization_id.clone(),
            authorization_record_hash: fill.authorization_record_hash.clone(),
            exchange_order_id: fill.order_id.clone(),
            client_order_id: fill.client_order_id.clone(),
            canonical_order_envelope_hash: evidence.canonical_order_envelope_hash.clone(),
        }
    }

    fn claim_exchange_fills(
        &self,
        evidence: &OrderBoundEligibilityEvidence,
    ) -> Result<bool, WorkflowError> {
        let owners = self.exchange_fill_owners(evidence);
        self.exchange_order_owner_store
            .claim_fills(&owners)
            .map_err(WorkflowError::exchange_fill_owner)
    }

    fn exchange_fill_owners(
        &self,
        evidence: &OrderBoundEligibilityEvidence,
    ) -> Vec<ExchangeFillOwner> {
        evidence
            .fills
            .iter()
            .map(|fill| self.exchange_fill_owner(evidence, fill))
            .collect()
    }

    fn reconcile_exchange_order_owner(&mut self) -> Result<(), WorkflowError> {
        let Some(exchange_order_id) = self.state.exchange_order_id().map(str::to_owned) else {
            return Ok(());
        };
        if self.claim_exchange_order(&exchange_order_id)?
            || self.state.stage == WorkflowStage::ManualReview
        {
            return Ok(());
        }
        self.mark_manual_review(
            EXCHANGE_ORDER_OWNER_CONFLICT_REASON,
            self.state.last_transition_at,
        )?;
        Err(WorkflowError::ContradictoryObservation(
            EXCHANGE_ORDER_OWNER_CONFLICT_REASON.into(),
        ))
    }

    fn reconcile_exchange_fill_owners(&mut self) -> Result<(), WorkflowError> {
        let evidence = self.records.iter().rev().find_map(|record| {
            if let WorkflowTransition::StakingEligibilityRecorded {
                evidence: Some(evidence),
                ..
            } = &record.event.transition
            {
                Some((**evidence).clone())
            } else {
                None
            }
        });
        let Some(evidence) = evidence else {
            return Ok(());
        };
        if self.claim_exchange_fills(&evidence)? || self.state.stage == WorkflowStage::ManualReview
        {
            return Ok(());
        }
        self.mark_manual_review(
            EXCHANGE_FILL_OWNER_CONFLICT_REASON,
            self.state.last_transition_at,
        )?;
        Err(WorkflowError::ContradictoryObservation(
            EXCHANGE_FILL_OWNER_CONFLICT_REASON.into(),
        ))
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
        evidence: &AuthenticatedOrderSubmission,
        recorded_at: DateTime<Utc>,
    ) -> Result<AppendOutcome, WorkflowError> {
        if evidence.accepted_at >= self.state.binding.order_envelope.signed_expiry_at {
            let reason = "order acceptance reached its signed expiry horizon";
            if self.state.stage != WorkflowStage::ManualReview {
                self.mark_manual_review(reason, recorded_at.max(self.state.last_transition_at))?;
            }
            return Err(WorkflowError::ContradictoryObservation(reason.into()));
        }
        let action_id = action_id_for(self.state.workflow_id(), ActionKind::SubmitOrder);
        let event_id = stable_id(
            "event/order_submission/v1",
            &[self.state.workflow_id(), &action_id],
        );
        let transition = WorkflowTransition::OrderSubmissionObserved {
            action_id,
            evidence: Box::new(evidence.clone()),
        };
        let candidate = WorkflowEvent {
            event_id: event_id.clone(),
            at: recorded_at,
            transition: transition.clone(),
        };
        if self.validate_append(&candidate).is_err() {
            return self.append_observation(event_id, recorded_at, transition);
        }
        let owner = self.exchange_order_owner(&evidence.exchange_order_id)?;
        let owner_store = Arc::clone(&self.exchange_order_owner_store);
        let mut append_result = None;
        let mut commit = || {
            let result = self.append_observation(event_id.clone(), recorded_at, transition.clone());
            let status = append_result_commit_status(&result);
            append_result = Some(result);
            status
        };
        let outcome = owner_store
            .claim_and_commit(&owner, &mut commit)
            .map_err(WorkflowError::exchange_order_owner)?;
        match outcome {
            OwnershipCommitOutcome::Committed
            | OwnershipCommitOutcome::CommitRejected
            | OwnershipCommitOutcome::CommitAmbiguous => append_result.ok_or_else(|| {
                WorkflowError::ExchangeOrderOwner(
                    "owner store did not invoke the journal commit".into(),
                )
            })?,
            OwnershipCommitOutcome::Conflict => {
                if self.state.stage != WorkflowStage::ManualReview {
                    self.mark_manual_review(
                        EXCHANGE_ORDER_OWNER_CONFLICT_REASON,
                        recorded_at.max(self.state.last_transition_at),
                    )?;
                }
                Err(WorkflowError::ContradictoryObservation(
                    EXCHANGE_ORDER_OWNER_CONFLICT_REASON.into(),
                ))
            }
        }
    }

    /// Records that authoritative post-expiry CLOID reconciliation found no
    /// accepted order, releasing the prepared intent as a zero-fill terminal
    /// outcome without allowing another submission.
    ///
    /// # Errors
    ///
    /// Returns an error unless the ledger clock and both gap-free authoritative
    /// history watermarks are strictly beyond the bound effective expiry.
    pub fn record_order_submission_absent(
        &mut self,
        evidence: ConclusiveAbsenceEvidence,
        at: DateTime<Utc>,
    ) -> Result<AppendOutcome, WorkflowError> {
        let action_id = action_id_for(self.state.workflow_id(), ActionKind::SubmitOrder);
        self.append_observation(
            stable_id(
                "event/order_submission_absent/v1",
                &[
                    self.state.workflow_id(),
                    &action_id,
                    &evidence.observation_id,
                ],
            ),
            at,
            WorkflowTransition::OrderSubmissionAbsent {
                action_id,
                evidence,
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
        let observation_id = observation_id.into().trim().to_owned();
        if observation_id.is_empty() {
            return Err(WorkflowError::InvalidTransition(
                "fill observation ID is empty".into(),
            ));
        }
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
    /// an eligible amount is audit information only and stays in spot. Every
    /// accepted order requires exact signer-side `order_bound` evidence and a
    /// canonical, timely fill set. Authoritative absence is the only case that
    /// permits `None` evidence.
    ///
    /// # Errors
    ///
    /// Returns an error unless the order is terminal, its authorization and
    /// fill evidence match, or the audit event cannot be persisted. Missing,
    /// late, or mismatched evidence durably stops the workflow for review.
    pub fn record_staking_eligibility(
        &mut self,
        evidence: Option<OrderBoundEligibilityEvidence>,
        at: DateTime<Utc>,
    ) -> Result<StakingEligibility, WorkflowError> {
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
        let eligibility_workflow_id = eligibility_workflow_id_for(
            self.state.workflow_id(),
            evidence.as_ref(),
            residual_hype,
            eligible_hype,
        )?;
        if matches!(
            self.state.stage,
            WorkflowStage::StakingEligibilityRecorded | WorkflowStage::Complete
        ) {
            if self.state.eligibility_workflow_id() == Some(&eligibility_workflow_id) {
                return Ok(self.state.staking_eligibility());
            }
            let reason =
                "eligibility evidence changed after its content-addressed workflow was recorded";
            self.mark_manual_review(reason, at.max(self.state.last_transition_at))?;
            return Err(WorkflowError::ContradictoryObservation(reason.into()));
        }
        if self.state.stage != WorkflowStage::OrderFinalized {
            return Err(WorkflowError::InvalidTransition(
                "staking eligibility requires a terminal order".into(),
            ));
        }
        if let Some(bound_evidence) = evidence.as_ref() {
            if let Err(error) = self
                .state
                .validate_eligibility_evidence(Some(bound_evidence), at)
            {
                if let WorkflowError::ContradictoryObservation(reason) = &error {
                    self.mark_manual_review(reason.clone(), at.max(self.state.last_transition_at))?;
                }
                return Err(error);
            }
        }
        let event_id = stable_id(
            "event/staking_eligibility/v2",
            &[self.state.workflow_id(), &eligibility_workflow_id],
        );
        let owners = evidence
            .as_ref()
            .map(|bound_evidence| self.exchange_fill_owners(bound_evidence));
        let transition = WorkflowTransition::StakingEligibilityRecorded {
            eligibility_workflow_id,
            evidence: evidence.map(Box::new),
            residual_hype,
            eligible_hype,
        };
        if let Some(owners) = owners {
            let owner_store = Arc::clone(&self.exchange_order_owner_store);
            let mut append_result = None;
            let mut commit = || {
                let result = self.append_observation(event_id.clone(), at, transition.clone());
                let status = append_result_commit_status(&result);
                append_result = Some(result);
                status
            };
            let outcome = owner_store
                .claim_fills_and_commit(&owners, &mut commit)
                .map_err(WorkflowError::exchange_fill_owner)?;
            match outcome {
                OwnershipCommitOutcome::Committed
                | OwnershipCommitOutcome::CommitRejected
                | OwnershipCommitOutcome::CommitAmbiguous => {
                    append_result.ok_or_else(|| {
                        WorkflowError::ExchangeFillOwner(
                            "owner store did not invoke the journal commit".into(),
                        )
                    })??;
                }
                OwnershipCommitOutcome::Conflict => {
                    if self.state.stage != WorkflowStage::ManualReview {
                        self.mark_manual_review(
                            EXCHANGE_FILL_OWNER_CONFLICT_REASON,
                            at.max(self.state.last_transition_at),
                        )?;
                    }
                    return Err(WorkflowError::ContradictoryObservation(
                        EXCHANGE_FILL_OWNER_CONFLICT_REASON.into(),
                    ));
                }
            }
        } else {
            self.append_observation(event_id, at, transition)?;
        }
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

    /// Persists a signer-free staking intent for fault-injection and replay.
    /// This method is absent from production/default builds and cannot submit
    /// an exchange action.
    ///
    /// # Errors
    ///
    /// Returns an error unless the decision carries the exact offline
    /// capability and staking eligibility has already been durably recorded.
    #[cfg(feature = "offline-staking-simulation")]
    pub fn prepare_offline_staking_deposit(
        &mut self,
        at: DateTime<Utc>,
    ) -> Result<PrepareOutcome, WorkflowError> {
        self.prepare_action(ActionKind::DepositToStaking, at)
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
        let action_id = action_id_for_state(&self.state, ActionKind::DepositToStaking)?;
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
        evidence: StakingBalanceConfirmation,
        observed_at: DateTime<Utc>,
    ) -> Result<AppendOutcome, WorkflowError> {
        let submitted_at = self.state.staking_submitted_at.ok_or_else(|| {
            WorkflowError::InvalidTransition("staking action is not submitted".into())
        })?;
        if observed_at < submitted_at {
            return Err(WorkflowError::StaleObservation);
        }
        let observation_id = evidence.observation_id.clone();
        self.append_observation(
            stable_id(
                "event/staking_balance/v1",
                &[self.state.workflow_id(), &observation_id],
            ),
            observed_at,
            WorkflowTransition::StakingBalanceConfirmed {
                evidence: Box::new(evidence),
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

    /// Persists a signer-free delegation intent for fault-injection and replay.
    /// This method is absent from production/default builds.
    ///
    /// # Errors
    ///
    /// Returns an error unless the exact simulated staking balance has already
    /// been authoritatively reconciled.
    #[cfg(feature = "offline-staking-simulation")]
    pub fn prepare_offline_delegation(
        &mut self,
        at: DateTime<Utc>,
    ) -> Result<PrepareOutcome, WorkflowError> {
        self.prepare_action(ActionKind::Delegate, at)
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
        let action_id = action_id_for_state(&self.state, ActionKind::Delegate)?;
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
        evidence: DelegatedBalanceConfirmation,
        observed_at: DateTime<Utc>,
    ) -> Result<AppendOutcome, WorkflowError> {
        let submitted_at = self.state.delegation_submitted_at.ok_or_else(|| {
            WorkflowError::InvalidTransition("delegation action is not submitted".into())
        })?;
        if observed_at < submitted_at {
            return Err(WorkflowError::StaleObservation);
        }
        let observation_id = evidence.observation_id.clone();
        self.append_observation(
            stable_id(
                "event/delegated_balance/v1",
                &[self.state.workflow_id(), &observation_id],
            ),
            observed_at,
            WorkflowTransition::DelegatedBalanceConfirmed {
                evidence: Box::new(evidence),
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
        if kind == ActionKind::SubmitOrder
            && (at >= self.state.binding.order_envelope.signed_expiry_at
                || at >= self.state.binding.order_envelope.effective_expiry_at)
        {
            return Err(WorkflowError::InvalidTransition(
                "order preparation is at or after its bound expiry".into(),
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
        let invalid_transition_contradiction = self
            .state
            .invalid_transition_contradiction_reason(&transition, at);
        let result = self.append_transition(event_id, at, transition);
        let reason = match &result {
            Err(WorkflowError::ContradictoryObservation(reason)) => Some(reason.clone()),
            Err(WorkflowError::EventCollision(event_id)) => {
                Some(format!("conflicting replay for event {event_id}"))
            }
            Err(WorkflowError::InvalidTransition(_)) => invalid_transition_contradiction,
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

    fn validate_append(&self, event: &WorkflowEvent) -> Result<(), WorkflowError> {
        if let Some(existing) = self.events_by_id.get(&event.event_id) {
            return if existing.transition == event.transition {
                Ok(())
            } else {
                Err(WorkflowError::EventCollision(event.event_id.clone()))
            };
        }
        let mut next_state = self.state.clone();
        next_state.apply(event)
    }

    fn append(&mut self, event: WorkflowEvent) -> Result<AppendOutcome, WorkflowError> {
        self.append_with_optional_lock(event, None)
    }

    fn append_while_locked(
        &mut self,
        event: WorkflowEvent,
        append_lock: &File,
    ) -> Result<AppendOutcome, WorkflowError> {
        self.append_with_optional_lock(event, Some(append_lock))
    }

    fn append_with_optional_lock(
        &mut self,
        event: WorkflowEvent,
        append_lock: Option<&File>,
    ) -> Result<AppendOutcome, WorkflowError> {
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
        if let Some(append_lock) = append_lock {
            self.write_record_while_locked(&record, append_lock)?;
        } else {
            self.write_record(&record)?;
        }
        self.events_by_id
            .insert(record.event.event_id.clone(), record.event.clone());
        self.records.push(record);
        self.state = next_state;
        Ok(AppendOutcome::Appended)
    }

    fn write_record(&mut self, record: &JournalRecord) -> Result<(), WorkflowError> {
        let append_lock = acquire_journal_append_lock(&self.path)?;
        self.write_record_while_locked(record, &append_lock)
    }

    fn write_record_while_locked(
        &mut self,
        record: &JournalRecord,
        _append_lock: &File,
    ) -> Result<(), WorkflowError> {
        let parent = normalized_parent(&self.path);
        fs::create_dir_all(parent).map_err(WorkflowError::io)?;
        let created = !self.path.exists();
        let current_len = fs::metadata(&self.path)
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        if current_len != self.file_len {
            return Err(WorkflowError::ConcurrentModification);
        }
        let current_protected_head = self
            .protected_head_store
            .load()
            .map_err(WorkflowError::protected_head)?;
        if current_protected_head != self.protected_head {
            return Err(WorkflowError::RollbackDetected(
                "protected workflow head changed since open".into(),
            ));
        }
        if let Some(last) = self.records.last() {
            verify_checkpoint_exact(&self.path, last, &self.state.workflow_id, self.file_len)?;
        } else {
            verify_bootstrap_checkpoint(&self.path, &self.state.workflow_id)?;
        }
        let mut line = serde_json::to_vec(record).map_err(WorkflowError::json)?;
        line.push(b'\n');
        let new_file_len = self
            .file_len
            .checked_add(
                u64::try_from(line.len())
                    .map_err(|_| WorkflowError::CorruptJournal("file length overflowed".into()))?,
            )
            .ok_or_else(|| WorkflowError::CorruptJournal("file length overflowed".into()))?;
        let next_protected_head = protected_head_for(record, &self.state.workflow_id, new_file_len);
        write_pending_append(
            &self.path,
            &PendingWorkflowAppend {
                schema_version: PENDING_APPEND_SCHEMA_VERSION,
                workflow_id: self.state.workflow_id.clone(),
                prior_head: self.protected_head.clone(),
                prior_journal_len: self.file_len,
                record: record.clone(),
                next_head: next_protected_head.clone(),
            },
        )?;
        // Protect the exact record hash before publishing mutable local state.
        // On an ambiguous response, recovery may materialize only that head.
        let advanced = self
            .protected_head_store
            .compare_and_swap(self.protected_head.as_ref(), &next_protected_head)
            .map_err(WorkflowError::protected_head)?;
        if !advanced {
            return Err(WorkflowError::RollbackDetected(
                "protected workflow head compare-and-swap failed".into(),
            ));
        }
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
        write_checkpoint(
            &self.path,
            &JournalCheckpoint {
                schema_version: CHECKPOINT_SCHEMA_VERSION,
                workflow_id: self.state.workflow_id.clone(),
                sequence: Some(record.sequence),
                record_hash: record.record_hash.clone(),
                journal_len: new_file_len,
            },
        )?;
        clear_pending_append(&self.path)?;
        self.protected_head = Some(next_protected_head);
        self.file_len = new_file_len;
        Ok(())
    }
}

fn external_action_for(
    state: &WorkflowState,
    kind: ActionKind,
) -> Result<ExternalAction, WorkflowError> {
    let action_id = action_id_for_state(state, kind)?;
    match kind {
        ActionKind::SubmitOrder if state.stage == WorkflowStage::Decided => {
            Ok(ExternalAction::SubmitOrder {
                action_id,
                client_order_id: client_order_id_for(state.workflow_id()),
                execution_identity_hash: state
                    .binding
                    .inventory_before
                    .execution_identity_hash
                    .clone(),
                signer_identity_hash: state.binding.order_envelope.signer_identity_hash.clone(),
                notional_usdc: state.binding.planned_usdc,
                max_debit_usdc: state.binding.committed_usdc,
                original_quantity_hype: state.binding.order_envelope.original_quantity_hype,
                hype_atoms_per_hype: state.binding.order_envelope.hype_atoms_per_hype,
                market_metadata_digest: state.binding.order_envelope.market_metadata_digest.clone(),
                limit_price_usdc_per_hype: state.binding.order_envelope.limit_price_usdc_per_hype,
                l1_nonce: state.binding.order_envelope.l1_nonce,
                signed_expiry_at: state.binding.order_envelope.signed_expiry_at,
            })
        }
        ActionKind::DepositToStaking
            if state.stage == WorkflowStage::StakingEligibilityRecorded =>
        {
            let eligibility_workflow_id = state.eligibility_workflow_id().ok_or_else(|| {
                WorkflowError::CorruptJournal("staking eligibility identity is missing".into())
            })?;
            let capability_binding_hash = state.offline_staking_capability_hash()?;
            let amount_hype = state.staking_eligible_hype;
            if amount_hype.is_zero() {
                return Err(WorkflowError::InvalidTransition(
                    "recorded eligibility has no HYPE to stake".into(),
                ));
            }
            Ok(ExternalAction::DepositToStaking {
                action_id,
                eligibility_workflow_id: eligibility_workflow_id.to_owned(),
                capability_binding_hash,
                amount_hype,
            })
        }
        ActionKind::Delegate if state.stage == WorkflowStage::StakingBalanceConfirmed => {
            let eligibility_workflow_id = state.eligibility_workflow_id().ok_or_else(|| {
                WorkflowError::CorruptJournal("staking eligibility identity is missing".into())
            })?;
            let capability = state.offline_staking_capability()?;
            Ok(ExternalAction::Delegate {
                action_id,
                eligibility_workflow_id: eligibility_workflow_id.to_owned(),
                capability_binding_hash: capability.digest()?,
                validator_address: capability.validator_address.clone(),
                validator_summary_evidence_hash: capability.validator_summary_evidence_hash.clone(),
                amount_hype: state.staking_target_hype,
            })
        }
        _ => Err(WorkflowError::InvalidTransition(
            "external action is invalid for current stage".into(),
        )),
    }
}

fn validate_receipt(receipt: &ExternalReceipt) -> Result<(), WorkflowError> {
    if matches!(
        receipt,
        ExternalReceipt::Confirmed { transaction_hash } if !lower_hex_digest(transaction_hash)
    ) {
        return Err(WorkflowError::ContradictoryObservation(
            "confirmed receipt lacks a canonical transaction hash".into(),
        ));
    }
    Ok(())
}

fn confirmation_transaction_matches(
    receipt_hash: &str,
    confirmed_transaction_hash: Option<&str>,
    matched_transaction_hash: &str,
) -> Result<bool, WorkflowError> {
    if !lower_hex_digest(matched_transaction_hash) {
        return Ok(false);
    }
    match confirmed_transaction_hash {
        Some(expected) => Ok(matched_transaction_hash == expected),
        None => Ok(receipt_hash == ExternalReceipt::Ambiguous.canonical_digest()?),
    }
}

fn valid_gap_free_watermark(
    watermark: &GapFreeHistoryWatermark,
    expected_domain: HistoryDomain,
    required_from_at: DateTime<Utc>,
    effective_expiry_at: DateTime<Utc>,
    recorded_at: DateTime<Utc>,
) -> bool {
    watermark.domain == expected_domain
        && canonical_nonempty(&watermark.watermark_id)
        && watermark.cursor > 0
        && canonical_nonempty(&watermark.evidence_hash)
        && watermark.gap_free_from_at <= required_from_at
        && watermark.through_at > effective_expiry_at
        && watermark.through_at <= recorded_at
}

fn canonical_nonempty(value: &str) -> bool {
    !value.is_empty() && value == value.trim()
}

fn lower_hex_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn canonical_ethereum_address(value: &str) -> bool {
    value.len() == 42
        && value.starts_with("0x")
        && value[2..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn independent_history_watermarks(
    left: &GapFreeHistoryWatermark,
    right: &GapFreeHistoryWatermark,
) -> bool {
    left.domain != right.domain
        && left.watermark_id != right.watermark_id
        && left.evidence_hash != right.evidence_hash
}

fn max_order_notional_usdc(envelope: &OrderEnvelopeBinding) -> Option<UsdcMicros> {
    let notional = max_fill_notional_usdc(envelope.original_quantity_hype, envelope)?;
    (!notional.is_zero()).then_some(notional)
}

fn max_fill_notional_usdc(
    cumulative_hype: HypeAtoms,
    envelope: &OrderEnvelopeBinding,
) -> Option<UsdcMicros> {
    let scale = u128::from(envelope.hype_atoms_per_hype);
    let rounding = scale.checked_sub(1)?;
    let numerator = u128::from(cumulative_hype.as_atoms())
        .checked_mul(u128::from(envelope.limit_price_usdc_per_hype.as_micros()))?;
    let rounded_micros = numerator.checked_add(rounding)?.checked_div(scale)?;
    let rounded_micros = u64::try_from(rounded_micros).ok()?;
    Some(UsdcMicros::from_micros(rounded_micros))
}

fn policy_time_delta(seconds: u64) -> Option<TimeDelta> {
    (seconds != 0)
        .then_some(seconds)
        .and_then(|seconds| i64::try_from(seconds).ok())
        .and_then(TimeDelta::try_seconds)
}

fn eligibility_policy_windows(
    policy: &EligibilityPolicyBinding,
) -> Result<(TimeDelta, TimeDelta), WorkflowError> {
    let registration_window = policy_time_delta(policy.fill_registration_deadline_seconds)
        .ok_or_else(|| {
            WorkflowError::ContradictoryObservation(
                "fill registration deadline must be positive and representable".into(),
            )
        })?;
    let lot_max_age =
        policy_time_delta(policy.lot_eligibility_max_age_seconds).ok_or_else(|| {
            WorkflowError::ContradictoryObservation(
                "lot eligibility maximum age must be positive and representable".into(),
            )
        })?;
    Ok((registration_window, lot_max_age))
}

fn checked_fill_totals(
    purchased: u64,
    executed_notional: u64,
    fill: &BoundFillEvidence,
) -> Result<(u64, u64), WorkflowError> {
    let purchased = purchased
        .checked_add(fill.purchased_hype.as_atoms())
        .ok_or_else(|| {
            WorkflowError::ContradictoryObservation("authorized fill quantity overflowed".into())
        })?;
    let executed_notional = executed_notional
        .checked_add(fill.executed_notional_usdc.as_micros())
        .ok_or_else(|| {
            WorkflowError::ContradictoryObservation("authorized fill notional overflowed".into())
        })?;
    Ok((purchased, executed_notional))
}

fn residual_hype_available_before(
    fills: &[BoundFillEvidence],
    residual_reservation: HypeAtoms,
    occurred_at: DateTime<Utc>,
) -> Option<u64> {
    let mut residual_remaining = residual_reservation.as_atoms();
    let mut available = 0_u64;
    for fill in fills {
        if fill.executed_at >= occurred_at {
            break;
        }
        let residual_for_fill = residual_remaining.min(fill.purchased_hype.as_atoms());
        available = available.checked_add(residual_for_fill)?;
        residual_remaining = residual_remaining.checked_sub(residual_for_fill)?;
    }
    Some(available)
}

fn valid_eligibility_history_watermark(
    watermark: &GapFreeHistoryWatermark,
    expected_domain: HistoryDomain,
    required_from_at: DateTime<Utc>,
    recorded_at: DateTime<Utc>,
) -> bool {
    watermark.domain == expected_domain
        && canonical_nonempty(&watermark.watermark_id)
        && watermark.cursor > 0
        && canonical_nonempty(&watermark.evidence_hash)
        && watermark.gap_free_from_at <= required_from_at
        && watermark.through_at == recorded_at
}

fn valid_expiry_binding(envelope: &OrderEnvelopeBinding, decided_at: DateTime<Utc>) -> bool {
    let Ok(lag_ms) = i64::try_from(envelope.max_venue_clock_lag_ms) else {
        return false;
    };
    let Some(offset_ms) = lag_ms.checked_add(1) else {
        return false;
    };
    let Some(offset) = TimeDelta::try_milliseconds(offset_ms) else {
        return false;
    };
    let Some(expected_signed_expiry_at) = envelope.effective_expiry_at.checked_sub_signed(offset)
    else {
        return false;
    };

    envelope.max_venue_clock_lag_ms > 0
        && !envelope.venue_clock_evidence_digest.trim().is_empty()
        && envelope.venue_clock_evidence_at <= decided_at
        && envelope.venue_clock_evidence_valid_through_at > envelope.effective_expiry_at
        && envelope.effective_expiry_at <= envelope.input_freshness.earliest_deadline()
        && envelope
            .venue_clock_evidence_valid_through_at
            .timestamp_subsec_nanos()
            % 1_000_000
            == 0
        && envelope.signed_expiry_at.timestamp_subsec_nanos() % 1_000_000 == 0
        && envelope.effective_expiry_at.timestamp_subsec_nanos() % 1_000_000 == 0
        && expected_signed_expiry_at.timestamp_millis() > 0
        && envelope.signed_expiry_at == expected_signed_expiry_at
        && envelope.signed_expiry_at > decided_at
}

fn workflow_id_for(binding: &DecisionBinding) -> String {
    format!(
        "wf_{}",
        stable_id("workflow/decision/v2", &[binding.decision_id.as_str()])
    )
}

#[derive(Serialize)]
struct CanonicalOrderEnvelope<'a> {
    execution_identity_hash: &'a str,
    signer_identity_hash: &'a str,
    decision_id: &'a str,
    client_order_id: String,
    planned_usdc: UsdcMicros,
    max_debit_usdc: UsdcMicros,
    original_quantity_hype: HypeAtoms,
    hype_atoms_per_hype: u64,
    market_metadata_digest: &'a str,
    limit_price_usdc_per_hype: UsdcMicros,
    l1_nonce: u64,
    signed_expiry_at: DateTime<Utc>,
    effective_expiry_at: DateTime<Utc>,
    venue_clock_evidence_at: DateTime<Utc>,
    venue_clock_evidence_valid_through_at: DateTime<Utc>,
    venue_clock_evidence_digest: &'a str,
    max_venue_clock_lag_ms: u64,
    input_freshness: &'a AuthorizationInputFreshness,
    market: &'static str,
    side: &'static str,
    time_in_force: &'static str,
}

fn canonical_order_envelope_hash(state: &WorkflowState) -> Result<String, WorkflowError> {
    let encoded = serde_json::to_vec(&CanonicalOrderEnvelope {
        execution_identity_hash: &state.binding.inventory_before.execution_identity_hash,
        signer_identity_hash: &state.binding.order_envelope.signer_identity_hash,
        decision_id: &state.binding.decision_id,
        client_order_id: client_order_id_for(&state.workflow_id),
        planned_usdc: state.binding.planned_usdc,
        max_debit_usdc: state.binding.committed_usdc,
        original_quantity_hype: state.binding.order_envelope.original_quantity_hype,
        hype_atoms_per_hype: state.binding.order_envelope.hype_atoms_per_hype,
        market_metadata_digest: &state.binding.order_envelope.market_metadata_digest,
        limit_price_usdc_per_hype: state.binding.order_envelope.limit_price_usdc_per_hype,
        l1_nonce: state.binding.order_envelope.l1_nonce,
        signed_expiry_at: state.binding.order_envelope.signed_expiry_at,
        effective_expiry_at: state.binding.order_envelope.effective_expiry_at,
        venue_clock_evidence_at: state.binding.order_envelope.venue_clock_evidence_at,
        venue_clock_evidence_valid_through_at: state
            .binding
            .order_envelope
            .venue_clock_evidence_valid_through_at,
        venue_clock_evidence_digest: &state.binding.order_envelope.venue_clock_evidence_digest,
        max_venue_clock_lag_ms: state.binding.order_envelope.max_venue_clock_lag_ms,
        input_freshness: &state.binding.order_envelope.input_freshness,
        market: "HYPE/USDC",
        side: "buy",
        time_in_force: "IOC",
    })
    .map_err(WorkflowError::json)?;
    Ok(digest_hex(&encoded))
}

#[derive(Serialize)]
struct EligibilityWorkflowInput<'a> {
    execution_workflow_id: &'a str,
    evidence: Option<&'a OrderBoundEligibilityEvidence>,
    residual_hype: HypeAtoms,
    eligible_hype: HypeAtoms,
}

fn eligibility_workflow_id_for(
    execution_workflow_id: &str,
    evidence: Option<&OrderBoundEligibilityEvidence>,
    residual_hype: HypeAtoms,
    eligible_hype: HypeAtoms,
) -> Result<String, WorkflowError> {
    let encoded = serde_json::to_vec(&EligibilityWorkflowInput {
        execution_workflow_id,
        evidence,
        residual_hype,
        eligible_hype,
    })
    .map_err(WorkflowError::json)?;
    Ok(format!("eligibility_wf_{}", digest_hex(&encoded)))
}

fn action_id_for(workflow_id: &str, kind: ActionKind) -> String {
    let kind = match kind {
        ActionKind::SubmitOrder => "submit_order",
        ActionKind::DepositToStaking => "deposit_to_staking",
        ActionKind::Delegate => "delegate",
    };
    stable_id("action/v1", &[workflow_id, kind])
}

fn action_id_for_state(state: &WorkflowState, kind: ActionKind) -> Result<String, WorkflowError> {
    if kind == ActionKind::SubmitOrder {
        return Ok(action_id_for(state.workflow_id(), kind));
    }
    let eligibility_workflow_id = state.eligibility_workflow_id().ok_or_else(|| {
        WorkflowError::InvalidTransition(
            "staking action requires a content-addressed eligibility workflow".into(),
        )
    })?;
    let capability_binding_hash = state.offline_staking_capability_hash()?;
    let kind = match kind {
        ActionKind::DepositToStaking => "deposit_to_staking",
        ActionKind::Delegate => "delegate",
        ActionKind::SubmitOrder => unreachable!("submit order returned above"),
    };
    Ok(stable_id(
        "action/staking/v2",
        &[
            state.workflow_id(),
            eligibility_workflow_id,
            &capability_binding_hash,
            kind,
        ],
    ))
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
            evidence,
        } => stable_id(
            "event/order_submission_absent/v1",
            &[workflow_id, action_id, &evidence.observation_id],
        ),
        WorkflowTransition::OrderFillObserved { observation_id, .. } => {
            stable_id("event/order_fill/v1", &[workflow_id, observation_id])
        }
        WorkflowTransition::OrderFinalized { action_id, .. } => {
            stable_id("event/order_finalized/v1", &[workflow_id, action_id])
        }
        WorkflowTransition::StakingEligibilityRecorded {
            eligibility_workflow_id,
            ..
        } => stable_id(
            "event/staking_eligibility/v2",
            &[workflow_id, eligibility_workflow_id],
        ),
        WorkflowTransition::StakingDepositObserved { action_id, .. } => {
            stable_id("event/staking_submitted/v1", &[workflow_id, action_id])
        }
        WorkflowTransition::StakingBalanceConfirmed { evidence } => stable_id(
            "event/staking_balance/v1",
            &[workflow_id, &evidence.observation_id],
        ),
        WorkflowTransition::DelegationObserved { action_id, .. } => {
            stable_id("event/delegation_submitted/v1", &[workflow_id, action_id])
        }
        WorkflowTransition::DelegatedBalanceConfirmed { evidence } => stable_id(
            "event/delegated_balance/v1",
            &[workflow_id, &evidence.observation_id],
        ),
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

fn checkpoint_path(path: &Path) -> PathBuf {
    let mut checkpoint = path.as_os_str().to_os_string();
    checkpoint.push(".head");
    PathBuf::from(checkpoint)
}

fn checkpoint_temp_path(path: &Path) -> PathBuf {
    let mut checkpoint = path.as_os_str().to_os_string();
    checkpoint.push(".head.tmp");
    PathBuf::from(checkpoint)
}

fn pending_append_path(path: &Path) -> PathBuf {
    let mut pending = path.as_os_str().to_os_string();
    pending.push(".pending-append.json");
    PathBuf::from(pending)
}

fn journal_append_lock_path(path: &Path) -> PathBuf {
    let mut lock = path.as_os_str().to_os_string();
    lock.push(".append.lock");
    PathBuf::from(lock)
}

fn acquire_journal_append_lock(path: &Path) -> Result<File, WorkflowError> {
    fs::create_dir_all(normalized_parent(path)).map_err(WorkflowError::io)?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(journal_append_lock_path(path))
        .map_err(WorkflowError::io)?;
    match lock.try_lock_exclusive() {
        Ok(()) => Ok(lock),
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
            Err(WorkflowError::ConcurrentModification)
        }
        Err(error) => Err(WorkflowError::io(error)),
    }
}

fn pending_append_temp_path(path: &Path) -> PathBuf {
    let mut pending = path.as_os_str().to_os_string();
    pending.push(".pending-append.tmp");
    PathBuf::from(pending)
}

fn write_pending_append(path: &Path, pending: &PendingWorkflowAppend) -> Result<(), WorkflowError> {
    let parent = normalized_parent(path);
    fs::create_dir_all(parent).map_err(WorkflowError::io)?;
    let pending_path = pending_append_path(path);
    let temp_path = pending_append_temp_path(path);
    let mut encoded = serde_json::to_vec(pending).map_err(WorkflowError::json)?;
    encoded.push(b'\n');
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temp_path)
        .map_err(WorkflowError::io)?;
    file.write_all(&encoded).map_err(WorkflowError::io)?;
    file.sync_all().map_err(WorkflowError::io)?;
    fs::rename(temp_path, pending_path).map_err(WorkflowError::io)?;
    sync_parent(path)
}

fn read_pending_append(path: &Path) -> Result<Option<PendingWorkflowAppend>, WorkflowError> {
    let pending_path = pending_append_path(path);
    let payload = match fs::read(pending_path) {
        Ok(payload) => payload,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(WorkflowError::io(error)),
    };
    let encoded = payload.strip_suffix(b"\n").ok_or_else(|| {
        WorkflowError::RollbackDetected("pending workflow append is incomplete".into())
    })?;
    serde_json::from_slice(encoded).map(Some).map_err(|error| {
        WorkflowError::RollbackDetected(format!("pending workflow append is malformed: {error}"))
    })
}

fn clear_pending_append(path: &Path) -> Result<(), WorkflowError> {
    match fs::remove_file(pending_append_path(path)) {
        Ok(()) => sync_parent(path),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(WorkflowError::io(error)),
    }
}

fn sync_parent(path: &Path) -> Result<(), WorkflowError> {
    File::open(normalized_parent(path))
        .and_then(|directory| directory.sync_all())
        .map_err(WorkflowError::io)
}

fn recover_pending_append(
    path: &Path,
    workflow_id: &str,
    protected_head_store: &dyn ProtectedWorkflowHeadStore,
) -> Result<(), WorkflowError> {
    let Some(pending) = read_pending_append(path)? else {
        return Ok(());
    };
    validate_pending_append(&pending, workflow_id)?;
    let mut line = serde_json::to_vec(&pending.record).map_err(WorkflowError::json)?;
    line.push(b'\n');
    let payload = match fs::read(path) {
        Ok(payload) => payload,
        Err(error) if error.kind() == io::ErrorKind::NotFound => Vec::new(),
        Err(error) => return Err(WorkflowError::io(error)),
    };
    let prior_len = usize::try_from(pending.prior_journal_len).map_err(|_| {
        WorkflowError::RollbackDetected("pending prior length is out of range".into())
    })?;
    let Some(tail) = payload.get(prior_len..) else {
        return Err(WorkflowError::RollbackDetected(
            "journal is shorter than its pending append prefix".into(),
        ));
    };
    let actual_head = protected_head_store
        .load()
        .map_err(WorkflowError::protected_head)?;
    if actual_head == pending.prior_head {
        if !tail.is_empty() {
            return Err(WorkflowError::RollbackDetected(
                "unprotected journal tail exists beside an uncommitted pending append".into(),
            ));
        }
        write_checkpoint(
            path,
            &checkpoint_for_head(workflow_id, pending.prior_head.as_ref()),
        )?;
        return clear_pending_append(path);
    }
    if actual_head.as_ref() != Some(&pending.next_head) {
        return Err(WorkflowError::RollbackDetected(
            "pending append does not match the protected workflow head".into(),
        ));
    }
    if tail == line {
        OpenOptions::new()
            .write(true)
            .open(path)
            .and_then(|file| file.sync_all())
            .map_err(WorkflowError::io)?;
    } else {
        if !line.starts_with(tail) {
            return Err(WorkflowError::RollbackDetected(
                "journal tail does not match its protected pending append".into(),
            ));
        }
        if !tail.is_empty() {
            let file = OpenOptions::new()
                .write(true)
                .open(path)
                .map_err(WorkflowError::io)?;
            file.set_len(pending.prior_journal_len)
                .map_err(WorkflowError::io)?;
            file.sync_all().map_err(WorkflowError::io)?;
        }
        append_pending_line(path, &line)?;
    }
    write_checkpoint(
        path,
        &checkpoint_for_head(workflow_id, Some(&pending.next_head)),
    )?;
    clear_pending_append(path)
}

fn append_pending_line(path: &Path, line: &[u8]) -> Result<(), WorkflowError> {
    let created = !path.exists();
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(WorkflowError::io)?;
    file.write_all(line).map_err(WorkflowError::io)?;
    file.sync_all().map_err(WorkflowError::io)?;
    if created {
        sync_parent(path)?;
    }
    Ok(())
}

fn validate_pending_append(
    pending: &PendingWorkflowAppend,
    workflow_id: &str,
) -> Result<(), WorkflowError> {
    let expected_sequence = pending
        .prior_head
        .as_ref()
        .map_or(Some(0), |head| head.sequence.checked_add(1));
    let expected_previous_hash = pending
        .prior_head
        .as_ref()
        .map_or("", |head| head.record_hash.as_str());
    let mut line = serde_json::to_vec(&pending.record).map_err(WorkflowError::json)?;
    line.push(b'\n');
    let expected_len = pending
        .prior_journal_len
        .checked_add(u64::try_from(line.len()).map_err(|_| {
            WorkflowError::RollbackDetected("pending record length is out of range".into())
        })?)
        .ok_or_else(|| WorkflowError::RollbackDetected("pending length overflowed".into()))?;
    let expected_next = protected_head_for(&pending.record, workflow_id, expected_len);
    if pending.schema_version != PENDING_APPEND_SCHEMA_VERSION
        || pending.workflow_id != workflow_id
        || pending.record.schema_version != JOURNAL_SCHEMA_VERSION
        || Some(pending.record.sequence) != expected_sequence
        || pending.record.previous_hash != expected_previous_hash
        || pending.record.record_hash
            != record_hash(
                pending.record.sequence,
                &pending.record.previous_hash,
                &pending.record.event,
            )?
        || pending.prior_head.as_ref().is_some_and(|head| {
            head.schema_version != PROTECTED_HEAD_SCHEMA_VERSION
                || head.workflow_id != workflow_id
                || head.journal_len != pending.prior_journal_len
        })
        || (pending.prior_head.is_none() && pending.prior_journal_len != 0)
        || pending.next_head != expected_next
    {
        return Err(WorkflowError::RollbackDetected(
            "pending workflow append is inconsistent".into(),
        ));
    }
    validate_event_id(workflow_id, &pending.record.event)
}

fn checkpoint_for_head(
    workflow_id: &str,
    head: Option<&ProtectedWorkflowHead>,
) -> JournalCheckpoint {
    head.map_or_else(
        || bootstrap_checkpoint(workflow_id),
        |head| JournalCheckpoint {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            workflow_id: workflow_id.to_owned(),
            sequence: Some(head.sequence),
            record_hash: head.record_hash.clone(),
            journal_len: head.journal_len,
        },
    )
}

fn read_checkpoint(path: &Path) -> Result<JournalCheckpoint, WorkflowError> {
    let payload = fs::read(checkpoint_path(path)).map_err(|error| {
        WorkflowError::RollbackDetected(format!("durable head is unavailable: {error}"))
    })?;
    let encoded = payload
        .strip_suffix(b"\n")
        .ok_or_else(|| WorkflowError::RollbackDetected("durable head is incomplete".into()))?;
    serde_json::from_slice(encoded).map_err(|error| {
        WorkflowError::RollbackDetected(format!("durable head is malformed: {error}"))
    })
}

fn write_checkpoint(path: &Path, checkpoint: &JournalCheckpoint) -> Result<(), WorkflowError> {
    let parent = normalized_parent(path);
    fs::create_dir_all(parent).map_err(WorkflowError::io)?;
    let checkpoint_path = checkpoint_path(path);
    let temp_path = checkpoint_temp_path(path);
    let mut encoded = serde_json::to_vec(checkpoint).map_err(WorkflowError::json)?;
    encoded.push(b'\n');
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temp_path)
        .map_err(WorkflowError::io)?;
    file.write_all(&encoded).map_err(WorkflowError::io)?;
    file.sync_all().map_err(WorkflowError::io)?;
    fs::rename(&temp_path, &checkpoint_path).map_err(WorkflowError::io)?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(WorkflowError::io)
}

fn bootstrap_checkpoint(workflow_id: &str) -> JournalCheckpoint {
    JournalCheckpoint {
        schema_version: CHECKPOINT_SCHEMA_VERSION,
        workflow_id: workflow_id.to_owned(),
        sequence: None,
        record_hash: String::new(),
        journal_len: 0,
    }
}

fn protected_head_for(
    record: &JournalRecord,
    workflow_id: &str,
    journal_len: u64,
) -> ProtectedWorkflowHead {
    ProtectedWorkflowHead {
        schema_version: PROTECTED_HEAD_SCHEMA_VERSION,
        workflow_id: workflow_id.to_owned(),
        sequence: record.sequence,
        record_hash: record.record_hash.clone(),
        journal_len,
    }
}

fn verify_bootstrap_checkpoint(path: &Path, workflow_id: &str) -> Result<(), WorkflowError> {
    if read_checkpoint(path)? == bootstrap_checkpoint(workflow_id) {
        Ok(())
    } else {
        Err(WorkflowError::RollbackDetected(
            "empty journal does not match its bootstrap durable head".into(),
        ))
    }
}

fn journal_len_through(records: &[JournalRecord], sequence: usize) -> Result<u64, WorkflowError> {
    let mut len = 0_u64;
    for record in records.iter().take(sequence.saturating_add(1)) {
        let encoded = serde_json::to_vec(record).map_err(WorkflowError::json)?;
        len = len
            .checked_add(u64::try_from(encoded.len().saturating_add(1)).map_err(|_| {
                WorkflowError::CorruptJournal("journal prefix length overflowed".into())
            })?)
            .ok_or_else(|| {
                WorkflowError::CorruptJournal("journal prefix length overflowed".into())
            })?;
    }
    Ok(len)
}

fn verify_checkpoint_exact(
    path: &Path,
    last: &JournalRecord,
    workflow_id: &str,
    journal_len: u64,
) -> Result<(), WorkflowError> {
    let checkpoint = read_checkpoint(path)?;
    if checkpoint.schema_version != CHECKPOINT_SCHEMA_VERSION
        || checkpoint.workflow_id != workflow_id
        || checkpoint.sequence != Some(last.sequence)
        || checkpoint.record_hash != last.record_hash
        || checkpoint.journal_len != journal_len
    {
        return Err(WorkflowError::RollbackDetected(
            "journal and durable head disagree".into(),
        ));
    }
    Ok(())
}

fn verify_or_advance_checkpoint(
    path: &Path,
    records: &[JournalRecord],
    workflow_id: &str,
    journal_len: u64,
) -> Result<(), WorkflowError> {
    let last = records.last().ok_or_else(|| {
        WorkflowError::CorruptJournal("cannot verify a head for an empty journal".into())
    })?;
    let checkpoint = read_checkpoint(path)?;
    if checkpoint.schema_version != CHECKPOINT_SCHEMA_VERSION
        || checkpoint.workflow_id != workflow_id
    {
        return Err(WorkflowError::RollbackDetected(
            "journal identity does not match its durable head".into(),
        ));
    }
    let Some(checkpoint_sequence) = checkpoint.sequence else {
        if checkpoint != bootstrap_checkpoint(workflow_id)
            || records.len() != 1
            || last.sequence != 0
        {
            return Err(WorkflowError::RollbackDetected(
                "initial journal does not match its bootstrap durable head".into(),
            ));
        }
        return write_checkpoint(
            path,
            &JournalCheckpoint {
                schema_version: CHECKPOINT_SCHEMA_VERSION,
                workflow_id: workflow_id.to_owned(),
                sequence: Some(last.sequence),
                record_hash: last.record_hash.clone(),
                journal_len,
            },
        );
    };
    let sequence = usize::try_from(checkpoint_sequence).map_err(|_| {
        WorkflowError::RollbackDetected("durable head sequence is out of range".into())
    })?;
    let Some(checkpoint_record) = records.get(sequence) else {
        return Err(WorkflowError::RollbackDetected(
            "journal rolled back behind its durable head".into(),
        ));
    };
    let expected_prefix_len = journal_len_through(records, sequence)?;
    if checkpoint.record_hash != checkpoint_record.record_hash
        || checkpoint.journal_len != expected_prefix_len
    {
        return Err(WorkflowError::RollbackDetected(
            "journal prefix does not match its durable head".into(),
        ));
    }
    if checkpoint.sequence == Some(last.sequence) {
        if checkpoint.journal_len != journal_len {
            return Err(WorkflowError::RollbackDetected(
                "journal length does not match its durable head".into(),
            ));
        }
        return Ok(());
    }
    write_checkpoint(
        path,
        &JournalCheckpoint {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            workflow_id: workflow_id.to_owned(),
            sequence: Some(last.sequence),
            record_hash: last.record_hash.clone(),
            journal_len,
        },
    )
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
    #[error("journal rollback detected: {0}")]
    RollbackDetected(String),
    #[error("protected workflow head failed: {0}")]
    ProtectedHead(String),
    #[error("exchange order owner store failed: {0}")]
    ExchangeOrderOwner(String),
    #[error("exchange fill owner store failed: {0}")]
    ExchangeFillOwner(String),
    #[error("journal I/O failed: {0}")]
    Io(String),
    #[error("journal serialization failed: {0}")]
    Json(String),
}

fn append_result_commit_status(
    result: &Result<AppendOutcome, WorkflowError>,
) -> JournalCommitStatus {
    match result {
        Ok(_) => JournalCommitStatus::Committed,
        Err(error) => error.journal_commit_status(),
    }
}

impl WorkflowError {
    fn journal_commit_status(&self) -> JournalCommitStatus {
        match self {
            Self::ProtectedHead(_) | Self::Io(_) => JournalCommitStatus::Ambiguous,
            _ => JournalCommitStatus::Rejected,
        }
    }

    #[allow(clippy::needless_pass_by_value)]
    fn io(error: io::Error) -> Self {
        Self::Io(error.to_string())
    }

    #[allow(clippy::needless_pass_by_value)]
    fn json(error: serde_json::Error) -> Self {
        Self::Json(error.to_string())
    }

    #[allow(clippy::needless_pass_by_value)]
    fn protected_head(error: String) -> Self {
        Self::ProtectedHead(error)
    }

    #[allow(clippy::needless_pass_by_value)]
    fn exchange_order_owner(error: String) -> Self {
        Self::ExchangeOrderOwner(error)
    }

    #[allow(clippy::needless_pass_by_value)]
    fn exchange_fill_owner(error: String) -> Self {
        Self::ExchangeFillOwner(error)
    }
}
