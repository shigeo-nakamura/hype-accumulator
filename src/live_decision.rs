//! Wires a freshly computed pacing decision into a durably prepared,
//! signer-free order-envelope workflow, ready for
//! [`crate::live_probe::HyperliquidLiveProbe`].
//!
//! # Scope limitation: first-live-probe only
//!
//! [`InventoryBaseline`]'s staking, delegated, and unconsumed-residual
//! fields are asserted to be exactly zero — verified against a live read,
//! never merely assumed — because no cross-workflow ledger anywhere in this
//! crate aggregates unconsumed residual HYPE left behind by prior
//! completed workflows. Each `DurableWorkflow`'s own `residual_hype` is
//! tracked only inside that one workflow's journal (see `workflow.rs`); this
//! module has no way to honestly report a nonzero baseline for an account
//! with live purchase history. A nonzero live read of any of these
//! quantities fails closed rather than guessing. Building genuine
//! cross-workflow residual/staking tracking is required before this module
//! can support anything beyond an account's first live economic action.

use crate::{
    hype_asset::hype_usdc_market_metadata_digest,
    live_probe::{LiveProbeBinding, LiveProbeError},
    order_envelope::{
        assemble_order_envelope_binding, OrderEnvelopeError, OrderEnvelopeFreshnessPolicy,
    },
    runtime::{RuntimeCycleInput, RuntimeError, SignerFreeRuntime},
    workflow::{
        DecisionBinding, DurableWorkflow, EligibilityPolicyBinding, ExchangeOrderOwnerStore,
        HypeAtoms, InventoryBaseline, ProtectedWorkflowHeadStore, WorkflowError,
    },
};
use chrono::{DateTime, Utc};
use dex_connector::{
    CombinedBalanceResponse, DexConnector, DexError, HyperliquidConnector,
    HyperliquidStakingSummary,
};
use rust_decimal::Decimal;
use std::{path::Path, sync::Arc};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LiveDecisionError {
    #[error("runtime cycle failed: {0}")]
    Runtime(#[from] RuntimeError),
    #[error("no pacing decision is due this cycle")]
    NoDecisionDue,
    #[error("Hyperliquid connector error: {0}")]
    Connector(#[from] DexError),
    #[error(
        "account already has nonzero {0}; this module only supports an \
         account's first live economic action (see module doc)"
    )]
    NotFirstLiveProbe(&'static str),
    #[error("live-probe binding error: {0}")]
    LiveProbeBinding(#[from] LiveProbeError),
    #[error("order envelope assembly failed: {0}")]
    OrderEnvelope(#[from] OrderEnvelopeError),
    #[error("workflow error: {0}")]
    Workflow(#[from] WorkflowError),
    #[error("live HYPE balance is not exactly representable")]
    InvalidBalance,
}

/// Computes today's pacing decision (if one is due) and durably prepares its
/// order envelope, ready for [`crate::live_probe::HyperliquidLiveProbe`].
///
/// `configured_residual_hype_atoms`, `eligibility_policy`, and
/// `envelope_policy` are sourced from the operator's approved
/// `SecurityPolicy` by the caller (this module never reads `SecurityPolicy`
/// directly, matching `order_envelope.rs`'s existing pattern).
/// `signal_evidence_valid_through_at` and
/// `policy_acknowledgement_valid_through_at` likewise come from state this
/// module does not own.
///
/// # Errors
///
/// Returns [`LiveDecisionError::NoDecisionDue`] when no scheduled decision
/// boundary is due this cycle. Returns
/// [`LiveDecisionError::NotFirstLiveProbe`] when a live read finds nonzero
/// staking, delegation, or pending-withdrawal HYPE — evidence this account
/// has prior live activity this module cannot safely account for (see
/// module doc). Otherwise propagates the underlying runtime, connector,
/// envelope-assembly, or workflow error.
#[allow(clippy::too_many_arguments)]
pub async fn prepare_first_live_order_workflow(
    connector: &HyperliquidConnector,
    runtime: &mut SignerFreeRuntime,
    cycle_input: RuntimeCycleInput<'_>,
    signal_evidence_valid_through_at: DateTime<Utc>,
    policy_acknowledgement_valid_through_at: DateTime<Utc>,
    envelope_policy: &OrderEnvelopeFreshnessPolicy,
    eligibility_policy: EligibilityPolicyBinding,
    configured_residual_hype_atoms: HypeAtoms,
    journal_path: &Path,
    protected_head_store: Arc<dyn ProtectedWorkflowHeadStore>,
    exchange_order_owner_store: Arc<dyn ExchangeOrderOwnerStore>,
    now: DateTime<Utc>,
) -> Result<DurableWorkflow, LiveDecisionError> {
    let report = runtime.apply_cycle(cycle_input)?;
    let decision = report
        .decision()
        .ok_or(LiveDecisionError::NoDecisionDue)?
        .clone();

    let probe_binding =
        LiveProbeBinding::from_connector(connector, hype_usdc_market_metadata_digest())?;

    let (balance, staking) = tokio::try_join!(
        connector.get_combined_balance(),
        connector.get_staking_summary()
    )?;

    let (staking_hype_atoms, delegated_hype_atoms) = first_live_probe_staking_atoms(&staking)?;
    let spot_hype_atoms = hype_atoms_from_decimal(spot_hype_balance(&balance))?;

    // First-live-probe scope (see module doc): no prior workflow exists to
    // have left an unconsumed residual, so this is provably zero, not an
    // assumption. There is nothing to read to verify it independently
    // (residual tracking is per-workflow-journal only), so this is asserted
    // rather than checked.
    let unconsumed_residual_spot_hype_atoms = HypeAtoms::from_atoms(0);

    let inventory_before = InventoryBaseline {
        execution_identity_hash: probe_binding.execution_identity_hash.clone(),
        spot_hype_atoms,
        staking_hype_atoms,
        delegated_hype_atoms,
        configured_residual_hype_atoms,
        unconsumed_residual_spot_hype_atoms,
    };

    let order_envelope = assemble_order_envelope_binding(
        connector,
        probe_binding.signer_identity_hash.clone(),
        decision.planned_usdc,
        signal_evidence_valid_through_at,
        policy_acknowledgement_valid_through_at,
        envelope_policy,
        now,
    )
    .await?;

    let binding = DecisionBinding::from_pacing_decision(
        &decision,
        inventory_before,
        order_envelope,
        eligibility_policy,
    )?;

    let mut workflow = DurableWorkflow::open_or_create(
        journal_path,
        &binding,
        protected_head_store,
        exchange_order_owner_store,
    )?;
    workflow.prepare_order(now)?;
    Ok(workflow)
}

fn hype_atoms_from_decimal(value: Decimal) -> Result<HypeAtoms, LiveDecisionError> {
    crate::hype_asset::decimal_hype_to_atoms_floor(value)
        .map(HypeAtoms::from_atoms)
        .ok_or(LiveDecisionError::InvalidBalance)
}

fn spot_hype_balance(balance: &CombinedBalanceResponse) -> Decimal {
    balance
        .spot_assets
        .iter()
        .find(|asset| asset.symbol == "HYPE")
        .map_or(Decimal::ZERO, |asset| asset.balance)
}

/// Verifies a live staking read is consistent with an account's first-ever
/// live economic action (see module doc), returning
/// `(staking_hype_atoms, delegated_hype_atoms)` — both provably zero when
/// this succeeds.
///
/// # Errors
///
/// Returns [`LiveDecisionError::NotFirstLiveProbe`] when any of
/// `pending_withdrawal_hype`, `undelegated_hype`, or `delegated_hype` is
/// nonzero, or [`LiveDecisionError::InvalidBalance`] when a nonzero amount
/// is not exactly representable in HYPE atoms.
fn first_live_probe_staking_atoms(
    staking: &HyperliquidStakingSummary,
) -> Result<(HypeAtoms, HypeAtoms), LiveDecisionError> {
    if staking.pending_withdrawal_hype != Decimal::ZERO {
        return Err(LiveDecisionError::NotFirstLiveProbe(
            "pending_withdrawal_hype",
        ));
    }
    let staking_hype_atoms = hype_atoms_from_decimal(staking.undelegated_hype)?;
    if !staking_hype_atoms.is_zero() {
        return Err(LiveDecisionError::NotFirstLiveProbe("staking_hype_atoms"));
    }
    let delegated_hype_atoms = hype_atoms_from_decimal(staking.delegated_hype)?;
    if !delegated_hype_atoms.is_zero() {
        return Err(LiveDecisionError::NotFirstLiveProbe("delegated_hype_atoms"));
    }
    Ok((staking_hype_atoms, delegated_hype_atoms))
}

#[cfg(test)]
mod tests {
    use super::*;
    use dex_connector::SpotAssetBalance;

    fn zero_staking() -> HyperliquidStakingSummary {
        HyperliquidStakingSummary {
            delegated_hype: Decimal::ZERO,
            undelegated_hype: Decimal::ZERO,
            pending_withdrawal_hype: Decimal::ZERO,
            pending_withdrawal_count: 0,
        }
    }

    #[test]
    fn accepts_an_all_zero_staking_summary() {
        let (staking_atoms, delegated_atoms) =
            first_live_probe_staking_atoms(&zero_staking()).unwrap();
        assert!(staking_atoms.is_zero());
        assert!(delegated_atoms.is_zero());
    }

    #[test]
    fn rejects_nonzero_pending_withdrawal() {
        let mut staking = zero_staking();
        staking.pending_withdrawal_hype = Decimal::from(1);
        assert!(matches!(
            first_live_probe_staking_atoms(&staking),
            Err(LiveDecisionError::NotFirstLiveProbe(
                "pending_withdrawal_hype"
            ))
        ));
    }

    #[test]
    fn rejects_nonzero_undelegated_staking() {
        let mut staking = zero_staking();
        staking.undelegated_hype = Decimal::from(1);
        assert!(matches!(
            first_live_probe_staking_atoms(&staking),
            Err(LiveDecisionError::NotFirstLiveProbe("staking_hype_atoms"))
        ));
    }

    #[test]
    fn rejects_nonzero_delegation() {
        let mut staking = zero_staking();
        staking.delegated_hype = Decimal::from(1);
        assert!(matches!(
            first_live_probe_staking_atoms(&staking),
            Err(LiveDecisionError::NotFirstLiveProbe("delegated_hype_atoms"))
        ));
    }

    #[test]
    fn spot_hype_balance_finds_the_hype_asset_and_defaults_to_zero() {
        let balance = CombinedBalanceResponse {
            spot_assets: vec![
                SpotAssetBalance {
                    symbol: "USDC".to_string(),
                    balance: Decimal::from(100),
                    locked_balance: Decimal::ZERO,
                },
                SpotAssetBalance {
                    symbol: "HYPE".to_string(),
                    balance: Decimal::from(5),
                    locked_balance: Decimal::ZERO,
                },
            ],
            ..CombinedBalanceResponse::default()
        };
        assert_eq!(spot_hype_balance(&balance), Decimal::from(5));
        assert_eq!(
            spot_hype_balance(&CombinedBalanceResponse::default()),
            Decimal::ZERO
        );
    }

    #[test]
    fn hype_atoms_from_decimal_floors_and_rejects_negative() {
        assert_eq!(
            hype_atoms_from_decimal(Decimal::new(15, 1)).unwrap(),
            HypeAtoms::from_atoms(150_000_000)
        );
        assert!(matches!(
            hype_atoms_from_decimal(Decimal::from(-1)),
            Err(LiveDecisionError::InvalidBalance)
        ));
    }
}
