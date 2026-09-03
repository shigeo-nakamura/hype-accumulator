//! Wires a freshly computed pacing decision into a durably prepared,
//! signer-free order-envelope workflow, ready for
//! [`crate::live_probe::HyperliquidLiveProbe`].
//!
//! # Scope limitation: first-live-probe only
//!
//! [`InventoryBaseline`]'s staking and delegated fields are asserted to be
//! exactly zero — verified against a live read, never merely assumed — and
//! a nonzero read fails closed rather than guessing. The
//! unconsumed-residual field, by contrast, is asserted zero with **no live
//! verification**: no cross-workflow ledger anywhere in this crate
//! aggregates unconsumed residual HYPE left behind by prior completed
//! workflows (each `DurableWorkflow`'s own `residual_hype` is tracked only
//! inside that one workflow's journal, see `workflow.rs`), so there is
//! nothing to read. Its correctness therefore rests entirely on the
//! staking/delegation checks: an account with zero staking and zero
//! delegation cannot have any prior workflow that ever reached the staking
//! stage, and therefore cannot have left an unconsumed residual behind.
//! This chain of reasoning — not an independent check — is what makes
//! asserting zero here safe, and it breaks the moment this module is ever
//! called more than once for the same account. Building genuine
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

    // A crash between `open_or_create` durably committing the first
    // attempt's binding and `prepare_order` completing must be retryable.
    // Since this binding's price/nonce/expiry are recomputed fresh from
    // live market state on every call, a naive retry would never reproduce
    // the exact durably committed binding and would permanently fail
    // `open_or_create`'s replay-match check. Reusing whatever binding is
    // already on disk — skipping every live read below — makes retry safe.
    let binding = if let Some(existing) = DurableWorkflow::peek_committed_binding(journal_path)? {
        existing
    } else {
        let probe_binding =
            LiveProbeBinding::from_connector(connector, hype_usdc_market_metadata_digest())?;

        let (balance, staking) = tokio::try_join!(
            connector.get_combined_balance(),
            connector.get_staking_summary()
        )?;

        let (staking_hype_atoms, delegated_hype_atoms) = first_live_probe_staking_atoms(&staking)?;
        let spot_hype_atoms = hype_atoms_from_decimal(spot_hype_balance(&balance))?;

        // Asserted, not independently checked (see module doc): there is
        // nothing to read that would verify this directly. Its
        // correctness rests entirely on the just-verified zero
        // staking/delegation above — no prior workflow ever reached the
        // staking stage, so none could have left an unconsumed residual
        // behind.
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

        DecisionBinding::from_pacing_decision(
            &decision,
            inventory_before,
            order_envelope,
            eligibility_policy,
        )?
    };

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

/// Sums every spot-asset entry matching "HYPE" case-insensitively, matching
/// `monitor.rs::spot_total`'s exact matching semantics — a venue that ever
/// splits one asset's balance across multiple case-varying entries must not
/// silently disagree between this inventory baseline and the observer's
/// reported balance.
fn spot_hype_balance(balance: &CombinedBalanceResponse) -> Decimal {
    balance
        .spot_assets
        .iter()
        .filter(|asset| asset.symbol.eq_ignore_ascii_case("HYPE"))
        .map(|asset| asset.balance)
        .sum()
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
    // Check the raw, un-floored decimal first: flooring to atom precision
    // (1e-8 HYPE) before comparing would silently let sub-atom dust (e.g.
    // 0.000000004 HYPE) pass as zero, defeating exactly the fail-closed
    // guarantee this function exists to provide.
    if staking.pending_withdrawal_hype != Decimal::ZERO {
        return Err(LiveDecisionError::NotFirstLiveProbe(
            "pending_withdrawal_hype",
        ));
    }
    if staking.undelegated_hype != Decimal::ZERO {
        return Err(LiveDecisionError::NotFirstLiveProbe("staking_hype_atoms"));
    }
    if staking.delegated_hype != Decimal::ZERO {
        return Err(LiveDecisionError::NotFirstLiveProbe("delegated_hype_atoms"));
    }
    let staking_hype_atoms = hype_atoms_from_decimal(staking.undelegated_hype)?;
    let delegated_hype_atoms = hype_atoms_from_decimal(staking.delegated_hype)?;
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
    fn rejects_sub_atom_staking_dust_that_would_floor_to_zero() {
        // 4e-9 HYPE is below the 1e-8 atom scale; flooring it to atoms
        // before comparing would wrongly read as zero. The raw decimal must
        // be checked directly.
        let mut staking = zero_staking();
        staking.undelegated_hype = Decimal::new(4, 9);
        assert!(matches!(
            first_live_probe_staking_atoms(&staking),
            Err(LiveDecisionError::NotFirstLiveProbe("staking_hype_atoms"))
        ));

        let mut staking = zero_staking();
        staking.delegated_hype = Decimal::new(4, 9);
        assert!(matches!(
            first_live_probe_staking_atoms(&staking),
            Err(LiveDecisionError::NotFirstLiveProbe("delegated_hype_atoms"))
        ));

        let mut staking = zero_staking();
        staking.pending_withdrawal_hype = Decimal::new(4, 9);
        assert!(matches!(
            first_live_probe_staking_atoms(&staking),
            Err(LiveDecisionError::NotFirstLiveProbe(
                "pending_withdrawal_hype"
            ))
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
    fn spot_hype_balance_sums_case_varying_duplicate_entries() {
        // Matches monitor.rs::spot_total's exact semantics: sum every entry
        // matching case-insensitively, not just the first exact match.
        let balance = CombinedBalanceResponse {
            spot_assets: vec![
                SpotAssetBalance {
                    symbol: "hype".to_string(),
                    balance: Decimal::from(2),
                    locked_balance: Decimal::ZERO,
                },
                SpotAssetBalance {
                    symbol: "HYPE".to_string(),
                    balance: Decimal::from(3),
                    locked_balance: Decimal::ZERO,
                },
            ],
            ..CombinedBalanceResponse::default()
        };
        assert_eq!(spot_hype_balance(&balance), Decimal::from(5));
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
