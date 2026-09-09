//! Wires a freshly computed pacing decision into a durably prepared,
//! signer-free order-envelope workflow, ready for
//! [`crate::live_probe::HyperliquidLiveProbe`].
//!
//! # Scope limitation: first-live-probe only
//!
//! [`InventoryBaseline`]'s staking and delegated fields are asserted to be
//! exactly zero — verified against a live read, never merely assumed — and
//! a nonzero read fails closed rather than guessing. The unconsumed-residual
//! field is now genuinely computed, not assumed:
//! [`crate::workflow::DurableWorkflow::aggregate_terminal_residual_hype`]
//! sums the terminal `residual_hype` left behind by every completed
//! workflow journal in `journal_directory` (see `workflow.rs`) and
//! reconciles that sum against this same call's live spot balance read,
//! failing closed on any journal that is not yet terminal or on a sum that
//! exceeds the live balance. This function remains restricted to an
//! account's first live economic action for a narrower reason than before:
//! no cross-workflow ledger in this crate yet tracks staking or delegation
//! across days (bot-strategy#929's remaining scope), so the zero
//! staking/delegation check above is still what this module's safety
//! depends on beyond residual HYPE. Building that ledger, a daily
//! scheduler, and observer/dashboard attribution wiring is required before
//! this module can support anything beyond an account's first live
//! economic action.

use crate::{
    hype_asset::hype_usdc_market_metadata_digest,
    live_probe::{LiveProbeBinding, LiveProbeError},
    order_envelope::{
        assemble_order_envelope_binding, OrderEnvelopeError, OrderEnvelopeFreshnessPolicy,
    },
    runtime::{
        LiveDecisionAllocation, LiveDecisionIdentity, RuntimeCycleInput, RuntimeError,
        SignerFreeRuntime,
    },
    workflow::{
        DecisionBinding, DurableWorkflow, EligibilityPolicyBinding, ExchangeOrderOwnerStore,
        HistoryScanRecorder, HypeAtoms, InventoryBaseline, JournalAdmissibilityCheck,
        ProtectedHeadStoreFactory, ProtectedWorkflowHeadStore, WorkflowError,
    },
};
use chrono::{DateTime, Utc};
use dex_connector::{
    CombinedBalanceResponse, DexConnector, DexError, HyperliquidConnector,
    HyperliquidStakingSummary,
};
use rust_decimal::Decimal;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::Arc,
};
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
    #[error(
        "journal is already bound to decision {existing} but this cycle's decision is {current}; \
         a journal path is never reused across decisions"
    )]
    JournalBoundToAnotherDecision { existing: String, current: String },
    #[error(
        "recorded journal intent for decision {decision_id} does not resolve: {journal} {reason}; \
         history is lost or tampered — restore it before preparing another order \
         (bot-strategy#944)"
    )]
    JournalIntentUnresolved {
        decision_id: String,
        journal: String,
        reason: &'static str,
    },
}

/// Checks that every journal intent the runtime recorded resolves to a
/// present, verified journal bound to exactly that decision — except the
/// current decision's own intent when its journal does not exist yet (a
/// retry after a crash between the intent record and the journal write);
/// a *different* decision's missing journal at that same path is lost
/// history, never a retry. This is
/// the manifest that turns a deleted-and-recreated or unmounted
/// `history_directory` into a hard failure before any history is aggregated
/// or any new order prepared (bot-strategy#944).
///
/// # Errors
///
/// Returns [`LiveDecisionError::JournalIntentUnresolved`] for a missing
/// journal, a journal with no committed binding, or one bound to a
/// different decision identity; propagates journal read errors.
pub fn verify_recorded_journal_intents(
    intents: &BTreeMap<String, PathBuf>,
    current_decision_id: &str,
    current_journal_path: &Path,
    identity_of: impl Fn(&str) -> Option<LiveDecisionIdentity>,
) -> Result<(), LiveDecisionError> {
    for (decision_id, journal) in intents {
        let unresolved = |reason: &'static str| LiveDecisionError::JournalIntentUnresolved {
            decision_id: decision_id.clone(),
            journal: journal.display().to_string(),
            reason,
        };
        if !journal.exists() {
            // Only the decision being prepared right now may lack its journal
            // (crash between the intent record and the journal write). A
            // different decision's intent naming the same file is lost
            // history that a new journal must never paper over.
            if journal == current_journal_path && decision_id == current_decision_id {
                continue;
            }
            return Err(unresolved("is missing"));
        }
        let Some(binding) = DurableWorkflow::peek_committed_binding(journal)? else {
            return Err(unresolved("has no committed binding"));
        };
        let Some(expected) = identity_of(decision_id) else {
            return Err(unresolved("names a decision this runtime does not hold"));
        };
        if bound_decision_identity(&binding) != expected {
            return Err(unresolved("is bound to a different decision"));
        }
    }
    Ok(())
}

/// The workflow's durable copy of the pacing decision it was bound to, in
/// the form the signer-free runtime verifies against its own decision
/// (`SignerFreeRuntime::settle_live_decision`,
/// `SignerFreeRuntime::record_live_journal_intent`) before moving capital.
#[must_use]
pub fn bound_decision_identity(binding: &DecisionBinding) -> LiveDecisionIdentity {
    let mut allocations = binding
        .capital_commitments
        .iter()
        .map(|commitment| LiveDecisionAllocation {
            tranche_id: commitment.event_id.clone(),
            planned_usdc: commitment.planned_usdc,
            committed_usdc: commitment.committed_usdc,
        })
        .collect::<Vec<_>>();
    allocations.sort_by(|left, right| left.tranche_id.cmp(&right.tranche_id));
    LiveDecisionIdentity {
        decision_id: binding.decision_id.clone(),
        decision_date: binding.decision_date,
        decided_at: binding.decided_at,
        capital_snapshot_hash: binding.capital_snapshot_hash.clone(),
        input_snapshot_hash: binding.input_snapshot_hash.clone(),
        planned_usdc: binding.planned_usdc,
        committed_usdc: binding.committed_usdc,
        allocations,
    }
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
/// module does not own. `journal_directory` must be a directory dedicated to
/// this execution account's own workflow journals under the exact same
/// network and vault-address routing mode as this call — `historical_
/// journal_admissible` is where the caller enforces that, since this
/// module has no notion of either (see
/// [`crate::workflow::DurableWorkflow::aggregate_terminal_residual_hype`])
/// — and should ordinarily be `journal_path`'s parent directory.
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
    journal_directory: &Path,
    // Journals an earlier run durably recorded as present in
    // `journal_directory` (bot-strategy#944). Every one of them must still
    // be there or the aggregation below is refused as an incomplete
    // history. Enforced inside the same scan the aggregation uses, so
    // history cannot go missing between the check and the inventory it
    // feeds, and compared by name so a newly created journal cannot
    // silently stand in for a lost one.
    recorded_history_journals: &BTreeSet<String>,
    historical_protected_head_store_for: &ProtectedHeadStoreFactory<'_>,
    historical_journal_admissible: &JournalAdmissibilityCheck<'_>,
    record_history_scan: &HistoryScanRecorder<'_>,
    protected_head_store: Arc<dyn ProtectedWorkflowHeadStore>,
    exchange_order_owner_store: Arc<dyn ExchangeOrderOwnerStore>,
    now: DateTime<Utc>,
) -> Result<DurableWorkflow, LiveDecisionError> {
    let report = runtime.apply_cycle(cycle_input)?;
    let decision = report
        .decision()
        .ok_or(LiveDecisionError::NoDecisionDue)?
        .clone();
    // Before touching the venue or aggregating history: every journal this
    // runtime ever declared must still be there and bound as declared.
    verify_recorded_journal_intents(
        runtime.live_journal_intents(),
        &decision.decision_id,
        journal_path,
        |id| runtime.decision_identity(id),
    )?;

    // A crash between `open_or_create` durably committing the first
    // attempt's binding and `prepare_order` completing must be retryable.
    // Since this binding's price/nonce/expiry are recomputed fresh from
    // live market state on every call, a naive retry would never reproduce
    // the exact durably committed binding and would permanently fail
    // `open_or_create`'s replay-match check. Reusing whatever binding is
    // already on disk — skipping every live read below — makes retry safe.
    let identity = LiveDecisionIdentity::of(&decision);
    let binding = if let Some(existing) = DurableWorkflow::peek_committed_binding(journal_path)? {
        // A journal already on disk must be *this* decision's retry, never a
        // reused path from another day: otherwise the intent below would
        // pin this decision to a foreign journal that `reconcile` can never
        // settle it from and `release` would then refuse forever.
        if bound_decision_identity(&existing) != identity {
            return Err(LiveDecisionError::JournalBoundToAnotherDecision {
                existing: existing.decision_id,
                current: identity.decision_id,
            });
        }
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

        // Genuinely aggregated and reconciled, not assumed (see module
        // doc): sums every completed past workflow's terminal residual
        // HYPE and fails closed if any historical journal is not yet
        // terminal or the sum exceeds this same call's live spot balance.
        // Never capped at the currently configured target: a residual
        // allocation a completed workflow already immutably classified
        // must never later become staking-eligible just because a policy
        // change lowered the target (docs/security/custody-threat-model.md
        // — "a residual allocation can never later become staking-
        // eligible" / "a terminal lot never becomes eligible again because
        // fungible spot later increases"). `residual_hype_deficit` already
        // treats an aggregate above the current target as zero deficit —
        // no new residual is reserved from today's fill, but the earlier
        // excess stays exactly what history says it is.
        let unconsumed_residual_spot_hype_atoms =
            DurableWorkflow::aggregate_terminal_residual_hype(
                journal_directory,
                Some(journal_path),
                spot_hype_atoms,
                &probe_binding.execution_identity_hash,
                historical_protected_head_store_for,
                historical_journal_admissible,
                recorded_history_journals,
            )?;
        // Before the journal below exists: this is the only moment at which
        // the scan's own result can be persisted and still be reproducible
        // by a retry. Once this run's journal is on disk, a retry reuses its
        // committed binding and never scans history again, so a write that
        // failed here would never get a second chance.
        record_history_scan()?;

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

    // Declared in the runtime's hash-chained state BEFORE the journal is
    // created (and after every fallible network read above, so a failure
    // there leaves the decision provably journal-less and releasable). Once
    // recorded, `hype-live-probe release` can never treat this decision as
    // unbound by absence — even if the journal directory is later lost.
    runtime.record_live_journal_intent(&identity, journal_path, now)?;
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
