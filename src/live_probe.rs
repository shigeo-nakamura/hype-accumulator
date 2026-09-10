//! Feature-gated bridge from a durably prepared workflow order to one exact
//! Hyperliquid IOC submission.
//!
//! This module deliberately has no scheduler, config loader, secret loader, or
//! retry loop. A caller must first obtain [`ExternalAction::SubmitOrder`] from
//! [`DurableWorkflow::prepare_order`](crate::workflow::DurableWorkflow::prepare_order),
//! which fsyncs the exact CLOID, nonce, and expiry, then pass that same
//! [`DurableWorkflow`](crate::workflow::DurableWorkflow) to this adapter. The
//! adapter never accepts a caller-supplied action. Once submission is invoked,
//! every error is reconciliation-only: the caller must query by CLOID and must
//! never call `submit` again for the same prepared workflow.

use crate::{
    hype_asset::HYPE_SPOT_MARKET,
    pacing::UsdcMicros,
    workflow::{
        AuthenticatedOrderSubmission, ConclusiveAbsenceEvidence, DecisionBinding, DurableWorkflow,
        ExternalAction, GapFreeHistoryWatermark, HistoryDomain, HypeAtoms, OrderFinality,
        WorkflowError, WorkflowStage, WorkflowState,
    },
};
use chrono::{DateTime, Utc};
use dex_connector::{
    DexError, FilledOrder, HyperliquidConnector, HyperliquidL1ActionEnvelope,
    HyperliquidOrderReconciliation, OrderSide,
};
use fs2::FileExt;
use rust_decimal::{prelude::ToPrimitive, Decimal};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io,
    path::{Path, PathBuf},
};
use thiserror::Error;

const EXECUTION_IDENTITY_DOMAIN: &[u8] = b"hype-accumulator/execution-account-identity/v1";
const SIGNER_IDENTITY_DOMAIN: &[u8] = b"hype-accumulator/api-wallet-identity/v1";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiveProbeBinding {
    pub symbol: String,
    pub execution_identity_hash: String,
    pub signer_identity_hash: String,
    pub market_metadata_digest: String,
}

impl LiveProbeBinding {
    /// Derives the durable execution and signer identities from the connector's
    /// configured public addresses. No private signing material is exposed.
    ///
    /// # Errors
    ///
    /// Returns an error when the connector is missing an execution account or
    /// API-wallet signer, or when the metadata digest is non-canonical.
    pub fn from_connector(
        connector: &HyperliquidConnector,
        market_metadata_digest: impl Into<String>,
    ) -> Result<Self, LiveProbeError> {
        let market_metadata_digest = market_metadata_digest.into();
        if market_metadata_digest.trim().is_empty()
            || market_metadata_digest != market_metadata_digest.trim()
        {
            return Err(LiveProbeError::BindingMismatch("market metadata"));
        }
        let (execution_identity_hash, signer_identity_hash) = connector_identity_hashes(connector)?;
        Ok(Self {
            symbol: HYPE_SPOT_MARKET.to_string(),
            execution_identity_hash,
            signer_identity_hash,
            market_metadata_digest,
        })
    }

    fn validate_connector(&self, connector: &HyperliquidConnector) -> Result<(), LiveProbeError> {
        let (execution_identity_hash, signer_identity_hash) = connector_identity_hashes(connector)?;
        if self.execution_identity_hash != execution_identity_hash {
            return Err(LiveProbeError::BindingMismatch("execution identity"));
        }
        if self.signer_identity_hash != signer_identity_hash {
            return Err(LiveProbeError::BindingMismatch("signer identity"));
        }
        if self.symbol != HYPE_SPOT_MARKET {
            return Err(LiveProbeError::BindingMismatch("symbol"));
        }
        if self.market_metadata_digest.trim().is_empty()
            || self.market_metadata_digest != self.market_metadata_digest.trim()
        {
            return Err(LiveProbeError::BindingMismatch("market metadata"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedIocOrder {
    pub action_id: String,
    pub client_order_id: String,
    pub symbol: String,
    pub quantity: Decimal,
    pub limit_price: Decimal,
    pub nonce: u64,
    pub expires_after_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProbeSubmission {
    pub action_id: String,
    pub client_order_id: String,
    pub exchange_order_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
#[allow(clippy::struct_excessive_bools)]
pub struct ProbeReconciliation {
    pub client_order_id: String,
    pub exchange_order_id: Option<String>,
    pub status: String,
    pub filled_hype: HypeAtoms,
    /// HYPE actually credited to the account: `filled_hype` less any fee
    /// the venue charged in HYPE (bot-strategy#998). `None` until the
    /// fills are gap-free, since it can only be computed from fill rows.
    pub credited_hype: Option<HypeAtoms>,
    pub remaining_hype: HypeAtoms,
    /// Whether `fills` (the detailed rows this lookup returned) fully
    /// accounts for `filled_hype` (the authoritative cumulative quantity
    /// from `orderStatus`). `false` means Hyperliquid's bounded recent-fill
    /// window has already dropped some of this order's rows — `filled_hype`/
    /// `remaining_hype` above are still authoritative, but this call could
    /// not durably record cumulative USDC (fill/finality recording is
    /// skipped rather than persisting an understated total). See
    /// docs/runbooks/live-probe-recovery.md.
    pub fills_complete: bool,
    /// True exactly when this call recorded (or already durably held) a
    /// terminal [`crate::workflow::WorkflowTransition::OrderFinalized`] for
    /// this workflow — see [`finality_from_status`] for which statuses
    /// count. `false` covers "not yet terminal", "order unknown to the
    /// venue" (`exchange_order_id` is `None`), and `fills_complete == false`
    /// blocking finalization.
    pub durable_finality: bool,
    /// True exactly when this call durably recorded conclusive absence for
    /// an expired order the venue never accepted — the zero-fill terminal
    /// outcome that releases a prepared intent (bot-strategy#982). `false`
    /// on every other path.
    pub absence_recorded: bool,
    /// True exactly when this call drove a zero-purchase workflow to
    /// `Complete`. Only a workflow that bought no HYPE is completed here;
    /// anything holding HYPE needs the separately approved staking custody
    /// design to classify residual versus eligible.
    pub workflow_completed: bool,
}

#[derive(Debug, Error)]
pub enum LiveProbeError {
    #[error("prepared action is not an order submission")]
    NotOrder,
    #[error("prepared order does not match the approved probe binding: {0}")]
    BindingMismatch(&'static str),
    #[error("prepared order is expired or has a non-canonical millisecond expiry")]
    InvalidExpiry,
    #[error("authenticated venue order status has no valid acceptance timestamp")]
    InvalidVenueTimestamp,
    #[error("prepared order contains an invalid exact decimal: {0}")]
    InvalidDecimal(&'static str),
    #[error("prepared order limit notional exceeds its durable capital bounds")]
    CapitalBound,
    #[error("purchase-fee ceiling must be below 10000 bps")]
    InvalidFeeCeiling,
    #[error("venue-reported order quantity does not match the authorized envelope")]
    QuantityMismatch,
    #[error("a durably observed fill contradicts a previously recorded observation of it: {0}")]
    ContradictoryFillEvidence(String),
    #[error("could not read or write the durable observed-fills record: {0}")]
    ObservedFillsAccumulator(String),
    #[error("durable workflow does not expose the authorized pending order: {0}")]
    Workflow(#[from] WorkflowError),
    #[error("Hyperliquid action requires CLOID reconciliation: {0}")]
    Connector(#[from] DexError),
}

pub struct HyperliquidLiveProbe {
    connector: HyperliquidConnector,
    binding: LiveProbeBinding,
    max_purchase_fee_bps: u16,
}

impl HyperliquidLiveProbe {
    /// Constructs a probe only when its durable identity binding matches the
    /// connector's actual execution account and API-wallet signer.
    ///
    /// `max_purchase_fee_bps` is the authoritative aggregate venue/builder
    /// fee ceiling used to bound the pre-submission debit check; it must
    /// match the same ceiling enforced by the [`crate::exchange::Exchange`]
    /// boundary for this deployment.
    ///
    /// # Errors
    ///
    /// Rejects missing or mismatched connector identities, invalid binding
    /// metadata, or a fee ceiling that is not below 10000 bps, before any
    /// nonce can be reserved or action submitted.
    pub fn new(
        connector: HyperliquidConnector,
        binding: LiveProbeBinding,
        max_purchase_fee_bps: u16,
    ) -> Result<Self, LiveProbeError> {
        binding.validate_connector(&connector)?;
        if max_purchase_fee_bps >= crate::bps::BPS_DENOMINATOR {
            return Err(LiveProbeError::InvalidFeeCeiling);
        }
        Ok(Self {
            connector,
            binding,
            max_purchase_fee_bps,
        })
    }

    /// Durably reserves one API-wallet nonce without signing or submitting.
    /// The caller must place the returned value in `OrderEnvelopeBinding` and
    /// fsync the resulting workflow action before calling [`Self::submit`].
    ///
    /// # Errors
    ///
    /// Propagates signer or persistent nonce-state failures.
    pub async fn reserve_nonce(&self) -> Result<u64, LiveProbeError> {
        Ok(self.connector.reserve_l1_action_nonce().await?)
    }

    /// Retires the nonce reservation for a durably prepared action that was
    /// never submitted and is now past its signed expiry. This performs no
    /// venue action and refuses unexpired or mismatched workflow actions.
    ///
    /// # Errors
    ///
    /// Rejects a mismatched/non-order action, a non-canonical or unexpired
    /// envelope, or a reservation that was already consumed.
    pub async fn abandon_expired(
        &self,
        workflow: &DurableWorkflow,
        now: DateTime<Utc>,
    ) -> Result<(), LiveProbeError> {
        let action = workflow.pending_prepared_order()?;
        let ExternalAction::SubmitOrder {
            execution_identity_hash,
            signer_identity_hash,
            market_metadata_digest,
            l1_nonce,
            signed_expiry_at,
            ..
        } = action
        else {
            return Err(LiveProbeError::NotOrder);
        };
        validate_binding(
            execution_identity_hash,
            signer_identity_hash,
            market_metadata_digest,
            &self.binding,
        )?;
        let expires_after_ms = u64::try_from(signed_expiry_at.timestamp_millis())
            .map_err(|_| LiveProbeError::InvalidExpiry)?;
        if signed_expiry_at.timestamp_subsec_nanos() % 1_000_000 != 0 || now < *signed_expiry_at {
            return Err(LiveProbeError::InvalidExpiry);
        }
        self.connector
            .abandon_expired_l1_action_envelope(HyperliquidL1ActionEnvelope {
                nonce: *l1_nonce,
                expires_after_ms,
            })
            .await?;
        Ok(())
    }

    /// Converts and submits one already-fsynced workflow action exactly once.
    ///
    /// # Errors
    ///
    /// Rejects identity, metadata, expiry, decimal, and capital mismatches
    /// before submission. Any connector error after this call begins is
    /// reconciliation-only; callers must never resubmit the action.
    pub async fn submit(
        &self,
        workflow: &DurableWorkflow,
        now: DateTime<Utc>,
    ) -> Result<ProbeSubmission, LiveProbeError> {
        let action = workflow.pending_prepared_order()?;
        let prepared =
            PreparedIocOrder::from_action(action, &self.binding, self.max_purchase_fee_bps, now)?;
        let response = self
            .connector
            .create_spot_ioc_order_with_envelope(
                &prepared.symbol,
                prepared.quantity,
                OrderSide::Long,
                prepared.limit_price,
                prepared.client_order_id.clone(),
                HyperliquidL1ActionEnvelope {
                    nonce: prepared.nonce,
                    expires_after_ms: prepared.expires_after_ms,
                },
            )
            .await?;
        Ok(ProbeSubmission {
            action_id: prepared.action_id,
            client_order_id: prepared.client_order_id,
            exchange_order_id: response.order_id,
        })
    }

    /// Performs an authenticated exact-CLOID lookup after any submission
    /// attempt or restart, and durably records the resulting order-submission,
    /// cumulative-fill, and (once terminal) finalization evidence on the
    /// workflow. This method never submits an economic action.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed quantities, connector failures, or a
    /// workflow that rejects the observed evidence.
    pub async fn reconcile(
        &self,
        workflow: &mut DurableWorkflow,
        journal_path: &Path,
        now: DateTime<Utc>,
    ) -> Result<ProbeReconciliation, LiveProbeError> {
        // Derived from the durable binding, not `pending_prepared_order()`:
        // that action is cleared once acceptance is observed, so a second
        // reconcile call after the first successfully recorded submission
        // (to catch up on fill/finality) must still work.
        let binding = workflow.state().binding();
        validate_binding(
            &binding.inventory_before.execution_identity_hash,
            &binding.order_envelope.signer_identity_hash,
            &binding.order_envelope.market_metadata_digest,
            &self.binding,
        )?;
        let client_order_id = workflow.state().client_order_id();
        let evidence = self
            .connector
            .reconcile_order_by_client_id(&client_order_id)
            .await?;
        // Matches the same check `lookup_read_only` already performs for
        // the unsigned recovery path: without it, evidence for a CLOID the
        // connector returned but that doesn't match what was actually
        // requested could still be recorded and finalized as though it
        // were the authorized order (client_order_id in the constructed
        // AuthenticatedOrderSubmission comes from workflow state, not from
        // this evidence, so nothing else would catch the mismatch).
        if evidence.client_order_id != client_order_id {
            return Err(LiveProbeError::BindingMismatch("client order ID"));
        }
        record_reconciliation(&self.connector, workflow, journal_path, evidence, now).await
    }
}

/// Reads the prepared order's exact CLOID using only its execution account,
/// and durably records the resulting order-submission, cumulative-fill, and
/// (once terminal) finalization evidence on the workflow.
///
/// No signer, nonce reservation, or live approval is needed: recovery must
/// remain possible after key revocation, a manual halt, or approval expiry.
/// Every evidence field this records comes either from this unauthenticated
/// exact-CLOID lookup or from the already-durable, already-authorized
/// workflow binding — never from a live signer.
///
/// # Errors
///
/// Rejects an account/market mismatch before contacting the venue, and
/// propagates invalid quantities, journal failures, and transport errors.
pub async fn reconcile_prepared_order(
    connector: &HyperliquidConnector,
    workflow: &mut DurableWorkflow,
    journal_path: &Path,
    now: DateTime<Utc>,
) -> Result<ProbeReconciliation, LiveProbeError> {
    let evidence = lookup_read_only(connector, workflow.state()).await?;
    record_reconciliation(connector, workflow, journal_path, evidence, now).await
}

/// Performs the unauthenticated exact-CLOID lookup itself: validates the
/// account/market before contacting the venue, then returns the venue's raw
/// reconciliation evidence unmodified. Split out from
/// [`reconcile_prepared_order`] so this half — the part that needs no
/// [`DurableWorkflow`] — stays independently testable.
///
/// Reads the account/market/CLOID from the durable binding
/// ([`WorkflowState::binding`]/[`WorkflowState::client_order_id`]), not
/// [`DurableWorkflow::pending_prepared_order`]: that action is cleared once
/// acceptance is observed, so recovery must keep working on a second call
/// after the first already recorded submission, to catch up on fill/finality.
async fn lookup_read_only(
    connector: &HyperliquidConnector,
    workflow_state: &WorkflowState,
) -> Result<HyperliquidOrderReconciliation, LiveProbeError> {
    let binding = workflow_state.binding();
    let account = connector.execution_account_address()?;
    if binding.inventory_before.execution_identity_hash
        != identity_hash(EXECUTION_IDENTITY_DOMAIN, account)
    {
        return Err(LiveProbeError::BindingMismatch("execution identity"));
    }
    if binding.order_envelope.market_metadata_digest
        != crate::hype_asset::hype_usdc_market_metadata_digest()
    {
        return Err(LiveProbeError::BindingMismatch("market metadata"));
    }
    let client_order_id = workflow_state.client_order_id();
    let evidence = connector
        .reconcile_order_by_client_id(&client_order_id)
        .await?;
    if evidence.client_order_id != client_order_id {
        return Err(LiveProbeError::BindingMismatch("client order ID"));
    }
    Ok(evidence)
}

/// Builds and durably records [`AuthenticatedOrderSubmission`] (once, guarded
/// by [`crate::workflow::WorkflowState::exchange_order_id`] already being
/// set), the cumulative fill observation, and — once the venue reports a
/// terminal status — the order finalization, from one already-fetched exact-
/// CLOID reconciliation. Shared by the signed and unsigned reconciliation
/// paths above, since neither needs a live signer for any of this: every
/// evidence field comes from the unauthenticated lookup itself or from the
/// already-durable, already-authorized workflow binding.
///
/// An order the venue does not (yet) know about (`evidence.order_id` is
/// `None`, e.g. `unknownOid`) records nothing — that is not proof of
/// absence. Durably recording conclusive absence
/// ([`DurableWorkflow::record_order_submission_absent`]) requires gap-free
/// history watermark evidence this binary does not yet construct; out of
/// scope here, matching bot-strategy#929's own scope boundary.
///
/// # Errors
///
/// Propagates invalid quantities/timestamps and any error the workflow
/// raises validating the observed evidence (contradiction, replay conflict,
/// or journal I/O failure).
#[allow(clippy::too_many_lines)]
async fn record_reconciliation(
    connector: &HyperliquidConnector,
    workflow: &mut DurableWorkflow,
    journal_path: &Path,
    evidence: HyperliquidOrderReconciliation,
    now: DateTime<Utc>,
) -> Result<ProbeReconciliation, LiveProbeError> {
    // Never rejected for being "before" the venue's own reported acceptance
    // time: the venue clock is permitted to run ahead of the local clock by
    // up to the order envelope's own `max_venue_clock_lag_ms` (already
    // authorized), and `validate_order_submission_evidence` rejects
    // `accepted_at > recorded_at` outright. Clamped once here so every
    // subsequent call below (fill observation, finalization) uses a
    // consistent, non-regressing timestamp too.
    let mut now = now;
    let binding = workflow.state().binding().clone();
    let hype_atoms_per_hype = binding.order_envelope.hype_atoms_per_hype;
    let cumulative_hype = decimal_to_atoms(evidence.filled_size, hype_atoms_per_hype)?;
    let remaining_hype = decimal_to_atoms(evidence.remaining_size, hype_atoms_per_hype)?;
    // Vacuously complete when the order itself is not (yet) known to the
    // venue — nothing to reconcile, not evidence of incompleteness.
    let mut fills_complete = true;
    let mut credited_hype = None;

    if let Some(exchange_order_id) = evidence.order_id.clone() {
        // The venue evidence's own quantity, independently reconstructed
        // (filled + remaining = orderStatus's origSz), must match the
        // authorized envelope before anything below trusts this
        // reconciliation as evidence *for that specific order* — otherwise
        // a wrong CLOID match (venue bug, or in principle a collision)
        // could pass every later cumulative-cap check by construction,
        // since those checks are bounded by the authorized quantity, not
        // verified against what the venue actually reported.
        let observed_original_quantity = verify_observed_quantity(&evidence, &binding)?;
        // CLOID, side, time-in-force, and limit price, read directly from
        // the raw venue envelope (no market-metadata resolution needed for
        // these — unlike `coin`, which is venue-internal and not
        // independently verified here; see the module-level note on that
        // residual gap). Without this, `orderStatus` matching the
        // requested quantity but describing e.g. a different order
        // entirely, a sell, a resting GTC order, or a different limit
        // price would still be accepted as though it were the authorized
        // HYPE IOC buy — evidence.client_order_id alone cannot catch this,
        // since dex-connector currently echoes the requested CLOID back
        // rather than parsing it from the response.
        verify_observed_order_envelope(
            &evidence.raw_order_status,
            &workflow.state().client_order_id(),
            binding
                .order_envelope
                .limit_price_usdc_per_hype
                .as_decimal(),
        )?;
        // Never trusted merely because *some* exchange order ID was
        // already recorded: if a later lookup returns a *different* ID
        // than the one already durably bound to this workflow, that must
        // fail closed before anything below acts on it — including before
        // this order's fills are merged into the durable accumulator,
        // which would otherwise poison it with a different order's data.
        if let Some(recorded) = workflow.state().exchange_order_id() {
            if recorded != exchange_order_id {
                return Err(LiveProbeError::BindingMismatch("exchange order ID"));
            }
        }

        // Hyperliquid's fill history is a bounded window shared across the
        // whole account, not scoped to this order: a fill can age out of
        // range while `orderStatus`'s origSz−sz still authoritatively
        // reports the order as filled (dex-connector's
        // `authoritative_filled_size`). Every fill row this lookup ever
        // returns is durably accumulated by trade ID next to the journal
        // (never discarded once seen), so a later call's narrower window
        // cannot un-see a fill an earlier call already recorded — without
        // this, an order could fall permanently short of gap-free fill
        // coverage once enough other account activity evicts its rows,
        // even though it genuinely filled. See
        // docs/runbooks/live-probe-recovery.md.
        let accumulated_fills = merge_and_persist_observed_fills(
            journal_path,
            workflow.state().workflow_id(),
            &exchange_order_id,
            &evidence.fills,
        )?;
        fills_complete =
            fills_cover_authoritative_quantity(&accumulated_fills.fills, evidence.filled_size)?;

        now = record_order_submission_if_new(
            connector,
            workflow,
            &binding,
            &exchange_order_id,
            &evidence.raw_order_status,
            observed_original_quantity,
            now,
        )
        .await?;

        if fills_complete {
            let (cumulative_filled_usdc, cumulative_debited_usdc) =
                cumulative_usdc_from_fills(&accumulated_fills.fills)?;
            let cumulative_credited_hype =
                cumulative_credited_hype_from_fills(&accumulated_fills.fills, hype_atoms_per_hype)?;
            credited_hype = Some(cumulative_credited_hype);
            // `observe_order_fill` never accepts a zero-HYPE observation
            // (`validate_cumulative_fill` allows zero only for a Canceled/
            // Expired *finalization*, not a bare fill observation) — a
            // freshly accepted order still open with no fills yet, or an
            // IOC canceled unfilled, must skip straight to finalization
            // (when terminal) rather than recording a fill observation that
            // would always be rejected.
            if !cumulative_hype.is_zero() {
                // "Completely filled" means the venue filled everything it
                // accepted, which is the authorized quantity rounded down
                // onto the venue's size lot, not the authorized quantity
                // itself (bot-strategy#845). Comparing against the latter
                // would leave every real full fill looking partial, and
                // `OrderFinality::Filled` would then be rejected as
                // contradictory.
                let fully_filled = cumulative_hype == observed_original_quantity;
                let fill_observation_id = content_hash(&[
                    "hype-accumulator/order-fill-observation/v1",
                    &exchange_order_id,
                    &cumulative_hype.as_atoms().to_string(),
                    &cumulative_filled_usdc.as_micros().to_string(),
                    &cumulative_debited_usdc.as_micros().to_string(),
                ]);
                workflow.observe_order_fill(
                    fill_observation_id,
                    cumulative_hype,
                    cumulative_credited_hype,
                    cumulative_filled_usdc,
                    cumulative_debited_usdc,
                    fully_filled,
                    now,
                )?;
            }

            if let Some(finality) = finality_from_status(&evidence.status) {
                workflow.finalize_order(
                    cumulative_hype,
                    cumulative_credited_hype,
                    cumulative_filled_usdc,
                    cumulative_debited_usdc,
                    finality,
                    now,
                )?;
            }
        }
    }

    // The venue does not know this order. Before its expiry that is
    // unresolved evidence and nothing may be recorded; after it, the order
    // can never be accepted, and gap-free history showing the CLOID in
    // neither orders nor fills makes absence conclusive.
    let absence_recorded =
        if evidence.order_id.is_none() && workflow.state().stage() == WorkflowStage::Decided {
            record_conclusive_absence(connector, workflow, &binding, now).await?
        } else {
            false
        };

    // A journal that bought nothing still has to reach `Complete`:
    // `DurableWorkflow::aggregate_terminal_residual_hype` treats anything
    // short of that as fail-closed, so a workflow left at `OrderFinalized`
    // blocks every later `prepare` for this account — the decision cannot
    // even be computed, let alone settled (bot-strategy#845 blocker 9).
    // There is nothing to classify or stake at zero HYPE, and
    // `validate_eligibility_evidence` already allows exactly this shape
    // (no exchange order, no evidence, nothing purchased).
    // Both stages are resumable: a crash between the eligibility append and
    // the completion append leaves the journal at
    // `StakingEligibilityRecorded`, which is just as non-terminal for
    // aggregation, so a later reconcile has to finish the job rather than
    // skip it.
    let workflow_completed = if workflow.state().exchange_order_id().is_none()
        && workflow.state().purchased_hype().is_zero()
        && matches!(
            workflow.state().stage(),
            WorkflowStage::OrderFinalized | WorkflowStage::StakingEligibilityRecorded
        ) {
        // Never earlier than the transition it follows: absence recording
        // just above clamps its own timestamp forward past the venue
        // evidence it attests to, so a bare `now` can regress behind it.
        let completion_at = now.max(workflow.state().last_transition_at());
        if workflow.state().stage() == WorkflowStage::OrderFinalized {
            workflow.record_staking_eligibility(None, completion_at)?;
        }
        workflow.complete(completion_at)?;
        true
    } else {
        false
    };

    Ok(ProbeReconciliation {
        client_order_id: evidence.client_order_id,
        exchange_order_id: evidence.order_id,
        status: evidence.status,
        filled_hype: cumulative_hype,
        credited_hype,
        remaining_hype,
        fills_complete,
        // Reflects the workflow's actual durable state, not just whether
        // *this* call reached `finalize_order` — an earlier call may already
        // have finalized it, and this one's fills could be incomplete.
        durable_finality: order_already_finalized(workflow.state().stage()),
        absence_recorded,
        workflow_completed,
    })
}

/// Durably records that an expired prepared order was never accepted by the
/// venue, releasing its prepared intent as a zero-fill terminal outcome
/// (bot-strategy#982). Returns `false` without recording anything whenever
/// absence is not yet conclusive.
///
/// Every condition here is a fail-closed gate on evidence:
///
/// * before `effective_expiry_at` the order can still be accepted, so
///   `unknownOid` proves nothing;
/// * each history window must be *complete* — dex-connector rejects a
///   response that reached the venue's row cap, since absence checked
///   against a truncated window is not absence;
/// * the fill history is queried from account inception through now, which
///   spans the order's whole possible lifetime and is the only form the
///   venue lets us prove untruncated;
/// * the CLOID must appear in neither window, and its presence is an
///   anomaly that fails closed rather than an absence to record.
///
/// The recorded watermarks claim gap-free coverage only from `decided_at`,
/// never from the oldest row observed: a weaker claim that a complete
/// window always supports.
async fn record_conclusive_absence(
    connector: &HyperliquidConnector,
    workflow: &mut DurableWorkflow,
    binding: &DecisionBinding,
    now: DateTime<Utc>,
) -> Result<bool, LiveProbeError> {
    let effective_expiry_at = binding.order_envelope.effective_expiry_at;
    if now <= effective_expiry_at {
        return Ok(false);
    }
    let decided_at = binding.decided_at;
    let orders = connector.historical_orders_window().await?;
    // The whole retained fill history through the present, not the decision
    // window: only a response the venue could not have truncated proves that
    // an interval inside it is genuinely empty rather than aged out.
    let fills = connector.retained_fills().await?;
    // Read once, after both windows: the instant through which both are
    // known to be gap-free.
    let observed_through_at = truncate_to_millis(Utc::now());
    if observed_through_at <= effective_expiry_at {
        return Ok(false);
    }
    let client_order_id = workflow.state().client_order_id();
    if orders.contains_client_order_id(&client_order_id)?
        || fills.contains_client_order_id(&client_order_id)?
    {
        return Err(LiveProbeError::BindingMismatch(
            "client order ID appears in venue history despite an unknown order status",
        ));
    }
    // `recorded_at` must not precede the coverage it attests to.
    let recorded_at = now.max(observed_through_at);
    let watermark = |domain: HistoryDomain, label: &str, raw_body: &str| GapFreeHistoryWatermark {
        domain,
        watermark_id: content_hash(&[
            "hype-accumulator/history-watermark/v1",
            label,
            &decided_at.timestamp_millis().to_string(),
            &observed_through_at.timestamp_millis().to_string(),
        ]),
        // Strictly positive and meaningful: the venue-time cursor through
        // which this window is gap-free.
        cursor: u64::try_from(observed_through_at.timestamp_millis()).unwrap_or(u64::MAX),
        gap_free_from_at: decided_at,
        through_at: observed_through_at,
        // Labelled per domain so two byte-identical bodies (an idle account
        // returns `[]` for both) still yield independent evidence hashes.
        evidence_hash: content_hash(&["hype-accumulator/history-evidence/v1", label, raw_body]),
    };
    let evidence = ConclusiveAbsenceEvidence {
        observation_id: content_hash(&[
            "hype-accumulator/order-absence-observation/v1",
            &client_order_id,
            orders.raw_body(),
            fills.raw_body(),
        ]),
        execution_identity_hash: binding.inventory_before.execution_identity_hash.clone(),
        client_order_id,
        effective_expiry_at,
        order_history: watermark(HistoryDomain::Order, "orders", orders.raw_body()),
        fill_history: watermark(HistoryDomain::Fill, "fills", fills.raw_body()),
    };
    workflow.record_order_submission_absent(evidence, recorded_at)?;
    Ok(true)
}

fn truncate_to_millis(at: DateTime<Utc>) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(at.timestamp_millis()).unwrap_or(at)
}

/// Builds and durably records [`AuthenticatedOrderSubmission`] exactly once
/// — a no-op once [`crate::workflow::WorkflowState::exchange_order_id`] is
/// already set. Returns `now`, clamped forward to the venue's own reported
/// acceptance time when this call actually performs the observation: the
/// venue clock may run ahead of the local clock by up to the order
/// envelope's own already-authorized `max_venue_clock_lag_ms`, and
/// `validate_order_submission_evidence` rejects `accepted_at > recorded_at`
/// outright.
///
/// # Errors
///
/// Propagates connector, timestamp-parsing, and workflow-validation errors.
/// The venue's own quantity for this order, independently reconstructed
/// from its `orderStatus` envelope (`filled + remaining` = `origSz`) and
/// checked against the authorized envelope.
///
/// The venue may only round the requested size **down** onto its own size
/// lot: HYPE spot trades on a `szDecimals` grid while the envelope
/// authorizes at wei precision, so an authorized 0.30798790 comes back as
/// 0.3 and equality can never hold (bot-strategy#845 blocker 10, hit on a
/// real fill). A size *larger* than authorized is a genuine contradiction
/// and fails closed, which is the property this check exists for: every
/// later cumulative cap is bounded by this quantity, so the venue must
/// never be able to enlarge it. Zero does not describe a submitted order.
/// Spend stays bounded independently by the envelope's `max_debit_usdc`,
/// and recorded fill totals come from the fills themselves.
fn verify_observed_quantity(
    evidence: &HyperliquidOrderReconciliation,
    binding: &DecisionBinding,
) -> Result<HypeAtoms, LiveProbeError> {
    let observed = decimal_to_atoms(
        evidence
            .filled_size
            .checked_add(evidence.remaining_size)
            .ok_or(LiveProbeError::InvalidDecimal("observed original quantity"))?,
        binding.order_envelope.hype_atoms_per_hype,
    )?;
    if observed.is_zero() || observed > binding.order_envelope.original_quantity_hype {
        return Err(LiveProbeError::QuantityMismatch);
    }
    Ok(observed)
}

async fn record_order_submission_if_new(
    connector: &HyperliquidConnector,
    workflow: &mut DurableWorkflow,
    binding: &DecisionBinding,
    exchange_order_id: &str,
    raw_order_status: &str,
    venue_accepted_quantity_hype: HypeAtoms,
    now: DateTime<Utc>,
) -> Result<DateTime<Utc>, LiveProbeError> {
    if let Some(recorded) = workflow.state().exchange_order_id() {
        // The accepted quantity is the full-fill target and the cumulative
        // fill cap, so once recorded it is immutable: a later lookup
        // reporting a different `origSz` for the same order is a
        // contradiction, and letting it pass would silently move both.
        if workflow.state().venue_accepted_quantity_hype() != Some(venue_accepted_quantity_hype) {
            return Err(LiveProbeError::QuantityMismatch);
        }
        // Never silently treated as "already observed, nothing to do": a
        // later lookup returning a *different* exchange order ID than the
        // one already durably recorded would otherwise let the caller
        // proceed to finalize using fills/status that belong to a
        // different order entirely.
        if recorded != exchange_order_id {
            return Err(LiveProbeError::BindingMismatch("exchange order ID"));
        }
        // Stays monotonic with the workflow's own last transition, for the
        // same reason as the first-observation path below: a fresh `now`
        // that happens to be earlier than an earlier call's clock-lag-
        // clamped transition time would otherwise regress and be rejected.
        return Ok(now.max(workflow.state().last_transition_at()));
    }
    let raw_accepted_at = accepted_at_from_raw_order_status(raw_order_status)?;
    // The venue clock may legitimately run up to the order envelope's own
    // already-authorized `max_venue_clock_lag_ms` *behind or ahead* of the
    // local clock: `validate_order_submission_evidence` otherwise rejects
    // `accepted_at` outright for either predating the workflow's own last
    // transition (`prepare_order`'s timestamp) or postdating `now`, which
    // ordinary tolerated clock skew can trigger in either direction even
    // though nothing is actually wrong. Normalized up to/bounded by that
    // tolerance — checked (and any network call skipped) before the
    // account-scope evidence lookup below, since a timestamp outside the
    // authorized tolerance is a genuine anomaly that must fail closed
    // regardless of what that lookup would return.
    let last_transition_at = workflow.state().last_transition_at();
    let max_lag = chrono::TimeDelta::try_milliseconds(
        i64::try_from(binding.order_envelope.max_venue_clock_lag_ms)
            .map_err(|_| LiveProbeError::InvalidVenueTimestamp)?,
    )
    .ok_or(LiveProbeError::InvalidVenueTimestamp)?;
    if raw_accepted_at < last_transition_at - max_lag || raw_accepted_at > now + max_lag {
        return Err(LiveProbeError::InvalidVenueTimestamp);
    }
    let accepted_at = raw_accepted_at.max(last_transition_at);
    let now = now.max(accepted_at);
    let account_scope_raw = connector.spot_state_raw().await?;
    let submission = AuthenticatedOrderSubmission {
        observation_id: content_hash(&[
            "hype-accumulator/order-submission-observation/v1",
            raw_order_status,
            &account_scope_raw,
        ]),
        account_scope_evidence_hash: content_hash(&[&account_scope_raw]),
        order_envelope_evidence_hash: content_hash(&[raw_order_status]),
        execution_identity_hash: binding.inventory_before.execution_identity_hash.clone(),
        signer_identity_hash: binding.order_envelope.signer_identity_hash.clone(),
        decision_id: binding.decision_id.clone(),
        client_order_id: workflow.state().client_order_id(),
        exchange_order_id: exchange_order_id.to_string(),
        canonical_order_envelope_hash: workflow.state().canonical_order_envelope_hash()?,
        planned_usdc: binding.planned_usdc,
        max_debit_usdc: binding.committed_usdc,
        original_quantity_hype: binding.order_envelope.original_quantity_hype,
        venue_accepted_quantity_hype: Some(venue_accepted_quantity_hype),
        hype_atoms_per_hype: binding.order_envelope.hype_atoms_per_hype,
        market_metadata_digest: binding.order_envelope.market_metadata_digest.clone(),
        limit_price_usdc_per_hype: binding.order_envelope.limit_price_usdc_per_hype,
        l1_nonce: binding.order_envelope.l1_nonce,
        signed_expiry_at: binding.order_envelope.signed_expiry_at,
        effective_expiry_at: binding.order_envelope.effective_expiry_at,
        market: HYPE_SPOT_MARKET.to_string(),
        side: "buy".to_string(),
        time_in_force: "IOC".to_string(),
        accepted_at,
    };
    workflow.observe_order_submission(&submission, now)?;
    Ok(now)
}

/// Whether the workflow has already durably recorded `OrderFinalized` (or
/// progressed past it, e.g. into staking-eligibility bookkeeping), for
/// [`ProbeReconciliation::durable_finality`] — computed from the workflow's
/// actual stage rather than from whether the current call happened to
/// (re)finalize it, so an earlier finalization is still reported correctly
/// even when this call's own fills are incomplete or the order is not
/// otherwise touched again.
fn order_already_finalized(stage: crate::workflow::WorkflowStage) -> bool {
    use crate::workflow::WorkflowStage;
    matches!(
        stage,
        WorkflowStage::OrderFinalized
            | WorkflowStage::StakingEligibilityRecorded
            | WorkflowStage::StakingDepositSubmitted
            | WorkflowStage::StakingBalanceConfirmed
            | WorkflowStage::DelegationSubmitted
            | WorkflowStage::DelegatedConfirmed
            | WorkflowStage::Complete
    )
}

/// Maps a raw Hyperliquid order status string to durable order finality.
///
/// Confirmed against real signed testnet activity (bot-strategy#901):
/// `"filled"` for a fully-filled IOC, `"canceled"` for a resting order
/// explicitly canceled. Hyperliquid's unfilled/partially-filled IOC
/// auto-cancel path was not independently observed — this conservatively
/// treats every other status (including `"open"` and any cancel-reason
/// string not yet confirmed, e.g. a margin- or self-trade-triggered
/// cancellation) as **not yet terminal** rather than guessing, so an
/// unrecognized string blocks finalization instead of silently
/// misclassifying it.
fn finality_from_status(status: &str) -> Option<OrderFinality> {
    match status {
        "filled" => Some(OrderFinality::Filled),
        "canceled" => Some(OrderFinality::Canceled),
        _ => None,
    }
}

/// Extracts the venue's own order-acceptance timestamp (`order.timestamp`,
/// milliseconds) from a raw `orderStatus` response body. Confirmed against
/// real signed testnet activity (bot-strategy#901) to be present, stable
/// across an order's lifetime (unlike `statusTimestamp`, which advances on
/// every status transition), and exactly the fill time for an IOC that
/// filled immediately at placement.
fn accepted_at_from_raw_order_status(raw: &str) -> Result<DateTime<Utc>, LiveProbeError> {
    let value: serde_json::Value =
        serde_json::from_str(raw).map_err(|_| LiveProbeError::InvalidVenueTimestamp)?;
    let millis = value
        .get("order")
        .and_then(|envelope| envelope.get("order"))
        .and_then(|order| order.get("timestamp"))
        .and_then(serde_json::Value::as_u64)
        .ok_or(LiveProbeError::InvalidVenueTimestamp)?;
    let millis = i64::try_from(millis).map_err(|_| LiveProbeError::InvalidVenueTimestamp)?;
    DateTime::from_timestamp_millis(millis).ok_or(LiveProbeError::InvalidVenueTimestamp)
}

/// Verifies the raw venue envelope's own `side`/`tif`/`limitPx` match the
/// authorized buy IOC at the authorized limit price before any evidence
/// built from it is trusted — confirmed real values (bot-strategy#901):
/// `side: "B"` for buy, `tif: "Ioc"`, `limitPx` as a plain decimal string.
///
/// Deliberately does **not** verify `coin` (the venue-internal asset index,
/// e.g. `"@1035"` — not a human-readable symbol) against the authorized
/// market: resolving it to confirm it names the HYPE/USDC spot market needs
/// dex-connector market-metadata plumbing this binary does not have direct
/// access to from a raw `orderStatus` body alone. This is a real residual
/// gap, not a solved case — tracked as follow-up, not silently assumed
/// covered.
///
/// # Errors
///
/// Returns an error for malformed JSON, or a side/tif/limit price that
/// doesn't match.
fn verify_observed_order_envelope(
    raw_order_status: &str,
    expected_client_order_id: &str,
    expected_limit_price_usdc_per_hype: Decimal,
) -> Result<(), LiveProbeError> {
    let value: serde_json::Value = serde_json::from_str(raw_order_status)
        .map_err(|_| LiveProbeError::BindingMismatch("orderStatus JSON"))?;
    let order = value
        .get("order")
        .and_then(|envelope| envelope.get("order"));
    // dex-connector's `reconcile_order_by_client_id` currently populates
    // `HyperliquidOrderReconciliation.client_order_id` by echoing the
    // requested (normalized) CLOID rather than parsing it from the venue
    // response, so comparing against that field alone can never actually
    // catch a wrong response — it would always match by construction. The
    // raw envelope's own `cloid` is the only field that genuinely reflects
    // what the venue returned.
    let cloid = order
        .and_then(|order| order.get("cloid"))
        .and_then(|v| v.as_str());
    let side = order
        .and_then(|order| order.get("side"))
        .and_then(|v| v.as_str());
    let tif = order
        .and_then(|order| order.get("tif"))
        .and_then(|v| v.as_str());
    let limit_price = order
        .and_then(|order| order.get("limitPx"))
        .and_then(|v| v.as_str())
        .and_then(|v| v.parse::<Decimal>().ok());
    if cloid != Some(expected_client_order_id) {
        return Err(LiveProbeError::BindingMismatch("order client id"));
    }
    if side != Some("B") {
        return Err(LiveProbeError::BindingMismatch("order side"));
    }
    if tif != Some("Ioc") {
        return Err(LiveProbeError::BindingMismatch("order time in force"));
    }
    // Hyperliquid normalizes an order's price onto its own grid (at most
    // five significant figures for spot), while the envelope authorizes at
    // micro-USDC precision: an authorized 81.172020 comes back as 81.172,
    // and equality can never hold (bot-strategy#845 blocker 12, hit on the
    // first real fill — same shape as the size lot in blocker 10).
    //
    // This is a *buy*, so the authorized limit price is a ceiling on what
    // may be paid per HYPE. A venue price at or below it is therefore
    // strictly within the authorization and can only lower the maximum
    // spend; a price *above* it would let the order pay more than was
    // authorized and still fails closed, which is the property this check
    // exists for. A non-positive price does not describe a real order.
    // Order identity itself comes from the CLOID check above, not from the
    // price. Deriving the price on the venue's grid up front, so intent
    // matches what the venue accepts, is bot-strategy#991.
    match limit_price {
        Some(price) if price > Decimal::ZERO && price <= expected_limit_price_usdc_per_hype => {}
        _ => return Err(LiveProbeError::BindingMismatch("order limit price")),
    }
    Ok(())
}

const OBSERVED_FILLS_SCHEMA_VERSION: u8 = 2;

/// One fill's economically relevant fields, durably persisted as exact
/// decimal strings (never re-derived from a float, never silently
/// defaulted). Deliberately does not store every [`FilledOrder`] field —
/// only what cumulative USDC accounting needs.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
struct AccumulatedFill {
    size: String,
    notional: String,
    /// Quote-equivalent total fee, as dex-connector reports it.
    fee: String,
    /// The part of `fee` the venue charged in the base asset, in HYPE
    /// (bot-strategy#998). `size - base_fee` is what the account was
    /// credited. Schema version 2 added this; a version-1 file cannot be
    /// upgraded (its rows never captured the fee token) and is rejected.
    base_fee: String,
}

/// Durable, append-only-in-spirit record of every fill row this journal's
/// order has ever been observed to have, keyed by Hyperliquid's own trade
/// ID. See [`observed_fills_path`] for why this exists.
///
/// `workflow_id` and `content_hash` bind this file to one specific workflow
/// and detect naive tampering/corruption/staleness (a hand-edit, a bad
/// backup restore, disk corruption, a bug elsewhere) — see
/// [`load_observed_fills`]. This is not a substitute for the journal's own
/// hash-chained protected-head mechanism (a determined tamperer with write
/// access could recompute a matching hash after modifying `fills`); the
/// journal's own `validate_cumulative_fill` regression check is the actual
/// backstop against manufactured cumulative totals.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
struct ObservedFillsAccumulator {
    #[serde(default)]
    schema_version: u8,
    #[serde(default)]
    workflow_id: String,
    /// Empty until the first successful merge binds it. Rejects a later
    /// merge for a *different* exchange order ID even when the workflow
    /// itself has no recorded `exchange_order_id` yet (e.g. a first
    /// attempt wrote fills here but failed before
    /// `observe_order_submission` persisted anything) — see
    /// [`record_reconciliation`]'s ordering.
    #[serde(default)]
    exchange_order_id: String,
    #[serde(default)]
    content_hash: String,
    #[serde(default)]
    fills: BTreeMap<String, AccumulatedFill>,
}

/// Sibling path to a journal, holding every fill row ever observed for its
/// order — durable because Hyperliquid's fill history is a bounded window
/// shared across the *whole account*, not scoped to one order. A fill can
/// age out of that window (evicted by unrelated later account activity)
/// while `orderStatus` still authoritatively reports the order as filled;
/// without durably keeping every row this binary has ever actually seen, a
/// delayed reconciliation could permanently lose the ability to prove
/// gap-free fill coverage for an order that genuinely did fill. See
/// docs/runbooks/live-probe-recovery.md.
fn observed_fills_path(journal_path: &Path) -> PathBuf {
    let mut path = journal_path.to_path_buf();
    path.set_extension("observed-fills.json");
    path
}

/// Serializes the accumulator's read-merge-write sequence against another
/// process doing the same for the same journal (`submit` and the
/// signer-free `reconcile` recovery command can run concurrently). Atomic
/// rename alone only prevents a torn/partial file; it does not prevent one
/// process's merge from silently overwriting another's, which could drop a
/// fill row that has since aged out of Hyperliquid's recent-fill window and
/// can never be recovered from the venue again. Fails fast (does not
/// block) on contention, mirroring `workflow.rs`'s own journal append
/// lock.
///
/// # Errors
///
/// Returns [`LiveProbeError::ObservedFillsAccumulator`] if the lock file
/// cannot be opened, or if another process already holds the lock.
fn acquire_observed_fills_lock(accumulator_path: &Path) -> Result<File, LiveProbeError> {
    let parent = accumulator_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)
        .map_err(|error| LiveProbeError::ObservedFillsAccumulator(error.to_string()))?;
    let mut lock_path = accumulator_path.as_os_str().to_os_string();
    lock_path.push(".lock");
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(lock_path)
        .map_err(|error| LiveProbeError::ObservedFillsAccumulator(error.to_string()))?;
    match lock.try_lock_exclusive() {
        Ok(()) => Ok(lock),
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
            Err(LiveProbeError::ObservedFillsAccumulator(
                "observed-fills record is locked by a concurrent reconciliation".into(),
            ))
        }
        Err(error) => Err(LiveProbeError::ObservedFillsAccumulator(error.to_string())),
    }
}

/// Loads, binds/verifies, merges, and durably persists this call's fill
/// rows into the observed-fills accumulator for `journal_path`, under an
/// exclusive lock held for the whole sequence.
///
/// # Errors
///
/// Propagates lock, I/O, and merge errors, and rejects a `fills` batch for
/// an `exchange_order_id` different from the one this accumulator was
/// first bound to — independently of whether the *workflow* has recorded
/// an exchange order ID yet, since a first attempt can write fills here
/// and then fail before `observe_order_submission` ever persists anything.
fn merge_and_persist_observed_fills(
    journal_path: &Path,
    workflow_id: &str,
    exchange_order_id: &str,
    fills: &[FilledOrder],
) -> Result<ObservedFillsAccumulator, LiveProbeError> {
    let accumulator_path = observed_fills_path(journal_path);
    // Held across the whole read-merge-write sequence, released as soon as
    // this function returns — nothing past that point needs exclusivity
    // (observe_order_fill/finalize_order are already protected by the
    // journal's own lock).
    let lock = acquire_observed_fills_lock(&accumulator_path)?;
    let mut accumulated = load_observed_fills(&accumulator_path, workflow_id)?;
    if accumulated.exchange_order_id.is_empty() {
        accumulated.exchange_order_id = exchange_order_id.to_string();
    } else if accumulated.exchange_order_id != exchange_order_id {
        return Err(LiveProbeError::BindingMismatch(
            "exchange order ID (accumulator)",
        ));
    }
    merge_observed_fills(&mut accumulated.fills, fills)?;
    accumulated.content_hash = observed_fills_content_hash(
        workflow_id,
        &accumulated.exchange_order_id,
        &accumulated.fills,
    );
    crate::status_io::write_private_json_atomic(&accumulator_path, &accumulated)
        .map_err(|error| LiveProbeError::ObservedFillsAccumulator(error.to_string()))?;
    drop(lock);
    Ok(accumulated)
}

/// Deterministic digest binding a workflow identity to its exact fill
/// contents. `BTreeMap` iterates in sorted key order, so this is stable
/// across process restarts regardless of insertion order.
fn observed_fills_content_hash(
    workflow_id: &str,
    exchange_order_id: &str,
    fills: &BTreeMap<String, AccumulatedFill>,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(workflow_id.as_bytes());
    hasher.update([0]);
    hasher.update(exchange_order_id.as_bytes());
    for (trade_id, fill) in fills {
        hasher.update([0]);
        hasher.update(trade_id.as_bytes());
        hasher.update([0]);
        hasher.update(fill.size.as_bytes());
        hasher.update([0]);
        hasher.update(fill.notional.as_bytes());
        hasher.update([0]);
        hasher.update(fill.fee.as_bytes());
        hasher.update([0]);
        hasher.update(fill.base_fee.as_bytes());
    }
    format!("{:x}", hasher.finalize())
}

/// Loads the durable fill accumulator for `workflow_id`, refusing a file
/// that does not carry that exact workflow ID or whose stored content hash
/// does not match its own fill contents.
///
/// # Errors
///
/// Returns [`LiveProbeError::ObservedFillsAccumulator`] for a read/parse
/// failure, a workflow ID mismatch (this file belongs to a different
/// order), or a content-hash mismatch (stale, corrupted, or hand-modified).
fn load_observed_fills(
    path: &Path,
    workflow_id: &str,
) -> Result<ObservedFillsAccumulator, LiveProbeError> {
    match fs::read_to_string(path) {
        Ok(contents) => {
            // Version first, on its own: a version-1 row has no `base_fee`,
            // so the full parse below would fail on that field with a
            // message that hides the actual cause.
            #[derive(serde::Deserialize)]
            struct Header {
                #[serde(default)]
                schema_version: u8,
            }
            let header: Header = serde_json::from_str(&contents)
                .map_err(|error| LiveProbeError::ObservedFillsAccumulator(error.to_string()))?;
            if header.schema_version != OBSERVED_FILLS_SCHEMA_VERSION {
                return Err(LiveProbeError::ObservedFillsAccumulator(format!(
                    "observed-fills record is schema version {} but this binary writes {}; \
                     a version-1 record never captured which asset each fee was charged in, so \
                     it cannot be upgraded — rebuild it from the venue by removing it",
                    header.schema_version, OBSERVED_FILLS_SCHEMA_VERSION
                )));
            }
            let accumulator: ObservedFillsAccumulator = serde_json::from_str(&contents)
                .map_err(|error| LiveProbeError::ObservedFillsAccumulator(error.to_string()))?;
            if accumulator.workflow_id != workflow_id {
                return Err(LiveProbeError::ObservedFillsAccumulator(
                    "observed-fills record belongs to a different workflow".into(),
                ));
            }
            if accumulator.content_hash
                != observed_fills_content_hash(
                    workflow_id,
                    &accumulator.exchange_order_id,
                    &accumulator.fills,
                )
            {
                return Err(LiveProbeError::ObservedFillsAccumulator(
                    "observed-fills record content hash does not match its own contents".into(),
                ));
            }
            Ok(accumulator)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok(ObservedFillsAccumulator {
                schema_version: OBSERVED_FILLS_SCHEMA_VERSION,
                workflow_id: workflow_id.to_string(),
                exchange_order_id: String::new(),
                content_hash: observed_fills_content_hash(workflow_id, "", &BTreeMap::new()),
                fills: BTreeMap::new(),
            })
        }
        Err(error) => Err(LiveProbeError::ObservedFillsAccumulator(error.to_string())),
    }
}

/// Merges freshly observed fill rows into the durable accumulator.
///
/// # Errors
///
/// Rejects a fill missing its size, notional, or fee outright — a missing
/// fee must never silently default to zero, since that would understate
/// `cumulative_debited_usdc` exactly like a missing notional would. Also
/// rejects a trade ID already recorded with *different* content: a
/// historical fill's own economics never change once observed, so a
/// mismatch means something is wrong (a bug, or in principle two orders
/// sharing a trade ID) and must never be silently overwritten.
fn merge_observed_fills(
    accumulated: &mut BTreeMap<String, AccumulatedFill>,
    fills: &[FilledOrder],
) -> Result<(), LiveProbeError> {
    for fill in fills {
        let entry = AccumulatedFill {
            size: fill
                .filled_size
                .ok_or(LiveProbeError::InvalidDecimal("fill size"))?
                .to_string(),
            notional: fill
                .filled_value
                .ok_or(LiveProbeError::InvalidDecimal("fill notional"))?
                .to_string(),
            fee: fill
                .filled_fee
                .ok_or(LiveProbeError::InvalidDecimal("fill fee"))?
                .to_string(),
            // `None` means the venue adapter does not say which asset the
            // fee was charged in. Hyperliquid always says; anything else
            // reaching this path is an unexpected connector and must not be
            // recorded as though nothing was taken from the base asset.
            base_fee: fill
                .filled_base_fee
                .ok_or(LiveProbeError::InvalidDecimal("fill base fee"))?
                .to_string(),
        };
        match accumulated.get(&fill.trade_id) {
            Some(existing) if *existing != entry => {
                return Err(LiveProbeError::ContradictoryFillEvidence(
                    fill.trade_id.clone(),
                ));
            }
            _ => {
                accumulated.insert(fill.trade_id.clone(), entry);
            }
        }
    }
    Ok(())
}

/// Whether the durably accumulated fills fully account for
/// `authoritative_filled_size` (from `orderStatus`'s own origSz−sz, which
/// dex-connector's `authoritative_filled_size` treats as authoritative even
/// when any single lookup's fills list is truncated). A caller must never
/// compute cumulative USDC totals before this is `true`.
///
/// # Errors
///
/// Returns an error for a corrupt stored size or on overflow summing it.
fn fills_cover_authoritative_quantity(
    fills: &BTreeMap<String, AccumulatedFill>,
    authoritative_filled_size: Decimal,
) -> Result<bool, LiveProbeError> {
    let mut total = Decimal::ZERO;
    for fill in fills.values() {
        let size = fill.size.parse::<Decimal>().map_err(|_| {
            LiveProbeError::ObservedFillsAccumulator("corrupt stored fill size".into())
        })?;
        total = total
            .checked_add(size)
            .ok_or(LiveProbeError::InvalidDecimal("cumulative fill size"))?;
    }
    Ok(total == authoritative_filled_size)
}

/// The HYPE actually credited to the account by the accumulated fills:
/// each fill's matched size less the part of its fee the venue charged in
/// HYPE (bot-strategy#998). The real 2026-09-10 fill matched 0.3 and
/// charged 0.00021 HYPE, so 0.29979 arrived; recording 0.3 would claim
/// HYPE the account does not hold and fail every later residual
/// reconciliation closed. Only meaningful once the fills are gap-free.
///
/// `size - base_fee` is the *buy* movement; a sell's would be
/// `-size - base_fee`. Every fill reaching this path is a buy:
/// `verify_observed_order_envelope` has already rejected any order whose
/// venue side is not `B`, and the fills are looked up by that order's ID.
///
/// # Errors
///
/// Corrupt stored values, overflow, a negative result (a fee larger than
/// its own fill), or a total not representable at `hype_atoms_per_hype`.
fn cumulative_credited_hype_from_fills(
    fills: &BTreeMap<String, AccumulatedFill>,
    hype_atoms_per_hype: u64,
) -> Result<HypeAtoms, LiveProbeError> {
    let mut credited = Decimal::ZERO;
    for entry in fills.values() {
        let size = entry.size.parse::<Decimal>().map_err(|_| {
            LiveProbeError::ObservedFillsAccumulator("corrupt stored fill size".into())
        })?;
        let base_fee = entry.base_fee.parse::<Decimal>().map_err(|_| {
            LiveProbeError::ObservedFillsAccumulator("corrupt stored fill base fee".into())
        })?;
        let net = size
            .checked_sub(base_fee)
            .ok_or(LiveProbeError::InvalidDecimal("credited fill size"))?;
        credited = credited
            .checked_add(net)
            .ok_or(LiveProbeError::InvalidDecimal("cumulative credited size"))?;
    }
    decimal_to_atoms(credited, hype_atoms_per_hype)
}

/// Sums each accumulated fill's notional, plus the fee **when it was
/// charged in USDC**, into cumulative filled and debited USDC.
///
/// A fee the venue charged in HYPE is not a USDC debit: the account paid
/// exactly the notional in USDC and received `size - base_fee` HYPE. That
/// fee is accounted for once, as fewer HYPE credited
/// ([`cumulative_credited_hype_from_fills`]); adding its quote-equivalent
/// here as well would count it twice and record a USDC spend the account
/// never made (bot-strategy#998). Hyperliquid charges each fill's fee in
/// exactly one asset, so a non-zero `base_fee` means the whole `fee` is
/// that amount converted, and none of it left the USDC balance.
fn cumulative_usdc_from_fills(
    fills: &BTreeMap<String, AccumulatedFill>,
) -> Result<(UsdcMicros, UsdcMicros), LiveProbeError> {
    let mut filled = Decimal::ZERO;
    let mut fee = Decimal::ZERO;
    for entry in fills.values() {
        let value = entry.notional.parse::<Decimal>().map_err(|_| {
            LiveProbeError::ObservedFillsAccumulator("corrupt stored fill notional".into())
        })?;
        let entry_fee = entry.fee.parse::<Decimal>().map_err(|_| {
            LiveProbeError::ObservedFillsAccumulator("corrupt stored fill fee".into())
        })?;
        let base_fee = entry.base_fee.parse::<Decimal>().map_err(|_| {
            LiveProbeError::ObservedFillsAccumulator("corrupt stored fill base fee".into())
        })?;
        let usdc_fee = if base_fee.is_zero() {
            entry_fee
        } else {
            Decimal::ZERO
        };
        filled = filled
            .checked_add(value)
            .ok_or(LiveProbeError::InvalidDecimal("cumulative fill notional"))?;
        fee = fee
            .checked_add(usdc_fee)
            .ok_or(LiveProbeError::InvalidDecimal("cumulative fill fee"))?;
    }
    let debited = filled
        .checked_add(fee)
        .ok_or(LiveProbeError::InvalidDecimal(
            "cumulative debited notional",
        ))?;
    Ok((
        UsdcMicros::from_decimal(filled).ok_or(LiveProbeError::InvalidDecimal(
            "cumulative fill notional precision",
        ))?,
        UsdcMicros::from_decimal(debited).ok_or(LiveProbeError::InvalidDecimal(
            "cumulative debited notional precision",
        ))?,
    ))
}

/// Domain-separated content hash for evidence provenance. Distinct from
/// [`identity_hash`], which fixes a compile-time domain constant for a
/// long-lived identity; this takes the domain as the caller's first `parts`
/// element since each evidence kind hashes a different, ad hoc set of raw
/// venue bytes.
fn content_hash(parts: &[&str]) -> String {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update([0]);
        hasher.update(part.as_bytes());
    }
    format!("{:x}", hasher.finalize())
}

impl PreparedIocOrder {
    fn from_action(
        action: &ExternalAction,
        binding: &LiveProbeBinding,
        max_purchase_fee_bps: u16,
        now: DateTime<Utc>,
    ) -> Result<Self, LiveProbeError> {
        let ExternalAction::SubmitOrder {
            action_id,
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
        } = action
        else {
            return Err(LiveProbeError::NotOrder);
        };
        validate_binding(
            execution_identity_hash,
            signer_identity_hash,
            market_metadata_digest,
            binding,
        )?;
        let expires_after_ms = u64::try_from(signed_expiry_at.timestamp_millis())
            .map_err(|_| LiveProbeError::InvalidExpiry)?;
        if signed_expiry_at.timestamp_subsec_nanos() % 1_000_000 != 0 || now >= *signed_expiry_at {
            return Err(LiveProbeError::InvalidExpiry);
        }
        let quantity = atoms_to_decimal(*original_quantity_hype, *hype_atoms_per_hype)?;
        let limit_price = limit_price_usdc_per_hype.as_decimal();
        let limit_notional = quantity
            .checked_mul(limit_price)
            .ok_or(LiveProbeError::CapitalBound)?;
        if limit_notional > notional_usdc.as_decimal() {
            return Err(LiveProbeError::CapitalBound);
        }
        let worst_case_debit = crate::bps::apply_bps_markup(limit_notional, max_purchase_fee_bps)
            .ok_or(LiveProbeError::CapitalBound)?;
        if worst_case_debit > max_debit_usdc.as_decimal() {
            return Err(LiveProbeError::CapitalBound);
        }
        Ok(Self {
            action_id: action_id.clone(),
            client_order_id: client_order_id.clone(),
            symbol: binding.symbol.clone(),
            quantity,
            limit_price,
            nonce: *l1_nonce,
            expires_after_ms,
        })
    }
}

fn validate_binding(
    execution_identity_hash: &str,
    signer_identity_hash: &str,
    market_metadata_digest: &str,
    binding: &LiveProbeBinding,
) -> Result<(), LiveProbeError> {
    if execution_identity_hash != binding.execution_identity_hash {
        return Err(LiveProbeError::BindingMismatch("execution identity"));
    }
    if signer_identity_hash != binding.signer_identity_hash {
        return Err(LiveProbeError::BindingMismatch("signer identity"));
    }
    if market_metadata_digest != binding.market_metadata_digest {
        return Err(LiveProbeError::BindingMismatch("market metadata"));
    }
    if binding.symbol != HYPE_SPOT_MARKET {
        return Err(LiveProbeError::BindingMismatch("symbol"));
    }
    Ok(())
}

fn connector_identity_hashes(
    connector: &HyperliquidConnector,
) -> Result<(String, String), LiveProbeError> {
    let execution_account = connector.execution_account_address()?;
    let api_wallet = connector.api_wallet_address()?;
    Ok((
        identity_hash(EXECUTION_IDENTITY_DOMAIN, execution_account),
        identity_hash(SIGNER_IDENTITY_DOMAIN, &api_wallet),
    ))
}

fn identity_hash(domain: &[u8], canonical_address: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update([0]);
    hasher.update(canonical_address.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn atoms_to_decimal(atoms: HypeAtoms, atoms_per_hype: u64) -> Result<Decimal, LiveProbeError> {
    if atoms.is_zero() || atoms_per_hype == 0 {
        return Err(LiveProbeError::InvalidDecimal("HYPE quantity"));
    }
    let original = Decimal::from(atoms.as_atoms());
    let value = original
        .checked_div(Decimal::from(atoms_per_hype))
        .filter(|value| *value > Decimal::ZERO)
        .ok_or(LiveProbeError::InvalidDecimal("HYPE quantity"))?;
    if value.checked_mul(Decimal::from(atoms_per_hype)) != Some(original) {
        return Err(LiveProbeError::InvalidDecimal(
            "HYPE atom scale is not exactly representable",
        ));
    }
    Ok(value)
}

fn decimal_to_atoms(value: Decimal, atoms_per_hype: u64) -> Result<HypeAtoms, LiveProbeError> {
    if value < Decimal::ZERO || atoms_per_hype == 0 {
        return Err(LiveProbeError::InvalidDecimal("reconciled HYPE quantity"));
    }
    let scaled = value
        .checked_mul(Decimal::from(atoms_per_hype))
        .ok_or(LiveProbeError::InvalidDecimal("reconciled HYPE quantity"))?;
    if !scaled.fract().is_zero() {
        return Err(LiveProbeError::InvalidDecimal(
            "reconciled HYPE quantity precision",
        ));
    }
    scaled
        .to_u64()
        .map(HypeAtoms::from_atoms)
        .ok_or(LiveProbeError::InvalidDecimal("reconciled HYPE quantity"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::{
        AuthorizationInputFreshness, CapitalCommitment, DecisionBinding, EligibilityPolicyBinding,
        ExchangeOrderOwnerStore, FileExchangeOrderOwnerStore, FileProtectedWorkflowHeadStore,
        InventoryBaseline, OrderEnvelopeBinding, ProtectedWorkflowHeadStore,
    };
    use chrono::TimeZone;
    use dex_connector::{HyperliquidAccountConfig, HyperliquidConnectorConfig};
    use std::{path::Path, str::FromStr, sync::Arc};

    const TEST_SIGNER_KEY: &str =
        "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

    fn at(second: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(second, 0).single().unwrap()
    }

    fn binding() -> LiveProbeBinding {
        LiveProbeBinding {
            symbol: HYPE_SPOT_MARKET.to_string(),
            execution_identity_hash: "execution-a".to_string(),
            signer_identity_hash: "signer-a".to_string(),
            market_metadata_digest: "market-a".to_string(),
        }
    }

    fn test_connector(account: &str, nonce_path: &Path) -> HyperliquidConnector {
        HyperliquidConnector::new(HyperliquidConnectorConfig {
            base_url: "http://127.0.0.1:1".to_string(),
            tracked_symbols: vec![HYPE_SPOT_MARKET.to_string()],
        })
        .unwrap()
        .with_account(HyperliquidAccountConfig {
            account_address: account.to_string(),
            signer_private_key: Some(TEST_SIGNER_KEY.to_string()),
            vault_address: None,
            is_mainnet: false,
            nonce_state_path: Some(nonce_path.to_path_buf()),
            max_taker_notional: Some(Decimal::from(25)),
            max_taker_slippage_bps: Some(50),
            max_taker_book_age_ms: 1_000,
        })
        .unwrap()
    }

    fn order() -> ExternalAction {
        ExternalAction::SubmitOrder {
            action_id: "action-a".to_string(),
            client_order_id: "0x00112233445566778899aabbccddeeff".to_string(),
            execution_identity_hash: "execution-a".to_string(),
            signer_identity_hash: "signer-a".to_string(),
            notional_usdc: UsdcMicros::from_micros(25_000_000),
            max_debit_usdc: UsdcMicros::from_micros(25_100_000),
            original_quantity_hype: HypeAtoms::from_atoms(100_000_000),
            hype_atoms_per_hype: 100_000_000,
            market_metadata_digest: "market-a".to_string(),
            limit_price_usdc_per_hype: UsdcMicros::from_micros(25_000_000),
            l1_nonce: 1_700_000_000_123,
            signed_expiry_at: at(30),
        }
    }

    #[test]
    fn maps_exact_workflow_envelope_without_regeneration() {
        let prepared = PreparedIocOrder::from_action(&order(), &binding(), 0, at(1)).unwrap();
        assert_eq!(prepared.action_id, "action-a");
        assert_eq!(
            prepared.client_order_id,
            "0x00112233445566778899aabbccddeeff"
        );
        assert_eq!(prepared.quantity, Decimal::ONE);
        assert_eq!(prepared.limit_price, Decimal::from(25));
        assert_eq!(prepared.nonce, 1_700_000_000_123);
        assert_eq!(prepared.expires_after_ms, 30_000);
    }

    #[test]
    fn rejects_identity_metadata_expiry_and_capital_mismatches() {
        let mut mismatched = order();
        if let ExternalAction::SubmitOrder {
            execution_identity_hash,
            ..
        } = &mut mismatched
        {
            *execution_identity_hash = "other".to_string();
        }
        assert!(matches!(
            PreparedIocOrder::from_action(&mismatched, &binding(), 0, at(1)),
            Err(LiveProbeError::BindingMismatch("execution identity"))
        ));

        let mut expired = order();
        if let ExternalAction::SubmitOrder {
            signed_expiry_at, ..
        } = &mut expired
        {
            *signed_expiry_at = at(1);
        }
        assert!(matches!(
            PreparedIocOrder::from_action(&expired, &binding(), 0, at(1)),
            Err(LiveProbeError::InvalidExpiry)
        ));

        let mut oversized = order();
        if let ExternalAction::SubmitOrder { notional_usdc, .. } = &mut oversized {
            *notional_usdc = UsdcMicros::from_micros(24_999_999);
        }
        assert!(matches!(
            PreparedIocOrder::from_action(&oversized, &binding(), 0, at(1)),
            Err(LiveProbeError::CapitalBound)
        ));

        let mut other_market = binding();
        other_market.symbol = "PURR/USDC".to_string();
        assert!(matches!(
            PreparedIocOrder::from_action(&order(), &other_market, 0, at(1)),
            Err(LiveProbeError::BindingMismatch("symbol"))
        ));

        let mut inexact_scale = order();
        if let ExternalAction::SubmitOrder {
            original_quantity_hype,
            hype_atoms_per_hype,
            ..
        } = &mut inexact_scale
        {
            *original_quantity_hype = HypeAtoms::from_atoms(1);
            *hype_atoms_per_hype = 3;
        }
        assert!(matches!(
            PreparedIocOrder::from_action(&inexact_scale, &binding(), 0, at(1)),
            Err(LiveProbeError::InvalidDecimal(
                "HYPE atom scale is not exactly representable"
            ))
        ));
    }

    #[test]
    fn debit_cap_accounts_for_worst_case_purchase_fee() {
        // order() carries a 100-bps margin between notional_usdc ($25) and
        // max_debit_usdc ($25.10). A fee ceiling that exactly exhausts that
        // margin must still pass; one basis point beyond it must not, since
        // the actual fill's fee could otherwise push the debit past the
        // durable committed cap that workflow reconciliation enforces.
        assert!(PreparedIocOrder::from_action(&order(), &binding(), 40, at(1)).is_ok());
        assert!(matches!(
            PreparedIocOrder::from_action(&order(), &binding(), 41, at(1)),
            Err(LiveProbeError::CapitalBound)
        ));
    }

    #[test]
    fn reconciliation_requires_exact_atom_precision() {
        assert_eq!(
            decimal_to_atoms(Decimal::new(123, 2), 100).unwrap(),
            HypeAtoms::from_atoms(123)
        );
        assert!(decimal_to_atoms(Decimal::new(1231, 3), 100).is_err());
        assert!(decimal_to_atoms(Decimal::NEGATIVE_ONE, 100).is_err());
    }

    #[test]
    fn derives_and_enforces_actual_connector_identities() {
        let temp = tempfile::tempdir().unwrap();
        let connector = test_connector(
            "0x1111111111111111111111111111111111111111",
            &temp.path().join("nonce-a.json"),
        );
        let binding = LiveProbeBinding::from_connector(&connector, "market-a").unwrap();
        assert_eq!(
            binding.execution_identity_hash,
            identity_hash(
                EXECUTION_IDENTITY_DOMAIN,
                connector.execution_account_address().unwrap()
            )
        );
        assert_eq!(
            binding.signer_identity_hash,
            identity_hash(
                SIGNER_IDENTITY_DOMAIN,
                &connector.api_wallet_address().unwrap()
            )
        );
        assert!(HyperliquidLiveProbe::new(connector, binding, 50).is_ok());

        let connector = test_connector(
            "0x2222222222222222222222222222222222222222",
            &temp.path().join("nonce-b.json"),
        );
        let mut stale = LiveProbeBinding::from_connector(&connector, "market-a").unwrap();
        stale.execution_identity_hash = "stale-account".to_string();
        assert!(matches!(
            HyperliquidLiveProbe::new(connector, stale, 50),
            Err(LiveProbeError::BindingMismatch("execution identity"))
        ));

        let connector = test_connector(
            "0x3333333333333333333333333333333333333333",
            &temp.path().join("nonce-c.json"),
        );
        let mut stale = LiveProbeBinding::from_connector(&connector, "market-a").unwrap();
        stale.signer_identity_hash = "stale-api-wallet".to_string();
        assert!(matches!(
            HyperliquidLiveProbe::new(connector, stale, 50),
            Err(LiveProbeError::BindingMismatch("signer identity"))
        ));
    }

    fn unsigned_connector(base_url: String) -> HyperliquidConnector {
        HyperliquidConnector::new(HyperliquidConnectorConfig {
            base_url,
            tracked_symbols: Vec::new(),
        })
        .unwrap()
        .with_account(HyperliquidAccountConfig {
            account_address: "0x1111111111111111111111111111111111111111".to_owned(),
            signer_private_key: None,
            vault_address: None,
            is_mainnet: false,
            nonce_state_path: None,
            max_taker_notional: None,
            max_taker_slippage_bps: None,
            max_taker_book_age_ms: 1000,
        })
        .unwrap()
    }

    fn fixture_at(minute: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 9, 6, 12, minute, 0)
            .single()
            .expect("valid UTC fixture")
    }

    /// A real, minimal, valid `DecisionBinding` — every field required by
    /// [`DecisionBinding::validate`], not a stub. `execution_identity_hash`
    /// is a parameter so it can be made to match a test connector's real
    /// derived identity.
    fn decision_binding(execution_identity_hash: String) -> DecisionBinding {
        DecisionBinding {
            decision_id: "decision-a".to_owned(),
            decision_date: fixture_at(0).date_naive(),
            decided_at: fixture_at(0),
            capital_snapshot_hash: "capital-snapshot-a".to_owned(),
            input_snapshot_hash: "input-snapshot-a".to_owned(),
            planned_usdc: UsdcMicros::from_micros(25_000_000),
            committed_usdc: UsdcMicros::from_micros(25_100_000),
            capital_commitments: vec![CapitalCommitment {
                event_id: "tranche-a".to_owned(),
                planned_usdc: UsdcMicros::from_micros(25_000_000),
                committed_usdc: UsdcMicros::from_micros(25_100_000),
            }],
            inventory_before: InventoryBaseline {
                execution_identity_hash,
                spot_hype_atoms: HypeAtoms::from_atoms(0),
                staking_hype_atoms: HypeAtoms::from_atoms(0),
                delegated_hype_atoms: HypeAtoms::from_atoms(0),
                configured_residual_hype_atoms: HypeAtoms::from_atoms(0),
                unconsumed_residual_spot_hype_atoms: HypeAtoms::from_atoms(0),
            },
            order_envelope: OrderEnvelopeBinding {
                signer_identity_hash: "signer-identity-hash-a".to_owned(),
                original_quantity_hype: HypeAtoms::from_atoms(100_000_000),
                hype_atoms_per_hype: 100_000_000,
                market_metadata_digest: crate::hype_asset::hype_usdc_market_metadata_digest(),
                limit_price_usdc_per_hype: UsdcMicros::from_micros(25_000_000),
                l1_nonce: 1,
                signed_expiry_at: fixture_at(29),
                effective_expiry_at: fixture_at(30),
                venue_clock_evidence_at: fixture_at(0),
                venue_clock_evidence_valid_through_at: fixture_at(31),
                venue_clock_evidence_digest: "venue-clock-evidence-a".to_owned(),
                max_venue_clock_lag_ms: 59_999,
                input_freshness: AuthorizationInputFreshness {
                    decision_valid_through_at: fixture_at(30),
                    signal_evidence_valid_through_at: fixture_at(30),
                    book_evidence_valid_through_at: fixture_at(30),
                    account_evidence_valid_through_at: fixture_at(30),
                    fee_schedule_valid_through_at: fixture_at(30),
                    policy_acknowledgement_valid_through_at: fixture_at(30),
                },
            },
            eligibility_policy: EligibilityPolicyBinding {
                policy_version: "custody-policy-v1".to_owned(),
                fill_registration_deadline_seconds: 60,
                lot_eligibility_max_age_seconds: 3_600,
            },
            offline_staking_capability: None,
        }
    }

    fn test_journal_path(dir: &Path) -> PathBuf {
        dir.join("journal.jsonl")
    }

    fn open_test_workflow(dir: &Path, binding: &DecisionBinding) -> DurableWorkflow {
        let head_store: Arc<dyn ProtectedWorkflowHeadStore> = Arc::new(
            FileProtectedWorkflowHeadStore::new(dir.join("journal.protected-head.json")).unwrap(),
        );
        let owner_store: Arc<dyn ExchangeOrderOwnerStore> = Arc::new(
            FileExchangeOrderOwnerStore::new(dir.join("exchange-order-owners.json")).unwrap(),
        );
        DurableWorkflow::open_or_create(test_journal_path(dir), binding, head_store, owner_store)
            .expect("valid fixture binding opens a fresh workflow")
    }

    /// Answers exactly one `reconcile_prepared_order`/`HyperliquidLiveProbe::
    /// reconcile` round: `userFillsByTime`, `orderStatus`, then (only when
    /// the order is found) `spotClearinghouseState` for account-scope
    /// evidence. One fake server accept-loop per round, since the client
    /// sends `Connection: close` and reconnects for each `/info` call.
    /// `expect_account_scope_lookup` must be `true` only when this round's
    /// call is expected to durably record `AuthenticatedOrderSubmission` for
    /// the first time (i.e. `WorkflowState::exchange_order_id()` is `None`
    /// going in) — that's the only case `record_reconciliation` fetches
    /// `spotClearinghouseState`. A later round reconciling an
    /// already-recorded order does not repeat that lookup.
    fn test_submission_evidence(
        workflow: &DurableWorkflow,
        binding: &DecisionBinding,
        accepted_at: DateTime<Utc>,
    ) -> AuthenticatedOrderSubmission {
        AuthenticatedOrderSubmission {
            observation_id: "obs-a".to_string(),
            account_scope_evidence_hash: "hash-a".to_string(),
            order_envelope_evidence_hash: "hash-b".to_string(),
            execution_identity_hash: binding.inventory_before.execution_identity_hash.clone(),
            signer_identity_hash: binding.order_envelope.signer_identity_hash.clone(),
            decision_id: binding.decision_id.clone(),
            client_order_id: workflow.state().client_order_id(),
            exchange_order_id: "7".to_string(),
            canonical_order_envelope_hash: workflow
                .state()
                .canonical_order_envelope_hash()
                .unwrap(),
            planned_usdc: binding.planned_usdc,
            max_debit_usdc: binding.committed_usdc,
            original_quantity_hype: binding.order_envelope.original_quantity_hype,
            venue_accepted_quantity_hype: Some(binding.order_envelope.original_quantity_hype),
            hype_atoms_per_hype: binding.order_envelope.hype_atoms_per_hype,
            market_metadata_digest: binding.order_envelope.market_metadata_digest.clone(),
            limit_price_usdc_per_hype: binding.order_envelope.limit_price_usdc_per_hype,
            l1_nonce: binding.order_envelope.l1_nonce,
            signed_expiry_at: binding.order_envelope.signed_expiry_at,
            effective_expiry_at: binding.order_envelope.effective_expiry_at,
            market: HYPE_SPOT_MARKET.to_string(),
            side: "buy".to_string(),
            time_in_force: "IOC".to_string(),
            accepted_at,
        }
    }

    fn spawn_reconcile_responder(
        listener: tokio::net::TcpListener,
        order_status: serde_json::Value,
        fills: serde_json::Value,
        expect_account_scope_lookup: bool,
    ) -> tokio::task::JoinHandle<()> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        tokio::spawn(async move {
            let request_types = if expect_account_scope_lookup {
                vec!["userFillsByTime", "orderStatus", "spotClearinghouseState"]
            } else {
                vec!["userFillsByTime", "orderStatus"]
            };
            for request_type in request_types {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut data = Vec::new();
                let (header_end, length) = loop {
                    let mut buffer = [0; 2048];
                    let count = socket.read(&mut buffer).await.unwrap();
                    assert!(count > 0);
                    data.extend_from_slice(&buffer[..count]);
                    if let Some(end) = data.windows(4).position(|part| part == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&data[..end]);
                        let length: usize = headers
                            .lines()
                            .find_map(|line| {
                                let (key, value) = line.split_once(':')?;
                                key.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse().unwrap())
                            })
                            .unwrap();
                        break (end + 4, length);
                    }
                };
                while data.len() < header_end + length {
                    let mut buffer = [0; 2048];
                    let count = socket.read(&mut buffer).await.unwrap();
                    assert!(count > 0);
                    data.extend_from_slice(&buffer[..count]);
                }
                let request: serde_json::Value =
                    serde_json::from_slice(&data[header_end..header_end + length]).unwrap();
                assert_eq!(request["type"], request_type);
                let body = match request_type {
                    "orderStatus" => order_status.to_string(),
                    "userFillsByTime" => fills.to_string(),
                    _ => serde_json::json!({"balances": []}).to_string(),
                };
                let response = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        })
    }

    /// Answers one reconcile round for an order the venue never accepted:
    /// `userFillsByTime` (the recent-fill window), `orderStatus`
    /// (`unknownOid`), then — only when absence recording is expected —
    /// `historicalOrders` and the decision-window `userFillsByTime`.
    fn spawn_absence_responder(
        listener: tokio::net::TcpListener,
        historical_orders: Option<serde_json::Value>,
        retained_fills: serde_json::Value,
    ) -> tokio::task::JoinHandle<()> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        tokio::spawn(async move {
            let mut request_types = vec!["userFillsByTime", "orderStatus"];
            if historical_orders.is_some() {
                request_types.push("historicalOrders");
                request_types.push("userFillsByTime");
            }
            for (index, request_type) in request_types.iter().enumerate() {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut data = Vec::new();
                let (header_end, length) = loop {
                    let mut buffer = [0; 2048];
                    let count = socket.read(&mut buffer).await.unwrap();
                    assert!(count > 0);
                    data.extend_from_slice(&buffer[..count]);
                    if let Some(end) = data.windows(4).position(|part| part == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&data[..end]);
                        let length: usize = headers
                            .lines()
                            .find_map(|line| {
                                let (key, value) = line.split_once(':')?;
                                key.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse().unwrap())
                            })
                            .unwrap();
                        break (end + 4, length);
                    }
                };
                while data.len() < header_end + length {
                    let mut buffer = [0; 2048];
                    let count = socket.read(&mut buffer).await.unwrap();
                    assert!(count > 0);
                    data.extend_from_slice(&buffer[..count]);
                }
                let request: serde_json::Value =
                    serde_json::from_slice(&data[header_end..header_end + length]).unwrap();
                assert_eq!(&request["type"], request_type);
                let body = match (*request_type, index) {
                    ("orderStatus", _) => serde_json::json!({"status": "unknownOid"}).to_string(),
                    ("historicalOrders", _) => historical_orders.clone().unwrap().to_string(),
                    // The absence query must cover the retained history
                    // from inception through the venue's own present: a
                    // sub-range could sit past the retention horizon, and
                    // any client-side upper bound could exclude fills that
                    // have already evicted older rows.
                    ("userFillsByTime", 3) => {
                        assert_eq!(request["startTime"], 0);
                        assert!(request.get("endTime").is_none());
                        retained_fills.to_string()
                    }
                    _ => serde_json::json!([]).to_string(),
                };
                let response = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        })
    }

    async fn reconcile_unknown_order(
        temp: &Path,
        workflow: &mut DurableWorkflow,
        now: DateTime<Utc>,
        historical_orders: Option<serde_json::Value>,
        retained_fills: serde_json::Value,
    ) -> Result<ProbeReconciliation, LiveProbeError> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let connector = unsigned_connector(format!("http://{}", listener.local_addr().unwrap()));
        let server = spawn_absence_responder(listener, historical_orders, retained_fills);
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            reconcile_prepared_order(&connector, workflow, &test_journal_path(temp), now),
        )
        .await
        .unwrap();
        server.await.unwrap();
        result
    }

    #[tokio::test]
    async fn expired_order_the_venue_never_accepted_is_recorded_absent_exactly_once() {
        // bot-strategy#982: a prepared order that was never submitted (the
        // operator aborted, or the process died before `submit`) must be
        // resolvable to a terminal zero-fill outcome from venue evidence,
        // or its pacing decision strands every later decision day.
        let temp = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let execution_identity_hash = identity_hash(
            EXECUTION_IDENTITY_DOMAIN,
            unsigned_connector(format!("http://{}", listener.local_addr().unwrap()))
                .execution_account_address()
                .unwrap(),
        );
        drop(listener);
        let binding = decision_binding(execution_identity_hash);
        let mut workflow = open_test_workflow(temp.path(), &binding);
        workflow.prepare_order(fixture_at(2)).unwrap();

        // Before the envelope expires, `unknownOid` proves nothing: the
        // order can still be accepted, so nothing may be recorded and the
        // history endpoints are not even consulted.
        let early = reconcile_unknown_order(
            temp.path(),
            &mut workflow,
            fixture_at(20),
            None,
            serde_json::json!([]),
        )
        .await
        .unwrap();
        assert!(!early.absence_recorded);
        assert!(!early.durable_finality);
        assert_eq!(workflow.state().stage(), WorkflowStage::Decided);

        // After expiry, with complete order and fill history that does not
        // contain this client order ID, absence is conclusive. Other
        // accounts' orders in the window are irrelevant.
        let other_order = serde_json::json!([
            {"order": {"oid": 11, "cloid": "0x00000000000000000000000000000001"}, "status": "filled"}
        ]);
        let recorded = reconcile_unknown_order(
            temp.path(),
            &mut workflow,
            fixture_at(35),
            Some(other_order.clone()),
            serde_json::json!([]),
        )
        .await
        .unwrap();
        assert!(recorded.absence_recorded);
        assert!(recorded.durable_finality);
        assert_eq!(recorded.filled_hype, HypeAtoms::from_atoms(0));
        // Driven all the way to `Complete`: a journal left at
        // `OrderFinalized` is not terminal for
        // `aggregate_terminal_residual_hype` and would block every later
        // `prepare` for this account.
        assert!(recorded.workflow_completed);
        assert_eq!(workflow.state().stage(), WorkflowStage::Complete);
        assert!(workflow.state().exchange_order_id().is_none());

        // Idempotent: the workflow is no longer `Decided`, so a later
        // reconcile neither re-records nor consults the history endpoints.
        let again = reconcile_unknown_order(
            temp.path(),
            &mut workflow,
            fixture_at(40),
            None,
            serde_json::json!([]),
        )
        .await
        .unwrap();
        assert!(!again.absence_recorded);
        assert!(again.durable_finality);
        // Already `Complete`, so nothing is appended a second time.
        assert!(!again.workflow_completed);
        assert_eq!(workflow.state().stage(), WorkflowStage::Complete);
    }

    /// The absence evidence `record_conclusive_absence` builds, rebuilt here
    /// so a test can drive a workflow into the mid-completion state a crash
    /// would leave behind.
    fn test_absence_evidence(
        workflow: &DurableWorkflow,
        observed_through_at: DateTime<Utc>,
    ) -> ConclusiveAbsenceEvidence {
        let binding = workflow.state().binding().clone();
        let decided_at = binding.decided_at;
        let watermark = |domain: HistoryDomain, label: &str| GapFreeHistoryWatermark {
            domain,
            watermark_id: content_hash(&["test/watermark", label]),
            cursor: u64::try_from(observed_through_at.timestamp_millis()).unwrap(),
            gap_free_from_at: decided_at,
            through_at: observed_through_at,
            evidence_hash: content_hash(&["test/evidence", label]),
        };
        ConclusiveAbsenceEvidence {
            observation_id: content_hash(&["test/absence", &workflow.state().client_order_id()]),
            execution_identity_hash: binding.inventory_before.execution_identity_hash.clone(),
            client_order_id: workflow.state().client_order_id(),
            effective_expiry_at: binding.order_envelope.effective_expiry_at,
            order_history: watermark(HistoryDomain::Order, "orders"),
            fill_history: watermark(HistoryDomain::Fill, "fills"),
        }
    }

    #[tokio::test]
    async fn completion_resumes_after_a_crash_between_eligibility_and_complete() {
        // A process that exits between the eligibility append and the
        // completion append leaves the journal at
        // `StakingEligibilityRecorded`, which aggregation rejects exactly
        // like `OrderFinalized`. A later reconcile must finish it.
        let temp = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let execution_identity_hash = identity_hash(
            EXECUTION_IDENTITY_DOMAIN,
            unsigned_connector(format!("http://{}", listener.local_addr().unwrap()))
                .execution_account_address()
                .unwrap(),
        );
        drop(listener);
        let binding = decision_binding(execution_identity_hash);
        let mut workflow = open_test_workflow(temp.path(), &binding);
        workflow.prepare_order(fixture_at(2)).unwrap();
        let through = fixture_at(34);
        workflow
            .record_order_submission_absent(test_absence_evidence(&workflow, through), through)
            .expect("absence recorded");
        workflow
            .record_staking_eligibility(None, through)
            .expect("eligibility recorded");
        // The crash: `complete` never ran.
        assert_eq!(
            workflow.state().stage(),
            WorkflowStage::StakingEligibilityRecorded
        );

        let resumed = reconcile_unknown_order(
            temp.path(),
            &mut workflow,
            fixture_at(35),
            None,
            serde_json::json!([]),
        )
        .await
        .unwrap();
        // No absence to record a second time, but the completion resumes.
        assert!(!resumed.absence_recorded);
        assert!(resumed.workflow_completed);
        assert_eq!(workflow.state().stage(), WorkflowStage::Complete);
    }

    #[tokio::test]
    async fn a_journal_that_never_prepared_its_order_is_still_resolvable() {
        // A crash between `open_or_create` and `prepare_order` leaves the
        // journal in `Decided` with no pending action, and after the bound
        // expiry no action can be staged any more. Absence must still
        // resolve it, or the decision strands every later decision day.
        let temp = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let execution_identity_hash = identity_hash(
            EXECUTION_IDENTITY_DOMAIN,
            unsigned_connector(format!("http://{}", listener.local_addr().unwrap()))
                .execution_account_address()
                .unwrap(),
        );
        drop(listener);
        let binding = decision_binding(execution_identity_hash);
        let mut workflow = open_test_workflow(temp.path(), &binding);
        // Deliberately no `prepare_order` call.
        assert!(workflow.pending_prepared_order().is_err());

        let recorded = reconcile_unknown_order(
            temp.path(),
            &mut workflow,
            fixture_at(35),
            Some(serde_json::json!([])),
            serde_json::json!([]),
        )
        .await
        .unwrap();
        assert!(recorded.absence_recorded);
        assert!(recorded.durable_finality);
        assert!(recorded.workflow_completed);
        assert_eq!(workflow.state().stage(), WorkflowStage::Complete);
    }

    #[tokio::test]
    async fn a_client_order_id_present_in_venue_history_never_records_absence() {
        // `unknownOid` contradicted by the account's own history is an
        // anomaly to resolve, never an absence to record — recording it
        // would release capital for an order that may have executed.
        let temp = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let execution_identity_hash = identity_hash(
            EXECUTION_IDENTITY_DOMAIN,
            unsigned_connector(format!("http://{}", listener.local_addr().unwrap()))
                .execution_account_address()
                .unwrap(),
        );
        drop(listener);
        let binding = decision_binding(execution_identity_hash);
        let mut workflow = open_test_workflow(temp.path(), &binding);
        workflow.prepare_order(fixture_at(2)).unwrap();
        let cloid = workflow.state().client_order_id();

        // Present among orders...
        let mut fresh = open_test_workflow(temp.path(), &binding);
        assert!(matches!(
            reconcile_unknown_order(
                temp.path(),
                &mut fresh,
                fixture_at(35),
                Some(serde_json::json!([{"order": {"oid": 9, "cloid": cloid}, "status": "open"}])),
                serde_json::json!([]),
            )
            .await,
            Err(LiveProbeError::BindingMismatch(_))
        ));
        assert_eq!(fresh.state().stage(), WorkflowStage::Decided);

        // ...and present among fills.
        let mut fresh = open_test_workflow(temp.path(), &binding);
        assert!(matches!(
            reconcile_unknown_order(
                temp.path(),
                &mut fresh,
                fixture_at(35),
                Some(serde_json::json!([])),
                serde_json::json!([{
                    "coin": "@1", "px": "25", "sz": "1", "side": "B", "time": 1_000,
                    "oid": 9, "tid": 3, "fee": "0.01", "feeToken": "USDC", "cloid": cloid
                }]),
            )
            .await,
            Err(LiveProbeError::BindingMismatch(_))
        ));
        assert_eq!(fresh.state().stage(), WorkflowStage::Decided);
    }

    #[tokio::test]
    async fn reconciliation_survives_being_called_again_after_acceptance_is_recorded() {
        // Regression test for a real Codex review finding on this PR:
        // `lookup_read_only`/`reconcile_prepared_order` must never depend on
        // `DurableWorkflow::pending_prepared_order()` — that action is
        // cleared once acceptance is durably observed, so a second
        // reconcile call (to catch up on fill/finality, or after a crash)
        // must keep working even though nothing is pending any more.
        let temp = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let connector = unsigned_connector(format!("http://{}", listener.local_addr().unwrap()));
        let execution_identity_hash = identity_hash(
            EXECUTION_IDENTITY_DOMAIN,
            connector.execution_account_address().unwrap(),
        );
        let binding = decision_binding(execution_identity_hash);
        let mut workflow = open_test_workflow(temp.path(), &binding);
        assert!(workflow.pending_prepared_order().is_err());
        workflow.prepare_order(fixture_at(2)).unwrap();
        // Staged and not yet observed: available now, but must not be
        // required again once observed (that's the regression below).
        assert!(workflow.pending_prepared_order().is_ok());

        let accepted_at_ms = fixture_at(5).timestamp_millis();
        let partially_filled = serde_json::json!({
            "status": "order",
            "order": {
                "order": {"oid": 7, "origSz": "1", "sz": "0.5", "side": "B", "tif": "Ioc", "cloid": workflow.state().client_order_id(), "limitPx": "25.0", "timestamp": accepted_at_ms},
                "status": "open",
                "statusTimestamp": accepted_at_ms
            }
        });
        let one_fill = serde_json::json!([{
            "coin": "@1", "px": "25", "sz": "0.5", "side": "B", "time": 1_000,
            "oid": 7, "tid": 1, "fee": "0.01", "feeToken": "USDC"
        }]);
        let server = spawn_reconcile_responder(listener, partially_filled, one_fill, true);
        let first = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            reconcile_prepared_order(
                &connector,
                &mut workflow,
                &test_journal_path(temp.path()),
                fixture_at(20),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        server.await.unwrap();
        assert_eq!(first.exchange_order_id.as_deref(), Some("7"));
        assert!(first.fills_complete);
        assert!(!first.durable_finality);
        assert!(workflow.state().exchange_order_id().is_some());
        // Now genuinely gone, confirming this path really did stop relying
        // on it.
        assert!(workflow.pending_prepared_order().is_err());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let connector = unsigned_connector(format!("http://{}", listener.local_addr().unwrap()));
        let fully_filled = serde_json::json!({
            "status": "order",
            "order": {
                "order": {"oid": 7, "origSz": "1", "sz": "0", "side": "B", "tif": "Ioc", "cloid": workflow.state().client_order_id(), "limitPx": "25.0", "timestamp": accepted_at_ms},
                "status": "filled",
                "statusTimestamp": fixture_at(21).timestamp_millis()
            }
        });
        let both_fills = serde_json::json!([
            {
                "coin": "@1", "px": "25", "sz": "0.5", "side": "B", "time": 1_000,
                "oid": 7, "tid": 1, "fee": "0.01", "feeToken": "USDC"
            },
            {
                "coin": "@1", "px": "25", "sz": "0.5", "side": "B", "time": 1_500,
                "oid": 7, "tid": 2, "fee": "0.01", "feeToken": "USDC"
            }
        ]);
        let server = spawn_reconcile_responder(listener, fully_filled, both_fills, false);
        // The regression itself: this must succeed even though acceptance
        // was already recorded on the call above.
        let second = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            reconcile_prepared_order(
                &connector,
                &mut workflow,
                &test_journal_path(temp.path()),
                fixture_at(25),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        server.await.unwrap();
        assert_eq!(second.status, "filled");
        assert!(second.fills_complete);
        assert!(second.durable_finality);
    }

    #[tokio::test]
    async fn zero_fill_cancellation_finalizes_without_a_rejected_fill_observation() {
        // Regression test for a real Codex review finding: `observe_order_
        // fill` never accepts a zero-HYPE observation, so an IOC canceled
        // with no fills at all must skip straight to finalization rather
        // than attempt (and be rejected by) a zero-quantity fill
        // observation first.
        let temp = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let connector = unsigned_connector(format!("http://{}", listener.local_addr().unwrap()));
        let execution_identity_hash = identity_hash(
            EXECUTION_IDENTITY_DOMAIN,
            connector.execution_account_address().unwrap(),
        );
        let binding = decision_binding(execution_identity_hash);
        let mut workflow = open_test_workflow(temp.path(), &binding);
        workflow.prepare_order(fixture_at(2)).unwrap();

        let accepted_at_ms = fixture_at(5).timestamp_millis();
        let canceled_unfilled = serde_json::json!({
            "status": "order",
            "order": {
                "order": {"oid": 7, "origSz": "1", "sz": "1", "side": "B", "tif": "Ioc", "cloid": workflow.state().client_order_id(), "limitPx": "25.0", "timestamp": accepted_at_ms},
                "status": "canceled",
                "statusTimestamp": accepted_at_ms
            }
        });
        let no_fills = serde_json::json!([]);
        let server = spawn_reconcile_responder(listener, canceled_unfilled, no_fills, true);
        let observed = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            reconcile_prepared_order(
                &connector,
                &mut workflow,
                &test_journal_path(temp.path()),
                fixture_at(20),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        server.await.unwrap();

        assert_eq!(observed.status, "canceled");
        assert!(observed.filled_hype.is_zero());
        assert!(observed.fills_complete);
        assert!(observed.durable_finality);
    }

    #[tokio::test]
    async fn rejects_a_venue_reported_quantity_that_does_not_match_the_authorized_envelope() {
        // Regression test for a real Codex review finding: the observed
        // order's own quantity (filled + remaining, from orderStatus) must
        // be independently verified against the authorized envelope before
        // trusting this reconciliation as evidence *for that order* —
        // otherwise a wrong CLOID match could pass every later cumulative
        // cap check by construction, since those caps are bounded by the
        // authorized quantity, not verified against what the venue
        // actually reported.
        let temp = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let connector = unsigned_connector(format!("http://{}", listener.local_addr().unwrap()));
        let execution_identity_hash = identity_hash(
            EXECUTION_IDENTITY_DOMAIN,
            connector.execution_account_address().unwrap(),
        );
        let binding = decision_binding(execution_identity_hash);
        let mut workflow = open_test_workflow(temp.path(), &binding);
        workflow.prepare_order(fixture_at(2)).unwrap();

        // Authorized envelope is 1.0 HYPE (decision_binding's
        // original_quantity_hype); this venue response reports a 2.0 HYPE
        // order instead.
        let accepted_at_ms = fixture_at(5).timestamp_millis();
        let wrong_quantity = serde_json::json!({
            "status": "order",
            "order": {
                "order": {"oid": 7, "origSz": "2", "sz": "1", "timestamp": accepted_at_ms},
                "status": "open",
                "statusTimestamp": accepted_at_ms
            }
        });
        let no_fills = serde_json::json!([]);
        let server = spawn_reconcile_responder(listener, wrong_quantity, no_fills, false);
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            reconcile_prepared_order(
                &connector,
                &mut workflow,
                &test_journal_path(temp.path()),
                fixture_at(20),
            ),
        )
        .await
        .unwrap();
        server.await.unwrap();
        assert!(matches!(result, Err(LiveProbeError::QuantityMismatch)));
        // Nothing was durably recorded from the mismatched evidence.
        assert!(workflow.state().exchange_order_id().is_none());
    }

    #[tokio::test]
    async fn accepts_a_venue_quantity_rounded_down_onto_the_venue_size_lot() {
        // Regression test for bot-strategy#845 blocker 10, hit on the first
        // real fill: the envelope authorizes a quantity at wei precision
        // (0.30798790 HYPE for a $25 budget) but HYPE spot trades on a 0.01
        // size lot, so the venue reported `origSz` 0.3 and the old equality
        // check made the fill permanently unrecordable. A quantity the
        // venue rounded *down* onto its own lot is accepted; every later
        // cumulative cap stays bounded by the larger authorized quantity,
        // and spend stays bounded independently by `max_debit_usdc`.
        let temp = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let connector = unsigned_connector(format!("http://{}", listener.local_addr().unwrap()));
        let execution_identity_hash = identity_hash(
            EXECUTION_IDENTITY_DOMAIN,
            connector.execution_account_address().unwrap(),
        );
        let binding = decision_binding(execution_identity_hash);
        let mut workflow = open_test_workflow(temp.path(), &binding);
        workflow.prepare_order(fixture_at(2)).unwrap();

        // Authorized envelope is 1.0 HYPE; the venue accepted 0.9 and
        // filled all of it. "Completely filled" therefore has to mean the
        // accepted 0.9, not the authorized 1.0 — otherwise the fill looks
        // partial, `OrderFinality::Filled` is rejected as contradictory,
        // and the journal is driven to ManualReview instead of settling.
        let accepted_at_ms = fixture_at(5).timestamp_millis();
        let rounded_down_and_filled = serde_json::json!({
            "status": "order",
            "order": {
                "order": {"oid": 7, "origSz": "0.9", "sz": "0", "side": "B", "tif": "Ioc", "cloid": workflow.state().client_order_id(), "limitPx": "25.0", "timestamp": accepted_at_ms},
                "status": "filled",
                "statusTimestamp": accepted_at_ms
            }
        });
        let one_fill = serde_json::json!([{
            "coin": "@1", "px": "25", "sz": "0.9", "side": "B", "time": 1_000,
            "oid": 7, "tid": 1, "fee": "0.01", "feeToken": "USDC"
        }]);
        let server = spawn_reconcile_responder(listener, rounded_down_and_filled, one_fill, true);
        let observed = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            reconcile_prepared_order(
                &connector,
                &mut workflow,
                &test_journal_path(temp.path()),
                fixture_at(20),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        server.await.unwrap();

        assert_eq!(observed.status, "filled");
        assert_eq!(observed.filled_hype, HypeAtoms::from_atoms(90_000_000));
        assert!(observed.fills_complete);
        assert!(observed.durable_finality);
        assert!(workflow.state().manual_review_reason().is_none());
        assert_eq!(workflow.state().exchange_order_id(), Some("7"));
    }

    #[tokio::test]
    async fn a_rounded_down_order_filled_to_its_accepted_size_reaches_the_filled_stage() {
        // The `fully_filled` flag drives the workflow stage
        // (Filled vs PartiallyFilled), so it must also be judged against
        // the accepted quantity rather than the authorized one. Observed
        // here on an order the venue still reports as `open`, so nothing
        // finalizes and the stage after the fill observation is what the
        // flag decided.
        let temp = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let connector = unsigned_connector(format!("http://{}", listener.local_addr().unwrap()));
        let execution_identity_hash = identity_hash(
            EXECUTION_IDENTITY_DOMAIN,
            connector.execution_account_address().unwrap(),
        );
        let binding = decision_binding(execution_identity_hash);
        let mut workflow = open_test_workflow(temp.path(), &binding);
        workflow.prepare_order(fixture_at(2)).unwrap();

        // Authorized 1.0 HYPE, accepted 0.9, all 0.9 filled, still open.
        let accepted_at_ms = fixture_at(5).timestamp_millis();
        let filled_but_open = serde_json::json!({
            "status": "order",
            "order": {
                "order": {"oid": 7, "origSz": "0.9", "sz": "0", "side": "B", "tif": "Ioc", "cloid": workflow.state().client_order_id(), "limitPx": "25.0", "timestamp": accepted_at_ms},
                "status": "open",
                "statusTimestamp": accepted_at_ms
            }
        });
        let one_fill = serde_json::json!([{
            "coin": "@1", "px": "25", "sz": "0.9", "side": "B", "time": 1_000,
            "oid": 7, "tid": 1, "fee": "0.01", "feeToken": "USDC"
        }]);
        let server = spawn_reconcile_responder(listener, filled_but_open, one_fill, true);
        let observed = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            reconcile_prepared_order(
                &connector,
                &mut workflow,
                &test_journal_path(temp.path()),
                fixture_at(20),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        server.await.unwrap();

        assert!(!observed.durable_finality);
        assert_eq!(workflow.state().stage(), WorkflowStage::Filled);
    }

    #[tokio::test]
    async fn a_later_lookup_may_not_change_the_recorded_accepted_quantity() {
        // Reported by Codex on PR #58. Once recorded, the accepted quantity
        // is both the full-fill target and the cumulative fill cap, so a
        // later `orderStatus` reporting a different `origSz` for the same
        // order must fail closed rather than silently move either bound.
        let temp = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let connector = unsigned_connector(format!("http://{}", listener.local_addr().unwrap()));
        let execution_identity_hash = identity_hash(
            EXECUTION_IDENTITY_DOMAIN,
            connector.execution_account_address().unwrap(),
        );
        let binding = decision_binding(execution_identity_hash);
        let mut workflow = open_test_workflow(temp.path(), &binding);
        workflow.prepare_order(fixture_at(2)).unwrap();

        // Authorized 1.0, accepted 0.9, half of it filled and still open.
        let accepted_at_ms = fixture_at(5).timestamp_millis();
        let accepted_at_nine = serde_json::json!({
            "status": "order",
            "order": {
                "order": {"oid": 7, "origSz": "0.9", "sz": "0.4", "side": "B", "tif": "Ioc", "cloid": workflow.state().client_order_id(), "limitPx": "25.0", "timestamp": accepted_at_ms},
                "status": "open",
                "statusTimestamp": accepted_at_ms
            }
        });
        let one_fill = serde_json::json!([{
            "coin": "@1", "px": "25", "sz": "0.5", "side": "B", "time": 1_000,
            "oid": 7, "tid": 1, "fee": "0.01", "feeToken": "USDC"
        }]);
        let server = spawn_reconcile_responder(listener, accepted_at_nine, one_fill.clone(), true);
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            reconcile_prepared_order(
                &connector,
                &mut workflow,
                &test_journal_path(temp.path()),
                fixture_at(20),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        server.await.unwrap();
        assert_eq!(
            workflow.state().venue_accepted_quantity_hype(),
            Some(HypeAtoms::from_atoms(90_000_000))
        );

        // The same order, now claiming it was accepted at the full 1.0 --
        // within the authorization, so only the recorded value catches it.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let connector = unsigned_connector(format!("http://{}", listener.local_addr().unwrap()));
        let accepted_at_one = serde_json::json!({
            "status": "order",
            "order": {
                "order": {"oid": 7, "origSz": "1", "sz": "0.5", "side": "B", "tif": "Ioc", "cloid": workflow.state().client_order_id(), "limitPx": "25.0", "timestamp": accepted_at_ms},
                "status": "open",
                "statusTimestamp": accepted_at_ms
            }
        });
        let server = spawn_reconcile_responder(listener, accepted_at_one, one_fill, false);
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            reconcile_prepared_order(
                &connector,
                &mut workflow,
                &test_journal_path(temp.path()),
                fixture_at(25),
            ),
        )
        .await
        .unwrap();
        server.await.unwrap();
        assert!(matches!(result, Err(LiveProbeError::QuantityMismatch)));
        assert_eq!(
            workflow.state().venue_accepted_quantity_hype(),
            Some(HypeAtoms::from_atoms(90_000_000))
        );
    }

    #[test]
    fn a_legacy_submission_event_re_encodes_unchanged_and_replays_at_the_authorized_quantity() {
        // The journal's record hash is recomputed by *re-serializing* the
        // decoded event, so a field that materializes on decode would
        // change that encoding and make every already-written record fail
        // verification. An event from before this field existed therefore
        // has to decode to `None` and re-encode without the key. It also
        // has to replay to a usable full-fill target: the code that wrote
        // such events required the venue quantity to equal the authorized
        // quantity, so that is exactly what absence means.
        let temp = tempfile::tempdir().unwrap();
        let connector = unsigned_connector("http://127.0.0.1:1".to_owned());
        let execution_identity_hash = identity_hash(
            EXECUTION_IDENTITY_DOMAIN,
            connector.execution_account_address().unwrap(),
        );
        let binding = decision_binding(execution_identity_hash);
        let mut workflow = open_test_workflow(temp.path(), &binding);
        workflow.prepare_order(fixture_at(2)).unwrap();

        let mut legacy = test_submission_evidence(&workflow, &binding, fixture_at(5));
        legacy.venue_accepted_quantity_hype = None;
        let encoded = serde_json::to_string(&legacy).unwrap();
        assert!(!encoded.contains("venue_accepted_quantity_hype"));
        let decoded: AuthenticatedOrderSubmission = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded.venue_accepted_quantity_hype, None);
        assert_eq!(serde_json::to_string(&decoded).unwrap(), encoded);

        workflow
            .observe_order_submission(&legacy, fixture_at(5))
            .unwrap();
        assert_eq!(
            workflow.state().venue_accepted_quantity_hype(),
            Some(binding.order_envelope.original_quantity_hype)
        );
    }

    #[tokio::test]
    async fn rejects_submission_evidence_claiming_more_than_the_authorized_quantity() {
        // The accepted quantity recorded from the venue becomes the
        // full-fill target and the cumulative fill cap, so it must itself
        // be bounded by the authorized envelope: evidence claiming the
        // venue accepted *more* than was authorized cannot be allowed to
        // raise either bound.
        // A rejected observation durably records a contradiction, so each
        // case needs its own workflow.
        let temp = tempfile::tempdir().unwrap();
        let connector = unsigned_connector("http://127.0.0.1:1".to_owned());
        let execution_identity_hash = identity_hash(
            EXECUTION_IDENTITY_DOMAIN,
            connector.execution_account_address().unwrap(),
        );
        let binding = decision_binding(execution_identity_hash);
        let authorized = binding.order_envelope.original_quantity_hype;

        let case = |directory: &str, accepted: Option<HypeAtoms>| {
            let root = temp.path().join(directory);
            std::fs::create_dir_all(&root).unwrap();
            let mut workflow = open_test_workflow(&root, &binding);
            workflow.prepare_order(fixture_at(2)).unwrap();
            let mut submission = test_submission_evidence(&workflow, &binding, fixture_at(5));
            submission.venue_accepted_quantity_hype = accepted;
            workflow.observe_order_submission(&submission, fixture_at(5))
        };

        assert!(case(
            "larger",
            Some(HypeAtoms::from_atoms(authorized.as_atoms() + 1))
        )
        .is_err());
        assert!(case("zero", Some(HypeAtoms::default())).is_err());
        // The same evidence at the authorized quantity is accepted, so the
        // rejections above are attributable to the accepted quantity alone.
        case("authorized", Some(authorized)).unwrap();
        // And so is anything the venue rounded down to.
        case(
            "rounded-down",
            Some(HypeAtoms::from_atoms(authorized.as_atoms() - 1)),
        )
        .unwrap();
    }

    #[tokio::test]
    async fn rejects_a_zero_venue_reported_quantity() {
        // A venue response whose order carries no quantity at all is not
        // "our order rounded down" — it is evidence that does not describe
        // a submitted order, and accepting it would let a nonsense response
        // advance the workflow past submission.
        let temp = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let connector = unsigned_connector(format!("http://{}", listener.local_addr().unwrap()));
        let execution_identity_hash = identity_hash(
            EXECUTION_IDENTITY_DOMAIN,
            connector.execution_account_address().unwrap(),
        );
        let binding = decision_binding(execution_identity_hash);
        let mut workflow = open_test_workflow(temp.path(), &binding);
        workflow.prepare_order(fixture_at(2)).unwrap();

        let accepted_at_ms = fixture_at(5).timestamp_millis();
        let zero_quantity = serde_json::json!({
            "status": "order",
            "order": {
                "order": {"oid": 7, "origSz": "0", "sz": "0", "side": "B", "tif": "Ioc", "cloid": workflow.state().client_order_id(), "limitPx": "25.0", "timestamp": accepted_at_ms},
                "status": "canceled",
                "statusTimestamp": accepted_at_ms
            }
        });
        let no_fills = serde_json::json!([]);
        let server = spawn_reconcile_responder(listener, zero_quantity, no_fills, false);
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            reconcile_prepared_order(
                &connector,
                &mut workflow,
                &test_journal_path(temp.path()),
                fixture_at(20),
            ),
        )
        .await
        .unwrap();
        server.await.unwrap();
        assert!(matches!(result, Err(LiveProbeError::QuantityMismatch)));
        assert!(workflow.state().exchange_order_id().is_none());
    }

    #[tokio::test]
    async fn rejects_a_venue_reported_cloid_that_does_not_match_the_requested_one() {
        // Regression test for a real Codex review finding: dex-connector's
        // reconcile_order_by_client_id currently echoes the requested
        // (normalized) CLOID back as HyperliquidOrderReconciliation::
        // client_order_id rather than parsing it from the venue response,
        // so that field alone can never catch a wrong response. The raw
        // envelope's own `cloid` is the only field that genuinely reflects
        // what the venue returned, and must be independently verified.
        let temp = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let connector = unsigned_connector(format!("http://{}", listener.local_addr().unwrap()));
        let execution_identity_hash = identity_hash(
            EXECUTION_IDENTITY_DOMAIN,
            connector.execution_account_address().unwrap(),
        );
        let binding = decision_binding(execution_identity_hash);
        let mut workflow = open_test_workflow(temp.path(), &binding);
        workflow.prepare_order(fixture_at(2)).unwrap();

        let accepted_at_ms = fixture_at(5).timestamp_millis();
        let wrong_cloid = serde_json::json!({
            "status": "order",
            "order": {
                "order": {
                    "oid": 7, "origSz": "1", "sz": "1", "side": "B", "tif": "Ioc",
                    "limitPx": "25.0", "timestamp": accepted_at_ms,
                    "cloid": "0xdeadbeefdeadbeefdeadbeefdeadbeef"
                },
                "status": "open",
                "statusTimestamp": accepted_at_ms
            }
        });
        let no_fills = serde_json::json!([]);
        let server = spawn_reconcile_responder(listener, wrong_cloid, no_fills, false);
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            reconcile_prepared_order(
                &connector,
                &mut workflow,
                &test_journal_path(temp.path()),
                fixture_at(20),
            ),
        )
        .await
        .unwrap();
        server.await.unwrap();
        assert!(matches!(
            result,
            Err(LiveProbeError::BindingMismatch("order client id"))
        ));
        assert!(workflow.state().exchange_order_id().is_none());
    }

    #[tokio::test]
    async fn rejects_a_venue_reported_side_or_time_in_force_that_does_not_match_the_authorized_order(
    ) {
        // Regression test for a real Codex review finding: `orderStatus`
        // matching our CLOID and quantity but describing e.g. a sell or a
        // resting GTC order must not be accepted as though it were the
        // authorized HYPE IOC buy.
        let temp = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let connector = unsigned_connector(format!("http://{}", listener.local_addr().unwrap()));
        let execution_identity_hash = identity_hash(
            EXECUTION_IDENTITY_DOMAIN,
            connector.execution_account_address().unwrap(),
        );
        let binding = decision_binding(execution_identity_hash);
        let mut workflow = open_test_workflow(temp.path(), &binding);
        workflow.prepare_order(fixture_at(2)).unwrap();

        let accepted_at_ms = fixture_at(5).timestamp_millis();
        let wrong_side = serde_json::json!({
            "status": "order",
            "order": {
                "order": {"oid": 7, "origSz": "1", "sz": "1", "side": "A", "tif": "Ioc", "cloid": workflow.state().client_order_id(), "timestamp": accepted_at_ms},
                "status": "open",
                "statusTimestamp": accepted_at_ms
            }
        });
        let no_fills = serde_json::json!([]);
        let server = spawn_reconcile_responder(listener, wrong_side, no_fills, false);
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            reconcile_prepared_order(
                &connector,
                &mut workflow,
                &test_journal_path(temp.path()),
                fixture_at(20),
            ),
        )
        .await
        .unwrap();
        server.await.unwrap();
        assert!(matches!(
            result,
            Err(LiveProbeError::BindingMismatch("order side"))
        ));
        assert!(workflow.state().exchange_order_id().is_none());
    }

    #[tokio::test]
    async fn rejects_a_venue_reported_limit_price_that_does_not_match_the_authorized_order() {
        // Regression test for a real Codex review finding:
        // `AuthenticatedOrderSubmission.limit_price_usdc_per_hype` was
        // copied from the authorized binding rather than compared with the
        // venue response, so a mismatched limit price would still be
        // recorded (and could even finalize) as though it were the
        // authorized order.
        let temp = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let connector = unsigned_connector(format!("http://{}", listener.local_addr().unwrap()));
        let execution_identity_hash = identity_hash(
            EXECUTION_IDENTITY_DOMAIN,
            connector.execution_account_address().unwrap(),
        );
        let binding = decision_binding(execution_identity_hash);
        let mut workflow = open_test_workflow(temp.path(), &binding);
        workflow.prepare_order(fixture_at(2)).unwrap();

        // Authorized limit price is 25.0 (decision_binding's
        // limit_price_usdc_per_hype); this venue response reports 30.0.
        let accepted_at_ms = fixture_at(5).timestamp_millis();
        let wrong_price = serde_json::json!({
            "status": "order",
            "order": {
                "order": {"oid": 7, "origSz": "1", "sz": "1", "side": "B", "tif": "Ioc", "cloid": workflow.state().client_order_id(), "limitPx": "30.0", "timestamp": accepted_at_ms},
                "status": "open",
                "statusTimestamp": accepted_at_ms
            }
        });
        let no_fills = serde_json::json!([]);
        let server = spawn_reconcile_responder(listener, wrong_price, no_fills, false);
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            reconcile_prepared_order(
                &connector,
                &mut workflow,
                &test_journal_path(temp.path()),
                fixture_at(20),
            ),
        )
        .await
        .unwrap();
        server.await.unwrap();
        assert!(matches!(
            result,
            Err(LiveProbeError::BindingMismatch("order limit price"))
        ));
        assert!(workflow.state().exchange_order_id().is_none());
    }

    #[tokio::test]
    async fn accepts_a_venue_price_normalized_onto_the_venue_grid_but_not_one_above_it() {
        // Regression test for bot-strategy#845 blocker 12, hit on the first
        // real fill: Hyperliquid normalizes an order's price onto its own
        // grid (five significant figures for spot), so an authorized
        // 81.172020 came back as 81.172 and the old equality check made the
        // fill permanently unrecordable. The order is a buy, so the
        // authorized price is a ceiling: at or below it is within the
        // authorization, above it is not.
        let temp = tempfile::tempdir().unwrap();
        let execution_identity_hash = identity_hash(
            EXECUTION_IDENTITY_DOMAIN,
            unsigned_connector("http://127.0.0.1:1".to_owned())
                .execution_account_address()
                .unwrap(),
        );
        let binding = decision_binding(execution_identity_hash);
        let workflow = open_test_workflow(temp.path(), &binding);
        let cloid = workflow.state().client_order_id();
        // decision_binding authorizes 25.0 USDC per HYPE.
        let authorized = binding
            .order_envelope
            .limit_price_usdc_per_hype
            .as_decimal();
        let envelope = |limit_px: &str| {
            serde_json::json!({
                "order": {"order": {"side": "B", "tif": "Ioc", "cloid": cloid, "limitPx": limit_px}}
            })
            .to_string()
        };

        // Normalized down onto the venue grid: within the ceiling.
        verify_observed_order_envelope(&envelope("24.999"), &cloid, authorized).unwrap();
        // Exactly the authorized price still passes.
        verify_observed_order_envelope(&envelope("25.0"), &cloid, authorized).unwrap();
        // Above the ceiling: the venue would be paying more than authorized.
        assert!(matches!(
            verify_observed_order_envelope(&envelope("25.001"), &cloid, authorized),
            Err(LiveProbeError::BindingMismatch("order limit price"))
        ));
        // Non-positive does not describe a real order.
        assert!(matches!(
            verify_observed_order_envelope(&envelope("0"), &cloid, authorized),
            Err(LiveProbeError::BindingMismatch("order limit price"))
        ));
        assert!(matches!(
            verify_observed_order_envelope(&envelope("-1"), &cloid, authorized),
            Err(LiveProbeError::BindingMismatch("order limit price"))
        ));
    }

    #[tokio::test]
    async fn rejects_an_exchange_order_id_change_before_touching_the_fill_accumulator() {
        // Regression test for a real Codex review finding: the
        // exchange-order-ID consistency check must run *before* merging
        // this call's fills into the durable accumulator — otherwise a
        // rejected, wrong-order response could still poison the
        // accumulator with that other order's fill data before the
        // mismatch is caught.
        let temp = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let connector = unsigned_connector(format!("http://{}", listener.local_addr().unwrap()));
        let execution_identity_hash = identity_hash(
            EXECUTION_IDENTITY_DOMAIN,
            connector.execution_account_address().unwrap(),
        );
        let binding = decision_binding(execution_identity_hash);
        let mut workflow = open_test_workflow(temp.path(), &binding);
        workflow.prepare_order(fixture_at(2)).unwrap();

        let accepted_at_ms = fixture_at(5).timestamp_millis();
        let first_order = serde_json::json!({
            "status": "order",
            "order": {
                "order": {"oid": 7, "origSz": "1", "sz": "0.5", "side": "B", "tif": "Ioc", "cloid": workflow.state().client_order_id(), "limitPx": "25.0", "timestamp": accepted_at_ms},
                "status": "open",
                "statusTimestamp": accepted_at_ms
            }
        });
        let first_fill = serde_json::json!([{
            "coin": "@1", "px": "25", "sz": "0.5", "side": "B", "time": 1_000,
            "oid": 7, "tid": 1, "fee": "0.01", "feeToken": "USDC"
        }]);
        let server = spawn_reconcile_responder(listener, first_order, first_fill, true);
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            reconcile_prepared_order(
                &connector,
                &mut workflow,
                &test_journal_path(temp.path()),
                fixture_at(20),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        server.await.unwrap();
        assert_eq!(workflow.state().exchange_order_id(), Some("7"));

        let accumulator_path = observed_fills_path(&test_journal_path(temp.path()));
        let before = fs::read_to_string(&accumulator_path).unwrap();

        // A later lookup returns a *different* exchange order ID (99, not
        // 7), same authorized quantity and fully filled with its own fill.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let connector = unsigned_connector(format!("http://{}", listener.local_addr().unwrap()));
        let different_order = serde_json::json!({
            "status": "order",
            "order": {
                "order": {"oid": 99, "origSz": "1", "sz": "0", "side": "B", "tif": "Ioc", "cloid": workflow.state().client_order_id(), "limitPx": "25.0", "timestamp": accepted_at_ms},
                "status": "filled",
                "statusTimestamp": accepted_at_ms
            }
        });
        let different_fill = serde_json::json!([{
            "coin": "@1", "px": "25", "sz": "1", "side": "B", "time": 2_000,
            "oid": 99, "tid": 2, "fee": "0.01", "feeToken": "USDC"
        }]);
        let server = spawn_reconcile_responder(listener, different_order, different_fill, false);
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            reconcile_prepared_order(
                &connector,
                &mut workflow,
                &test_journal_path(temp.path()),
                fixture_at(25),
            ),
        )
        .await
        .unwrap();
        server.await.unwrap();
        assert!(matches!(
            result,
            Err(LiveProbeError::BindingMismatch("exchange order ID"))
        ));
        // The workflow's recorded exchange order ID and the accumulator's
        // contents must be untouched by the rejected, wrong-order response.
        assert_eq!(workflow.state().exchange_order_id(), Some("7"));
        let after = fs::read_to_string(&accumulator_path).unwrap();
        assert_eq!(before, after);
    }

    #[tokio::test]
    async fn accumulator_rejects_a_different_order_even_when_the_workflow_has_not_recorded_one_yet()
    {
        // Regression test for a real Codex review finding: a first attempt
        // can merge fills into the accumulator and then fail *before*
        // observe_order_submission ever persists an exchange_order_id on
        // the workflow — leaving workflow.state().exchange_order_id() at
        // None. Without binding the accumulator itself to the exchange
        // order ID it was first written for, a retry resolving to a
        // *different* order would sail past the workflow-level check
        // (nothing recorded yet to compare against) and merge a different
        // order's fills into the same accumulator.
        let temp = tempfile::tempdir().unwrap();
        let connector = unsigned_connector("http://127.0.0.1:1".to_owned());
        let execution_identity_hash = identity_hash(
            EXECUTION_IDENTITY_DOMAIN,
            connector.execution_account_address().unwrap(),
        );
        let binding = decision_binding(execution_identity_hash);
        let workflow = open_test_workflow(temp.path(), &binding);
        // Simulates a first attempt that merged order 7's fills into the
        // accumulator but never reached observe_order_submission.
        let accumulator_path = observed_fills_path(&test_journal_path(temp.path()));
        let workflow_id = workflow.state().workflow_id().to_string();
        let mut accumulator = load_observed_fills(&accumulator_path, &workflow_id).unwrap();
        accumulator.exchange_order_id = "7".to_string();
        merge_observed_fills(
            &mut accumulator.fills,
            &[raw_fill("1", "0.5", "12.5", "0.01")],
        )
        .unwrap();
        accumulator.content_hash = observed_fills_content_hash(
            &workflow_id,
            &accumulator.exchange_order_id,
            &accumulator.fills,
        );
        crate::status_io::write_private_json_atomic(&accumulator_path, &accumulator).unwrap();
        assert!(workflow.state().exchange_order_id().is_none());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let connector = unsigned_connector(format!("http://{}", listener.local_addr().unwrap()));
        // Re-derive the binding against the new connector's identity, and
        // re-open the same journal so the workflow_id (and thus the
        // accumulator path) stays identical.
        let execution_identity_hash = identity_hash(
            EXECUTION_IDENTITY_DOMAIN,
            connector.execution_account_address().unwrap(),
        );
        let binding = decision_binding(execution_identity_hash);
        let mut workflow = open_test_workflow(temp.path(), &binding);
        workflow.prepare_order(fixture_at(2)).unwrap();

        let accepted_at_ms = fixture_at(5).timestamp_millis();
        let different_order = serde_json::json!({
            "status": "order",
            "order": {
                "order": {"oid": 99, "origSz": "1", "sz": "0.5", "side": "B", "tif": "Ioc", "cloid": workflow.state().client_order_id(), "limitPx": "25.0", "timestamp": accepted_at_ms},
                "status": "open",
                "statusTimestamp": accepted_at_ms
            }
        });
        let different_fill = serde_json::json!([{
            "coin": "@1", "px": "25", "sz": "0.5", "side": "B", "time": 2_000,
            "oid": 99, "tid": 2, "fee": "0.01", "feeToken": "USDC"
        }]);
        let server = spawn_reconcile_responder(listener, different_order, different_fill, false);
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            reconcile_prepared_order(
                &connector,
                &mut workflow,
                &test_journal_path(temp.path()),
                fixture_at(20),
            ),
        )
        .await
        .unwrap();
        server.await.unwrap();
        assert!(matches!(
            result,
            Err(LiveProbeError::BindingMismatch(
                "exchange order ID (accumulator)"
            ))
        ));
    }

    #[tokio::test]
    async fn accepted_at_ahead_of_the_local_clock_does_not_block_recording() {
        // Regression test for a real Codex review finding: the venue clock
        // is permitted to run ahead of the local clock (up to the order
        // envelope's own max_venue_clock_lag_ms, already authorized), so
        // `accepted_at` (the venue's own order.timestamp) can legitimately
        // be later than the `now` this call was invoked with.
        let temp = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let connector = unsigned_connector(format!("http://{}", listener.local_addr().unwrap()));
        let execution_identity_hash = identity_hash(
            EXECUTION_IDENTITY_DOMAIN,
            connector.execution_account_address().unwrap(),
        );
        let binding = decision_binding(execution_identity_hash);
        let mut workflow = open_test_workflow(temp.path(), &binding);
        workflow.prepare_order(fixture_at(2)).unwrap();

        // 30s ahead of the local `now` below — well within the ~60s
        // authorized lag (decision_binding's max_venue_clock_lag_ms).
        let accepted_at_ms = (fixture_at(5) + chrono::TimeDelta::seconds(30)).timestamp_millis();
        let filled = serde_json::json!({
            "status": "order",
            "order": {
                "order": {"oid": 7, "origSz": "1", "sz": "0", "side": "B", "tif": "Ioc", "cloid": workflow.state().client_order_id(), "limitPx": "25.0", "timestamp": accepted_at_ms},
                "status": "filled",
                "statusTimestamp": accepted_at_ms
            }
        });
        let fills = serde_json::json!([{
            "coin": "@1", "px": "25", "sz": "1", "side": "B", "time": 1_000,
            "oid": 7, "tid": 1, "fee": "0.01", "feeToken": "USDC"
        }]);
        let server = spawn_reconcile_responder(listener, filled, fills, true);
        // Local `now` (fixture_at(5)) is BEFORE the venue's reported
        // acceptance time — must still succeed.
        let observed = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            reconcile_prepared_order(
                &connector,
                &mut workflow,
                &test_journal_path(temp.path()),
                fixture_at(5),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        server.await.unwrap();

        assert_eq!(observed.status, "filled");
        assert!(observed.durable_finality);
    }

    #[tokio::test]
    async fn accepted_at_too_far_ahead_of_the_local_clock_fails_closed() {
        // Regression test for a real Codex review finding: forward clock
        // skew is only authorized up to max_venue_clock_lag_ms too (not
        // just the lagging direction) — an accepted_at far enough ahead of
        // `now` that it exceeds that tolerance is a genuine anomaly and
        // must be rejected, not unconditionally clamped through as valid
        // evidence.
        let temp = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let connector = unsigned_connector(format!("http://{}", listener.local_addr().unwrap()));
        let execution_identity_hash = identity_hash(
            EXECUTION_IDENTITY_DOMAIN,
            connector.execution_account_address().unwrap(),
        );
        let binding = decision_binding(execution_identity_hash);
        let mut workflow = open_test_workflow(temp.path(), &binding);
        workflow.prepare_order(fixture_at(2)).unwrap();

        // 5 minutes ahead of `now` below — far beyond the ~60s authorized lag.
        let accepted_at_ms = (fixture_at(5) + chrono::TimeDelta::minutes(5)).timestamp_millis();
        let filled = serde_json::json!({
            "status": "order",
            "order": {
                "order": {"oid": 7, "origSz": "1", "sz": "0", "side": "B", "tif": "Ioc", "cloid": workflow.state().client_order_id(), "limitPx": "25.0", "timestamp": accepted_at_ms},
                "status": "filled",
                "statusTimestamp": accepted_at_ms
            }
        });
        let fills = serde_json::json!([{
            "coin": "@1", "px": "25", "sz": "1", "side": "B", "time": 1_000,
            "oid": 7, "tid": 1, "fee": "0.01", "feeToken": "USDC"
        }]);
        // Rejected before the account-scope lookup, so only 2 requests.
        let server = spawn_reconcile_responder(listener, filled, fills, false);
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            reconcile_prepared_order(
                &connector,
                &mut workflow,
                &test_journal_path(temp.path()),
                fixture_at(5),
            ),
        )
        .await
        .unwrap();
        server.await.unwrap();
        assert!(matches!(result, Err(LiveProbeError::InvalidVenueTimestamp)));
        assert!(workflow.state().exchange_order_id().is_none());
    }

    #[tokio::test]
    async fn accepted_at_lagging_within_the_authorized_tolerance_does_not_block_recording() {
        // Regression test for a real Codex review finding: the venue clock
        // may also run *behind* the local clock, by up to the order
        // envelope's own already-authorized max_venue_clock_lag_ms
        // (59_999ms in decision_binding's fixture). A venue timestamp
        // slightly before prepare_order's own transition time must not be
        // rejected as "predates preparation".
        let temp = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let connector = unsigned_connector(format!("http://{}", listener.local_addr().unwrap()));
        let execution_identity_hash = identity_hash(
            EXECUTION_IDENTITY_DOMAIN,
            connector.execution_account_address().unwrap(),
        );
        let binding = decision_binding(execution_identity_hash);
        let mut workflow = open_test_workflow(temp.path(), &binding);
        workflow.prepare_order(fixture_at(2)).unwrap();

        // 30s behind fixture_at(2) — well within the ~60s authorized lag.
        let accepted_at_ms = (fixture_at(2) - chrono::TimeDelta::seconds(30)).timestamp_millis();
        let filled = serde_json::json!({
            "status": "order",
            "order": {
                "order": {"oid": 7, "origSz": "1", "sz": "0", "side": "B", "tif": "Ioc", "cloid": workflow.state().client_order_id(), "limitPx": "25.0", "timestamp": accepted_at_ms},
                "status": "filled",
                "statusTimestamp": accepted_at_ms
            }
        });
        let fills = serde_json::json!([{
            "coin": "@1", "px": "25", "sz": "1", "side": "B", "time": 1_000,
            "oid": 7, "tid": 1, "fee": "0.01", "feeToken": "USDC"
        }]);
        let server = spawn_reconcile_responder(listener, filled, fills, true);
        let observed = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            reconcile_prepared_order(
                &connector,
                &mut workflow,
                &test_journal_path(temp.path()),
                fixture_at(20),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        server.await.unwrap();
        assert_eq!(observed.status, "filled");
        assert!(observed.durable_finality);
    }

    #[tokio::test]
    async fn accepted_at_lagging_beyond_the_authorized_tolerance_fails_closed() {
        // A venue timestamp far enough behind prepare_order's transition
        // time that it exceeds the authorized clock-lag tolerance is a
        // genuine anomaly, not ordinary skew, and must still be rejected
        // rather than silently normalized away.
        let temp = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let connector = unsigned_connector(format!("http://{}", listener.local_addr().unwrap()));
        let execution_identity_hash = identity_hash(
            EXECUTION_IDENTITY_DOMAIN,
            connector.execution_account_address().unwrap(),
        );
        let binding = decision_binding(execution_identity_hash);
        let mut workflow = open_test_workflow(temp.path(), &binding);
        workflow.prepare_order(fixture_at(2)).unwrap();

        // 5 minutes behind fixture_at(2) — far beyond the ~60s authorized lag.
        let accepted_at_ms = (fixture_at(2) - chrono::TimeDelta::minutes(5)).timestamp_millis();
        let filled = serde_json::json!({
            "status": "order",
            "order": {
                "order": {"oid": 7, "origSz": "1", "sz": "0", "side": "B", "tif": "Ioc", "cloid": workflow.state().client_order_id(), "limitPx": "25.0", "timestamp": accepted_at_ms},
                "status": "filled",
                "statusTimestamp": accepted_at_ms
            }
        });
        let fills = serde_json::json!([{
            "coin": "@1", "px": "25", "sz": "1", "side": "B", "time": 1_000,
            "oid": 7, "tid": 1, "fee": "0.01", "feeToken": "USDC"
        }]);
        // Rejected before the account-scope lookup, so only 2 requests.
        let server = spawn_reconcile_responder(listener, filled, fills, false);
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            reconcile_prepared_order(
                &connector,
                &mut workflow,
                &test_journal_path(temp.path()),
                fixture_at(20),
            ),
        )
        .await
        .unwrap();
        server.await.unwrap();
        assert!(matches!(result, Err(LiveProbeError::InvalidVenueTimestamp)));
        assert!(workflow.state().exchange_order_id().is_none());
    }

    #[tokio::test]
    async fn already_recorded_submission_returns_a_time_no_earlier_than_the_last_transition() {
        // Regression test for a real Codex review finding: the early-
        // return path (submission already recorded) must stay monotonic
        // with the workflow's own last transition too, not just the
        // first-observation path — otherwise a later call's fresh `now`
        // regressing behind an earlier clock-lag-clamped transition time
        // would make the caller's subsequent observe_order_fill/
        // finalize_order calls reject it as regressing.
        let temp = tempfile::tempdir().unwrap();
        let connector = unsigned_connector("http://127.0.0.1:1".to_owned());
        let execution_identity_hash = identity_hash(
            EXECUTION_IDENTITY_DOMAIN,
            connector.execution_account_address().unwrap(),
        );
        let binding = decision_binding(execution_identity_hash);
        let mut workflow = open_test_workflow(temp.path(), &binding);
        workflow.prepare_order(fixture_at(2)).unwrap();

        let submission = test_submission_evidence(&workflow, &binding, fixture_at(20));
        // Simulates an earlier call's positive clock-lag clamp having
        // pushed the recorded transition time forward to fixture_at(20).
        workflow
            .observe_order_submission(&submission, fixture_at(20))
            .unwrap();
        assert_eq!(workflow.state().last_transition_at(), fixture_at(20));

        // A later call's fresh wall-clock `now` regresses behind that.
        let result = record_order_submission_if_new(
            &connector,
            &mut workflow,
            &binding,
            "7",
            "{}",
            binding.order_envelope.original_quantity_hype,
            fixture_at(15),
        )
        .await
        .unwrap();
        assert_eq!(result, fixture_at(20));
    }

    #[tokio::test]
    async fn unsigned_recovery_rejects_wrong_account_and_market_before_network() {
        let temp = tempfile::tempdir().unwrap();
        let connector = unsigned_connector("http://127.0.0.1:1".to_owned());
        let mismatched_account = decision_binding("some-other-account-identity".to_owned());
        let workflow = open_test_workflow(temp.path(), &mismatched_account);
        assert!(matches!(
            lookup_read_only(&connector, workflow.state()).await,
            Err(LiveProbeError::BindingMismatch("execution identity"))
        ));

        let execution_identity_hash = identity_hash(
            EXECUTION_IDENTITY_DOMAIN,
            connector.execution_account_address().unwrap(),
        );
        let mut wrong_market = decision_binding(execution_identity_hash);
        wrong_market.order_envelope.market_metadata_digest = "other-market".to_owned();
        let temp = tempfile::tempdir().unwrap();
        let workflow = open_test_workflow(temp.path(), &wrong_market);
        assert!(matches!(
            lookup_read_only(&connector, workflow.state()).await,
            Err(LiveProbeError::BindingMismatch("market metadata"))
        ));
    }

    // Capture the full HTTP body rather than assuming one TCP read contains it.
    async fn unsigned_lookup_fixture(status: serde_json::Value) -> HyperliquidOrderReconciliation {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let connector = unsigned_connector(format!("http://{}", listener.local_addr().unwrap()));
        assert!(connector.api_wallet_address().is_err());
        let execution_identity_hash = identity_hash(
            EXECUTION_IDENTITY_DOMAIN,
            connector.execution_account_address().unwrap(),
        );
        let binding = decision_binding(execution_identity_hash);
        let temp = tempfile::tempdir().unwrap();
        let workflow = open_test_workflow(temp.path(), &binding);
        let expected_cloid = workflow.state().client_order_id();
        let server = tokio::spawn(async move {
            for request_type in ["userFillsByTime", "orderStatus"] {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut data = Vec::new();
                let (header_end, length) = loop {
                    let mut buffer = [0; 2048];
                    let count = socket.read(&mut buffer).await.unwrap();
                    assert!(count > 0);
                    data.extend_from_slice(&buffer[..count]);
                    if let Some(end) = data.windows(4).position(|part| part == b"\r\n\r\n") {
                        let headers = String::from_utf8_lossy(&data[..end]);
                        assert!(headers.starts_with("POST /info HTTP/1.1"));
                        let length: usize = headers
                            .lines()
                            .find_map(|line| {
                                let (key, value) = line.split_once(':')?;
                                key.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse().unwrap())
                            })
                            .unwrap();
                        break (end + 4, length);
                    }
                };
                while data.len() < header_end + length {
                    let mut buffer = [0; 2048];
                    let count = socket.read(&mut buffer).await.unwrap();
                    assert!(count > 0);
                    data.extend_from_slice(&buffer[..count]);
                }
                let request: serde_json::Value =
                    serde_json::from_slice(&data[header_end..header_end + length]).unwrap();
                assert_eq!(request["type"], request_type);
                assert_eq!(
                    request["user"],
                    "0x1111111111111111111111111111111111111111"
                );
                assert!(request.get("signature").is_none());
                assert!(request.get("action").is_none());
                if request_type == "orderStatus" {
                    assert_eq!(request["oid"], expected_cloid);
                }
                let body = if request_type == "orderStatus" {
                    status.to_string()
                } else {
                    "[]".to_owned()
                };
                let response = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        let observed = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            lookup_read_only(&connector, workflow.state()),
        )
        .await
        .unwrap()
        .unwrap();
        server.await.unwrap();
        observed
    }

    #[tokio::test]
    async fn unsigned_recovery_reads_exact_cloid_after_expiry_without_signer() {
        let observed = unsigned_lookup_fixture(serde_json::json!({
            "status": "order", "order": {
                "order": {"oid": 42, "origSz": "1", "sz": "0.25"},
                "status": "canceled"
            }
        }))
        .await;
        assert_eq!(observed.order_id.as_deref(), Some("42"));
        assert_eq!(observed.filled_size, Decimal::new(75, 2));
        assert_eq!(observed.remaining_size, Decimal::new(25, 2));
        assert_eq!(observed.status, "canceled");
    }

    #[tokio::test]
    async fn unsigned_unknown_cloid_remains_unknown_not_finalized_or_retryable() {
        let observed = unsigned_lookup_fixture(serde_json::json!({"status": "unknownOid"})).await;
        assert_eq!(observed.status, "unknownOid");
        assert_eq!(observed.order_id, None);
        assert!(observed.filled_size.is_zero());
    }

    #[test]
    fn rejects_a_fee_ceiling_at_or_above_10000_bps() {
        let temp = tempfile::tempdir().unwrap();
        let connector = test_connector(
            "0x4444444444444444444444444444444444444444",
            &temp.path().join("nonce-d.json"),
        );
        let binding = LiveProbeBinding::from_connector(&connector, "market-a").unwrap();
        assert!(matches!(
            HyperliquidLiveProbe::new(connector, binding, 10_000),
            Err(LiveProbeError::InvalidFeeCeiling)
        ));
    }

    #[test]
    fn finality_from_status_only_recognizes_confirmed_terminal_strings() {
        assert_eq!(finality_from_status("filled"), Some(OrderFinality::Filled));
        assert_eq!(
            finality_from_status("canceled"),
            Some(OrderFinality::Canceled)
        );
        // Not yet independently confirmed against a real unfilled/partially
        // filled IOC auto-cancel, and "open"/"unknownOid" are plainly
        // non-terminal — all must block finalization, not guess.
        assert_eq!(finality_from_status("open"), None);
        assert_eq!(finality_from_status("unknownOid"), None);
        assert_eq!(finality_from_status("marginCanceled"), None);
    }

    #[test]
    fn accepted_at_reads_the_stable_order_timestamp_not_the_advancing_status_timestamp() {
        // Real testnet evidence (bot-strategy#901): after a cancel,
        // `order.timestamp` stays at the original placement time while
        // `statusTimestamp` advances to the cancel time. This must read the
        // former.
        let raw = serde_json::json!({
            "order": {
                "order": {"oid": 42, "timestamp": 1_788_720_765_987u64},
                "status": "canceled",
                "statusTimestamp": 1_788_720_767_521u64
            }
        })
        .to_string();
        let accepted_at = accepted_at_from_raw_order_status(&raw).unwrap();
        assert_eq!(accepted_at.timestamp_millis(), 1_788_720_765_987);
    }

    #[test]
    fn accepted_at_rejects_a_missing_or_malformed_timestamp() {
        assert!(matches!(
            accepted_at_from_raw_order_status("{}"),
            Err(LiveProbeError::InvalidVenueTimestamp)
        ));
        assert!(matches!(
            accepted_at_from_raw_order_status("not json"),
            Err(LiveProbeError::InvalidVenueTimestamp)
        ));
        assert!(matches!(
            accepted_at_from_raw_order_status(
                &serde_json::json!({"order": {"order": {"timestamp": "not-a-number"}}}).to_string()
            ),
            Err(LiveProbeError::InvalidVenueTimestamp)
        ));
    }

    fn raw_fill(trade_id: &str, size: &str, value: &str, fee: &str) -> FilledOrder {
        FilledOrder {
            order_id: "42".to_string(),
            is_rejected: false,
            trade_id: trade_id.to_string(),
            filled_side: None,
            filled_size: Some(Decimal::from_str(size).unwrap()),
            filled_value: Some(Decimal::from_str(value).unwrap()),
            filled_fee: Some(Decimal::from_str(fee).unwrap()),
            filled_base_fee: Some(Decimal::ZERO),
            filled_ts_ms: None,
            tx_hash: None,
        }
    }

    fn accumulated(size: &str, value: &str, fee: &str) -> AccumulatedFill {
        accumulated_with_base_fee(size, value, fee, "0")
    }

    fn accumulated_with_base_fee(
        size: &str,
        value: &str,
        fee: &str,
        base_fee: &str,
    ) -> AccumulatedFill {
        AccumulatedFill {
            size: size.to_string(),
            notional: value.to_string(),
            fee: fee.to_string(),
            base_fee: base_fee.to_string(),
        }
    }

    #[test]
    fn a_fee_charged_in_hype_is_fewer_hype_credited_not_a_usdc_debit() {
        // bot-strategy#998, with the real 2026-09-10 fill's shape: 0.3 HYPE
        // matched, 0.00021 HYPE fee. dex-connector reports the fee's quote
        // value (0.00021 × px) as `fee` and the raw 0.00021 as `base_fee`.
        // The account paid exactly the notional in USDC and received
        // 0.29979 HYPE — the fee must be counted once, on the HYPE side.
        let mut fills = BTreeMap::new();
        fills.insert(
            "1".to_string(),
            accumulated_with_base_fee("0.3", "7.5", "0.00525", "0.00021"),
        );
        assert_eq!(
            cumulative_usdc_from_fills(&fills).unwrap(),
            (
                UsdcMicros::from_micros(7_500_000),
                UsdcMicros::from_micros(7_500_000)
            )
        );
        assert_eq!(
            cumulative_credited_hype_from_fills(&fills, 100_000_000).unwrap(),
            HypeAtoms::from_atoms(29_979_000)
        );

        // A fee charged in USDC is the opposite: a USDC debit, full HYPE.
        let mut fills = BTreeMap::new();
        fills.insert(
            "1".to_string(),
            accumulated_with_base_fee("0.3", "7.5", "0.0075", "0"),
        );
        assert_eq!(
            cumulative_usdc_from_fills(&fills).unwrap(),
            (
                UsdcMicros::from_micros(7_500_000),
                UsdcMicros::from_micros(7_507_500)
            )
        );
        assert_eq!(
            cumulative_credited_hype_from_fills(&fills, 100_000_000).unwrap(),
            HypeAtoms::from_atoms(30_000_000)
        );

        // A base fee larger than its own fill is corrupt, never a negative
        // credit.
        let mut fills = BTreeMap::new();
        fills.insert(
            "1".to_string(),
            accumulated_with_base_fee("0.3", "7.5", "7.5", "0.4"),
        );
        assert!(cumulative_credited_hype_from_fills(&fills, 100_000_000).is_err());
    }

    #[test]
    fn a_fill_whose_fee_asset_is_unknown_is_not_recorded_as_fee_free() {
        // `filled_base_fee: None` means the adapter does not say which asset
        // the fee left; recording it as zero base fee would silently claim
        // HYPE the account may not hold.
        let mut fill = raw_fill("1", "0.3", "7.5", "0.0075");
        fill.filled_base_fee = None;
        let mut accumulated = BTreeMap::new();
        assert!(matches!(
            merge_observed_fills(&mut accumulated, &[fill]),
            Err(LiveProbeError::InvalidDecimal("fill base fee"))
        ));
        assert!(accumulated.is_empty());
    }

    #[test]
    fn a_version_one_fill_accumulator_is_refused_not_upgraded() {
        // Its rows never captured which asset each fee was charged in, so
        // there is no correct way to fill in `base_fee`.
        let temp = tempfile::tempdir().unwrap();
        let journal = test_journal_path(temp.path());
        let legacy = serde_json::json!({
            "schema_version": 1,
            "workflow_id": "wf_legacy",
            "exchange_order_id": "7",
            "fills": {"1": {"size": "0.3", "notional": "7.5", "fee": "0.0075"}},
            "content_hash": ""
        });
        let path = observed_fills_path(&journal);
        std::fs::write(&path, legacy.to_string()).unwrap();
        let error = load_observed_fills(&path, "wf_legacy").unwrap_err();
        assert!(
            error.to_string().contains("schema version 1"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn a_legacy_fill_event_without_credited_hype_re_encodes_unchanged() {
        // Record hashes re-serialize the decoded event, so an event written
        // before `cumulative_credited_hype` existed must decode to `None`
        // and re-encode byte-for-byte; replay then treats credited as equal
        // to matched, which is what that code recorded.
        let legacy = serde_json::json!({
            "type": "order_fill_observed",
            "observation_id": "obs",
            "cumulative_hype": 30_000_000,
            "cumulative_filled_usdc": 7_500_000,
            "cumulative_debited_usdc": 7_507_500,
            "fully_filled": true
        });
        let encoded = legacy.to_string();
        let decoded: crate::workflow::WorkflowTransition = serde_json::from_str(&encoded).unwrap();
        match &decoded {
            crate::workflow::WorkflowTransition::OrderFillObserved {
                cumulative_credited_hype,
                ..
            } => assert_eq!(*cumulative_credited_hype, None),
            other => panic!("unexpected transition {other:?}"),
        }
        assert_eq!(
            serde_json::to_value(&decoded).unwrap(),
            legacy,
            "re-encoding a legacy event must not add the new field"
        );
    }

    #[tokio::test]
    async fn a_real_fill_with_its_fee_charged_in_hype_records_what_the_account_holds() {
        // End-to-end shape of the 2026-09-10 order (bot-strategy#998), at
        // the fixture's 25.0 limit: the venue matched 0.3 HYPE in full and
        // charged 0.00021 HYPE. The journal must end with purchased HYPE =
        // 0.29979 (what the account holds), matched = 0.3 (what "filled"
        // means for the order), and a USDC debit equal to the notional —
        // through both the fill observation and the finalization.
        let temp = tempfile::tempdir().unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let connector = unsigned_connector(format!("http://{}", listener.local_addr().unwrap()));
        let execution_identity_hash = identity_hash(
            EXECUTION_IDENTITY_DOMAIN,
            connector.execution_account_address().unwrap(),
        );
        let binding = decision_binding(execution_identity_hash);
        let mut workflow = open_test_workflow(temp.path(), &binding);
        workflow.prepare_order(fixture_at(2)).unwrap();

        let accepted_at_ms = fixture_at(5).timestamp_millis();
        let filled = serde_json::json!({
            "status": "order",
            "order": {
                "order": {"oid": 7, "origSz": "0.3", "sz": "0.0", "side": "B", "tif": "Ioc", "cloid": workflow.state().client_order_id(), "limitPx": "25.0", "timestamp": accepted_at_ms},
                "status": "filled",
                "statusTimestamp": accepted_at_ms
            }
        });
        let hype_fee_fill = serde_json::json!([{
            "coin": "@107", "px": "25", "sz": "0.3", "side": "B", "time": 1_000,
            "oid": 7, "tid": 1, "fee": "0.00021", "feeToken": "HYPE"
        }]);
        let server = spawn_reconcile_responder(listener, filled, hype_fee_fill, true);
        let observed = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            reconcile_prepared_order(
                &connector,
                &mut workflow,
                &test_journal_path(temp.path()),
                fixture_at(20),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        server.await.unwrap();

        assert_eq!(observed.status, "filled");
        assert!(observed.fills_complete);
        assert!(observed.durable_finality);
        assert_eq!(observed.filled_hype, HypeAtoms::from_atoms(30_000_000));
        assert_eq!(
            observed.credited_hype,
            Some(HypeAtoms::from_atoms(29_979_000))
        );
        let state = workflow.state();
        assert_eq!(state.stage(), WorkflowStage::OrderFinalized);
        assert_eq!(state.matched_hype(), HypeAtoms::from_atoms(30_000_000));
        assert_eq!(state.purchased_hype(), HypeAtoms::from_atoms(29_979_000));
        assert_eq!(state.filled_usdc(), UsdcMicros::from_micros(7_500_000));
        assert_eq!(state.debited_usdc(), UsdcMicros::from_micros(7_500_000));
    }

    #[tokio::test]
    async fn credited_hype_may_never_exceed_or_vanish_from_the_matched_quantity() {
        let temp = tempfile::tempdir().unwrap();
        let connector = unsigned_connector("http://127.0.0.1:1".to_owned());
        let execution_identity_hash = identity_hash(
            EXECUTION_IDENTITY_DOMAIN,
            connector.execution_account_address().unwrap(),
        );
        let binding = decision_binding(execution_identity_hash);
        let matched = HypeAtoms::from_atoms(50_000_000);
        let usdc = UsdcMicros::from_micros(12_500_000);

        let case = |directory: &str, credited: HypeAtoms| {
            let root = temp.path().join(directory);
            std::fs::create_dir_all(&root).unwrap();
            let mut workflow = open_test_workflow(&root, &binding);
            workflow.prepare_order(fixture_at(2)).unwrap();
            let submission = test_submission_evidence(&workflow, &binding, fixture_at(5));
            workflow
                .observe_order_submission(&submission, fixture_at(5))
                .unwrap();
            workflow.observe_order_fill("fill", matched, credited, usdc, usdc, false, fixture_at(6))
        };

        assert!(case("more-than-matched", HypeAtoms::from_atoms(50_000_001)).is_err());
        assert!(case("vanished", HypeAtoms::default()).is_err());
        case("net-of-fee", HypeAtoms::from_atoms(49_965_000)).unwrap();
        case("no-base-fee", matched).unwrap();
    }

    fn fills_map(entries: &[(&str, &str, &str, &str)]) -> BTreeMap<String, AccumulatedFill> {
        entries
            .iter()
            .map(|(trade_id, size, value, fee)| {
                ((*trade_id).to_string(), accumulated(size, value, fee))
            })
            .collect()
    }

    #[test]
    fn cumulative_usdc_sums_notional_and_fee_across_fills() {
        let fills = fills_map(&[
            ("1", "0.3", "10.692", "0.0075"),
            ("2", "0.14", "5.0", "0.0035"),
        ]);
        let (filled, debited) = cumulative_usdc_from_fills(&fills).unwrap();
        assert_eq!(
            filled,
            UsdcMicros::from_decimal(Decimal::from_str("15.692").unwrap()).unwrap()
        );
        assert_eq!(
            debited,
            UsdcMicros::from_decimal(Decimal::from_str("15.703").unwrap()).unwrap()
        );
    }

    #[test]
    fn cumulative_usdc_of_no_fills_is_zero() {
        let (filled, debited) = cumulative_usdc_from_fills(&BTreeMap::new()).unwrap();
        assert!(filled.is_zero());
        assert!(debited.is_zero());
    }

    #[test]
    fn cumulative_usdc_rejects_a_corrupt_stored_notional() {
        let fills = fills_map(&[("1", "0.3", "not-a-number", "0")]);
        assert!(matches!(
            cumulative_usdc_from_fills(&fills),
            Err(LiveProbeError::ObservedFillsAccumulator(_))
        ));
    }

    #[test]
    fn fills_covering_the_authoritative_quantity_are_complete() {
        let fills = fills_map(&[
            ("1", "0.3", "10.692", "0.0075"),
            ("2", "0.14", "5.0", "0.0035"),
        ]);
        assert!(
            fills_cover_authoritative_quantity(&fills, Decimal::from_str("0.44").unwrap()).unwrap()
        );
    }

    #[test]
    fn fills_missing_from_the_rolling_window_are_detected_as_incomplete() {
        // Only 0.3 of the authoritative 0.44 is present — the rest aged out
        // of Hyperliquid's shared recent-fill window (bot-strategy#901).
        let fills = fills_map(&[("1", "0.3", "10.692", "0.0075")]);
        assert!(
            !fills_cover_authoritative_quantity(&fills, Decimal::from_str("0.44").unwrap())
                .unwrap()
        );
    }

    #[test]
    fn merge_rejects_a_fill_missing_its_size_notional_or_fee() {
        let mut accumulated = BTreeMap::new();
        let mut missing_size = raw_fill("1", "0.3", "10", "0");
        missing_size.filled_size = None;
        assert!(matches!(
            merge_observed_fills(&mut accumulated, &[missing_size]),
            Err(LiveProbeError::InvalidDecimal("fill size"))
        ));

        let mut missing_value = raw_fill("1", "0.3", "10", "0");
        missing_value.filled_value = None;
        assert!(matches!(
            merge_observed_fills(&mut accumulated, &[missing_value]),
            Err(LiveProbeError::InvalidDecimal("fill notional"))
        ));

        // A missing fee must fail closed too, exactly like a missing
        // notional — it must never silently default to zero (a real Codex
        // review finding: that would understate cumulative_debited_usdc).
        let mut missing_fee = raw_fill("1", "0.3", "10", "0");
        missing_fee.filled_fee = None;
        assert!(matches!(
            merge_observed_fills(&mut accumulated, &[missing_fee]),
            Err(LiveProbeError::InvalidDecimal("fill fee"))
        ));
    }

    #[test]
    fn merge_is_idempotent_for_an_identical_replay_of_the_same_trade_id() {
        let mut accumulated = BTreeMap::new();
        merge_observed_fills(
            &mut accumulated,
            &[raw_fill("1", "0.3", "10.692", "0.0075")],
        )
        .unwrap();
        merge_observed_fills(
            &mut accumulated,
            &[raw_fill("1", "0.3", "10.692", "0.0075")],
        )
        .unwrap();
        assert_eq!(accumulated.len(), 1);
    }

    #[test]
    fn merge_accumulates_across_calls_and_never_drops_a_previously_seen_trade_id() {
        // The regression this exists for: a later call's narrower window
        // (only trade 2 this time — trade 1 aged out) must not erase trade
        // 1 from the durable record.
        let mut accumulated = BTreeMap::new();
        merge_observed_fills(
            &mut accumulated,
            &[raw_fill("1", "0.3", "10.692", "0.0075")],
        )
        .unwrap();
        merge_observed_fills(&mut accumulated, &[raw_fill("2", "0.14", "5.0", "0.0035")]).unwrap();
        assert_eq!(accumulated.len(), 2);
        assert!(fills_cover_authoritative_quantity(
            &accumulated,
            Decimal::from_str("0.44").unwrap()
        )
        .unwrap());
    }

    #[test]
    fn merge_rejects_the_same_trade_id_reappearing_with_different_content() {
        let mut accumulated = BTreeMap::new();
        merge_observed_fills(
            &mut accumulated,
            &[raw_fill("1", "0.3", "10.692", "0.0075")],
        )
        .unwrap();
        assert!(matches!(
            merge_observed_fills(&mut accumulated, &[raw_fill("1", "0.31", "10.7", "0.0075")]),
            Err(LiveProbeError::ContradictoryFillEvidence(id)) if id == "1"
        ));
    }

    #[test]
    fn observed_fills_round_trip_through_the_sibling_file() {
        let temp = tempfile::tempdir().unwrap();
        let journal = temp.path().join("journal.jsonl");
        let path = observed_fills_path(&journal);
        assert!(!path.exists());
        assert!(load_observed_fills(&path, "workflow-a")
            .unwrap()
            .fills
            .is_empty());

        let mut accumulator = load_observed_fills(&path, "workflow-a").unwrap();
        merge_observed_fills(
            &mut accumulator.fills,
            &[raw_fill("1", "0.3", "10.692", "0.0075")],
        )
        .unwrap();
        accumulator.content_hash =
            observed_fills_content_hash("workflow-a", "", &accumulator.fills);
        crate::status_io::write_private_json_atomic(&path, &accumulator).unwrap();

        let reloaded = load_observed_fills(&path, "workflow-a").unwrap();
        assert_eq!(reloaded.fills.len(), 1);
        assert_eq!(
            reloaded.fills.get("1").unwrap(),
            &accumulated("0.3", "10.692", "0.0075")
        );
    }

    #[test]
    fn observed_fills_rejects_a_file_belonging_to_a_different_workflow() {
        let temp = tempfile::tempdir().unwrap();
        let path = observed_fills_path(&temp.path().join("journal.jsonl"));
        let mut accumulator = load_observed_fills(&path, "workflow-a").unwrap();
        merge_observed_fills(
            &mut accumulator.fills,
            &[raw_fill("1", "0.3", "10.692", "0.0075")],
        )
        .unwrap();
        accumulator.content_hash =
            observed_fills_content_hash("workflow-a", "", &accumulator.fills);
        crate::status_io::write_private_json_atomic(&path, &accumulator).unwrap();

        assert!(matches!(
            load_observed_fills(&path, "workflow-b"),
            Err(LiveProbeError::ObservedFillsAccumulator(_))
        ));
    }

    #[test]
    fn observed_fills_rejects_content_that_does_not_match_its_own_stored_hash() {
        // Regression test for a real Codex review finding: this sidecar is
        // not covered by the journal's own protected hash chain, so a
        // stale or hand-modified copy must be detected rather than
        // silently trusted for cumulative USDC totals.
        let temp = tempfile::tempdir().unwrap();
        let path = observed_fills_path(&temp.path().join("journal.jsonl"));
        let mut accumulator = load_observed_fills(&path, "workflow-a").unwrap();
        merge_observed_fills(
            &mut accumulator.fills,
            &[raw_fill("1", "0.3", "10.692", "0.0075")],
        )
        .unwrap();
        accumulator.content_hash =
            observed_fills_content_hash("workflow-a", "", &accumulator.fills);
        crate::status_io::write_private_json_atomic(&path, &accumulator).unwrap();

        // Hand-tamper the notional without updating the stored hash.
        let mut tampered: ObservedFillsAccumulator =
            serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        tampered.fills.get_mut("1").unwrap().notional = "999".to_string();
        crate::status_io::write_private_json_atomic(&path, &tampered).unwrap();

        assert!(matches!(
            load_observed_fills(&path, "workflow-a"),
            Err(LiveProbeError::ObservedFillsAccumulator(_))
        ));
    }

    #[test]
    fn observed_fills_lock_rejects_concurrent_access_instead_of_blocking() {
        // Regression test for a real Codex review finding: `submit` and the
        // signer-free `reconcile` recovery command can run concurrently
        // against the same journal. Without a lock, one process's
        // read-merge-write could silently overwrite fills the other just
        // recorded — this must fail fast (not block or corrupt) on
        // contention instead.
        let temp = tempfile::tempdir().unwrap();
        let path = observed_fills_path(&temp.path().join("journal.jsonl"));
        let held = acquire_observed_fills_lock(&path).unwrap();
        assert!(matches!(
            acquire_observed_fills_lock(&path),
            Err(LiveProbeError::ObservedFillsAccumulator(_))
        ));
        drop(held);
        // Released: a fresh acquisition now succeeds.
        acquire_observed_fills_lock(&path).unwrap();
    }
}
