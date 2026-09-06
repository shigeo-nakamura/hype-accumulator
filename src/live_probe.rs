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
        AuthenticatedOrderSubmission, DurableWorkflow, ExternalAction, HypeAtoms, OrderFinality,
        WorkflowError, WorkflowState,
    },
};
use chrono::{DateTime, Utc};
use dex_connector::{
    DexError, FilledOrder, HyperliquidConnector, HyperliquidL1ActionEnvelope,
    HyperliquidOrderReconciliation, OrderSide,
};
use rust_decimal::{prelude::ToPrimitive, Decimal};
use sha2::{Digest, Sha256};
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
pub struct ProbeReconciliation {
    pub client_order_id: String,
    pub exchange_order_id: Option<String>,
    pub status: String,
    pub filled_hype: HypeAtoms,
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
        record_reconciliation(&self.connector, workflow, evidence, now).await
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
    now: DateTime<Utc>,
) -> Result<ProbeReconciliation, LiveProbeError> {
    let evidence = lookup_read_only(connector, workflow.state()).await?;
    record_reconciliation(connector, workflow, evidence, now).await
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
async fn record_reconciliation(
    connector: &HyperliquidConnector,
    workflow: &mut DurableWorkflow,
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
    // Hyperliquid's fill history is a bounded window shared across the whole
    // account, not scoped to this order: a fill can age out of range while
    // `orderStatus`'s origSz−sz still authoritatively reports the order as
    // filled (dex-connector's `authoritative_filled_size`). Computing
    // cumulative USDC from an incomplete `fills` list would silently
    // understate it, so fill/finality recording is gated on the list
    // actually summing to the authoritative filled quantity. See
    // docs/runbooks/live-probe-recovery.md.
    let fills_complete = fills_cover_authoritative_quantity(&evidence.fills, evidence.filled_size)?;

    if let Some(exchange_order_id) = evidence.order_id.clone() {
        if workflow.state().exchange_order_id().is_none() {
            let account_scope_raw = connector.spot_state_raw().await?;
            let accepted_at = accepted_at_from_raw_order_status(&evidence.raw_order_status)?;
            now = now.max(accepted_at);
            let submission = AuthenticatedOrderSubmission {
                observation_id: content_hash(&[
                    "hype-accumulator/order-submission-observation/v1",
                    &evidence.raw_order_status,
                    &account_scope_raw,
                ]),
                account_scope_evidence_hash: content_hash(&[&account_scope_raw]),
                order_envelope_evidence_hash: content_hash(&[&evidence.raw_order_status]),
                execution_identity_hash: binding.inventory_before.execution_identity_hash.clone(),
                signer_identity_hash: binding.order_envelope.signer_identity_hash.clone(),
                decision_id: binding.decision_id.clone(),
                client_order_id: workflow.state().client_order_id(),
                exchange_order_id: exchange_order_id.clone(),
                canonical_order_envelope_hash: workflow.state().canonical_order_envelope_hash()?,
                planned_usdc: binding.planned_usdc,
                max_debit_usdc: binding.committed_usdc,
                original_quantity_hype: binding.order_envelope.original_quantity_hype,
                hype_atoms_per_hype,
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
        }

        if fills_complete {
            let (cumulative_filled_usdc, cumulative_debited_usdc) =
                cumulative_usdc_from_fills(&evidence.fills)?;
            // `observe_order_fill` never accepts a zero-HYPE observation
            // (`validate_cumulative_fill` allows zero only for a Canceled/
            // Expired *finalization*, not a bare fill observation) — a
            // freshly accepted order still open with no fills yet, or an
            // IOC canceled unfilled, must skip straight to finalization
            // (when terminal) rather than recording a fill observation that
            // would always be rejected.
            if !cumulative_hype.is_zero() {
                let fully_filled = cumulative_hype == binding.order_envelope.original_quantity_hype;
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
                    cumulative_filled_usdc,
                    cumulative_debited_usdc,
                    fully_filled,
                    now,
                )?;
            }

            if let Some(finality) = finality_from_status(&evidence.status) {
                workflow.finalize_order(
                    cumulative_hype,
                    cumulative_filled_usdc,
                    cumulative_debited_usdc,
                    finality,
                    now,
                )?;
            }
        }
    }

    Ok(ProbeReconciliation {
        client_order_id: evidence.client_order_id,
        exchange_order_id: evidence.order_id,
        status: evidence.status,
        filled_hype: cumulative_hype,
        remaining_hype,
        fills_complete,
        // Reflects the workflow's actual durable state, not just whether
        // *this* call reached `finalize_order` — an earlier call may already
        // have finalized it, and this one's fills could be incomplete.
        durable_finality: order_already_finalized(workflow.state().stage()),
    })
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

/// Whether `fills` (a rolling, account-wide-bounded window) fully accounts
/// for `authoritative_filled_size` (from `orderStatus`'s own origSz−sz,
/// which dex-connector's `authoritative_filled_size` treats as authoritative
/// even when the fills list is truncated). A caller must never compute
/// cumulative USDC totals from an incomplete fills list: a fill can age out
/// of Hyperliquid's shared recent-fill window while `orderStatus` still
/// correctly reports the order as filled, which would silently understate
/// the total. See docs/runbooks/live-probe-recovery.md.
///
/// # Errors
///
/// Returns an error for a fill missing its size or on overflow summing it.
fn fills_cover_authoritative_quantity(
    fills: &[FilledOrder],
    authoritative_filled_size: Decimal,
) -> Result<bool, LiveProbeError> {
    let mut total = Decimal::ZERO;
    for fill in fills {
        let size = fill
            .filled_size
            .ok_or(LiveProbeError::InvalidDecimal("fill size"))?;
        total = total
            .checked_add(size)
            .ok_or(LiveProbeError::InvalidDecimal("cumulative fill size"))?;
    }
    Ok(total == authoritative_filled_size)
}

/// Sums each fill's notional and fee (both already quote-denominated by
/// `dex-connector`, regardless of which token the fee was actually charged
/// in) into cumulative filled and debited USDC, matching the same
/// notional-plus-fee-markup semantics `PreparedIocOrder::from_action`'s
/// pre-submission worst-case-debit check already uses.
fn cumulative_usdc_from_fills(
    fills: &[FilledOrder],
) -> Result<(UsdcMicros, UsdcMicros), LiveProbeError> {
    let mut filled = Decimal::ZERO;
    let mut fee = Decimal::ZERO;
    for fill in fills {
        let value = fill
            .filled_value
            .ok_or(LiveProbeError::InvalidDecimal("fill notional"))?;
        filled = filled
            .checked_add(value)
            .ok_or(LiveProbeError::InvalidDecimal("cumulative fill notional"))?;
        fee = fee
            .checked_add(fill.filled_fee.unwrap_or(Decimal::ZERO))
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

    fn open_test_workflow(dir: &Path, binding: &DecisionBinding) -> DurableWorkflow {
        let head_store: Arc<dyn ProtectedWorkflowHeadStore> = Arc::new(
            FileProtectedWorkflowHeadStore::new(dir.join("journal.protected-head.json")).unwrap(),
        );
        let owner_store: Arc<dyn ExchangeOrderOwnerStore> = Arc::new(
            FileExchangeOrderOwnerStore::new(dir.join("exchange-order-owners.json")).unwrap(),
        );
        DurableWorkflow::open_or_create(dir.join("journal.jsonl"), binding, head_store, owner_store)
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
                "order": {"oid": 7, "origSz": "1", "sz": "0.5", "timestamp": accepted_at_ms},
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
            reconcile_prepared_order(&connector, &mut workflow, fixture_at(20)),
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
                "order": {"oid": 7, "origSz": "1", "sz": "0", "timestamp": accepted_at_ms},
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
            reconcile_prepared_order(&connector, &mut workflow, fixture_at(25)),
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
                "order": {"oid": 7, "origSz": "1", "sz": "1", "timestamp": accepted_at_ms},
                "status": "canceled",
                "statusTimestamp": accepted_at_ms
            }
        });
        let no_fills = serde_json::json!([]);
        let server = spawn_reconcile_responder(listener, canceled_unfilled, no_fills, true);
        let observed = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            reconcile_prepared_order(&connector, &mut workflow, fixture_at(20)),
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

        let accepted_at_ms = fixture_at(10).timestamp_millis();
        let filled = serde_json::json!({
            "status": "order",
            "order": {
                "order": {"oid": 7, "origSz": "1", "sz": "0", "timestamp": accepted_at_ms},
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
        // acceptance time (fixture_at(10)) — must still succeed.
        let observed = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            reconcile_prepared_order(&connector, &mut workflow, fixture_at(5)),
        )
        .await
        .unwrap()
        .unwrap();
        server.await.unwrap();

        assert_eq!(observed.status, "filled");
        assert!(observed.durable_finality);
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

    fn fill(size: &str, value: &str, fee: &str) -> FilledOrder {
        FilledOrder {
            order_id: "42".to_string(),
            is_rejected: false,
            trade_id: "1".to_string(),
            filled_side: None,
            filled_size: Some(Decimal::from_str(size).unwrap()),
            filled_value: Some(Decimal::from_str(value).unwrap()),
            filled_fee: Some(Decimal::from_str(fee).unwrap()),
            filled_ts_ms: None,
            tx_hash: None,
        }
    }

    #[test]
    fn cumulative_usdc_sums_notional_and_fee_across_fills() {
        let fills = [
            fill("0.3", "10.692", "0.0075"),
            fill("0.14", "5.0", "0.0035"),
        ];
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
        let (filled, debited) = cumulative_usdc_from_fills(&[]).unwrap();
        assert!(filled.is_zero());
        assert!(debited.is_zero());
    }

    #[test]
    fn cumulative_usdc_rejects_a_fill_missing_its_notional() {
        let mut missing_value = fill("0.3", "10", "0");
        missing_value.filled_value = None;
        assert!(matches!(
            cumulative_usdc_from_fills(&[missing_value]),
            Err(LiveProbeError::InvalidDecimal("fill notional"))
        ));
    }

    #[test]
    fn fills_covering_the_authoritative_quantity_are_complete() {
        let fills = [
            fill("0.3", "10.692", "0.0075"),
            fill("0.14", "5.0", "0.0035"),
        ];
        assert!(
            fills_cover_authoritative_quantity(&fills, Decimal::from_str("0.44").unwrap()).unwrap()
        );
    }

    #[test]
    fn fills_missing_from_the_rolling_window_are_detected_as_incomplete() {
        // Only 0.3 of the authoritative 0.44 is present — the rest aged out
        // of Hyperliquid's shared recent-fill window (bot-strategy#901).
        let fills = [fill("0.3", "10.692", "0.0075")];
        assert!(
            !fills_cover_authoritative_quantity(&fills, Decimal::from_str("0.44").unwrap())
                .unwrap()
        );
    }

    #[test]
    fn fills_completeness_rejects_a_fill_missing_its_size() {
        let mut missing_size = fill("0.3", "10.692", "0.0075");
        missing_size.filled_size = None;
        assert!(matches!(
            fills_cover_authoritative_quantity(&[missing_size], Decimal::from_str("0.3").unwrap()),
            Err(LiveProbeError::InvalidDecimal("fill size"))
        ));
    }
}
