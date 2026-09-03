//! Assembles one [`OrderEnvelopeBinding`] from live Hyperliquid market and
//! account state.
//!
//! This module only reads. It never signs, submits, or constructs a signer;
//! it produces the immutable, fully-bound envelope that
//! [`DecisionBinding::from_pacing_decision`](crate::workflow::DecisionBinding::from_pacing_decision)
//! and [`crate::live_probe::HyperliquidLiveProbe`] consume downstream.
//!
//! # Caller obligations this module cannot enforce
//!
//! [`DurableWorkflow::open_or_create`](crate::workflow::DurableWorkflow::open_or_create)'s
//! validation requires `venue_clock_evidence_at <= decided_at < signed_expiry_at`
//! (see `workflow.rs::valid_expiry_binding`). This module has no `decided_at`
//! to bind against — the caller must stamp its `DailyDecision.decided_at` at
//! a moment between calling this function and using its result, not reuse an
//! older decision. In practice: recompute today's pacing decision
//! immediately before calling this, not after.
//!
//! # Judgment calls made here (flagged for review, not settled elsewhere)
//!
//! - `decision_valid_through_at` has no dedicated staleness config field in
//!   [`SecurityExecutionPolicy`]; this reuses `signal_stale_after_seconds`
//!   (a decision is only as fresh as the signal it was computed from).
//! - The book's best ask is taken as `asks.first()`, assuming the connector
//!   returns levels best-first (as Hyperliquid's `l2Book` does).
//! - `market_metadata_digest` is a fixed digest of HYPE/USDC's static,
//!   protocol-level asset properties (decimals), not a live query — spot
//!   asset metadata is not currently exposed as a public dex-connector API,
//!   and these properties do not change.
//! - Account and fee-schedule reads exist to attest current reachability
//!   (their success stamps `*_valid_through_at`); their content does not
//!   flow into the envelope, which relies only on the durable, config-bound
//!   `max_purchase_fee_bps`.

use crate::{
    pacing::UsdcMicros,
    workflow::{AuthorizationInputFreshness, HypeAtoms, OrderEnvelopeBinding},
};
use chrono::{DateTime, TimeDelta, Utc};
use dex_connector::{DexConnector, DexError, HyperliquidConnector};
use rust_decimal::{prelude::ToPrimitive, Decimal};
use sha2::{Digest, Sha256};
use thiserror::Error;

const HYPE_SPOT_MARKET: &str = "HYPE/USDC";
/// Hyperliquid HYPE spot asset decimals (`weiDecimals`). Protocol-fixed, not
/// queried live: spot asset metadata is not currently exposed as a public
/// dex-connector API, and this value does not change for an existing asset.
const HYPE_WEI_DECIMALS: u32 = 8;
const HYPE_ATOMS_PER_HYPE: u64 = 100_000_000;
const USDC_MICROS_PER_USDC: u64 = 1_000_000;
const BPS_DENOMINATOR: u16 = 10_000;
const MARKET_METADATA_DOMAIN: &[u8] = b"hype-accumulator/hyperliquid-hype-usdc-spot-metadata/v1";

/// Read-only freshness/pricing policy this assembly binds against. Every
/// field is sourced from the operator's approved `SecurityPolicy`
/// (`security_policy.execution`), never invented locally.
#[derive(Clone, Copy, Debug)]
pub struct OrderEnvelopeFreshnessPolicy {
    pub max_venue_clock_lag_ms: u64,
    pub venue_clock_evidence_stale_after_seconds: u64,
    pub book_stale_after_seconds: u64,
    pub account_history_stale_after_seconds: u64,
    pub fee_schedule_stale_after_seconds: u64,
    pub signal_stale_after_seconds: u64,
    pub order_timeout_seconds: u64,
    pub max_slippage_bps: u16,
    pub order_book_depth: usize,
}

#[derive(Debug, Error)]
pub enum OrderEnvelopeError {
    #[error("Hyperliquid connector error: {0}")]
    Connector(#[from] DexError),
    #[error("order book has no ask levels")]
    EmptyBook,
    #[error("planned notional must be positive")]
    NonPositivePlanned,
    #[error("computed order quantity is zero")]
    ZeroQuantity,
    #[error("invalid decimal: {0}")]
    InvalidDecimal(&'static str),
    #[error("venue-reported time is not representable")]
    InvalidVenueTime,
    #[error("computed expiry window is invalid: {0}")]
    InvalidExpiryWindow(&'static str),
}

/// Assembles a fully-bound [`OrderEnvelopeBinding`] for one HYPE/USDC spot
/// buy of up to `planned_usdc`, using live Hyperliquid book/account/fee
/// state read through `connector`.
///
/// `now` is the assembly reference instant (injected for determinism);
/// callers pass `Utc::now()` in production. `signal_evidence_valid_through_at`
/// and `policy_acknowledgement_valid_through_at` are supplied by the caller
/// because they originate from state this module does not own (the signal
/// snapshot actually used for the pacing decision, and the approved
/// `SecurityPolicy`'s acknowledgement expiry).
///
/// # Errors
///
/// Propagates connector failures, and rejects an empty order book, a
/// non-positive planned notional, a zero computed quantity, or an expiry
/// window that a misconfigured policy makes internally inconsistent.
///
/// # Panics
///
/// Never in practice: the internal `.min()` over `AuthorizationInputFreshness`
/// runs over a fixed six-element array, which is never empty.
#[allow(clippy::too_many_arguments)]
pub async fn assemble_order_envelope_binding(
    connector: &HyperliquidConnector,
    signer_identity_hash: String,
    planned_usdc: UsdcMicros,
    signal_evidence_valid_through_at: DateTime<Utc>,
    policy_acknowledgement_valid_through_at: DateTime<Utc>,
    policy: &OrderEnvelopeFreshnessPolicy,
    now: DateTime<Utc>,
) -> Result<OrderEnvelopeBinding, OrderEnvelopeError> {
    let now = truncate_to_millis(now);
    if planned_usdc.is_zero() {
        return Err(OrderEnvelopeError::NonPositivePlanned);
    }

    let venue_book = connector
        .get_order_book_with_venue_time(HYPE_SPOT_MARKET, policy.order_book_depth)
        .await?;
    let venue_clock_evidence_at = millis_to_datetime(venue_book.venue_time_ms)?;
    let best_ask = venue_book
        .book
        .asks
        .first()
        .ok_or(OrderEnvelopeError::EmptyBook)?;

    // Read-only reachability attestations; their content is not otherwise used.
    connector.get_combined_balance().await?;
    connector.get_user_fees().await?;

    let limit_price = worst_case_price_with_slippage(best_ask.price, policy.max_slippage_bps)?;
    let original_quantity_hype = quantity_for_budget(planned_usdc, limit_price)?;

    let book_evidence_valid_through_at =
        now + seconds(policy.book_stale_after_seconds, "book_stale_after_seconds")?;
    let account_evidence_valid_through_at = now
        + seconds(
            policy.account_history_stale_after_seconds,
            "account_history_stale_after_seconds",
        )?;
    let fee_schedule_valid_through_at = now
        + seconds(
            policy.fee_schedule_stale_after_seconds,
            "fee_schedule_stale_after_seconds",
        )?;
    // No dedicated decision-staleness config exists; a decision is only as
    // fresh as the signal it was computed from (see module doc).
    let decision_valid_through_at = now
        + seconds(
            policy.signal_stale_after_seconds,
            "signal_stale_after_seconds",
        )?;
    let venue_clock_evidence_valid_through_at = venue_clock_evidence_at
        + seconds(
            policy.venue_clock_evidence_stale_after_seconds,
            "venue_clock_evidence_stale_after_seconds",
        )?;

    let input_freshness = AuthorizationInputFreshness {
        decision_valid_through_at,
        signal_evidence_valid_through_at,
        book_evidence_valid_through_at,
        account_evidence_valid_through_at,
        fee_schedule_valid_through_at,
        policy_acknowledgement_valid_through_at,
    };
    let earliest_deadline = [
        input_freshness.decision_valid_through_at,
        input_freshness.signal_evidence_valid_through_at,
        input_freshness.book_evidence_valid_through_at,
        input_freshness.account_evidence_valid_through_at,
        input_freshness.fee_schedule_valid_through_at,
        input_freshness.policy_acknowledgement_valid_through_at,
    ]
    .into_iter()
    .min()
    .expect("fixed non-empty field set");

    let requested_expiry = now + seconds(policy.order_timeout_seconds, "order_timeout_seconds")?;
    let effective_expiry_at = requested_expiry.min(earliest_deadline);
    if effective_expiry_at <= now {
        return Err(OrderEnvelopeError::InvalidExpiryWindow(
            "effective expiry does not leave a positive window after now",
        ));
    }
    if venue_clock_evidence_valid_through_at <= effective_expiry_at {
        return Err(OrderEnvelopeError::InvalidExpiryWindow(
            "venue clock evidence does not outlive the effective expiry",
        ));
    }
    let lag_ms = i64::try_from(policy.max_venue_clock_lag_ms)
        .map_err(|_| OrderEnvelopeError::InvalidExpiryWindow("max_venue_clock_lag_ms overflow"))?;
    let offset = TimeDelta::try_milliseconds(lag_ms.checked_add(1).ok_or(
        OrderEnvelopeError::InvalidExpiryWindow("max_venue_clock_lag_ms overflow"),
    )?)
    .ok_or(OrderEnvelopeError::InvalidExpiryWindow(
        "max_venue_clock_lag_ms out of range",
    ))?;
    let signed_expiry_at = effective_expiry_at.checked_sub_signed(offset).ok_or(
        OrderEnvelopeError::InvalidExpiryWindow("signed expiry underflow"),
    )?;

    let l1_nonce = connector.reserve_l1_action_nonce().await?;

    Ok(OrderEnvelopeBinding {
        signer_identity_hash,
        original_quantity_hype,
        hype_atoms_per_hype: HYPE_ATOMS_PER_HYPE,
        market_metadata_digest: hype_usdc_market_metadata_digest(),
        limit_price_usdc_per_hype: decimal_to_usdc_micros(limit_price)?,
        l1_nonce,
        signed_expiry_at,
        effective_expiry_at,
        venue_clock_evidence_at,
        venue_clock_evidence_valid_through_at,
        venue_clock_evidence_digest: venue_clock_evidence_digest(venue_book.venue_time_ms),
        max_venue_clock_lag_ms: policy.max_venue_clock_lag_ms,
        input_freshness,
    })
}

fn seconds(value: u64, field: &'static str) -> Result<TimeDelta, OrderEnvelopeError> {
    let seconds =
        i64::try_from(value).map_err(|_| OrderEnvelopeError::InvalidExpiryWindow(field))?;
    TimeDelta::try_seconds(seconds).ok_or(OrderEnvelopeError::InvalidExpiryWindow(field))
}

fn truncate_to_millis(at: DateTime<Utc>) -> DateTime<Utc> {
    let millis = at.timestamp_millis();
    DateTime::from_timestamp_millis(millis).unwrap_or(at)
}

fn millis_to_datetime(millis: u64) -> Result<DateTime<Utc>, OrderEnvelopeError> {
    let millis = i64::try_from(millis).map_err(|_| OrderEnvelopeError::InvalidVenueTime)?;
    DateTime::from_timestamp_millis(millis).ok_or(OrderEnvelopeError::InvalidVenueTime)
}

fn worst_case_price_with_slippage(
    best_ask: Decimal,
    max_slippage_bps: u16,
) -> Result<Decimal, OrderEnvelopeError> {
    let multiplier = Decimal::from(BPS_DENOMINATOR)
        .checked_add(Decimal::from(max_slippage_bps))
        .ok_or(OrderEnvelopeError::InvalidDecimal("slippage multiplier"))?;
    best_ask
        .checked_mul(multiplier)
        .and_then(|value| value.checked_div(Decimal::from(BPS_DENOMINATOR)))
        .ok_or(OrderEnvelopeError::InvalidDecimal("limit price"))
}

fn quantity_for_budget(
    planned_usdc: UsdcMicros,
    limit_price: Decimal,
) -> Result<HypeAtoms, OrderEnvelopeError> {
    if limit_price <= Decimal::ZERO {
        return Err(OrderEnvelopeError::InvalidDecimal("limit price"));
    }
    let planned = Decimal::from(planned_usdc.as_micros())
        .checked_div(Decimal::from(USDC_MICROS_PER_USDC))
        .ok_or(OrderEnvelopeError::InvalidDecimal("planned notional"))?;
    let quantity_hype = planned
        .checked_div(limit_price)
        .ok_or(OrderEnvelopeError::InvalidDecimal("quantity"))?;
    // Always round the quantity down: overspending the planned budget is
    // never acceptable, and the live-probe's own debit-cap check
    // (`live_probe.rs::worst_case_debit_with_fee`) independently re-bounds
    // this at submission time regardless.
    let atoms = quantity_hype
        .checked_mul(Decimal::from(HYPE_ATOMS_PER_HYPE))
        .ok_or(OrderEnvelopeError::InvalidDecimal("quantity atoms"))?
        .trunc()
        .to_u64()
        .ok_or(OrderEnvelopeError::InvalidDecimal("quantity atoms"))?;
    if atoms == 0 {
        return Err(OrderEnvelopeError::ZeroQuantity);
    }
    Ok(HypeAtoms::from_atoms(atoms))
}

fn decimal_to_usdc_micros(value: Decimal) -> Result<UsdcMicros, OrderEnvelopeError> {
    let micros = value
        .checked_mul(Decimal::from(USDC_MICROS_PER_USDC))
        .ok_or(OrderEnvelopeError::InvalidDecimal("usdc micros"))?
        .round()
        .to_u64()
        .ok_or(OrderEnvelopeError::InvalidDecimal("usdc micros"))?;
    Ok(UsdcMicros::from_micros(micros))
}

fn hype_usdc_market_metadata_digest() -> String {
    let mut hasher = Sha256::new();
    hasher.update(MARKET_METADATA_DOMAIN);
    hasher.update([0]);
    hasher.update(HYPE_SPOT_MARKET.as_bytes());
    hasher.update([0]);
    hasher.update(HYPE_WEI_DECIMALS.to_be_bytes());
    format!("{:x}", hasher.finalize())
}

fn venue_clock_evidence_digest(venue_time_ms: u64) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"hype-accumulator/hyperliquid-venue-clock-evidence/v1");
    hasher.update([0]);
    hasher.update(venue_time_ms.to_be_bytes());
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use dex_connector::{HyperliquidAccountConfig, HyperliquidConnectorConfig};
    use std::str::FromStr;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    const TEST_SIGNER_KEY: &str =
        "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";

    fn at(second: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(second, 0).single().unwrap()
    }

    fn policy() -> OrderEnvelopeFreshnessPolicy {
        OrderEnvelopeFreshnessPolicy {
            max_venue_clock_lag_ms: 2_000,
            venue_clock_evidence_stale_after_seconds: 30,
            book_stale_after_seconds: 5,
            account_history_stale_after_seconds: 30,
            fee_schedule_stale_after_seconds: 3_600,
            signal_stale_after_seconds: 3_600,
            order_timeout_seconds: 10,
            max_slippage_bps: 20,
            order_book_depth: 5,
        }
    }

    #[test]
    fn slippage_multiplier_is_exact_and_monotonic() {
        let base = Decimal::from(100);
        assert_eq!(
            worst_case_price_with_slippage(base, 0).unwrap(),
            Decimal::from(100)
        );
        // 20 bps = 0.2% of 100 = 100.2
        assert_eq!(
            worst_case_price_with_slippage(base, 20).unwrap(),
            Decimal::new(1002, 1)
        );
    }

    #[test]
    fn quantity_rounds_down_and_rejects_nonpositive_price() {
        // $25 budget at $25/HYPE with 8 decimals = exactly 1.0 HYPE.
        let exact =
            quantity_for_budget(UsdcMicros::from_micros(25_000_000), Decimal::from(25)).unwrap();
        assert_eq!(exact, HypeAtoms::from_atoms(HYPE_ATOMS_PER_HYPE));

        // $10 at a price that doesn't divide evenly must floor, never round up.
        let floored =
            quantity_for_budget(UsdcMicros::from_micros(10_000_000), Decimal::from(3)).unwrap();
        // 10/3 = 3.333...HYPE -> floor at 8 decimals, never overspending $10.
        let spent = Decimal::from(floored.as_atoms()) / Decimal::from(HYPE_ATOMS_PER_HYPE)
            * Decimal::from(3);
        assert!(spent <= Decimal::from(10));

        assert!(matches!(
            quantity_for_budget(UsdcMicros::from_micros(1), Decimal::ZERO),
            Err(OrderEnvelopeError::InvalidDecimal("limit price"))
        ));
        assert!(matches!(
            quantity_for_budget(UsdcMicros::from_micros(1), Decimal::from(1_000_000_000)),
            Err(OrderEnvelopeError::ZeroQuantity)
        ));
    }

    #[test]
    fn millis_conversions_round_trip_and_reject_negative() {
        let dt = millis_to_datetime(1_700_000_000_123).unwrap();
        assert_eq!(dt.timestamp_millis(), 1_700_000_000_123);
        assert!(matches!(
            millis_to_datetime(u64::MAX),
            Err(OrderEnvelopeError::InvalidVenueTime)
        ));
    }

    #[test]
    fn truncate_to_millis_drops_submillisecond_precision() {
        let with_nanos = at(100) + TimeDelta::microseconds(123_456);
        let truncated = truncate_to_millis(with_nanos);
        assert_eq!(truncated.timestamp_subsec_nanos() % 1_000_000, 0);
        assert_eq!(truncated.timestamp_millis(), with_nanos.timestamp_millis());
    }

    #[test]
    fn digests_are_deterministic_and_distinct_per_input() {
        assert_eq!(
            hype_usdc_market_metadata_digest(),
            hype_usdc_market_metadata_digest()
        );
        assert_eq!(hype_usdc_market_metadata_digest().len(), 64);
        assert_ne!(
            venue_clock_evidence_digest(1),
            venue_clock_evidence_digest(2)
        );
        assert_eq!(venue_clock_evidence_digest(1).len(), 64);
    }

    fn test_nonce_path() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "hype-accumulator-order-envelope-{}-{}.json",
            std::process::id(),
            at(0).timestamp_nanos_opt().unwrap_or_default()
        ))
    }

    /// Responds to a fixed, ordered sequence of `/info` POSTs with canned
    /// bodies, one connection per request (matching this crate's existing
    /// "Connection: close" mock-server tests). This mirrors
    /// `assemble_order_envelope_binding`'s exact current call sequence
    /// (spotMeta, l2Book, spotClearinghouseState, spotMeta, allMids,
    /// userFees) rather than dispatching by request type, so it is
    /// intentionally coupled to dex-connector's present internals; a
    /// reordering there is expected to require updating this list, not a
    /// sign this test is wrong.
    async fn spawn_ordered_mock_server(
        responses: Vec<String>,
    ) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            for body in responses {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = [0_u8; 8192];
                assert!(socket.read(&mut request).await.unwrap() > 0);
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        (address, handle)
    }

    #[tokio::test]
    async fn assembles_a_complete_envelope_end_to_end() {
        let spot_meta = serde_json::json!({
            "universe": [{"name": "HYPE/USDC", "tokens": [1, 0], "index": 0, "isCanonical": true}],
            "tokens": [
                {"name": "USDC", "szDecimals": 2, "weiDecimals": 6, "index": 0},
                {"name": "HYPE", "szDecimals": 2, "weiDecimals": 8, "index": 1},
            ],
        })
        .to_string();
        let l2_book = serde_json::json!({
            "coin": "HYPE",
            "time": 1_700_000_000_000_u64,
            "levels": [
                [{"px": "24.9", "sz": "100", "n": 1}],
                [{"px": "25.0", "sz": "100", "n": 1}, {"px": "25.1", "sz": "100", "n": 1}],
            ],
        })
        .to_string();
        let spot_state = serde_json::json!({"balances": []}).to_string();
        let all_mids = serde_json::json!({}).to_string();
        let user_fees = serde_json::json!({}).to_string();

        let (address, server) = spawn_ordered_mock_server(vec![
            spot_meta.clone(),
            l2_book,
            spot_state,
            spot_meta,
            all_mids,
            user_fees,
        ])
        .await;

        let nonce_path = test_nonce_path();
        let connector = HyperliquidConnector::new(HyperliquidConnectorConfig {
            base_url: format!("http://{address}"),
            tracked_symbols: Vec::new(),
        })
        .unwrap()
        .with_account(HyperliquidAccountConfig {
            account_address: "0x0000000000000000000000000000000000000001".to_string(),
            signer_private_key: Some(TEST_SIGNER_KEY.to_string()),
            vault_address: None,
            is_mainnet: false,
            nonce_state_path: Some(nonce_path.clone()),
            max_taker_notional: None,
            max_taker_slippage_bps: None,
            max_taker_book_age_ms: 60_000,
        })
        .unwrap();

        let now = at(1_700_000_000);
        let envelope = assemble_order_envelope_binding(
            &connector,
            "signer-identity-hash-a".to_string(),
            UsdcMicros::from_micros(25_000_000),
            now + TimeDelta::hours(1),
            now + TimeDelta::hours(1),
            &policy(),
            now,
        )
        .await
        .unwrap();

        server.await.unwrap();
        let _ = std::fs::remove_file(nonce_path);

        // Best ask is 25.0; quantity floors to stay within the $25 budget.
        assert!(
            Decimal::from(envelope.original_quantity_hype.as_atoms())
                / Decimal::from(HYPE_ATOMS_PER_HYPE)
                * Decimal::from_str("25.05").unwrap()
                <= Decimal::from(25)
        );
        assert_eq!(envelope.hype_atoms_per_hype, HYPE_ATOMS_PER_HYPE);
        assert_eq!(
            envelope.venue_clock_evidence_at.timestamp_millis(),
            1_700_000_000_000
        );
        assert!(envelope.effective_expiry_at > now);
        assert_eq!(
            envelope.signed_expiry_at,
            envelope.effective_expiry_at - TimeDelta::milliseconds(2_001)
        );
        assert!(envelope.venue_clock_evidence_valid_through_at > envelope.effective_expiry_at);
    }
}
