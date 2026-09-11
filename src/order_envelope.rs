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
//! validation requires `decided_at <= venue_clock_evidence_at < signed_expiry_at`
//! (see `workflow.rs::valid_expiry_binding`): the decision is dated at its
//! scheduled boundary and the venue evidence that prices the order must be
//! observed after it. This module has no `decided_at` to bind against — the
//! caller must assemble the envelope after the decision boundary it
//! executes, never from a book read before it. In practice: compute today's pacing decision
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
//!   protocol-level asset properties (wei decimals and the venue size lot).
//!   The venue's live `spotMeta` grid is verified against those constants
//!   here (`hype_asset::verify_hype_usdc_order_grid`), and the quantity and
//!   limit price are derived *on* that grid, so what is authorized is
//!   exactly what the venue will accept (bot-strategy#991): a quantity at
//!   wei precision was silently truncated to the `szDecimals` lot and a
//!   micro-USDC price to five significant figures on the first real fill
//!   (bot-strategy#845 blockers 10 and 12).
//! - Account and fee-schedule reads exist to attest current reachability
//!   (their success stamps `*_valid_through_at`); their content does not
//!   flow into the envelope, which relies only on the durable, config-bound
//!   `max_purchase_fee_bps`.

use crate::{
    hype_asset::{
        hype_usdc_market_metadata_digest, verify_hype_usdc_order_grid, OrderGridMismatch,
        HYPE_ATOMS_PER_HYPE, HYPE_SPOT_MARKET,
    },
    pacing::UsdcMicros,
    workflow::{AuthorizationInputFreshness, HypeAtoms, OrderEnvelopeBinding},
};
use chrono::{DateTime, TimeDelta, Utc};
use dex_connector::{
    DexConnector, DexError, HyperliquidConnector, HyperliquidSpotOrderGrid, OrderSide,
};
use rust_decimal::{prelude::ToPrimitive, Decimal};
use sha2::{Digest, Sha256};
use thiserror::Error;

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
    #[error("venue order grid does not match this build's bound market metadata: {0}")]
    OrderGrid(#[from] OrderGridMismatch),
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
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
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

    // The venue's own lot/tick rule, verified against the constants the
    // digest below binds, before anything is derived on it.
    let grid = connector.spot_order_grid(HYPE_SPOT_MARKET).await?;
    verify_hype_usdc_order_grid(&grid)?;

    let venue_book = connector
        .get_order_book_with_venue_time(HYPE_SPOT_MARKET, policy.order_book_depth)
        .await?;
    // Local clock read AFTER the response: future venue evidence is rejected.
    let fetched_at = Utc::now();
    let venue_clock_evidence_at = millis_to_datetime(venue_book.venue_time_ms)?;
    if venue_clock_evidence_at > fetched_at {
        return Err(OrderEnvelopeError::InvalidExpiryWindow(
            "venue clock evidence is ahead of the local clock at fetch time",
        ));
    }
    let best_ask = venue_book
        .book
        .asks
        .first()
        .ok_or(OrderEnvelopeError::EmptyBook)?;

    // Read-only reachability attestations; their content is not otherwise
    // used. Independent of each other and of the book/price computation
    // above, so run them concurrently.
    tokio::try_join!(connector.get_combined_balance(), connector.get_user_fees())?;

    let limit_price = limit_price_on_grid(
        worst_case_price_with_slippage(best_ask.price, policy.max_slippage_bps)?,
        &grid,
    )?;
    // Quantity from the micro-rounded limit, so the (rounded-up) fill
    // notional in `workflow.rs::max_fill_notional_usdc` never exceeds the plan.
    let limit_price_usdc_per_hype = decimal_to_usdc_micros(limit_price)?;
    let original_quantity_hype =
        quantity_for_budget(planned_usdc, limit_price_usdc_per_hype.as_decimal(), &grid)?;

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
    let earliest_deadline = input_freshness.earliest_deadline();

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
    // `workflow.rs::valid_expiry_binding` additionally requires
    // `signed_expiry_at > decided_at`, which this module cannot check (see
    // module doc: decided_at is a caller obligation). It DOES require
    // `signed_expiry_at` to be in the future relative to assembly time,
    // which is checkable here: a policy where `order_timeout_seconds` is too
    // small relative to `max_venue_clock_lag_ms` would otherwise silently
    // produce an already-expired envelope.
    if signed_expiry_at <= now {
        return Err(OrderEnvelopeError::InvalidExpiryWindow(
            "signed expiry is not after assembly time; order_timeout_seconds is too small \
             relative to max_venue_clock_lag_ms",
        ));
    }

    let l1_nonce = connector.reserve_l1_action_nonce().await?;

    Ok(OrderEnvelopeBinding {
        signer_identity_hash,
        original_quantity_hype,
        hype_atoms_per_hype: HYPE_ATOMS_PER_HYPE,
        market_metadata_digest: hype_usdc_market_metadata_digest(),
        limit_price_usdc_per_hype,
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
    crate::bps::apply_bps_markup(best_ask, max_slippage_bps)
        .ok_or(OrderEnvelopeError::InvalidDecimal("limit price"))
}

/// Normalizes a buy's worst-case limit price onto the venue's price tick
/// (rounding down, the way the connector's order path does for a buy), and
/// requires the result to be exactly representable in micro-USDC — the
/// precision the envelope binds — so the price that is authorized is the
/// price the venue accepts, byte for byte.
fn limit_price_on_grid(
    worst_case: Decimal,
    grid: &HyperliquidSpotOrderGrid,
) -> Result<Decimal, OrderEnvelopeError> {
    let on_grid = grid.normalize_price(worst_case, OrderSide::Long);
    if on_grid <= Decimal::ZERO {
        return Err(OrderEnvelopeError::InvalidDecimal("limit price"));
    }
    if decimal_to_usdc_micros(on_grid)?.as_decimal() != on_grid.normalize() {
        return Err(OrderEnvelopeError::InvalidDecimal(
            "limit price on the venue grid is not representable in micro-USDC",
        ));
    }
    Ok(on_grid)
}

fn quantity_for_budget(
    planned_usdc: UsdcMicros,
    limit_price: Decimal,
    grid: &HyperliquidSpotOrderGrid,
) -> Result<HypeAtoms, OrderEnvelopeError> {
    if limit_price <= Decimal::ZERO {
        return Err(OrderEnvelopeError::InvalidDecimal("limit price"));
    }
    let quantity_hype = planned_usdc
        .as_decimal()
        .checked_div(limit_price)
        .ok_or(OrderEnvelopeError::InvalidDecimal("quantity"))?;
    // Always round the quantity down: overspending the planned budget is
    // never acceptable, and the live-probe's own debit-cap check
    // (`live_probe.rs`'s `apply_bps_markup`-based debit bound) independently
    // re-bounds this at submission time regardless. Down onto the venue's
    // size lot, not merely to wei: the venue truncates to the lot itself,
    // and an authorized quantity finer than the lot is one the venue can
    // never report back (bot-strategy#991).
    let atoms = grid
        .floor_size(quantity_hype)
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
    UsdcMicros::from_decimal(value).ok_or(OrderEnvelopeError::InvalidDecimal("usdc micros"))
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

    /// The live HYPE/USDC grid (`spotMeta` on 2026-09-10: `szDecimals` 2,
    /// `weiDecimals` 8).
    fn grid() -> HyperliquidSpotOrderGrid {
        HyperliquidSpotOrderGrid {
            pair: "HYPE/USDC".to_string(),
            coin: "@107".to_string(),
            asset: 10_107,
            size_decimals: 2,
            base_wei_decimals: 8,
        }
    }

    /// A hypothetical wei-fine lot, to show the lot itself is what floors.
    fn wei_fine_grid() -> HyperliquidSpotOrderGrid {
        HyperliquidSpotOrderGrid {
            size_decimals: 8,
            ..grid()
        }
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
        let exact = quantity_for_budget(
            UsdcMicros::from_micros(25_000_000),
            Decimal::from(25),
            &grid(),
        )
        .unwrap();
        assert_eq!(exact, HypeAtoms::from_atoms(HYPE_ATOMS_PER_HYPE));

        // $10 at a price that doesn't divide evenly must floor, never round up.
        let floored = quantity_for_budget(
            UsdcMicros::from_micros(10_000_000),
            Decimal::from(3),
            &grid(),
        )
        .unwrap();
        // 10/3 = 3.333...HYPE -> floor onto the 0.01 lot, never overspending $10.
        assert_eq!(floored, HypeAtoms::from_atoms(333_000_000));
        let spent = Decimal::from(floored.as_atoms()) / Decimal::from(HYPE_ATOMS_PER_HYPE)
            * Decimal::from(3);
        assert!(spent <= Decimal::from(10));

        assert!(matches!(
            quantity_for_budget(UsdcMicros::from_micros(1), Decimal::ZERO, &grid()),
            Err(OrderEnvelopeError::InvalidDecimal("limit price"))
        ));
        assert!(matches!(
            quantity_for_budget(
                UsdcMicros::from_micros(1),
                Decimal::from(1_000_000_000),
                &grid()
            ),
            Err(OrderEnvelopeError::ZeroQuantity)
        ));
    }

    #[tokio::test]
    async fn rejects_venue_clock_evidence_from_the_future() {
        let spot_meta = serde_json::json!({
            "universe": [{"name": "HYPE/USDC", "tokens": [1, 0], "index": 0, "isCanonical": true}],
            "tokens": [
                {"name": "USDC", "szDecimals": 2, "weiDecimals": 6, "index": 0},
                {"name": "HYPE", "szDecimals": 2, "weiDecimals": 8, "index": 1},
            ],
        })
        .to_string();
        // Venue timestamp one minute ahead of this process's clock.
        let future_ms =
            u64::try_from((Utc::now() + TimeDelta::minutes(1)).timestamp_millis()).unwrap();
        let l2_book = serde_json::json!({
            "coin": "HYPE",
            "time": future_ms,
            "levels": [
                [{"px": "24.9", "sz": "100", "n": 1}],
                [{"px": "25.0", "sz": "100", "n": 1}],
            ],
        })
        .to_string();
        let responses = std::collections::HashMap::from([
            ("spotMeta", spot_meta),
            ("l2Book", l2_book),
            (
                "spotClearinghouseState",
                serde_json::json!({"balances": []}).to_string(),
            ),
            ("allMids", serde_json::json!({}).to_string()),
            ("userFees", serde_json::json!({}).to_string()),
        ]);
        // Assembly must stop right after the book: spotMeta + l2Book only.
        let (address, server) = spawn_typed_mock_server(responses, 2).await;
        let nonce_path = std::env::temp_dir().join(format!(
            "hype-future-evidence-nonce-{}.json",
            std::process::id()
        ));
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
            max_taker_book_age_ms: 600_000,
        })
        .unwrap();
        let now = Utc::now();
        let result = assemble_order_envelope_binding(
            &connector,
            "signer-identity-hash-a".to_string(),
            UsdcMicros::from_micros(25_000_000),
            now + TimeDelta::hours(1),
            now + TimeDelta::hours(1),
            &policy(),
            now,
        )
        .await;
        server.await.unwrap();
        let _ = std::fs::remove_file(nonce_path);
        assert!(matches!(
            result,
            Err(OrderEnvelopeError::InvalidExpiryWindow(
                "venue clock evidence is ahead of the local clock at fetch time"
            ))
        ));
    }

    #[tokio::test]
    async fn refuses_to_derive_anything_on_a_venue_grid_that_drifted() {
        // The venue reports a finer lot than this build's digest binds.
        let spot_meta = serde_json::json!({
            "universe": [{"name": "HYPE/USDC", "tokens": [1, 0], "index": 0, "isCanonical": true}],
            "tokens": [
                {"name": "USDC", "szDecimals": 2, "weiDecimals": 6, "index": 0},
                {"name": "HYPE", "szDecimals": 3, "weiDecimals": 8, "index": 1},
            ],
        })
        .to_string();
        let responses = std::collections::HashMap::from([("spotMeta", spot_meta)]);
        // Assembly must stop at the grid: no book, account or fee request.
        let (address, server) = spawn_typed_mock_server(responses, 1).await;
        let nonce_path =
            std::env::temp_dir().join(format!("hype-grid-drift-nonce-{}.json", std::process::id()));
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

        let now = Utc::now();
        let result = assemble_order_envelope_binding(
            &connector,
            "signer-identity-hash-a".to_string(),
            UsdcMicros::from_micros(25_000_000),
            now + TimeDelta::hours(1),
            now + TimeDelta::hours(1),
            &policy(),
            now,
        )
        .await;
        server.await.unwrap();
        let _ = std::fs::remove_file(nonce_path);

        assert!(matches!(
            result,
            Err(OrderEnvelopeError::OrderGrid(
                OrderGridMismatch::SizeDecimals {
                    venue: 3,
                    expected: 2
                }
            ))
        ));
    }

    #[test]
    fn quantity_from_the_micro_rounded_limit_never_exceeds_the_budget_after_ceil() {
        // The bound limit price is rounded to micros (nearest, so possibly
        // UP). `workflow.rs::max_fill_notional_usdc` computes
        // ceil(atoms * limit_micros / atoms_per_hype) and the binding rejects
        // it above `planned_usdc`. Deriving the quantity from the unrounded
        // price can overshoot by one micro; deriving it from the rounded
        // price cannot. Sweep asks whose 20 bps markup has 7 decimals.
        // On a wei-fine lot, so the sweep still exercises the micro-rounding
        // argument itself: the live 0.01 lot floors far more coarsely and
        // would hide it.
        let grid = wei_fine_grid();
        let planned = UsdcMicros::from_micros(25_000_000);
        let scale = u128::from(HYPE_ATOMS_PER_HYPE);
        let ceil_notional = |atoms: u64, limit_micros: u64| -> u128 {
            (u128::from(atoms) * u128::from(limit_micros)).div_ceil(scale)
        };
        let mut unrounded_overshoots = 0;
        for tenth in 0..1_000u32 {
            // 80.0001 .. 80.1000 in 0.0001 steps: five significant digits,
            // the venue's own tick granularity for this price band.
            let best_ask = Decimal::from(800_001 + tenth) / Decimal::from(10_000);
            let limit = worst_case_price_with_slippage(best_ask, 20).unwrap();
            let limit_micros = decimal_to_usdc_micros(limit).unwrap();
            let rounded_atoms =
                quantity_for_budget(planned, limit_micros.as_decimal(), &grid).unwrap();
            assert!(
                ceil_notional(rounded_atoms.as_atoms(), limit_micros.as_micros())
                    <= u128::from(planned.as_micros()),
                "ask {best_ask}: rounded-price quantity overshoots"
            );
            let unrounded_atoms = quantity_for_budget(planned, limit, &grid).unwrap();
            if ceil_notional(unrounded_atoms.as_atoms(), limit_micros.as_micros())
                > u128::from(planned.as_micros())
            {
                unrounded_overshoots += 1;
            }
        }
        // The sweep must actually contain the failure mode being fixed.
        assert!(unrounded_overshoots > 0);
    }

    /// The first real fill (bot-strategy#845 blockers 10 and 12), re-derived
    /// on the venue grid: the envelope now authorizes exactly what the venue
    /// accepted (`origSz` 0.3 at `limitPx` 81.172) instead of 0.30798790 at
    /// 81.172020.
    #[test]
    fn derives_the_first_real_fill_on_the_venue_grid() {
        // 81.172020 = best ask 81.01 × (1 + 20 bps).
        let best_ask = Decimal::from_str("81.01").unwrap();
        let worst_case = worst_case_price_with_slippage(best_ask, 20).unwrap();
        assert_eq!(worst_case, Decimal::from_str("81.17202").unwrap());
        let limit = limit_price_on_grid(worst_case, &grid()).unwrap();
        assert_eq!(limit, Decimal::from_str("81.172").unwrap());
        let limit_micros = decimal_to_usdc_micros(limit).unwrap();
        assert_eq!(limit_micros.as_micros(), 81_172_000);
        let quantity =
            quantity_for_budget(UsdcMicros::from_micros(25_000_000), limit, &grid()).unwrap();
        assert_eq!(quantity, HypeAtoms::from_atoms(30_000_000));
        // Both are fixed points of the connector's own order-path
        // normalization, so they reach the venue unchanged.
        assert_eq!(
            grid().floor_size(Decimal::from_str("0.3").unwrap()),
            Decimal::from_str("0.3").unwrap()
        );
        assert_eq!(grid().normalize_price(limit, OrderSide::Long), limit);
        // And still inside the plan: 0.3 × 81.172 = 24.3516 ≤ 25.
        assert!(
            Decimal::from(quantity.as_atoms()) / Decimal::from(HYPE_ATOMS_PER_HYPE) * limit
                <= Decimal::from(25)
        );
    }

    #[test]
    fn limit_price_on_grid_rounds_a_buy_down_and_rejects_the_unrepresentable() {
        // Rounding is toward the cheaper side for a buy: the authorized
        // ceiling never rises above the worst case that was computed.
        let worst_case = Decimal::from_str("81.17299").unwrap();
        assert_eq!(
            limit_price_on_grid(worst_case, &grid()).unwrap(),
            Decimal::from_str("81.172").unwrap()
        );
        // A four-digit price has a 0.1 tick: 1234.5678 → 1234.5.
        assert_eq!(
            limit_price_on_grid(Decimal::from_str("1234.5678").unwrap(), &grid()).unwrap(),
            Decimal::from_str("1234.5").unwrap()
        );
        // A sub-dollar price on a fine lot lands on a tick finer than a
        // micro-USDC; the envelope cannot bind it exactly, so it is refused
        // rather than silently re-rounded a second time.
        let fine = HyperliquidSpotOrderGrid {
            size_decimals: 0,
            ..grid()
        };
        assert!(matches!(
            limit_price_on_grid(Decimal::from_str("0.0012345678").unwrap(), &fine),
            Err(OrderEnvelopeError::InvalidDecimal(
                "limit price on the venue grid is not representable in micro-USDC"
            ))
        ));
        assert!(matches!(
            limit_price_on_grid(Decimal::ZERO, &grid()),
            Err(OrderEnvelopeError::InvalidDecimal("limit price"))
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

    /// Responds to `/info` POSTs by dispatching on the request body's
    /// `"type"` field, one accepted connection handled concurrently per
    /// request. Order-independent by design: `assemble_order_envelope_binding`
    /// now issues some requests concurrently (`tokio::try_join!`), and a
    /// position-ordered mock would be an inaccurate, fragile stand-in for
    /// what dex-connector's internals actually request.
    async fn spawn_typed_mock_server(
        responses: std::collections::HashMap<&'static str, String>,
        expected_requests: usize,
    ) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            // Accept and hand off each connection to its own task as soon as
            // it arrives, rather than pre-accepting a fixed batch: some
            // requests are only sent after an earlier one's response is
            // received (e.g. `l2Book` waits on a prior `spotMeta`), so
            // accepting up front would deadlock waiting for a connection the
            // client has not opened yet.
            let mut tasks = Vec::with_capacity(expected_requests);
            for _ in 0..expected_requests {
                let (mut socket, _) = listener.accept().await.unwrap();
                let responses = responses.clone();
                tasks.push(tokio::spawn(async move {
                    let mut buffer = vec![0_u8; 8192];
                    let n = socket.read(&mut buffer).await.unwrap();
                    let request = String::from_utf8_lossy(&buffer[..n]);
                    let body = request.split("\r\n\r\n").nth(1).unwrap_or_default();
                    let parsed: serde_json::Value = serde_json::from_str(body).unwrap();
                    let request_type = parsed["type"].as_str().unwrap_or_default();
                    let body = responses
                        .get(request_type)
                        .unwrap_or_else(|| panic!("unexpected /info type: {request_type}"));
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    socket.write_all(response.as_bytes()).await.unwrap();
                }));
            }
            for task in tasks {
                task.await.unwrap();
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

        let responses = std::collections::HashMap::from([
            ("spotMeta", spot_meta),
            ("l2Book", l2_book),
            ("spotClearinghouseState", spot_state),
            ("allMids", all_mids),
            ("userFees", user_fees),
        ]);
        // spotMeta is requested twice (l2Book's spot-symbol resolution, and
        // get_combined_balance's unconditional refresh); every other type once.
        let (address, server) = spawn_typed_mock_server(responses, 6).await;

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
        // …and onto the venue's 0.01 lot: 25 / 25.05 = 0.998003… → 0.99.
        assert_eq!(
            envelope.original_quantity_hype,
            HypeAtoms::from_atoms(99_000_000)
        );
        assert_eq!(envelope.limit_price_usdc_per_hype.as_micros(), 25_050_000);
        assert_eq!(
            envelope.market_metadata_digest,
            hype_usdc_market_metadata_digest()
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
