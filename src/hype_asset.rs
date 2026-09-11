//! Shared HYPE/USDC spot asset constants and conversions.
//!
//! Kept in one place so `order_envelope.rs` (envelope assembly),
//! `live_decision.rs` (inventory/decision wiring), and the `hype-live-probe`
//! binary agree on exactly the same market identity, atom scale, and
//! metadata digest — a mismatch between them would make every
//! `DecisionBinding`/`LiveProbeBinding` pairing silently fail its equality
//! check downstream. `live_probe.rs` (probe binding) never imports this
//! module: it receives the atom scale and digest as opaque values from
//! those callers instead. `workflow.rs` (durable order evidence) and
//! `monitor.rs` (read-only observer) each import only the market identity.
//!
//! The market identity and metadata digest are available in every build;
//! the atom-scale conversion is only compiled with the `live-probe` feature
//! because no default-build caller consumes it.

#[cfg(feature = "live-probe")]
use dex_connector::HyperliquidSpotOrderGrid;
#[cfg(feature = "live-probe")]
use rust_decimal::{prelude::ToPrimitive, Decimal};
use sha2::{Digest, Sha256};

pub(crate) const HYPE_SPOT_MARKET: &str = "HYPE/USDC";
/// Hyperliquid HYPE spot asset decimals (`weiDecimals`). Protocol-fixed for
/// an existing asset; the live-probe paths additionally verify it against
/// the venue's own `spotMeta` (see [`verify_hype_usdc_order_grid`]).
pub(crate) const HYPE_WEI_DECIMALS: u32 = 8;
/// Hyperliquid HYPE spot **size lot** decimals (`szDecimals`): the venue
/// only accepts order sizes on a `10^-2` HYPE grid and truncates anything
/// finer (bot-strategy#845 blocker 10: authorized 0.30798790, venue
/// `origSz` 0.3). Bound into the market-metadata digest so a prepared
/// envelope is tied to the lot it was derived on, and verified live at
/// prepare and submit time so a venue change fails closed instead of
/// silently re-rounding an authorized quantity (bot-strategy#991).
pub(crate) const HYPE_SIZE_DECIMALS: u32 = 2;
#[cfg(feature = "live-probe")]
pub(crate) const HYPE_ATOMS_PER_HYPE: u64 = 100_000_000;
/// v2: adds the size lot (`HYPE_SIZE_DECIMALS`) to the digest. A v1 binding
/// (prepared before bot-strategy#991) no longer matches; every v1 journal
/// must be terminal before a v2 binary reconciles against this account.
const MARKET_METADATA_DOMAIN: &[u8] = b"hype-accumulator/hyperliquid-hype-usdc-spot-metadata/v2";

/// Canonical digest binding `order_envelope::assemble_order_envelope_binding`'s
/// `OrderEnvelopeBinding::market_metadata_digest` and
/// `live_probe::LiveProbeBinding`'s `market_metadata_digest` to the
/// same market identity. A caller constructing a `LiveProbeBinding`
/// independently (e.g. the live-probe binary, at submit time) must pass
/// this exact value, not any other digest (in particular, not
/// `Config::effective_security_policy_digest`, which is a different,
/// policy-fingerprint concept) — passing a different value here silently
/// produces a `BindingMismatch` against the durably prepared action.
///
/// Written as plain code spans, not intra-doc links: `order_envelope` and
/// `live_probe` are only compiled with the `live-probe` feature, and a
/// default-build `cargo doc` cannot resolve a link into a module it did not
/// compile.
#[must_use]
pub fn hype_usdc_market_metadata_digest() -> String {
    let mut hasher = Sha256::new();
    hasher.update(MARKET_METADATA_DOMAIN);
    hasher.update([0]);
    hasher.update(HYPE_SPOT_MARKET.as_bytes());
    hasher.update([0]);
    hasher.update(HYPE_WEI_DECIMALS.to_be_bytes());
    hasher.update([0]);
    hasher.update(HYPE_SIZE_DECIMALS.to_be_bytes());
    format!("{:x}", hasher.finalize())
}

/// Why a venue-reported order grid cannot be the one this crate's constants
/// (and therefore its market-metadata digest) were derived for.
#[cfg(feature = "live-probe")]
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum OrderGridMismatch {
    #[error("venue spot market pair is {venue}, expected {expected}")]
    Pair {
        venue: String,
        expected: &'static str,
    },
    #[error("venue HYPE size lot has {venue} decimals, this build binds {expected}")]
    SizeDecimals { venue: u32, expected: u32 },
    #[error("venue HYPE wei scale has {venue} decimals, this build binds {expected}")]
    WeiDecimals { venue: u32, expected: u32 },
}

/// Checks the venue's live HYPE/USDC order grid against the constants this
/// crate binds into every envelope. A mismatch means the venue changed the
/// market's lot or atom scale under an existing build: nothing derived from
/// these constants may then be authorized or submitted.
///
/// # Errors
///
/// Returns which property differs; the caller fails closed.
#[cfg(feature = "live-probe")]
pub(crate) fn verify_hype_usdc_order_grid(
    grid: &HyperliquidSpotOrderGrid,
) -> Result<(), OrderGridMismatch> {
    if grid.pair != HYPE_SPOT_MARKET {
        return Err(OrderGridMismatch::Pair {
            venue: grid.pair.clone(),
            expected: HYPE_SPOT_MARKET,
        });
    }
    if grid.size_decimals != HYPE_SIZE_DECIMALS {
        return Err(OrderGridMismatch::SizeDecimals {
            venue: grid.size_decimals,
            expected: HYPE_SIZE_DECIMALS,
        });
    }
    if grid.base_wei_decimals != HYPE_WEI_DECIMALS {
        return Err(OrderGridMismatch::WeiDecimals {
            venue: grid.base_wei_decimals,
            expected: HYPE_WEI_DECIMALS,
        });
    }
    Ok(())
}

#[cfg(feature = "live-probe")]
/// Converts a decimal HYPE quantity to atoms, rounding toward zero.
///
/// Returns `None` on overflow or a negative input.
pub(crate) fn decimal_hype_to_atoms_floor(value: Decimal) -> Option<u64> {
    if value < Decimal::ZERO {
        return None;
    }
    value
        .checked_mul(Decimal::from(HYPE_ATOMS_PER_HYPE))?
        .trunc()
        .to_u64()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest_is_deterministic() {
        assert_eq!(
            hype_usdc_market_metadata_digest(),
            hype_usdc_market_metadata_digest()
        );
        assert_eq!(hype_usdc_market_metadata_digest().len(), 64);
    }

    #[cfg(feature = "live-probe")]
    fn venue_grid() -> HyperliquidSpotOrderGrid {
        HyperliquidSpotOrderGrid {
            pair: "HYPE/USDC".to_string(),
            coin: "@107".to_string(),
            asset: 10_107,
            size_decimals: 2,
            base_wei_decimals: 8,
        }
    }

    #[cfg(feature = "live-probe")]
    #[test]
    fn order_grid_verification_accepts_the_live_market_and_rejects_each_drift() {
        assert_eq!(verify_hype_usdc_order_grid(&venue_grid()), Ok(()));
        let mut other_pair = venue_grid();
        other_pair.pair = "HYPE/USDT".to_string();
        assert!(matches!(
            verify_hype_usdc_order_grid(&other_pair),
            Err(OrderGridMismatch::Pair { .. })
        ));
        let mut finer_lot = venue_grid();
        finer_lot.size_decimals = 3;
        assert_eq!(
            verify_hype_usdc_order_grid(&finer_lot),
            Err(OrderGridMismatch::SizeDecimals {
                venue: 3,
                expected: 2
            })
        );
        let mut other_scale = venue_grid();
        other_scale.base_wei_decimals = 6;
        assert_eq!(
            verify_hype_usdc_order_grid(&other_scale),
            Err(OrderGridMismatch::WeiDecimals {
                venue: 6,
                expected: 8
            })
        );
    }

    #[test]
    fn digest_binds_the_size_lot() {
        // The v2 digest is not the v1 one: a v1 binding must not silently
        // pass as bound to the lot it was never derived on.
        let mut v1 = Sha256::new();
        v1.update(b"hype-accumulator/hyperliquid-hype-usdc-spot-metadata/v1");
        v1.update([0]);
        v1.update(HYPE_SPOT_MARKET.as_bytes());
        v1.update([0]);
        v1.update(HYPE_WEI_DECIMALS.to_be_bytes());
        assert_ne!(
            hype_usdc_market_metadata_digest(),
            format!("{:x}", v1.finalize())
        );
        assert_eq!(
            hype_usdc_market_metadata_digest(),
            "5e4ccd472673d9976b00837ffcc55cd76e3dee18a4fe2c956c688354fc70059f"
        );
    }

    #[cfg(feature = "live-probe")]
    #[test]
    fn atoms_conversion_floors_and_rejects_negative() {
        assert_eq!(
            decimal_hype_to_atoms_floor(Decimal::from(1)),
            Some(100_000_000)
        );
        assert_eq!(
            decimal_hype_to_atoms_floor(Decimal::new(15, 1)),
            Some(150_000_000)
        );
        assert_eq!(decimal_hype_to_atoms_floor(Decimal::from(-1)), None);
    }
}
