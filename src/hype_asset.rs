//! Shared HYPE/USDC spot asset constants and conversions.
//!
//! Kept in one place so `order_envelope.rs` (envelope assembly),
//! `live_decision.rs` (inventory/decision wiring), `live_probe.rs` (probe
//! binding), and `workflow.rs` (durable order evidence) agree on exactly the
//! same market identity, atom scale, and metadata digest — a mismatch
//! between them would make every `DecisionBinding`/`LiveProbeBinding`
//! pairing silently fail its equality check downstream. `monitor.rs`
//! (read-only observer) shares only the market identity, not the atom scale
//! or metadata digest.
//!
//! The market identity and metadata digest are available in every build;
//! the atom-scale conversion is only compiled with the `live-probe` feature
//! because no default-build caller consumes it.

#[cfg(feature = "live-probe")]
use rust_decimal::{prelude::ToPrimitive, Decimal};
use sha2::{Digest, Sha256};

pub(crate) const HYPE_SPOT_MARKET: &str = "HYPE/USDC";
/// Hyperliquid HYPE spot asset decimals (`weiDecimals`). Protocol-fixed, not
/// queried live: spot asset metadata is not currently exposed as a public
/// dex-connector API, and this value does not change for an existing asset.
pub(crate) const HYPE_WEI_DECIMALS: u32 = 8;
#[cfg(feature = "live-probe")]
pub(crate) const HYPE_ATOMS_PER_HYPE: u64 = 100_000_000;
const MARKET_METADATA_DOMAIN: &[u8] = b"hype-accumulator/hyperliquid-hype-usdc-spot-metadata/v1";

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
    format!("{:x}", hasher.finalize())
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
