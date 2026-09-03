//! Shared exact basis-point markup arithmetic.
//!
//! Used wherever a worst-case value must be grossed up by a bps ceiling
//! before a pre-submission capital-bound check (fee ceilings in
//! `live_probe.rs`, slippage ceilings in `order_envelope.rs`). Kept in one
//! place so a future rounding/overflow-handling change applies uniformly
//! rather than drifting between independent copies.

use rust_decimal::Decimal;

pub(crate) const BPS_DENOMINATOR: u16 = 10_000;

/// Returns `base * (10_000 + bps) / 10_000`, exact (no precision loss: this
/// is division by a power of ten in a decimal type). `None` on overflow.
pub(crate) fn apply_bps_markup(base: Decimal, bps: u16) -> Option<Decimal> {
    let multiplier = Decimal::from(BPS_DENOMINATOR).checked_add(Decimal::from(bps))?;
    base.checked_mul(multiplier)?
        .checked_div(Decimal::from(BPS_DENOMINATOR))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_bps_is_identity() {
        assert_eq!(
            apply_bps_markup(Decimal::from(100), 0),
            Some(Decimal::from(100))
        );
    }

    #[test]
    fn positive_bps_grosses_up_exactly() {
        // 100 bps = 1% of 100 = 101, exact.
        assert_eq!(
            apply_bps_markup(Decimal::from(100), 100),
            Some(Decimal::from(101))
        );
    }

    #[test]
    fn overflow_returns_none() {
        assert_eq!(apply_bps_markup(Decimal::MAX, 1), None);
    }
}
