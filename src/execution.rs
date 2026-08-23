use crate::{
    config::ExecutionConfig,
    exchange::{Exchange, ExchangeError, OrderIntent, Submission},
};
pub struct Executor<E> {
    exchange: E,
    limits: ExecutionConfig,
}
impl<E: Exchange> Executor<E> {
    #[must_use]
    pub const fn new(exchange: E, limits: ExecutionConfig) -> Self {
        Self { exchange, limits }
    }

    /// Validates notional limits and submits one order intent.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::Rejected`] for a non-finite, non-positive, or
    /// oversized notional, or propagates an error from the exchange boundary.
    pub fn execute(&mut self, notional_usdc: f64) -> Result<Submission, ExchangeError> {
        if !notional_usdc.is_finite()
            || notional_usdc <= 0.0
            || notional_usdc > self.limits.max_order_usdc
        {
            return Err(ExchangeError::Rejected(
                "notional exceeds configured limits".into(),
            ));
        }
        self.exchange.submit(&OrderIntent {
            notional_usdc,
            max_slippage_bps: self.limits.max_slippage_bps,
        })
    }
}
