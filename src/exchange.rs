use thiserror::Error;
#[derive(Clone, Debug, PartialEq)]
pub struct OrderIntent {
    pub notional_usdc: f64,
    pub max_slippage_bps: u16,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Submission {
    Simulated,
    Accepted(String),
}
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ExchangeError {
    #[error("live exchange implementation is unavailable")]
    LiveUnavailable,
    #[error("exchange rejected action: {0}")]
    Rejected(String),
}
pub trait Exchange: Send {
    fn mode(&self) -> &'static str;

    /// Submits an intent to the configured exchange boundary.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError`] when the intent is rejected or the selected
    /// exchange implementation cannot perform live actions.
    fn submit(&mut self, intent: &OrderIntent) -> Result<Submission, ExchangeError>;
}
#[derive(Default)]
pub struct DryRunExchange {
    simulated: Vec<OrderIntent>,
}
impl DryRunExchange {
    #[must_use]
    pub fn simulated(&self) -> &[OrderIntent] {
        &self.simulated
    }
}
impl Exchange for DryRunExchange {
    fn mode(&self) -> &'static str {
        "dry-run"
    }
    fn submit(&mut self, intent: &OrderIntent) -> Result<Submission, ExchangeError> {
        self.simulated.push(intent.clone());
        Ok(Submission::Simulated)
    }
}
pub struct UnavailableLiveExchange;
impl Exchange for UnavailableLiveExchange {
    fn mode(&self) -> &'static str {
        "live-unavailable"
    }
    fn submit(&mut self, _: &OrderIntent) -> Result<Submission, ExchangeError> {
        Err(ExchangeError::LiveUnavailable)
    }
}
