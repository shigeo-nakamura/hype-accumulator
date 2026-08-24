use crate::{clock::Clock, config::RuntimeActionPolicy};
use chrono::{DateTime, Utc};
use thiserror::Error;
#[derive(Clone, Debug, PartialEq)]
pub struct OrderIntent {
    pub notional_usdc: f64,
    pub max_slippage_bps: u16,
    /// Hard aggregate ceiling for venue, builder, and other purchase fees.
    pub max_purchase_fee_bps: u16,
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
    /// Implementations must reject an action when its authoritative aggregate
    /// purchase fee can exceed `intent.max_purchase_fee_bps`. Returns
    /// [`ExchangeError`] when the intent is rejected or the selected exchange
    /// implementation cannot perform live actions.
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

pub(crate) fn validate_order_intent(
    intent: &OrderIntent,
    policy: &RuntimeActionPolicy,
    now: DateTime<Utc>,
) -> Result<(), ExchangeError> {
    if policy
        .acknowledgement_expires_at
        .is_some_and(|expires_at| now >= expires_at)
    {
        return Err(ExchangeError::Rejected(
            "live acknowledgement is expired".to_owned(),
        ));
    }
    if !intent.notional_usdc.is_finite()
        || intent.notional_usdc <= 0.0
        || intent.notional_usdc > policy.max_order_usdc
    {
        return Err(ExchangeError::Rejected(
            "notional exceeds configured limits".to_owned(),
        ));
    }
    if intent.max_slippage_bps > policy.max_slippage_bps {
        return Err(ExchangeError::Rejected(
            "slippage exceeds configured limits".to_owned(),
        ));
    }
    if intent.max_purchase_fee_bps > policy.max_purchase_fee_bps {
        return Err(ExchangeError::Rejected(
            "purchase fee exceeds acknowledged ceiling".to_owned(),
        ));
    }
    Ok(())
}

pub(crate) struct PolicyEnforcedExchange<C> {
    inner: Box<dyn Exchange>,
    policy: RuntimeActionPolicy,
    clock: C,
}

impl<C> PolicyEnforcedExchange<C> {
    pub(crate) fn new(inner: Box<dyn Exchange>, policy: RuntimeActionPolicy, clock: C) -> Self {
        Self {
            inner,
            policy,
            clock,
        }
    }
}

impl<C: Clock> Exchange for PolicyEnforcedExchange<C> {
    fn mode(&self) -> &'static str {
        self.inner.mode()
    }

    fn submit(&mut self, intent: &OrderIntent) -> Result<Submission, ExchangeError> {
        validate_order_intent(intent, &self.policy, self.clock.now())?;
        self.inner.submit(intent)
    }
}
