use crate::{clock::Clock, config::RuntimeActionPolicy};
use chrono::{DateTime, Utc};
use rust_decimal::{
    prelude::{FromPrimitive, ToPrimitive},
    Decimal,
};
use thiserror::Error;

const USDC_MICROS_PER_UNIT: u64 = 1_000_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrderTimeInForce {
    ImmediateOrCancel,
    GoodTilCanceled,
    AddLiquidityOnly,
}

#[derive(Clone, Debug, PartialEq)]
pub struct OrderIntent {
    pub notional_usdc: f64,
    pub max_slippage_bps: u16,
    /// Hard aggregate ceiling for venue, builder, and other purchase fees.
    pub max_purchase_fee_bps: u16,
    pub time_in_force: OrderTimeInForce,
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
    /// purchase fee can exceed `intent.max_purchase_fee_bps`, and must submit
    /// exactly `intent.time_in_force`. Returns [`ExchangeError`] when the intent
    /// is rejected or the selected exchange implementation cannot perform live
    /// actions.
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
    if intent.time_in_force != OrderTimeInForce::ImmediateOrCancel {
        return Err(ExchangeError::Rejected(
            "only immediate-or-cancel orders are authorized".to_owned(),
        ));
    }
    Ok(())
}

#[derive(Default)]
pub(crate) struct DailyNotionalLedger {
    current_date: Option<chrono::NaiveDate>,
    reserved_microusd: u64,
}

impl DailyNotionalLedger {
    pub(crate) fn reserve(
        &mut self,
        intent: &OrderIntent,
        policy: &RuntimeActionPolicy,
        now: DateTime<Utc>,
    ) -> Result<(), ExchangeError> {
        let date = now.date_naive();
        match self.current_date {
            Some(current) if date < current => {
                return Err(ExchangeError::Rejected(
                    "UTC clock moved backward across the daily limit boundary".to_owned(),
                ));
            }
            Some(current) if date > current => {
                self.current_date = Some(date);
                self.reserved_microusd = 0;
            }
            None => self.current_date = Some(date),
            Some(_) => {}
        }
        let amount = notional_microusd_ceil(intent.notional_usdc)?;
        let reserved = self
            .reserved_microusd
            .checked_add(amount)
            .ok_or_else(|| ExchangeError::Rejected("daily notional overflow".to_owned()))?;
        if reserved > policy.max_daily_notional_microusd {
            return Err(ExchangeError::Rejected(
                "daily notional exceeds acknowledged limit".to_owned(),
            ));
        }
        self.reserved_microusd = reserved;
        Ok(())
    }
}

fn notional_microusd_ceil(value: f64) -> Result<u64, ExchangeError> {
    let value = Decimal::from_f64(value)
        .ok_or_else(|| ExchangeError::Rejected("notional is not finite".to_owned()))?;
    value
        .checked_mul(Decimal::from(USDC_MICROS_PER_UNIT))
        .and_then(|scaled| scaled.ceil().to_u64())
        .ok_or_else(|| ExchangeError::Rejected("notional is out of range".to_owned()))
}

pub(crate) struct PolicyEnforcedExchange<C> {
    inner: Box<dyn Exchange>,
    policy: RuntimeActionPolicy,
    clock: C,
    daily_notional: DailyNotionalLedger,
}

impl<C> PolicyEnforcedExchange<C> {
    pub(crate) fn new(inner: Box<dyn Exchange>, policy: RuntimeActionPolicy, clock: C) -> Self {
        Self {
            inner,
            policy,
            clock,
            daily_notional: DailyNotionalLedger::default(),
        }
    }
}

impl<C: Clock> Exchange for PolicyEnforcedExchange<C> {
    fn mode(&self) -> &'static str {
        self.inner.mode()
    }

    fn submit(&mut self, intent: &OrderIntent) -> Result<Submission, ExchangeError> {
        let now = self.clock.now();
        validate_order_intent(intent, &self.policy, now)?;
        self.daily_notional.reserve(intent, &self.policy, now)?;
        self.inner.submit(intent)
    }
}
