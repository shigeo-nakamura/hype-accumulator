use crate::{
    clock::{Clock, SystemClock},
    config::{Config, ConfigError, Environment, RuntimeActionPolicy},
    exchange::{validate_order_intent, Exchange, ExchangeError, OrderIntent, Submission},
};
pub struct Executor<E, C = SystemClock> {
    exchange: E,
    policy: RuntimeActionPolicy,
    clock: C,
}

impl<E: Exchange> Executor<E, SystemClock> {
    /// Constructs an executor from a fully validated config and the system UTC clock.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] when any startup or security-policy gate fails.
    pub fn from_config<Env: Environment>(
        exchange: E,
        config: &Config,
        env: &Env,
    ) -> Result<Self, ConfigError> {
        Self::from_config_with_clock(exchange, config, env, SystemClock)
    }
}

impl<E: Exchange, C: Clock> Executor<E, C> {
    /// Constructs an executor with an injected clock for deterministic boundary checks.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] when any startup or security-policy gate fails.
    pub fn from_config_with_clock<Env: Environment>(
        exchange: E,
        config: &Config,
        env: &Env,
        clock: C,
    ) -> Result<Self, ConfigError> {
        let policy = config.runtime_action_policy_at(env, clock.now())?;
        Ok(Self {
            exchange,
            policy,
            clock,
        })
    }

    /// Revalidates action limits and acknowledgement expiry, then submits one intent.
    ///
    /// # Errors
    ///
    /// Returns [`ExchangeError::Rejected`] for a non-finite, non-positive, or
    /// oversized notional, or propagates an error from the exchange boundary.
    pub fn execute(&mut self, notional_usdc: f64) -> Result<Submission, ExchangeError> {
        let intent = OrderIntent {
            notional_usdc,
            max_slippage_bps: self.policy.max_slippage_bps,
            max_purchase_fee_bps: self.policy.max_purchase_fee_bps,
        };
        validate_order_intent(&intent, &self.policy, self.clock.now())?;
        self.exchange.submit(&intent)
    }
}
