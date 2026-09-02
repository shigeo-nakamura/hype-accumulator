pub mod account;
pub mod capital;
pub mod clock;
pub mod config;
pub mod exchange;
pub mod execution;
pub mod ledger;
pub mod metrics;
pub mod monitor;
pub mod pacing;
pub mod runtime;
pub mod signal;
pub mod status;
pub mod status_io;
pub mod workflow;

use clock::{Clock, SystemClock};
use config::{Config, ConfigError, Environment};
use exchange::{DryRunExchange, Exchange, PolicyEnforcedExchange};

/// Validates every safety boundary before constructing a network-capable exchange.
///
/// # Errors
///
/// Returns [`ConfigError`] when configuration validation fails. The live
/// factory is never called on an error or while `dry_run` is enabled.
pub fn bootstrap<E, F>(
    config: &Config,
    env: &E,
    live_factory: F,
) -> Result<Box<dyn Exchange>, ConfigError>
where
    E: Environment,
    F: FnOnce(&Config) -> Box<dyn Exchange>,
{
    bootstrap_with_clock(config, env, SystemClock, live_factory)
}

/// Validates safety boundaries and injects a clock for action-time policy checks.
///
/// # Errors
///
/// Returns [`ConfigError`] when configuration validation fails. The live
/// factory is never called on an error or while `dry_run` is enabled.
pub fn bootstrap_with_clock<E, F, C>(
    config: &Config,
    env: &E,
    clock: C,
    live_factory: F,
) -> Result<Box<dyn Exchange>, ConfigError>
where
    E: Environment,
    F: FnOnce(&Config) -> Box<dyn Exchange>,
    C: Clock + 'static,
{
    let policy = config.runtime_action_policy_at(env, clock.now())?;
    if config.dry_run {
        Ok(Box::new(DryRunExchange::default()))
    } else {
        Ok(Box::new(PolicyEnforcedExchange::new(
            live_factory(config),
            policy,
            clock,
        )))
    }
}
