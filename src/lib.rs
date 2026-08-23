pub mod account;
pub mod capital;
pub mod clock;
pub mod config;
pub mod exchange;
pub mod execution;
pub mod ledger;
pub mod metrics;
pub mod signal;

use config::{Config, ConfigError, Environment};
use exchange::{DryRunExchange, Exchange};

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
    config.validate(env)?;
    if config.dry_run {
        Ok(Box::new(DryRunExchange::default()))
    } else {
        Ok(live_factory(config))
    }
}
