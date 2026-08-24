use serde::{Deserialize, Serialize};
use std::{collections::HashMap, env, hash::BuildHasher};
use thiserror::Error;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default = "default_true")]
    pub dry_run: bool,
    #[serde(default = "default_true")]
    pub manual_halt: bool,
    #[serde(default)]
    pub live_approved: bool,
    pub capital: CapitalConfig,
    pub pacing: PacingConfig,
    pub schedule: UtcSchedule,
    pub hyperliquid: HyperliquidConfig,
    pub execution: ExecutionConfig,
    #[serde(default)]
    pub validator_allowlist: Vec<String>,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapitalConfig {
    pub min_deposit_confirmations: u32,
    pub max_automatically_deployable_usdc: f64,
    pub yearly_deployment_cap_usdc: f64,
    pub cumulative_deployment_cap_usdc: f64,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PacingConfig {
    pub min_order_usdc: f64,
    pub max_order_usdc: f64,
    pub deposit_cooldown_seconds: u64,
    pub target_horizon_days: u32,
    #[serde(default = "default_fee_spread_reserve_bps")]
    pub fee_spread_reserve_bps: u16,
    #[serde(default = "default_final_catch_up_days")]
    pub final_catch_up_days: u32,
    #[serde(default)]
    pub carry_over_policy: CarryOverPolicy,
}
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CarryOverPolicy {
    #[default]
    HoldForApproval,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UtcSchedule {
    pub utc_hour: u8,
    pub utc_minute: u8,
    pub weekdays: Vec<u8>,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HyperliquidConfig {
    pub endpoint: String,
    pub account_env: String,
    pub signing_key_env: String,
}
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionConfig {
    pub max_order_usdc: f64,
    pub max_slippage_bps: u16,
    pub order_timeout_seconds: u64,
}
const fn default_true() -> bool {
    true
}

const fn default_fee_spread_reserve_bps() -> u16 {
    25
}

const fn default_final_catch_up_days() -> u32 {
    7
}

const MAX_SLIPPAGE_BPS_HARD_CAP: u16 = 100;

pub trait Environment {
    fn get(&self, name: &str) -> Option<String>;
}
pub struct ProcessEnvironment;
impl Environment for ProcessEnvironment {
    fn get(&self, name: &str) -> Option<String> {
        env::var(name).ok()
    }
}
impl<S: BuildHasher> Environment for HashMap<String, String, S> {
    fn get(&self, name: &str) -> Option<String> {
        HashMap::get(self, name).cloned()
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("configuration parse failed: {0}")]
    Parse(String),
    #[error("invalid configuration: {0}")]
    Invalid(String),
    #[error("live mode requires environment variable {0}")]
    MissingLiveSecret(String),
    #[error("status observation requires environment variable {0}")]
    MissingObservationAccount(String),
}

impl Config {
    /// Parses a complete configuration from TOML.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Parse`] when the input is not valid TOML or does
    /// not match the fail-closed configuration schema.
    pub fn from_toml(input: &str) -> Result<Self, ConfigError> {
        toml::from_str(input).map_err(|error| ConfigError::Parse(error.to_string()))
    }

    /// Validates configuration invariants before any exchange is constructed.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Invalid`] for inconsistent safety limits or live
    /// gates, and [`ConfigError::MissingLiveSecret`] when required live-only
    /// environment values are absent.
    pub fn validate<E: Environment>(&self, env: &E) -> Result<(), ConfigError> {
        positive(
            "max_automatically_deployable_usdc",
            self.capital.max_automatically_deployable_usdc,
        )?;
        positive(
            "yearly_deployment_cap_usdc",
            self.capital.yearly_deployment_cap_usdc,
        )?;
        positive(
            "cumulative_deployment_cap_usdc",
            self.capital.cumulative_deployment_cap_usdc,
        )?;
        positive("pacing.min_order_usdc", self.pacing.min_order_usdc)?;
        positive("pacing.max_order_usdc", self.pacing.max_order_usdc)?;
        positive("execution.max_order_usdc", self.execution.max_order_usdc)?;
        if self.execution.max_slippage_bps > MAX_SLIPPAGE_BPS_HARD_CAP {
            return Err(ConfigError::Invalid(format!(
                "execution.max_slippage_bps must not exceed {MAX_SLIPPAGE_BPS_HARD_CAP}"
            )));
        }
        if self.capital.min_deposit_confirmations == 0 {
            return Err(ConfigError::Invalid(
                "min_deposit_confirmations must be positive".into(),
            ));
        }
        if self.pacing.target_horizon_days == 0 || self.pacing.deposit_cooldown_seconds == 0 {
            return Err(ConfigError::Invalid(
                "pacing horizon and cooldown must be positive".into(),
            ));
        }
        if self.pacing.fee_spread_reserve_bps >= 10_000 {
            return Err(ConfigError::Invalid(
                "pacing.fee_spread_reserve_bps must be below 10000".into(),
            ));
        }
        if self.pacing.final_catch_up_days == 0
            || self.pacing.final_catch_up_days > self.pacing.target_horizon_days
        {
            return Err(ConfigError::Invalid(
                "pacing final catch-up window is inconsistent with the target horizon".into(),
            ));
        }
        if self.capital.max_automatically_deployable_usdc > self.capital.yearly_deployment_cap_usdc
            || self.capital.yearly_deployment_cap_usdc > self.capital.cumulative_deployment_cap_usdc
        {
            return Err(ConfigError::Invalid(
                "capital admission caps must be ordered automatic <= yearly <= cumulative".into(),
            ));
        }
        if self.pacing.min_order_usdc > self.pacing.max_order_usdc
            || self.pacing.max_order_usdc > self.execution.max_order_usdc
        {
            return Err(ConfigError::Invalid("order limits are inconsistent".into()));
        }
        let scheduled_weekdays = u32::try_from(
            self.schedule
                .weekdays
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
        )
        .map_err(|_| ConfigError::Invalid("too many UTC weekdays".into()))?;
        let regular_days = self
            .pacing
            .target_horizon_days
            .saturating_sub(self.pacing.final_catch_up_days);
        let regular_slots = (f64::from(regular_days) * f64::from(scheduled_weekdays) / 7.0).ceil();
        let optimistic_slots = regular_slots + f64::from(self.pacing.final_catch_up_days);
        let schedule_capacity = optimistic_slots * self.pacing.max_order_usdc;
        if self.capital.max_automatically_deployable_usdc > schedule_capacity {
            return Err(ConfigError::Invalid(
                "automatic capital cap cannot fit the configured schedule and daily order cap"
                    .into(),
            ));
        }
        self.validate_observation_inputs()?;
        if !self.dry_run {
            self.validate_live(env)?;
        }
        Ok(())
    }

    /// Resolves the public account identity used by the read-only status probe.
    /// This path never reads or validates the signing-key environment variable.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] when the observation endpoint, schedule, account
    /// environment name, or account environment value is invalid.
    pub fn observation_account<E: Environment>(&self, env: &E) -> Result<String, ConfigError> {
        self.validate_observation_inputs()?;
        let name = self.hyperliquid.account_env.trim();
        if name.is_empty() {
            return Err(ConfigError::Invalid(
                "Hyperliquid account environment name is empty".into(),
            ));
        }
        env.get(name)
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| ConfigError::MissingObservationAccount(name.to_owned()))
    }

    fn validate_observation_inputs(&self) -> Result<(), ConfigError> {
        if self.schedule.utc_hour > 23
            || self.schedule.utc_minute > 59
            || self.schedule.weekdays.is_empty()
            || self
                .schedule
                .weekdays
                .iter()
                .any(|day| !(1..=7).contains(day))
        {
            return Err(ConfigError::Invalid("UTC schedule is invalid".into()));
        }
        if !self.hyperliquid.endpoint.starts_with("https://") {
            return Err(ConfigError::Invalid(
                "Hyperliquid endpoint must use HTTPS".into(),
            ));
        }
        Ok(())
    }
    fn validate_live<E: Environment>(&self, env: &E) -> Result<(), ConfigError> {
        if self.manual_halt {
            return Err(ConfigError::Invalid("manual halt is active".into()));
        }
        if !self.live_approved {
            return Err(ConfigError::Invalid(
                "explicit live approval is absent".into(),
            ));
        }
        if self.hyperliquid.account_env.trim() == self.hyperliquid.signing_key_env.trim() {
            return Err(ConfigError::Invalid(
                "account and signing key must use distinct environment variables".into(),
            ));
        }
        if self.validator_allowlist.is_empty()
            || self
                .validator_allowlist
                .iter()
                .any(|validator| validator.trim().is_empty())
        {
            return Err(ConfigError::Invalid("validator allowlist is empty".into()));
        }
        for name in [
            &self.hyperliquid.account_env,
            &self.hyperliquid.signing_key_env,
        ] {
            if name.is_empty() || env.get(name).is_none_or(|value| value.trim().is_empty()) {
                return Err(ConfigError::MissingLiveSecret(name.clone()));
            }
        }
        Ok(())
    }
}
fn positive(name: &str, value: f64) -> Result<(), ConfigError> {
    if value.is_finite() && value > 0.0 {
        Ok(())
    } else {
        Err(ConfigError::Invalid(format!(
            "{name} must be finite and positive"
        )))
    }
}
