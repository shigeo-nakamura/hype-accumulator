use chrono::{DateTime, SecondsFormat, Utc};
use rust_decimal::{
    prelude::{FromPrimitive, ToPrimitive},
    Decimal,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeSet, HashMap},
    env,
    fmt::Write,
    hash::BuildHasher,
};
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
    #[serde(skip)]
    security_policy: Option<SecurityPolicy>,
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
    #[error("live mode requires an attached effective security policy")]
    MissingSecurityPolicy,
    #[error(
        "live execution is unavailable until durable reservation and authenticated authorization are implemented"
    )]
    LiveExecutionUnavailable,
    #[error(transparent)]
    SecurityPolicy(#[from] SecurityPolicyError),
}

#[derive(Clone, Debug)]
pub(crate) struct RuntimeActionPolicy {
    pub max_order_usdc: f64,
    pub max_daily_notional_microusd: u64,
    pub max_slippage_bps: u16,
    pub max_purchase_fee_bps: u16,
    pub acknowledgement_expires_at: Option<DateTime<Utc>>,
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

    /// Parses the runtime configuration and attaches the separately reviewed
    /// security policy used to authorize live mode.
    ///
    /// # Errors
    ///
    /// Returns a parse or static policy validation error when either document
    /// is malformed. No environment value or credential is read by this step.
    pub fn from_toml_with_security_policy(
        input: &str,
        security_policy: &str,
    ) -> Result<Self, ConfigError> {
        let mut config = Self::from_toml(input)?;
        config.security_policy = Some(SecurityPolicy::from_toml(security_policy)?);
        Ok(config)
    }

    /// Validates configuration invariants before any exchange is constructed.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Invalid`] for inconsistent safety limits or live
    /// gates, and [`ConfigError::MissingLiveSecret`] when required live-only
    /// environment values are absent.
    pub fn validate<E: Environment>(&self, env: &E) -> Result<(), ConfigError> {
        self.validate_at(env, Utc::now())
    }

    /// Validates the artifact-install boundary without enabling runtime actions.
    ///
    /// This gate is intentionally stricter than ordinary dry-run startup: an
    /// explicit typed security policy is required and both the runtime and the
    /// policy must remain dry-run, manually halted, and without live approval.
    /// It does not read the signing-key environment value.
    ///
    /// # Errors
    ///
    /// Returns a fail-closed configuration or policy error unless the release
    /// can be installed for signer-free, halted verification only.
    pub fn validate_offline_install<E: Environment>(&self, env: &E) -> Result<(), ConfigError> {
        if !self.dry_run || !self.manual_halt || self.live_approved {
            return Err(ConfigError::Invalid(
                "offline install requires dry_run=true, manual_halt=true, and live_approved=false"
                    .into(),
            ));
        }
        if self.security_policy.is_none() {
            return Err(ConfigError::MissingSecurityPolicy);
        }
        self.validate(env)
    }

    /// Validates configuration at an injected UTC instant.
    ///
    /// This entry point keeps acknowledgement-expiry boundary tests
    /// deterministic while validate remains the runtime API.
    ///
    /// # Errors
    ///
    /// Returns the same errors as validate.
    pub fn validate_at<E: Environment>(
        &self,
        env: &E,
        now: DateTime<Utc>,
    ) -> Result<(), ConfigError> {
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
        let automatic_cap_microusd = usdc_to_microusd(
            self.capital.max_automatically_deployable_usdc,
            "runtime automatic capital cap",
        )?;
        let cumulative_cap_microusd = usdc_to_microusd(
            self.capital.cumulative_deployment_cap_usdc,
            "runtime cumulative capital cap",
        )?;
        let fixed_reserve_microusd = self
            .effective_security_reserve_microusd()
            .unwrap_or_default();
        if fixed_reserve_microusd >= automatic_cap_microusd {
            return Err(ConfigError::Invalid(
                "security-policy reserve must be below the automatic capital cap".into(),
            ));
        }
        // The reserve reduces the first admitted tranche, but an expired
        // tranche may later hold it for the account as a whole. A subsequent
        // active tranche can therefore spend up to the automatic cap, bounded
        // only by cumulative capacity remaining after that global reserve.
        let schedulable_cap_microusd = automatic_cap_microusd
            .min(cumulative_cap_microusd.saturating_sub(fixed_reserve_microusd));
        let schedule_capacity_microusd = self.schedule_capacity_microusd()?;
        if schedulable_cap_microusd > schedule_capacity_microusd {
            return Err(ConfigError::Invalid(
                "automatic capital cap cannot fit the configured schedule and daily order cap"
                    .into(),
            ));
        }
        self.validate_observation_inputs()?;
        if let Some(policy) = &self.security_policy {
            policy.validate_mode(self, env, now)?;
        }
        if !self.dry_run {
            self.validate_live(env, now)?;
        }
        Ok(())
    }

    /// Computes the acknowledgement value for the attached effective policy.
    ///
    /// This never reads the signing-key environment value. It resolves only
    /// public account identities that form part of the approval boundary.
    ///
    /// # Errors
    ///
    /// Returns an error when no policy is attached or an identity/policy field
    /// cannot be normalized safely.
    pub fn expected_live_acknowledgement<E: Environment>(
        &self,
        env: &E,
    ) -> Result<String, ConfigError> {
        self.security_policy
            .as_ref()
            .ok_or(ConfigError::MissingSecurityPolicy)?
            .expected_acknowledgement(self, env)
            .map_err(ConfigError::from)
    }

    /// Computes the lowercase SHA-256 digest of the canonical effective policy.
    ///
    /// The digest excludes the acknowledgement field itself and includes the
    /// normalized resolved identities and approval-relevant runtime bindings.
    ///
    /// # Errors
    ///
    /// Returns an error when no policy is attached or the effective policy
    /// cannot be normalized safely.
    pub fn effective_security_policy_digest<E: Environment>(
        &self,
        env: &E,
    ) -> Result<String, ConfigError> {
        let acknowledgement = self.expected_live_acknowledgement(env)?;
        acknowledgement
            .strip_prefix(LIVE_ACKNOWLEDGEMENT_PREFIX)
            .map(str::to_owned)
            .ok_or_else(|| {
                ConfigError::SecurityPolicy(SecurityPolicyError::Invalid(
                    "generated acknowledgement has an invalid prefix".to_owned(),
                ))
            })
    }

    pub(crate) fn effective_max_order_usdc(&self) -> f64 {
        self.pacing
            .max_order_usdc
            .min(self.execution.max_order_usdc)
    }

    fn schedule_capacity_microusd(&self) -> Result<u64, ConfigError> {
        let scheduled_weekdays = u64::try_from(
            self.schedule
                .weekdays
                .iter()
                .copied()
                .collect::<BTreeSet<_>>()
                .len(),
        )
        .map_err(|_| ConfigError::Invalid("too many UTC weekdays".into()))?;
        let regular_days = self
            .pacing
            .target_horizon_days
            .saturating_sub(self.pacing.final_catch_up_days);
        let regular_slot_numerator = u64::from(regular_days)
            .checked_mul(scheduled_weekdays)
            .ok_or_else(|| ConfigError::Invalid("runtime schedule capacity overflow".into()))?;
        let regular_slots = regular_slot_numerator
            .checked_add(6)
            .ok_or_else(|| ConfigError::Invalid("runtime schedule capacity overflow".into()))?
            / 7;
        let optimistic_slots = regular_slots
            .checked_add(u64::from(self.pacing.final_catch_up_days))
            .ok_or_else(|| ConfigError::Invalid("runtime schedule capacity overflow".into()))?;
        usdc_to_microusd(self.pacing.max_order_usdc, "runtime maximum order")?
            .checked_mul(optimistic_slots)
            .ok_or_else(|| ConfigError::Invalid("runtime schedule capacity overflow".into()))
    }

    pub(crate) fn effective_security_reserve_microusd(&self) -> Option<u64> {
        self.security_policy
            .as_ref()
            .map(|policy| policy.wire.capital.reserve_microusd)
    }

    pub(crate) fn runtime_action_policy_at<E: Environment>(
        &self,
        env: &E,
        now: DateTime<Utc>,
    ) -> Result<RuntimeActionPolicy, ConfigError> {
        self.validate_at(env, now)?;
        if !self.dry_run {
            return Err(ConfigError::LiveExecutionUnavailable);
        }
        let max_purchase_fee_bps = self
            .security_policy
            .as_ref()
            .map_or(0, |policy| policy.wire.execution.max_purchase_fee_bps);
        if max_purchase_fee_bps >= 10_000 {
            return Err(ConfigError::SecurityPolicy(SecurityPolicyError::Invalid(
                "purchase-fee ceiling must be below 10000 bps".to_owned(),
            )));
        }
        let acknowledgement_expires_at = if self.dry_run {
            None
        } else {
            let policy = self
                .security_policy
                .as_ref()
                .ok_or(ConfigError::MissingSecurityPolicy)?;
            Some(parse_canonical_utc(
                &policy.wire.operator.live_acknowledgement_expires_at,
                "live acknowledgement expiry",
            )?)
        };
        let max_daily_notional_microusd = match &self.security_policy {
            Some(policy) => policy.wire.capital.max_daily_notional_microusd,
            None => usdc_to_microusd(
                self.effective_max_order_usdc(),
                "runtime effective maximum order",
            )?,
        };
        Ok(RuntimeActionPolicy {
            max_order_usdc: self.effective_max_order_usdc(),
            max_daily_notional_microusd,
            max_slippage_bps: self.execution.max_slippage_bps,
            max_purchase_fee_bps,
            acknowledgement_expires_at,
        })
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
    fn validate_live<E: Environment>(
        &self,
        env: &E,
        now: DateTime<Utc>,
    ) -> Result<(), ConfigError> {
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
        self.security_policy
            .as_ref()
            .ok_or(ConfigError::MissingSecurityPolicy)?
            .validate_live(self, env, now)?;
        Ok(())
    }
}

const SECURITY_POLICY_SCHEMA_VERSION: u16 = 1;
const LIVE_ACKNOWLEDGEMENT_PREFIX: &str = "v1:sha256:";

/// A statically checked security-policy document attached to runtime config.
///
/// The raw wire is private so callers cannot deserialize around the mandatory
/// schema-version, staking-disable, timestamp, address, and hash-shape checks.
#[derive(Clone, Debug)]
pub struct SecurityPolicy {
    wire: SecurityPolicyWire,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SecurityPolicyWire {
    schema_version: u16,
    capital: SecurityCapitalPolicy,
    custody: CustodyPolicy,
    execution: SecurityExecutionPolicy,
    staking: StakingPolicy,
    operator: OperatorPolicy,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SecurityCapitalPolicy {
    max_auto_deposit_microusd: u64,
    max_daily_notional_microusd: u64,
    max_yearly_deployable_microusd: u64,
    max_cumulative_deployable_microusd: u64,
    reserve_microusd: u64,
    daily_limit_period: LimitPeriod,
    yearly_limit_period: LimitPeriod,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CustodyPolicy {
    api_wallet_authority: ApiWalletAuthority,
    signer_mode: SignerMode,
    require_dedicated_execution_account: bool,
    execution_account_kind: ExecutionAccountKind,
    max_hot_trading_balance_microusd: u64,
    hot_balance_enforcement: HotBalanceEnforcement,
    hot_balance_sweep_threshold_microusd: u64,
    hot_balance_worst_case_headroom_microusd: u64,
    hot_balance_enforcement_evidence_sha256: String,
    hot_balance_enforcement_change_ref: String,
    funding_mode: FundingMode,
    allow_traced_parent_transfer_admission: bool,
    admitted_parent_account_env: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SecurityExecutionPolicy {
    max_slippage_bps: u16,
    max_purchase_fee_bps: u16,
    authorized_order_tif: AuthorizedOrderTif,
    require_signed_request_expiry: bool,
    max_venue_clock_lag_ms: u64,
    venue_clock_evidence_stale_after_seconds: u64,
    book_stale_after_seconds: u64,
    account_history_stale_after_seconds: u64,
    fee_schedule_stale_after_seconds: u64,
    require_prepurchase_authorization: bool,
    signal_stale_after_seconds: u64,
    cancel_known_open_orders_while_halted: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StakingPolicy {
    enabled: bool,
    validator_allowlist: Vec<String>,
    residual_hype_wei: u64,
    fill_registration_deadline_seconds: u64,
    lot_consumption_policy: LotConsumptionPolicy,
    lot_eligibility_max_age_seconds: u64,
    no_eligible_validator_policy: NoEligibleValidatorPolicy,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OperatorPolicy {
    dry_run: bool,
    manual_halt: bool,
    live_acknowledgement: String,
    live_acknowledgement_expires_at: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum LimitPeriod {
    UtcCalendarDayV1,
    UtcCalendarYearV1,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum ApiWalletAuthority {
    FullAssignedAccountTrading,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum SignerMode {
    DedicatedApiWalletSpotOnly,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum ExecutionAccountKind {
    Unapproved,
    DedicatedMaster,
    Subaccount,
    Vault,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum HotBalanceEnforcement {
    Unapproved,
    ExternallyEnforcedBounded,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum FundingMode {
    ExternalDepositOnly,
    TracedParentTransfer,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum AuthorizedOrderTif {
    Ioc,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum LotConsumptionPolicy {
    OldestAuthoritativeFillFirst,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum NoEligibleValidatorPolicy {
    HoldInSpot,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SecurityPolicyError {
    #[error("security policy parse failed: {0}")]
    Parse(String),
    #[error("invalid security policy: {0}")]
    Invalid(String),
    #[error("security policy requires resolved identity {0}")]
    MissingResolvedIdentity(String),
    #[error("live security-policy acknowledgement does not match the effective policy")]
    AcknowledgementMismatch,
    #[error("live security-policy acknowledgement is expired")]
    AcknowledgementExpired,
}

#[derive(Debug)]
struct LivePolicyContext {
    execution_account: String,
    admitted_parent_account: Option<String>,
    validator_allowlist: Vec<String>,
    schedule_weekdays: Vec<u8>,
    acknowledgement_expires_at: String,
    acknowledgement_expiry: DateTime<Utc>,
}

#[derive(Serialize)]
struct CanonicalEffectiveSecurityPolicy<'a> {
    schema_version: u16,
    capital: &'a SecurityCapitalPolicy,
    custody: CanonicalCustodyPolicy<'a>,
    execution: &'a SecurityExecutionPolicy,
    staking: CanonicalStakingPolicy<'a>,
    operator: CanonicalOperatorPolicy<'a>,
    runtime: CanonicalRuntimeBindings<'a>,
    resolved_identities: CanonicalResolvedIdentities<'a>,
}

#[derive(Serialize)]
struct CanonicalCustodyPolicy<'a> {
    api_wallet_authority: &'a str,
    signer_mode: &'a str,
    require_dedicated_execution_account: bool,
    execution_account_kind: &'a str,
    max_hot_trading_balance_microusd: u64,
    hot_balance_enforcement: &'a str,
    hot_balance_sweep_threshold_microusd: u64,
    hot_balance_worst_case_headroom_microusd: u64,
    hot_balance_enforcement_evidence_sha256: &'a str,
    hot_balance_enforcement_change_ref: &'a str,
    funding_mode: &'a str,
    allow_traced_parent_transfer_admission: bool,
}

#[derive(Serialize)]
struct CanonicalStakingPolicy<'a> {
    enabled: bool,
    validator_allowlist: &'a [String],
    residual_hype_wei: u64,
    lot_consumption_policy: &'a str,
    fill_registration_deadline_seconds: u64,
    lot_eligibility_max_age_seconds: u64,
    no_eligible_validator_policy: &'a str,
}

#[derive(Serialize)]
struct CanonicalOperatorPolicy<'a> {
    dry_run: bool,
    manual_halt: bool,
    live_acknowledgement_expires_at: &'a str,
}

#[derive(Serialize)]
struct CanonicalRuntimeBindings<'a> {
    live_approved: bool,
    hyperliquid_endpoint: &'a str,
    min_deposit_confirmations: u32,
    pacing_min_order_microusd: u64,
    effective_max_order_microusd: u64,
    execution_max_order_microusd: u64,
    pacing_deposit_cooldown_seconds: u64,
    pacing_target_horizon_days: u32,
    pacing_fee_spread_reserve_bps: u16,
    pacing_final_catch_up_days: u32,
    pacing_carry_over_policy: &'a str,
    schedule_utc_hour: u8,
    schedule_utc_minute: u8,
    schedule_weekdays: &'a [u8],
    execution_order_timeout_seconds: u64,
}

#[derive(Serialize)]
struct CanonicalResolvedIdentities<'a> {
    execution_account: &'a str,
    admitted_parent_account: Option<&'a str>,
}

impl SecurityPolicy {
    /// Parses and statically validates a complete security-policy TOML document.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown/missing field, unsupported schema value,
    /// enabled runtime staking, malformed address/hash, or non-canonical expiry.
    pub fn from_toml(input: &str) -> Result<Self, SecurityPolicyError> {
        let policy = Self {
            wire: toml::from_str(input)
                .map_err(|error| SecurityPolicyError::Parse(error.to_string()))?,
        };
        policy.validate_static()?;
        Ok(policy)
    }

    fn validate_static(&self) -> Result<(), SecurityPolicyError> {
        let policy = &self.wire;
        if policy.schema_version != SECURITY_POLICY_SCHEMA_VERSION {
            return invalid_policy("unsupported schema_version");
        }
        if policy.staking.enabled {
            return invalid_policy("runtime staking must remain disabled");
        }
        if !policy
            .custody
            .hot_balance_enforcement_evidence_sha256
            .is_empty()
            && !is_lower_sha256(&policy.custody.hot_balance_enforcement_evidence_sha256)
        {
            return invalid_policy("hot-balance evidence hash must be lowercase SHA-256");
        }
        normalized_addresses(
            &policy.staking.validator_allowlist,
            "validator allowlist",
            false,
        )?;
        if !policy.operator.live_acknowledgement.is_empty()
            && !is_versioned_acknowledgement(&policy.operator.live_acknowledgement)
        {
            return invalid_policy("live acknowledgement has an invalid versioned hash");
        }
        if !policy.operator.live_acknowledgement_expires_at.is_empty() {
            parse_canonical_utc(
                &policy.operator.live_acknowledgement_expires_at,
                "live acknowledgement expiry",
            )?;
        }
        Ok(())
    }

    fn validate_mode<E: Environment>(
        &self,
        config: &Config,
        _env: &E,
        _now: DateTime<Utc>,
    ) -> Result<(), SecurityPolicyError> {
        if config.dry_run {
            self.validate_dry_run(config)?;
        }
        Ok(())
    }

    fn validate_dry_run(&self, config: &Config) -> Result<(), SecurityPolicyError> {
        let operator = &self.wire.operator;
        if !operator.dry_run || !operator.manual_halt {
            return invalid_policy("attached dry-run policy must remain dry-run and halted");
        }
        if !config.manual_halt {
            return invalid_policy("runtime and policy manual-halt modes differ");
        }
        if !operator.live_acknowledgement.is_empty()
            || !operator.live_acknowledgement_expires_at.is_empty()
        {
            return invalid_policy("dry-run policy must not carry a live acknowledgement");
        }
        Ok(())
    }

    fn validate_live<E: Environment>(
        &self,
        config: &Config,
        env: &E,
        now: DateTime<Utc>,
    ) -> Result<(), SecurityPolicyError> {
        let context = self.live_context(config, env)?;
        if now >= context.acknowledgement_expiry {
            return Err(SecurityPolicyError::AcknowledgementExpired);
        }
        let expected = self.acknowledgement_for_context(config, &context)?;
        if self.wire.operator.live_acknowledgement != expected {
            return Err(SecurityPolicyError::AcknowledgementMismatch);
        }
        Ok(())
    }

    fn expected_acknowledgement<E: Environment>(
        &self,
        config: &Config,
        env: &E,
    ) -> Result<String, SecurityPolicyError> {
        let context = self.live_context(config, env)?;
        self.acknowledgement_for_context(config, &context)
    }

    fn acknowledgement_for_context(
        &self,
        config: &Config,
        context: &LivePolicyContext,
    ) -> Result<String, SecurityPolicyError> {
        let custody = &self.wire.custody;
        let staking = &self.wire.staking;
        let canonical = CanonicalEffectiveSecurityPolicy {
            schema_version: self.wire.schema_version,
            capital: &self.wire.capital,
            custody: CanonicalCustodyPolicy {
                api_wallet_authority: "full_assigned_account_trading",
                signer_mode: "dedicated_api_wallet_spot_only",
                require_dedicated_execution_account: custody.require_dedicated_execution_account,
                execution_account_kind: match custody.execution_account_kind {
                    ExecutionAccountKind::Unapproved => "unapproved",
                    ExecutionAccountKind::DedicatedMaster => "dedicated_master",
                    ExecutionAccountKind::Subaccount => "subaccount",
                    ExecutionAccountKind::Vault => "vault",
                },
                max_hot_trading_balance_microusd: custody.max_hot_trading_balance_microusd,
                hot_balance_enforcement: "externally_enforced_bounded",
                hot_balance_sweep_threshold_microusd: custody.hot_balance_sweep_threshold_microusd,
                hot_balance_worst_case_headroom_microusd: custody
                    .hot_balance_worst_case_headroom_microusd,
                hot_balance_enforcement_evidence_sha256: &custody
                    .hot_balance_enforcement_evidence_sha256,
                hot_balance_enforcement_change_ref: custody
                    .hot_balance_enforcement_change_ref
                    .trim(),
                funding_mode: match custody.funding_mode {
                    FundingMode::ExternalDepositOnly => "external_deposit_only",
                    FundingMode::TracedParentTransfer => "traced_parent_transfer",
                },
                allow_traced_parent_transfer_admission: custody
                    .allow_traced_parent_transfer_admission,
            },
            execution: &self.wire.execution,
            staking: CanonicalStakingPolicy {
                enabled: staking.enabled,
                validator_allowlist: &context.validator_allowlist,
                residual_hype_wei: staking.residual_hype_wei,
                lot_consumption_policy: "oldest_authoritative_fill_first",
                fill_registration_deadline_seconds: staking.fill_registration_deadline_seconds,
                lot_eligibility_max_age_seconds: staking.lot_eligibility_max_age_seconds,
                no_eligible_validator_policy: "hold_in_spot",
            },
            operator: CanonicalOperatorPolicy {
                dry_run: self.wire.operator.dry_run,
                manual_halt: self.wire.operator.manual_halt,
                live_acknowledgement_expires_at: &context.acknowledgement_expires_at,
            },
            runtime: CanonicalRuntimeBindings {
                live_approved: config.live_approved,
                hyperliquid_endpoint: config.hyperliquid.endpoint.trim(),
                min_deposit_confirmations: config.capital.min_deposit_confirmations,
                pacing_min_order_microusd: usdc_to_microusd(
                    config.pacing.min_order_usdc,
                    "runtime minimum order",
                )?,
                effective_max_order_microusd: usdc_to_microusd(
                    config.effective_max_order_usdc(),
                    "runtime effective maximum order",
                )?,
                execution_max_order_microusd: usdc_to_microusd(
                    config.execution.max_order_usdc,
                    "runtime execution maximum order",
                )?,
                pacing_deposit_cooldown_seconds: config.pacing.deposit_cooldown_seconds,
                pacing_target_horizon_days: config.pacing.target_horizon_days,
                pacing_fee_spread_reserve_bps: config.pacing.fee_spread_reserve_bps,
                pacing_final_catch_up_days: config.pacing.final_catch_up_days,
                pacing_carry_over_policy: "hold_for_approval",
                schedule_utc_hour: config.schedule.utc_hour,
                schedule_utc_minute: config.schedule.utc_minute,
                schedule_weekdays: &context.schedule_weekdays,
                execution_order_timeout_seconds: config.execution.order_timeout_seconds,
            },
            resolved_identities: CanonicalResolvedIdentities {
                execution_account: &context.execution_account,
                admitted_parent_account: context.admitted_parent_account.as_deref(),
            },
        };
        let encoded = serde_json::to_vec(&canonical)
            .map_err(|error| SecurityPolicyError::Invalid(error.to_string()))?;
        let digest = Sha256::digest(encoded);
        let mut hex = String::with_capacity(64);
        for byte in digest {
            write!(&mut hex, "{byte:02x}")
                .map_err(|error| SecurityPolicyError::Invalid(error.to_string()))?;
        }
        Ok(format!("{LIVE_ACKNOWLEDGEMENT_PREFIX}{hex}"))
    }

    fn live_context<E: Environment>(
        &self,
        config: &Config,
        env: &E,
    ) -> Result<LivePolicyContext, SecurityPolicyError> {
        self.validate_live_contract(config)?;
        let account_name = config.hyperliquid.account_env.trim();
        let execution_account = resolved_address(env, account_name, "execution account")?;
        let custody = &self.wire.custody;
        let admitted_parent_account = match custody.funding_mode {
            FundingMode::ExternalDepositOnly => {
                if custody.allow_traced_parent_transfer_admission
                    || !custody.admitted_parent_account_env.trim().is_empty()
                {
                    return invalid_policy(
                        "external-deposit-only funding cannot inherit parent capital",
                    );
                }
                None
            }
            FundingMode::TracedParentTransfer => {
                if !custody.allow_traced_parent_transfer_admission {
                    return invalid_policy(
                        "traced parent funding requires explicit inheritance enablement",
                    );
                }
                let name = custody.admitted_parent_account_env.trim();
                if name.is_empty() {
                    return invalid_policy("traced parent funding requires a parent account");
                }
                Some(resolved_address(env, name, "admitted parent account")?)
            }
        };
        if admitted_parent_account.as_ref() == Some(&execution_account) {
            return invalid_policy("execution and admitted parent accounts must differ");
        }
        let validator_allowlist = normalized_addresses(
            &self.wire.staking.validator_allowlist,
            "validator allowlist",
            true,
        )?;
        let config_validators = normalized_addresses(
            &config.validator_allowlist,
            "runtime validator allowlist",
            true,
        )?;
        if validator_allowlist != config_validators {
            return invalid_policy("runtime and policy validator allowlists differ");
        }
        let acknowledgement_expiry = parse_canonical_utc(
            &self.wire.operator.live_acknowledgement_expires_at,
            "live acknowledgement expiry",
        )?;
        let schedule_weekdays = config
            .schedule
            .weekdays
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        Ok(LivePolicyContext {
            execution_account,
            admitted_parent_account,
            validator_allowlist,
            schedule_weekdays,
            acknowledgement_expires_at: self.wire.operator.live_acknowledgement_expires_at.clone(),
            acknowledgement_expiry,
        })
    }

    fn validate_live_contract(&self, config: &Config) -> Result<(), SecurityPolicyError> {
        self.validate_static()?;
        let policy = &self.wire;
        if config.dry_run || policy.operator.dry_run {
            return invalid_policy("live config and policy must both disable dry-run");
        }
        if config.manual_halt || policy.operator.manual_halt {
            return invalid_policy("live config and policy must both clear manual halt");
        }
        self.validate_live_custody()?;
        self.validate_live_capital(config)?;
        self.validate_live_execution(config)?;
        self.validate_live_staking()?;
        Self::validate_runtime_bindings(config)?;
        if policy.operator.live_acknowledgement_expires_at.is_empty() {
            return invalid_policy("live acknowledgement expiry is missing");
        }
        Ok(())
    }

    fn validate_runtime_bindings(config: &Config) -> Result<(), SecurityPolicyError> {
        if !config.live_approved {
            return invalid_policy("runtime live approval flag is absent");
        }
        if config.hyperliquid.endpoint.trim() != config.hyperliquid.endpoint
            || !config.hyperliquid.endpoint.starts_with("https://")
        {
            return invalid_policy("runtime Hyperliquid endpoint is not canonical HTTPS");
        }
        if config.execution.order_timeout_seconds == 0 {
            return invalid_policy("runtime order timeout must be positive");
        }
        let weekdays = config
            .schedule
            .weekdays
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if weekdays.len() != config.schedule.weekdays.len() {
            return invalid_policy("runtime UTC schedule contains duplicate weekdays");
        }
        Ok(())
    }

    fn validate_live_custody(&self) -> Result<(), SecurityPolicyError> {
        let custody = &self.wire.custody;
        if custody.api_wallet_authority != ApiWalletAuthority::FullAssignedAccountTrading
            || custody.signer_mode != SignerMode::DedicatedApiWalletSpotOnly
            || !custody.require_dedicated_execution_account
            || custody.execution_account_kind == ExecutionAccountKind::Unapproved
        {
            return invalid_policy(
                "live custody requires an approved isolated account and spot-only API wallet",
            );
        }
        if custody.hot_balance_enforcement != HotBalanceEnforcement::ExternallyEnforcedBounded {
            return invalid_policy("live mode requires approved bounded hot-balance enforcement");
        }
        let threshold_with_headroom = custody
            .hot_balance_sweep_threshold_microusd
            .checked_add(custody.hot_balance_worst_case_headroom_microusd)
            .ok_or_else(|| SecurityPolicyError::Invalid("hot-balance bound overflow".to_owned()))?;
        if custody.max_hot_trading_balance_microusd == 0
            || custody.hot_balance_sweep_threshold_microusd == 0
            || custody.hot_balance_worst_case_headroom_microusd == 0
            || threshold_with_headroom > custody.max_hot_trading_balance_microusd
            || !is_lower_sha256(&custody.hot_balance_enforcement_evidence_sha256)
            || custody.hot_balance_enforcement_change_ref.trim().is_empty()
        {
            return invalid_policy("bounded hot-balance evidence is incomplete");
        }
        Ok(())
    }

    fn validate_live_capital(&self, config: &Config) -> Result<(), SecurityPolicyError> {
        let capital = &self.wire.capital;
        if capital.max_auto_deposit_microusd == 0
            || capital.max_daily_notional_microusd == 0
            || capital.max_yearly_deployable_microusd == 0
            || capital.max_cumulative_deployable_microusd == 0
            || capital.reserve_microusd == 0
            || capital.max_auto_deposit_microusd > capital.max_yearly_deployable_microusd
            || capital.max_yearly_deployable_microusd > capital.max_cumulative_deployable_microusd
            || capital.reserve_microusd >= capital.max_auto_deposit_microusd
            || capital.daily_limit_period != LimitPeriod::UtcCalendarDayV1
            || capital.yearly_limit_period != LimitPeriod::UtcCalendarYearV1
        {
            return invalid_policy("live capital limits are missing or inconsistent");
        }
        if capital.max_auto_deposit_microusd
            != usdc_to_microusd(
                config.capital.max_automatically_deployable_usdc,
                "runtime automatic capital cap",
            )?
            || capital.max_yearly_deployable_microusd
                != usdc_to_microusd(
                    config.capital.yearly_deployment_cap_usdc,
                    "runtime yearly capital cap",
                )?
            || capital.max_cumulative_deployable_microusd
                != usdc_to_microusd(
                    config.capital.cumulative_deployment_cap_usdc,
                    "runtime cumulative capital cap",
                )?
            || capital.max_daily_notional_microusd
                != usdc_to_microusd(
                    config.effective_max_order_usdc(),
                    "runtime effective maximum order",
                )?
        {
            return invalid_policy("runtime and policy capital limits differ");
        }
        Ok(())
    }

    fn validate_live_execution(&self, config: &Config) -> Result<(), SecurityPolicyError> {
        let execution = &self.wire.execution;
        if execution.max_slippage_bps != config.execution.max_slippage_bps
            || execution.max_purchase_fee_bps >= 10_000
            || execution.authorized_order_tif != AuthorizedOrderTif::Ioc
            || !execution.require_signed_request_expiry
            || execution.max_venue_clock_lag_ms == 0
            || execution.venue_clock_evidence_stale_after_seconds == 0
            || execution.book_stale_after_seconds == 0
            || execution.account_history_stale_after_seconds == 0
            || execution.fee_schedule_stale_after_seconds == 0
            || !execution.require_prepurchase_authorization
            || execution.signal_stale_after_seconds == 0
            || !execution.cancel_known_open_orders_while_halted
        {
            return invalid_policy("live execution controls are missing or inconsistent");
        }
        Ok(())
    }

    fn validate_live_staking(&self) -> Result<(), SecurityPolicyError> {
        let staking = &self.wire.staking;
        if staking.residual_hype_wei == 0
            || staking.fill_registration_deadline_seconds == 0
            || staking.lot_eligibility_max_age_seconds == 0
            || staking.lot_consumption_policy != LotConsumptionPolicy::OldestAuthoritativeFillFirst
            || staking.no_eligible_validator_policy != NoEligibleValidatorPolicy::HoldInSpot
        {
            return invalid_policy("dormant staking controls are missing or inconsistent");
        }
        Ok(())
    }
}

fn invalid_policy<T>(message: &str) -> Result<T, SecurityPolicyError> {
    Err(SecurityPolicyError::Invalid(message.to_owned()))
}

fn parse_canonical_utc(value: &str, field: &str) -> Result<DateTime<Utc>, SecurityPolicyError> {
    let parsed = DateTime::parse_from_rfc3339(value)
        .map_err(|_| SecurityPolicyError::Invalid(format!("{field} is invalid")))?
        .with_timezone(&Utc);
    if parsed.to_rfc3339_opts(SecondsFormat::Secs, true) != value {
        return invalid_policy(&format!("{field} must be canonical UTC seconds"));
    }
    Ok(parsed)
}

fn is_lower_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn is_versioned_acknowledgement(value: &str) -> bool {
    value
        .strip_prefix(LIVE_ACKNOWLEDGEMENT_PREFIX)
        .is_some_and(is_lower_sha256)
}

fn normalize_address(value: &str, field: &str) -> Result<String, SecurityPolicyError> {
    let trimmed = value.trim();
    let Some(hex) = trimmed.strip_prefix("0x") else {
        return invalid_policy(&format!("{field} is not a canonical account address"));
    };
    if hex.len() != 40
        || !hex.bytes().all(|byte| byte.is_ascii_hexdigit())
        || hex.bytes().all(|byte| byte == b'0')
    {
        return invalid_policy(&format!("{field} is not a canonical account address"));
    }
    Ok(format!("0x{}", hex.to_ascii_lowercase()))
}

fn normalized_addresses(
    values: &[String],
    field: &str,
    require_nonempty: bool,
) -> Result<Vec<String>, SecurityPolicyError> {
    let normalized = values
        .iter()
        .map(|value| normalize_address(value, field))
        .collect::<Result<BTreeSet<_>, _>>()?;
    if normalized.len() != values.len() {
        return invalid_policy(&format!("{field} contains a duplicate"));
    }
    if require_nonempty && normalized.is_empty() {
        return invalid_policy(&format!("{field} must contain at least one address"));
    }
    Ok(normalized.into_iter().collect())
}

fn resolved_address<E: Environment>(
    env: &E,
    name: &str,
    identity: &str,
) -> Result<String, SecurityPolicyError> {
    if name.is_empty() {
        return Err(SecurityPolicyError::MissingResolvedIdentity(
            identity.to_owned(),
        ));
    }
    let value = env
        .get(name)
        .ok_or_else(|| SecurityPolicyError::MissingResolvedIdentity(identity.to_owned()))?;
    normalize_address(&value, identity)
}

fn usdc_to_microusd(value: f64, field: &str) -> Result<u64, SecurityPolicyError> {
    if !value.is_finite() || value <= 0.0 {
        return invalid_policy(&format!("{field} must be finite and positive"));
    }
    let decimal = Decimal::from_f64(value)
        .ok_or_else(|| SecurityPolicyError::Invalid(format!("{field} is out of range")))?;
    let scaled = decimal
        .checked_mul(Decimal::from(1_000_000_u64))
        .ok_or_else(|| SecurityPolicyError::Invalid(format!("{field} is out of range")))?;
    if !scaled.fract().is_zero() {
        return invalid_policy(&format!("{field} must use exact microunits"));
    }
    scaled
        .to_u64()
        .ok_or_else(|| SecurityPolicyError::Invalid(format!("{field} is out of range")))
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
