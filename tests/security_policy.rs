use chrono::{DateTime, TimeZone, Utc};
use hype_accumulator::{
    bootstrap_with_clock,
    clock::Clock,
    config::{Config, ConfigError, SecurityPolicyError},
    exchange::{DryRunExchange, Exchange, ExchangeError, OrderIntent, Submission},
    execution::Executor,
    pacing::{
        CapitalEvent, DecisionInput, DecisionReason, DepositEvent, PacingError, PacingLimits,
        PacingState, UsdcMicros,
    },
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

const EXECUTION_ACCOUNT: &str = "0x11111111111111111111111111111111111111aa";
const OTHER_EXECUTION_ACCOUNT: &str = "0x2222222222222222222222222222222222222222";
const VALIDATOR: &str = "0x3333333333333333333333333333333333333333";
const PARENT_ACCOUNT: &str = "0x4444444444444444444444444444444444444444";
const OTHER_PARENT_ACCOUNT: &str = "0x5555555555555555555555555555555555555555";
const EVIDENCE_HASH: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const EXPIRY: &str = "2026-09-01T00:00:00Z";

fn at(value: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value)
        .expect("valid test timestamp")
        .with_timezone(&Utc)
}

fn live_runtime_toml() -> String {
    format!(
        "live_approved = true\nvalidator_allowlist = [\"{VALIDATOR}\"]\n{}",
        include_str!("fixtures/safe.toml")
    )
    .replace("dry_run = true", "dry_run = false")
    .replace("manual_halt = true", "manual_halt = false")
}

fn live_policy_template() -> String {
    include_str!("../config/security-policy.example.toml")
        .replace(
            "max_auto_deposit_microusd = 0",
            "max_auto_deposit_microusd = 100000000",
        )
        .replace(
            "max_daily_notional_microusd = 0",
            "max_daily_notional_microusd = 25000000",
        )
        .replace(
            "max_yearly_deployable_microusd = 0",
            "max_yearly_deployable_microusd = 500000000",
        )
        .replace(
            "max_cumulative_deployable_microusd = 0",
            "max_cumulative_deployable_microusd = 1000000000",
        )
        .replace("reserve_microusd = 0", "reserve_microusd = 1000000")
        .replace(
            "execution_account_kind = \"unapproved\"",
            "execution_account_kind = \"dedicated_master\"",
        )
        .replace(
            "max_hot_trading_balance_microusd = 0",
            "max_hot_trading_balance_microusd = 200000000",
        )
        .replace(
            "hot_balance_enforcement = \"unapproved\"",
            "hot_balance_enforcement = \"externally_enforced_bounded\"",
        )
        .replace(
            "hot_balance_sweep_threshold_microusd = 0",
            "hot_balance_sweep_threshold_microusd = 150000000",
        )
        .replace(
            "hot_balance_worst_case_headroom_microusd = 0",
            "hot_balance_worst_case_headroom_microusd = 25000000",
        )
        .replace(
            "hot_balance_enforcement_evidence_sha256 = \"\"",
            &format!("hot_balance_enforcement_evidence_sha256 = \"{EVIDENCE_HASH}\""),
        )
        .replace(
            "hot_balance_enforcement_change_ref = \"\"",
            "hot_balance_enforcement_change_ref = \"test-change-record\"",
        )
        .replace("max_slippage_bps = 0", "max_slippage_bps = 20")
        .replace("max_purchase_fee_bps = 0", "max_purchase_fee_bps = 5")
        .replace("max_venue_clock_lag_ms = 0", "max_venue_clock_lag_ms = 500")
        .replace(
            "venue_clock_evidence_stale_after_seconds = 0",
            "venue_clock_evidence_stale_after_seconds = 30",
        )
        .replace(
            "book_stale_after_seconds = 0",
            "book_stale_after_seconds = 15",
        )
        .replace(
            "account_history_stale_after_seconds = 0",
            "account_history_stale_after_seconds = 30",
        )
        .replace(
            "fee_schedule_stale_after_seconds = 0",
            "fee_schedule_stale_after_seconds = 3600",
        )
        .replace(
            "signal_stale_after_seconds = 0",
            "signal_stale_after_seconds = 86400",
        )
        .replace(
            "validator_allowlist = []",
            &format!("validator_allowlist = [\"{VALIDATOR}\"]"),
        )
        .replace("residual_hype_wei = 0", "residual_hype_wei = 1000")
        .replace(
            "fill_registration_deadline_seconds = 0",
            "fill_registration_deadline_seconds = 300",
        )
        .replace(
            "lot_eligibility_max_age_seconds = 0",
            "lot_eligibility_max_age_seconds = 86400",
        )
        .replace("dry_run = true", "dry_run = false")
        .replace("manual_halt = true", "manual_halt = false")
        .replace(
            "live_acknowledgement_expires_at = \"\"",
            &format!("live_acknowledgement_expires_at = \"{EXPIRY}\""),
        )
}

fn live_environment() -> HashMap<String, String> {
    HashMap::from([
        ("HYPE_ACCOUNT_ID".to_owned(), EXECUTION_ACCOUNT.to_owned()),
        ("HYPE_SIGNING_KEY".to_owned(), "present".to_owned()),
    ])
}

fn config_with_policy(policy: &str) -> Config {
    Config::from_toml_with_security_policy(&live_runtime_toml(), policy)
        .expect("valid live test documents")
}

fn acknowledged_policy(policy: &str, env: &HashMap<String, String>) -> String {
    let expected = config_with_policy(policy)
        .expected_live_acknowledgement(env)
        .expect("complete effective policy");
    policy.replace(
        "live_acknowledgement = \"\"",
        &format!("live_acknowledgement = \"{expected}\""),
    )
}

#[test]
fn example_policy_is_typed_and_safe_for_dry_run() {
    let config = Config::from_toml_with_security_policy(
        include_str!("fixtures/safe.toml"),
        include_str!("../config/security-policy.example.toml"),
    )
    .expect("safe example parses");
    config
        .validate_at(&HashMap::new(), at("2026-08-24T00:00:00Z"))
        .expect("safe dry-run policy");
}

#[test]
fn unknown_policy_fields_are_rejected() {
    let policy = format!(
        "unknown_top_level = true\n{}",
        include_str!("../config/security-policy.example.toml")
    );
    assert!(matches!(
        Config::from_toml_with_security_policy(include_str!("fixtures/safe.toml"), &policy),
        Err(ConfigError::SecurityPolicy(SecurityPolicyError::Parse(_)))
    ));
}

#[test]
fn live_runtime_without_attached_policy_fails_closed() {
    let config = Config::from_toml(&live_runtime_toml()).expect("runtime config parses");
    assert_eq!(
        config.validate_at(&live_environment(), at("2026-08-24T00:00:00Z")),
        Err(ConfigError::MissingSecurityPolicy)
    );
    assert_eq!(
        PacingLimits::from_config(&config),
        Err(PacingError::InvalidLimits)
    );
}

#[test]
fn exact_acknowledgement_is_required_and_expires_at_the_boundary() {
    let env = live_environment();
    let policy = live_policy_template();
    assert_eq!(
        config_with_policy(&policy).validate_at(&env, at("2026-08-31T23:59:59Z")),
        Err(ConfigError::SecurityPolicy(
            SecurityPolicyError::AcknowledgementMismatch
        ))
    );

    let acknowledged = acknowledged_policy(&policy, &env);
    let config = config_with_policy(&acknowledged);
    config
        .validate_at(&env, at("2026-08-31T23:59:59Z"))
        .expect("acknowledgement is current");
    assert_eq!(
        config.validate_at(&env, at(EXPIRY)),
        Err(ConfigError::SecurityPolicy(
            SecurityPolicyError::AcknowledgementExpired
        ))
    );
}

#[test]
fn canonical_digest_excludes_acknowledgement_but_binds_policy_fields() {
    let env = live_environment();
    let policy = live_policy_template();
    let expected = config_with_policy(&policy)
        .expected_live_acknowledgement(&env)
        .expect("expected acknowledgement");
    let digest = config_with_policy(&policy)
        .effective_security_policy_digest(&env)
        .expect("effective digest");
    assert_eq!(expected, format!("v1:sha256:{digest}"));
    assert_eq!(digest.len(), 64);
    let public_identities_only =
        HashMap::from([("HYPE_ACCOUNT_ID".to_owned(), EXECUTION_ACCOUNT.to_owned())]);
    assert_eq!(
        config_with_policy(&policy)
            .expected_live_acknowledgement(&public_identities_only)
            .expect("digest does not read signing credentials"),
        expected
    );
    let acknowledged = acknowledged_policy(&policy, &env);
    assert_eq!(
        config_with_policy(&acknowledged)
            .expected_live_acknowledgement(&env)
            .expect("same effective policy"),
        expected
    );

    let tampered = acknowledged.replace(
        "max_hot_trading_balance_microusd = 200000000",
        "max_hot_trading_balance_microusd = 200000001",
    );
    assert_eq!(
        config_with_policy(&tampered).validate_at(&env, at("2026-08-31T23:59:59Z")),
        Err(ConfigError::SecurityPolicy(
            SecurityPolicyError::AcknowledgementMismatch
        ))
    );

    let changed_runtime = live_runtime_toml().replace(
        "deposit_cooldown_seconds = 3600",
        "deposit_cooldown_seconds = 3601",
    );
    let config = Config::from_toml_with_security_policy(&changed_runtime, &acknowledged)
        .expect("changed runtime parses");
    assert_eq!(
        config.validate_at(&env, at("2026-08-31T23:59:59Z")),
        Err(ConfigError::SecurityPolicy(
            SecurityPolicyError::AcknowledgementMismatch
        ))
    );
}

#[test]
fn acknowledged_caps_and_reserve_drive_effective_pacing() {
    let env = live_environment();
    let runtime = live_runtime_toml().replace(
        "execution = { max_order_usdc = 25.0",
        "execution = { max_order_usdc = 40.0",
    );
    let policy = live_policy_template();
    let expected = Config::from_toml_with_security_policy(&runtime, &policy)
        .expect("live documents")
        .expected_live_acknowledgement(&env)
        .expect("effective acknowledgement");
    let acknowledged = policy.replace(
        "live_acknowledgement = \"\"",
        &format!("live_acknowledgement = \"{expected}\""),
    );
    let config = Config::from_toml_with_security_policy(&runtime, &acknowledged)
        .expect("acknowledged live documents");
    config
        .validate_at(&env, at("2026-08-24T00:00:00Z"))
        .expect("effective pacing cap is acknowledged");
    let limits = PacingLimits::from_config(&config).expect("validated pacing limits");
    assert_eq!(
        limits.max_daily_notional_usdc,
        UsdcMicros::from_micros(25_000_000)
    );
    assert_eq!(
        limits.fixed_reserve_usdc,
        UsdcMicros::from_micros(1_000_000)
    );

    let received_at = Utc
        .with_ymd_and_hms(2026, 8, 24, 8, 0, 0)
        .single()
        .expect("valid UTC fixture");
    let mut state = PacingState::default();
    state
        .reconcile_capital(
            &[CapitalEvent::Deposit(DepositEvent {
                event_id: "policy-reserve".to_owned(),
                amount_usdc: UsdcMicros::from_micros(5_500_000),
                received_at,
                confirmed_at: Some(received_at),
                confirmation_count: 2,
                admission_approved_at: Some(received_at),
            })],
            Utc.with_ymd_and_hms(2026, 8, 24, 10, 0, 0)
                .single()
                .expect("valid UTC fixture"),
            &limits,
        )
        .expect("capital admitted");
    let decision = state
        .decide(
            &DecisionInput {
                at: Utc
                    .with_ymd_and_hms(2026, 8, 24, 12, 0, 0)
                    .single()
                    .expect("valid UTC fixture"),
                observed_spot_usdc: UsdcMicros::from_micros(5_500_000),
                capital_history_complete: true,
                manual_pause: false,
            },
            &limits,
        )
        .expect("fail-closed pacing decision");
    assert_eq!(decision.decision().planned_usdc, UsdcMicros::from_micros(0));
    assert_eq!(
        decision.decision().reason,
        DecisionReason::ReserveBelowMinimum
    );
}

#[derive(Clone)]
struct MutableClock(Arc<Mutex<DateTime<Utc>>>);

impl MutableClock {
    fn new(now: DateTime<Utc>) -> Self {
        Self(Arc::new(Mutex::new(now)))
    }

    fn set(&self, now: DateTime<Utc>) {
        *self.0.lock().expect("clock lock") = now;
    }
}

impl Clock for MutableClock {
    fn now(&self) -> DateTime<Utc> {
        *self.0.lock().expect("clock lock")
    }
}

#[derive(Clone, Default)]
struct RecordingExchange(Arc<Mutex<Vec<OrderIntent>>>);

impl Exchange for RecordingExchange {
    fn mode(&self) -> &'static str {
        "recording"
    }

    fn submit(&mut self, intent: &OrderIntent) -> Result<Submission, ExchangeError> {
        self.0.lock().expect("intent lock").push(intent.clone());
        Ok(Submission::Simulated)
    }
}

#[test]
fn fee_ceiling_and_acknowledgement_expiry_are_enforced_per_action() {
    let env = live_environment();
    let policy = live_policy_template();
    let acknowledged = acknowledged_policy(&policy, &env);
    let config = config_with_policy(&acknowledged);
    let clock = MutableClock::new(at("2026-08-31T23:59:59Z"));

    let recording = RecordingExchange::default();
    let mut executor =
        Executor::from_config_with_clock(recording.clone(), &config, &env, clock.clone())
            .expect("live policy authorizes the action boundary");
    assert_eq!(executor.execute(10.0), Ok(Submission::Simulated));
    assert_eq!(
        recording.0.lock().expect("intent lock")[0].max_purchase_fee_bps,
        5
    );
    clock.set(at(EXPIRY));
    assert!(matches!(
        executor.execute(10.0),
        Err(ExchangeError::Rejected(message)) if message.contains("expired")
    ));

    clock.set(at("2026-08-31T23:59:59Z"));
    let mut guarded = bootstrap_with_clock(&config, &env, clock.clone(), |_| {
        Box::new(DryRunExchange::default())
    })
    .expect("live exchange is policy wrapped");
    assert_eq!(
        guarded.submit(&OrderIntent {
            notional_usdc: 10.0,
            max_slippage_bps: 20,
            max_purchase_fee_bps: 5,
        }),
        Ok(Submission::Simulated)
    );
    assert!(matches!(
        guarded.submit(&OrderIntent {
            notional_usdc: 10.0,
            max_slippage_bps: 20,
            max_purchase_fee_bps: 6,
        }),
        Err(ExchangeError::Rejected(message)) if message.contains("fee")
    ));
    clock.set(at(EXPIRY));
    assert!(matches!(
        guarded.submit(&OrderIntent {
            notional_usdc: 10.0,
            max_slippage_bps: 20,
            max_purchase_fee_bps: 5,
        }),
        Err(ExchangeError::Rejected(message)) if message.contains("expired")
    ));
}

#[test]
fn resolved_execution_identity_is_normalized_and_digest_bound() {
    let env = live_environment();
    let policy = live_policy_template();
    let lowercase = config_with_policy(&policy)
        .expected_live_acknowledgement(&env)
        .expect("lowercase identity");
    let mut mixed_case = env.clone();
    mixed_case.insert(
        "HYPE_ACCOUNT_ID".to_owned(),
        "0x11111111111111111111111111111111111111AA".to_owned(),
    );
    let different = config_with_policy(&policy)
        .expected_live_acknowledgement(&mixed_case)
        .expect("mixed-case identity normalizes");
    assert_eq!(lowercase, different);

    let acknowledged = acknowledged_policy(&policy, &env);
    let mut changed = env;
    changed.insert(
        "HYPE_ACCOUNT_ID".to_owned(),
        OTHER_EXECUTION_ACCOUNT.to_owned(),
    );
    assert_eq!(
        config_with_policy(&acknowledged).validate_at(&changed, at("2026-08-31T23:59:59Z")),
        Err(ConfigError::SecurityPolicy(
            SecurityPolicyError::AcknowledgementMismatch
        ))
    );
}

#[test]
fn resolved_parent_identity_is_digest_bound_when_inheritance_is_enabled() {
    let mut env = live_environment();
    env.insert("HYPE_PARENT_ACCOUNT".to_owned(), PARENT_ACCOUNT.to_owned());
    let policy = live_policy_template()
        .replace(
            "funding_mode = \"external_deposit_only\"",
            "funding_mode = \"traced_parent_transfer\"",
        )
        .replace(
            "allow_traced_parent_transfer_admission = false",
            "allow_traced_parent_transfer_admission = true",
        )
        .replace(
            "admitted_parent_account_env = \"\"",
            "admitted_parent_account_env = \"HYPE_PARENT_ACCOUNT\"",
        );
    let acknowledged = acknowledged_policy(&policy, &env);
    env.insert(
        "HYPE_PARENT_ACCOUNT".to_owned(),
        OTHER_PARENT_ACCOUNT.to_owned(),
    );
    assert_eq!(
        config_with_policy(&acknowledged).validate_at(&env, at("2026-08-31T23:59:59Z")),
        Err(ConfigError::SecurityPolicy(
            SecurityPolicyError::AcknowledgementMismatch
        ))
    );
}

#[test]
fn unsafe_custody_and_staking_modes_fail_before_live() {
    let env = live_environment();
    let subaccount = live_policy_template().replace(
        "execution_account_kind = \"dedicated_master\"",
        "execution_account_kind = \"subaccount\"",
    );
    assert!(matches!(
        config_with_policy(&subaccount).expected_live_acknowledgement(&env),
        Err(ConfigError::SecurityPolicy(SecurityPolicyError::Invalid(_)))
    ));

    let staking_enabled = live_policy_template().replace("enabled = false", "enabled = true");
    assert!(matches!(
        Config::from_toml_with_security_policy(&live_runtime_toml(), &staking_enabled),
        Err(ConfigError::SecurityPolicy(SecurityPolicyError::Invalid(_)))
    ));
}

#[test]
fn noncanonical_expiry_is_rejected_during_policy_parse() {
    let policy = live_policy_template().replace(EXPIRY, "2026-09-01T01:00:00+01:00");
    assert!(matches!(
        Config::from_toml_with_security_policy(&live_runtime_toml(), &policy),
        Err(ConfigError::SecurityPolicy(SecurityPolicyError::Invalid(_)))
    ));
}
