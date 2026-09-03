use chrono::{DateTime, TimeZone, Utc};
use hype_accumulator::{
    bootstrap_with_clock,
    clock::FixedClock,
    config::{Config, ConfigError, SecurityPolicyError},
    exchange::DryRunExchange,
    execution::Executor,
    pacing::{
        CapitalEvent, DecisionInput, DecisionReason, DepositEvent, PacingError, PacingLimits,
        PacingState, UsdcMicros,
    },
};
use std::{
    cell::Cell,
    collections::{BTreeSet, HashMap},
    rc::Rc,
};

const EXECUTION_ACCOUNT: &str = "0x11111111111111111111111111111111111111aa";
const OTHER_EXECUTION_ACCOUNT: &str = "0x2222222222222222222222222222222222222222";
const VALIDATOR: &str = "0x3333333333333333333333333333333333333333";
const OTHER_VALIDATOR: &str = "0x6666666666666666666666666666666666666666";
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
fn signer_free_runtime_refuses_signing_material_live_mode_and_missing_policy() {
    let runtime = include_str!("fixtures/safe.toml");
    let policy = include_str!("../config/security-policy.example.toml");
    let safe = Config::from_toml_with_security_policy(runtime, policy)
        .expect("safe signer-free documents parse");
    safe.validate_signer_free_runtime(&HashMap::new())
        .expect("signer-free dry-run is accepted");

    let mut scheduled = safe.clone();
    scheduled.manual_halt = false;
    scheduled
        .validate_signer_free_runtime(&HashMap::new())
        .expect("scheduled signer-free planning may clear the runtime halt");
    assert!(scheduled.validate(&HashMap::new()).is_err());

    let unhalted_policy = Config::from_toml_with_security_policy(
        runtime,
        &policy.replace("manual_halt = true", "manual_halt = false"),
    )
    .expect("unhalted policy still parses");
    assert!(unhalted_policy
        .validate_signer_free_runtime(&HashMap::new())
        .is_err());

    let with_signer = HashMap::from([(
        "HYPE_SIGNING_KEY".to_owned(),
        "must-never-be-read".to_owned(),
    )]);
    assert!(matches!(
        safe.validate_signer_free_runtime(&with_signer),
        Err(ConfigError::Invalid(message))
            if message.contains("refuses a populated signing-key")
    ));

    let live = Config::from_toml_with_security_policy(
        &runtime.replace("dry_run = true", "dry_run = false"),
        policy,
    )
    .expect("unsafe mode still parses");
    assert!(matches!(
        live.validate_signer_free_runtime(&HashMap::new()),
        Err(ConfigError::Invalid(message)) if message.contains("dry_run=true")
    ));

    let missing_policy = Config::from_toml(runtime).expect("runtime parses");
    assert_eq!(
        missing_policy.validate_signer_free_runtime(&HashMap::new()),
        Err(ConfigError::MissingSecurityPolicy)
    );
}

#[test]
fn offline_install_requires_an_explicit_halted_dry_run_policy() {
    let runtime = include_str!("fixtures/safe.toml");
    let policy = include_str!("../config/security-policy.example.toml");
    let safe = Config::from_toml_with_security_policy(runtime, policy)
        .expect("offline install documents parse");
    safe.validate_offline_install(&HashMap::new())
        .expect("halted dry-run install is accepted");

    let missing_policy = Config::from_toml(runtime).expect("runtime document parses");
    assert_eq!(
        missing_policy.validate_offline_install(&HashMap::new()),
        Err(ConfigError::MissingSecurityPolicy)
    );

    for (field, invalid_runtime) in [
        (
            "dry_run",
            runtime.replace("dry_run = true", "dry_run = false"),
        ),
        (
            "manual_halt",
            runtime.replace("manual_halt = true", "manual_halt = false"),
        ),
        ("live_approved", format!("live_approved = true\n{runtime}")),
    ] {
        let invalid = Config::from_toml_with_security_policy(&invalid_runtime, policy)
            .expect("invalid-mode documents remain typed");
        assert_eq!(
            invalid.validate_offline_install(&HashMap::new()),
            Err(ConfigError::Invalid(
                "offline install requires dry_run=true, manual_halt=true, and live_approved=false"
                    .into()
            )),
            "{field} must fail closed"
        );
    }
}

#[test]
fn dry_run_policy_reserve_must_fit_the_runtime_admission_cap() {
    let policy = include_str!("../config/security-policy.example.toml")
        .replace("reserve_microusd = 0", "reserve_microusd = 100000000");
    let config =
        Config::from_toml_with_security_policy(include_str!("fixtures/safe.toml"), &policy)
            .expect("typed dry-run documents");
    assert!(matches!(
        config.validate_at(&HashMap::new(), at("2026-08-24T00:00:00Z")),
        Err(ConfigError::Invalid(message)) if message.contains("reserve")
    ));
    assert_eq!(
        PacingLimits::from_config(&config),
        Err(PacingError::InvalidLimits)
    );
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

#[test]
fn schedule_capacity_accounts_for_the_global_reserve_across_tranches() {
    let env = live_environment();
    let runtime = live_runtime_toml()
        .replace(
            "max_automatically_deployable_usdc = 100.0",
            "max_automatically_deployable_usdc = 80.0",
        )
        .replace("max_order_usdc = 25.0", "max_order_usdc = 10.0")
        .replace("target_horizon_days = 365", "target_horizon_days = 7");
    assert!(matches!(
        Config::from_toml(&runtime)
            .expect("runtime parses")
            .validate_at(&env, at("2026-08-24T00:00:00Z")),
        Err(ConfigError::Invalid(message)) if message.contains("configured schedule")
    ));

    let multi_tranche_policy = live_policy_template()
        .replace(
            "max_auto_deposit_microusd = 100000000",
            "max_auto_deposit_microusd = 80000000",
        )
        .replace(
            "max_daily_notional_microusd = 25000000",
            "max_daily_notional_microusd = 10000000",
        )
        .replace("reserve_microusd = 1000000", "reserve_microusd = 10000000");
    let expected = Config::from_toml_with_security_policy(&runtime, &multi_tranche_policy)
        .expect("live documents")
        .expected_live_acknowledgement(&env)
        .expect("effective acknowledgement");
    let acknowledged = multi_tranche_policy.replace(
        "live_acknowledgement = \"\"",
        &format!("live_acknowledgement = \"{expected}\""),
    );
    assert!(matches!(
        Config::from_toml_with_security_policy(&runtime, &acknowledged)
            .expect("acknowledged multi-tranche documents")
            .validate_at(&env, at("2026-08-24T00:00:00Z")),
        Err(ConfigError::Invalid(message)) if message.contains("configured schedule")
    ));

    let one_tranche_runtime = runtime
        .replace(
            "yearly_deployment_cap_usdc = 500.0",
            "yearly_deployment_cap_usdc = 80.0",
        )
        .replace(
            "cumulative_deployment_cap_usdc = 1000.0",
            "cumulative_deployment_cap_usdc = 80.0",
        );
    let one_tranche_policy = multi_tranche_policy
        .replace(
            "max_yearly_deployable_microusd = 500000000",
            "max_yearly_deployable_microusd = 80000000",
        )
        .replace(
            "max_cumulative_deployable_microusd = 1000000000",
            "max_cumulative_deployable_microusd = 80000000",
        );
    let expected =
        Config::from_toml_with_security_policy(&one_tranche_runtime, &one_tranche_policy)
            .expect("single-tranche live documents")
            .expected_live_acknowledgement(&env)
            .expect("effective acknowledgement");
    let acknowledged = one_tranche_policy.replace(
        "live_acknowledgement = \"\"",
        &format!("live_acknowledgement = \"{expected}\""),
    );
    Config::from_toml_with_security_policy(&one_tranche_runtime, &acknowledged)
        .expect("acknowledged documents")
        .validate_at(&env, at("2026-08-24T00:00:00Z"))
        .expect("70 USDC capacity fits the 80 USDC cap minus 10 USDC reserve");
}

#[test]
fn live_action_construction_requires_durable_authorization() {
    let env = live_environment();
    let policy = live_policy_template();
    let acknowledged = acknowledged_policy(&policy, &env);
    let config = config_with_policy(&acknowledged);
    let before_expiry = at("2026-08-31T23:59:59Z");
    config
        .validate_at(&env, before_expiry)
        .expect("effective live policy validates");
    assert!(matches!(
        Executor::from_config_with_clock(
            DryRunExchange::default(),
            &config,
            &env,
            FixedClock(before_expiry),
        ),
        Err(ConfigError::LiveExecutionUnavailable)
    ));
    let factory_called = Rc::new(Cell::new(false));
    let marker = Rc::clone(&factory_called);
    assert!(matches!(
        bootstrap_with_clock(&config, &env, FixedClock(before_expiry), move |_| {
            marker.set(true);
            Box::new(DryRunExchange::default())
        }),
        Err(ConfigError::LiveExecutionUnavailable)
    ));
    assert!(!factory_called.get());
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
fn multiple_validators_are_normalized_as_a_digest_bound_set() {
    let env = live_environment();
    let single = format!("validator_allowlist = [\"{VALIDATOR}\"]");
    let runtime = live_runtime_toml().replace(
        &single,
        &format!("validator_allowlist = [\"{OTHER_VALIDATOR}\", \"{VALIDATOR}\"]"),
    );
    let first = live_policy_template().replace(
        &single,
        &format!("validator_allowlist = [\"{VALIDATOR}\", \"{OTHER_VALIDATOR}\"]"),
    );
    let reversed = first.replace(
        &format!("validator_allowlist = [\"{VALIDATOR}\", \"{OTHER_VALIDATOR}\"]"),
        &format!("validator_allowlist = [\"{OTHER_VALIDATOR}\", \"{VALIDATOR}\"]"),
    );
    let expected = Config::from_toml_with_security_policy(&runtime, &first)
        .expect("multi-validator documents")
        .expected_live_acknowledgement(&env)
        .expect("nonempty validator set is supported");
    assert_eq!(
        Config::from_toml_with_security_policy(&runtime, &reversed)
            .expect("reversed validator documents")
            .expected_live_acknowledgement(&env)
            .expect("validator order is normalized"),
        expected
    );
    let acknowledged = first.replace(
        "live_acknowledgement = \"\"",
        &format!("live_acknowledgement = \"{expected}\""),
    );
    Config::from_toml_with_security_policy(&runtime, &acknowledged)
        .expect("acknowledged multi-validator documents")
        .validate_at(&env, at("2026-08-31T23:59:59Z"))
        .expect("matching nonempty validator sets validate");
}

#[test]
fn supported_isolated_account_kinds_are_acknowledged_and_digest_bound() {
    let env = live_environment();
    let acknowledgements = ["dedicated_master", "subaccount", "vault"]
        .into_iter()
        .map(|kind| {
            let policy = live_policy_template().replace(
                "execution_account_kind = \"dedicated_master\"",
                &format!("execution_account_kind = \"{kind}\""),
            );
            let acknowledgement = acknowledged_policy(&policy, &env);
            config_with_policy(&acknowledgement)
                .validate_at(&env, at("2026-08-31T23:59:59Z"))
                .expect("supported isolated account kind validates");
            acknowledgement
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(acknowledgements.len(), 3);
}

#[test]
fn unsafe_custody_and_staking_modes_fail_before_live() {
    let env = live_environment();
    let unapproved = live_policy_template().replace(
        "execution_account_kind = \"dedicated_master\"",
        "execution_account_kind = \"unapproved\"",
    );
    assert!(matches!(
        config_with_policy(&unapproved).expected_live_acknowledgement(&env),
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

#[test]
fn effective_live_order_policy_extracts_the_approved_values() {
    let env = live_environment();
    let policy = acknowledged_policy(&live_policy_template(), &env);
    let effective = config_with_policy(&policy)
        .effective_live_order_policy(&env)
        .expect("live-approved policy yields an effective order policy");

    assert_eq!(effective.max_slippage_bps, 20);
    assert_eq!(effective.max_purchase_fee_bps, 5);
    assert_eq!(effective.max_venue_clock_lag_ms, 500);
    assert_eq!(effective.venue_clock_evidence_stale_after_seconds, 30);
    assert_eq!(effective.book_stale_after_seconds, 15);
    assert_eq!(effective.account_history_stale_after_seconds, 30);
    assert_eq!(effective.fee_schedule_stale_after_seconds, 3600);
    assert_eq!(effective.signal_stale_after_seconds, 86400);
    assert_eq!(effective.validator_allowlist, vec![VALIDATOR.to_owned()]);
    assert_eq!(effective.residual_hype_wei, 1000);
    assert_eq!(effective.fill_registration_deadline_seconds, 300);
    assert_eq!(effective.lot_eligibility_max_age_seconds, 86400);
    assert_eq!(
        effective.policy_acknowledgement_valid_through_at,
        at(EXPIRY)
    );
}

#[test]
fn effective_live_order_policy_fails_the_same_way_as_the_acknowledgement() {
    let env = live_environment();
    // Reuses the same invalid-policy fixture exercised above for
    // `expected_live_acknowledgement`: an execution account kind that is
    // still "unapproved" must fail identically for this accessor, since
    // both share the same underlying live-contract validation.
    let unapproved = live_policy_template().replace(
        "execution_account_kind = \"dedicated_master\"",
        "execution_account_kind = \"unapproved\"",
    );
    assert!(matches!(
        config_with_policy(&unapproved).effective_live_order_policy(&env),
        Err(ConfigError::SecurityPolicy(SecurityPolicyError::Invalid(_)))
    ));

    let no_policy = Config::from_toml(&live_runtime_toml()).expect("runtime-only config parses");
    assert!(matches!(
        no_policy.effective_live_order_policy(&env),
        Err(ConfigError::MissingSecurityPolicy)
    ));
}
