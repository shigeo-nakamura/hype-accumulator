use super::*;
use crate::{
    config::{CarryOverPolicy, UtcSchedule},
    pacing::DecisionReason,
    signal::{FreshnessRequirement, LiveSignalNormalizer, RevisionQuery, SnapshotRequest},
    signal_source::{build_snapshot, plan_snapshot, TopOfBookObservation},
};
use chrono::{NaiveDate, TimeDelta, TimeZone};
use rust_decimal::Decimal;
use serde_json::Value;

const RAW_SIGNALS: &str = include_str!("../../fixtures/signal-snapshots-v1/raw.json");

fn at(year: i32, month: u32, day: u32, hour: u32, minute: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(year, month, day, hour, minute, 0)
        .single()
        .expect("valid UTC fixture")
}

fn ms(value: DateTime<Utc>) -> u64 {
    u64::try_from(value.timestamp_millis()).expect("positive fixture timestamp")
}

fn usd(value: u64) -> UsdcMicros {
    UsdcMicros::checked_from_whole_usdc(value).expect("small fixture amount")
}

fn limits() -> PacingLimits {
    PacingLimits {
        min_deposit_confirmations: 2,
        max_automatically_admitted_usdc: usd(1_000),
        yearly_admission_cap_usdc: usd(1_000),
        cumulative_admission_cap_usdc: usd(2_000),
        deposit_cooldown_seconds: 1,
        min_order_usdc: usd(1),
        max_daily_notional_usdc: usd(25),
        fixed_reserve_usdc: UsdcMicros::default(),
        fee_spread_reserve_bps: 0,
        final_catch_up_days: 7,
        carry_over_policy: CarryOverPolicy::HoldForApproval,
        utc_hour: 12,
        utc_minute: 0,
        weekdays: (1..=7).collect(),
    }
}

fn config(directory: &Path, history_start_ms: u64) -> RuntimeConfig {
    RuntimeConfig::from_toml(&format!(
        r#"
schema_version = 1
state_directory = "{}"
protected_anchor_path = "{}"
admission_approvals_path = "{}"
signal_snapshot_path = "{}"
status_path = "{}"
metrics_path = "{}"
cycle_report_path = "{}"
movement_history_start_ms = {history_start_ms}
movement_overlap_ms = 86400000
stuck_after_seconds = 3600
"#,
        directory.join("state").display(),
        directory.join("protected/ledger-anchor.json").display(),
        directory.join("inputs/admissions.json").display(),
        directory.join("inputs/signal.json").display(),
        directory.join("public/status.json").display(),
        directory.join("public/metrics.prom").display(),
        directory.join("private/cycle.json").display(),
    ))
    .expect("valid runtime config")
}

fn status(observed_at: DateTime<Utc>, usdc: f64) -> AccumulatorStatus {
    AccumulatorStatus::new(
        usdc,
        0.0,
        10.0,
        observed_at,
        None,
        "daily",
        Some("HYPE attribution unavailable; account holdings excluded".to_owned()),
    )
    .expect("valid status")
}

fn status_window(
    started_at: DateTime<Utc>,
    completed_at: DateTime<Utc>,
    usdc: f64,
) -> AccumulatorStatus {
    AccumulatorStatus::new_with_balance_window(
        usdc,
        0.0,
        10.0,
        started_at,
        completed_at,
        None,
        "daily",
        Some("HYPE attribution unavailable; account holdings excluded".to_owned()),
    )
    .expect("valid status window")
}

fn deposit(event_id: &str, occurred_at: DateTime<Utc>, amount: u64) -> HyperliquidAccountMovement {
    HyperliquidAccountMovement {
        event_id: event_id.to_owned(),
        timestamp_ms: ms(occurred_at),
        kind: HyperliquidAccountMovementKind::ExternalDeposit,
        token: "USDC".to_owned(),
        amount: Decimal::from(amount),
        transaction_hash: None,
        counterparty: None,
    }
}

fn withdrawal(
    event_id: &str,
    occurred_at: DateTime<Utc>,
    amount: u64,
) -> HyperliquidAccountMovement {
    HyperliquidAccountMovement {
        event_id: event_id.to_owned(),
        timestamp_ms: ms(occurred_at),
        kind: HyperliquidAccountMovementKind::ExternalWithdrawal,
        token: "USDC".to_owned(),
        amount: -Decimal::from(amount),
        transaction_hash: None,
        counterparty: None,
    }
}

fn approvals(
    event_id: &str,
    confirmed_at: DateTime<Utc>,
    approved_at: DateTime<Utc>,
) -> AdmissionApprovals {
    AdmissionApprovals::from_json(&format!(
        r#"{{
  "schema_version": 1,
  "approvals": [{{
    "event_id": "{event_id}",
    "confirmed_at": "{}",
    "confirmation_count": 2,
    "approved_at": "{}"
  }}]
}}"#,
        confirmed_at.to_rfc3339(),
        approved_at.to_rfc3339(),
    ))
    .expect("valid approvals")
}

fn day(value: &str) -> NaiveDate {
    NaiveDate::parse_from_str(value, "%Y-%m-%d").expect("valid fixture date")
}

fn signal(decision_at: DateTime<Utc>) -> SignalSnapshot {
    signal_for(decision_at, "2026-07-06")
}

fn signal_for(decision_at: DateTime<Utc>, core_date: &str) -> SignalSnapshot {
    let core = RevisionQuery::new("fixture-core", "v1", "hype_market", day(core_date))
        .expect("core query");
    let auxiliary = RevisionQuery::new("fixture-aux", "v1", "btc_etf_net_flow", day("2026-07-03"))
        .expect("auxiliary query");
    LiveSignalNormalizer::normalize_json(RAW_SIGNALS)
        .expect("normalized signals")
        .snapshot(&SnapshotRequest::new(
            decision_at,
            FreshnessRequirement::new(core, 60).expect("core freshness"),
            FreshnessRequirement::new(auxiliary, 604_800).expect("auxiliary freshness"),
        ))
        .expect("signal snapshot")
}

#[test]
fn runtime_config_defaults_and_bounds_the_signal_snapshot_freshness() {
    let directory = tempfile::tempdir().expect("temporary directory");
    assert_eq!(
        config(directory.path(), 1).signal_snapshot_stale_after_seconds(),
        900
    );
    let explicit = |value: &str| {
        RuntimeConfig::from_toml(&format!(
            r#"
schema_version = 1
state_directory = "{}"
protected_anchor_path = "{}"
admission_approvals_path = "{}"
signal_snapshot_path = "{}"
status_path = "{}"
metrics_path = "{}"
cycle_report_path = "{}"
movement_history_start_ms = 1
signal_snapshot_stale_after_seconds = {value}
"#,
            directory.path().join("state").display(),
            directory
                .path()
                .join("protected/ledger-anchor.json")
                .display(),
            directory.path().join("inputs/admissions.json").display(),
            directory.path().join("inputs/signal.json").display(),
            directory.path().join("public/status.json").display(),
            directory.path().join("public/metrics.prom").display(),
            directory.path().join("private/cycle.json").display(),
        ))
    };
    assert_eq!(
        explicit("120")
            .expect("explicit freshness")
            .signal_snapshot_stale_after_seconds(),
        120
    );
    assert!(matches!(explicit("0"), Err(RuntimeError::InvalidConfig(_))));
}

#[test]
fn producer_snapshot_makes_the_boundary_decision_purchase_eligible() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let start = at(2026, 7, 6, 8, 0);
    let deposit_at = at(2026, 7, 6, 9, 0);
    let boundary = at(2026, 7, 6, 12, 0);
    let produced_at = boundary - TimeDelta::seconds(90);
    let observed_at = boundary + TimeDelta::seconds(4);
    let schedule = UtcSchedule {
        utc_hour: 12,
        utc_minute: 0,
        weekdays: (1..=7).collect(),
    };
    let plan = plan_snapshot(produced_at, &schedule, 900).expect("producer plan");
    assert_eq!(plan.decision_at, boundary);
    let signal = build_snapshot(
        &plan,
        &TopOfBookObservation {
            bid_price: Decimal::new(85_008, 3),
            ask_price: Decimal::new(85_009, 3),
            venue_time_ms: ms(produced_at),
            fetched_at: produced_at + TimeDelta::milliseconds(400),
        },
    )
    .expect("producer snapshot");
    let runtime_config = config(directory.path(), ms(start));
    let movement = deposit("deposit-with-producer-signal", deposit_at, 100);
    let admission = approvals("deposit-with-producer-signal", deposit_at, deposit_at);
    let mut runtime =
        SignerFreeRuntime::open(runtime_config.clone(), limits()).expect("open runtime");

    let report = runtime
        .apply_cycle(RuntimeCycleInput {
            observed_at,
            scan_start_ms: ms(start),
            scan_end_ms: ms(observed_at),
            movements: std::slice::from_ref(&movement),
            approvals: &admission,
            signal: Some(&signal),
            accumulator: status(observed_at, 100.0),
            capital_history_complete: true,
            manual_pause: false,
            api_errors: 0,
            decision_mode: DecisionMode::DryRun,
        })
        .expect("producer snapshot binds to the configured boundary");

    assert!(report.is_new_decision());
    assert!(report.signal_available);
    let decision = report.decision().expect("durable boundary decision");
    assert_eq!(decision.reason, DecisionReason::Planned);
    assert!(!decision.planned_usdc.is_zero());
    assert_eq!(runtime.state.stale_signal_events_total, 0);

    let missing_signal = at(2026, 7, 7, 12, 0);
    let stale_report = runtime
        .apply_cycle(RuntimeCycleInput {
            observed_at: missing_signal,
            scan_start_ms: runtime.next_scan_start_ms(),
            scan_end_ms: ms(missing_signal),
            movements: std::slice::from_ref(&movement),
            approvals: &admission,
            signal: Some(&signal),
            accumulator: status(missing_signal, 100.0),
            capital_history_complete: true,
            manual_pause: false,
            api_errors: 0,
            decision_mode: DecisionMode::DryRun,
        })
        .expect("yesterday's snapshot cannot serve today's boundary");
    assert!(!stale_report.signal_available);
    assert_eq!(
        stale_report.decision().expect("durable skip").reason,
        DecisionReason::CoreSignalUnavailable
    );
}

#[test]
fn runtime_config_rejects_colocated_anchor_and_relative_paths() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let state = directory.path().join("state");
    let colocated = format!(
        r#"
schema_version = 1
state_directory = "{}"
protected_anchor_path = "{}"
admission_approvals_path = "{}"
signal_snapshot_path = "{}"
status_path = "{}"
metrics_path = "{}"
cycle_report_path = "{}"
movement_history_start_ms = 1
"#,
        state.display(),
        state.join("anchor.json").display(),
        directory.path().join("admissions.json").display(),
        directory.path().join("signal.json").display(),
        directory.path().join("status.json").display(),
        directory.path().join("metrics.prom").display(),
        directory.path().join("cycle.json").display(),
    );
    assert!(matches!(
        RuntimeConfig::from_toml(&colocated),
        Err(RuntimeError::InvalidConfig(_))
    ));
    assert!(matches!(
        RuntimeConfig::from_toml(
            r#"
schema_version = 1
state_directory = "relative"
protected_anchor_path = "/tmp/anchor"
admission_approvals_path = "/tmp/admissions"
signal_snapshot_path = "/tmp/signal"
status_path = "/tmp/status"
metrics_path = "/tmp/metrics"
cycle_report_path = "/tmp/report"
movement_history_start_ms = 1
"#
        ),
        Err(RuntimeError::InvalidConfig(_))
    ));
    assert!(matches!(
        RuntimeConfig::from_toml(
            &colocated.replace(
                &state.join("anchor.json").display().to_string(),
                &directory
                    .path()
                    .join("protected/../anchor.json")
                    .display()
                    .to_string(),
            )
        ),
        Err(RuntimeError::InvalidConfig(_))
    ));
    let reserved_artifact = colocated
        .replace(
            &state.join("anchor.json").display().to_string(),
            &directory
                .path()
                .join("protected/anchor.json")
                .display()
                .to_string(),
        )
        .replace(
            &directory.path().join("cycle.json").display().to_string(),
            &state.join(STATE_FILE_NAME).display().to_string(),
        );
    assert!(matches!(
        RuntimeConfig::from_toml(&reserved_artifact),
        Err(RuntimeError::InvalidConfig(_))
    ));
}

#[test]
fn runtime_config_rejects_configured_file_ancestor_relationships() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let status_path = directory.path().join("public/status");
    let input = format!(
        r#"
schema_version = 1
state_directory = "{}"
protected_anchor_path = "{}"
admission_approvals_path = "{}"
signal_snapshot_path = "{}"
status_path = "{}"
metrics_path = "{}"
cycle_report_path = "{}"
movement_history_start_ms = 1
"#,
        directory.path().join("state").display(),
        directory.path().join("protected/anchor.json").display(),
        directory.path().join("inputs/admissions.json").display(),
        directory.path().join("inputs/signal.json").display(),
        status_path.display(),
        status_path.join("metrics.prom").display(),
        directory.path().join("private/cycle.json").display(),
    );

    assert!(matches!(
        RuntimeConfig::from_toml(&input),
        Err(RuntimeError::InvalidConfig(message)) if message.contains("ancestors")
    ));
    assert!(!status_path.exists());
}

#[cfg(unix)]
#[test]
fn runtime_lock_replacement_blocks_a_second_runtime_and_stops_the_holder() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let start = at(2026, 7, 6, 8, 0);
    let observed_at = at(2026, 7, 6, 10, 0);
    let runtime_config = config(directory.path(), ms(start));
    let mut runtime =
        SignerFreeRuntime::open(runtime_config.clone(), limits()).expect("open runtime");
    let lock_path = runtime_config.state_directory.join(RUNTIME_LOCK_FILE_NAME);
    fs::remove_file(&lock_path).expect("unlink held runtime lock");
    fs::write(&lock_path, b"replacement").expect("replace runtime lock");

    assert!(matches!(
        SignerFreeRuntime::open(runtime_config.clone(), limits()),
        Err(RuntimeError::AlreadyRunning)
    ));
    let error = runtime
        .apply_cycle(RuntimeCycleInput {
            observed_at,
            scan_start_ms: ms(start),
            scan_end_ms: ms(observed_at),
            movements: &[],
            approvals: &AdmissionApprovals::empty(),
            signal: None,
            accumulator: status(observed_at, 0.0),
            capital_history_complete: true,
            manual_pause: true,
            api_errors: 0,
            decision_mode: DecisionMode::DryRun,
        })
        .expect_err("holder fails closed after runtime lock replacement");
    assert!(matches!(error, RuntimeError::UnsafeRuntimeLock));
    assert!(!runtime_config
        .state_directory
        .join(STATE_FILE_NAME)
        .exists());
}

#[cfg(unix)]
#[test]
fn runtime_open_rejects_a_symlinked_runtime_lock() {
    use std::os::unix::fs::symlink;

    let directory = tempfile::tempdir().expect("temporary directory");
    let runtime_config = config(directory.path(), 1);
    fs::create_dir_all(&runtime_config.state_directory).expect("state directory");
    let target = directory.path().join("lock-target");
    fs::write(&target, b"").expect("lock target");
    symlink(
        &target,
        runtime_config.state_directory.join(RUNTIME_LOCK_FILE_NAME),
    )
    .expect("symlinked runtime lock");

    assert!(matches!(
        SignerFreeRuntime::open(runtime_config, limits()),
        Err(RuntimeError::UnsafeRuntimeLock)
    ));
}

#[cfg(unix)]
#[test]
fn runtime_open_rejects_a_hard_linked_runtime_lock() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let runtime_config = config(directory.path(), 1);
    fs::create_dir_all(&runtime_config.state_directory).expect("state directory");
    let target = directory.path().join("lock-target");
    fs::write(&target, b"").expect("lock target");
    fs::hard_link(
        &target,
        runtime_config.state_directory.join(RUNTIME_LOCK_FILE_NAME),
    )
    .expect("hard-linked runtime lock");

    assert!(matches!(
        SignerFreeRuntime::open(runtime_config, limits()),
        Err(RuntimeError::UnsafeRuntimeLock)
    ));
}

#[cfg(unix)]
#[test]
fn runtime_open_rejects_canonical_file_ancestors_before_creating_them() {
    use std::os::unix::fs::symlink;

    let directory = tempfile::tempdir().expect("temporary directory");
    let public = directory.path().join("public");
    fs::create_dir_all(&public).expect("public directory");
    symlink(&public, directory.path().join("public-alias")).expect("public parent alias");
    let mut runtime_config = config(directory.path(), 1);
    let status_path = public.join("status");
    runtime_config.status_path = status_path.clone();
    runtime_config.metrics_path = directory.path().join("public-alias/status/metrics.prom");

    assert!(matches!(
        SignerFreeRuntime::open(runtime_config, limits()),
        Err(RuntimeError::InvalidConfig(message)) if message.contains("ancestor")
    ));
    assert!(!status_path.exists());
}

#[cfg(unix)]
#[test]
fn runtime_open_rejects_anchor_parent_symlinked_into_mutable_state() {
    use std::os::unix::fs::symlink;

    let directory = tempfile::tempdir().expect("temporary directory");
    let state = directory.path().join("state");
    fs::create_dir_all(&state).expect("state directory");
    symlink(&state, directory.path().join("protected")).expect("protected parent symlink");
    let runtime_config = config(directory.path(), 1);
    assert!(matches!(
        SignerFreeRuntime::open(runtime_config, limits()),
        Err(RuntimeError::InvalidConfig(_))
    ));
}

#[cfg(unix)]
#[test]
fn runtime_open_rejects_a_hard_linked_protected_anchor() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let protected = directory.path().join("protected");
    fs::create_dir_all(&protected).expect("protected directory");
    let alias_source = directory.path().join("anchor-alias-source.json");
    fs::write(&alias_source, b"{}\n").expect("anchor alias source");
    fs::hard_link(&alias_source, protected.join("ledger-anchor.json")).expect("hard-linked anchor");

    assert!(SignerFreeRuntime::open(config(directory.path(), 1), limits()).is_err());
}

#[cfg(unix)]
#[test]
fn runtime_open_rejects_an_output_parent_symlinked_into_state() {
    use std::os::unix::fs::symlink;

    let directory = tempfile::tempdir().expect("temporary directory");
    let state = directory.path().join("state");
    fs::create_dir_all(&state).expect("state directory");
    symlink(&state, directory.path().join("private")).expect("private parent symlink");

    assert!(matches!(
        SignerFreeRuntime::open(config(directory.path(), 1), limits()),
        Err(RuntimeError::InvalidConfig(_))
    ));
}

#[cfg(unix)]
#[test]
fn runtime_open_rejects_configured_paths_with_aliased_parents() {
    use std::os::unix::fs::symlink;

    let directory = tempfile::tempdir().expect("temporary directory");
    let public = directory.path().join("public");
    fs::create_dir_all(&public).expect("public directory");
    symlink(&public, directory.path().join("public-alias")).expect("public parent alias");
    let mut runtime_config = config(directory.path(), 1);
    runtime_config.metrics_path = directory.path().join("public-alias/status.json");

    assert!(matches!(
        SignerFreeRuntime::open(runtime_config, limits()),
        Err(RuntimeError::InvalidConfig(message)) if message.contains("same path")
    ));
}

#[test]
fn empty_ledger_requires_exact_pristine_runtime_state() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let runtime_config = config(directory.path(), 1);
    fs::create_dir_all(&runtime_config.state_directory).expect("state directory");
    let mut forged = RuntimeState::new(1);
    forged.last_complete_scan_end_ms = Some(2);
    write_private_json_atomic(
        runtime_config.state_directory.join(STATE_FILE_NAME),
        &forged,
    )
    .expect("forged pristine state");

    assert!(matches!(
        SignerFreeRuntime::open(runtime_config, limits()),
        Err(RuntimeError::RuntimeStateRollback)
    ));
}

#[test]
fn directional_send_counterparty_does_not_authorize_capital_admission() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let start = at(2026, 7, 6, 8, 0);
    let observed_at = at(2026, 7, 6, 12, 0);
    let config = config(directory.path(), ms(start));
    let mut runtime = SignerFreeRuntime::open(config, limits()).expect("open runtime");
    let mut movement = deposit("send-from-parent", start + TimeDelta::hours(1), 100);
    movement.kind = HyperliquidAccountMovementKind::InternalTransfer;
    movement.counterparty = Some("0x1111111111111111111111111111111111111111".to_owned());
    let snapshot = signal(observed_at);
    let report = runtime
        .apply_cycle(RuntimeCycleInput {
            observed_at,
            scan_start_ms: ms(start),
            scan_end_ms: ms(observed_at),
            movements: &[movement.clone()],
            approvals: &AdmissionApprovals::empty(),
            signal: Some(&snapshot),
            accumulator: status(observed_at, 100.0),
            capital_history_complete: true,
            manual_pause: false,
            api_errors: 0,
            decision_mode: DecisionMode::DryRun,
        })
        .expect("known directional transfer can be observed");
    assert!(report.capital_history_complete);
    assert!(runtime.state.pacing.deposits().is_empty());
    assert!(report.decision().unwrap().planned_usdc.is_zero());
    assert!(!report.signed_action_created);

    let transfer_approval = approvals(
        "send-from-parent",
        start + TimeDelta::hours(1),
        start + TimeDelta::hours(2),
    );
    assert!(matches!(
        runtime.apply_cycle(RuntimeCycleInput {
            observed_at: observed_at + TimeDelta::minutes(5),
            scan_start_ms: runtime.next_scan_start_ms(),
            scan_end_ms: ms(observed_at + TimeDelta::minutes(5)),
            movements: &[movement],
            approvals: &transfer_approval,
            signal: Some(&snapshot),
            accumulator: status(observed_at + TimeDelta::minutes(5), 100.0),
            capital_history_complete: true,
            manual_pause: false,
            api_errors: 0,
            decision_mode: DecisionMode::DryRun,
        }),
        Err(RuntimeError::UnknownAdmissionApproval(_))
    ));
}

#[test]
fn unapproved_deposit_stays_unallocated_and_missing_signal_is_durable_skip() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let start = at(2026, 7, 6, 8, 0);
    let observed_at = at(2026, 7, 6, 12, 0);
    let config = config(directory.path(), ms(start));
    let mut runtime = SignerFreeRuntime::open(config.clone(), limits()).expect("open runtime");
    let movement = deposit("deposit-unapproved", start + TimeDelta::hours(1), 100);
    let report = runtime
        .apply_cycle(RuntimeCycleInput {
            observed_at,
            scan_start_ms: ms(start),
            scan_end_ms: ms(observed_at),
            movements: &[movement],
            approvals: &AdmissionApprovals::empty(),
            signal: None,
            accumulator: status(observed_at, 100.0),
            capital_history_complete: true,
            manual_pause: false,
            api_errors: 0,
            decision_mode: DecisionMode::DryRun,
        })
        .expect("dry-run cycle");
    let decision = report.decision().expect("durable decision");
    assert_eq!(decision.reason, DecisionReason::CoreSignalUnavailable);
    assert!(decision.planned_usdc.is_zero());
    assert!(report.is_new_decision());
    assert!(report.economic_action_suppressed);
    assert!(!report.signed_action_created);
    assert_eq!(
        runtime
            .state
            .pacing
            .deposits()
            .get("deposit-unapproved")
            .expect("deposit tranche")
            .admitted_usdc,
        UsdcMicros::default()
    );
    assert!(config.status_path.exists());
    assert!(config.metrics_path.exists());
    assert!(config.cycle_report_path.exists());
    let public_status: Value =
        serde_json::from_str(&fs::read_to_string(&config.status_path).expect("status payload"))
            .expect("status JSON");
    assert_eq!(
        public_status["operations"]["unallocated_deposits_usdc"],
        100.0
    );
    assert!(public_status.get("account").is_none());
}

#[test]
fn stale_account_observation_fails_before_runtime_state_changes() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let start = at(2026, 7, 6, 8, 0);
    let observed_at = at(2026, 7, 6, 10, 0);
    let runtime_config = config(directory.path(), ms(start));
    let mut runtime =
        SignerFreeRuntime::open(runtime_config.clone(), limits()).expect("open runtime");
    let error = runtime
        .apply_cycle(RuntimeCycleInput {
            observed_at,
            scan_start_ms: ms(start),
            scan_end_ms: ms(observed_at),
            movements: &[],
            approvals: &AdmissionApprovals::empty(),
            signal: None,
            accumulator: status(observed_at - TimeDelta::minutes(2), 0.0),
            capital_history_complete: true,
            manual_pause: true,
            api_errors: 0,
            decision_mode: DecisionMode::DryRun,
        })
        .expect_err("stale balance must fail closed");
    assert!(matches!(error, RuntimeError::InvalidCycle(message) if message.contains("stale")));
    assert!(runtime.state.last_complete_scan_end_ms.is_none());
    assert!(!runtime_config
        .state_directory
        .join(STATE_FILE_NAME)
        .exists());
}

#[test]
fn approved_deposit_plans_once_and_same_day_restart_replays_without_second_action() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let start = at(2026, 7, 6, 8, 0);
    let deposit_at = start + TimeDelta::hours(1);
    let decision_at = at(2026, 7, 6, 12, 0);
    let runtime_config = config(directory.path(), ms(start));
    let movement = deposit("deposit-approved", deposit_at, 100);
    let admission = approvals("deposit-approved", deposit_at, deposit_at);
    let signal = signal(decision_at);

    let mut runtime =
        SignerFreeRuntime::open(runtime_config.clone(), limits()).expect("open runtime");
    let first = runtime
        .apply_cycle(RuntimeCycleInput {
            observed_at: decision_at,
            scan_start_ms: ms(start),
            scan_end_ms: ms(decision_at),
            movements: std::slice::from_ref(&movement),
            approvals: &admission,
            signal: Some(&signal),
            accumulator: status(decision_at, 100.0),
            capital_history_complete: true,
            manual_pause: false,
            api_errors: 0,
            decision_mode: DecisionMode::DryRun,
        })
        .expect("first dry-run cycle");
    assert!(first.is_new_decision());
    assert_eq!(
        first.decision().expect("planned decision").reason,
        DecisionReason::Planned
    );
    assert!(!first
        .decision()
        .expect("planned decision")
        .planned_usdc
        .is_zero());
    assert!(first.decision().expect("planned decision").settled);
    assert_eq!(
        runtime.ledger.state().committed_usdc(),
        UsdcMicros::default()
    );
    assert_eq!(runtime.state.dry_run_actions_total, 1);
    drop(runtime);

    let replay_at = decision_at + TimeDelta::minutes(5);
    let mut reopened =
        SignerFreeRuntime::open(runtime_config.clone(), limits()).expect("reopen runtime");
    assert_eq!(reopened.next_scan_start_ms(), ms(start));
    let replay = reopened
        .apply_cycle(RuntimeCycleInput {
            observed_at: replay_at,
            scan_start_ms: ms(start),
            scan_end_ms: ms(replay_at),
            movements: std::slice::from_ref(&movement),
            approvals: &admission,
            signal: Some(&signal),
            accumulator: status(replay_at, 100.0),
            capital_history_complete: true,
            manual_pause: false,
            api_errors: 0,
            decision_mode: DecisionMode::DryRun,
        })
        .expect("same-day replay");
    assert!(!replay.is_new_decision());
    assert_eq!(reopened.state.dry_run_actions_total, 1);
    assert_eq!(reopened.state.pacing.decisions().len(), 1);

    let next_decision_at = at(2026, 7, 7, 12, 0);
    let next_signal = signal_for(next_decision_at, "2026-07-07");
    let next_scan_start_ms = reopened.next_scan_start_ms();
    let next = reopened
        .apply_cycle(RuntimeCycleInput {
            observed_at: next_decision_at,
            scan_start_ms: next_scan_start_ms,
            scan_end_ms: ms(next_decision_at),
            movements: &[movement],
            approvals: &admission,
            signal: Some(&next_signal),
            accumulator: status(next_decision_at, 100.0),
            capital_history_complete: true,
            manual_pause: false,
            api_errors: 0,
            decision_mode: DecisionMode::DryRun,
        })
        .expect("next-day dry-run plan");
    assert!(next.is_new_decision());
    assert_eq!(
        next.decision().expect("next-day decision").reason,
        DecisionReason::Planned
    );
    assert!(next.decision().expect("next-day decision").settled);
    assert_eq!(reopened.state.dry_run_actions_total, 2);
    assert_eq!(reopened.state.pacing.decisions().len(), 2);
    assert_eq!(
        reopened.ledger.state().committed_usdc(),
        UsdcMicros::default()
    );
}

#[test]
fn boundary_mismatched_signal_becomes_a_durable_unavailable_skip() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let start = at(2026, 7, 7, 8, 0);
    let deposit_at = at(2026, 7, 7, 9, 0);
    let decision_at = at(2026, 7, 7, 12, 0);
    let previous_signal = signal(at(2026, 7, 6, 12, 0));
    let runtime_config = config(directory.path(), ms(start));
    let movement = deposit("deposit-with-old-signal", deposit_at, 100);
    let admission = approvals("deposit-with-old-signal", deposit_at, deposit_at);
    let mut runtime =
        SignerFreeRuntime::open(runtime_config.clone(), limits()).expect("open runtime");

    let report = runtime
        .apply_cycle(RuntimeCycleInput {
            observed_at: decision_at,
            scan_start_ms: ms(start),
            scan_end_ms: ms(decision_at),
            movements: std::slice::from_ref(&movement),
            approvals: &admission,
            signal: Some(&previous_signal),
            accumulator: status(decision_at, 100.0),
            capital_history_complete: true,
            manual_pause: false,
            api_errors: 0,
            decision_mode: DecisionMode::DryRun,
        })
        .expect("mismatched signal fails closed without failing the cycle");

    assert_eq!(
        report.decision().expect("durable unavailable skip").reason,
        DecisionReason::CoreSignalUnavailable
    );
    assert!(!report.signal_available);
    assert_eq!(
        runtime.state.last_complete_scan_end_ms,
        Some(ms(decision_at))
    );
    assert_eq!(runtime.state.stale_signal_events_total, 1);
    assert_eq!(
        runtime
            .state
            .decision_evidence
            .get(&decision_at.date_naive()),
        Some(&RuntimeDecisionEvidence {
            signal_available: false,
            boundary_balance_available: true,
        })
    );
    drop(runtime);

    let replay_at = decision_at + TimeDelta::minutes(5);
    let valid_signal = signal(decision_at);
    let mut reopened = SignerFreeRuntime::open(runtime_config, limits()).expect("reopen runtime");
    let replay = reopened
        .apply_cycle(RuntimeCycleInput {
            observed_at: replay_at,
            scan_start_ms: reopened.next_scan_start_ms(),
            scan_end_ms: ms(replay_at),
            movements: std::slice::from_ref(&movement),
            approvals: &admission,
            signal: Some(&valid_signal),
            accumulator: status(replay_at, 100.0),
            capital_history_complete: true,
            manual_pause: false,
            api_errors: 0,
            decision_mode: DecisionMode::DryRun,
        })
        .expect("later valid signal cannot replace decision-time evidence");
    assert!(!replay.is_new_decision());
    assert_eq!(
        replay.decision().expect("existing unavailable skip").reason,
        DecisionReason::CoreSignalUnavailable
    );
    assert!(!replay.signal_available);
}

#[test]
fn existing_decision_preserves_missing_boundary_balance_evidence() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let start = at(2026, 7, 7, 8, 0);
    let decision_at = at(2026, 7, 7, 12, 0);
    let runtime_config = config(directory.path(), ms(start));
    let signal = signal(decision_at);
    let mut runtime =
        SignerFreeRuntime::open(runtime_config.clone(), limits()).expect("open runtime");

    let first = runtime
        .apply_cycle(RuntimeCycleInput {
            observed_at: decision_at,
            scan_start_ms: ms(start),
            scan_end_ms: ms(decision_at),
            movements: &[],
            approvals: &AdmissionApprovals::empty(),
            signal: Some(&signal),
            accumulator: status(decision_at, 0.0),
            capital_history_complete: false,
            manual_pause: false,
            api_errors: 1,
            decision_mode: DecisionMode::DryRun,
        })
        .expect("incomplete history records a durable skip");
    assert_eq!(
        first.decision().expect("missing history decision").reason,
        DecisionReason::MissingCapitalHistory
    );
    assert!(!first.boundary_balance_available);
    drop(runtime);

    let replay_at = decision_at + TimeDelta::minutes(5);
    let mut reopened = SignerFreeRuntime::open(runtime_config, limits()).expect("reopen runtime");
    let replay = reopened
        .apply_cycle(RuntimeCycleInput {
            observed_at: replay_at,
            scan_start_ms: reopened.next_scan_start_ms(),
            scan_end_ms: ms(replay_at),
            movements: &[],
            approvals: &AdmissionApprovals::empty(),
            signal: Some(&signal),
            accumulator: status(replay_at, 0.0),
            capital_history_complete: true,
            manual_pause: false,
            api_errors: 0,
            decision_mode: DecisionMode::DryRun,
        })
        .expect("complete retry keeps decision-time boundary evidence");
    assert!(!replay.is_new_decision());
    assert!(!replay.boundary_balance_available);
}

#[test]
fn later_approval_cannot_redistribute_journaled_admission() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let start = at(2026, 7, 7, 8, 0);
    let earlier_at = at(2026, 7, 7, 8, 30);
    let later_at = at(2026, 7, 7, 9, 0);
    let first_observed_at = at(2026, 7, 7, 10, 0);
    let second_observed_at = at(2026, 7, 7, 11, 0);
    let runtime_config = config(directory.path(), ms(start));
    let movements = vec![
        deposit("earlier-approved-later", earlier_at, 100),
        deposit("later-admitted-first", later_at, 100),
    ];
    let later_approval = approvals("later-admitted-first", later_at, later_at);
    let earlier_approval = approvals("earlier-approved-later", earlier_at, earlier_at);
    let mut capped_limits = limits();
    capped_limits.max_automatically_admitted_usdc = usd(100);
    capped_limits.yearly_admission_cap_usdc = usd(100);
    capped_limits.cumulative_admission_cap_usdc = usd(100);
    let mut runtime =
        SignerFreeRuntime::open(runtime_config, capped_limits).expect("open capped runtime");

    runtime
        .apply_cycle(RuntimeCycleInput {
            observed_at: first_observed_at,
            scan_start_ms: ms(start),
            scan_end_ms: ms(first_observed_at),
            movements: &movements,
            approvals: &later_approval,
            signal: None,
            accumulator: status(first_observed_at, 200.0),
            capital_history_complete: true,
            manual_pause: false,
            api_errors: 0,
            decision_mode: DecisionMode::DryRun,
        })
        .expect("later tranche consumes the append-only admission cap");
    assert_eq!(
        runtime
            .ledger
            .state()
            .admitted_deposit_usdc("later-admitted-first"),
        Some(usd(100))
    );

    runtime
        .apply_cycle(RuntimeCycleInput {
            observed_at: second_observed_at,
            scan_start_ms: runtime.next_scan_start_ms(),
            scan_end_ms: ms(second_observed_at),
            movements: &movements,
            approvals: &earlier_approval,
            signal: None,
            accumulator: status(second_observed_at, 200.0),
            capital_history_complete: true,
            manual_pause: false,
            api_errors: 0,
            decision_mode: DecisionMode::DryRun,
        })
        .expect("older approval preserves the journaled admission allocation");
    assert_eq!(
        runtime.state.pacing.deposits()["earlier-approved-later"].admitted_usdc,
        UsdcMicros::default()
    );
    assert_eq!(
        runtime.state.pacing.deposits()["later-admitted-first"].admitted_usdc,
        usd(100)
    );
    assert_eq!(
        runtime.state.last_complete_scan_end_ms,
        Some(ms(second_observed_at))
    );
}

#[test]
fn newly_admitted_deposit_is_committed_before_a_dependent_withdrawal() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let start = at(2026, 7, 6, 8, 0);
    let deposit_at = at(2026, 7, 6, 9, 0);
    let withdrawal_at = at(2026, 7, 6, 9, 5);
    let observed_at = at(2026, 7, 6, 10, 0);
    let runtime_config = config(directory.path(), ms(start));
    let movements = vec![
        deposit("deposit-with-withdrawal", deposit_at, 100),
        withdrawal("withdrawal-after-deposit", withdrawal_at, 40),
    ];
    let admission = approvals("deposit-with-withdrawal", deposit_at, deposit_at);

    let mut runtime =
        SignerFreeRuntime::open(runtime_config.clone(), limits()).expect("open runtime");
    let report = runtime
        .apply_cycle(RuntimeCycleInput {
            observed_at,
            scan_start_ms: ms(start),
            scan_end_ms: ms(observed_at),
            movements: &movements,
            approvals: &admission,
            signal: None,
            accumulator: status(observed_at, 60.0),
            capital_history_complete: true,
            manual_pause: false,
            api_errors: 0,
            decision_mode: DecisionMode::DryRun,
        })
        .expect("deposit and dependent withdrawal reconcile in one cycle");

    assert!(report.decision().is_none());
    assert_eq!(
        runtime
            .ledger
            .state()
            .admitted_deposit_usdc("deposit-with-withdrawal"),
        Some(usd(100))
    );
    assert_eq!(runtime.ledger.state().withdrawn_usdc(), usd(40));
    assert_eq!(runtime.ledger.state().deployable_usdc(), usd(60));
    assert!(!runtime_config
        .state_directory
        .join(PENDING_CYCLE_FILE_NAME)
        .exists());
    drop(runtime);

    let replay_at = observed_at + TimeDelta::minutes(5);
    let mut reopened =
        SignerFreeRuntime::open(runtime_config, limits()).expect("reopen committed cycle");
    let replay = reopened
        .apply_cycle(RuntimeCycleInput {
            observed_at: replay_at,
            scan_start_ms: reopened.next_scan_start_ms(),
            scan_end_ms: ms(replay_at),
            movements: &movements,
            approvals: &admission,
            signal: None,
            accumulator: status(replay_at, 60.0),
            capital_history_complete: true,
            manual_pause: false,
            api_errors: 0,
            decision_mode: DecisionMode::DryRun,
        })
        .expect("overlap scan reuses the durable withdrawal reconciliation time");
    assert!(replay.decision().is_none());
    assert_eq!(reopened.ledger.state().withdrawn_usdc(), usd(40));
}

#[test]
fn delayed_cycle_preserves_a_preboundary_withdrawal_identity() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let start = at(2026, 7, 6, 8, 0);
    let deposit_at = at(2026, 7, 6, 9, 0);
    let withdrawal_at = at(2026, 7, 6, 10, 0);
    let boundary = at(2026, 7, 6, 12, 0);
    let observed_at = boundary + TimeDelta::minutes(5);
    let runtime_config = config(directory.path(), ms(start));
    let movements = vec![
        deposit("deposit-before-withdrawal", deposit_at, 100),
        withdrawal("withdrawal-before-boundary", withdrawal_at, 40),
    ];
    let admission = approvals("deposit-before-withdrawal", deposit_at, deposit_at);
    let signal = signal(boundary);
    let mut runtime =
        SignerFreeRuntime::open(runtime_config.clone(), limits()).expect("open runtime");

    let report = runtime
        .apply_cycle(RuntimeCycleInput {
            observed_at,
            scan_start_ms: ms(start),
            scan_end_ms: ms(observed_at),
            movements: &movements,
            approvals: &admission,
            signal: Some(&signal),
            accumulator: status(observed_at, 60.0),
            capital_history_complete: true,
            manual_pause: false,
            api_errors: 0,
            decision_mode: DecisionMode::DryRun,
        })
        .expect("pre-boundary withdrawal remains identical across both reconciliations");

    let decision = report.decision().expect("durable boundary decision");
    assert_eq!(decision.reason, DecisionReason::Planned);
    assert_eq!(
        decision.explanation.observed_budget_after_reserve_usdc,
        usd(60)
    );
    assert_eq!(runtime.ledger.state().withdrawn_usdc(), usd(40));
    assert_eq!(
        runtime
            .state
            .pacing
            .withdrawals()
            .get("withdrawal-before-boundary")
            .expect("durable withdrawal")
            .event
            .reconciled_at,
        boundary
    );
    drop(runtime);

    let replay_at = observed_at + TimeDelta::minutes(5);
    let mut reopened =
        SignerFreeRuntime::open(runtime_config, limits()).expect("reopen committed cycle");
    let replay = reopened
        .apply_cycle(RuntimeCycleInput {
            observed_at: replay_at,
            scan_start_ms: reopened.next_scan_start_ms(),
            scan_end_ms: ms(replay_at),
            movements: &movements,
            approvals: &admission,
            signal: None,
            accumulator: status(replay_at, 60.0),
            capital_history_complete: true,
            manual_pause: false,
            api_errors: 0,
            decision_mode: DecisionMode::DryRun,
        })
        .expect("overlap replay keeps the boundary reconciliation timestamp");
    assert!(!replay.is_new_decision());
    assert!(replay.signal_available);
    assert_eq!(reopened.ledger.state().withdrawn_usdc(), usd(40));
}

#[test]
fn delayed_cycle_reconstructs_boundary_balance_before_a_later_withdrawal() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let start = at(2026, 7, 6, 8, 0);
    let deposit_at = at(2026, 7, 6, 9, 0);
    let boundary = at(2026, 7, 6, 12, 0);
    let withdrawal_at = boundary + TimeDelta::minutes(3);
    let observed_at = boundary + TimeDelta::minutes(5);
    let runtime_config = config(directory.path(), ms(start));
    let movements = vec![
        deposit("deposit-before-boundary", deposit_at, 100),
        withdrawal("withdrawal-after-boundary", withdrawal_at, 40),
    ];
    let admission = approvals("deposit-before-boundary", deposit_at, deposit_at);
    let signal = signal(boundary);
    let mut runtime = SignerFreeRuntime::open(runtime_config, limits()).expect("open runtime");

    let report = runtime
        .apply_cycle(RuntimeCycleInput {
            observed_at,
            scan_start_ms: ms(start),
            scan_end_ms: ms(observed_at),
            movements: &movements,
            approvals: &admission,
            signal: Some(&signal),
            accumulator: status(observed_at, 60.0),
            capital_history_complete: true,
            manual_pause: false,
            api_errors: 0,
            decision_mode: DecisionMode::DryRun,
        })
        .expect("delayed dry-run cycle");

    let decision = report.decision().expect("durable boundary decision");
    assert_eq!(decision.decided_at, boundary);
    assert_eq!(decision.reason, DecisionReason::Planned);
    assert_eq!(
        decision.explanation.observed_budget_after_reserve_usdc,
        usd(100)
    );
    assert!(report.boundary_balance_available);
    assert_eq!(runtime.ledger.state().withdrawn_usdc(), usd(40));
}

#[test]
fn delayed_decision_removes_a_later_deposit_from_boundary_balance() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let start = at(2026, 7, 6, 8, 0);
    let boundary = at(2026, 7, 6, 12, 0);
    let deposit_at = boundary + TimeDelta::minutes(3);
    let observed_at = boundary + TimeDelta::minutes(5);
    let runtime_config = config(directory.path(), ms(start));
    let movement = deposit("deposit-after-boundary", deposit_at, 100);
    let admission = approvals("deposit-after-boundary", deposit_at, deposit_at);
    let signal = signal(boundary);
    let mut runtime = SignerFreeRuntime::open(runtime_config, limits()).expect("open runtime");

    let report = runtime
        .apply_cycle(RuntimeCycleInput {
            observed_at,
            scan_start_ms: ms(start),
            scan_end_ms: ms(observed_at),
            movements: &[movement],
            approvals: &admission,
            signal: Some(&signal),
            accumulator: status(observed_at, 100.0),
            capital_history_complete: true,
            manual_pause: false,
            api_errors: 0,
            decision_mode: DecisionMode::DryRun,
        })
        .expect("delayed dry-run cycle");

    let decision = report.decision().expect("durable boundary decision");
    assert_eq!(decision.decided_at, boundary);
    assert_eq!(decision.reason, DecisionReason::NoAdmittedCapital);
    assert!(decision.planned_usdc.is_zero());
    assert_eq!(
        decision.explanation.observed_budget_after_reserve_usdc,
        UsdcMicros::default()
    );
    assert!(report.boundary_balance_available);
    assert_eq!(
        runtime
            .state
            .pacing
            .deposits()
            .get("deposit-after-boundary")
            .expect("post-boundary deposit")
            .admitted_usdc,
        usd(100)
    );
}

#[test]
fn movement_during_balance_request_window_makes_boundary_balance_unavailable() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let start = at(2026, 7, 6, 8, 0);
    let boundary = at(2026, 7, 6, 12, 0);
    let request_started_at = boundary + TimeDelta::minutes(3);
    let deposit_at = boundary + TimeDelta::minutes(4);
    let observed_at = boundary + TimeDelta::minutes(5);
    let runtime_config = config(directory.path(), ms(start));
    let movement = deposit("deposit-during-balance-request", deposit_at, 100);
    let admission = approvals("deposit-during-balance-request", deposit_at, deposit_at);
    let signal = signal(boundary);
    let mut runtime = SignerFreeRuntime::open(runtime_config, limits()).expect("open runtime");

    let report = runtime
        .apply_cycle(RuntimeCycleInput {
            observed_at,
            scan_start_ms: ms(start),
            scan_end_ms: ms(observed_at),
            movements: &[movement],
            approvals: &admission,
            signal: Some(&signal),
            accumulator: status_window(request_started_at, observed_at, 100.0),
            capital_history_complete: true,
            manual_pause: false,
            api_errors: 0,
            decision_mode: DecisionMode::DryRun,
        })
        .expect("ambiguous balance window records a fail-closed skip");

    assert_eq!(
        report.decision().expect("durable boundary decision").reason,
        DecisionReason::MissingCapitalHistory
    );
    assert!(!report.boundary_balance_available);
}

#[cfg(unix)]
#[test]
fn private_runtime_artifacts_are_owner_only() {
    use std::os::unix::fs::PermissionsExt;

    let directory = tempfile::tempdir().expect("temporary directory");
    let start = at(2026, 7, 6, 8, 0);
    let observed_at = at(2026, 7, 6, 10, 0);
    let runtime_config = config(directory.path(), ms(start));
    let mut runtime =
        SignerFreeRuntime::open(runtime_config.clone(), limits()).expect("open runtime");
    runtime
        .apply_cycle(RuntimeCycleInput {
            observed_at,
            scan_start_ms: ms(start),
            scan_end_ms: ms(observed_at),
            movements: &[],
            approvals: &AdmissionApprovals::empty(),
            signal: None,
            accumulator: status(observed_at, 0.0),
            capital_history_complete: true,
            manual_pause: true,
            api_errors: 0,
            decision_mode: DecisionMode::DryRun,
        })
        .expect("dry-run cycle");

    for path in [
        runtime_config.state_directory.join(STATE_FILE_NAME),
        runtime_config
            .state_directory
            .join(COMMITTED_CYCLE_PROOF_FILE_NAME),
        runtime_config.protected_anchor_path,
        runtime_config.cycle_report_path,
    ] {
        let mode = fs::metadata(path)
            .expect("private artifact")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
    }
}

#[test]
fn unknown_approval_fails_closed_without_creating_capital() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let start = at(2026, 7, 6, 8, 0);
    let observed_at = at(2026, 7, 6, 10, 0);
    let mut runtime = SignerFreeRuntime::open(config(directory.path(), ms(start)), limits())
        .expect("open runtime");
    let unknown = approvals("not-observed", start, start);
    let error = runtime
        .apply_cycle(RuntimeCycleInput {
            observed_at,
            scan_start_ms: ms(start),
            scan_end_ms: ms(observed_at),
            movements: &[],
            approvals: &unknown,
            signal: None,
            accumulator: status(observed_at, 0.0),
            capital_history_complete: true,
            manual_pause: true,
            api_errors: 0,
            decision_mode: DecisionMode::DryRun,
        })
        .expect_err("unknown approval must fail");
    assert!(matches!(error, RuntimeError::UnknownAdmissionApproval(_)));
    assert!(runtime.state.pacing.deposits().is_empty());
}

#[test]
fn future_approval_is_rejected_before_persistence_and_can_be_corrected() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let start = at(2026, 7, 6, 8, 0);
    let deposit_at = at(2026, 7, 6, 9, 0);
    let observed_at = at(2026, 7, 6, 10, 0);
    let future_at = at(2026, 7, 7, 9, 0);
    let runtime_config = config(directory.path(), ms(start));
    let movement = deposit("deposit-future-approval", deposit_at, 100);
    let future_approval = approvals("deposit-future-approval", future_at, future_at);
    let mut runtime =
        SignerFreeRuntime::open(runtime_config.clone(), limits()).expect("open runtime");

    let error = runtime
        .apply_cycle(RuntimeCycleInput {
            observed_at,
            scan_start_ms: ms(start),
            scan_end_ms: ms(observed_at),
            movements: std::slice::from_ref(&movement),
            approvals: &future_approval,
            signal: None,
            accumulator: status(observed_at, 100.0),
            capital_history_complete: true,
            manual_pause: false,
            api_errors: 0,
            decision_mode: DecisionMode::DryRun,
        })
        .expect_err("future approval evidence fails before persistence");
    assert!(
        matches!(error, RuntimeError::InvalidAdmissionArtifact(message) if message.contains("future"))
    );
    assert!(runtime.state.pacing.deposits().is_empty());
    assert!(!runtime_config
        .state_directory
        .join(STATE_FILE_NAME)
        .exists());

    let corrected_approval = approvals("deposit-future-approval", deposit_at, deposit_at);
    runtime
        .apply_cycle(RuntimeCycleInput {
            observed_at,
            scan_start_ms: ms(start),
            scan_end_ms: ms(observed_at),
            movements: std::slice::from_ref(&movement),
            approvals: &corrected_approval,
            signal: None,
            accumulator: status(observed_at, 100.0),
            capital_history_complete: true,
            manual_pause: false,
            api_errors: 0,
            decision_mode: DecisionMode::DryRun,
        })
        .expect("corrected approval recovers without manual state repair");
    assert_eq!(
        runtime
            .ledger
            .state()
            .admitted_deposit_usdc("deposit-future-approval"),
        Some(usd(100))
    );
}

#[test]
fn unknown_approval_is_deferred_only_while_history_is_incomplete() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let start = at(2026, 7, 6, 8, 0);
    let deposit_at = at(2026, 7, 6, 9, 0);
    let decision_at = at(2026, 7, 6, 12, 0);
    let runtime_config = config(directory.path(), ms(start));
    let movement = deposit("deposit-hidden-by-outage", deposit_at, 100);
    let admission = approvals("deposit-hidden-by-outage", deposit_at, deposit_at);
    let signal = signal(decision_at);
    let mut runtime =
        SignerFreeRuntime::open(runtime_config.clone(), limits()).expect("open runtime");

    let outage = runtime
        .apply_cycle(RuntimeCycleInput {
            observed_at: decision_at,
            scan_start_ms: ms(start),
            scan_end_ms: ms(decision_at),
            movements: &[],
            approvals: &admission,
            signal: Some(&signal),
            accumulator: status(decision_at, 0.0),
            capital_history_complete: false,
            manual_pause: false,
            api_errors: 1,
            decision_mode: DecisionMode::DryRun,
        })
        .expect("unknown approval is deferred during history outage");
    assert_eq!(
        outage.decision().expect("durable outage decision").reason,
        DecisionReason::MissingCapitalHistory
    );
    assert_eq!(runtime.state.api_errors_total, 1);
    assert!(runtime.state.last_complete_scan_end_ms.is_none());
    drop(runtime);

    let replay_at = decision_at + TimeDelta::minutes(5);
    let mut reopened = SignerFreeRuntime::open(runtime_config, limits()).expect("reopen runtime");
    reopened
        .apply_cycle(RuntimeCycleInput {
            observed_at: replay_at,
            scan_start_ms: reopened.next_scan_start_ms(),
            scan_end_ms: ms(replay_at),
            movements: &[movement],
            approvals: &admission,
            signal: Some(&signal),
            accumulator: status(replay_at, 100.0),
            capital_history_complete: true,
            manual_pause: false,
            api_errors: 0,
            decision_mode: DecisionMode::DryRun,
        })
        .expect("complete history validates and applies the deferred approval");
    assert_eq!(
        reopened
            .ledger
            .state()
            .admitted_deposit_usdc("deposit-hidden-by-outage"),
        Some(usd(100))
    );
    assert_eq!(
        reopened.state.last_complete_scan_end_ms,
        Some(ms(replay_at))
    );
}

#[test]
fn prepared_partial_cycle_resumes_from_authenticated_pending_payload() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let start = at(2026, 7, 6, 8, 0);
    let observed_at = at(2026, 7, 6, 10, 0);
    let runtime_config = config(directory.path(), ms(start));
    let mut runtime =
        SignerFreeRuntime::open(runtime_config.clone(), limits()).expect("open runtime");
    let balance_event = LedgerEvent {
        event_id: "balance:partial-cycle".to_owned(),
        occurred_at: observed_at,
        kind: LedgerEventKind::BalanceObserved {
            observed_usdc: usd(10),
            observed_hype_atoms: 0,
        },
    };
    let pending = PendingRuntimeCycle::new(
        observed_at,
        runtime.state.clone(),
        vec![balance_event.clone()],
    )
    .expect("pending cycle");
    write_private_json_atomic(
        runtime_config.state_directory.join(PENDING_CYCLE_FILE_NAME),
        &pending,
    )
    .expect("pending payload");
    runtime
        .ledger
        .append(LedgerEvent {
            event_id: format!("runtime-cycle:{}:prepared", pending.cycle_hash),
            occurred_at: observed_at,
            kind: LedgerEventKind::RuntimeCyclePrepared {
                cycle_hash: pending.cycle_hash.clone(),
            },
        })
        .expect("prepared anchor");
    runtime
        .ledger
        .append(balance_event)
        .expect("partial economic append");
    let cycle_hash = pending.cycle_hash.clone();
    drop(runtime);

    let recovered =
        SignerFreeRuntime::open(runtime_config.clone(), limits()).expect("recover runtime");
    assert!(recovered
        .ledger
        .state()
        .runtime_cycle_committed(&cycle_hash));
    assert_eq!(recovered.ledger.state().observed_usdc(), usd(10));
    assert_eq!(
        recovered.state.last_committed_cycle_hash.as_deref(),
        Some(cycle_hash.as_str())
    );
    assert!(!runtime_config
        .state_directory
        .join(PENDING_CYCLE_FILE_NAME)
        .exists());
}

#[test]
fn tampered_pending_after_prepare_fails_closed() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let start = at(2026, 7, 6, 8, 0);
    let observed_at = at(2026, 7, 6, 10, 0);
    let runtime_config = config(directory.path(), ms(start));
    let mut runtime =
        SignerFreeRuntime::open(runtime_config.clone(), limits()).expect("open runtime");
    let mut pending = PendingRuntimeCycle::new(observed_at, runtime.state.clone(), Vec::new())
        .expect("pending cycle");
    runtime
        .ledger
        .append(LedgerEvent {
            event_id: format!("runtime-cycle:{}:prepared", pending.cycle_hash),
            occurred_at: observed_at,
            kind: LedgerEventKind::RuntimeCyclePrepared {
                cycle_hash: pending.cycle_hash.clone(),
            },
        })
        .expect("prepared anchor");
    pending.body.state.api_errors_total = 1;
    write_private_json_atomic(
        runtime_config.state_directory.join(PENDING_CYCLE_FILE_NAME),
        &pending,
    )
    .expect("tampered pending payload");
    drop(runtime);

    assert!(matches!(
        SignerFreeRuntime::open(runtime_config, limits()),
        Err(RuntimeError::CorruptPendingCycle)
    ));
}

#[test]
fn committed_runtime_state_rollback_is_rejected_by_protected_cycle_head() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let start = at(2026, 7, 6, 8, 0);
    let observed_at = at(2026, 7, 6, 10, 0);
    let runtime_config = config(directory.path(), ms(start));
    let mut runtime =
        SignerFreeRuntime::open(runtime_config.clone(), limits()).expect("open runtime");
    runtime
        .apply_cycle(RuntimeCycleInput {
            observed_at,
            scan_start_ms: ms(start),
            scan_end_ms: ms(observed_at),
            movements: &[],
            approvals: &AdmissionApprovals::empty(),
            signal: None,
            accumulator: status(observed_at, 0.0),
            capital_history_complete: true,
            manual_pause: true,
            api_errors: 0,
            decision_mode: DecisionMode::DryRun,
        })
        .expect("committed cycle");
    let mut rolled_back = runtime.state.clone();
    rolled_back.last_committed_cycle_hash = None;
    write_private_json_atomic(
        runtime_config.state_directory.join(STATE_FILE_NAME),
        &rolled_back,
    )
    .expect("rolled-back runtime state");
    drop(runtime);

    assert!(matches!(
        SignerFreeRuntime::open(runtime_config, limits()),
        Err(RuntimeError::RuntimeStateRollback)
    ));
}

#[test]
fn committed_runtime_state_content_tampering_is_rejected_by_cycle_proof() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let start = at(2026, 7, 6, 8, 0);
    let observed_at = at(2026, 7, 6, 10, 0);
    let runtime_config = config(directory.path(), ms(start));
    let mut runtime =
        SignerFreeRuntime::open(runtime_config.clone(), limits()).expect("open runtime");
    runtime
        .apply_cycle(RuntimeCycleInput {
            observed_at,
            scan_start_ms: ms(start),
            scan_end_ms: ms(observed_at),
            movements: &[],
            approvals: &AdmissionApprovals::empty(),
            signal: None,
            accumulator: status(observed_at, 0.0),
            capital_history_complete: true,
            manual_pause: true,
            api_errors: 0,
            decision_mode: DecisionMode::DryRun,
        })
        .expect("committed cycle");
    let mut tampered = runtime.state.clone();
    tampered.api_errors_total = 1;
    write_private_json_atomic(
        runtime_config.state_directory.join(STATE_FILE_NAME),
        &tampered,
    )
    .expect("tampered runtime state");
    drop(runtime);

    assert!(matches!(
        SignerFreeRuntime::open(runtime_config, limits()),
        Err(RuntimeError::RuntimeStateRollback)
    ));
}

#[test]
fn missing_committed_cycle_proof_fails_closed() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let start = at(2026, 7, 6, 8, 0);
    let observed_at = at(2026, 7, 6, 10, 0);
    let runtime_config = config(directory.path(), ms(start));
    let mut runtime =
        SignerFreeRuntime::open(runtime_config.clone(), limits()).expect("open runtime");
    runtime
        .apply_cycle(RuntimeCycleInput {
            observed_at,
            scan_start_ms: ms(start),
            scan_end_ms: ms(observed_at),
            movements: &[],
            approvals: &AdmissionApprovals::empty(),
            signal: None,
            accumulator: status(observed_at, 0.0),
            capital_history_complete: true,
            manual_pause: true,
            api_errors: 0,
            decision_mode: DecisionMode::DryRun,
        })
        .expect("committed cycle");
    fs::remove_file(
        runtime_config
            .state_directory
            .join(COMMITTED_CYCLE_PROOF_FILE_NAME),
    )
    .expect("remove committed proof");
    drop(runtime);

    assert!(matches!(
        SignerFreeRuntime::open(runtime_config, limits()),
        Err(RuntimeError::MissingCommittedCycleProof)
    ));
}

#[test]
fn f64_usdc_micros_floors_sub_microunit_precision_from_the_live_venue() {
    // Observed live on Hyperliquid's spotClearinghouseState for a real
    // account (2026-09-04): the venue's own USDC balance precision exceeds
    // USDC's 6-decimal on-chain precision. This must floor, not reject.
    assert_eq!(
        f64_usdc_micros(24_098.690_000_62).expect("floors instead of rejecting"),
        UsdcMicros::from_micros(24_098_690_000)
    );
}

#[test]
fn f64_usdc_micros_floors_never_rounds_up() {
    assert_eq!(
        f64_usdc_micros(1.999_999_9).expect("floors down"),
        UsdcMicros::from_micros(1_999_999)
    );
}

#[test]
fn f64_usdc_micros_rejects_non_finite_or_negative() {
    assert!(f64_usdc_micros(f64::NAN).is_err());
    assert!(f64_usdc_micros(f64::INFINITY).is_err());
    assert!(f64_usdc_micros(-0.01).is_err());
}

#[test]
fn f64_usdc_micros_rejects_overflow() {
    assert!(f64_usdc_micros(f64::MAX).is_err());
}

const FUNDING_CHILD: &str = "0x1111111111111111111111111111111111111111";
const FUNDING_PARENT: &str = "0x2222222222222222222222222222222222222222";

fn parent_route() -> ParentFundingRoute {
    ParentFundingRoute::new(FUNDING_CHILD, FUNDING_PARENT).expect("funding route")
}

fn parent_transfer(id: &str, timestamp: DateTime<Utc>, amount: u64) -> HyperliquidAccountMovement {
    let mut value = deposit(id, timestamp, amount);
    value.kind = HyperliquidAccountMovementKind::InternalTransfer;
    value.counterparty = Some(FUNDING_PARENT.to_owned());
    value
}

fn funding_cycle(
    runtime: &mut SignerFreeRuntime,
    now: DateTime<Utc>,
    movements: &[HyperliquidAccountMovement],
    admissions: &AdmissionApprovals,
    balance: f64,
) -> Result<RuntimeCycleReport, RuntimeError> {
    runtime.apply_cycle(RuntimeCycleInput {
        observed_at: now,
        scan_start_ms: runtime.next_scan_start_ms(),
        scan_end_ms: ms(now),
        movements,
        approvals: admissions,
        signal: None,
        accumulator: status(now, balance),
        capital_history_complete: true,
        manual_pause: true,
        api_errors: 0,
        decision_mode: DecisionMode::DryRun,
    })
}

#[test]
fn parent_funding_is_visible_but_not_admitted_without_confirmation_and_approval() {
    let directory = tempfile::tempdir().unwrap();
    let start = at(2026, 7, 6, 8, 0);
    let now = at(2026, 7, 6, 12, 0);
    let cfg = config(directory.path(), ms(start)).with_parent_funding_route(Some(parent_route()));
    let mut runtime = SignerFreeRuntime::open(cfg, limits()).unwrap();
    let movement = parent_transfer("parent-funds", start + TimeDelta::hours(1), 100);
    let report = funding_cycle(
        &mut runtime,
        now,
        &[movement],
        &AdmissionApprovals::default(),
        100.0,
    )
    .unwrap();
    assert!(report.capital_history_complete);
    assert_eq!(
        runtime.ledger.state().authoritative_deposits_usdc(),
        Some(usd(100))
    );
    assert_eq!(runtime.ledger.state().admitted_usdc(), usd(0));
    let journal = fs::read_to_string(directory.path().join("state/ledger/ledger.jsonl")).unwrap();
    assert!(journal.contains("authoritative_parent_funding"));
    assert!(!journal.contains("authoritative_deposit"));
}

#[test]
fn parent_funding_replay_and_restart_do_not_double_admit_and_bind_route() {
    let directory = tempfile::tempdir().unwrap();
    let start = at(2026, 7, 6, 8, 0);
    let now = at(2026, 7, 6, 12, 0);
    let cfg = config(directory.path(), ms(start)).with_parent_funding_route(Some(parent_route()));
    let movement = parent_transfer("parent-funds", start + TimeDelta::hours(1), 100);
    let admission = approvals(
        "parent-funds",
        start + TimeDelta::hours(1),
        start + TimeDelta::hours(1),
    );
    let mut runtime = SignerFreeRuntime::open(cfg.clone(), limits()).unwrap();
    funding_cycle(
        &mut runtime,
        now,
        &[movement.clone(), movement.clone()],
        &admission,
        100.0,
    )
    .unwrap();
    assert_eq!(runtime.ledger.state().admitted_usdc(), usd(100));
    drop(runtime);
    let mut runtime = SignerFreeRuntime::open(cfg.clone(), limits()).unwrap();
    funding_cycle(
        &mut runtime,
        now + TimeDelta::minutes(5),
        &[movement],
        &admission,
        100.0,
    )
    .unwrap();
    assert_eq!(runtime.ledger.state().admitted_usdc(), usd(100));
    assert_eq!(runtime.state.pacing.deposits().len(), 1);
    drop(runtime);
    assert!(SignerFreeRuntime::open(cfg.with_parent_funding_route(None), limits()).is_err());
    let changed =
        ParentFundingRoute::new(FUNDING_CHILD, "0x3333333333333333333333333333333333333333")
            .unwrap();
    assert!(SignerFreeRuntime::open(
        config(directory.path(), ms(start)).with_parent_funding_route(Some(changed)),
        limits()
    )
    .is_err());
}

#[test]
fn parent_funding_wrong_or_missing_sender_and_self_transfer_fail_closed() {
    for sender in [
        None,
        Some(FUNDING_CHILD),
        Some("0x3333333333333333333333333333333333333333"),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let start = at(2026, 7, 6, 8, 0);
        let cfg =
            config(directory.path(), ms(start)).with_parent_funding_route(Some(parent_route()));
        let mut runtime = SignerFreeRuntime::open(cfg, limits()).unwrap();
        let mut movement = parent_transfer("untrusted", start + TimeDelta::hours(1), 100);
        movement.counterparty = sender.map(str::to_owned);
        let report = funding_cycle(
            &mut runtime,
            at(2026, 7, 6, 12, 0),
            &[movement],
            &AdmissionApprovals::default(),
            100.0,
        )
        .unwrap();
        assert!(!report.capital_history_complete);
        assert!(runtime.state.pacing.deposits().is_empty());
        assert_eq!(runtime.ledger.state().admitted_usdc(), usd(0));
    }
}

#[test]
fn parent_funding_withdrawal_reduces_capital_and_redeposit_does_not_reset_year_cap() {
    let directory = tempfile::tempdir().unwrap();
    let start = at(2026, 7, 6, 8, 0);
    let now = at(2026, 7, 6, 12, 0);
    let cfg = config(directory.path(), ms(start)).with_parent_funding_route(Some(parent_route()));
    let mut cap = limits();
    cap.max_automatically_admitted_usdc = usd(100_000);
    cap.yearly_admission_cap_usdc = usd(100_000);
    cap.cumulative_admission_cap_usdc = usd(100_000);
    cap.max_daily_notional_usdc = usd(300);
    let mut runtime = SignerFreeRuntime::open(cfg, cap).unwrap();
    let received = start + TimeDelta::hours(1);
    let first = parent_transfer("first", received, 100_000);
    let admission = approvals("first", received, received);
    funding_cycle(
        &mut runtime,
        now,
        std::slice::from_ref(&first),
        &admission,
        100_000.0,
    )
    .unwrap();
    let mut outgoing = parent_transfer("return", now + TimeDelta::minutes(1), 10_000);
    outgoing.amount = -outgoing.amount;
    funding_cycle(
        &mut runtime,
        now + TimeDelta::minutes(2),
        &[first, outgoing],
        &admission,
        90_000.0,
    )
    .unwrap();
    assert_eq!(runtime.ledger.state().deployable_usdc(), usd(90_000));
    let received = now + TimeDelta::minutes(3);
    let again = parent_transfer("again", received, 10_000);
    let admission = approvals("again", received, received);
    funding_cycle(
        &mut runtime,
        now + TimeDelta::minutes(5),
        &[again],
        &admission,
        100_000.0,
    )
    .unwrap();
    assert_eq!(runtime.ledger.state().admitted_usdc(), usd(100_000));
    assert_eq!(runtime.ledger.state().deployable_usdc(), usd(90_000));
    assert_eq!(
        runtime.state.pacing.deposits()["again"].admitted_usdc,
        usd(0)
    );
}

#[test]
fn parent_funding_respects_300_daily_cap_and_late_funding_cannot_create_second_purchase() {
    let directory = tempfile::tempdir().unwrap();
    let start = at(2026, 7, 6, 8, 0);
    let now = at(2026, 7, 6, 12, 0);
    let cfg = config(directory.path(), ms(start)).with_parent_funding_route(Some(parent_route()));
    let mut cap = limits();
    cap.max_automatically_admitted_usdc = usd(100_000);
    cap.yearly_admission_cap_usdc = usd(100_000);
    cap.cumulative_admission_cap_usdc = usd(100_000);
    cap.max_daily_notional_usdc = usd(300);
    let mut runtime = SignerFreeRuntime::open(cfg, cap).unwrap();
    let received = start + TimeDelta::hours(1);
    let movement = parent_transfer("initial-capital", received, 90_000);
    let admission = approvals("initial-capital", received, received);
    let signal = signal(now);
    let first = runtime
        .apply_cycle(RuntimeCycleInput {
            observed_at: now,
            scan_start_ms: ms(start),
            scan_end_ms: ms(now),
            movements: std::slice::from_ref(&movement),
            approvals: &admission,
            signal: Some(&signal),
            accumulator: status(now, 90_000.0),
            capital_history_complete: true,
            manual_pause: false,
            api_errors: 0,
            decision_mode: DecisionMode::DryRun,
        })
        .unwrap();
    assert_eq!(first.decision().unwrap().planned_usdc, usd(300));
    let received = now + TimeDelta::minutes(1);
    let late = parent_transfer("late-capital", received, 10_000);
    let admission = approvals("late-capital", received, received);
    let next = funding_cycle(
        &mut runtime,
        now + TimeDelta::minutes(5),
        &[movement, late],
        &admission,
        100_000.0,
    )
    .unwrap();
    assert!(!next.is_new_decision());
    assert_eq!(next.decision().unwrap(), first.decision().unwrap());
    assert_eq!(runtime.ledger.state().admitted_usdc(), usd(100_000));
    assert_eq!(runtime.state.dry_run_actions_total, 1);
}

#[test]
fn parent_funding_malformed_withdrawal_is_rejected_before_any_pending_commit() {
    let directory = tempfile::tempdir().unwrap();
    let start = at(2026, 7, 6, 8, 0);
    let cfg = config(directory.path(), ms(start)).with_parent_funding_route(Some(parent_route()));
    let mut runtime = SignerFreeRuntime::open(cfg, limits()).unwrap();
    for sender in [None, Some("not-an-account"), Some(FUNDING_CHILD)] {
        let mut movement = parent_transfer("bad-withdrawal", start + TimeDelta::hours(1), 10);
        movement.amount = -movement.amount;
        movement.counterparty = sender.map(str::to_owned);
        assert!(funding_cycle(
            &mut runtime,
            at(2026, 7, 6, 12, 0),
            &[movement],
            &AdmissionApprovals::default(),
            0.0
        )
        .is_err());
        assert!(!runtime
            .config
            .state_directory
            .join(PENDING_CYCLE_FILE_NAME)
            .exists());
        assert!(runtime.ledger.state().last_event_at().is_none());
    }
}

#[test]
fn parent_funding_return_before_approval_cannot_be_admitted_later() {
    for returned in [40, 100] {
        let directory = tempfile::tempdir().unwrap();
        let start = at(2026, 7, 6, 8, 0);
        let now = at(2026, 7, 6, 12, 0);
        let cfg =
            config(directory.path(), ms(start)).with_parent_funding_route(Some(parent_route()));
        let mut runtime = SignerFreeRuntime::open(cfg.clone(), limits()).unwrap();
        let funding_at = start + TimeDelta::hours(1);
        let incoming = parent_transfer("not-yet-approved", funding_at, 100);
        let mut outgoing = parent_transfer(
            "returned-before-approval",
            funding_at + TimeDelta::minutes(30),
            returned,
        );
        outgoing.amount = -outgoing.amount;
        let balance = f64::from(u32::try_from(100 - returned).unwrap());
        funding_cycle(
            &mut runtime,
            now,
            &[incoming.clone(), outgoing.clone()],
            &AdmissionApprovals::default(),
            balance,
        )
        .unwrap();
        assert_eq!(runtime.ledger.state().admitted_usdc(), usd(0));
        assert_eq!(
            runtime.ledger.state().authoritative_deposits_usdc(),
            Some(usd(100 - returned))
        );
        assert_eq!(
            runtime.state.pacing.deposits()["not-yet-approved"].returned_unadmitted_usdc,
            usd(returned)
        );
        let returned_at = funding_at + TimeDelta::minutes(30);
        assert_eq!(
            runtime.ledger.state().last_capital_event_at(),
            Some(returned_at)
        );
        let public: Value =
            serde_json::from_str(&fs::read_to_string(&runtime.config.status_path).unwrap())
                .unwrap();
        let published = public["operations"]["last_capital_event_at"]
            .as_str()
            .unwrap();
        assert_eq!(
            DateTime::parse_from_rfc3339(published)
                .unwrap()
                .with_timezone(&Utc),
            returned_at
        );
        drop(runtime);
        let mut runtime = SignerFreeRuntime::open(cfg, limits()).unwrap();
        assert_eq!(
            runtime.ledger.state().last_capital_event_at(),
            Some(returned_at)
        );
        let later = now + TimeDelta::hours(1);
        let admission = approvals("not-yet-approved", funding_at, later);
        funding_cycle(
            &mut runtime,
            later,
            &[incoming, outgoing],
            &admission,
            balance,
        )
        .unwrap();
        assert_eq!(runtime.ledger.state().admitted_usdc(), usd(100 - returned));
        assert_eq!(
            runtime.ledger.state().deployable_usdc(),
            usd(100 - returned)
        );
        assert_eq!(
            runtime.state.pacing.deposits()["not-yet-approved"].returned_unadmitted_usdc,
            usd(returned)
        );
    }
}

#[test]
fn parent_funding_return_during_cooldown_stays_unavailable_after_cooldown() {
    let directory = tempfile::tempdir().unwrap();
    let start = at(2026, 7, 6, 8, 0);
    let cfg = config(directory.path(), ms(start)).with_parent_funding_route(Some(parent_route()));
    let mut cap = limits();
    cap.deposit_cooldown_seconds = 4 * 3600;
    let mut runtime = SignerFreeRuntime::open(cfg, cap).unwrap();
    let received = start + TimeDelta::hours(1);
    let incoming = parent_transfer("cooling", received, 100);
    let mut outgoing = parent_transfer("return-cooling", received + TimeDelta::minutes(30), 100);
    outgoing.amount = -outgoing.amount;
    let admission = approvals("cooling", received, received);
    let movements = [incoming, outgoing];
    funding_cycle(
        &mut runtime,
        at(2026, 7, 6, 12, 0),
        &movements,
        &admission,
        0.0,
    )
    .unwrap();
    funding_cycle(
        &mut runtime,
        at(2026, 7, 6, 14, 0),
        &movements,
        &admission,
        0.0,
    )
    .unwrap();
    assert_eq!(runtime.ledger.state().admitted_usdc(), usd(0));
    assert_eq!(
        runtime.state.pacing.deposits()["cooling"].returned_unadmitted_usdc,
        usd(100)
    );
}

#[test]
fn parent_funding_mixed_return_preserves_only_admitted_withdrawal_debit() {
    let directory = tempfile::tempdir().unwrap();
    let start = at(2026, 7, 6, 8, 0);
    let now = at(2026, 7, 6, 12, 0);
    let cfg = config(directory.path(), ms(start)).with_parent_funding_route(Some(parent_route()));
    let mut cap = limits();
    cap.max_automatically_admitted_usdc = usd(100);
    let mut runtime = SignerFreeRuntime::open(cfg.clone(), cap.clone()).unwrap();
    let received = start + TimeDelta::hours(1);
    let incoming = parent_transfer("partly-admitted", received, 1_000);
    let admission = approvals("partly-admitted", received, received);
    funding_cycle(
        &mut runtime,
        now,
        std::slice::from_ref(&incoming),
        &admission,
        1_000.0,
    )
    .unwrap();
    let mut outgoing = parent_transfer("mixed-return", now + TimeDelta::minutes(1), 950);
    outgoing.amount = -outgoing.amount;
    let movements = [incoming, outgoing];
    funding_cycle(
        &mut runtime,
        now + TimeDelta::minutes(2),
        &movements,
        &admission,
        50.0,
    )
    .unwrap();
    assert_eq!(runtime.ledger.state().admitted_usdc(), usd(100));
    assert_eq!(runtime.ledger.state().deployable_usdc(), usd(50));
    assert_eq!(
        runtime.ledger.state().authoritative_deposits_usdc(),
        Some(usd(100))
    );
    let tranche = &runtime.state.pacing.deposits()["partly-admitted"];
    assert_eq!(tranche.returned_unadmitted_usdc, usd(900));
    assert_eq!(tranche.withdrawn_usdc, usd(50));
    drop(runtime);
    let mut runtime = SignerFreeRuntime::open(cfg, cap).unwrap();
    funding_cycle(
        &mut runtime,
        now + TimeDelta::minutes(5),
        &movements,
        &admission,
        50.0,
    )
    .unwrap();
    assert_eq!(runtime.ledger.state().deployable_usdc(), usd(50));
}

#[test]
fn parent_funding_conflicting_withdrawal_ids_leave_no_pending_cycle_and_can_recover() {
    let directory = tempfile::tempdir().unwrap();
    let start = at(2026, 7, 6, 8, 0);
    let now = at(2026, 7, 6, 12, 0);
    let cfg = config(directory.path(), ms(start)).with_parent_funding_route(Some(parent_route()));
    let mut runtime = SignerFreeRuntime::open(cfg.clone(), limits()).unwrap();
    let received = start + TimeDelta::hours(1);
    let incoming = parent_transfer("initial", received, 1_000);
    let admission = approvals("initial", received, received);
    funding_cycle(
        &mut runtime,
        now,
        std::slice::from_ref(&incoming),
        &admission,
        1_000.0,
    )
    .unwrap();
    let count = runtime.ledger.record_count();
    let before = runtime.state.clone();
    let mut outgoing = parent_transfer("conflicting-return", now + TimeDelta::minutes(1), 100);
    outgoing.amount = -outgoing.amount;
    let mut conflict = outgoing.clone();
    conflict.counterparty = Some("0x3333333333333333333333333333333333333333".to_owned());
    assert!(funding_cycle(
        &mut runtime,
        now + TimeDelta::minutes(2),
        &[incoming.clone(), outgoing.clone(), conflict.clone()],
        &admission,
        900.0
    )
    .is_err());
    assert_eq!(runtime.ledger.record_count(), count);
    assert_eq!(runtime.state, before);
    assert!(!runtime
        .config
        .state_directory
        .join(PENDING_CYCLE_FILE_NAME)
        .exists());
    drop(runtime);
    let mut runtime = SignerFreeRuntime::open(cfg.clone(), limits()).unwrap();
    funding_cycle(
        &mut runtime,
        now + TimeDelta::minutes(2),
        &[incoming.clone(), outgoing.clone()],
        &admission,
        900.0,
    )
    .unwrap();
    let count = runtime.ledger.record_count();
    let before = runtime.state.clone();
    // A later overlapping scan must not poison the journal either.
    assert!(funding_cycle(
        &mut runtime,
        now + TimeDelta::minutes(3),
        &[incoming.clone(), conflict],
        &admission,
        900.0
    )
    .is_err());
    assert_eq!(runtime.ledger.record_count(), count);
    assert_eq!(runtime.state, before);
    assert!(!runtime
        .config
        .state_directory
        .join(PENDING_CYCLE_FILE_NAME)
        .exists());
    drop(runtime);
    let mut runtime = SignerFreeRuntime::open(cfg, limits()).unwrap();
    funding_cycle(
        &mut runtime,
        now + TimeDelta::minutes(3),
        &[incoming, outgoing],
        &admission,
        900.0,
    )
    .unwrap();
    assert_eq!(runtime.ledger.state().deployable_usdc(), usd(900));
}

fn amount_approval(
    id: &str,
    confirmed: DateTime<Utc>,
    approved: DateTime<Utc>,
    amount: u64,
) -> AdmissionApprovals {
    AdmissionApprovals::from_json(
        &serde_json::json!({
            "schema_version": 1,
            "approvals": [{"event_id": id, "confirmed_at": confirmed,
                "confirmation_count": 2, "approved_at": approved,
                "max_admitted_usdc": usd(amount)}]
        })
        .to_string(),
    )
    .unwrap()
}

#[test]
fn explicit_parent_admission_survives_restart_and_cannot_be_rewritten() {
    let directory = tempfile::tempdir().unwrap();
    let start = at(2026, 7, 6, 8, 0);
    let now = at(2026, 7, 6, 12, 0);
    let cfg = config(directory.path(), ms(start)).with_parent_funding_route(Some(parent_route()));
    let mut cap = limits();
    cap.max_automatically_admitted_usdc = usd(100);
    cap.yearly_admission_cap_usdc = usd(100_000);
    cap.cumulative_admission_cap_usdc = usd(100_000);
    cap.max_daily_notional_usdc = usd(300);
    let incoming = parent_transfer("operator-funds", start, 20_000);
    let mut runtime = SignerFreeRuntime::open(cfg.clone(), cap.clone()).unwrap();
    funding_cycle(
        &mut runtime,
        start + TimeDelta::minutes(1),
        std::slice::from_ref(&incoming),
        &AdmissionApprovals::empty(),
        20_000.0,
    )
    .unwrap();
    let approval = amount_approval("operator-funds", start, start + TimeDelta::hours(1), 10_000);
    funding_cycle(
        &mut runtime,
        now,
        std::slice::from_ref(&incoming),
        &approval,
        20_000.0,
    )
    .unwrap();
    assert_eq!(runtime.ledger.state().admitted_usdc(), usd(10_000));
    drop(runtime);
    let mut runtime = SignerFreeRuntime::open(cfg, cap).unwrap();
    funding_cycle(
        &mut runtime,
        now + TimeDelta::minutes(5),
        &[],
        &AdmissionApprovals::empty(),
        20_000.0,
    )
    .unwrap();
    assert_eq!(runtime.ledger.state().admitted_usdc(), usd(10_000));
    let before = runtime.state.clone();
    for altered in [
        amount_approval("operator-funds", start, start + TimeDelta::hours(1), 11_000),
        amount_approval("operator-funds", start, start + TimeDelta::hours(1), 9_000),
        approvals("operator-funds", start, start + TimeDelta::hours(1)),
    ] {
        assert!(funding_cycle(
            &mut runtime,
            now + TimeDelta::minutes(10),
            &[],
            &altered,
            20_000.0
        )
        .is_err());
        assert_eq!(runtime.state, before);
        assert!(!runtime
            .config
            .state_directory
            .join(PENDING_CYCLE_FILE_NAME)
            .exists());
    }
}

#[test]
fn late_explicit_admission_does_not_rewrite_the_daily_decision() {
    let directory = tempfile::tempdir().unwrap();
    let start = at(2026, 7, 6, 8, 0);
    let cfg = config(directory.path(), ms(start)).with_parent_funding_route(Some(parent_route()));
    let mut cap = limits();
    cap.max_automatically_admitted_usdc = usd(100);
    let mut runtime = SignerFreeRuntime::open(cfg, cap).unwrap();
    let approval = amount_approval("late-approved", start, at(2026, 7, 6, 13, 0), 900);
    let movement = parent_transfer("late-approved", start, 1_000);
    let report = funding_cycle(
        &mut runtime,
        at(2026, 7, 6, 13, 5),
        &[movement],
        &approval,
        1_000.0,
    )
    .unwrap();
    assert_eq!(
        report.decision().unwrap().explanation.admitted_unspent_usdc,
        usd(0)
    );
    assert_eq!(runtime.ledger.state().admitted_usdc(), usd(900));
    let again = funding_cycle(
        &mut runtime,
        at(2026, 7, 6, 13, 10),
        &[],
        &approval,
        1_000.0,
    )
    .unwrap();
    assert!(!again.is_new_decision());
    assert_eq!(report.decision(), again.decision());
}

#[test]
fn explicit_admission_artifact_rejects_invalid_microunit_values() {
    let now = at(2026, 7, 6, 8, 0);
    for amount in [
        serde_json::json!(0),
        serde_json::json!(-1),
        serde_json::json!(1.5),
        serde_json::json!("1000000"),
    ] {
        let wire = serde_json::json!({"schema_version":1,"approvals":[{
            "event_id":"invalid", "confirmed_at":now,"approved_at":now,
            "confirmation_count":2,"max_admitted_usdc":amount
        }]});
        assert!(AdmissionApprovals::from_json(&wire.to_string()).is_err());
    }
}

fn live_mode() -> DecisionMode {
    DecisionMode::Live {
        history_directory: PathBuf::from("/var/lib/hype-accumulator/journals"),
    }
}

fn live_planned_decision(
    runtime: &mut SignerFreeRuntime,
    start: DateTime<Utc>,
    decision_at: DateTime<Utc>,
    movement: &HyperliquidAccountMovement,
    admission: &AdmissionApprovals,
    signal: &SignalSnapshot,
) -> RuntimeCycleReport {
    runtime
        .apply_cycle(RuntimeCycleInput {
            observed_at: decision_at,
            scan_start_ms: ms(start),
            scan_end_ms: ms(decision_at),
            movements: std::slice::from_ref(movement),
            approvals: admission,
            signal: Some(signal),
            accumulator: status(decision_at, 100.0),
            capital_history_complete: true,
            manual_pause: false,
            api_errors: 0,
            decision_mode: live_mode(),
        })
        .expect("live cycle")
}

#[test]
fn live_cycle_leaves_the_planned_decision_committed_and_unsettled() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let start = at(2026, 7, 6, 8, 0);
    let deposit_at = start + TimeDelta::hours(1);
    let decision_at = at(2026, 7, 6, 12, 0);
    let runtime_config = config(directory.path(), ms(start));
    let movement = deposit("deposit-approved", deposit_at, 100);
    let admission = approvals("deposit-approved", deposit_at, deposit_at);
    let signal = signal(decision_at);

    let mut runtime =
        SignerFreeRuntime::open(runtime_config.clone(), limits()).expect("open runtime");
    let first = live_planned_decision(
        &mut runtime,
        start,
        decision_at,
        &movement,
        &admission,
        &signal,
    );
    let decision = first.decision().expect("planned decision").clone();
    assert!(first.is_new_decision());
    assert_eq!(decision.reason, DecisionReason::Planned);
    assert!(!decision.planned_usdc.is_zero());
    // The whole point of live mode: nothing settles the decision in-cycle,
    // so the execution workflow can bind it and its capital stays committed
    // in both the pacing state and the replayed ledger.
    assert!(!decision.settled);
    assert_eq!(decision.filled_usdc, UsdcMicros::default());
    assert_eq!(decision.debited_usdc, UsdcMicros::default());
    assert!(!first.economic_action_suppressed);
    assert!(!first.signed_action_created);
    assert_eq!(
        runtime.ledger.state().committed_usdc(),
        decision.committed_usdc
    );
    assert_eq!(runtime.ledger.state().spent_usdc(), UsdcMicros::default());
    assert_eq!(runtime.state.dry_run_actions_total, 0);
    assert!(runtime.ledger.state().last_runtime_cycle_hash().is_some());
    let ledger_events = std::fs::read_to_string(
        runtime_config
            .state_directory
            .join(LEDGER_DIRECTORY_NAME)
            .join("ledger.jsonl"),
    )
    .expect("ledger journal");
    assert!(ledger_events.contains(":commitment"));
    assert!(!ledger_events.contains("dry-run-settlement"));
    drop(runtime);

    // A same-day replay (crash between prepare's cycle and the workflow
    // commit) hands back the same unsettled decision instead of a second one.
    let replay_at = decision_at + TimeDelta::minutes(5);
    let mut reopened =
        SignerFreeRuntime::open(runtime_config.clone(), limits()).expect("reopen runtime");
    let replay = reopened
        .apply_cycle(RuntimeCycleInput {
            observed_at: replay_at,
            scan_start_ms: ms(start),
            scan_end_ms: ms(replay_at),
            movements: std::slice::from_ref(&movement),
            approvals: &admission,
            signal: Some(&signal),
            accumulator: status(replay_at, 100.0),
            capital_history_complete: true,
            manual_pause: false,
            api_errors: 0,
            decision_mode: live_mode(),
        })
        .expect("same-day live replay");
    assert!(!replay.is_new_decision());
    assert!(!replay.decision().expect("existing decision").settled);
    assert_eq!(reopened.state.pacing.decisions().len(), 1);

    // Until the fill settles it, the next decision day fails closed rather
    // than stacking a second purchase on an unsettled commitment.
    let next_decision_at = at(2026, 7, 7, 12, 0);
    let next_signal = signal_for(next_decision_at, "2026-07-07");
    let next_scan_start_ms = reopened.next_scan_start_ms();
    let next = reopened
        .apply_cycle(RuntimeCycleInput {
            observed_at: next_decision_at,
            scan_start_ms: next_scan_start_ms,
            scan_end_ms: ms(next_decision_at),
            movements: &[movement],
            approvals: &admission,
            signal: Some(&next_signal),
            accumulator: status(next_decision_at, 100.0),
            capital_history_complete: true,
            manual_pause: false,
            api_errors: 0,
            decision_mode: live_mode(),
        })
        .expect("next-day live cycle");
    assert!(next.is_new_decision());
    let next_decision = next.decision().expect("next-day decision");
    assert_eq!(next_decision.reason, DecisionReason::PriorDecisionUnsettled);
    assert!(next_decision.planned_usdc.is_zero());
    assert_eq!(
        reopened.ledger.state().committed_usdc(),
        decision.committed_usdc
    );
}

/// Records `decision`'s journal intent (a live settlement may only be
/// evidenced by the journal its decision was bound to) and returns the
/// acquisition evidence a settlement crediting `credited_hype_atoms` must
/// quote.
#[allow(clippy::needless_pass_by_value)]
fn bound_acquisition(
    runtime: &mut SignerFreeRuntime,
    decision: &DailyDecision,
    journal: &Path,
    recorded_at: DateTime<Utc>,
    credited_hype_atoms: u64,
) -> LiveHypeAcquisition {
    runtime
        .record_live_journal_intent(&LiveDecisionIdentity::of(decision), journal, recorded_at)
        .expect("record journal intent");
    LiveHypeAcquisition::Workflow {
        workflow_id: format!("workflow:{}", decision.decision_id),
        journal: journal.to_path_buf(),
        credited_hype_atoms,
        last_fill_at: (credited_hype_atoms > 0).then_some(recorded_at),
    }
}

#[test]
#[allow(clippy::too_many_lines)]
fn live_settlement_converts_the_commitment_to_spend_exactly_once() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let start = at(2026, 7, 6, 8, 0);
    let deposit_at = start + TimeDelta::hours(1);
    let decision_at = at(2026, 7, 6, 12, 0);
    let runtime_config = config(directory.path(), ms(start));
    let movement = deposit("deposit-approved", deposit_at, 100);
    let admission = approvals("deposit-approved", deposit_at, deposit_at);
    let signal = signal(decision_at);
    // A fee/spread reserve so the commitment has headroom above the plan and
    // a debit above the filled notional (fees) is exercised.
    let mut reserve_limits = limits();
    reserve_limits.fee_spread_reserve_bps = 25;

    let mut runtime = SignerFreeRuntime::open(runtime_config.clone(), reserve_limits.clone())
        .expect("open runtime");
    let report = live_planned_decision(
        &mut runtime,
        start,
        decision_at,
        &movement,
        &admission,
        &signal,
    );
    let decision = report.decision().expect("planned decision").clone();
    let planned = decision.planned_usdc.as_micros();
    assert!(planned > 10_000);
    assert!(decision.committed_usdc > decision.planned_usdc);
    // Partial fill below the plan, debit above the fill (fees) but within
    // the commitment's reserve headroom.
    let filled = UsdcMicros::from_micros(planned - 10_000);
    let debited = UsdcMicros::from_micros(planned - 5_000);
    assert!(debited > filled && debited <= decision.committed_usdc);
    let journal = Path::new("/var/lib/hype-accumulator/journals/2026-07-06.jsonl");
    let acquired = bound_acquisition(&mut runtime, &decision, journal, decision_at, 30_000_000);

    // Settlement dated before the decision is refused.
    assert!(runtime
        .settle_live_decision(
            &LiveDecisionIdentity::of(&decision),
            filled,
            debited,
            &acquired,
            decision_at - TimeDelta::seconds(1),
        )
        .is_err());
    // Unknown decision, overfill, and a debit above the commitment all fail
    // closed without touching state.
    let mut unknown = LiveDecisionIdentity::of(&decision);
    unknown.decision_id = "fixed-dca:2026-07-05".to_owned();
    assert!(runtime
        .settle_live_decision(&unknown, filled, debited, &acquired, decision_at)
        .is_err());
    let overfill = UsdcMicros::from_micros(planned + 1);
    assert!(runtime
        .settle_live_decision(
            &LiveDecisionIdentity::of(&decision),
            overfill,
            overfill,
            &acquired,
            decision_at + TimeDelta::minutes(1),
        )
        .is_err());
    assert!(runtime
        .settle_live_decision(
            &LiveDecisionIdentity::of(&decision),
            filled,
            UsdcMicros::from_micros(decision.committed_usdc.as_micros() + 1),
            &acquired,
            decision_at + TimeDelta::minutes(1),
        )
        .is_err());
    assert!(!runtime.state.pacing.decisions()[&decision.decision_date].settled);
    assert_eq!(
        runtime.ledger.state().committed_usdc(),
        decision.committed_usdc
    );

    let settled_at = decision_at + TimeDelta::minutes(2);
    assert_eq!(
        runtime
            .settle_live_decision(
                &LiveDecisionIdentity::of(&decision),
                filled,
                debited,
                &acquired,
                settled_at
            )
            .expect("settle from the terminal fill"),
        LiveSettlementOutcome::Settled
    );
    let settled = runtime.state.pacing.decisions()[&decision.decision_date].clone();
    assert!(settled.settled);
    assert_eq!(settled.filled_usdc, filled);
    assert_eq!(settled.debited_usdc, debited);
    assert_eq!(
        runtime.ledger.state().committed_usdc(),
        UsdcMicros::default()
    );
    assert_eq!(runtime.ledger.state().spent_usdc(), debited);
    // Metrics are republished from the settled state without waiting for a
    // scheduled cycle.
    let metrics = std::fs::read_to_string(&runtime_config.metrics_path).expect("metrics file");
    #[allow(clippy::cast_precision_loss)]
    let spent_usdc = debited.as_micros() as f64 / 1_000_000.0;
    assert!(metrics.contains(&format!("hype_accumulator_spent_usdc {spent_usdc}")));
    assert!(metrics.contains("hype_accumulator_committed_usdc 0"));
    let invested = runtime
        .state
        .pacing
        .deposits()
        .values()
        .map(|tranche| tranche.invested_usdc)
        .fold(UsdcMicros::default(), |acc, value| {
            UsdcMicros::from_micros(acc.as_micros() + value.as_micros())
        });
    assert_eq!(invested, debited);

    // Exact replay is idempotent and writes nothing; a conflicting replay
    // fails closed.
    let head_before = runtime.state.last_committed_cycle_hash.clone();
    // ...and an idempotent replay republishes the derived outputs even if
    // the first publication had been lost.
    std::fs::remove_file(&runtime_config.metrics_path).expect("drop metrics file");
    assert_eq!(
        runtime
            .settle_live_decision(
                &LiveDecisionIdentity::of(&decision),
                filled,
                debited,
                &acquired,
                settled_at
            )
            .expect("idempotent replay"),
        LiveSettlementOutcome::AlreadySettled
    );
    assert_eq!(runtime.state.last_committed_cycle_hash, head_before);
    assert!(std::fs::read_to_string(&runtime_config.metrics_path)
        .expect("metrics republished on replay")
        .contains(&format!("hype_accumulator_spent_usdc {spent_usdc}")));
    assert!(runtime
        .settle_live_decision(
            &LiveDecisionIdentity::of(&decision),
            decision.planned_usdc,
            decision.planned_usdc,
            &acquired,
            settled_at
        )
        .is_err());
    drop(runtime);

    // The settled state survives a reopen (ledger/anchor/state agree) and
    // the next decision day plans again instead of failing closed.
    let mut reopened =
        SignerFreeRuntime::open(runtime_config, reserve_limits).expect("reopen after settlement");
    let next_decision_at = at(2026, 7, 7, 12, 0);
    let next_signal = signal_for(next_decision_at, "2026-07-07");
    let next_scan_start_ms = reopened.next_scan_start_ms();
    let next = reopened
        .apply_cycle(RuntimeCycleInput {
            observed_at: next_decision_at,
            scan_start_ms: next_scan_start_ms,
            scan_end_ms: ms(next_decision_at),
            movements: &[movement],
            approvals: &admission,
            signal: Some(&next_signal),
            accumulator: status(next_decision_at, 100.0),
            capital_history_complete: true,
            manual_pause: false,
            api_errors: 0,
            decision_mode: live_mode(),
        })
        .expect("next-day live cycle after settlement");
    let next_decision = next.decision().expect("next-day decision");
    assert!(next.is_new_decision());
    assert_eq!(next_decision.reason, DecisionReason::Planned);
    assert!(!next_decision.settled);
}

#[test]
#[allow(clippy::too_many_lines)]
fn live_settlement_records_the_hype_it_bought_and_attributes_it() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let start = at(2026, 7, 6, 8, 0);
    let deposit_at = start + TimeDelta::hours(1);
    let decision_at = at(2026, 7, 6, 12, 0);
    let runtime_config = config(directory.path(), ms(start));
    let movement = deposit("deposit-approved", deposit_at, 100);
    let admission = approvals("deposit-approved", deposit_at, deposit_at);
    let signal = signal(decision_at);
    let journal = Path::new("/var/lib/hype-accumulator/journals/2026-07-06.jsonl");

    let mut runtime =
        SignerFreeRuntime::open(runtime_config.clone(), limits()).expect("open runtime");
    let decision = live_planned_decision(
        &mut runtime,
        start,
        decision_at,
        &movement,
        &admission,
        &signal,
    )
    .decision()
    .expect("planned decision")
    .clone();
    // Nothing is attributed before a settlement, and an empty ledger is
    // complete: there is no purchase whose evidence could be missing.
    assert_eq!(runtime.attributed_hype(), AttributedHype::default());
    assert!(runtime.attributed_hype().is_complete());

    let filled = UsdcMicros::from_micros(decision.planned_usdc.as_micros() - 1_000);
    let credited = 29_979_000_u64;
    let fill_at = decision_at + TimeDelta::seconds(20);
    // Binds the decision to its journal the way `prepare` does, then quotes
    // that journal's own workflow as the settlement's evidence.
    bound_acquisition(&mut runtime, &decision, journal, decision_at, credited);
    let acquired = LiveHypeAcquisition::Workflow {
        workflow_id: "workflow-a".to_owned(),
        journal: journal.to_path_buf(),
        credited_hype_atoms: credited,
        last_fill_at: Some(fill_at),
    };

    // A settlement quoting a journal this decision was never bound to is
    // refused, as is one whose cash and inventory sides contradict.
    assert!(matches!(
        runtime.settle_live_decision(
            &LiveDecisionIdentity::of(&decision),
            filled,
            filled,
            &LiveHypeAcquisition::Workflow {
                workflow_id: "workflow-a".to_owned(),
                journal: PathBuf::from("/elsewhere/2026-07-06.jsonl"),
                credited_hype_atoms: credited,
                last_fill_at: Some(fill_at),
            },
            decision_at + TimeDelta::minutes(1),
        ),
        Err(RuntimeError::InvalidHypeAcquisition(_))
    ));
    assert!(matches!(
        runtime.settle_live_decision(
            &LiveDecisionIdentity::of(&decision),
            filled,
            filled,
            &LiveHypeAcquisition::Workflow {
                workflow_id: "workflow-a".to_owned(),
                journal: journal.to_path_buf(),
                credited_hype_atoms: 0,
                last_fill_at: None,
            },
            decision_at + TimeDelta::minutes(1),
        ),
        Err(RuntimeError::InvalidHypeAcquisition(_))
    ));
    // A decision that *was* bound to a journal can never be settled as
    // though no workflow existed.
    assert!(matches!(
        runtime.settle_live_decision(
            &LiveDecisionIdentity::of(&decision),
            UsdcMicros::default(),
            UsdcMicros::default(),
            &LiveHypeAcquisition::NoWorkflow,
            decision_at + TimeDelta::minutes(1),
        ),
        Err(RuntimeError::InvalidHypeAcquisition(_))
    ));
    assert!(!runtime.state.pacing.decisions()[&decision.decision_date].settled);
    assert_eq!(runtime.attributed_hype(), AttributedHype::default());

    let settled_at = decision_at + TimeDelta::minutes(2);
    assert_eq!(
        runtime
            .settle_live_decision(
                &LiveDecisionIdentity::of(&decision),
                filled,
                filled,
                &acquired,
                settled_at,
            )
            .expect("settle with its inventory"),
        LiveSettlementOutcome::Settled
    );
    let attributed = runtime.attributed_hype();
    assert_eq!(attributed.credited_hype_atoms, credited);
    assert_eq!(attributed.last_fill_at, Some(fill_at));
    assert_eq!(attributed.settled_purchases_with_evidence, 1);
    assert_eq!(attributed.settled_purchases_without_evidence, 0);
    assert!(attributed.is_complete());
    assert!((attributed.credited_hype() - 0.299_79).abs() < 1e-12);
    assert_eq!(
        attributed.to_attribution(),
        HypeAttribution::Reconciled {
            hype: 0.299_79,
            last_trade_at: Some(fill_at),
        }
    );

    // Exact replay is idempotent; a replay quoting different inventory for
    // the same decision fails closed rather than overwriting the record the
    // committed settlement was hash-chained with.
    let head_before = runtime.state.last_committed_cycle_hash.clone();
    assert_eq!(
        runtime
            .settle_live_decision(
                &LiveDecisionIdentity::of(&decision),
                filled,
                filled,
                &acquired,
                settled_at,
            )
            .expect("idempotent replay"),
        LiveSettlementOutcome::AlreadySettled
    );
    assert_eq!(runtime.state.last_committed_cycle_hash, head_before);
    assert!(matches!(
        runtime.settle_live_decision(
            &LiveDecisionIdentity::of(&decision),
            filled,
            filled,
            &LiveHypeAcquisition::Workflow {
                workflow_id: "workflow-a".to_owned(),
                journal: journal.to_path_buf(),
                credited_hype_atoms: credited + 1,
                last_fill_at: Some(fill_at),
            },
            settled_at,
        ),
        Err(RuntimeError::HypeAcquisitionConflict(_))
    ));
    assert_eq!(runtime.attributed_hype().credited_hype_atoms, credited);

    // The record survives a reopen: it is part of the committed state, not a
    // cache the next process rebuilds.
    drop(runtime);
    let reopened =
        SignerFreeRuntime::open(runtime_config, limits()).expect("reopen after settlement");
    assert_eq!(reopened.attributed_hype(), attributed);
}

#[test]
fn a_settled_purchase_without_acquisition_evidence_excludes_all_holdings() {
    // A purchase settled by a build older than bot-strategy#929 has no
    // acquisition row. Reporting the remaining rows as if they were the whole
    // would understate bot-owned inventory without saying so, so the whole
    // attribution is withheld until the missing row is backfilled.
    let directory = tempfile::tempdir().expect("temporary directory");
    let start = at(2026, 7, 6, 8, 0);
    let deposit_at = start + TimeDelta::hours(1);
    let decision_at = at(2026, 7, 6, 12, 0);
    let runtime_config = config(directory.path(), ms(start));
    let movement = deposit("deposit-approved", deposit_at, 100);
    let admission = approvals("deposit-approved", deposit_at, deposit_at);
    let signal = signal(decision_at);
    let journal = Path::new("/var/lib/hype-accumulator/journals/2026-07-06.jsonl");

    let mut runtime = SignerFreeRuntime::open(runtime_config, limits()).expect("open runtime");
    let decision = live_planned_decision(
        &mut runtime,
        start,
        decision_at,
        &movement,
        &admission,
        &signal,
    )
    .decision()
    .expect("planned decision")
    .clone();
    let filled = UsdcMicros::from_micros(decision.planned_usdc.as_micros() - 1_000);
    let acquired = bound_acquisition(&mut runtime, &decision, journal, decision_at, 29_979_000);
    runtime
        .settle_live_decision(
            &LiveDecisionIdentity::of(&decision),
            filled,
            filled,
            &acquired,
            decision_at + TimeDelta::minutes(2),
        )
        .expect("settle");
    assert!(runtime.attributed_hype().is_complete());

    // Simulate the legacy state: the settled purchase is there, its evidence
    // is not.
    runtime.state.hype_acquisitions.clear();
    let attributed = runtime.attributed_hype();
    assert_eq!(attributed.credited_hype_atoms, 0);
    assert_eq!(attributed.settled_purchases_without_evidence, 1);
    assert!(!attributed.is_complete());
    assert_eq!(attributed.to_attribution(), HypeAttribution::Unavailable);
}

#[test]
fn live_settlement_at_zero_releases_an_unfilled_commitment() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let start = at(2026, 7, 6, 8, 0);
    let deposit_at = start + TimeDelta::hours(1);
    let decision_at = at(2026, 7, 6, 12, 0);
    let runtime_config = config(directory.path(), ms(start));
    let movement = deposit("deposit-approved", deposit_at, 100);
    let admission = approvals("deposit-approved", deposit_at, deposit_at);
    let signal = signal(decision_at);

    let mut runtime =
        SignerFreeRuntime::open(runtime_config.clone(), limits()).expect("open runtime");
    let report = live_planned_decision(
        &mut runtime,
        start,
        decision_at,
        &movement,
        &admission,
        &signal,
    );
    let decision = report.decision().expect("planned decision").clone();
    let journal = Path::new("/var/lib/hype-accumulator/journals/2026-07-06.jsonl");
    // An IOC canceled unfilled credits no HYPE, and the acquisition record
    // says exactly that.
    let acquired = bound_acquisition(&mut runtime, &decision, journal, decision_at, 0);
    // An IOC canceled unfilled finalizes at zero: the commitment is released
    // and nothing is recorded as spent.
    assert_eq!(
        runtime
            .settle_live_decision(
                &LiveDecisionIdentity::of(&decision),
                UsdcMicros::default(),
                UsdcMicros::default(),
                &acquired,
                decision_at + TimeDelta::seconds(30),
            )
            .expect("zero settlement"),
        LiveSettlementOutcome::Settled
    );
    assert!(runtime.state.pacing.decisions()[&decision.decision_date].settled);
    assert_eq!(
        runtime.ledger.state().committed_usdc(),
        UsdcMicros::default()
    );
    assert_eq!(runtime.ledger.state().spent_usdc(), UsdcMicros::default());
    drop(runtime);
    SignerFreeRuntime::open(runtime_config, limits()).expect("reopen after zero settlement");
}

#[test]
fn live_settlement_refuses_a_decision_identity_from_another_runtime() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let start = at(2026, 7, 6, 8, 0);
    let deposit_at = start + TimeDelta::hours(1);
    let decision_at = at(2026, 7, 6, 12, 0);
    let runtime_config = config(directory.path(), ms(start));
    let movement = deposit("deposit-approved", deposit_at, 100);
    let admission = approvals("deposit-approved", deposit_at, deposit_at);
    let signal = signal(decision_at);

    let mut runtime =
        SignerFreeRuntime::open(runtime_config.clone(), limits()).expect("open runtime");
    let report = live_planned_decision(
        &mut runtime,
        start,
        decision_at,
        &movement,
        &admission,
        &signal,
    );
    let decision = report.decision().expect("planned decision").clone();
    assert_eq!(
        runtime.unsettled_planned_decisions(),
        vec![decision.clone()]
    );
    let journal = Path::new("/var/lib/hype-accumulator/journals/2026-07-06.jsonl");
    let acquired = bound_acquisition(&mut runtime, &decision, journal, decision_at, 0);
    let head_before = runtime.state.last_committed_cycle_hash.clone();
    let settled_at = decision_at + TimeDelta::minutes(2);

    // Same date-derived ID, but a binding produced by a different runtime
    // state (capital snapshot / input snapshot / amounts / tranches) must
    // never settle this runtime's decision — and must not touch it.
    let mut other_capital = LiveDecisionIdentity::of(&decision);
    other_capital.capital_snapshot_hash = "0".repeat(64);
    let mut other_signal = LiveDecisionIdentity::of(&decision);
    other_signal.input_snapshot_hash = "1".repeat(64);
    let mut other_amount = LiveDecisionIdentity::of(&decision);
    other_amount.planned_usdc = UsdcMicros::from_micros(decision.planned_usdc.as_micros() - 1);
    let mut other_tranche = LiveDecisionIdentity::of(&decision);
    other_tranche.allocations[0].tranche_id = "deposit-other".to_owned();
    let mut other_clock = LiveDecisionIdentity::of(&decision);
    other_clock.decided_at = decision_at + TimeDelta::days(1);
    for (identity, field) in [
        (other_capital, "capital_snapshot_hash"),
        (other_signal, "input_snapshot_hash"),
        (other_amount, "planned_usdc"),
        (other_tranche, "allocations"),
        (other_clock, "decided_at"),
    ] {
        match runtime.settle_live_decision(
            &identity,
            UsdcMicros::default(),
            UsdcMicros::default(),
            &acquired,
            settled_at,
        ) {
            Err(RuntimeError::LiveDecisionMismatch(mismatch)) => assert_eq!(mismatch, field),
            other => panic!("expected an identity mismatch on {field}, got {other:?}"),
        }
        assert!(!runtime.state.pacing.decisions()[&decision.decision_date].settled);
        assert_eq!(runtime.state.last_committed_cycle_hash, head_before);
    }

    // The genuine identity settles, after which nothing is left unsettled.
    assert_eq!(
        runtime
            .settle_live_decision(
                &LiveDecisionIdentity::of(&decision),
                UsdcMicros::default(),
                UsdcMicros::default(),
                &acquired,
                settled_at,
            )
            .expect("genuine identity settles"),
        LiveSettlementOutcome::Settled
    );
    assert!(runtime.unsettled_planned_decisions().is_empty());
}

#[test]
fn live_cycles_bind_the_runtime_to_one_history_directory() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let start = at(2026, 7, 6, 8, 0);
    let deposit_at = start + TimeDelta::hours(1);
    let decision_at = at(2026, 7, 6, 12, 0);
    let runtime_config = config(directory.path(), ms(start));
    let movement = deposit("deposit-approved", deposit_at, 100);
    let admission = approvals("deposit-approved", deposit_at, deposit_at);
    let signal = signal(decision_at);

    let mut runtime =
        SignerFreeRuntime::open(runtime_config.clone(), limits()).expect("open runtime");
    assert_eq!(runtime.live_history_directory(), None);
    // A DRY_RUN cycle never binds a namespace.
    runtime
        .apply_cycle(RuntimeCycleInput {
            observed_at: start + TimeDelta::hours(2),
            scan_start_ms: ms(start),
            scan_end_ms: ms(start + TimeDelta::hours(2)),
            movements: std::slice::from_ref(&movement),
            approvals: &admission,
            signal: None,
            accumulator: status(start + TimeDelta::hours(2), 100.0),
            capital_history_complete: true,
            manual_pause: false,
            api_errors: 0,
            decision_mode: DecisionMode::DryRun,
        })
        .expect("dry-run cycle");
    assert_eq!(runtime.live_history_directory(), None);

    live_planned_decision(
        &mut runtime,
        start,
        decision_at,
        &movement,
        &admission,
        &signal,
    );
    assert_eq!(
        runtime.live_history_directory(),
        Some(Path::new("/var/lib/hype-accumulator/journals"))
    );
    let head = runtime.state.last_committed_cycle_hash.clone();
    drop(runtime);

    // The binding is part of the committed state and survives a reopen; a
    // live cycle naming another directory fails closed before any change.
    let mut reopened =
        SignerFreeRuntime::open(runtime_config.clone(), limits()).expect("reopen runtime");
    assert_eq!(
        reopened.live_history_directory(),
        Some(Path::new("/var/lib/hype-accumulator/journals"))
    );
    let replay_at = decision_at + TimeDelta::minutes(5);
    let result = reopened.apply_cycle(RuntimeCycleInput {
        observed_at: replay_at,
        scan_start_ms: ms(start),
        scan_end_ms: ms(replay_at),
        movements: std::slice::from_ref(&movement),
        approvals: &admission,
        signal: Some(&signal),
        accumulator: status(replay_at, 100.0),
        capital_history_complete: true,
        manual_pause: false,
        api_errors: 0,
        decision_mode: DecisionMode::Live {
            history_directory: PathBuf::from("/somewhere/else"),
        },
    });
    assert!(matches!(
        result,
        Err(RuntimeError::LiveHistoryDirectoryMismatch(_))
    ));
    assert_eq!(reopened.state.last_committed_cycle_hash, head);
    assert_eq!(
        reopened.live_history_directory(),
        Some(Path::new("/var/lib/hype-accumulator/journals"))
    );
    // The same directory keeps working (same-day replay).
    reopened
        .apply_cycle(RuntimeCycleInput {
            observed_at: replay_at,
            scan_start_ms: ms(start),
            scan_end_ms: ms(replay_at),
            movements: std::slice::from_ref(&movement),
            approvals: &admission,
            signal: Some(&signal),
            accumulator: status(replay_at, 100.0),
            capital_history_complete: true,
            manual_pause: false,
            api_errors: 0,
            decision_mode: live_mode(),
        })
        .expect("same-directory live replay");
}

#[test]
#[allow(clippy::too_many_lines)]
fn journal_intent_is_recorded_before_the_journal_and_survives_reopen() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let start = at(2026, 7, 6, 8, 0);
    let deposit_at = start + TimeDelta::hours(1);
    let decision_at = at(2026, 7, 6, 12, 0);
    let runtime_config = config(directory.path(), ms(start));
    let movement = deposit("deposit-approved", deposit_at, 100);
    let admission = approvals("deposit-approved", deposit_at, deposit_at);
    let signal = signal(decision_at);
    let journal = Path::new("/var/lib/hype-accumulator/journals/2026-07-06.jsonl");

    let mut runtime =
        SignerFreeRuntime::open(runtime_config.clone(), limits()).expect("open runtime");
    let decision = live_planned_decision(
        &mut runtime,
        start,
        decision_at,
        &movement,
        &admission,
        &signal,
    )
    .decision()
    .expect("planned decision")
    .clone();
    assert_eq!(runtime.live_journal_intent(&decision.decision_id), None);
    let head_before = runtime.state.last_committed_cycle_hash.clone();

    // Identity mismatch and a backdated record fail closed without a write.
    let mut other = LiveDecisionIdentity::of(&decision);
    other.capital_snapshot_hash = "0".repeat(64);
    assert!(runtime
        .record_live_journal_intent(&other, journal, decision_at)
        .is_err());
    assert!(runtime
        .record_live_journal_intent(
            &LiveDecisionIdentity::of(&decision),
            journal,
            decision_at - TimeDelta::seconds(1)
        )
        .is_err());
    assert_eq!(runtime.state.last_committed_cycle_hash, head_before);

    let recorded_at = decision_at + TimeDelta::seconds(30);
    assert_eq!(
        runtime
            .record_live_journal_intent(&LiveDecisionIdentity::of(&decision), journal, recorded_at)
            .expect("intent recorded"),
        LiveSettlementOutcome::Settled
    );
    assert_eq!(
        runtime.live_journal_intent(&decision.decision_id),
        Some(journal)
    );
    assert_ne!(runtime.state.last_committed_cycle_hash, head_before);
    let head_after = runtime.state.last_committed_cycle_hash.clone();

    // Same path again (retry after a crash before the journal write) is a
    // no-op; a different path is a conflict.
    assert_eq!(
        runtime
            .record_live_journal_intent(&LiveDecisionIdentity::of(&decision), journal, recorded_at)
            .expect("idempotent"),
        LiveSettlementOutcome::AlreadySettled
    );
    assert_eq!(runtime.state.last_committed_cycle_hash, head_after);
    assert!(matches!(
        runtime.record_live_journal_intent(
            &LiveDecisionIdentity::of(&decision),
            Path::new("/elsewhere/2026-07-06.jsonl"),
            recorded_at
        ),
        Err(RuntimeError::LiveHistoryDirectoryMismatch(_))
    ));

    // A later decision can never claim the same journal file.
    runtime
        .settle_live_decision(
            &LiveDecisionIdentity::of(&decision),
            UsdcMicros::default(),
            UsdcMicros::default(),
            &LiveHypeAcquisition::Workflow {
                workflow_id: format!("workflow:{}", decision.decision_id),
                journal: journal.to_path_buf(),
                credited_hype_atoms: 0,
                last_fill_at: None,
            },
            recorded_at + TimeDelta::minutes(1),
        )
        .expect("settle day one");
    let next_decision_at = at(2026, 7, 7, 12, 0);
    let next_signal = signal_for(next_decision_at, "2026-07-07");
    let next_scan_start_ms = runtime.next_scan_start_ms();
    let next = runtime
        .apply_cycle(RuntimeCycleInput {
            observed_at: next_decision_at,
            scan_start_ms: next_scan_start_ms,
            scan_end_ms: ms(next_decision_at),
            movements: std::slice::from_ref(&movement),
            approvals: &admission,
            signal: Some(&next_signal),
            accumulator: status(next_decision_at, 100.0),
            capital_history_complete: true,
            manual_pause: false,
            api_errors: 0,
            decision_mode: live_mode(),
        })
        .expect("next-day live cycle")
        .decision()
        .expect("next-day decision")
        .clone();
    assert_eq!(next.reason, DecisionReason::Planned);
    assert!(matches!(
        runtime.record_live_journal_intent(
            &LiveDecisionIdentity::of(&next),
            journal,
            next_decision_at + TimeDelta::seconds(30)
        ),
        Err(RuntimeError::LiveHistoryDirectoryMismatch(_))
    ));
    assert_eq!(runtime.live_journal_intent(&next.decision_id), None);
    drop(runtime);

    // Hash-chained: still there after a reopen; a settled decision can no
    // longer take an intent.
    let reopened = SignerFreeRuntime::open(runtime_config, limits()).expect("reopen runtime");
    assert_eq!(
        reopened.live_journal_intent(&decision.decision_id),
        Some(journal)
    );
    let mut reopened = reopened;
    assert!(reopened
        .record_live_journal_intent(&LiveDecisionIdentity::of(&decision), journal, recorded_at)
        .is_err());
}
