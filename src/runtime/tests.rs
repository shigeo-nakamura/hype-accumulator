use super::*;
use crate::{
    config::CarryOverPolicy,
    pacing::DecisionReason,
    signal::{FreshnessRequirement, LiveSignalNormalizer, RevisionQuery, SnapshotRequest},
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
