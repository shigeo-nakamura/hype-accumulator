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
    let core = RevisionQuery::new("fixture-core", "v1", "hype_market", day("2026-07-06"))
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
    assert_eq!(runtime.state.dry_run_actions_total, 1);
    drop(runtime);

    let replay_at = decision_at + TimeDelta::minutes(5);
    let mut reopened = SignerFreeRuntime::open(runtime_config, limits()).expect("reopen runtime");
    assert_eq!(reopened.next_scan_start_ms(), ms(start));
    let replay = reopened
        .apply_cycle(RuntimeCycleInput {
            observed_at: replay_at,
            scan_start_ms: ms(start),
            scan_end_ms: ms(replay_at),
            movements: &[movement],
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
}

#[test]
fn deposit_after_scheduled_boundary_is_admitted_only_after_that_days_decision() {
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
