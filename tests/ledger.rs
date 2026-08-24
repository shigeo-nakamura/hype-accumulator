use chrono::{DateTime, TimeZone, Utc};
use hype_accumulator::{
    ledger::{
        AppendOutcome, DurableLedger, LedgerError, LedgerEvent, LedgerEventKind, LEDGER_FILE_NAME,
        SNAPSHOT_FILE_NAME,
    },
    pacing::UsdcMicros,
};
use serde_json::Value;
use std::{fs, path::Path};

fn at(hour: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 8, 24, hour, 0, 0)
        .single()
        .expect("valid UTC fixture")
}

fn usd(value: u64) -> UsdcMicros {
    UsdcMicros::checked_from_whole_usdc(value).expect("small test amount")
}

fn event(id: &str, hour: u32, kind: LedgerEventKind) -> LedgerEvent {
    LedgerEvent {
        event_id: id.into(),
        occurred_at: at(hour),
        kind,
    }
}

fn deposit(id: &str, hour: u32, amount: u64) -> LedgerEvent {
    event(
        id,
        hour,
        LedgerEventKind::AuthoritativeDeposit {
            amount_usdc: usd(amount),
        },
    )
}

fn observed(id: &str, hour: u32, usdc: u64, hype_atoms: u64) -> LedgerEvent {
    event(
        id,
        hour,
        LedgerEventKind::BalanceObserved {
            observed_usdc: usd(usdc),
            observed_hype_atoms: hype_atoms,
        },
    )
}

fn ledger_path(directory: &Path) -> std::path::PathBuf {
    directory.join(LEDGER_FILE_NAME)
}

fn snapshot_path(directory: &Path) -> std::path::PathBuf {
    directory.join(SNAPSHOT_FILE_NAME)
}

#[test]
fn duplicate_event_is_idempotent_and_id_collision_fails_closed() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let mut ledger = DurableLedger::open(directory.path()).expect("open ledger");
    let first = deposit("deposit-source", 1, 100);

    assert_eq!(
        ledger.append(first.clone()).expect("append deposit"),
        AppendOutcome::Appended
    );
    let durable_before = fs::read(ledger_path(directory.path())).expect("read ledger");
    assert_eq!(
        ledger.append(first.clone()).expect("replay duplicate"),
        AppendOutcome::Duplicate
    );
    assert_eq!(ledger.record_count(), 1);
    assert_eq!(
        fs::read(ledger_path(directory.path())).expect("read ledger"),
        durable_before
    );

    let collision = LedgerEvent {
        kind: LedgerEventKind::AuthoritativeDeposit {
            amount_usdc: usd(101),
        },
        ..first
    };
    assert_eq!(
        ledger.append(collision),
        Err(LedgerError::EventCollision("deposit-source".into()))
    );
    assert_eq!(ledger.record_count(), 1);
    assert_eq!(
        fs::read(ledger_path(directory.path())).expect("read ledger"),
        durable_before
    );
}

#[test]
fn duplicate_retry_verifies_that_the_event_is_still_durable() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let mut ledger = DurableLedger::open(directory.path()).expect("open ledger");
    let durable_event = deposit("deposit-durable-retry", 1, 100);
    ledger
        .append(durable_event.clone())
        .expect("append deposit");
    fs::remove_file(ledger_path(directory.path())).expect("delete journal fixture");

    assert!(matches!(
        ledger.append(durable_event),
        Err(LedgerError::TruncatedLedger)
    ));
}

#[test]
fn concurrent_writers_serialize_and_rebase_from_the_durable_head() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let first = DurableLedger::open(directory.path()).expect("open first writer");
    let second = DurableLedger::open(directory.path()).expect("open second writer");
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let first_barrier = std::sync::Arc::clone(&barrier);
    let second_barrier = std::sync::Arc::clone(&barrier);

    let first_thread = std::thread::spawn(move || {
        let mut first = first;
        first_barrier.wait();
        first.append(deposit("deposit-concurrent-a", 1, 100))
    });
    let second_thread = std::thread::spawn(move || {
        let mut second = second;
        second_barrier.wait();
        second.append(deposit("deposit-concurrent-b", 1, 100))
    });
    let outcomes = [
        first_thread.join().expect("first writer joined"),
        second_thread.join().expect("second writer joined"),
    ];

    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, Ok(AppendOutcome::Appended)))
            .count(),
        2
    );
    assert_eq!(
        DurableLedger::open(directory.path())
            .expect("journal remains replayable")
            .record_count(),
        2
    );
}

#[test]
fn only_admitted_authoritative_deposits_increase_deployable_capital() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let mut ledger = DurableLedger::open(directory.path()).expect("open ledger");

    ledger
        .append(observed("balance-before", 0, 9_999, 70_000_000))
        .expect("append balance");
    assert_eq!(ledger.state().deployable_usdc(), usd(0));

    ledger
        .append(deposit("deposit-authoritative", 1, 100))
        .expect("append authoritative deposit");
    assert_eq!(ledger.state().deployable_usdc(), usd(0));

    ledger
        .append(event(
            "deposit-admission",
            2,
            LedgerEventKind::DepositAdmission {
                deposit_event_id: "deposit-authoritative".into(),
                amount_usdc: usd(60),
            },
        ))
        .expect("append admission");
    assert_eq!(ledger.state().admitted_usdc(), usd(60));
    assert_eq!(ledger.state().deployable_usdc(), usd(60));

    ledger
        .append(observed("balance-after", 3, 50_000, 80_000_000))
        .expect("append later balance");
    assert_eq!(ledger.state().observed_usdc(), usd(50_000));
    assert_eq!(ledger.state().observed_hype_atoms(), 80_000_000);
    assert_eq!(ledger.state().deployable_usdc(), usd(60));

    ledger
        .append(event(
            "capital-commit",
            4,
            LedgerEventKind::CapitalCommitted {
                commitment_id: "commitment-1".into(),
                amount_usdc: usd(20),
            },
        ))
        .expect("append commitment");
    assert_eq!(ledger.state().deployable_usdc(), usd(40));

    ledger
        .append(event(
            "capital-settlement",
            5,
            LedgerEventKind::CapitalSettled {
                commitment_id: "commitment-1".into(),
                debited_usdc: usd(15),
            },
        ))
        .expect("append settlement");
    assert_eq!(ledger.state().committed_usdc(), usd(0));
    assert_eq!(ledger.state().spent_usdc(), usd(15));
    assert_eq!(ledger.state().deployable_usdc(), usd(45));

    ledger
        .append(event(
            "withdrawal-1",
            6,
            LedgerEventKind::AuthoritativeWithdrawal {
                amount_usdc: usd(5),
            },
        ))
        .expect("append withdrawal");
    assert_eq!(ledger.state().withdrawn_usdc(), usd(5));
    assert_eq!(ledger.state().deployable_usdc(), usd(40));

    let replayed = DurableLedger::open(directory.path()).expect("replay ledger");
    assert_eq!(replayed.state(), ledger.state());
}

#[test]
fn rejected_capital_transition_does_not_mutate_memory_or_disk() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let mut ledger = DurableLedger::open(directory.path()).expect("open ledger");
    ledger
        .append(deposit("deposit-authoritative", 1, 10))
        .expect("append deposit");
    ledger
        .append(event(
            "deposit-admission",
            2,
            LedgerEventKind::DepositAdmission {
                deposit_event_id: "deposit-authoritative".into(),
                amount_usdc: usd(10),
            },
        ))
        .expect("append admission");
    let durable_before = fs::read(ledger_path(directory.path())).expect("read ledger");
    let state_before = ledger.state().clone();

    assert_eq!(
        ledger.append(event(
            "oversized-commitment",
            3,
            LedgerEventKind::CapitalCommitted {
                commitment_id: "commitment-too-large".into(),
                amount_usdc: usd(11),
            },
        )),
        Err(LedgerError::InsufficientDeployableCapital)
    );
    assert_eq!(ledger.state(), &state_before);
    assert_eq!(
        fs::read(ledger_path(directory.path())).expect("read ledger"),
        durable_before
    );
}

#[test]
fn journal_hash_tampering_is_detected() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let mut ledger = DurableLedger::open(directory.path()).expect("open ledger");
    ledger
        .append(deposit("deposit-source", 1, 100))
        .expect("append deposit");
    ledger
        .append(observed("balance-source", 2, 100, 1))
        .expect("append balance");
    drop(ledger);

    let path = ledger_path(directory.path());
    let original = fs::read_to_string(&path).expect("read ledger");
    let tampered = original.replacen("deposit-source", "deposit-tamper", 1);
    assert_eq!(tampered.len(), original.len());
    assert_ne!(tampered, original);
    fs::write(path, tampered).expect("tamper fixture");

    assert!(matches!(
        DurableLedger::open(directory.path()),
        Err(LedgerError::CorruptLedger(_))
    ));
}

#[test]
fn unknown_unhashed_journal_field_is_rejected() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let mut ledger = DurableLedger::open(directory.path()).expect("open ledger");
    ledger
        .append(deposit("deposit-unknown-field", 1, 100))
        .expect("append deposit");
    drop(ledger);

    let path = ledger_path(directory.path());
    let original = fs::read_to_string(&path).expect("read ledger");
    let tampered = original.replacen(
        ",\"record_hash\"",
        ",\"unhashed_field\":\"injected\",\"record_hash\"",
        1,
    );
    assert_ne!(tampered, original);
    fs::write(path, tampered).expect("tamper fixture");

    assert!(matches!(
        DurableLedger::open(directory.path()),
        Err(LedgerError::Json(_))
    ));
}

#[test]
fn partial_final_record_is_reported_as_truncation() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let mut ledger = DurableLedger::open(directory.path()).expect("open ledger");
    ledger
        .append(deposit("deposit-partial", 1, 100))
        .expect("append deposit");
    drop(ledger);

    let path = ledger_path(directory.path());
    let mut payload = fs::read(&path).expect("read ledger");
    assert_eq!(payload.pop(), Some(b'\n'));
    fs::write(path, payload).expect("truncate fixture");

    assert!(matches!(
        DurableLedger::open(directory.path()),
        Err(LedgerError::TruncatedLedger)
    ));
}

#[test]
fn checkpoint_anchor_detects_complete_tail_loss() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let mut ledger = DurableLedger::open(directory.path()).expect("open ledger");
    ledger
        .append(deposit("deposit-tail", 1, 100))
        .expect("append deposit");
    ledger.checkpoint().expect("write earlier checkpoint");
    ledger
        .append(observed("balance-tail", 2, 100, 1))
        .expect("append and advance latest-head snapshot");
    drop(ledger);

    let path = ledger_path(directory.path());
    let payload = fs::read(&path).expect("read ledger");
    let first_line = payload
        .iter()
        .position(|byte| *byte == b'\n')
        .map(|index| index + 1)
        .expect("first complete record");
    fs::write(path, &payload[..first_line]).expect("remove complete tail record");

    assert!(matches!(
        DurableLedger::open(directory.path()),
        Err(LedgerError::TruncatedLedger)
    ));
}

#[test]
fn snapshot_checksum_tampering_and_empty_snapshot_are_detected() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let mut ledger = DurableLedger::open(directory.path()).expect("open ledger");
    ledger
        .append(deposit("deposit-snapshot", 1, 100))
        .expect("append deposit");
    ledger.checkpoint().expect("write checkpoint");
    drop(ledger);

    let path = snapshot_path(directory.path());
    let mut document: Value =
        serde_json::from_slice(&fs::read(&path).expect("read snapshot")).expect("valid JSON");
    let checksum = document["checksum"]
        .as_str()
        .expect("checksum string")
        .to_owned();
    let first = if checksum.starts_with('0') { '1' } else { '0' };
    document["checksum"] = Value::String(format!("{first}{}", &checksum[1..]));
    fs::write(
        &path,
        serde_json::to_vec(&document).expect("encode fixture"),
    )
    .expect("tamper fixture");
    assert!(matches!(
        DurableLedger::open(directory.path()),
        Err(LedgerError::CorruptSnapshot)
    ));

    fs::write(path, []).expect("empty snapshot fixture");
    assert!(matches!(
        DurableLedger::open(directory.path()),
        Err(LedgerError::CorruptSnapshot)
    ));
}

#[test]
fn clean_directory_restore_round_trips_exact_checkpoint() {
    let source = tempfile::tempdir().expect("source directory");
    let container = tempfile::tempdir().expect("destination container");
    let destination = container.path().join("restored");
    let mut ledger = DurableLedger::open(source.path()).expect("open ledger");
    ledger
        .append(deposit("deposit-restore", 1, 100))
        .expect("append deposit");
    ledger
        .append(event(
            "admission-restore",
            2,
            LedgerEventKind::DepositAdmission {
                deposit_event_id: "deposit-restore".into(),
                amount_usdc: usd(40),
            },
        ))
        .expect("append admission");
    let expected_state = ledger.state().clone();
    let expected_head = ledger.head_hash().to_owned();
    ledger.checkpoint().expect("write checkpoint");
    drop(ledger);

    let restored = DurableLedger::restore_clean(source.path(), &destination).expect("restore");
    assert_eq!(restored.state(), &expected_state);
    assert_eq!(restored.head_hash(), expected_head);
    assert_eq!(restored.record_count(), 2);
    let file_names = fs::read_dir(&destination)
        .expect("read restored directory")
        .map(|entry| {
            entry
                .expect("directory entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(file_names.len(), 3);
    assert!(file_names.contains(LEDGER_FILE_NAME));
    assert!(file_names.contains(SNAPSHOT_FILE_NAME));
    assert!(file_names.contains(".ledger.lock"));
    assert_eq!(
        DurableLedger::open(&destination)
            .expect("reopen restored ledger")
            .state(),
        &expected_state
    );
}

#[test]
fn restore_rejects_stale_or_missing_snapshot_and_nonempty_destination() {
    let source = tempfile::tempdir().expect("source directory");
    let destination = tempfile::tempdir().expect("destination directory");
    let mut ledger = DurableLedger::open(source.path()).expect("open ledger");
    ledger
        .append(deposit("deposit-restore-guard", 1, 100))
        .expect("append deposit");

    let first_snapshot = fs::read(snapshot_path(source.path())).expect("read exact snapshot");
    fs::remove_file(snapshot_path(source.path())).expect("remove snapshot fixture");

    assert!(matches!(
        DurableLedger::restore_clean(source.path(), destination.path()),
        Err(LedgerError::MissingSnapshot)
    ));

    fs::write(snapshot_path(source.path()), &first_snapshot)
        .expect("restore exact snapshot fixture");
    ledger
        .append(observed("balance-after-checkpoint", 2, 100, 1))
        .expect("append event after checkpoint");
    let current_snapshot = fs::read(snapshot_path(source.path())).expect("read current snapshot");
    fs::write(snapshot_path(source.path()), first_snapshot)
        .expect("restore stale snapshot fixture");
    assert!(matches!(
        DurableLedger::restore_clean(source.path(), destination.path()),
        Err(LedgerError::StaleSnapshot)
    ));
    assert!(matches!(
        DurableLedger::open(source.path()),
        Err(LedgerError::StaleSnapshot)
    ));

    fs::write(snapshot_path(source.path()), current_snapshot).expect("restore current snapshot");
    fs::write(destination.path().join("occupied"), b"data").expect("occupy destination");
    assert!(matches!(
        DurableLedger::restore_clean(source.path(), destination.path()),
        Err(LedgerError::RestoreDestinationNotEmpty)
    ));
}
