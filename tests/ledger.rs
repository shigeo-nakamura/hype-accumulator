use chrono::{DateTime, TimeZone, Utc};
use hype_accumulator::{
    ledger::{
        AppendOutcome, DurableLedger, LedgerError, LedgerEvent, LedgerEventKind,
        ProtectedAnchorStore, ProtectedHeadAnchor, LEDGER_FILE_NAME, SNAPSHOT_FILE_NAME,
    },
    pacing::UsdcMicros,
};
use serde_json::Value;
use std::{
    fs,
    path::Path,
    sync::{Arc, Mutex},
};

#[derive(Default)]
struct MemoryProtectedAnchorStore(Mutex<Option<ProtectedHeadAnchor>>);

impl ProtectedAnchorStore for MemoryProtectedAnchorStore {
    fn load(&self) -> Result<Option<ProtectedHeadAnchor>, String> {
        self.0
            .lock()
            .map(|anchor| anchor.clone())
            .map_err(|_| "protected anchor lock poisoned".into())
    }

    fn compare_and_swap(
        &self,
        expected: Option<&ProtectedHeadAnchor>,
        next: &ProtectedHeadAnchor,
    ) -> Result<bool, String> {
        let mut anchor = self
            .0
            .lock()
            .map_err(|_| "protected anchor lock poisoned".to_owned())?;
        if anchor.as_ref() != expected {
            return Ok(false);
        }
        *anchor = Some(next.clone());
        Ok(true)
    }
}

impl MemoryProtectedAnchorStore {
    fn replace_for_test(&self, anchor: Option<ProtectedHeadAnchor>) {
        *self.0.lock().expect("protected anchor lock") = anchor;
    }
}

type TestAnchor = Arc<MemoryProtectedAnchorStore>;

fn anchor_store() -> TestAnchor {
    Arc::new(MemoryProtectedAnchorStore::default())
}

fn open(directory: &Path, anchor: &TestAnchor) -> Result<DurableLedger, LedgerError> {
    DurableLedger::open(directory, anchor.clone())
}

fn restore(
    source: &Path,
    destination: &Path,
    source_anchor: &TestAnchor,
    destination_anchor: &TestAnchor,
) -> Result<DurableLedger, LedgerError> {
    DurableLedger::restore_clean(
        source,
        destination,
        source_anchor.clone(),
        destination_anchor.clone(),
    )
}

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
    let anchor = anchor_store();
    let mut ledger = open(directory.path(), &anchor).expect("open ledger");
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
    let anchor = anchor_store();
    let mut ledger = open(directory.path(), &anchor).expect("open ledger");
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
    let anchor = anchor_store();
    let first = open(directory.path(), &anchor).expect("open first writer");
    let second = open(directory.path(), &anchor).expect("open second writer");
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
        open(directory.path(), &anchor)
            .expect("journal remains replayable")
            .record_count(),
        2
    );
}

#[test]
fn only_admitted_authoritative_deposits_increase_deployable_capital() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let anchor = anchor_store();
    let mut ledger = open(directory.path(), &anchor).expect("open ledger");

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

    let replayed = open(directory.path(), &anchor).expect("replay ledger");
    assert_eq!(replayed.state(), ledger.state());
}

#[test]
fn rejected_capital_transition_does_not_mutate_memory_or_disk() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let anchor = anchor_store();
    let mut ledger = open(directory.path(), &anchor).expect("open ledger");
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
    let anchor = anchor_store();
    let mut ledger = open(directory.path(), &anchor).expect("open ledger");
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
        open(directory.path(), &anchor),
        Err(LedgerError::CorruptLedger(_))
    ));
}

#[test]
fn unknown_unhashed_journal_field_is_rejected() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let anchor = anchor_store();
    let mut ledger = open(directory.path(), &anchor).expect("open ledger");
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
        open(directory.path(), &anchor),
        Err(LedgerError::Json(_))
    ));
}

#[test]
fn partial_final_record_is_reported_as_truncation() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let anchor = anchor_store();
    let mut ledger = open(directory.path(), &anchor).expect("open ledger");
    ledger
        .append(deposit("deposit-partial", 1, 100))
        .expect("append deposit");
    drop(ledger);

    let path = ledger_path(directory.path());
    let mut payload = fs::read(&path).expect("read ledger");
    assert_eq!(payload.pop(), Some(b'\n'));
    fs::write(path, payload).expect("truncate fixture");

    assert!(matches!(
        open(directory.path(), &anchor),
        Err(LedgerError::TruncatedLedger)
    ));
}

#[test]
fn checkpoint_anchor_detects_complete_tail_loss() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let anchor = anchor_store();
    let mut ledger = open(directory.path(), &anchor).expect("open ledger");
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
        open(directory.path(), &anchor),
        Err(LedgerError::TruncatedLedger)
    ));
}

#[test]
fn protected_anchor_rejects_a_matching_local_journal_and_snapshot_rollback() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let anchor = anchor_store();
    let mut ledger = open(directory.path(), &anchor).expect("open ledger");
    ledger
        .append(deposit("deposit-before-rollback", 1, 100))
        .expect("append deposit");
    let old_journal = fs::read(ledger_path(directory.path())).expect("read old journal");
    let old_snapshot = fs::read(snapshot_path(directory.path())).expect("read old snapshot");
    ledger
        .append(observed("balance-after-rollback", 2, 100, 1))
        .expect("append newer state");
    drop(ledger);

    fs::write(ledger_path(directory.path()), old_journal).expect("roll back local journal");
    fs::write(snapshot_path(directory.path()), old_snapshot).expect("roll back local snapshot");

    assert!(matches!(
        open(directory.path(), &anchor),
        Err(LedgerError::TruncatedLedger)
    ));
}

#[test]
fn protected_anchor_rejects_an_old_anchor_reappearing() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let anchor = anchor_store();
    let mut ledger = open(directory.path(), &anchor).expect("open ledger");
    ledger
        .append(deposit("deposit-before-anchor-rollback", 1, 100))
        .expect("append deposit");
    let old_anchor = anchor.load().expect("load old anchor");
    ledger
        .append(observed("balance-after-anchor-rollback", 2, 100, 1))
        .expect("append newer state");
    drop(ledger);

    anchor.replace_for_test(old_anchor);
    assert!(matches!(
        open(directory.path(), &anchor),
        Err(LedgerError::ProtectedAnchorMismatch)
    ));
}

#[test]
fn nonempty_ledger_requires_its_protected_anchor_scope() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let anchor = anchor_store();
    let mut ledger = open(directory.path(), &anchor).expect("open ledger");
    ledger
        .append(deposit("deposit-before-missing-anchor", 1, 100))
        .expect("append deposit");
    drop(ledger);

    let unrelated_empty_scope = anchor_store();
    assert!(matches!(
        open(directory.path(), &unrelated_empty_scope),
        Err(LedgerError::MissingProtectedAnchor)
    ));
}

#[test]
fn snapshot_checksum_tampering_and_empty_snapshot_are_detected() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let anchor = anchor_store();
    let mut ledger = open(directory.path(), &anchor).expect("open ledger");
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
        open(directory.path(), &anchor),
        Err(LedgerError::CorruptSnapshot)
    ));

    fs::write(path, []).expect("empty snapshot fixture");
    assert!(matches!(
        open(directory.path(), &anchor),
        Err(LedgerError::CorruptSnapshot)
    ));
}

#[test]
fn clean_directory_restore_round_trips_exact_checkpoint() {
    let source = tempfile::tempdir().expect("source directory");
    let container = tempfile::tempdir().expect("destination container");
    let destination = container.path().join("restored");
    let source_anchor = anchor_store();
    let destination_anchor = anchor_store();
    let mut ledger = open(source.path(), &source_anchor).expect("open ledger");
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

    let restored = restore(
        source.path(),
        &destination,
        &source_anchor,
        &destination_anchor,
    )
    .expect("restore");
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
        open(&destination, &destination_anchor)
            .expect("reopen restored ledger")
            .state(),
        &expected_state
    );
}

#[cfg(unix)]
#[test]
fn restore_rejects_a_source_directory_alias() {
    use std::os::unix::fs::symlink;

    let source = tempfile::tempdir().expect("source directory");
    let container = tempfile::tempdir().expect("alias container");
    let alias = container.path().join("source-alias");
    let source_anchor = anchor_store();
    let destination_anchor = anchor_store();
    let mut ledger = open(source.path(), &source_anchor).expect("open ledger");
    ledger
        .append(deposit("deposit-before-alias", 1, 100))
        .expect("append deposit");
    drop(ledger);
    symlink(source.path(), &alias).expect("create source alias");

    assert!(matches!(
        restore(source.path(), &alias, &source_anchor, &destination_anchor,),
        Err(LedgerError::RestoreDestinationNotEmpty)
    ));
}

#[test]
fn opposing_restores_acquire_directory_locks_in_the_same_order() {
    use std::{sync::Barrier, time::Duration};

    let left = tempfile::tempdir().expect("left directory");
    let right = tempfile::tempdir().expect("right directory");
    let left_path = left.path().to_path_buf();
    let right_path = right.path().to_path_buf();
    let left_anchor = anchor_store();
    let right_anchor = anchor_store();
    let mut left_ledger = open(&left_path, &left_anchor).expect("open left ledger");
    left_ledger
        .append(deposit("deposit-left", 1, 100))
        .expect("append left deposit");
    let mut right_ledger = open(&right_path, &right_anchor).expect("open right ledger");
    right_ledger
        .append(deposit("deposit-right", 1, 100))
        .expect("append right deposit");
    drop((left_ledger, right_ledger));

    let barrier = Arc::new(Barrier::new(2));
    let (sender, receiver) = std::sync::mpsc::channel();
    let left_to_right = {
        let barrier = Arc::clone(&barrier);
        let sender = sender.clone();
        let left_path = left_path.clone();
        let right_path = right_path.clone();
        let left_anchor = Arc::clone(&left_anchor);
        let right_anchor = Arc::clone(&right_anchor);
        std::thread::spawn(move || {
            barrier.wait();
            sender
                .send(restore(
                    &left_path,
                    &right_path,
                    &left_anchor,
                    &right_anchor,
                ))
                .expect("send left-to-right result");
        })
    };
    let right_to_left = {
        let barrier = Arc::clone(&barrier);
        let sender = sender.clone();
        let left_anchor = Arc::clone(&left_anchor);
        let right_anchor = Arc::clone(&right_anchor);
        std::thread::spawn(move || {
            barrier.wait();
            sender
                .send(restore(
                    &right_path,
                    &left_path,
                    &right_anchor,
                    &left_anchor,
                ))
                .expect("send right-to-left result");
        })
    };
    drop(sender);

    for _ in 0..2 {
        assert!(matches!(
            receiver
                .recv_timeout(Duration::from_secs(2))
                .expect("opposing restore must not deadlock"),
            Err(LedgerError::RestoreDestinationNotEmpty)
        ));
    }
    left_to_right.join().expect("left-to-right thread");
    right_to_left.join().expect("right-to-left thread");
}

#[test]
fn protected_anchor_rejects_complete_local_ledger_and_snapshot_loss() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let anchor = anchor_store();
    let mut ledger = open(directory.path(), &anchor).expect("open ledger");
    ledger
        .append(deposit("deposit-before-paired-loss", 1, 100))
        .expect("append deposit");
    drop(ledger);

    fs::remove_file(directory.path().join(LEDGER_FILE_NAME)).expect("remove ledger fixture");
    fs::remove_file(directory.path().join(SNAPSHOT_FILE_NAME)).expect("remove snapshot fixture");

    assert!(matches!(
        open(directory.path(), &anchor),
        Err(LedgerError::TruncatedLedger)
    ));
}

#[test]
fn restore_rejects_stale_or_missing_snapshot_and_nonempty_destination() {
    let source = tempfile::tempdir().expect("source directory");
    let destination = tempfile::tempdir().expect("destination directory");
    let source_anchor = anchor_store();
    let destination_anchor = anchor_store();
    let mut ledger = open(source.path(), &source_anchor).expect("open ledger");
    ledger
        .append(deposit("deposit-restore-guard", 1, 100))
        .expect("append deposit");

    let first_snapshot = fs::read(snapshot_path(source.path())).expect("read exact snapshot");
    fs::remove_file(snapshot_path(source.path())).expect("remove snapshot fixture");

    assert!(matches!(
        restore(
            source.path(),
            destination.path(),
            &source_anchor,
            &destination_anchor,
        ),
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
        restore(
            source.path(),
            destination.path(),
            &source_anchor,
            &destination_anchor,
        ),
        Err(LedgerError::StaleSnapshot)
    ));
    assert!(matches!(
        open(source.path(), &source_anchor),
        Err(LedgerError::StaleSnapshot)
    ));

    fs::write(snapshot_path(source.path()), current_snapshot).expect("restore current snapshot");
    fs::write(destination.path().join("occupied"), b"data").expect("occupy destination");
    assert!(matches!(
        restore(
            source.path(),
            destination.path(),
            &source_anchor,
            &destination_anchor,
        ),
        Err(LedgerError::RestoreDestinationNotEmpty)
    ));
}
