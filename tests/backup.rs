use chrono::{TimeZone, Utc};
use hype_accumulator::{
    backup::{
        create_ledger_backup, restore_ledger_backup, verify_ledger_backup, LedgerBackupError,
        LedgerBackupManifest,
    },
    ledger::{DurableLedger, LedgerEvent, LedgerEventKind, ProtectedAnchorStore},
    pacing::UsdcMicros,
    runtime::FileProtectedAnchorStore,
};
use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};

struct BackupFixture {
    temporary: tempfile::TempDir,
    source_directory: PathBuf,
    source_anchor: PathBuf,
    bundle_directory: PathBuf,
    anchor_export: PathBuf,
    manifest: LedgerBackupManifest,
}

impl BackupFixture {
    fn new() -> Self {
        Self::captured_at(8, 30)
    }

    fn captured_at(hour: u32, minute: u32) -> Self {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = temporary.path();
        let source_directory = root.join("source-ledger");
        let source_anchor = root.join("source-anchor.json");
        let bundle_directory = root.join("backup-bundle");
        let anchor_export = root.join("backup-anchor.json");
        seed_ledger(&source_directory, &source_anchor);
        let manifest = create_ledger_backup(
            &source_directory,
            &source_anchor,
            &bundle_directory,
            &anchor_export,
            Utc.with_ymd_and_hms(2026, 9, 2, hour, minute, 0)
                .single()
                .expect("valid capture time"),
        )
        .expect("create backup");
        Self {
            temporary,
            source_directory,
            source_anchor,
            bundle_directory,
            anchor_export,
            manifest,
        }
    }
}

fn seed_ledger(directory: &Path, anchor_path: &Path) {
    let anchor = Arc::new(
        FileProtectedAnchorStore::new(anchor_path.to_path_buf()).expect("source anchor store"),
    );
    let mut ledger = DurableLedger::open(directory, anchor).expect("open source ledger");
    let amount = UsdcMicros::checked_from_whole_usdc(125).expect("small fixture amount");
    ledger
        .append(LedgerEvent {
            event_id: "backup-deposit".to_owned(),
            occurred_at: Utc
                .with_ymd_and_hms(2026, 9, 2, 8, 0, 0)
                .single()
                .expect("valid event time"),
            kind: LedgerEventKind::AuthoritativeDeposit {
                amount_usdc: amount,
            },
        })
        .expect("append deposit");
    ledger
        .append(LedgerEvent {
            event_id: "backup-admission".to_owned(),
            occurred_at: Utc
                .with_ymd_and_hms(2026, 9, 2, 8, 1, 0)
                .single()
                .expect("valid event time"),
            kind: LedgerEventKind::DepositAdmission {
                deposit_event_id: "backup-deposit".to_owned(),
                amount_usdc: amount,
            },
        })
        .expect("append admission");
}

#[test]
fn checksummed_backup_verifies_and_restores_into_a_clean_scope() {
    let fixture = BackupFixture::new();

    let verified = verify_ledger_backup(&fixture.bundle_directory, &fixture.anchor_export)
        .expect("verify backup");
    assert_eq!(verified, fixture.manifest);
    assert_eq!(verified.record_count, 2);

    let destination_directory = fixture.temporary.path().join("clean-restore-ledger");
    let destination_anchor = fixture.temporary.path().join("clean-restore-anchor.json");
    let restored_manifest = restore_ledger_backup(
        &fixture.bundle_directory,
        &fixture.anchor_export,
        &destination_directory,
        &destination_anchor,
    )
    .expect("restore verified backup");
    assert_eq!(restored_manifest, fixture.manifest);

    let destination_store = Arc::new(
        FileProtectedAnchorStore::new(destination_anchor).expect("destination anchor store"),
    );
    let restored = DurableLedger::open(destination_directory, destination_store.clone())
        .expect("reopen restored ledger");
    assert_eq!(restored.record_count(), 2);
    assert_eq!(restored.head_hash(), fixture.manifest.head_hash);
    let protected_head = destination_store
        .load()
        .expect("load destination anchor")
        .expect("destination anchor exists");
    assert_eq!(protected_head.record_count, fixture.manifest.record_count);
    assert_eq!(protected_head.head_hash, fixture.manifest.head_hash);

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(&fixture.bundle_directory)
                .expect("bundle metadata")
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        for name in [
            "ledger.jsonl",
            "snapshot.json",
            ".ledger.lock",
            "manifest.json",
            "manifest.json.sha256",
        ] {
            assert_eq!(
                fs::metadata(fixture.bundle_directory.join(name))
                    .expect("backup file metadata")
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
        assert_eq!(
            fs::metadata(&fixture.anchor_export)
                .expect("anchor export metadata")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
}

#[test]
fn tampered_ledger_is_rejected_before_restore_creates_destination_state() {
    let fixture = BackupFixture::new();
    OpenOptions::new()
        .append(true)
        .open(fixture.bundle_directory.join("ledger.jsonl"))
        .expect("open bundled ledger")
        .write_all(b"tamper")
        .expect("tamper bundled ledger");
    let destination_directory = fixture.temporary.path().join("tampered-restore");
    let destination_anchor = fixture.temporary.path().join("tampered-anchor.json");

    assert!(matches!(
        restore_ledger_backup(
            &fixture.bundle_directory,
            &fixture.anchor_export,
            &destination_directory,
            &destination_anchor,
        ),
        Err(LedgerBackupError::ChecksumMismatch(name)) if name == "ledger.jsonl"
    ));
    assert!(!destination_directory.exists());
    assert!(!destination_anchor.exists());
}

#[test]
fn unexpected_bundle_entry_is_rejected() {
    let fixture = BackupFixture::new();
    fs::write(fixture.bundle_directory.join("untracked"), b"unexpected")
        .expect("write unexpected entry");

    assert!(matches!(
        verify_ledger_backup(&fixture.bundle_directory, &fixture.anchor_export),
        Err(LedgerBackupError::InvalidBundle(message))
            if message == "bundle file set is not exact"
    ));
}

#[test]
fn protected_anchor_export_from_another_backup_is_rejected() {
    let fixture = BackupFixture::new();
    let other = BackupFixture::captured_at(8, 31);

    assert!(matches!(
        verify_ledger_backup(&fixture.bundle_directory, &other.anchor_export),
        Err(LedgerBackupError::ChecksumMismatch(name))
            if name == "protected anchor export"
    ));
}

#[test]
fn overlapping_output_is_rejected_before_any_backup_is_published() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let source_directory = temporary.path().join("source-ledger");
    let source_anchor = temporary.path().join("source-anchor.json");
    seed_ledger(&source_directory, &source_anchor);
    let nested_bundle = source_directory.join("backup-bundle");
    let anchor_export = temporary.path().join("backup-anchor.json");

    assert!(matches!(
        create_ledger_backup(
            &source_directory,
            &source_anchor,
            &nested_bundle,
            &anchor_export,
            Utc.with_ymd_and_hms(2026, 9, 2, 9, 0, 0)
                .single()
                .expect("valid capture time"),
        ),
        Err(LedgerBackupError::InvalidPath(message))
            if message == "backup paths must be distinct and non-overlapping"
    ));
    assert!(!nested_bundle.exists());
    assert!(!anchor_export.exists());
}

#[test]
fn output_cannot_alias_the_source_anchor_lock() {
    let temporary = tempfile::tempdir().expect("temporary directory");
    let source_directory = temporary.path().join("source-ledger");
    let source_anchor = temporary.path().join("source-anchor.json");
    seed_ledger(&source_directory, &source_anchor);
    let bundle = temporary.path().join("backup-bundle");
    let colliding_export = temporary.path().join("source-anchor.json.lock");

    assert!(matches!(
        create_ledger_backup(
            &source_directory,
            &source_anchor,
            &bundle,
            &colliding_export,
            Utc.with_ymd_and_hms(2026, 9, 2, 9, 0, 0)
                .single()
                .expect("valid capture time"),
        ),
        Err(LedgerBackupError::InvalidPath(message))
            if message == "backup paths must be distinct and non-overlapping"
    ));
    assert!(!bundle.exists());
}

#[cfg(unix)]
#[test]
fn symlinked_payload_is_rejected() {
    use std::os::unix::fs::symlink;

    let fixture = BackupFixture::new();
    let snapshot = fixture.bundle_directory.join("snapshot.json");
    fs::remove_file(&snapshot).expect("remove snapshot");
    symlink("ledger.jsonl", &snapshot).expect("replace snapshot with symlink");

    assert!(matches!(
        verify_ledger_backup(&fixture.bundle_directory, &fixture.anchor_export),
        Err(LedgerBackupError::InvalidBundle(message))
            if message.contains("not a regular file")
    ));
}

#[cfg(unix)]
#[test]
fn symlinked_restore_destination_is_rejected_without_mutating_its_target() {
    use std::os::unix::fs::symlink;

    let fixture = BackupFixture::new();
    let target = fixture.temporary.path().join("restore-target");
    fs::create_dir(&target).expect("create empty target");
    let destination = fixture.temporary.path().join("restore-alias");
    symlink(&target, &destination).expect("create destination symlink");
    let destination_anchor = fixture.temporary.path().join("restore-anchor.json");

    assert!(matches!(
        restore_ledger_backup(
            &fixture.bundle_directory,
            &fixture.anchor_export,
            &destination,
            &destination_anchor,
        ),
        Err(LedgerBackupError::InvalidPath(message)) if message.contains("path contains an alias")
    ));
    assert_eq!(fs::read_dir(&target).expect("read target").count(), 0);
    assert!(!destination_anchor.exists());
}

#[cfg(unix)]
#[test]
fn multiply_linked_payload_is_rejected() {
    let fixture = BackupFixture::new();
    let snapshot = fixture.bundle_directory.join("snapshot.json");
    fs::remove_file(&snapshot).expect("remove snapshot");
    fs::hard_link(fixture.bundle_directory.join("ledger.jsonl"), &snapshot)
        .expect("replace snapshot with hard link");

    assert!(matches!(
        verify_ledger_backup(&fixture.bundle_directory, &fixture.anchor_export),
        Err(LedgerBackupError::InvalidBundle(message))
            if message.contains("multiple hard links")
    ));
}

#[test]
fn fixture_uses_independent_source_and_backup_boundaries() {
    let fixture = BackupFixture::new();
    assert!(fixture.source_directory.exists());
    assert!(fixture.source_anchor.exists());
    assert!(!fixture
        .bundle_directory
        .starts_with(&fixture.source_directory));
    assert_ne!(fixture.source_anchor, fixture.anchor_export);
}
