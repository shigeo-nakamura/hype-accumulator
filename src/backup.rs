//! Checksummed, signer-free durable-ledger backup and clean-restore drills.
//!
//! The ledger payload bundle and its protected-head export are deliberately
//! separate outputs. Operators must retain them in independently versioned
//! storage boundaries; co-locating both removes rollback protection.

use crate::{
    ledger::{
        DurableLedger, LedgerError, ProtectedAnchorStore, ProtectedHeadAnchor, LEDGER_FILE_NAME,
        SNAPSHOT_FILE_NAME,
    },
    runtime::FileProtectedAnchorStore,
};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsStr,
    fmt::Write as _,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Component, Path, PathBuf},
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};
use thiserror::Error;

const BACKUP_SCHEMA_VERSION: u8 = 1;
const ANCHOR_EXPORT_SCHEMA_VERSION: u8 = 1;
const MANIFEST_FILE_NAME: &str = "manifest.json";
const MANIFEST_CHECKSUM_FILE_NAME: &str = "manifest.json.sha256";
const LEDGER_LOCK_FILE_NAME: &str = ".ledger.lock";
const PAYLOAD_FILE_NAMES: [&str; 3] = [LEDGER_FILE_NAME, SNAPSHOT_FILE_NAME, LEDGER_LOCK_FILE_NAME];

/// One exact file digest recorded in a backup manifest.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BackupFileDigest {
    pub sha256: String,
    pub size_bytes: u64,
}

/// Manifest for one immutable durable-ledger payload bundle.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LedgerBackupManifest {
    pub schema_version: u8,
    pub backup_id: String,
    pub captured_at: DateTime<Utc>,
    pub record_count: u64,
    pub head_hash: String,
    pub files: BTreeMap<String, BackupFileDigest>,
    pub protected_anchor_export: BackupFileDigest,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ProtectedAnchorExport {
    schema_version: u8,
    backup_id: String,
    anchor: Option<ProtectedHeadAnchor>,
}

struct VerifiedBackup {
    manifest: LedgerBackupManifest,
    anchor: Option<ProtectedHeadAnchor>,
    ledger_payload: Vec<u8>,
    snapshot_payload: Vec<u8>,
}

#[derive(Default)]
struct MemoryProtectedAnchorStore(Mutex<Option<ProtectedHeadAnchor>>);

impl MemoryProtectedAnchorStore {
    fn from_anchor(anchor: Option<ProtectedHeadAnchor>) -> Self {
        Self(Mutex::new(anchor))
    }
}

impl ProtectedAnchorStore for MemoryProtectedAnchorStore {
    fn load(&self) -> Result<Option<ProtectedHeadAnchor>, String> {
        self.0
            .lock()
            .map(|anchor| anchor.clone())
            .map_err(|_| "backup anchor lock poisoned".to_owned())
    }

    fn compare_and_swap(
        &self,
        expected: Option<&ProtectedHeadAnchor>,
        next: &ProtectedHeadAnchor,
    ) -> Result<bool, String> {
        let mut anchor = self
            .0
            .lock()
            .map_err(|_| "backup anchor lock poisoned".to_owned())?;
        if anchor.as_ref() != expected {
            return Ok(false);
        }
        *anchor = Some(next.clone());
        Ok(true)
    }
}

/// Errors from backup creation, verification, or clean restore.
#[derive(Debug, Error)]
pub enum LedgerBackupError {
    #[error("invalid backup path boundary: {0}")]
    InvalidPath(String),
    #[error("backup output already exists: {0:?}")]
    OutputExists(PathBuf),
    #[error("backup bundle is invalid: {0}")]
    InvalidBundle(String),
    #[error("backup checksum mismatch: {0}")]
    ChecksumMismatch(String),
    #[error("protected anchor store failed: {0}")]
    ProtectedAnchorStore(String),
    #[error(transparent)]
    Ledger(#[from] LedgerError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// Creates an immutable payload directory and a separate protected-head export.
///
/// The source ledger is checkpointed and the staged copy is reopened against
/// the exported protected head before either output is published.
///
/// # Errors
///
/// Returns an error for unsafe/overlapping paths, existing outputs, concurrent
/// source mutation, invalid ledger state, or durable publication failure.
pub fn create_ledger_backup(
    ledger_directory: impl AsRef<Path>,
    source_anchor_path: impl AsRef<Path>,
    bundle_directory: impl AsRef<Path>,
    anchor_export_path: impl AsRef<Path>,
    captured_at: DateTime<Utc>,
) -> Result<LedgerBackupManifest, LedgerBackupError> {
    let ledger_directory = ledger_directory.as_ref();
    let source_anchor_path = source_anchor_path.as_ref();
    let bundle_directory = bundle_directory.as_ref();
    let anchor_export_path = anchor_export_path.as_ref();
    validate_create_paths(
        ledger_directory,
        source_anchor_path,
        bundle_directory,
        anchor_export_path,
    )?;

    let source_anchor_store = Arc::new(
        FileProtectedAnchorStore::new(source_anchor_path.to_path_buf())
            .map_err(|error| LedgerBackupError::InvalidPath(error.to_string()))?,
    );
    let ledger = DurableLedger::open(ledger_directory, source_anchor_store.clone())?;
    let snapshot = ledger.checkpoint()?;
    let anchor = source_anchor_store
        .load()
        .map_err(LedgerBackupError::ProtectedAnchorStore)?;
    let ledger_payload = fs::read(ledger_directory.join(LEDGER_FILE_NAME))?;
    let snapshot_payload = fs::read(ledger_directory.join(SNAPSHOT_FILE_NAME))?;
    drop(ledger);

    let mut files = BTreeMap::new();
    files.insert(LEDGER_FILE_NAME.to_owned(), digest_bytes(&ledger_payload)?);
    files.insert(
        SNAPSHOT_FILE_NAME.to_owned(),
        digest_bytes(&snapshot_payload)?,
    );
    files.insert(LEDGER_LOCK_FILE_NAME.to_owned(), digest_bytes(&[])?);
    let backup_id = backup_id(
        captured_at,
        snapshot.record_count(),
        snapshot.head_hash(),
        &files,
    );
    let anchor_export = ProtectedAnchorExport {
        schema_version: ANCHOR_EXPORT_SCHEMA_VERSION,
        backup_id: backup_id.clone(),
        anchor,
    };
    let anchor_payload = pretty_json_bytes(&anchor_export)?;
    let manifest = LedgerBackupManifest {
        schema_version: BACKUP_SCHEMA_VERSION,
        backup_id,
        captured_at,
        record_count: snapshot.record_count(),
        head_hash: snapshot.head_hash().to_owned(),
        files,
        protected_anchor_export: digest_bytes(&anchor_payload)?,
    };
    let manifest_payload = pretty_json_bytes(&manifest)?;
    let manifest_digest = sha256_hex(&manifest_payload);
    let checksum_payload = format!("{manifest_digest}  {MANIFEST_FILE_NAME}\n").into_bytes();

    let temporary_bundle = temporary_sibling(bundle_directory, "bundle")?;
    let temporary_anchor = temporary_sibling(anchor_export_path, "anchor")?;
    let staged = (|| {
        create_private_directory(&temporary_bundle)?;
        write_private_file(&temporary_bundle.join(LEDGER_FILE_NAME), &ledger_payload)?;
        write_private_file(
            &temporary_bundle.join(SNAPSHOT_FILE_NAME),
            &snapshot_payload,
        )?;
        write_private_file(&temporary_bundle.join(LEDGER_LOCK_FILE_NAME), &[])?;
        write_private_file(
            &temporary_bundle.join(MANIFEST_FILE_NAME),
            &manifest_payload,
        )?;
        write_private_file(
            &temporary_bundle.join(MANIFEST_CHECKSUM_FILE_NAME),
            &checksum_payload,
        )?;
        sync_directory(&temporary_bundle)?;
        write_private_file(&temporary_anchor, &anchor_payload)?;
        let verified = load_verified_backup(&temporary_bundle, &temporary_anchor)?;
        if verified.manifest != manifest {
            return Err(LedgerBackupError::InvalidBundle(
                "staged manifest changed during verification".to_owned(),
            ));
        }
        Ok::<(), LedgerBackupError>(())
    })();
    if let Err(error) = staged {
        let _ = fs::remove_dir_all(&temporary_bundle);
        let _ = fs::remove_file(&temporary_anchor);
        return Err(error);
    }

    publish_directory_noreplace(&temporary_bundle, bundle_directory)?;
    if let Err(error) = publish_file_noreplace(&temporary_anchor, anchor_export_path) {
        // A two-path publication cannot be one filesystem transaction. Leave
        // the verified bundle for operator inspection instead of recursively
        // deleting a final path that could have been replaced concurrently.
        let _ = fs::remove_file(&temporary_anchor);
        return Err(error);
    }
    sync_parent(bundle_directory)?;
    sync_parent(anchor_export_path)?;
    Ok(manifest)
}

/// Verifies every checksum and replays the bundled ledger against its separate
/// protected-head export without restoring it.
///
/// # Errors
///
/// Returns an error for changed files, unexpected entries, unsafe links,
/// mismatched anchor evidence, or ledger replay failure.
pub fn verify_ledger_backup(
    bundle_directory: impl AsRef<Path>,
    anchor_export_path: impl AsRef<Path>,
) -> Result<LedgerBackupManifest, LedgerBackupError> {
    Ok(load_verified_backup(bundle_directory.as_ref(), anchor_export_path.as_ref())?.manifest)
}

/// Verifies a backup and restores it into a missing or clean directory with a
/// distinct protected-anchor scope.
///
/// # Errors
///
/// Returns an error before restore on checksum or replay failure, and otherwise
/// forwards the fail-closed clean-restore contract.
pub fn restore_ledger_backup(
    bundle_directory: impl AsRef<Path>,
    anchor_export_path: impl AsRef<Path>,
    destination_directory: impl AsRef<Path>,
    destination_anchor_path: impl AsRef<Path>,
) -> Result<LedgerBackupManifest, LedgerBackupError> {
    let bundle_directory = bundle_directory.as_ref();
    let anchor_export_path = anchor_export_path.as_ref();
    let destination_directory = destination_directory.as_ref();
    let destination_anchor_path = destination_anchor_path.as_ref();
    validate_restore_paths(
        bundle_directory,
        anchor_export_path,
        destination_directory,
        destination_anchor_path,
    )?;
    let verified = load_verified_backup(bundle_directory, anchor_export_path)?;
    let restore_source = stage_verified_restore_source(
        &verified.ledger_payload,
        &verified.snapshot_payload,
        destination_directory.parent().ok_or_else(|| {
            LedgerBackupError::InvalidPath(format!(
                "restore destination has no parent: {}",
                destination_directory.display()
            ))
        })?,
    )?;
    let source_store: Arc<dyn ProtectedAnchorStore> = Arc::new(
        MemoryProtectedAnchorStore::from_anchor(verified.anchor.clone()),
    );
    let destination_store: Arc<dyn ProtectedAnchorStore> = Arc::new(
        FileProtectedAnchorStore::new(destination_anchor_path.to_path_buf())
            .map_err(|error| LedgerBackupError::InvalidPath(error.to_string()))?,
    );
    let restored = DurableLedger::restore_clean(
        restore_source.path(),
        destination_directory,
        source_store,
        destination_store,
    )?;
    if restored.record_count()
        != usize::try_from(verified.manifest.record_count).map_err(|_| {
            LedgerBackupError::InvalidBundle("record count is not representable".to_owned())
        })?
        || restored.head_hash() != verified.manifest.head_hash
    {
        return Err(LedgerBackupError::InvalidBundle(
            "restored ledger does not match the manifest head".to_owned(),
        ));
    }
    Ok(verified.manifest)
}

fn load_verified_backup(
    bundle_directory: &Path,
    anchor_export_path: &Path,
) -> Result<VerifiedBackup, LedgerBackupError> {
    validate_existing_backup_paths(bundle_directory, anchor_export_path)?;
    validate_bundle_entries(bundle_directory)?;
    let manifest_payload = read_single_linked_file(&bundle_directory.join(MANIFEST_FILE_NAME))?;
    let checksum_payload =
        read_single_linked_file(&bundle_directory.join(MANIFEST_CHECKSUM_FILE_NAME))?;
    let expected_manifest_checksum = format!(
        "{}  {}\n",
        sha256_hex(&manifest_payload),
        MANIFEST_FILE_NAME
    );
    if checksum_payload != expected_manifest_checksum.as_bytes() {
        return Err(LedgerBackupError::ChecksumMismatch(
            MANIFEST_FILE_NAME.to_owned(),
        ));
    }
    let manifest: LedgerBackupManifest = serde_json::from_slice(&manifest_payload)?;
    if manifest.schema_version != BACKUP_SCHEMA_VERSION {
        return Err(LedgerBackupError::InvalidBundle(
            "unsupported manifest schema".to_owned(),
        ));
    }
    let expected_names = PAYLOAD_FILE_NAMES
        .into_iter()
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    if manifest.files.keys().cloned().collect::<BTreeSet<_>>() != expected_names {
        return Err(LedgerBackupError::InvalidBundle(
            "manifest payload file set is not exact".to_owned(),
        ));
    }
    for (name, expected) in &manifest.files {
        let payload = read_single_linked_file(&bundle_directory.join(name))?;
        verify_digest(name, &payload, expected)?;
    }
    let anchor_payload = read_single_linked_file(anchor_export_path)?;
    verify_digest(
        "protected anchor export",
        &anchor_payload,
        &manifest.protected_anchor_export,
    )?;
    let anchor_export: ProtectedAnchorExport = serde_json::from_slice(&anchor_payload)?;
    if anchor_export.schema_version != ANCHOR_EXPORT_SCHEMA_VERSION
        || anchor_export.backup_id != manifest.backup_id
    {
        return Err(LedgerBackupError::InvalidBundle(
            "protected anchor export is not bound to this backup".to_owned(),
        ));
    }
    if anchor_export.anchor.as_ref().is_some_and(|anchor| {
        anchor.record_count != manifest.record_count || anchor.head_hash != manifest.head_hash
    }) || (anchor_export.anchor.is_none() && manifest.record_count != 0)
    {
        return Err(LedgerBackupError::InvalidBundle(
            "protected anchor does not match the manifest head".to_owned(),
        ));
    }
    if manifest.backup_id
        != backup_id(
            manifest.captured_at,
            manifest.record_count,
            &manifest.head_hash,
            &manifest.files,
        )
    {
        return Err(LedgerBackupError::InvalidBundle(
            "backup ID does not match manifest contents".to_owned(),
        ));
    }
    let store: Arc<dyn ProtectedAnchorStore> = Arc::new(MemoryProtectedAnchorStore::from_anchor(
        anchor_export.anchor.clone(),
    ));
    let ledger = DurableLedger::open(bundle_directory, store)?;
    if ledger.record_count()
        != usize::try_from(manifest.record_count).map_err(|_| {
            LedgerBackupError::InvalidBundle("record count is not representable".to_owned())
        })?
        || ledger.head_hash() != manifest.head_hash
    {
        return Err(LedgerBackupError::InvalidBundle(
            "ledger replay does not match the manifest head".to_owned(),
        ));
    }
    drop(ledger);
    let (ledger_payload, snapshot_payload) =
        read_verified_restore_payloads(bundle_directory, &manifest)?;
    Ok(VerifiedBackup {
        manifest,
        anchor: anchor_export.anchor,
        ledger_payload,
        snapshot_payload,
    })
}

fn read_verified_restore_payloads(
    bundle_directory: &Path,
    manifest: &LedgerBackupManifest,
) -> Result<(Vec<u8>, Vec<u8>), LedgerBackupError> {
    let mut payloads = BTreeMap::new();
    for (name, expected) in &manifest.files {
        let payload = read_single_linked_file(&bundle_directory.join(name))?;
        verify_digest(name, &payload, expected)?;
        payloads.insert(name.as_str(), payload);
    }
    let ledger = payloads.remove(LEDGER_FILE_NAME).ok_or_else(|| {
        LedgerBackupError::InvalidBundle("verified ledger payload is missing".to_owned())
    })?;
    let snapshot = payloads.remove(SNAPSHOT_FILE_NAME).ok_or_else(|| {
        LedgerBackupError::InvalidBundle("verified snapshot payload is missing".to_owned())
    })?;
    Ok((ledger, snapshot))
}

fn validate_create_paths(
    ledger_directory: &Path,
    source_anchor_path: &Path,
    bundle_directory: &Path,
    anchor_export_path: &Path,
) -> Result<(), LedgerBackupError> {
    let source_anchor_lock = protected_anchor_lock_path(source_anchor_path)?;
    validate_normal_paths(&[
        ledger_directory,
        source_anchor_path,
        &source_anchor_lock,
        bundle_directory,
        anchor_export_path,
    ])?;
    require_canonical_existing(ledger_directory)?;
    require_canonical_existing_or_parent(source_anchor_path)?;
    require_canonical_existing_or_parent(&source_anchor_lock)?;
    require_canonical_parent(bundle_directory)?;
    require_canonical_parent(anchor_export_path)?;
    ensure_disjoint(&[
        ledger_directory,
        source_anchor_path,
        &source_anchor_lock,
        bundle_directory,
        anchor_export_path,
    ])?;
    for output in [bundle_directory, anchor_export_path] {
        match fs::symlink_metadata(output) {
            Ok(_) => return Err(LedgerBackupError::OutputExists(output.to_path_buf())),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn validate_restore_paths(
    bundle_directory: &Path,
    anchor_export_path: &Path,
    destination_directory: &Path,
    destination_anchor_path: &Path,
) -> Result<(), LedgerBackupError> {
    let destination_anchor_lock = protected_anchor_lock_path(destination_anchor_path)?;
    validate_normal_paths(&[
        bundle_directory,
        anchor_export_path,
        destination_directory,
        destination_anchor_path,
        &destination_anchor_lock,
    ])?;
    require_canonical_existing(bundle_directory)?;
    require_canonical_existing(anchor_export_path)?;
    require_canonical_existing_or_parent(destination_directory)?;
    require_canonical_existing_or_parent(destination_anchor_path)?;
    require_canonical_existing_or_parent(&destination_anchor_lock)?;
    ensure_disjoint(&[
        bundle_directory,
        anchor_export_path,
        destination_directory,
        destination_anchor_path,
        &destination_anchor_lock,
    ])
}

fn validate_existing_backup_paths(
    bundle_directory: &Path,
    anchor_export_path: &Path,
) -> Result<(), LedgerBackupError> {
    validate_normal_paths(&[bundle_directory, anchor_export_path])?;
    require_canonical_existing(bundle_directory)?;
    require_canonical_existing(anchor_export_path)?;
    ensure_disjoint(&[bundle_directory, anchor_export_path])
}

fn validate_normal_paths(paths: &[&Path]) -> Result<(), LedgerBackupError> {
    if paths.iter().any(|path| {
        !path.is_absolute()
            || path.file_name().is_none()
            || !path.components().all(|component| {
                matches!(
                    component,
                    Component::Prefix(_) | Component::RootDir | Component::Normal(_)
                )
            })
    }) {
        return Err(LedgerBackupError::InvalidPath(
            "paths must be normal absolute paths".to_owned(),
        ));
    }
    Ok(())
}

fn ensure_disjoint(paths: &[&Path]) -> Result<(), LedgerBackupError> {
    for (index, path) in paths.iter().enumerate() {
        for other in &paths[index + 1..] {
            if path.starts_with(other) || other.starts_with(path) {
                return Err(LedgerBackupError::InvalidPath(
                    "backup paths must be distinct and non-overlapping".to_owned(),
                ));
            }
        }
    }
    Ok(())
}

fn require_canonical_existing(path: &Path) -> Result<(), LedgerBackupError> {
    if fs::canonicalize(path)? != path {
        return Err(LedgerBackupError::InvalidPath(format!(
            "path contains an alias: {}",
            path.display()
        )));
    }
    Ok(())
}

fn require_canonical_parent(path: &Path) -> Result<(), LedgerBackupError> {
    let parent = path.parent().ok_or_else(|| {
        LedgerBackupError::InvalidPath(format!("path has no parent: {}", path.display()))
    })?;
    if fs::canonicalize(parent)? != parent {
        return Err(LedgerBackupError::InvalidPath(format!(
            "path parent contains an alias: {}",
            path.display()
        )));
    }
    Ok(())
}

fn require_canonical_existing_or_parent(path: &Path) -> Result<(), LedgerBackupError> {
    match fs::symlink_metadata(path) {
        Ok(_) => require_canonical_existing(path),
        Err(error) if error.kind() == io::ErrorKind::NotFound => require_canonical_parent(path),
        Err(error) => Err(error.into()),
    }
}

fn protected_anchor_lock_path(path: &Path) -> Result<PathBuf, LedgerBackupError> {
    let mut lock_name = path
        .file_name()
        .ok_or_else(|| {
            LedgerBackupError::InvalidPath(format!(
                "protected anchor has no file name: {}",
                path.display()
            ))
        })?
        .to_os_string();
    lock_name.push(".lock");
    Ok(path
        .parent()
        .ok_or_else(|| {
            LedgerBackupError::InvalidPath(format!(
                "protected anchor has no parent: {}",
                path.display()
            ))
        })?
        .join(lock_name))
}

fn validate_bundle_entries(bundle_directory: &Path) -> Result<(), LedgerBackupError> {
    let mut expected = PAYLOAD_FILE_NAMES
        .into_iter()
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    expected.insert(MANIFEST_FILE_NAME.to_owned());
    expected.insert(MANIFEST_CHECKSUM_FILE_NAME.to_owned());
    let mut actual = BTreeSet::new();
    for entry in fs::read_dir(bundle_directory)? {
        let entry = entry?;
        let name = entry.file_name().into_string().map_err(|_| {
            LedgerBackupError::InvalidBundle("bundle contains a non-UTF-8 entry".to_owned())
        })?;
        actual.insert(name);
    }
    if actual != expected {
        return Err(LedgerBackupError::InvalidBundle(
            "bundle file set is not exact".to_owned(),
        ));
    }
    Ok(())
}

fn read_single_linked_file(path: &Path) -> Result<Vec<u8>, LedgerBackupError> {
    let path_metadata = fs::symlink_metadata(path)?;
    if !path_metadata.file_type().is_file() {
        return Err(LedgerBackupError::InvalidBundle(format!(
            "bundle entry is not a regular file: {}",
            path.display()
        )));
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let mut file = options.open(path)?;
    let file_metadata = file.metadata()?;
    if !single_linked_file_identity(&path_metadata, &file_metadata) {
        return Err(LedgerBackupError::InvalidBundle(format!(
            "bundle entry changed or has multiple hard links: {}",
            path.display()
        )));
    }
    let mut payload = Vec::new();
    file.read_to_end(&mut payload)?;
    let final_path_metadata = fs::symlink_metadata(path)?;
    let final_file_metadata = file.metadata()?;
    if !single_linked_file_identity(&final_path_metadata, &final_file_metadata) {
        return Err(LedgerBackupError::InvalidBundle(format!(
            "bundle entry changed during read: {}",
            path.display()
        )));
    }
    Ok(payload)
}

#[cfg(unix)]
fn single_linked_file_identity(path: &fs::Metadata, file: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;

    path.file_type().is_file()
        && file.file_type().is_file()
        && path.dev() == file.dev()
        && path.ino() == file.ino()
        && path.nlink() == 1
        && file.nlink() == 1
}

#[cfg(not(unix))]
fn single_linked_file_identity(path: &fs::Metadata, file: &fs::Metadata) -> bool {
    path.file_type().is_file() && file.file_type().is_file()
}

fn verify_digest(
    name: &str,
    payload: &[u8],
    expected: &BackupFileDigest,
) -> Result<(), LedgerBackupError> {
    if &digest_bytes(payload)? != expected {
        return Err(LedgerBackupError::ChecksumMismatch(name.to_owned()));
    }
    Ok(())
}

fn digest_bytes(payload: &[u8]) -> Result<BackupFileDigest, LedgerBackupError> {
    Ok(BackupFileDigest {
        sha256: sha256_hex(payload),
        size_bytes: u64::try_from(payload.len()).map_err(|_| {
            LedgerBackupError::InvalidBundle("file size is not representable".to_owned())
        })?,
    })
}

fn sha256_hex(payload: &[u8]) -> String {
    let digest = Sha256::digest(payload);
    let mut output = String::with_capacity(64);
    for byte in digest {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

fn backup_id(
    captured_at: DateTime<Utc>,
    record_count: u64,
    head_hash: &str,
    files: &BTreeMap<String, BackupFileDigest>,
) -> String {
    let mut digest = Sha256::new();
    digest.update(b"hype-ledger-backup/v1\0");
    digest.update(
        captured_at
            .to_rfc3339_opts(SecondsFormat::Nanos, true)
            .as_bytes(),
    );
    digest.update(b"\0");
    digest.update(record_count.to_string().as_bytes());
    digest.update(b"\0");
    digest.update(head_hash.as_bytes());
    for (name, file) in files {
        digest.update(b"\0");
        digest.update(name.as_bytes());
        digest.update(b"\0");
        digest.update(file.sha256.as_bytes());
        digest.update(b"\0");
        digest.update(file.size_bytes.to_string().as_bytes());
    }
    sha256_hex(&digest.finalize())
}

fn pretty_json_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, LedgerBackupError> {
    let mut payload = serde_json::to_vec_pretty(value)?;
    payload.push(b'\n');
    Ok(payload)
}

fn temporary_sibling(path: &Path, kind: &str) -> Result<PathBuf, LedgerBackupError> {
    let parent = path.parent().ok_or_else(|| {
        LedgerBackupError::InvalidPath(format!("path has no parent: {}", path.display()))
    })?;
    let file_name = path.file_name().and_then(OsStr::to_str).ok_or_else(|| {
        LedgerBackupError::InvalidPath(format!("path has no UTF-8 name: {}", path.display()))
    })?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(io::Error::other)?
        .as_nanos();
    Ok(parent.join(format!(
        ".{file_name}.{kind}.{}.{}.tmp",
        std::process::id(),
        nonce
    )))
}

fn create_private_directory(path: &Path) -> Result<(), LedgerBackupError> {
    fs::create_dir(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn write_private_file(path: &Path, payload: &[u8]) -> Result<(), LedgerBackupError> {
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(payload)?;
    file.sync_all()?;
    Ok(())
}

fn stage_verified_restore_source(
    ledger_payload: &[u8],
    snapshot_payload: &[u8],
    parent: &Path,
) -> Result<tempfile::TempDir, LedgerBackupError> {
    let directory = tempfile::Builder::new()
        .prefix(".hype-ledger-restore.")
        .tempdir_in(parent)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))?;
    }
    write_private_file(&directory.path().join(LEDGER_FILE_NAME), ledger_payload)?;
    write_private_file(&directory.path().join(SNAPSHOT_FILE_NAME), snapshot_payload)?;
    write_private_file(&directory.path().join(LEDGER_LOCK_FILE_NAME), &[])?;
    sync_directory(directory.path())?;
    Ok(directory)
}

fn publish_file_noreplace(source: &Path, destination: &Path) -> Result<(), LedgerBackupError> {
    // Both paths are siblings by construction. A hard link therefore provides
    // an atomic no-replace publication primitive on the same filesystem.
    fs::hard_link(source, destination)?;
    fs::remove_file(source)?;
    Ok(())
}

#[cfg(any(
    target_os = "android",
    target_os = "linux",
    target_vendor = "apple",
    target_os = "redox"
))]
fn publish_directory_noreplace(source: &Path, destination: &Path) -> Result<(), LedgerBackupError> {
    use rustix::fs::{renameat_with, RenameFlags, CWD};

    renameat_with(CWD, source, CWD, destination, RenameFlags::NOREPLACE)
        .map_err(io::Error::from)?;
    Ok(())
}

#[cfg(not(any(
    target_os = "android",
    target_os = "linux",
    target_vendor = "apple",
    target_os = "redox"
)))]
fn publish_directory_noreplace(
    _source: &Path,
    _destination: &Path,
) -> Result<(), LedgerBackupError> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "atomic no-replace directory publication is unsupported on this platform",
    )
    .into())
}

fn sync_parent(path: &Path) -> Result<(), LedgerBackupError> {
    let parent = path.parent().ok_or_else(|| {
        LedgerBackupError::InvalidPath(format!("path has no parent: {}", path.display()))
    })?;
    sync_directory(parent)
}

fn sync_directory(path: &Path) -> Result<(), LedgerBackupError> {
    #[cfg(unix)]
    File::open(path)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        publish_directory_noreplace, publish_file_noreplace, stage_verified_restore_source,
        LedgerBackupError,
    };
    use std::{fs, io};

    #[test]
    fn anchor_publication_never_replaces_an_existing_file() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let first = directory.path().join("first.tmp");
        let second = directory.path().join("second.tmp");
        let published = directory.path().join("anchor.json");
        fs::write(&first, b"first").expect("write first anchor");
        fs::write(&second, b"second").expect("write second anchor");

        publish_file_noreplace(&first, &published).expect("publish first anchor");
        assert!(matches!(
            publish_file_noreplace(&second, &published),
            Err(LedgerBackupError::Io(error)) if error.kind() == io::ErrorKind::AlreadyExists
        ));
        assert_eq!(fs::read(&published).expect("read anchor"), b"first");
        assert_eq!(fs::read(&second).expect("read staged anchor"), b"second");
    }

    #[cfg(any(
        target_os = "android",
        target_os = "linux",
        target_vendor = "apple",
        target_os = "redox"
    ))]
    #[test]
    fn bundle_publication_never_replaces_an_empty_reserved_directory() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let staged = directory.path().join("staged");
        let reserved = directory.path().join("reserved");
        fs::create_dir(&staged).expect("create staged directory");
        fs::write(staged.join("payload"), b"staged").expect("write staged payload");
        fs::create_dir(&reserved).expect("create empty reservation");

        assert!(matches!(
            publish_directory_noreplace(&staged, &reserved),
            Err(LedgerBackupError::Io(error)) if error.kind() == io::ErrorKind::AlreadyExists
        ));
        assert!(staged.join("payload").exists());
        assert_eq!(
            fs::read_dir(&reserved).expect("read reservation").count(),
            0
        );
    }

    #[test]
    fn restore_source_is_materialized_from_captured_verified_bytes() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let staged = stage_verified_restore_source(
            b"verified-ledger",
            b"verified-snapshot",
            directory.path(),
        )
        .expect("stage verified restore source");
        assert_eq!(
            fs::read(staged.path().join(super::LEDGER_FILE_NAME)).expect("read staged ledger"),
            b"verified-ledger"
        );
        assert_eq!(
            fs::read(staged.path().join(super::SNAPSHOT_FILE_NAME)).expect("read staged snapshot"),
            b"verified-snapshot"
        );
    }
}
