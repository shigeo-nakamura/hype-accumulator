//! Crash-safe, signer-free recurring `DRY_RUN` orchestration.
//!
//! This module consumes already-normalized authoritative account movements,
//! persists the capital cursor and pacing state, mirrors economic facts into
//! the protected audit ledger, and publishes identifier-free status/metrics.
//! It deliberately has no order, staking, signing, or submission dependency.

use crate::{
    config::ParentFundingRoute,
    fs_safety::{normal_absolute_path, reject_linked_file, reject_multiple_links},
    ledger::{
        DurableLedger, LedgerError, LedgerEvent, LedgerEventKind, ProtectedAnchorStore,
        ProtectedHeadAnchor,
    },
    metrics::{MetricsError, MetricsSnapshot},
    monitor::HypeAttribution,
    pacing::{
        CapitalEvent, DailyDecision, DecisionInput, DecisionResult, DepositEvent, PacingError,
        PacingLimits, PacingState, UsdcMicros, WithdrawalEvent,
    },
    signal::SignalSnapshot,
    status::{AccumulatorStatus, DashboardStatus, StatusError},
    status_io::{
        write_metrics_atomic, write_private_json_atomic, write_status_atomic, StatusIoError,
    },
    workflow::WorkflowState,
};
use chrono::{DateTime, Datelike, TimeDelta, TimeZone, Utc};
use dex_connector::{HyperliquidAccountMovement, HyperliquidAccountMovementKind};
use fs2::FileExt;
use rust_decimal::{prelude::ToPrimitive, Decimal};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{self, ErrorKind, Read},
    path::{Path, PathBuf},
    sync::Arc,
};
use thiserror::Error;

const RUNTIME_CONFIG_SCHEMA_VERSION: u8 = 1;
const RUNTIME_STATE_SCHEMA_VERSION: u8 = 3;
const ADMISSION_SCHEMA_VERSION: u8 = 1;
const CYCLE_REPORT_SCHEMA_VERSION: u8 = 1;
const STATE_FILE_NAME: &str = "runtime-state.json";
const PENDING_CYCLE_FILE_NAME: &str = ".pending-runtime-cycle.json";
const COMMITTED_CYCLE_PROOF_FILE_NAME: &str = ".last-committed-runtime-cycle.json";
const LEDGER_DIRECTORY_NAME: &str = "ledger";
const RUNTIME_LOCK_FILE_NAME: &str = ".runtime.lock";
const DEFAULT_MOVEMENT_OVERLAP_MS: u64 = 86_400_000;
const DEFAULT_STUCK_AFTER_SECONDS: u64 = 3_600;
const DEFAULT_ACCOUNT_OBSERVATION_MAX_AGE_SECONDS: u64 = 60;
const DEFAULT_SIGNAL_SNAPSHOT_STALE_AFTER_SECONDS: u64 = 900;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeConfigWire {
    schema_version: u8,
    state_directory: PathBuf,
    protected_anchor_path: PathBuf,
    admission_approvals_path: PathBuf,
    signal_snapshot_path: PathBuf,
    status_path: PathBuf,
    metrics_path: PathBuf,
    cycle_report_path: PathBuf,
    movement_history_start_ms: u64,
    #[serde(default = "default_movement_overlap_ms")]
    movement_overlap_ms: u64,
    #[serde(default = "default_stuck_after_seconds")]
    stuck_after_seconds: u64,
    #[serde(default = "default_account_observation_max_age_seconds")]
    account_observation_max_age_seconds: u64,
    #[serde(default = "default_signal_snapshot_stale_after_seconds")]
    signal_snapshot_stale_after_seconds: u64,
}

const fn default_signal_snapshot_stale_after_seconds() -> u64 {
    DEFAULT_SIGNAL_SNAPSHOT_STALE_AFTER_SECONDS
}

const fn default_movement_overlap_ms() -> u64 {
    DEFAULT_MOVEMENT_OVERLAP_MS
}

const fn default_stuck_after_seconds() -> u64 {
    DEFAULT_STUCK_AFTER_SECONDS
}

const fn default_account_observation_max_age_seconds() -> u64 {
    DEFAULT_ACCOUNT_OBSERVATION_MAX_AGE_SECONDS
}

fn validate_runtime_lock(path: &Path, file: &File) -> Result<(), RuntimeError> {
    let path_metadata = fs::symlink_metadata(path).map_err(|error| {
        if error.kind() == ErrorKind::NotFound {
            RuntimeError::UnsafeRuntimeLock
        } else {
            RuntimeError::Io(error)
        }
    })?;
    let file_metadata = file.metadata()?;
    if !path_metadata.file_type().is_file() || !file_metadata.file_type().is_file() {
        return Err(RuntimeError::UnsafeRuntimeLock);
    }
    validate_runtime_lock_identity(&path_metadata, &file_metadata)
}

#[cfg(unix)]
fn validate_runtime_lock_identity(
    path_metadata: &fs::Metadata,
    file_metadata: &fs::Metadata,
) -> Result<(), RuntimeError> {
    use std::os::unix::fs::MetadataExt;

    if path_metadata.dev() != file_metadata.dev()
        || path_metadata.ino() != file_metadata.ino()
        || path_metadata.nlink() != 1
        || file_metadata.nlink() != 1
    {
        return Err(RuntimeError::UnsafeRuntimeLock);
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_runtime_lock_identity(
    _path_metadata: &fs::Metadata,
    _file_metadata: &fs::Metadata,
) -> Result<(), RuntimeError> {
    Ok(())
}

fn open_runtime_lock(path: &Path) -> Result<File, RuntimeError> {
    reject_linked_file(path).map_err(|_| RuntimeError::UnsafeRuntimeLock)?;
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(libc::O_NOFOLLOW).mode(0o600);
    }
    let file = match options.open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            let mut create_options = OpenOptions::new();
            create_options.read(true).write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;

                create_options.custom_flags(libc::O_NOFOLLOW).mode(0o600);
            }
            match create_options.open(path) {
                Ok(file) => file,
                Err(error) if error.kind() == ErrorKind::AlreadyExists => options.open(path)?,
                Err(error) => return Err(error.into()),
            }
        }
        Err(error) => return Err(error.into()),
    };
    validate_runtime_lock(path, &file)?;
    Ok(file)
}

#[cfg(unix)]
fn acquire_runtime_directory_lock(state_directory: &Path) -> Result<File, RuntimeError> {
    let directory = File::open(fs::canonicalize(state_directory)?)?;
    directory.try_lock_exclusive().map_err(|error| {
        if error.kind() == ErrorKind::WouldBlock {
            RuntimeError::AlreadyRunning
        } else {
            RuntimeError::Io(error)
        }
    })?;
    Ok(directory)
}

fn acquire_runtime_lock(state_directory: &Path) -> Result<File, RuntimeError> {
    let path = state_directory.join(RUNTIME_LOCK_FILE_NAME);
    let lock = open_runtime_lock(&path)?;
    validate_runtime_lock(&path, &lock)?;
    lock.try_lock_exclusive().map_err(|error| {
        if error.kind() == ErrorKind::WouldBlock {
            RuntimeError::AlreadyRunning
        } else {
            RuntimeError::Io(error)
        }
    })?;
    validate_runtime_lock(&path, &lock)?;
    Ok(lock)
}

/// Filesystem and history boundaries for the recurring dry-run process.
#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    state_directory: PathBuf,
    protected_anchor_path: PathBuf,
    admission_approvals_path: PathBuf,
    signal_snapshot_path: PathBuf,
    status_path: PathBuf,
    metrics_path: PathBuf,
    cycle_report_path: PathBuf,
    movement_history_start_ms: u64,
    movement_overlap_ms: u64,
    stuck_after_seconds: u64,
    account_observation_max_age_seconds: u64,
    signal_snapshot_stale_after_seconds: u64,
    parent_funding_route: Option<ParentFundingRoute>,
}

impl RuntimeConfig {
    /// Parses a fail-closed runtime document. Operational paths must be
    /// absolute so the service working directory cannot retarget state.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError::InvalidConfig`] for unsupported schemas,
    /// relative/colliding paths, or invalid history/alert bounds.
    pub fn from_toml(input: &str) -> Result<Self, RuntimeError> {
        let wire: RuntimeConfigWire = toml::from_str(input)
            .map_err(|error| RuntimeError::InvalidConfig(error.to_string()))?;
        if wire.schema_version != RUNTIME_CONFIG_SCHEMA_VERSION {
            return Err(RuntimeError::InvalidConfig(
                "unsupported runtime config schema".to_owned(),
            ));
        }
        let paths = [
            &wire.state_directory,
            &wire.protected_anchor_path,
            &wire.admission_approvals_path,
            &wire.signal_snapshot_path,
            &wire.status_path,
            &wire.metrics_path,
            &wire.cycle_report_path,
        ];
        if paths.iter().any(|path| !normal_absolute_path(path)) {
            return Err(RuntimeError::InvalidConfig(
                "runtime paths must be absolute and may not contain . or .. components".to_owned(),
            ));
        }
        if paths
            .iter()
            .skip(1)
            .any(|path| path.starts_with(&wire.state_directory))
        {
            return Err(RuntimeError::InvalidConfig(
                "configured runtime files must be outside the reserved state directory".to_owned(),
            ));
        }
        let mut unique_paths = BTreeSet::new();
        for path in &paths {
            if !unique_paths.insert((*path).clone()) {
                return Err(RuntimeError::InvalidConfig(
                    "runtime paths must be distinct".to_owned(),
                ));
            }
        }
        for (index, path) in paths.iter().enumerate() {
            for other in &paths[index + 1..] {
                if path.as_path().starts_with(other.as_path())
                    || other.as_path().starts_with(path.as_path())
                {
                    return Err(RuntimeError::InvalidConfig(
                        "runtime paths may not be ancestors of one another".to_owned(),
                    ));
                }
            }
        }
        if wire.movement_history_start_ms == 0
            || wire.movement_overlap_ms == 0
            || wire.stuck_after_seconds == 0
            || wire.account_observation_max_age_seconds == 0
            || i64::try_from(wire.account_observation_max_age_seconds).is_err()
            || wire.signal_snapshot_stale_after_seconds == 0
            || i64::try_from(wire.signal_snapshot_stale_after_seconds).is_err()
        {
            return Err(RuntimeError::InvalidConfig(
                "runtime history and alert bounds must be positive".to_owned(),
            ));
        }
        Ok(Self {
            parent_funding_route: None,
            state_directory: wire.state_directory,
            protected_anchor_path: wire.protected_anchor_path,
            admission_approvals_path: wire.admission_approvals_path,
            signal_snapshot_path: wire.signal_snapshot_path,
            status_path: wire.status_path,
            metrics_path: wire.metrics_path,
            cycle_report_path: wire.cycle_report_path,
            movement_history_start_ms: wire.movement_history_start_ms,
            movement_overlap_ms: wire.movement_overlap_ms,
            stuck_after_seconds: wire.stuck_after_seconds,
            account_observation_max_age_seconds: wire.account_observation_max_age_seconds,
            signal_snapshot_stale_after_seconds: wire.signal_snapshot_stale_after_seconds,
        })
    }

    /// Binds funding recognition to startup-resolved policy identities.
    #[must_use]
    pub fn with_parent_funding_route(mut self, route: Option<ParentFundingRoute>) -> Self {
        self.parent_funding_route = route;
        self
    }

    #[must_use]
    pub fn admission_approvals_path(&self) -> &Path {
        &self.admission_approvals_path
    }

    #[must_use]
    pub fn signal_snapshot_path(&self) -> &Path {
        &self.signal_snapshot_path
    }

    /// Core freshness limit the snapshot producer binds to the decision boundary.
    #[must_use]
    pub const fn signal_snapshot_stale_after_seconds(&self) -> u64 {
        self.signal_snapshot_stale_after_seconds
    }

    /// Where the identifier-free public dashboard status document is
    /// written each cycle. Used by the async CLI caller to best-effort
    /// mirror the just-written file to S3 without threading async I/O
    /// through this otherwise synchronous crash-safe runtime.
    #[must_use]
    pub fn status_path(&self) -> &Path {
        &self.status_path
    }

    fn configured_file_paths(&self) -> [&Path; 6] {
        [
            &self.protected_anchor_path,
            &self.admission_approvals_path,
            &self.signal_snapshot_path,
            &self.status_path,
            &self.metrics_path,
            &self.cycle_report_path,
        ]
    }
}

/// Separately reviewed confirmation and admission evidence for one deposit.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DepositAdmissionApproval {
    /// Optional operator-approved total admission ceiling, in integer USDC micros.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_admitted_usdc: Option<UsdcMicros>,
    pub event_id: String,
    pub confirmed_at: DateTime<Utc>,
    pub confirmation_count: u32,
    pub approved_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AdmissionApprovalsWire {
    schema_version: u8,
    approvals: Vec<DepositAdmissionApproval>,
}

/// Canonical admission artifact keyed by authoritative movement event ID.
#[derive(Clone, Debug, Default)]
pub struct AdmissionApprovals(BTreeMap<String, DepositAdmissionApproval>);

impl AdmissionApprovals {
    /// Parses a closed admission artifact and rejects duplicate or malformed
    /// event IDs.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError::InvalidAdmissionArtifact`] for unsupported
    /// schemas, duplicates, blank IDs, or impossible timestamps/counts.
    pub fn from_json(input: &str) -> Result<Self, RuntimeError> {
        let wire: AdmissionApprovalsWire = serde_json::from_str(input)
            .map_err(|error| RuntimeError::InvalidAdmissionArtifact(error.to_string()))?;
        if wire.schema_version != ADMISSION_SCHEMA_VERSION {
            return Err(RuntimeError::InvalidAdmissionArtifact(
                "unsupported admission artifact schema".to_owned(),
            ));
        }
        let mut approvals = BTreeMap::new();
        for approval in wire.approvals {
            if approval.event_id.trim() != approval.event_id
                || approval.event_id.is_empty()
                || approval.confirmation_count == 0
                || approval.max_admitted_usdc.is_some_and(UsdcMicros::is_zero)
                || approval.confirmed_at > approval.approved_at
            {
                return Err(RuntimeError::InvalidAdmissionArtifact(
                    "malformed deposit admission approval".to_owned(),
                ));
            }
            if approvals
                .insert(approval.event_id.clone(), approval)
                .is_some()
            {
                return Err(RuntimeError::InvalidAdmissionArtifact(
                    "duplicate deposit admission approval".to_owned(),
                ));
            }
        }
        Ok(Self(approvals))
    }

    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    fn get(&self, event_id: &str) -> Option<&DepositAdmissionApproval> {
        self.0.get(event_id)
    }
}

/// Atomic file-backed protected-head store. Deployments must place this path
/// outside the mutable ledger directory and protect it with a separate
/// filesystem/IAM boundary.
#[derive(Debug)]
pub struct FileProtectedAnchorStore {
    path: PathBuf,
    lock_path: PathBuf,
}

impl FileProtectedAnchorStore {
    /// Constructs a store without reading or creating the anchor.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError::InvalidConfig`] if the path does not name an
    /// absolute file.
    pub fn new(path: impl Into<PathBuf>) -> Result<Self, RuntimeError> {
        let path = path.into();
        if !normal_absolute_path(&path) {
            return Err(RuntimeError::InvalidConfig(
                "protected anchor path must name a normal absolute file".to_owned(),
            ));
        }
        let mut lock_name = path
            .file_name()
            .ok_or_else(|| RuntimeError::InvalidConfig("anchor has no file name".to_owned()))?
            .to_os_string();
        lock_name.push(".lock");
        let lock_path = path
            .parent()
            .ok_or_else(|| RuntimeError::InvalidConfig("anchor has no parent".to_owned()))?
            .join(lock_name);
        Ok(Self { path, lock_path })
    }

    fn read(&self) -> Result<Option<ProtectedHeadAnchor>, String> {
        reject_linked_file(&self.path)
            .map_err(|error| format!("unsafe protected anchor: {error}"))?;
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.custom_flags(libc::O_NOFOLLOW);
        }
        match options.open(&self.path) {
            Ok(mut file) => {
                reject_multiple_links(&file)
                    .map_err(|error| format!("unsafe protected anchor: {error}"))?;
                let mut payload = String::new();
                file.read_to_string(&mut payload)
                    .map_err(|error| format!("protected anchor read failed: {error}"))?;
                serde_json::from_str(&payload)
                    .map(Some)
                    .map_err(|error| format!("invalid protected anchor: {error}"))
            }
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
            Err(error) => Err(format!("protected anchor read failed: {error}")),
        }
    }

    fn lock(&self) -> Result<File, String> {
        let parent = self
            .lock_path
            .parent()
            .ok_or_else(|| "protected anchor lock has no parent".to_owned())?;
        fs::create_dir_all(parent)
            .map_err(|error| format!("protected anchor directory create failed: {error}"))?;
        reject_linked_file(&self.lock_path)
            .map_err(|error| format!("unsafe protected anchor lock: {error}"))?;
        let mut options = OpenOptions::new();
        options.create(true).read(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
        }
        let lock = options
            .open(&self.lock_path)
            .map_err(|error| format!("protected anchor lock open failed: {error}"))?;
        reject_multiple_links(&lock)
            .map_err(|error| format!("unsafe protected anchor lock: {error}"))?;
        lock.lock_exclusive()
            .map_err(|error| format!("protected anchor lock failed: {error}"))?;
        Ok(lock)
    }
}

impl ProtectedAnchorStore for FileProtectedAnchorStore {
    fn load(&self) -> Result<Option<ProtectedHeadAnchor>, String> {
        self.read()
    }

    fn compare_and_swap(
        &self,
        expected: Option<&ProtectedHeadAnchor>,
        next: &ProtectedHeadAnchor,
    ) -> Result<bool, String> {
        let lock = self.lock()?;
        let current = self.read()?;
        if current.as_ref() != expected {
            FileExt::unlock(&lock)
                .map_err(|error| format!("protected anchor unlock failed: {error}"))?;
            return Ok(false);
        }
        write_private_json_atomic(&self.path, next)
            .map_err(|error| format!("protected anchor write failed: {error}"))?;
        FileExt::unlock(&lock)
            .map_err(|error| format!("protected anchor unlock failed: {error}"))?;
        Ok(true)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct RuntimeState {
    schema_version: u8,
    movement_history_start_ms: u64,
    last_complete_scan_end_ms: Option<u64>,
    pacing: PacingState,
    decision_evidence: BTreeMap<chrono::NaiveDate, RuntimeDecisionEvidence>,
    api_errors_total: u64,
    stale_signal_events_total: u64,
    dry_run_actions_total: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_committed_cycle_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parent_funding_route: Option<ParentFundingRoute>,
    /// Workflow-journal namespace every live decision of this runtime is
    /// prepared into; write-once, part of the committed cycle hash.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    live_history_directory: Option<PathBuf>,
    /// Decision ID → journal path recorded by `prepare` immediately BEFORE
    /// it creates that journal (hash-chained). A decision listed here may
    /// have a submit-capable journal even if the directory no longer shows
    /// one, so absence from the directory can never release it.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    live_journal_intents: BTreeMap<String, PathBuf>,
    /// Decision ID → the HYPE that decision's authenticated workflow proves
    /// this account was credited (bot-strategy#929). Append-only and
    /// hash-chained with the settlement that produced it: this is the
    /// cross-workflow ownership record that distinguishes HYPE this bot
    /// acquired from HYPE the account holds for any other reason, and it is
    /// the only thing the read-only observer is allowed to attribute.
    ///
    /// Written by the same commit as the USDC settlement it belongs to, so
    /// inventory can never drift from capital: there is no second write to
    /// forget, and a conflicting replay fails closed.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    hype_acquisitions: BTreeMap<String, RuntimeHypeAcquisition>,
}

/// One settled decision's HYPE acquisition, as durably recorded beside the
/// capital settlement that produced it.
///
/// `credited_hype_atoms` is what the account was actually credited — net of
/// any fee the venue charged in HYPE (bot-strategy#998) — never the quantity
/// the venue matched.
///
/// Deliberately records only the acquisition, never a later outflow. HYPE
/// leaving the account again is a separate, later event: a workflow's own
/// `residual_consumed_by_movements_hype` is only fixed at
/// `StakingEligibilityRecorded`, which can follow the settlement that wrote
/// this row, and a sale after the workflow completed is not attributable to
/// it at all. Snapshotting it here would either go stale or turn an honest
/// later reconcile into a permanent replay conflict. Netting outflows needs
/// the movement ledger keyed by `movement_id` in bot-strategy#929's
/// remaining scope; until it exists an unexplained shortfall surfaces as the
/// divergence health failure and halts the cycle, which is the fail-closed
/// answer rather than a silently wrong number.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct RuntimeHypeAcquisition {
    workflow_id: String,
    journal: PathBuf,
    credited_hype_atoms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_fill_at: Option<DateTime<Utc>>,
    recorded_at: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct RuntimeDecisionEvidence {
    signal_available: bool,
    boundary_balance_available: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PendingRuntimeCycleBody {
    schema_version: u8,
    observed_at: DateTime<Utc>,
    state: RuntimeState,
    ledger_events: Vec<LedgerEvent>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PendingRuntimeCycle {
    body: PendingRuntimeCycleBody,
    cycle_hash: String,
}

impl PendingRuntimeCycle {
    fn new(
        observed_at: DateTime<Utc>,
        state: RuntimeState,
        ledger_events: Vec<LedgerEvent>,
    ) -> Result<Self, RuntimeError> {
        let body = PendingRuntimeCycleBody {
            schema_version: RUNTIME_STATE_SCHEMA_VERSION,
            observed_at,
            state,
            ledger_events,
        };
        let cycle_hash = canonical_sha256(&body)?;
        Ok(Self { body, cycle_hash })
    }

    fn validate(&self, history_start_ms: u64) -> Result<(), RuntimeError> {
        if self.body.schema_version != RUNTIME_STATE_SCHEMA_VERSION
            || self.body.state.schema_version != RUNTIME_STATE_SCHEMA_VERSION
            || self.body.state.movement_history_start_ms != history_start_ms
            || canonical_sha256(&self.body)? != self.cycle_hash
        {
            return Err(RuntimeError::CorruptPendingCycle);
        }
        ensure_decision_evidence(&self.body.state)
    }
}

impl RuntimeState {
    fn new(movement_history_start_ms: u64) -> Self {
        Self {
            schema_version: RUNTIME_STATE_SCHEMA_VERSION,
            movement_history_start_ms,
            last_complete_scan_end_ms: None,
            pacing: PacingState::default(),
            decision_evidence: BTreeMap::new(),
            api_errors_total: 0,
            stale_signal_events_total: 0,
            dry_run_actions_total: 0,
            last_committed_cycle_hash: None,
            parent_funding_route: None,
            live_history_directory: None,
            live_journal_intents: BTreeMap::new(),
            hype_acquisitions: BTreeMap::new(),
        }
    }
}

/// The HYPE side of a live settlement: what, if anything, this account was
/// credited by the decision being settled.
///
/// Passed to [`SignerFreeRuntime::settle_live_decision`] rather than recorded
/// by a separate call, so a settlement can never move capital without also
/// accounting for the inventory it bought (bot-strategy#929). The two
/// variants are the two ways a live decision can reach settlement, and the
/// runtime checks each against its own record rather than trusting the
/// caller's choice.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LiveHypeAcquisition {
    /// Settled from an authenticated workflow journal.
    Workflow {
        /// The workflow whose terminal, protected-head-verified state these
        /// figures were read from.
        workflow_id: String,
        /// The journal that workflow lives in. Must be the journal this
        /// runtime itself recorded as this decision's intent — a settlement
        /// quoting any other journal is refused.
        journal: PathBuf,
        /// HYPE atoms credited to the account, net of a HYPE-denominated fee
        /// (bot-strategy#998). Never the matched quantity.
        credited_hype_atoms: u64,
        /// When the last fill backing those atoms executed, if any fill did.
        last_fill_at: Option<DateTime<Utc>>,
    },
    /// Released capital for a decision this runtime never bound a journal to:
    /// no order can exist, so no HYPE can have been acquired. Refused unless
    /// the runtime agrees there is no journal intent and the settlement moves
    /// no cash.
    NoWorkflow,
}

impl LiveHypeAcquisition {
    /// The evidence a finalized workflow's state proves, shaped for
    /// settlement or backfill — the one place the field mapping lives, so
    /// a row written at settlement and one written by the backfill can never
    /// describe different quantities under the same names.
    ///
    /// Callers own the gates that differ between them (venue-vs-journal at
    /// settlement, decision-vs-journal at backfill); this owns the one they
    /// share: the order must have reached durable finality
    /// (`WorkflowStage::is_order_finalized`) — never `ManualReview`, and
    /// never a stage at which a restored journal might still be missing its
    /// finalization.
    ///
    /// `credited_hype_atoms` is the journal's `purchased_hype`. An event
    /// written before bot-strategy#998 carried no credited quantity and
    /// replays it as the matched one — indistinguishable in the journal from
    /// a modern fee-free fill, so no reader can refuse it. Settlement guards
    /// this with the venue's own credited figure; a venue-free reader cannot,
    /// and a pre-#998 journal whose fee was charged in HYPE would then be
    /// attributed the fee too. That surfaces as attribution above holdings —
    /// the divergence halt — rather than as a silently accepted number.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError::InvalidHypeAcquisition`] when the workflow is
    /// not finalized.
    pub fn from_finalized_workflow(
        state: &WorkflowState,
        journal: &Path,
    ) -> Result<Self, RuntimeError> {
        if !state.stage().is_order_finalized() {
            return Err(RuntimeError::InvalidHypeAcquisition(format!(
                "workflow {} in {} has not reached durable finality (stage={:?}); its figures \
                 may still change and must not be recorded as inventory",
                state.workflow_id(),
                journal.display(),
                state.stage()
            )));
        }
        Ok(Self::Workflow {
            workflow_id: state.workflow_id().to_owned(),
            journal: journal.to_path_buf(),
            credited_hype_atoms: state.purchased_hype().as_atoms(),
            last_fill_at: state.last_fill_at(),
        })
    }
}

/// HYPE this runtime's own settled history proves the workflow acquired,
/// plus how much of that history is still missing its evidence.
///
/// A consumer must treat a nonzero `settled_purchases_without_evidence` as
/// "attribution incomplete" and exclude account holdings entirely, never as
/// "the rest is zero": a purchase settled by a build older than
/// bot-strategy#929 has no acquisition row until it is backfilled, and
/// reporting the partial sum as if it were the whole would understate
/// bot-owned inventory without saying so.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AttributedHype {
    pub credited_hype_atoms: u64,
    pub last_fill_at: Option<DateTime<Utc>>,
    pub settled_purchases_with_evidence: usize,
    pub settled_purchases_without_evidence: usize,
}

impl AttributedHype {
    /// Whether every settled purchase has its acquisition evidence.
    #[must_use]
    pub const fn is_complete(&self) -> bool {
        self.settled_purchases_without_evidence == 0
    }

    /// The credited HYPE as the fractional quantity a dashboard reports.
    #[must_use]
    pub fn credited_hype(&self) -> f64 {
        crate::hype_asset::atoms_to_hype_f64(self.credited_hype_atoms)
    }

    /// The attribution an observer may report for this account.
    ///
    /// The single place the "incomplete evidence excludes holdings" rule
    /// lives, so no observer can accidentally publish a partial sum as if it
    /// were the whole (bot-strategy#929).
    #[must_use]
    pub fn to_attribution(&self) -> HypeAttribution {
        if self.is_complete() {
            HypeAttribution::Reconciled {
                hype: self.credited_hype(),
                last_trade_at: self.last_fill_at,
            }
        } else {
            HypeAttribution::Unavailable
        }
    }
}

/// How a cycle treats a NEW planned purchase decision.
///
/// The signer-free runtime never constructs an economic action itself in
/// either mode. The difference is only what happens to the capital a new
/// planned decision commits:
///
/// * [`DecisionMode::DryRun`] — the recurring, halted `DRY_RUN` cycle. A new
///   planned decision is immediately settled at zero fill / zero debit in
///   the same cycle (`decision:<id>:dry-run-settlement`), so no commitment
///   ever outlives the cycle and no later settlement is expected.
/// * [`DecisionMode::Live`] — the live-probe `prepare` path. A new planned
///   decision is committed and left **unsettled**; the execution workflow
///   binds it (`DecisionBinding::from_pacing_decision` refuses a settled
///   decision), and the caller must later settle it from the reconciled
///   terminal fill via [`SignerFreeRuntime::settle_live_decision`]. Until
///   that happens every later decision day fails closed as
///   `PriorDecisionUnsettled`, so a forgotten settlement can never be
///   silently compounded by a second purchase.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DecisionMode {
    DryRun,
    /// `history_directory` is the (canonical) workflow-journal namespace the
    /// caller will create this decision's journal in. The runtime records it
    /// in its hash-chained state on the first live cycle and refuses any
    /// later live cycle naming a different directory, so `release` can
    /// prove "no journal in *this* directory binds the decision" is the
    /// same statement as "no journal anywhere binds it".
    Live {
        history_directory: PathBuf,
    },
}

/// One closed movement-history and account-observation cycle.
pub struct RuntimeCycleInput<'a> {
    pub observed_at: DateTime<Utc>,
    pub scan_start_ms: u64,
    pub scan_end_ms: u64,
    pub movements: &'a [HyperliquidAccountMovement],
    pub approvals: &'a AdmissionApprovals,
    pub signal: Option<&'a SignalSnapshot>,
    pub accumulator: AccumulatorStatus,
    pub capital_history_complete: bool,
    pub manual_pause: bool,
    pub api_errors: u64,
    pub decision_mode: DecisionMode,
}

/// One tranche allocation of a live planned decision, as bound into the
/// execution workflow (`DecisionBinding::capital_commitments`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiveDecisionAllocation {
    pub tranche_id: String,
    pub planned_usdc: UsdcMicros,
    pub committed_usdc: UsdcMicros,
}

/// The complete identity of the pacing decision an execution workflow was
/// bound to. Decision IDs are only date-derived (`fixed-dca:<date>`), so a
/// settlement must prove it is talking to the runtime that produced the
/// bound decision: every field here is copied from the workflow's durable
/// `DecisionBinding` and compared against the runtime's own decision before
/// any capital moves. A different runtime (or a rewritten one) fails closed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LiveDecisionIdentity {
    pub decision_id: String,
    pub decision_date: chrono::NaiveDate,
    pub decided_at: DateTime<Utc>,
    pub capital_snapshot_hash: String,
    pub input_snapshot_hash: String,
    pub planned_usdc: UsdcMicros,
    pub committed_usdc: UsdcMicros,
    pub allocations: Vec<LiveDecisionAllocation>,
}

impl LiveDecisionIdentity {
    /// Identity of a pacing decision as the runtime itself holds it.
    #[must_use]
    pub fn of(decision: &DailyDecision) -> Self {
        let mut allocations = decision
            .allocations
            .iter()
            .map(|allocation| LiveDecisionAllocation {
                tranche_id: allocation.tranche_id.clone(),
                planned_usdc: allocation.planned_usdc,
                committed_usdc: allocation.committed_usdc,
            })
            .collect::<Vec<_>>();
        allocations.sort_by(|left, right| left.tranche_id.cmp(&right.tranche_id));
        Self {
            decision_id: decision.decision_id.clone(),
            decision_date: decision.decision_date,
            decided_at: decision.decided_at,
            capital_snapshot_hash: decision.capital_snapshot_hash.clone(),
            input_snapshot_hash: decision.input_snapshot_hash.clone(),
            planned_usdc: decision.planned_usdc,
            committed_usdc: decision.committed_usdc,
            allocations,
        }
    }

    fn mismatch_against(&self, decision: &DailyDecision) -> Option<&'static str> {
        let expected = Self::of(decision);
        if self.decision_id != expected.decision_id {
            Some("decision_id")
        } else if self.decision_date != expected.decision_date {
            Some("decision_date")
        } else if self.decided_at != expected.decided_at {
            Some("decided_at")
        } else if self.capital_snapshot_hash != expected.capital_snapshot_hash {
            Some("capital_snapshot_hash")
        } else if self.input_snapshot_hash != expected.input_snapshot_hash {
            Some("input_snapshot_hash")
        } else if self.planned_usdc != expected.planned_usdc {
            Some("planned_usdc")
        } else if self.committed_usdc != expected.committed_usdc {
            Some("committed_usdc")
        } else if self.allocations != expected.allocations {
            Some("allocations")
        } else {
            None
        }
    }
}

/// Outcome of [`SignerFreeRuntime::settle_live_decision`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LiveSettlementOutcome {
    /// This call durably settled the decision and committed the ledger cycle.
    Settled,
    /// The decision was already settled with exactly these amounts; nothing
    /// was written (idempotent replay after a crash or a repeated reconcile).
    AlreadySettled,
}

/// Private, durable cycle evidence. Public status/metrics remain identifier-free.
#[derive(Clone, Debug, Serialize)]
#[allow(clippy::struct_excessive_bools)]
pub struct RuntimeCycleReport {
    schema_version: u8,
    observed_at: DateTime<Utc>,
    scan_start_ms: u64,
    scan_end_ms: u64,
    capital_history_complete: bool,
    normalized_movement_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    decision: Option<DailyDecision>,
    new_decision: bool,
    economic_action_suppressed: bool,
    signed_action_created: bool,
    signal_available: bool,
    boundary_balance_available: bool,
}

impl RuntimeCycleReport {
    #[must_use]
    pub const fn decision(&self) -> Option<&DailyDecision> {
        self.decision.as_ref()
    }

    #[must_use]
    pub const fn is_new_decision(&self) -> bool {
        self.new_decision
    }
}

/// Exclusively locked, persistent signer-free runtime instance.
pub struct SignerFreeRuntime {
    config: RuntimeConfig,
    limits: PacingLimits,
    state: RuntimeState,
    ledger: DurableLedger,
    process_started_at: DateTime<Utc>,
    lock: File,
    #[cfg(unix)]
    _directory_lock: File,
}

impl SignerFreeRuntime {
    /// Opens and validates runtime state plus its protected ledger while taking
    /// a non-blocking single-process lock.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] for concurrent execution, corrupt state,
    /// incompatible history bounds, or protected-ledger verification failure.
    pub fn open(config: RuntimeConfig, limits: PacingLimits) -> Result<Self, RuntimeError> {
        limits.validate()?;
        fs::create_dir_all(&config.state_directory)?;
        ensure_configured_files_outside_state(&config)?;
        ensure_protected_boundary(&config)?;
        #[cfg(unix)]
        let directory_lock = acquire_runtime_directory_lock(&config.state_directory)?;
        let lock = acquire_runtime_lock(&config.state_directory)?;
        let state_path = config.state_directory.join(STATE_FILE_NAME);
        let mut state = load_runtime_state(&state_path, config.movement_history_start_ms)?;
        state.pacing.validate_for_limits(&limits)?;
        ensure_decision_evidence(&state)?;
        let anchor_store: Arc<dyn ProtectedAnchorStore> = Arc::new(FileProtectedAnchorStore::new(
            config.protected_anchor_path.clone(),
        )?);
        let mut ledger = DurableLedger::open(
            config.state_directory.join(LEDGER_DIRECTORY_NAME),
            anchor_store,
        )?;
        recover_pending_cycle(&config, &limits, &mut ledger, &mut state)?;
        ensure_capital_totals_match(&state.pacing, ledger.state())?;
        ensure_runtime_head_matches(&state, ledger.state())?;
        ensure_runtime_state_authenticated(&config, &state, ledger.state())?;
        if state.last_committed_cycle_hash.is_some()
            && state.parent_funding_route != config.parent_funding_route
        {
            return Err(RuntimeError::InvalidConfig(
                "funding identity changed; use an explicitly migrated separate runtime ledger"
                    .into(),
            ));
        }
        Ok(Self {
            config,
            limits,
            state,
            ledger,
            process_started_at: Utc::now(),
            lock,
            #[cfg(unix)]
            _directory_lock: directory_lock,
        })
    }

    /// The workflow-journal namespace this runtime's live decisions were
    /// prepared into (recorded, hash-chained, on the first live cycle);
    /// `None` for a runtime that has only ever run `DRY_RUN` cycles.
    #[must_use]
    pub fn live_history_directory(&self) -> Option<&Path> {
        self.state.live_history_directory.as_deref()
    }

    /// The journal path `prepare` durably declared for `decision_id` before
    /// creating it, if any. `Some` means a journal may exist (or may have
    /// existed and been lost with its directory), so the decision can only
    /// be resolved through that journal, never by absence.
    #[must_use]
    pub fn live_journal_intent(&self, decision_id: &str) -> Option<&Path> {
        self.state
            .live_journal_intents
            .get(decision_id)
            .map(PathBuf::as_path)
    }

    /// Every journal intent this runtime has recorded (decision ID → journal
    /// path), i.e. the manifest of journals that must exist for its history
    /// to be considered intact.
    #[must_use]
    pub const fn live_journal_intents(&self) -> &BTreeMap<String, PathBuf> {
        &self.state.live_journal_intents
    }

    /// HYPE this runtime's own settled decisions prove the workflow
    /// acquired, together with how many settled purchases are still missing
    /// their acquisition evidence (bot-strategy#929).
    ///
    /// Read-only and signer-free: this is what the observer attributes to the
    /// bot, and the only figure allowed to be reported as bot-owned HYPE. A
    /// nonzero `settled_purchases_without_evidence` means the record is
    /// incomplete and the caller must exclude account holdings rather than
    /// report the partial sum.
    #[must_use]
    pub fn attributed_hype(&self) -> AttributedHype {
        let mut aggregate = AttributedHype::default();
        for decision in self.settled_purchases() {
            match self.state.hype_acquisitions.get(&decision.decision_id) {
                // An unrepresentable total is unusable evidence, not a
                // capped one: counting it as missing withholds the whole
                // attribution instead of publishing a saturated number.
                Some(row) => match aggregate
                    .credited_hype_atoms
                    .checked_add(row.credited_hype_atoms)
                {
                    Some(total) => {
                        aggregate.settled_purchases_with_evidence += 1;
                        aggregate.credited_hype_atoms = total;
                        aggregate.last_fill_at = match (aggregate.last_fill_at, row.last_fill_at) {
                            (Some(current), Some(candidate)) => Some(current.max(candidate)),
                            (current, candidate) => current.or(candidate),
                        };
                    }
                    None => aggregate.settled_purchases_without_evidence += 1,
                },
                None => aggregate.settled_purchases_without_evidence += 1,
            }
        }
        aggregate
    }

    /// Every settled purchase whose HYPE acquisition was never recorded.
    ///
    /// These are the decisions [`Self::attributed_hype`] counts as missing
    /// evidence — settled by a build older than bot-strategy#929 — and the
    /// exact set a backfill must supply before attribution can be reported
    /// at all.
    #[must_use]
    pub fn settled_purchases_without_acquisition(&self) -> Vec<DailyDecision> {
        self.settled_purchases()
            .filter(|decision| {
                !self
                    .state
                    .hype_acquisitions
                    .contains_key(&decision.decision_id)
            })
            .cloned()
            .collect()
    }

    /// The decisions that can own HYPE — see
    /// [`DailyDecision::is_settled_purchase`].
    fn settled_purchases(&self) -> impl Iterator<Item = &DailyDecision> {
        self.state
            .pacing
            .decisions()
            .values()
            .filter(|decision| decision.is_settled_purchase())
    }

    /// The runtime's own identity view of `decision_id`, if it exists.
    #[must_use]
    pub fn decision_identity(&self, decision_id: &str) -> Option<LiveDecisionIdentity> {
        self.state
            .pacing
            .decisions()
            .values()
            .find(|decision| decision.decision_id == decision_id)
            .map(LiveDecisionIdentity::of)
    }

    /// Durably records, BEFORE the journal is created, that the live
    /// decision `identity` is about to be bound by the workflow journal at
    /// `journal_path`. Committed through the same pending/commit cycle
    /// machinery (hash-chained, protected-anchor-backed), so a journal that
    /// later disappears with its directory still leaves the runtime knowing
    /// the decision was bound. Idempotent for the same path (a retried
    /// `prepare` after a crash between this record and the journal write);
    /// a different path, a settled decision, or an identity mismatch fails
    /// closed without touching state.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] for an unknown/settled decision, an identity
    /// mismatch, a conflicting path, or ledger/persistence failures.
    pub fn record_live_journal_intent(
        &mut self,
        identity: &LiveDecisionIdentity,
        journal_path: &Path,
        recorded_at: DateTime<Utc>,
    ) -> Result<LiveSettlementOutcome, RuntimeError> {
        self.ensure_runtime_lock_current()?;
        let decision_id = identity.decision_id.as_str();
        let decision = self
            .state
            .pacing
            .decisions()
            .values()
            .find(|decision| decision.decision_id == decision_id)
            .cloned()
            .ok_or(PacingError::UnknownDecision)?;
        if let Some(field) = identity.mismatch_against(&decision) {
            return Err(RuntimeError::LiveDecisionMismatch(field));
        }
        if decision.settled || decision.planned_usdc.is_zero() {
            return Err(RuntimeError::InvalidCycle(
                "journal intent requires an unsettled planned decision".to_owned(),
            ));
        }
        match self.state.live_journal_intents.get(decision_id) {
            Some(recorded) if recorded == journal_path => {
                return Ok(LiveSettlementOutcome::AlreadySettled);
            }
            Some(recorded) => {
                return Err(RuntimeError::LiveHistoryDirectoryMismatch(format!(
                    "decision {decision_id} already declared journal {} but {} was requested",
                    recorded.display(),
                    journal_path.display()
                )));
            }
            None => {}
        }
        // One journal file belongs to exactly one decision for the life of
        // this runtime: reusing a filename a previous decision declared would
        // let a new journal stand in for lost history.
        if let Some((owner, _)) = self
            .state
            .live_journal_intents
            .iter()
            .find(|(_, recorded)| same_journal_path(recorded, journal_path))
        {
            return Err(RuntimeError::LiveHistoryDirectoryMismatch(format!(
                "journal {} is already declared by decision {owner}; a journal path is never \
                 reused across decisions",
                journal_path.display()
            )));
        }
        if recorded_at < decision.decided_at {
            return Err(RuntimeError::InvalidCycle(
                "journal intent predates its decision".to_owned(),
            ));
        }
        let mut next_state = self.state.clone();
        next_state
            .live_journal_intents
            .insert(decision_id.to_owned(), journal_path.to_path_buf());
        self.commit_state_change(recorded_at, next_state, Vec::new())?;
        Ok(LiveSettlementOutcome::Settled)
    }

    /// Planned decisions that a live cycle committed and nothing has settled
    /// yet. Empty for a runtime that has only ever run `DRY_RUN` cycles.
    #[must_use]
    pub fn unsettled_planned_decisions(&self) -> Vec<DailyDecision> {
        self.state
            .pacing
            .decisions()
            .values()
            .filter(|decision| !decision.settled && !decision.planned_usdc.is_zero())
            .cloned()
            .collect()
    }

    /// Records the HYPE acquisition of a decision that was already settled
    /// without one (bot-strategy#929).
    ///
    /// Exists because settlements committed before this runtime recorded
    /// inventory left their purchases unevidenced, and attribution is
    /// withheld entirely while any such purchase remains — see
    /// [`Self::attributed_hype`]. It changes no capital: the decision is
    /// already settled and its cash figures are untouched.
    ///
    /// Idempotent by content. Re-running it with the same evidence writes
    /// nothing; different evidence for the same decision fails closed rather
    /// than replacing a record the caller cannot prove is wrong.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError::LiveDecisionMismatch`] when `identity` is not
    /// this runtime's view of the decision, [`RuntimeError::InvalidCycle`]
    /// when the decision is not a settled purchase (nothing unsettled, and
    /// nothing that never bought anything, has inventory to backfill),
    /// [`RuntimeError::InvalidHypeAcquisition`] when the evidence does not
    /// match the journal this runtime bound the decision to,
    /// [`RuntimeError::HypeAcquisitionConflict`] when a different record
    /// already exists, and the usual persistence errors.
    pub fn backfill_hype_acquisition(
        &mut self,
        identity: &LiveDecisionIdentity,
        acquisition: &LiveHypeAcquisition,
        recorded_at: DateTime<Utc>,
    ) -> Result<LiveSettlementOutcome, RuntimeError> {
        self.ensure_runtime_lock_current()?;
        let decision_id = identity.decision_id.as_str();
        let decision = self
            .state
            .pacing
            .decisions()
            .values()
            .find(|decision| decision.decision_id == decision_id)
            .cloned()
            .ok_or(PacingError::UnknownDecision)?;
        if let Some(field) = identity.mismatch_against(&decision) {
            return Err(RuntimeError::LiveDecisionMismatch(field));
        }
        if !decision.is_settled_purchase() {
            return Err(RuntimeError::InvalidCycle(format!(
                "decision {decision_id} is not a settled purchase (settled={}, filled_usdc={}); \
                 an unsettled decision records its inventory through the settlement itself, and \
                 one that filled nothing bought no HYPE to backfill",
                decision.settled,
                decision.filled_usdc.as_micros()
            )));
        }
        // The same checks a live settlement's evidence passes: the quoted
        // journal must be the one this decision was bound to, and the cash
        // and inventory sides must agree.
        let Some(row) = self.validated_acquisition_row(
            decision_id,
            decision.filled_usdc,
            acquisition,
            recorded_at,
        )?
        else {
            return Err(RuntimeError::InvalidHypeAcquisition(format!(
                "decision {decision_id} settled a purchase, so it cannot be backfilled as \
                 though no workflow ever existed"
            )));
        };
        if let Some(existing) = self.state.hype_acquisitions.get(decision_id) {
            if same_acquisition(existing, &row) {
                return Ok(LiveSettlementOutcome::AlreadySettled);
            }
            return Err(RuntimeError::HypeAcquisitionConflict(format!(
                "decision {decision_id} already records {} HYPE atoms from workflow {} ({}) \
                 but the backfill offers {} from workflow {} ({})",
                existing.credited_hype_atoms,
                existing.workflow_id,
                existing.journal.display(),
                row.credited_hype_atoms,
                row.workflow_id,
                row.journal.display()
            )));
        }
        let mut next_state = self.state.clone();
        next_state
            .hype_acquisitions
            .insert(decision_id.to_owned(), row);
        self.commit_state_change(recorded_at, next_state, Vec::new())?;
        Ok(LiveSettlementOutcome::Settled)
    }

    /// Commits one durable state change: the single write protocol behind
    /// every cycle, settlement, journal intent and acquisition record, so a
    /// change to it (an extra lock re-check, a different pending-file name)
    /// cannot be applied to some write paths and not others.
    ///
    /// Publishes nothing: derived documents are the caller's decision, since
    /// only some state changes affect them.
    fn commit_state_change(
        &mut self,
        at: DateTime<Utc>,
        next_state: RuntimeState,
        ledger_events: Vec<LedgerEvent>,
    ) -> Result<(), RuntimeError> {
        let pending = PendingRuntimeCycle::new(at, next_state, ledger_events)?;
        let replayed = self
            .ledger
            .validate_append_batch(&pending_cycle_ledger_events(&pending))?;
        ensure_capital_totals_match(&pending.body.state.pacing, &replayed)?;
        self.ensure_runtime_lock_current()?;
        write_private_json_atomic(
            self.config.state_directory.join(PENDING_CYCLE_FILE_NAME),
            &pending,
        )?;
        self.ensure_runtime_lock_current()?;
        self.state = commit_pending_cycle(&self.config, &self.limits, &mut self.ledger, &pending)?;
        Ok(())
    }

    /// Validates the HYPE side of a settlement and shapes it into the row
    /// that will be committed with it, or `None` for a release — a decision
    /// that never had a journal owns no inventory and needs no row.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError::InvalidHypeAcquisition`] when the workflow
    /// identity is blank, when the quoted journal is not the one this runtime
    /// recorded as this decision's intent (a settlement may only be evidenced
    /// by the journal the decision was bound to), or when the cash and
    /// inventory sides contradict each other — a settlement that filled USDC
    /// but credited no HYPE, or credited HYPE without filling any USDC, is a
    /// caller bug and is refused rather than recorded.
    fn validated_acquisition_row(
        &self,
        decision_id: &str,
        filled_usdc: UsdcMicros,
        acquisition: &LiveHypeAcquisition,
        settled_at: DateTime<Utc>,
    ) -> Result<Option<RuntimeHypeAcquisition>, RuntimeError> {
        let recorded_intent = self.state.live_journal_intents.get(decision_id);
        let LiveHypeAcquisition::Workflow {
            workflow_id,
            journal,
            credited_hype_atoms,
            last_fill_at,
        } = acquisition
        else {
            // A release: the runtime must agree no journal was ever bound —
            // the caller's own absence check is not evidence on its own — and
            // capital that bought nothing cannot have moved.
            if let Some(intent) = recorded_intent {
                return Err(RuntimeError::InvalidHypeAcquisition(format!(
                    "decision {decision_id} declared journal {} but is being settled as though \
                     no workflow ever existed",
                    intent.display()
                )));
            }
            if !filled_usdc.is_zero() {
                return Err(RuntimeError::InvalidHypeAcquisition(format!(
                    "decision {decision_id} settles {} filled USDC micros with no workflow to \
                     account for the HYPE it bought",
                    filled_usdc.as_micros()
                )));
            }
            return Ok(None);
        };
        let workflow_id = workflow_id.trim();
        if workflow_id.is_empty() {
            return Err(RuntimeError::InvalidHypeAcquisition(
                "acquisition evidence has no workflow identity".to_owned(),
            ));
        }
        let Some(intent) = recorded_intent else {
            return Err(RuntimeError::InvalidHypeAcquisition(format!(
                "decision {decision_id} has no recorded journal intent to evidence a \
                 settlement with"
            )));
        };
        if !same_journal_path(intent, journal) {
            return Err(RuntimeError::InvalidHypeAcquisition(format!(
                "decision {decision_id} is bound to journal {} but its settlement quotes {}",
                intent.display(),
                journal.display()
            )));
        }
        if filled_usdc.is_zero() != (*credited_hype_atoms == 0) {
            return Err(RuntimeError::InvalidHypeAcquisition(format!(
                "decision {decision_id} settles {} filled USDC micros against {} credited HYPE \
                 atoms; a purchase that moved cash must credit HYPE and one that credited HYPE \
                 must have moved cash",
                filled_usdc.as_micros(),
                credited_hype_atoms
            )));
        }
        Ok(Some(RuntimeHypeAcquisition {
            workflow_id: workflow_id.to_owned(),
            journal: journal.clone(),
            credited_hype_atoms: *credited_hype_atoms,
            last_fill_at: *last_fill_at,
            recorded_at: settled_at,
        }))
    }

    /// Durably settles a live planned decision from its reconciled terminal
    /// fill: `filled_usdc` is the cumulative filled notional and
    /// `debited_usdc` the cumulative cash debit including fees, both taken
    /// from the execution workflow's durable `OrderFinalized` evidence. A
    /// canceled/expired unfilled order settles at zero, releasing the
    /// commitment. `identity` is the workflow's durable copy of the decision
    /// it was bound to; every field must match this runtime's own decision,
    /// so a settlement can never land on a different runtime that happens to
    /// hold a decision with the same date-derived ID. Repeating the exact
    /// settlement is idempotent; a conflicting replay, an overfill, or a
    /// debit above the commitment fails closed without touching runtime
    /// state.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] for an unknown or zero-planned decision, an
    /// identity mismatch, a conflicting or over-committed settlement, a
    /// settlement dated before the decision, or ledger/persistence failures.
    pub fn settle_live_decision(
        &mut self,
        identity: &LiveDecisionIdentity,
        filled_usdc: UsdcMicros,
        debited_usdc: UsdcMicros,
        acquisition: &LiveHypeAcquisition,
        settled_at: DateTime<Utc>,
    ) -> Result<LiveSettlementOutcome, RuntimeError> {
        self.ensure_runtime_lock_current()?;
        let decision_id = identity.decision_id.as_str();
        let decision = self
            .state
            .pacing
            .decisions()
            .values()
            .find(|decision| decision.decision_id == decision_id)
            .cloned()
            .ok_or(PacingError::UnknownDecision)?;
        if let Some(field) = identity.mismatch_against(&decision) {
            return Err(RuntimeError::LiveDecisionMismatch(field));
        }
        let acquisition_row =
            self.validated_acquisition_row(decision_id, filled_usdc, acquisition, settled_at)?;
        if decision.settled {
            // Delegates the exact-replay-vs-conflict distinction to pacing so
            // the two never disagree; a matching replay writes nothing.
            let mut probe = self.state.pacing.clone();
            probe.settle_decision(decision_id, filled_usdc, debited_usdc)?;
            // The HYPE side gets the same treatment as the USDC side: an
            // exact replay writes nothing, a replay quoting different
            // inventory fails closed rather than overwriting the record the
            // committed settlement was hash-chained with. A settled decision
            // with no row at all predates bot-strategy#929 and is left for
            // the explicit backfill: silently minting a row here would let
            // any later reconcile assert inventory the committed cycle never
            // authenticated.
            match (
                self.state.hype_acquisitions.get(decision_id),
                acquisition_row.as_ref(),
            ) {
                (Some(existing), Some(replayed)) if !same_acquisition(existing, replayed) => {
                    return Err(RuntimeError::HypeAcquisitionConflict(format!(
                        "decision {decision_id} already recorded {} HYPE atoms from workflow {} \
                         ({}) but this settlement reports {} from workflow {} ({})",
                        existing.credited_hype_atoms,
                        existing.workflow_id,
                        existing.journal.display(),
                        replayed.credited_hype_atoms,
                        replayed.workflow_id,
                        replayed.journal.display()
                    )));
                }
                (Some(existing), None) => {
                    return Err(RuntimeError::HypeAcquisitionConflict(format!(
                        "decision {decision_id} recorded {} HYPE atoms from workflow {} ({}) \
                         but is being replayed as though no workflow ever existed",
                        existing.credited_hype_atoms,
                        existing.workflow_id,
                        existing.journal.display()
                    )));
                }
                // A settled decision with no recorded row predates
                // bot-strategy#929: left to the explicit backfill rather than
                // minted here (see the comment above).
                (None, _) | (Some(_), Some(_)) => {}
            }
            // A retry after a commit whose metrics publication failed must
            // still leave the derived outputs current.
            self.publish_metrics(settled_at)?;
            return Ok(LiveSettlementOutcome::AlreadySettled);
        }
        if settled_at < decision.decided_at {
            return Err(RuntimeError::InvalidCycle(
                "live settlement predates its decision".to_owned(),
            ));
        }
        let mut next_state = self.state.clone();
        next_state
            .pacing
            .settle_decision(decision_id, filled_usdc, debited_usdc)?;
        next_state.pacing.validate_for_limits(&self.limits)?;
        // Same commit as the capital settlement above: the inventory this
        // decision bought and the cash it spent become durable together or
        // not at all (bot-strategy#929).
        if let Some(row) = acquisition_row {
            next_state
                .hype_acquisitions
                .insert(decision_id.to_owned(), row);
        }
        let ledger_events = vec![LedgerEvent {
            event_id: format!("decision:{decision_id}:live-settlement"),
            occurred_at: settled_at,
            kind: LedgerEventKind::CapitalSettled {
                commitment_id: format!("commitment:{decision_id}"),
                debited_usdc,
            },
        }];
        self.commit_state_change(settled_at, next_state, ledger_events)?;
        // Committed and durable; the derived metrics must not keep showing
        // the pre-settlement commitment until some later scheduled cycle.
        self.publish_metrics(settled_at)?;
        Ok(LiveSettlementOutcome::Settled)
    }

    /// Publishes the status and metrics documents for an observation that is
    /// not allowed to become a cycle.
    ///
    /// Writes exactly what [`Self::apply_cycle`] would publish — including
    /// the operations block a dashboard needs to show committed capital and
    /// stuck detection — but commits no state, appends no ledger event, and
    /// makes no decision. Used when an observation is itself the reason to
    /// stop (bot-strategy#929: attributed HYPE above what the account holds),
    /// so the dashboard keeps being updated while the economic path fails
    /// closed.
    ///
    /// # Errors
    ///
    /// Returns [`RuntimeError`] when the metrics snapshot cannot be derived
    /// or either document cannot be written.
    pub fn publish_halted_status(
        &self,
        accumulator: AccumulatorStatus,
        observed_at: DateTime<Utc>,
    ) -> Result<(), RuntimeError> {
        let signal = self.disk_signal_for_metrics(observed_at)?;
        let metrics = self.derive_metrics(observed_at, signal.as_ref())?;
        let status = DashboardStatus::new(
            observed_at,
            self.process_started_at.min(observed_at),
            true,
            accumulator,
        )
        .with_operations(metrics.clone())?;
        write_metrics_atomic(&self.config.metrics_path, &metrics)?;
        write_status_atomic(&self.config.status_path, &status)?;
        Ok(())
    }

    /// Returns the inclusive start of the next overlapping movement query.
    #[must_use]
    pub fn next_scan_start_ms(&self) -> u64 {
        self.state
            .last_complete_scan_end_ms
            .map_or(self.state.movement_history_start_ms, |end| {
                end.saturating_sub(self.config.movement_overlap_ms)
                    .max(self.state.movement_history_start_ms)
            })
    }

    /// Reconciles one read-only cycle, durably records any due decision/skip,
    /// and atomically publishes private report plus public status/metrics.
    ///
    /// # Errors
    ///
    /// Fails closed on incomplete range binding, malformed movements,
    /// unrecognized approvals, pacing/ledger disagreement, or persistence
    /// errors. No economic action is constructed on any path.
    #[allow(clippy::too_many_lines)]
    pub fn apply_cycle(
        &mut self,
        input: RuntimeCycleInput<'_>,
    ) -> Result<RuntimeCycleReport, RuntimeError> {
        self.ensure_runtime_lock_current()?;
        validate_cycle_range(
            &input,
            self.next_scan_start_ms(),
            self.config.account_observation_max_age_seconds,
        )?;
        if input.approvals.0.values().any(|approval| {
            approval.confirmed_at > input.observed_at || approval.approved_at > input.observed_at
        }) {
            return Err(RuntimeError::InvalidAdmissionArtifact(
                "deposit approval evidence must not be in the future".to_owned(),
            ));
        }
        let existing_decision = self
            .state
            .pacing
            .decisions()
            .get(&input.observed_at.date_naive())
            .cloned();
        let scheduled_boundary = scheduled_decision_boundary(input.observed_at, &self.limits)?;
        let decision_signal = scheduled_boundary.and_then(|boundary| {
            input.signal.filter(|signal| {
                signal.decision_at() == boundary && signal.decision_date() == boundary.date_naive()
            })
        });
        let boundary_replay_safe = scheduled_boundary.is_some_and(|boundary| {
            existing_decision.is_none()
                && self
                    .state
                    .pacing
                    .capital_reconciled_through()
                    .is_none_or(|watermark| watermark <= boundary)
        });
        let mut capital_history_complete = input.capital_history_complete;
        let balance_observation_started_at = *input.accumulator.balance_observation_started_at();
        let balance_observed_at = *input.accumulator.balance_observed_at();
        let boundary_interval_covered = scheduled_boundary.is_some_and(|boundary| {
            let start_ms = boundary
                .timestamp_millis()
                .min(balance_observation_started_at.timestamp_millis());
            let end_ms = boundary
                .timestamp_millis()
                .max(balance_observed_at.timestamp_millis());
            u64::try_from(start_ms)
                .ok()
                .zip(u64::try_from(end_ms).ok())
                .is_some_and(|(start_ms, end_ms)| {
                    input.scan_start_ms <= start_ms && input.scan_end_ms >= end_ms
                })
        });
        let mut boundary_balance_reconstructable =
            input.capital_history_complete && boundary_interval_covered;
        let mut boundary_balance_effects = BTreeMap::new();
        let mut capital_events = existing_deposit_events(&self.state.pacing, input.approvals)?;
        let mut observed_deposit_ids = self
            .state
            .pacing
            .deposits()
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        let mut new_authoritative_deposit_ids = BTreeSet::new();
        let mut movement_ledger_events = Vec::new();

        let ordered_movements = unique_ordered_movements(input.movements)?;
        for movement in ordered_movements {
            validate_movement_range(movement, &input)?;
            let occurred_at = timestamp_ms(movement.timestamp_ms)?;
            let in_balance_request_window = movement_in_balance_observation_window(
                movement.timestamp_ms,
                balance_observation_started_at,
                balance_observed_at,
            );
            let between_balance_and_boundary = scheduled_boundary.is_some_and(|boundary| {
                boundary_balance_direction(occurred_at, balance_observed_at, boundary).is_some()
            });
            let route = self.config.parent_funding_route.as_ref();
            let internal_usdc = movement.kind == HyperliquidAccountMovementKind::InternalTransfer
                && movement.token == "USDC";
            let parent_funding = internal_usdc
                && movement.amount > Decimal::ZERO
                && route.is_some_and(|route| {
                    movement.counterparty.as_deref() == Some(route.parent_account.as_str())
                });
            let transfer_withdrawal =
                internal_usdc && movement.amount < Decimal::ZERO && route.is_some();
            if internal_usdc && route.is_some() && !parent_funding && !transfer_withdrawal {
                capital_history_complete = false;
                boundary_balance_reconstructable = false;
            }
            if matches!(
                movement.kind,
                HyperliquidAccountMovementKind::Unknown
                    | HyperliquidAccountMovementKind::InternalTransfer
                    | HyperliquidAccountMovementKind::TradingRelated
            ) && !parent_funding
                && !transfer_withdrawal
                && (in_balance_request_window || between_balance_and_boundary)
            {
                boundary_balance_reconstructable = false;
            }
            if movement.token != "USDC" {
                continue;
            }
            if in_balance_request_window {
                boundary_balance_reconstructable = false;
            }
            match movement.kind {
                HyperliquidAccountMovementKind::ExternalDeposit
                | HyperliquidAccountMovementKind::InternalTransfer
                    if movement.kind == HyperliquidAccountMovementKind::ExternalDeposit
                        || parent_funding =>
                {
                    let amount = positive_usdc_micros(movement.amount)?;
                    record_boundary_balance_effect(
                        &mut boundary_balance_effects,
                        &movement.event_id,
                        occurred_at,
                        scheduled_boundary,
                        balance_observed_at,
                        i128::from(amount.as_micros()),
                    )?;
                    movement_ledger_events.push(LedgerEvent {
                        event_id: movement.event_id.clone(),
                        occurred_at,
                        kind: if parent_funding {
                            let route = route.ok_or_else(|| {
                                RuntimeError::InvalidCycle("parent funding route missing".into())
                            })?;
                            LedgerEventKind::AuthoritativeParentFunding {
                                amount_usdc: amount,
                                parent_account: route.parent_account.clone(),
                                execution_account: route.execution_account.clone(),
                            }
                        } else {
                            LedgerEventKind::AuthoritativeDeposit {
                                amount_usdc: amount,
                            }
                        },
                    });
                    let approval = input.approvals.get(&movement.event_id);
                    let durable_tranche = self.state.pacing.deposits().get(&movement.event_id);
                    capital_events.push(CapitalEvent::Deposit(DepositEvent {
                        max_admitted_usdc: approval.map_or_else(
                            || durable_tranche.and_then(|tranche| tranche.max_admitted_usdc),
                            |value| value.max_admitted_usdc,
                        ),
                        event_id: movement.event_id.clone(),
                        amount_usdc: amount,
                        received_at: occurred_at,
                        confirmed_at: approval
                            .map(|value| value.confirmed_at)
                            .or_else(|| durable_tranche.and_then(|tranche| tranche.confirmed_at)),
                        confirmation_count: approval.map_or_else(
                            || durable_tranche.map_or(0, |tranche| tranche.confirmation_count),
                            |value| value.confirmation_count,
                        ),
                        admission_approved_at: approval.map(|value| value.approved_at).or_else(
                            || durable_tranche.and_then(|tranche| tranche.admission_approved_at),
                        ),
                    }));
                    observed_deposit_ids.insert(movement.event_id.clone());
                    if !self
                        .state
                        .pacing
                        .deposits()
                        .contains_key(&movement.event_id)
                    {
                        new_authoritative_deposit_ids.insert(movement.event_id.clone());
                    }
                }
                HyperliquidAccountMovementKind::ExternalWithdrawal
                | HyperliquidAccountMovementKind::InternalTransfer
                    if movement.kind == HyperliquidAccountMovementKind::ExternalWithdrawal
                        || transfer_withdrawal =>
                {
                    let amount = positive_usdc_micros(movement.amount.abs())?;
                    record_boundary_balance_effect(
                        &mut boundary_balance_effects,
                        &movement.event_id,
                        occurred_at,
                        scheduled_boundary,
                        balance_observed_at,
                        -i128::from(amount.as_micros()),
                    )?;
                    movement_ledger_events.push(LedgerEvent {
                        event_id: movement.event_id.clone(),
                        occurred_at,
                        kind: if transfer_withdrawal {
                            let route = route.ok_or_else(|| {
                                RuntimeError::InvalidCycle(
                                    "transfer withdrawal route missing".into(),
                                )
                            })?;
                            let counterparty = movement.counterparty.as_ref().ok_or_else(|| {
                                RuntimeError::InvalidCycle(
                                    "internal withdrawal counterparty unavailable".into(),
                                )
                            })?;
                            let validated =
                                ParentFundingRoute::new(&route.execution_account, counterparty)
                                    .map_err(|_| {
                                        RuntimeError::InvalidCycle(
                                            "invalid internal withdrawal counterparty".into(),
                                        )
                                    })?;
                            LedgerEventKind::AuthoritativeTransferWithdrawal {
                                unadmitted_allocations: Vec::new(),
                                amount_usdc: amount,
                                execution_account: validated.execution_account,
                                counterparty: validated.parent_account,
                            }
                        } else {
                            LedgerEventKind::AuthoritativeWithdrawal {
                                amount_usdc: amount,
                            }
                        },
                    });
                    let reconciled_at = self
                        .state
                        .pacing
                        .withdrawals()
                        .get(&movement.event_id)
                        .map_or_else(
                            || {
                                scheduled_boundary
                                    .filter(|boundary| {
                                        boundary_replay_safe && occurred_at <= *boundary
                                    })
                                    .unwrap_or(input.observed_at)
                            },
                            |record| record.event.reconciled_at,
                        );
                    capital_events.push(CapitalEvent::Withdrawal(WithdrawalEvent {
                        allow_unadmitted_funding: transfer_withdrawal,
                        event_id: movement.event_id.clone(),
                        amount_usdc: amount,
                        occurred_at,
                        reconciled_at,
                    }));
                }
                HyperliquidAccountMovementKind::Unknown => {
                    capital_history_complete = false;
                    boundary_balance_reconstructable = false;
                }
                HyperliquidAccountMovementKind::InternalTransfer
                | HyperliquidAccountMovementKind::TradingRelated => {}
                HyperliquidAccountMovementKind::ExternalDeposit
                | HyperliquidAccountMovementKind::ExternalWithdrawal => {
                    return Err(RuntimeError::InvalidCycle(
                        "unhandled external movement".into(),
                    ))
                }
            }
        }
        for approval_id in input.approvals.0.keys() {
            if capital_history_complete && !observed_deposit_ids.contains(approval_id) {
                return Err(RuntimeError::UnknownAdmissionApproval(approval_id.clone()));
            }
        }

        let observed_spot_usdc = f64_usdc_micros(input.accumulator.usdc_balance())?;
        let (boundary_observed_spot_usdc, boundary_balance_available) =
            reconstruct_boundary_balance(
                observed_spot_usdc,
                balance_observed_at,
                scheduled_boundary,
                capital_history_complete && boundary_balance_reconstructable,
                &boundary_balance_effects,
            )?;
        let mut next_state = self.state.clone();
        next_state
            .parent_funding_route
            .clone_from(&self.config.parent_funding_route);
        if let DecisionMode::Live { history_directory } = &input.decision_mode {
            match &next_state.live_history_directory {
                Some(bound) if bound != history_directory => {
                    return Err(RuntimeError::LiveHistoryDirectoryMismatch(format!(
                        "this runtime's live decisions are bound to {} but the cycle names {}",
                        bound.display(),
                        history_directory.display()
                    )));
                }
                _ => next_state.live_history_directory = Some(history_directory.clone()),
            }
        }
        let mut ledger_events = Vec::new();
        let decision_result = if let Some(decision) = existing_decision {
            next_state.pacing.reconcile_capital_preserving_admissions(
                &capital_events,
                input.observed_at,
                &self.limits,
            )?;
            let admission_events = admission_delta_events(
                &self.ledger,
                &next_state.pacing,
                input.observed_at,
                &new_authoritative_deposit_ids,
            )?;
            ledger_events.extend(ordered_capital_ledger_events(
                movement_ledger_events,
                admission_events,
                &next_state.pacing,
            ));
            Some(DecisionResult::Existing(decision))
        } else if boundary_replay_safe {
            let boundary = scheduled_boundary.ok_or_else(|| {
                RuntimeError::InvalidCycle("missing scheduled decision boundary".to_owned())
            })?;
            let boundary_capital_events = capital_events_as_of(&capital_events, boundary);
            let (boundary_movements, later_movements): (Vec<_>, Vec<_>) = movement_ledger_events
                .into_iter()
                .partition(|event| event.occurred_at <= boundary);
            next_state.pacing.reconcile_capital_preserving_admissions(
                &boundary_capital_events,
                boundary,
                &self.limits,
            )?;
            let boundary_admission_events = admission_delta_events(
                &self.ledger,
                &next_state.pacing,
                boundary,
                &new_authoritative_deposit_ids,
            )?;
            ledger_events.extend(ordered_capital_ledger_events(
                boundary_movements,
                boundary_admission_events,
                &next_state.pacing,
            ));
            let boundary_pacing = next_state.pacing.clone();
            let decision_input = DecisionInput {
                at: boundary,
                observed_spot_usdc: boundary_observed_spot_usdc,
                capital_history_complete: capital_history_complete && boundary_balance_available,
                manual_pause: input.manual_pause,
            };
            let result = match decision_signal {
                Some(signal) => {
                    next_state
                        .pacing
                        .decide_with_signal(&decision_input, &self.limits, signal)
                }
                None => next_state
                    .pacing
                    .decide_with_unavailable_signal(&decision_input, &self.limits),
            };
            let mut decision = match result {
                Ok(result) => Some(result),
                Err(PacingError::DecisionNotDue) => None,
                Err(error) => return Err(error.into()),
            };
            if let Some(result) = &mut decision {
                ledger_events.extend(match input.decision_mode {
                    DecisionMode::DryRun => {
                        dry_run_decision_events(&mut next_state.pacing, result)?
                    }
                    DecisionMode::Live { .. } => live_decision_events(result)?,
                });
            }
            next_state.pacing.reconcile_capital_preserving_admissions(
                &capital_events,
                input.observed_at,
                &self.limits,
            )?;
            let later_admission_events = admission_delta_events_between(
                &boundary_pacing,
                &next_state.pacing,
                input.observed_at,
            )?;
            ledger_events.extend(ordered_capital_ledger_events(
                later_movements,
                later_admission_events,
                &next_state.pacing,
            ));
            decision
        } else {
            next_state.pacing.reconcile_capital_preserving_admissions(
                &capital_events,
                input.observed_at,
                &self.limits,
            )?;
            let admission_events = admission_delta_events(
                &self.ledger,
                &next_state.pacing,
                input.observed_at,
                &new_authoritative_deposit_ids,
            )?;
            ledger_events.extend(ordered_capital_ledger_events(
                movement_ledger_events,
                admission_events,
                &next_state.pacing,
            ));
            None
        };
        let decision_evidence = match &decision_result {
            Some(result) => {
                let decision_date = result.decision().decision_date;
                if result.is_new()
                    && next_state
                        .decision_evidence
                        .insert(
                            decision_date,
                            RuntimeDecisionEvidence {
                                signal_available: decision_signal.is_some(),
                                boundary_balance_available,
                            },
                        )
                        .is_some()
                {
                    return Err(RuntimeError::IncompatibleRuntimeState);
                }
                *next_state
                    .decision_evidence
                    .get(&decision_date)
                    .ok_or(RuntimeError::IncompatibleRuntimeState)?
            }
            None => RuntimeDecisionEvidence {
                signal_available: decision_signal.is_some(),
                boundary_balance_available,
            },
        };
        if let Some(result) = &decision_result {
            // Counts economic actions the DRY_RUN cycle suppressed. A live
            // cycle suppresses nothing: its planned decision is handed to the
            // execution workflow instead.
            if matches!(input.decision_mode, DecisionMode::DryRun)
                && result.is_new()
                && !result.decision().planned_usdc.is_zero()
            {
                next_state.dry_run_actions_total = next_state
                    .dry_run_actions_total
                    .checked_add(1)
                    .ok_or(RuntimeError::CounterOverflow)?;
            }
            if result.is_new()
                && (decision_signal.is_none()
                    || decision_signal.is_some_and(|signal| {
                        signal.core_is_stale_at(result.decision().decided_at)
                    }))
            {
                next_state.stale_signal_events_total = next_state
                    .stale_signal_events_total
                    .checked_add(1)
                    .ok_or(RuntimeError::CounterOverflow)?;
            }
        }
        next_state.api_errors_total = next_state
            .api_errors_total
            .checked_add(input.api_errors)
            .ok_or(RuntimeError::CounterOverflow)?;
        if capital_history_complete {
            next_state.last_complete_scan_end_ms = Some(
                next_state
                    .last_complete_scan_end_ms
                    .map_or(input.scan_end_ms, |old| old.max(input.scan_end_ms)),
            );
        }
        ledger_events.push(LedgerEvent {
            event_id: format!("balance:{}", input.scan_end_ms),
            occurred_at: input.observed_at,
            kind: LedgerEventKind::BalanceObserved {
                observed_usdc: observed_spot_usdc,
                observed_hype_atoms: 0,
            },
        });
        let report = RuntimeCycleReport {
            schema_version: CYCLE_REPORT_SCHEMA_VERSION,
            observed_at: input.observed_at,
            scan_start_ms: input.scan_start_ms,
            scan_end_ms: input.scan_end_ms,
            capital_history_complete,
            normalized_movement_count: input.movements.len(),
            decision: decision_result
                .as_ref()
                .map(|result| result.decision().clone()),
            new_decision: decision_result.as_ref().is_some_and(DecisionResult::is_new),
            economic_action_suppressed: matches!(input.decision_mode, DecisionMode::DryRun),
            signed_action_created: false,
            signal_available: decision_evidence.signal_available,
            boundary_balance_available: decision_evidence.boundary_balance_available,
        };
        self.commit_state_change(input.observed_at, next_state, ledger_events)?;
        let metrics = self.derive_metrics(input.observed_at, decision_signal)?;
        let status = DashboardStatus::new(
            input.observed_at,
            self.process_started_at.min(input.observed_at),
            true,
            input.accumulator,
        )
        .with_operations(metrics.clone())?;
        write_private_json_atomic(&self.config.cycle_report_path, &report)?;
        write_metrics_atomic(&self.config.metrics_path, &metrics)?;
        write_status_atomic(&self.config.status_path, &status)?;
        Ok(report)
    }

    /// Republishes the metrics file from the current committed state. Used
    /// after a live settlement changes committed/spent capital outside a
    /// scheduled cycle, when the recurring cycle that would normally refresh
    /// it is (by the probe runbook) stopped. The public dashboard status is
    /// venue-observation-bound and keeps refreshing through the observer
    /// timer, which stays active during a probe.
    fn publish_metrics(&self, observed_at: DateTime<Utc>) -> Result<(), RuntimeError> {
        let signal = self.disk_signal_for_metrics(observed_at)?;
        let metrics = self.derive_metrics(observed_at, signal.as_ref())?;
        write_metrics_atomic(&self.config.metrics_path, &metrics)?;
        Ok(())
    }

    /// The signal snapshot on disk, as an out-of-cycle publication may cite
    /// it. A snapshot dated after `observed_at` is dropped rather than cited:
    /// the producer writes the *next* boundary's snapshot ~90 s before that
    /// boundary, and the metrics validator rejects a future-dated decision —
    /// which would turn a publication in that window into no publication at
    /// all. `apply_cycle` filters its signal to the scheduled boundary for
    /// the same reason.
    fn disk_signal_for_metrics(
        &self,
        observed_at: DateTime<Utc>,
    ) -> Result<Option<SignalSnapshot>, RuntimeError> {
        let signal = match fs::read_to_string(self.config.signal_snapshot_path()) {
            Ok(payload) => SignalSnapshot::from_json(&payload).ok(),
            Err(error) if error.kind() == ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        Ok(signal.filter(|signal| signal.decision_at() <= observed_at))
    }

    /// The one derivation of the metrics document from this runtime's state,
    /// so every publication — a cycle, a settlement, a halted observation —
    /// reports the same inputs.
    fn derive_metrics(
        &self,
        observed_at: DateTime<Utc>,
        signal: Option<&SignalSnapshot>,
    ) -> Result<MetricsSnapshot, RuntimeError> {
        Ok(MetricsSnapshot::from_runtime(
            observed_at,
            &self.state.pacing,
            &self.limits,
            self.ledger.state(),
            &[],
            signal,
            self.state.api_errors_total,
            self.state.stale_signal_events_total,
            self.state.dry_run_actions_total,
            self.config.stuck_after_seconds,
        )?)
    }

    fn ensure_runtime_lock_current(&self) -> Result<(), RuntimeError> {
        validate_runtime_lock(
            &self.config.state_directory.join(RUNTIME_LOCK_FILE_NAME),
            &self.lock,
        )
    }
}

fn ensure_protected_boundary(config: &RuntimeConfig) -> Result<(), RuntimeError> {
    let anchor_parent = config.protected_anchor_path.parent().ok_or_else(|| {
        RuntimeError::InvalidConfig("protected anchor has no parent directory".to_owned())
    })?;
    fs::create_dir_all(anchor_parent)?;
    reject_linked_file(&config.protected_anchor_path)?;
    let canonical_state = fs::canonicalize(&config.state_directory)?;
    let canonical_anchor_parent = fs::canonicalize(anchor_parent)?;
    if canonical_anchor_parent.starts_with(&canonical_state) {
        return Err(RuntimeError::InvalidConfig(
            "protected anchor resolves inside the mutable runtime state directory".to_owned(),
        ));
    }
    Ok(())
}

fn ensure_configured_files_outside_state(config: &RuntimeConfig) -> Result<(), RuntimeError> {
    let canonical_state = fs::canonicalize(&config.state_directory)?;
    let mut resolved_paths = Vec::new();
    let mut resolved_parents = Vec::new();
    for path in config.configured_file_paths() {
        let parent = path.parent().ok_or_else(|| {
            RuntimeError::InvalidConfig("configured runtime file has no parent".to_owned())
        })?;
        let resolved_parent = resolve_without_creating(parent)?;
        if resolved_parent.starts_with(&canonical_state) {
            return Err(RuntimeError::InvalidConfig(
                "configured runtime file resolves inside the reserved state directory".to_owned(),
            ));
        }
        let file_name = path.file_name().ok_or_else(|| {
            RuntimeError::InvalidConfig("configured runtime file has no file name".to_owned())
        })?;
        let resolved_path = resolved_parent.join(file_name);
        if resolved_paths.iter().any(|existing: &PathBuf| {
            existing == &resolved_path
                || existing.starts_with(&resolved_path)
                || resolved_path.starts_with(existing)
        }) {
            return Err(RuntimeError::InvalidConfig(
                "configured runtime files resolve to the same path or an ancestor relationship"
                    .to_owned(),
            ));
        }
        resolved_paths.push(resolved_path);
        resolved_parents.push(resolved_parent);
    }
    for (path, resolved_parent) in config
        .configured_file_paths()
        .into_iter()
        .zip(resolved_parents)
    {
        let parent = path.parent().ok_or_else(|| {
            RuntimeError::InvalidConfig("configured runtime file has no parent".to_owned())
        })?;
        fs::create_dir_all(parent)?;
        if fs::canonicalize(parent)? != resolved_parent {
            return Err(RuntimeError::InvalidConfig(
                "configured runtime file parent changed during validation".to_owned(),
            ));
        }
        reject_linked_file(path)?;
    }
    Ok(())
}

fn resolve_without_creating(path: &Path) -> Result<PathBuf, RuntimeError> {
    let mut existing = path;
    let mut missing = Vec::new();
    loop {
        match fs::metadata(existing) {
            Ok(metadata) => {
                if !metadata.is_dir() {
                    return Err(RuntimeError::InvalidConfig(
                        "configured runtime file parent is not a directory".to_owned(),
                    ));
                }
                let mut resolved = fs::canonicalize(existing)?;
                for component in missing.iter().rev() {
                    resolved.push(component);
                }
                return Ok(resolved);
            }
            Err(error) if error.kind() == ErrorKind::NotFound => {
                let component = existing.file_name().ok_or_else(|| {
                    RuntimeError::InvalidConfig(
                        "configured runtime file has no existing path ancestor".to_owned(),
                    )
                })?;
                missing.push(component.to_os_string());
                existing = existing.parent().ok_or_else(|| {
                    RuntimeError::InvalidConfig(
                        "configured runtime file has no existing path ancestor".to_owned(),
                    )
                })?;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

fn recover_pending_cycle(
    config: &RuntimeConfig,
    limits: &PacingLimits,
    ledger: &mut DurableLedger,
    state: &mut RuntimeState,
) -> Result<(), RuntimeError> {
    let path = config.state_directory.join(PENDING_CYCLE_FILE_NAME);
    let pending = match fs::read_to_string(&path) {
        Ok(payload) => Some(serde_json::from_str::<PendingRuntimeCycle>(&payload)?),
        Err(error) if error.kind() == ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    let Some(pending) = pending else {
        if ledger.state().has_uncommitted_runtime_cycle() {
            return Err(RuntimeError::MissingAuthenticatedPendingCycle);
        }
        return Ok(());
    };
    pending.validate(config.movement_history_start_ms)?;
    pending.body.state.pacing.validate_for_limits(limits)?;
    if ledger.state().runtime_cycle_prepared(&pending.cycle_hash) {
        *state = commit_pending_cycle(config, limits, ledger, &pending)?;
    } else {
        if ledger.state().has_uncommitted_runtime_cycle() {
            return Err(RuntimeError::PendingCycleConflict);
        }
        remove_file_durable(&path)?;
    }
    Ok(())
}

fn pending_cycle_ledger_events(pending: &PendingRuntimeCycle) -> Vec<LedgerEvent> {
    let mut events = vec![LedgerEvent {
        event_id: format!("runtime-cycle:{}:prepared", pending.cycle_hash),
        occurred_at: pending.body.observed_at,
        kind: LedgerEventKind::RuntimeCyclePrepared {
            cycle_hash: pending.cycle_hash.clone(),
        },
    }];
    events.extend(pending.body.ledger_events.iter().cloned());
    events.push(LedgerEvent {
        event_id: format!("runtime-cycle:{}:committed", pending.cycle_hash),
        occurred_at: pending.body.observed_at,
        kind: LedgerEventKind::RuntimeCycleCommitted {
            cycle_hash: pending.cycle_hash.clone(),
        },
    });
    events
}

fn commit_pending_cycle(
    config: &RuntimeConfig,
    limits: &PacingLimits,
    ledger: &mut DurableLedger,
    pending: &PendingRuntimeCycle,
) -> Result<RuntimeState, RuntimeError> {
    pending.validate(config.movement_history_start_ms)?;
    pending.body.state.pacing.validate_for_limits(limits)?;
    let already_committed = ledger.state().runtime_cycle_committed(&pending.cycle_hash);
    if !already_committed
        && pending.body.state.last_committed_cycle_hash.as_deref()
            != ledger.state().last_runtime_cycle_hash()
    {
        return Err(RuntimeError::PendingCycleConflict);
    }
    if ledger.state().has_uncommitted_runtime_cycle()
        && !ledger.state().runtime_cycle_prepared(&pending.cycle_hash)
    {
        return Err(RuntimeError::PendingCycleConflict);
    }
    for event in pending_cycle_ledger_events(pending) {
        ledger.append(event)?;
    }
    if !ledger.state().runtime_cycle_committed(&pending.cycle_hash) {
        return Err(RuntimeError::CorruptPendingCycle);
    }
    let mut committed_state = pending.body.state.clone();
    committed_state.last_committed_cycle_hash = Some(pending.cycle_hash.clone());
    ensure_capital_totals_match(&committed_state.pacing, ledger.state())?;
    ensure_runtime_head_matches(&committed_state, ledger.state())?;
    write_private_json_atomic(
        config.state_directory.join(STATE_FILE_NAME),
        &committed_state,
    )?;
    write_private_json_atomic(
        config.state_directory.join(COMMITTED_CYCLE_PROOF_FILE_NAME),
        pending,
    )?;
    remove_file_durable(&config.state_directory.join(PENDING_CYCLE_FILE_NAME))?;
    Ok(committed_state)
}

fn remove_file_durable(path: &Path) -> Result<(), RuntimeError> {
    match fs::remove_file(path) {
        Ok(()) => {
            #[cfg(unix)]
            File::open(path.parent().ok_or_else(|| {
                RuntimeError::InvalidConfig("runtime file has no parent".to_owned())
            })?)?
            .sync_all()?;
            Ok(())
        }
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn canonical_sha256<T: Serialize + ?Sized>(value: &T) -> Result<String, RuntimeError> {
    let payload = serde_json::to_vec(value)?;
    Ok(format!("{:x}", Sha256::digest(payload)))
}

fn load_runtime_state(
    path: &Path,
    movement_history_start_ms: u64,
) -> Result<RuntimeState, RuntimeError> {
    match fs::read_to_string(path) {
        Ok(payload) => {
            let state: RuntimeState = serde_json::from_str(&payload)?;
            if state.schema_version != RUNTIME_STATE_SCHEMA_VERSION
                || state.movement_history_start_ms != movement_history_start_ms
            {
                return Err(RuntimeError::IncompatibleRuntimeState);
            }
            Ok(state)
        }
        Err(error) if error.kind() == ErrorKind::NotFound => {
            Ok(RuntimeState::new(movement_history_start_ms))
        }
        Err(error) => Err(error.into()),
    }
}

fn boundary_balance_direction(
    occurred_at: DateTime<Utc>,
    balance_observed_at: DateTime<Utc>,
    boundary: DateTime<Utc>,
) -> Option<i128> {
    if balance_observed_at < boundary
        && occurred_at > balance_observed_at
        && occurred_at <= boundary
    {
        Some(1)
    } else if balance_observed_at > boundary
        && occurred_at > boundary
        && occurred_at <= balance_observed_at
    {
        Some(-1)
    } else {
        None
    }
}

fn movement_in_balance_observation_window(
    movement_timestamp_ms: u64,
    started_at: DateTime<Utc>,
    completed_at: DateTime<Utc>,
) -> bool {
    u64::try_from(started_at.timestamp_millis())
        .ok()
        .zip(u64::try_from(completed_at.timestamp_millis()).ok())
        .is_none_or(|(started_ms, completed_ms)| {
            movement_timestamp_ms >= started_ms && movement_timestamp_ms <= completed_ms
        })
}

fn record_boundary_balance_effect(
    effects: &mut BTreeMap<String, i128>,
    event_id: &str,
    occurred_at: DateTime<Utc>,
    scheduled_boundary: Option<DateTime<Utc>>,
    balance_observed_at: DateTime<Utc>,
    balance_effect: i128,
) -> Result<(), RuntimeError> {
    let Some(direction) = scheduled_boundary.and_then(|boundary| {
        boundary_balance_direction(occurred_at, balance_observed_at, boundary)
    }) else {
        return Ok(());
    };
    let adjustment = balance_effect
        .checked_mul(direction)
        .ok_or_else(|| RuntimeError::InvalidCycle("boundary balance overflow".to_owned()))?;
    if let Some(existing) = effects.get(event_id) {
        if *existing != adjustment {
            return Err(RuntimeError::InvalidCycle(
                "conflicting movement in boundary balance interval".to_owned(),
            ));
        }
        return Ok(());
    }
    effects.insert(event_id.to_owned(), adjustment);
    Ok(())
}

fn reconstruct_boundary_balance(
    observed_spot_usdc: UsdcMicros,
    balance_observed_at: DateTime<Utc>,
    scheduled_boundary: Option<DateTime<Utc>>,
    interval_complete: bool,
    effects: &BTreeMap<String, i128>,
) -> Result<(UsdcMicros, bool), RuntimeError> {
    let Some(boundary) = scheduled_boundary else {
        return Ok((UsdcMicros::default(), false));
    };
    if balance_observed_at == boundary && interval_complete {
        return Ok((observed_spot_usdc, true));
    }
    if !interval_complete {
        return Ok((UsdcMicros::default(), false));
    }
    let adjustment = effects.values().try_fold(0_i128, |total, value| {
        total
            .checked_add(*value)
            .ok_or_else(|| RuntimeError::InvalidCycle("boundary balance overflow".to_owned()))
    })?;
    let reconstructed = i128::from(observed_spot_usdc.as_micros())
        .checked_add(adjustment)
        .ok_or_else(|| RuntimeError::InvalidCycle("boundary balance overflow".to_owned()))?;
    let Ok(reconstructed) = u64::try_from(reconstructed) else {
        return Ok((UsdcMicros::default(), false));
    };
    Ok((UsdcMicros::from_micros(reconstructed), true))
}

fn validate_cycle_range(
    input: &RuntimeCycleInput<'_>,
    expected_start_ms: u64,
    account_observation_max_age_seconds: u64,
) -> Result<(), RuntimeError> {
    let observed_ms = u64::try_from(input.observed_at.timestamp_millis())
        .map_err(|_| RuntimeError::InvalidCycle("negative observation time".to_owned()))?;
    if input.scan_start_ms != expected_start_ms
        || input.scan_end_ms > observed_ms
        || input.scan_start_ms > input.scan_end_ms
    {
        return Err(RuntimeError::InvalidCycle(
            "movement scan range does not match the durable cursor".to_owned(),
        ));
    }
    if *input.accumulator.balance_observed_at() > input.observed_at {
        return Err(RuntimeError::InvalidCycle(
            "account observation is after the runtime cycle".to_owned(),
        ));
    }
    let max_age = TimeDelta::try_seconds(
        i64::try_from(account_observation_max_age_seconds)
            .map_err(|_| RuntimeError::InvalidCycle("account freshness overflow".to_owned()))?,
    )
    .ok_or_else(|| RuntimeError::InvalidCycle("account freshness overflow".to_owned()))?;
    if input
        .observed_at
        .signed_duration_since(*input.accumulator.balance_observed_at())
        > max_age
    {
        return Err(RuntimeError::InvalidCycle(
            "account observation is stale".to_owned(),
        ));
    }
    Ok(())
}

fn scheduled_decision_boundary(
    observed_at: DateTime<Utc>,
    limits: &PacingLimits,
) -> Result<Option<DateTime<Utc>>, RuntimeError> {
    let boundary = Utc
        .with_ymd_and_hms(
            observed_at.year(),
            observed_at.month(),
            observed_at.day(),
            u32::from(limits.utc_hour),
            u32::from(limits.utc_minute),
            0,
        )
        .single()
        .ok_or_else(|| RuntimeError::InvalidCycle("invalid UTC decision boundary".to_owned()))?;
    Ok((observed_at >= boundary).then_some(boundary))
}

fn capital_events_as_of(events: &[CapitalEvent], at: DateTime<Utc>) -> Vec<CapitalEvent> {
    events
        .iter()
        .filter_map(|event| match event {
            CapitalEvent::Deposit(deposit) if deposit.received_at <= at => {
                let mut deposit = deposit.clone();
                if deposit
                    .confirmed_at
                    .is_none_or(|confirmed_at| confirmed_at > at)
                {
                    deposit.confirmed_at = None;
                    deposit.confirmation_count = 0;
                    deposit.admission_approved_at = None;
                    deposit.max_admitted_usdc = None;
                } else if deposit
                    .admission_approved_at
                    .is_some_and(|approved_at| approved_at > at)
                {
                    deposit.admission_approved_at = None;
                    deposit.max_admitted_usdc = None;
                }
                Some(CapitalEvent::Deposit(deposit))
            }
            CapitalEvent::Withdrawal(withdrawal) if withdrawal.occurred_at <= at => {
                Some(CapitalEvent::Withdrawal(withdrawal.clone()))
            }
            CapitalEvent::Deposit(_) | CapitalEvent::Withdrawal(_) => None,
        })
        .collect()
}

fn unique_ordered_movements(
    movements: &[HyperliquidAccountMovement],
) -> Result<Vec<&HyperliquidAccountMovement>, RuntimeError> {
    let mut unique = BTreeMap::<&str, &HyperliquidAccountMovement>::new();
    for movement in movements {
        if let Some(previous) = unique.insert(&movement.event_id, movement) {
            if previous.timestamp_ms != movement.timestamp_ms
                || previous.kind != movement.kind
                || previous.token != movement.token
                || previous.amount != movement.amount
                || previous.counterparty != movement.counterparty
                || previous.transaction_hash != movement.transaction_hash
            {
                return Err(RuntimeError::InvalidCycle(
                    "conflicting normalized movement ID".into(),
                ));
            }
        }
    }
    let mut ordered = unique.into_values().collect::<Vec<_>>();
    ordered.sort_by_key(|movement| (movement.timestamp_ms, &movement.event_id));
    Ok(ordered)
}

fn validate_movement_range(
    movement: &HyperliquidAccountMovement,
    input: &RuntimeCycleInput<'_>,
) -> Result<(), RuntimeError> {
    if movement.event_id.trim() != movement.event_id
        || movement.event_id.is_empty()
        || movement.timestamp_ms < input.scan_start_ms
        || movement.timestamp_ms > input.scan_end_ms
    {
        return Err(RuntimeError::InvalidMovement(
            "movement identity or timestamp is outside the requested range".to_owned(),
        ));
    }
    Ok(())
}

fn existing_deposit_events(
    pacing: &PacingState,
    approvals: &AdmissionApprovals,
) -> Result<Vec<CapitalEvent>, RuntimeError> {
    pacing
        .deposits()
        .values()
        .map(|tranche| {
            let approval = approvals.get(&tranche.event_id);
            if let Some(approval) = approval {
                if approval.confirmed_at < tranche.received_at
                    || tranche
                        .confirmed_at
                        .is_some_and(|existing| existing != approval.confirmed_at)
                    || tranche.confirmation_count > approval.confirmation_count
                    || (tranche.admission_approved_at.is_some()
                        && tranche.max_admitted_usdc != approval.max_admitted_usdc)
                    || tranche
                        .admission_approved_at
                        .is_some_and(|existing| existing != approval.approved_at)
                {
                    return Err(RuntimeError::InvalidAdmissionArtifact(
                        "deposit approval conflicts with durable capital state".to_owned(),
                    ));
                }
            }
            Ok(CapitalEvent::Deposit(DepositEvent {
                max_admitted_usdc: approval
                    .map_or(tranche.max_admitted_usdc, |value| value.max_admitted_usdc),
                event_id: tranche.event_id.clone(),
                amount_usdc: tranche.source_amount_usdc,
                received_at: tranche.received_at,
                confirmed_at: approval
                    .map(|value| value.confirmed_at)
                    .or(tranche.confirmed_at),
                confirmation_count: approval
                    .map_or(tranche.confirmation_count, |value| value.confirmation_count),
                admission_approved_at: approval
                    .map(|value| value.approved_at)
                    .or(tranche.admission_approved_at),
            }))
        })
        .collect()
}

fn admission_delta_events(
    ledger: &DurableLedger,
    pacing: &PacingState,
    at: DateTime<Utc>,
    new_authoritative_deposit_ids: &BTreeSet<String>,
) -> Result<Vec<LedgerEvent>, RuntimeError> {
    let mut events = Vec::new();
    for tranche in pacing.deposits().values() {
        let ledger_admitted = ledger
            .state()
            .admitted_deposit_usdc(&tranche.event_id)
            .or_else(|| {
                new_authoritative_deposit_ids
                    .contains(&tranche.event_id)
                    .then_some(UsdcMicros::default())
            })
            .ok_or_else(|| RuntimeError::MissingAuthoritativeDeposit(tranche.event_id.clone()))?;
        let delta = tranche
            .admitted_usdc
            .as_micros()
            .checked_sub(ledger_admitted.as_micros())
            .ok_or(RuntimeError::CapitalStateMismatch)?;
        if delta == 0 {
            continue;
        }
        let occurred_at = tranche
            .first_usable_at
            .filter(|value| *value <= at)
            .ok_or(RuntimeError::CapitalStateMismatch)?;
        events.push(LedgerEvent {
            event_id: format!(
                "admission:{}:{}",
                tranche.event_id,
                tranche.admitted_usdc.as_micros()
            ),
            occurred_at,
            kind: LedgerEventKind::DepositAdmission {
                deposit_event_id: tranche.event_id.clone(),
                amount_usdc: UsdcMicros::from_micros(delta),
            },
        });
    }
    Ok(events)
}

fn admission_delta_events_between(
    before: &PacingState,
    after: &PacingState,
    at: DateTime<Utc>,
) -> Result<Vec<LedgerEvent>, RuntimeError> {
    let mut events = Vec::new();
    for tranche in after.deposits().values() {
        let previously_admitted = before
            .deposits()
            .get(&tranche.event_id)
            .map_or(UsdcMicros::default(), |value| value.admitted_usdc);
        let delta = tranche
            .admitted_usdc
            .as_micros()
            .checked_sub(previously_admitted.as_micros())
            .ok_or(RuntimeError::CapitalStateMismatch)?;
        if delta == 0 {
            continue;
        }
        let occurred_at = tranche
            .first_usable_at
            .filter(|value| *value <= at)
            .ok_or(RuntimeError::CapitalStateMismatch)?;
        events.push(LedgerEvent {
            event_id: format!(
                "admission:{}:{}",
                tranche.event_id,
                tranche.admitted_usdc.as_micros()
            ),
            occurred_at,
            kind: LedgerEventKind::DepositAdmission {
                deposit_event_id: tranche.event_id.clone(),
                amount_usdc: UsdcMicros::from_micros(delta),
            },
        });
    }
    Ok(events)
}

fn ordered_capital_ledger_events(
    mut movement_events: Vec<LedgerEvent>,
    admission_events: Vec<LedgerEvent>,
    pacing: &PacingState,
) -> Vec<LedgerEvent> {
    for event in &mut movement_events {
        if let LedgerEventKind::AuthoritativeTransferWithdrawal {
            unadmitted_allocations,
            ..
        } = &mut event.kind
        {
            if let Some(record) = pacing.withdrawals().get(&event.event_id) {
                if let Some(frozen) = &record.unadmitted_allocations {
                    unadmitted_allocations.clone_from(frozen);
                }
            }
        }
    }
    movement_events.extend(admission_events);
    movement_events.sort_by(|left, right| {
        left.occurred_at
            .cmp(&right.occurred_at)
            .then_with(|| {
                capital_ledger_event_order(&left.kind).cmp(&capital_ledger_event_order(&right.kind))
            })
            .then_with(|| left.event_id.cmp(&right.event_id))
    });
    movement_events
}

fn capital_ledger_event_order(kind: &LedgerEventKind) -> u8 {
    match kind {
        LedgerEventKind::AuthoritativeDeposit { .. }
        | LedgerEventKind::AuthoritativeParentFunding { .. } => 0,
        LedgerEventKind::DepositAdmission { .. } => 1,
        LedgerEventKind::AuthoritativeWithdrawal { .. }
        | LedgerEventKind::AuthoritativeTransferWithdrawal { .. } => 2,
        _ => 3,
    }
}

fn dry_run_decision_events(
    pacing: &mut PacingState,
    result: &mut DecisionResult,
) -> Result<Vec<LedgerEvent>, RuntimeError> {
    let decision = result.decision().clone();
    let mut events = decision_events(&decision)?;
    if result.is_new() && !decision.planned_usdc.is_zero() {
        pacing.settle_decision(
            &decision.decision_id,
            UsdcMicros::default(),
            UsdcMicros::default(),
        )?;
        events.push(LedgerEvent {
            event_id: format!("decision:{}:dry-run-settlement", decision.decision_id),
            occurred_at: decision.decided_at,
            kind: LedgerEventKind::CapitalSettled {
                commitment_id: format!("commitment:{}", decision.decision_id),
                debited_usdc: UsdcMicros::default(),
            },
        });
        let settled = pacing
            .decisions()
            .get(&decision.decision_date)
            .cloned()
            .ok_or_else(|| {
                RuntimeError::InvalidCycle("settled dry-run decision is missing".to_owned())
            })?;
        *result = DecisionResult::New(settled);
    }
    Ok(events)
}

/// Live-mode counterpart of [`dry_run_decision_events`]: records the
/// commitment/plan (or skip) exactly like `DRY_RUN` does but deliberately does
/// NOT settle a new planned decision, leaving its capital committed for the
/// execution workflow. Settlement follows from the reconciled terminal fill
/// through [`SignerFreeRuntime::settle_live_decision`].
fn live_decision_events(result: &DecisionResult) -> Result<Vec<LedgerEvent>, RuntimeError> {
    decision_events(result.decision())
}

fn decision_events(decision: &DailyDecision) -> Result<Vec<LedgerEvent>, RuntimeError> {
    if decision.planned_usdc.is_zero() {
        let reason = serde_json::to_value(decision.reason)?
            .as_str()
            .ok_or_else(|| RuntimeError::InvalidCycle("invalid skip reason".to_owned()))?
            .to_owned();
        return Ok(vec![LedgerEvent {
            event_id: format!("decision:{}:skip", decision.decision_id),
            occurred_at: decision.decided_at,
            kind: LedgerEventKind::DailySkip {
                decision_id: decision.decision_id.clone(),
                decision_date: decision.decision_date,
                reason,
            },
        }]);
    }
    let commitment_id = format!("commitment:{}", decision.decision_id);
    Ok(vec![
        LedgerEvent {
            event_id: format!("decision:{}:commitment", decision.decision_id),
            occurred_at: decision.decided_at,
            kind: LedgerEventKind::CapitalCommitted {
                commitment_id: commitment_id.clone(),
                amount_usdc: decision.committed_usdc,
            },
        },
        LedgerEvent {
            event_id: format!("decision:{}:planned", decision.decision_id),
            occurred_at: decision.decided_at,
            kind: LedgerEventKind::DailyDecision {
                decision_id: decision.decision_id.clone(),
                decision_date: decision.decision_date,
                commitment_id,
                planned_usdc: decision.planned_usdc,
                committed_usdc: decision.committed_usdc,
            },
        },
    ])
}

fn ensure_capital_totals_match(
    pacing: &PacingState,
    ledger: &crate::ledger::ReplayState,
) -> Result<(), RuntimeError> {
    let admitted = sum_micros(
        pacing
            .deposits()
            .values()
            .map(|tranche| tranche.admitted_usdc),
    )?;
    let committed = sum_micros(
        pacing
            .deposits()
            .values()
            .map(|tranche| tranche.committed_usdc),
    )?;
    let spent = sum_micros(
        pacing
            .deposits()
            .values()
            .map(|tranche| tranche.invested_usdc),
    )?;
    if admitted != ledger.admitted_usdc()
        || committed != ledger.committed_usdc()
        || spent != ledger.spent_usdc()
    {
        return Err(RuntimeError::CapitalStateMismatch);
    }
    Ok(())
}

fn ensure_runtime_head_matches(
    state: &RuntimeState,
    ledger: &crate::ledger::ReplayState,
) -> Result<(), RuntimeError> {
    if state.last_committed_cycle_hash.as_deref() != ledger.last_runtime_cycle_hash() {
        return Err(RuntimeError::RuntimeStateRollback);
    }
    Ok(())
}

fn ensure_decision_evidence(state: &RuntimeState) -> Result<(), RuntimeError> {
    if !state
        .decision_evidence
        .keys()
        .eq(state.pacing.decisions().keys())
    {
        return Err(RuntimeError::IncompatibleRuntimeState);
    }
    Ok(())
}

fn ensure_runtime_state_authenticated(
    config: &RuntimeConfig,
    state: &RuntimeState,
    ledger: &crate::ledger::ReplayState,
) -> Result<(), RuntimeError> {
    let proof_path = config.state_directory.join(COMMITTED_CYCLE_PROOF_FILE_NAME);
    let proof = match fs::read_to_string(&proof_path) {
        Ok(payload) => Some(serde_json::from_str::<PendingRuntimeCycle>(&payload)?),
        Err(error) if error.kind() == ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    match (ledger.last_runtime_cycle_hash(), proof) {
        (None, None)
            if state == &RuntimeState::new(config.movement_history_start_ms)
                && ledger.last_event_at().is_none() =>
        {
            Ok(())
        }
        (None, None) => Err(RuntimeError::RuntimeStateRollback),
        (Some(head), Some(proof)) => {
            proof.validate(config.movement_history_start_ms)?;
            if proof.cycle_hash != head {
                return Err(RuntimeError::CommittedCycleProofMismatch);
            }
            let mut authenticated_state = proof.body.state;
            authenticated_state.last_committed_cycle_hash = Some(proof.cycle_hash);
            if &authenticated_state != state {
                return Err(RuntimeError::RuntimeStateRollback);
            }
            Ok(())
        }
        _ => Err(RuntimeError::MissingCommittedCycleProof),
    }
}

fn sum_micros(mut values: impl Iterator<Item = UsdcMicros>) -> Result<UsdcMicros, RuntimeError> {
    values
        .try_fold(0_u64, |total, value| {
            total
                .checked_add(value.as_micros())
                .ok_or(RuntimeError::CounterOverflow)
        })
        .map(UsdcMicros::from_micros)
}

fn timestamp_ms(value: u64) -> Result<DateTime<Utc>, RuntimeError> {
    let signed = i64::try_from(value)
        .map_err(|_| RuntimeError::InvalidMovement("timestamp overflow".to_owned()))?;
    Utc.timestamp_millis_opt(signed)
        .single()
        .ok_or_else(|| RuntimeError::InvalidMovement("invalid timestamp".to_owned()))
}

fn positive_usdc_micros(value: Decimal) -> Result<UsdcMicros, RuntimeError> {
    if value <= Decimal::ZERO {
        return Err(RuntimeError::InvalidMovement(
            "capital movement amount must be positive".to_owned(),
        ));
    }
    let normalized = value.normalize();
    if normalized.scale() > 6 {
        return Err(RuntimeError::InvalidMovement(
            "USDC movement has sub-microunit precision".to_owned(),
        ));
    }
    let micros = (normalized * Decimal::from(1_000_000_u64))
        .to_u64()
        .ok_or_else(|| RuntimeError::InvalidMovement("USDC amount overflow".to_owned()))?;
    if micros == 0 {
        return Err(RuntimeError::InvalidMovement(
            "USDC movement rounds to zero".to_owned(),
        ));
    }
    Ok(UsdcMicros::from_micros(micros))
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]
fn f64_usdc_micros(value: f64) -> Result<UsdcMicros, RuntimeError> {
    if !value.is_finite() || value < 0.0 {
        return Err(RuntimeError::InvalidCycle(
            "observed USDC balance is invalid".to_owned(),
        ));
    }
    let scaled = value * 1_000_000.0;
    // Floors rather than requiring near-exact microunit representability.
    // Hyperliquid's spot USDC balance is observed at up to 8 decimal places
    // (verified against the live venue: e.g. "24098.69000062"), finer than
    // USDC's own 6-decimal on-chain precision, so real accounts routinely
    // carry sub-microunit residue that can never itself be spendable
    // capital. Flooring is the conservative direction: it can only ever
    // understate observed capital, never overstate it.
    let floored = scaled.floor();
    if floored > u64::MAX as f64 {
        return Err(RuntimeError::InvalidCycle(
            "observed USDC balance overflows microunits".to_owned(),
        ));
    }
    Ok(UsdcMicros::from_micros(floored as u64))
}

/// Whether two paths name the same journal.
///
/// Compares resolved paths when both resolve, exactly as the live-probe
/// binary's own submit-time preflight does: `prepare` and `reconcile` can be
/// invoked with different spellings of the same file (relative vs absolute, a
/// symlinked parent), and a raw byte comparison would then refuse to settle a
/// real fill — leaving committed capital permanently unsettleable, which also
/// fails every later decision day closed with `PriorDecisionUnsettled`.
/// Falls back to a byte comparison when either side cannot be resolved (a
/// journal that no longer exists), which is strictly the stricter answer.
fn same_journal_path(left: &Path, right: &Path) -> bool {
    match (fs::canonicalize(left), fs::canonicalize(right)) {
        (Ok(left), Ok(right)) => left == right,
        _ => left == right,
    }
}

/// Whether two acquisition records describe the same evidence, ignoring when
/// each was recorded: a retry of the same settlement re-derives the figures
/// from the same terminal journal but observes a later clock.
fn same_acquisition(left: &RuntimeHypeAcquisition, right: &RuntimeHypeAcquisition) -> bool {
    left.workflow_id == right.workflow_id
        && same_journal_path(&left.journal, &right.journal)
        && left.credited_hype_atoms == right.credited_hype_atoms
        && left.last_fill_at == right.last_fill_at
}

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("invalid runtime configuration: {0}")]
    InvalidConfig(String),
    #[error("invalid admission artifact: {0}")]
    InvalidAdmissionArtifact(String),
    #[error("runtime cycle is invalid: {0}")]
    InvalidCycle(String),
    #[error("account movement is invalid: {0}")]
    InvalidMovement(String),
    #[error("admission approval references unknown deposit: {0}")]
    UnknownAdmissionApproval(String),
    #[error("protected ledger is missing authoritative deposit: {0}")]
    MissingAuthoritativeDeposit(String),
    #[error("persistent pacing and protected-ledger capital totals disagree")]
    CapitalStateMismatch,
    #[error("runtime state is incompatible with configured history boundary")]
    IncompatibleRuntimeState,
    #[error("authenticated pending runtime cycle is corrupt")]
    CorruptPendingCycle,
    #[error("protected ledger has an uncommitted cycle but its pending payload is missing")]
    MissingAuthenticatedPendingCycle,
    #[error("pending runtime cycle conflicts with the protected ledger")]
    PendingCycleConflict,
    #[error("runtime state does not match the protected ledger cycle head")]
    RuntimeStateRollback,
    #[error("protected ledger cycle head is missing its committed runtime proof")]
    MissingCommittedCycleProof,
    #[error("committed runtime proof does not match the protected ledger cycle head")]
    CommittedCycleProofMismatch,
    #[error("another runtime cycle already holds the state lock")]
    AlreadyRunning,
    #[error("runtime lock is not the current safe state-directory entry")]
    UnsafeRuntimeLock,
    #[error("runtime counter overflowed")]
    CounterOverflow,
    #[error("live settlement identity does not match this runtime's decision ({0})")]
    LiveDecisionMismatch(&'static str),
    #[error("live history directory mismatch: {0}")]
    LiveHistoryDirectoryMismatch(String),
    #[error("live settlement HYPE acquisition evidence is invalid: {0}")]
    InvalidHypeAcquisition(String),
    #[error("live settlement replay contradicts the recorded HYPE acquisition: {0}")]
    HypeAcquisitionConflict(String),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Pacing(#[from] PacingError),
    #[error(transparent)]
    Ledger(#[from] LedgerError),
    #[error(transparent)]
    Metrics(#[from] MetricsError),
    #[error(transparent)]
    Status(#[from] StatusError),
    #[error(transparent)]
    StatusIo(#[from] StatusIoError),
}

#[cfg(test)]
mod tests;
