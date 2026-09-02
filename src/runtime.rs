//! Crash-safe, signer-free recurring `DRY_RUN` orchestration.
//!
//! This module consumes already-normalized authoritative account movements,
//! persists the capital cursor and pacing state, mirrors economic facts into
//! the protected audit ledger, and publishes identifier-free status/metrics.
//! It deliberately has no order, staking, signing, or submission dependency.

use crate::{
    ledger::{
        DurableLedger, LedgerError, LedgerEvent, LedgerEventKind, ProtectedAnchorStore,
        ProtectedHeadAnchor,
    },
    metrics::{MetricsError, MetricsSnapshot},
    pacing::{
        CapitalEvent, DailyDecision, DecisionInput, DecisionResult, DepositEvent, PacingError,
        PacingLimits, PacingState, UsdcMicros, WithdrawalEvent,
    },
    signal::SignalSnapshot,
    status::{AccumulatorStatus, DashboardStatus, StatusError},
    status_io::{
        write_metrics_atomic, write_private_json_atomic, write_status_atomic, StatusIoError,
    },
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
    path::{Component, Path, PathBuf},
    sync::Arc,
};
use thiserror::Error;

const RUNTIME_CONFIG_SCHEMA_VERSION: u8 = 1;
const RUNTIME_STATE_SCHEMA_VERSION: u8 = 1;
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

fn normal_absolute_path(path: &Path) -> bool {
    path.is_absolute()
        && path.file_name().is_some()
        && path.components().all(|component| {
            matches!(
                component,
                Component::Prefix(_) | Component::RootDir | Component::Normal(_)
            )
        })
}

fn reject_linked_file(path: &Path) -> Result<(), io::Error> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(io::Error::new(
            ErrorKind::PermissionDenied,
            "symbolic links are forbidden",
        )),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn reject_multiple_links(file: &File) -> Result<(), io::Error> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if file.metadata()?.nlink() != 1 {
            return Err(io::Error::new(
                ErrorKind::PermissionDenied,
                "multiple hard links are forbidden",
            ));
        }
    }
    Ok(())
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
        for path in paths {
            if !unique_paths.insert((*path).clone()) {
                return Err(RuntimeError::InvalidConfig(
                    "runtime paths must be distinct".to_owned(),
                ));
            }
        }
        if wire.movement_history_start_ms == 0
            || wire.movement_overlap_ms == 0
            || wire.stuck_after_seconds == 0
            || wire.account_observation_max_age_seconds == 0
            || i64::try_from(wire.account_observation_max_age_seconds).is_err()
        {
            return Err(RuntimeError::InvalidConfig(
                "runtime history and alert bounds must be positive".to_owned(),
            ));
        }
        Ok(Self {
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
        })
    }

    #[must_use]
    pub fn admission_approvals_path(&self) -> &Path {
        &self.admission_approvals_path
    }

    #[must_use]
    pub fn signal_snapshot_path(&self) -> &Path {
        &self.signal_snapshot_path
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
    api_errors_total: u64,
    stale_signal_events_total: u64,
    dry_run_actions_total: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    last_committed_cycle_hash: Option<String>,
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
        Ok(())
    }
}

impl RuntimeState {
    fn new(movement_history_start_ms: u64) -> Self {
        Self {
            schema_version: RUNTIME_STATE_SCHEMA_VERSION,
            movement_history_start_ms,
            last_complete_scan_end_ms: None,
            pacing: PacingState::default(),
            api_errors_total: 0,
            stale_signal_events_total: 0,
            dry_run_actions_total: 0,
            last_committed_cycle_hash: None,
        }
    }
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
    _lock: File,
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
        let lock_path = config.state_directory.join(RUNTIME_LOCK_FILE_NAME);
        let mut options = OpenOptions::new();
        options.create(true).read(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let lock = options.open(lock_path)?;
        lock.try_lock_exclusive().map_err(|error| {
            if error.kind() == ErrorKind::WouldBlock {
                RuntimeError::AlreadyRunning
            } else {
                RuntimeError::Io(error)
            }
        })?;
        let state_path = config.state_directory.join(STATE_FILE_NAME);
        let mut state = load_runtime_state(&state_path, config.movement_history_start_ms)?;
        state.pacing.validate_for_limits(&limits)?;
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
        Ok(Self {
            config,
            limits,
            state,
            ledger,
            process_started_at: Utc::now(),
            _lock: lock,
        })
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
        validate_cycle_range(
            &input,
            self.next_scan_start_ms(),
            self.config.account_observation_max_age_seconds,
        )?;
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
        let boundary_balance_available = scheduled_boundary
            .is_some_and(|boundary| *input.accumulator.balance_observed_at() == boundary);
        let boundary_replay_safe = scheduled_boundary.is_some_and(|boundary| {
            existing_decision.is_none()
                && self
                    .state
                    .pacing
                    .capital_reconciled_through()
                    .is_none_or(|watermark| watermark <= boundary)
        });
        let mut capital_history_complete = input.capital_history_complete;
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

        let mut ordered_movements = input.movements.iter().collect::<Vec<_>>();
        ordered_movements.sort_by_key(|movement| (movement.timestamp_ms, &movement.event_id));
        for movement in ordered_movements {
            validate_movement_range(movement, &input)?;
            if movement.token != "USDC" {
                continue;
            }
            match movement.kind {
                HyperliquidAccountMovementKind::ExternalDeposit => {
                    let amount = positive_usdc_micros(movement.amount)?;
                    let occurred_at = timestamp_ms(movement.timestamp_ms)?;
                    movement_ledger_events.push(LedgerEvent {
                        event_id: movement.event_id.clone(),
                        occurred_at,
                        kind: LedgerEventKind::AuthoritativeDeposit {
                            amount_usdc: amount,
                        },
                    });
                    let approval = input.approvals.get(&movement.event_id);
                    capital_events.push(CapitalEvent::Deposit(DepositEvent {
                        event_id: movement.event_id.clone(),
                        amount_usdc: amount,
                        received_at: occurred_at,
                        confirmed_at: approval.map(|value| value.confirmed_at),
                        confirmation_count: approval.map_or(0, |value| value.confirmation_count),
                        admission_approved_at: approval.map(|value| value.approved_at),
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
                HyperliquidAccountMovementKind::ExternalWithdrawal => {
                    let amount = positive_usdc_micros(movement.amount.abs())?;
                    let occurred_at = timestamp_ms(movement.timestamp_ms)?;
                    movement_ledger_events.push(LedgerEvent {
                        event_id: movement.event_id.clone(),
                        occurred_at,
                        kind: LedgerEventKind::AuthoritativeWithdrawal {
                            amount_usdc: amount,
                        },
                    });
                    capital_events.push(CapitalEvent::Withdrawal(WithdrawalEvent {
                        event_id: movement.event_id.clone(),
                        amount_usdc: amount,
                        occurred_at,
                        reconciled_at: input.observed_at,
                    }));
                }
                HyperliquidAccountMovementKind::Unknown => {
                    capital_history_complete = false;
                }
                HyperliquidAccountMovementKind::InternalTransfer
                | HyperliquidAccountMovementKind::TradingRelated => {}
            }
        }
        for approval_id in input.approvals.0.keys() {
            if !observed_deposit_ids.contains(approval_id) {
                return Err(RuntimeError::UnknownAdmissionApproval(approval_id.clone()));
            }
        }

        let observed_spot_usdc = f64_usdc_micros(input.accumulator.usdc_balance())?;
        let mut next_state = self.state.clone();
        let mut ledger_events = Vec::new();
        let decision_result = if let Some(decision) = existing_decision {
            next_state.pacing.reconcile_capital(
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
            next_state.pacing.reconcile_capital(
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
            ));
            let boundary_pacing = next_state.pacing.clone();
            let decision_input = DecisionInput {
                at: boundary,
                observed_spot_usdc: if boundary_balance_available {
                    observed_spot_usdc
                } else {
                    UsdcMicros::default()
                },
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
                ledger_events.extend(dry_run_decision_events(&mut next_state.pacing, result)?);
            }
            next_state.pacing.reconcile_capital(
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
            ));
            decision
        } else {
            next_state.pacing.reconcile_capital(
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
            ));
            None
        };
        if let Some(result) = &decision_result {
            if result.is_new() && !result.decision().planned_usdc.is_zero() {
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
            economic_action_suppressed: true,
            signed_action_created: false,
            signal_available: decision_signal.is_some(),
            boundary_balance_available,
        };
        let pending = PendingRuntimeCycle::new(input.observed_at, next_state, ledger_events)?;
        write_private_json_atomic(
            self.config.state_directory.join(PENDING_CYCLE_FILE_NAME),
            &pending,
        )?;
        self.state = commit_pending_cycle(&self.config, &self.limits, &mut self.ledger, &pending)?;
        let metrics = MetricsSnapshot::from_runtime(
            input.observed_at,
            &self.state.pacing,
            &self.limits,
            self.ledger.state(),
            &[],
            decision_signal,
            self.state.api_errors_total,
            self.state.stale_signal_events_total,
            self.state.dry_run_actions_total,
            self.config.stuck_after_seconds,
        )?;
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
    let mut resolved_paths = BTreeSet::new();
    for path in config.configured_file_paths() {
        let parent = path.parent().ok_or_else(|| {
            RuntimeError::InvalidConfig("configured runtime file has no parent".to_owned())
        })?;
        fs::create_dir_all(parent)?;
        let canonical_parent = fs::canonicalize(parent)?;
        if canonical_parent.starts_with(&canonical_state) {
            return Err(RuntimeError::InvalidConfig(
                "configured runtime file resolves inside the reserved state directory".to_owned(),
            ));
        }
        let file_name = path.file_name().ok_or_else(|| {
            RuntimeError::InvalidConfig("configured runtime file has no file name".to_owned())
        })?;
        if !resolved_paths.insert(canonical_parent.join(file_name)) {
            return Err(RuntimeError::InvalidConfig(
                "configured runtime files resolve to the same path".to_owned(),
            ));
        }
        reject_linked_file(path)?;
    }
    Ok(())
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
    ledger.append(LedgerEvent {
        event_id: format!("runtime-cycle:{}:prepared", pending.cycle_hash),
        occurred_at: pending.body.observed_at,
        kind: LedgerEventKind::RuntimeCyclePrepared {
            cycle_hash: pending.cycle_hash.clone(),
        },
    })?;
    for event in &pending.body.ledger_events {
        ledger.append(event.clone())?;
    }
    ledger.append(LedgerEvent {
        event_id: format!("runtime-cycle:{}:committed", pending.cycle_hash),
        occurred_at: pending.body.observed_at,
        kind: LedgerEventKind::RuntimeCycleCommitted {
            cycle_hash: pending.cycle_hash.clone(),
        },
    })?;
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
                } else if deposit
                    .admission_approved_at
                    .is_some_and(|approved_at| approved_at > at)
                {
                    deposit.admission_approved_at = None;
                }
                Some(CapitalEvent::Deposit(deposit))
            }
            CapitalEvent::Withdrawal(withdrawal) if withdrawal.occurred_at <= at => {
                let mut withdrawal = withdrawal.clone();
                withdrawal.reconciled_at = at;
                Some(CapitalEvent::Withdrawal(withdrawal))
            }
            CapitalEvent::Deposit(_) | CapitalEvent::Withdrawal(_) => None,
        })
        .collect()
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
) -> Vec<LedgerEvent> {
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
        LedgerEventKind::AuthoritativeDeposit { .. } => 0,
        LedgerEventKind::DepositAdmission { .. } => 1,
        LedgerEventKind::AuthoritativeWithdrawal { .. } => 2,
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
    let rounded = scaled.round();
    if (scaled - rounded).abs() > 1e-4 || rounded > u64::MAX as f64 {
        return Err(RuntimeError::InvalidCycle(
            "observed USDC balance is not representable in microunits".to_owned(),
        ));
    }
    Ok(UsdcMicros::from_micros(rounded as u64))
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
    #[error("runtime counter overflowed")]
    CounterOverflow,
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
