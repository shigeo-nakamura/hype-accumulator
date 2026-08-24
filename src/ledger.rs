//! Durable, tamper-evident audit ledger and local restore boundary.
//!
//! This module is deliberately transport-agnostic. It writes only to caller-
//! supplied local directories and never submits, signs, uploads, or deploys
//! anything.

use crate::pacing::UsdcMicros;
use chrono::{DateTime, NaiveDate, Utc};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};
use thiserror::Error;

pub const LEDGER_SCHEMA_VERSION: u8 = 1;
pub const SNAPSHOT_SCHEMA_VERSION: u8 = 1;
pub const LEDGER_FILE_NAME: &str = "ledger.jsonl";
pub const SNAPSHOT_FILE_NAME: &str = "snapshot.json";
const LOCK_FILE_NAME: &str = ".ledger.lock";
const PENDING_FILE_NAME: &str = ".pending-append.json";
const PENDING_SCHEMA_VERSION: u8 = 1;
const GENESIS_HASH: &str = "0000000000000000000000000000000000000000000000000000000000000000";

/// Legacy in-memory observation retained for callers that have not migrated
/// to [`DurableLedger`]. It is not part of the durable replay contract.
#[derive(Clone, Debug, PartialEq)]
pub struct LedgerEntry {
    pub at: DateTime<Utc>,
    pub admitted_deposit_usdc: f64,
    pub deployed_usdc: f64,
}

pub trait Ledger: Send {
    /// Appends one immutable observation.
    ///
    /// # Errors
    ///
    /// Returns an implementation-specific persistence error.
    fn append(&mut self, entry: LedgerEntry) -> Result<(), String>;
}

#[derive(Default)]
pub struct MemoryLedger(pub Vec<LedgerEntry>);

impl Ledger for MemoryLedger {
    fn append(&mut self, entry: LedgerEntry) -> Result<(), String> {
        self.0.push(entry);
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LedgerEvent {
    pub event_id: String,
    pub occurred_at: DateTime<Utc>,
    pub kind: LedgerEventKind,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum LedgerEventKind {
    AuthoritativeDeposit {
        amount_usdc: UsdcMicros,
    },
    DepositAdmission {
        deposit_event_id: String,
        amount_usdc: UsdcMicros,
    },
    AuthoritativeWithdrawal {
        amount_usdc: UsdcMicros,
    },
    CapitalCommitted {
        commitment_id: String,
        amount_usdc: UsdcMicros,
    },
    CapitalSettled {
        commitment_id: String,
        debited_usdc: UsdcMicros,
    },
    BalanceObserved {
        observed_usdc: UsdcMicros,
        observed_hype_atoms: u64,
    },
    DailyDecision {
        decision_id: String,
        decision_date: NaiveDate,
        planned_usdc: UsdcMicros,
        committed_usdc: UsdcMicros,
    },
    DailySkip {
        decision_id: String,
        decision_date: NaiveDate,
        reason: String,
    },
    OrderRecorded {
        order_id: String,
        decision_id: String,
    },
    FillRecorded {
        order_id: String,
        filled_usdc: UsdcMicros,
        received_hype_atoms: u64,
    },
    FeeRecorded {
        order_id: String,
        fee_usdc: UsdcMicros,
    },
    StakingDepositRecorded {
        action_id: String,
        hype_atoms: u64,
    },
    DelegationRecorded {
        action_id: String,
        validator_id: String,
        hype_atoms: u64,
    },
    RewardRecorded {
        reward_id: String,
        hype_atoms: u64,
    },
    ReconciliationCorrection {
        correction_id: String,
        observed_usdc: UsdcMicros,
        observed_hype_atoms: u64,
        reason: String,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LedgerEnvelope {
    pub schema_version: u8,
    pub sequence: u64,
    pub previous_hash: String,
    pub event: LedgerEvent,
    pub record_hash: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AppendOutcome {
    Appended,
    Duplicate,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct DepositReplay {
    authoritative_usdc: UsdcMicros,
    admitted_usdc: UsdcMicros,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct CommitmentReplay {
    committed_usdc: UsdcMicros,
    debited_usdc: UsdcMicros,
    settled: bool,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayState {
    deposits: BTreeMap<String, DepositReplay>,
    commitments: BTreeMap<String, CommitmentReplay>,
    admitted_usdc: UsdcMicros,
    withdrawn_usdc: UsdcMicros,
    committed_usdc: UsdcMicros,
    spent_usdc: UsdcMicros,
    observed_usdc: UsdcMicros,
    observed_hype_atoms: u64,
    last_event_at: Option<DateTime<Utc>>,
}

impl ReplayState {
    #[must_use]
    pub const fn admitted_usdc(&self) -> UsdcMicros {
        self.admitted_usdc
    }

    #[must_use]
    pub const fn withdrawn_usdc(&self) -> UsdcMicros {
        self.withdrawn_usdc
    }

    #[must_use]
    pub const fn committed_usdc(&self) -> UsdcMicros {
        self.committed_usdc
    }

    #[must_use]
    pub const fn spent_usdc(&self) -> UsdcMicros {
        self.spent_usdc
    }

    #[must_use]
    pub const fn observed_usdc(&self) -> UsdcMicros {
        self.observed_usdc
    }

    #[must_use]
    pub const fn observed_hype_atoms(&self) -> u64 {
        self.observed_hype_atoms
    }

    #[must_use]
    pub const fn last_event_at(&self) -> Option<&DateTime<Utc>> {
        self.last_event_at.as_ref()
    }

    #[must_use]
    pub fn deployable_usdc(&self) -> UsdcMicros {
        let used = self
            .withdrawn_usdc
            .as_micros()
            .saturating_add(self.committed_usdc.as_micros())
            .saturating_add(self.spent_usdc.as_micros());
        UsdcMicros::from_micros(self.admitted_usdc.as_micros().saturating_sub(used))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LedgerSnapshot {
    ledger_schema_version: u8,
    record_count: u64,
    head_hash: String,
    state: ReplayState,
}

impl LedgerSnapshot {
    #[must_use]
    pub const fn record_count(&self) -> u64 {
        self.record_count
    }

    #[must_use]
    pub fn head_hash(&self) -> &str {
        &self.head_hash
    }

    #[must_use]
    pub const fn state(&self) -> &ReplayState {
        &self.state
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct SnapshotEnvelope {
    schema_version: u8,
    snapshot: LedgerSnapshot,
    checksum: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct PendingAppend {
    schema_version: u8,
    prior_file_len: u64,
    prior_record_count: u64,
    prior_head_hash: String,
    record: LedgerEnvelope,
    snapshot: LedgerSnapshot,
    checksum: String,
}

#[derive(Serialize)]
struct RecordHashInput<'a> {
    schema_version: u8,
    sequence: u64,
    previous_hash: &'a str,
    event: &'a LedgerEvent,
}

#[derive(Serialize)]
struct SnapshotHashInput<'a> {
    schema_version: u8,
    snapshot: &'a LedgerSnapshot,
}

#[derive(Serialize)]
struct PendingHashInput<'a> {
    schema_version: u8,
    prior_file_len: u64,
    prior_record_count: u64,
    prior_head_hash: &'a str,
    record: &'a LedgerEnvelope,
    snapshot: &'a LedgerSnapshot,
}

pub struct DurableLedger {
    directory: PathBuf,
    records: Vec<LedgerEnvelope>,
    events_by_id: BTreeMap<String, LedgerEvent>,
    state: ReplayState,
    file_len: u64,
}

struct PreparedAppend {
    record: LedgerEnvelope,
    snapshot: LedgerSnapshot,
    next_state: ReplayState,
    pending: PendingAppend,
}

impl DurableLedger {
    /// Opens a ledger and verifies its journal, hash chain, and snapshot anchor.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] for malformed, truncated, hash-invalid, or
    /// snapshot-inconsistent state.
    pub fn open(directory: impl AsRef<Path>) -> Result<Self, LedgerError> {
        let directory = directory.as_ref().to_path_buf();
        let _lock = LedgerLock::acquire(&directory)?;
        recover_pending(&directory)?;
        Self::open_unlocked(directory)
    }

    fn open_unlocked(directory: PathBuf) -> Result<Self, LedgerError> {
        let payload = read_optional(&directory.join(LEDGER_FILE_NAME))?;
        let records = load_records(&payload)?;
        let events = records
            .iter()
            .map(|record| record.event.clone())
            .collect::<Vec<_>>();
        let state = replay(&events)?;
        validate_current_snapshot(&directory, &records)?;
        let mut events_by_id = BTreeMap::new();
        for event in events {
            if events_by_id.insert(event.event_id.clone(), event).is_some() {
                return Err(LedgerError::CorruptLedger(
                    "duplicate event ID in journal".into(),
                ));
            }
        }
        Ok(Self {
            directory,
            records,
            events_by_id,
            state,
            file_len: u64::try_from(payload.len())
                .map_err(|_| LedgerError::CorruptLedger("file length overflowed".into()))?,
        })
    }

    #[must_use]
    pub const fn state(&self) -> &ReplayState {
        &self.state
    }

    #[must_use]
    pub fn head_hash(&self) -> &str {
        self.records
            .last()
            .map_or(GENESIS_HASH, |record| record.record_hash.as_str())
    }

    #[must_use]
    pub fn record_count(&self) -> usize {
        self.records.len()
    }

    /// Appends one validated event and fsyncs it before returning.
    ///
    /// Replaying the exact same event ID and payload is idempotent. Reusing an
    /// ID for different content fails closed.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] for invalid transitions, ID collisions,
    /// concurrent file changes, serialization, or durable-write failures.
    pub fn append(&mut self, event: LedgerEvent) -> Result<AppendOutcome, LedgerError> {
        validate_event(&event)?;
        let _lock = LedgerLock::acquire(&self.directory)?;
        recover_pending(&self.directory)?;
        *self = Self::open_unlocked(self.directory.clone())?;
        if let Some(existing) = self.events_by_id.get(&event.event_id) {
            return if existing == &event {
                Ok(AppendOutcome::Duplicate)
            } else {
                Err(LedgerError::EventCollision(event.event_id))
            };
        }
        let prepared = self.prepare_append(event)?;
        write_pending(&self.directory, &prepared.pending)?;
        let next_file_len = self.write_record(&prepared.record)?;
        write_snapshot(&self.directory, &prepared.snapshot)?;
        clear_pending(&self.directory)?;
        self.events_by_id.insert(
            prepared.record.event.event_id.clone(),
            prepared.record.event.clone(),
        );
        self.records.push(prepared.record);
        self.state = prepared.next_state;
        self.file_len = next_file_len;
        Ok(AppendOutcome::Appended)
    }

    /// Atomically writes a checksummed replay snapshot.
    ///
    /// The checksum and snapshot share one atomically-renamed envelope, so a
    /// crash cannot publish one without the other.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] if the journal changed concurrently or the
    /// snapshot cannot be serialized or durably replaced.
    pub fn checkpoint(&self) -> Result<LedgerSnapshot, LedgerError> {
        let _lock = LedgerLock::acquire(&self.directory)?;
        if recover_pending(&self.directory)? {
            return Err(LedgerError::ConcurrentModification);
        }
        self.ensure_current()?;
        let snapshot = LedgerSnapshot {
            ledger_schema_version: LEDGER_SCHEMA_VERSION,
            record_count: u64::try_from(self.records.len())
                .map_err(|_| LedgerError::CorruptLedger("record count overflowed".into()))?,
            head_hash: self.head_hash().to_owned(),
            state: self.state.clone(),
        };
        write_snapshot(&self.directory, &snapshot)?;
        Ok(snapshot)
    }

    /// Restores a fully checkpointed ledger into a missing or empty directory.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] for a non-clean destination, missing/stale
    /// snapshot, source mutation, verification failure, or local I/O failure.
    pub fn restore_clean(
        source: impl AsRef<Path>,
        destination: impl AsRef<Path>,
    ) -> Result<Self, LedgerError> {
        let source = source.as_ref();
        let destination = destination.as_ref();
        if source == destination {
            return Err(LedgerError::RestoreDestinationNotEmpty);
        }
        let _source_lock = LedgerLock::acquire(source)?;
        recover_pending(source)?;
        let verified = Self::open_unlocked(source.to_path_buf())?;
        let snapshot_payload = read_optional(&source.join(SNAPSHOT_FILE_NAME))?;
        if snapshot_payload.is_empty() {
            return Err(LedgerError::MissingSnapshot);
        }
        let snapshot = decode_snapshot(&snapshot_payload)?;
        if snapshot.record_count
            != u64::try_from(verified.records.len())
                .map_err(|_| LedgerError::CorruptLedger("record count overflowed".into()))?
            || snapshot.head_hash != verified.head_hash()
            || snapshot.state != verified.state
        {
            return Err(LedgerError::StaleSnapshot);
        }
        let ledger_payload = read_optional(&source.join(LEDGER_FILE_NAME))?;
        let source_records = load_records(&ledger_payload)?;
        if source_records != verified.records {
            return Err(LedgerError::ConcurrentModification);
        }
        ensure_clean_directory(destination)?;
        let _destination_lock = LedgerLock::acquire(destination)?;
        ensure_clean_directory(destination)?;
        write_atomic(&destination.join(LEDGER_FILE_NAME), &ledger_payload)?;
        write_atomic(&destination.join(SNAPSHOT_FILE_NAME), &snapshot_payload)?;
        let restored = Self::open_unlocked(destination.to_path_buf())?;
        if restored.records != verified.records || restored.state != verified.state {
            return Err(LedgerError::SnapshotMismatch);
        }
        Ok(restored)
    }

    fn prepare_append(&self, event: LedgerEvent) -> Result<PreparedAppend, LedgerError> {
        let mut events = self
            .records
            .iter()
            .map(|record| record.event.clone())
            .collect::<Vec<_>>();
        events.push(event.clone());
        let next_state = replay(&events)?;
        let sequence = u64::try_from(self.records.len())
            .map_err(|_| LedgerError::CorruptLedger("sequence overflowed".into()))?;
        let previous_hash = self.head_hash().to_owned();
        let record = LedgerEnvelope {
            schema_version: LEDGER_SCHEMA_VERSION,
            sequence,
            previous_hash: previous_hash.clone(),
            record_hash: record_hash(sequence, &previous_hash, &event)?,
            event,
        };
        let snapshot = LedgerSnapshot {
            ledger_schema_version: LEDGER_SCHEMA_VERSION,
            record_count: sequence
                .checked_add(1)
                .ok_or_else(|| LedgerError::CorruptLedger("record count overflowed".into()))?,
            head_hash: record.record_hash.clone(),
            state: next_state.clone(),
        };
        let pending = build_pending(self.file_len, sequence, previous_hash, &record, &snapshot)?;
        Ok(PreparedAppend {
            record,
            snapshot,
            next_state,
            pending,
        })
    }

    fn write_record(&self, record: &LedgerEnvelope) -> Result<u64, LedgerError> {
        fs::create_dir_all(&self.directory).map_err(LedgerError::io)?;
        let path = self.directory.join(LEDGER_FILE_NAME);
        let created = !path.exists();
        let line = record_line(record)?;
        let next_file_len = self
            .file_len
            .checked_add(
                u64::try_from(line.len())
                    .map_err(|_| LedgerError::CorruptLedger("file length overflowed".into()))?,
            )
            .ok_or_else(|| LedgerError::CorruptLedger("file length overflowed".into()))?;
        let mut options = OpenOptions::new();
        options.create(true).append(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o640);
        }
        let mut file = options.open(&path).map_err(LedgerError::io)?;
        file.write_all(&line).map_err(LedgerError::io)?;
        file.sync_all().map_err(LedgerError::io)?;
        if created {
            sync_directory(&self.directory)?;
        }
        Ok(next_file_len)
    }

    fn ensure_current(&self) -> Result<(), LedgerError> {
        let payload = read_optional(&self.directory.join(LEDGER_FILE_NAME))?;
        let current_len = u64::try_from(payload.len())
            .map_err(|_| LedgerError::CorruptLedger("file length overflowed".into()))?;
        let records = load_records(&payload)?;
        if current_len != self.file_len || records != self.records {
            Err(LedgerError::ConcurrentModification)
        } else {
            validate_current_snapshot(&self.directory, &records)
        }
    }
}

fn replay(events: &[LedgerEvent]) -> Result<ReplayState, LedgerError> {
    let mut state = ReplayState::default();
    let mut event_ids = BTreeSet::new();
    for event in events {
        validate_event(event)?;
        if !event_ids.insert(event.event_id.as_str()) {
            return Err(LedgerError::CorruptLedger(
                "duplicate event ID in journal".into(),
            ));
        }
        apply_event(&mut state, event)?;
        state.last_event_at = Some(event.occurred_at);
    }
    Ok(state)
}

fn apply_event(state: &mut ReplayState, event: &LedgerEvent) -> Result<(), LedgerError> {
    match &event.kind {
        LedgerEventKind::AuthoritativeDeposit { amount_usdc } => {
            state.deposits.insert(
                event.event_id.clone(),
                DepositReplay {
                    authoritative_usdc: *amount_usdc,
                    admitted_usdc: UsdcMicros::default(),
                },
            );
        }
        LedgerEventKind::DepositAdmission {
            deposit_event_id,
            amount_usdc,
        } => {
            let deposit = state
                .deposits
                .get_mut(deposit_event_id)
                .ok_or_else(|| LedgerError::UnknownDeposit(deposit_event_id.clone()))?;
            let next_admitted = checked_add(deposit.admitted_usdc, *amount_usdc)?;
            if next_admitted > deposit.authoritative_usdc {
                return Err(LedgerError::AdmissionExceedsDeposit(
                    deposit_event_id.clone(),
                ));
            }
            deposit.admitted_usdc = next_admitted;
            state.admitted_usdc = checked_add(state.admitted_usdc, *amount_usdc)?;
        }
        LedgerEventKind::AuthoritativeWithdrawal { amount_usdc } => {
            require_deployable(state, *amount_usdc)?;
            state.withdrawn_usdc = checked_add(state.withdrawn_usdc, *amount_usdc)?;
        }
        LedgerEventKind::CapitalCommitted {
            commitment_id,
            amount_usdc,
        } => {
            if state.commitments.contains_key(commitment_id) {
                return Err(LedgerError::CommitmentCollision(commitment_id.clone()));
            }
            require_deployable(state, *amount_usdc)?;
            state.committed_usdc = checked_add(state.committed_usdc, *amount_usdc)?;
            state.commitments.insert(
                commitment_id.clone(),
                CommitmentReplay {
                    committed_usdc: *amount_usdc,
                    debited_usdc: UsdcMicros::default(),
                    settled: false,
                },
            );
        }
        LedgerEventKind::CapitalSettled {
            commitment_id,
            debited_usdc,
        } => {
            let commitment = state
                .commitments
                .get_mut(commitment_id)
                .ok_or_else(|| LedgerError::UnknownCommitment(commitment_id.clone()))?;
            if commitment.settled {
                return Err(LedgerError::CommitmentAlreadySettled(commitment_id.clone()));
            }
            if *debited_usdc > commitment.committed_usdc {
                return Err(LedgerError::DebitExceedsCommitment(commitment_id.clone()));
            }
            state.committed_usdc = checked_sub(state.committed_usdc, commitment.committed_usdc)?;
            state.spent_usdc = checked_add(state.spent_usdc, *debited_usdc)?;
            commitment.debited_usdc = *debited_usdc;
            commitment.settled = true;
        }
        LedgerEventKind::BalanceObserved {
            observed_usdc,
            observed_hype_atoms,
        }
        | LedgerEventKind::ReconciliationCorrection {
            observed_usdc,
            observed_hype_atoms,
            ..
        } => {
            state.observed_usdc = *observed_usdc;
            state.observed_hype_atoms = *observed_hype_atoms;
        }
        LedgerEventKind::DailyDecision { .. }
        | LedgerEventKind::DailySkip { .. }
        | LedgerEventKind::OrderRecorded { .. }
        | LedgerEventKind::FillRecorded { .. }
        | LedgerEventKind::FeeRecorded { .. }
        | LedgerEventKind::StakingDepositRecorded { .. }
        | LedgerEventKind::DelegationRecorded { .. }
        | LedgerEventKind::RewardRecorded { .. } => {}
    }
    Ok(())
}

fn validate_event(event: &LedgerEvent) -> Result<(), LedgerError> {
    validate_id("event_id", &event.event_id)?;
    match &event.kind {
        LedgerEventKind::AuthoritativeDeposit { amount_usdc }
        | LedgerEventKind::AuthoritativeWithdrawal { amount_usdc } => {
            require_nonzero(*amount_usdc)?;
        }
        LedgerEventKind::DepositAdmission {
            deposit_event_id,
            amount_usdc,
        } => {
            validate_id("deposit_event_id", deposit_event_id)?;
            require_nonzero(*amount_usdc)?;
        }
        LedgerEventKind::CapitalCommitted {
            commitment_id,
            amount_usdc,
        } => {
            validate_id("commitment_id", commitment_id)?;
            require_nonzero(*amount_usdc)?;
        }
        LedgerEventKind::CapitalSettled { commitment_id, .. } => {
            validate_id("commitment_id", commitment_id)?;
        }
        LedgerEventKind::BalanceObserved { .. } => {}
        LedgerEventKind::DailyDecision {
            decision_id,
            planned_usdc,
            committed_usdc,
            ..
        } => {
            validate_id("decision_id", decision_id)?;
            require_nonzero(*planned_usdc)?;
            if *committed_usdc < *planned_usdc {
                return Err(LedgerError::InvalidEvent(
                    "decision commitment is below planned notional".into(),
                ));
            }
        }
        LedgerEventKind::DailySkip {
            decision_id,
            reason,
            ..
        } => {
            validate_id("decision_id", decision_id)?;
            validate_text("reason", reason)?;
        }
        LedgerEventKind::OrderRecorded {
            order_id,
            decision_id,
        } => {
            validate_id("order_id", order_id)?;
            validate_id("decision_id", decision_id)?;
        }
        LedgerEventKind::FillRecorded {
            order_id,
            filled_usdc,
            received_hype_atoms,
        } => {
            validate_id("order_id", order_id)?;
            require_nonzero(*filled_usdc)?;
            require_hype(*received_hype_atoms)?;
        }
        LedgerEventKind::FeeRecorded { order_id, fee_usdc } => {
            validate_id("order_id", order_id)?;
            require_nonzero(*fee_usdc)?;
        }
        LedgerEventKind::StakingDepositRecorded {
            action_id,
            hype_atoms,
        } => {
            validate_id("action_id", action_id)?;
            require_hype(*hype_atoms)?;
        }
        LedgerEventKind::DelegationRecorded {
            action_id,
            validator_id,
            hype_atoms,
        } => {
            validate_id("action_id", action_id)?;
            validate_id("validator_id", validator_id)?;
            require_hype(*hype_atoms)?;
        }
        LedgerEventKind::RewardRecorded {
            reward_id,
            hype_atoms,
        } => {
            validate_id("reward_id", reward_id)?;
            require_hype(*hype_atoms)?;
        }
        LedgerEventKind::ReconciliationCorrection {
            correction_id,
            reason,
            ..
        } => {
            validate_id("correction_id", correction_id)?;
            validate_text("reason", reason)?;
        }
    }
    Ok(())
}

fn validate_current_snapshot(
    directory: &Path,
    records: &[LedgerEnvelope],
) -> Result<(), LedgerError> {
    match load_snapshot(&directory.join(SNAPSHOT_FILE_NAME))? {
        Some(snapshot) => validate_snapshot_anchor(&snapshot, records),
        None if records.is_empty() => Ok(()),
        None => Err(LedgerError::MissingSnapshot),
    }
}

fn validate_snapshot_anchor(
    snapshot: &LedgerSnapshot,
    records: &[LedgerEnvelope],
) -> Result<(), LedgerError> {
    if snapshot.ledger_schema_version != LEDGER_SCHEMA_VERSION {
        return Err(LedgerError::SnapshotMismatch);
    }
    let count =
        usize::try_from(snapshot.record_count).map_err(|_| LedgerError::SnapshotMismatch)?;
    if count > records.len() {
        return Err(LedgerError::TruncatedLedger);
    }
    if count < records.len() {
        return Err(LedgerError::StaleSnapshot);
    }
    let expected_head = if count == 0 {
        GENESIS_HASH
    } else {
        &records[count - 1].record_hash
    };
    if snapshot.head_hash != expected_head {
        return Err(LedgerError::SnapshotMismatch);
    }
    let events = records
        .iter()
        .map(|record| record.event.clone())
        .collect::<Vec<_>>();
    if replay(&events)? != snapshot.state {
        return Err(LedgerError::SnapshotMismatch);
    }
    Ok(())
}

fn load_records(payload: &[u8]) -> Result<Vec<LedgerEnvelope>, LedgerError> {
    if payload.is_empty() {
        return Ok(Vec::new());
    }
    if payload.last() != Some(&b'\n') {
        return Err(LedgerError::TruncatedLedger);
    }
    let mut records = Vec::new();
    let mut expected_previous_hash = GENESIS_HASH.to_owned();
    for line in payload
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let record: LedgerEnvelope = serde_json::from_slice(line).map_err(LedgerError::json)?;
        let expected_sequence = u64::try_from(records.len())
            .map_err(|_| LedgerError::CorruptLedger("sequence overflowed".into()))?;
        if record.schema_version != LEDGER_SCHEMA_VERSION
            || record.sequence != expected_sequence
            || record.previous_hash != expected_previous_hash
            || record.record_hash
                != record_hash(record.sequence, &record.previous_hash, &record.event)?
        {
            return Err(LedgerError::CorruptLedger(
                "record sequence or hash chain is invalid".into(),
            ));
        }
        expected_previous_hash.clone_from(&record.record_hash);
        records.push(record);
    }
    Ok(records)
}

fn load_snapshot(path: &Path) -> Result<Option<LedgerSnapshot>, LedgerError> {
    match fs::read(path) {
        Ok(payload) if payload.is_empty() => Err(LedgerError::CorruptSnapshot),
        Ok(payload) => decode_snapshot(&payload).map(Some),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(LedgerError::io(error)),
    }
}

fn decode_snapshot(payload: &[u8]) -> Result<LedgerSnapshot, LedgerError> {
    let envelope: SnapshotEnvelope =
        serde_json::from_slice(payload).map_err(|_| LedgerError::CorruptSnapshot)?;
    if envelope.schema_version != SNAPSHOT_SCHEMA_VERSION
        || envelope.checksum != snapshot_checksum(&envelope.snapshot)?
    {
        return Err(LedgerError::CorruptSnapshot);
    }
    Ok(envelope.snapshot)
}

fn build_pending(
    prior_file_len: u64,
    prior_record_count: u64,
    prior_head_hash: String,
    record: &LedgerEnvelope,
    snapshot: &LedgerSnapshot,
) -> Result<PendingAppend, LedgerError> {
    let mut pending = PendingAppend {
        schema_version: PENDING_SCHEMA_VERSION,
        prior_file_len,
        prior_record_count,
        prior_head_hash,
        record: record.clone(),
        snapshot: snapshot.clone(),
        checksum: String::new(),
    };
    pending.checksum = pending_checksum(&pending)?;
    Ok(pending)
}

fn load_pending(directory: &Path) -> Result<Option<PendingAppend>, LedgerError> {
    match fs::read(directory.join(PENDING_FILE_NAME)) {
        Ok(payload) if payload.is_empty() => Err(LedgerError::CorruptPending),
        Ok(payload) => decode_pending(&payload).map(Some),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(LedgerError::io(error)),
    }
}

fn decode_pending(payload: &[u8]) -> Result<PendingAppend, LedgerError> {
    let pending: PendingAppend =
        serde_json::from_slice(payload).map_err(|_| LedgerError::CorruptPending)?;
    let expected_count = pending
        .prior_record_count
        .checked_add(1)
        .ok_or(LedgerError::CorruptPending)?;
    if pending.schema_version != PENDING_SCHEMA_VERSION
        || pending.checksum != pending_checksum(&pending)?
        || pending.record.schema_version != LEDGER_SCHEMA_VERSION
        || pending.record.sequence != pending.prior_record_count
        || pending.record.previous_hash != pending.prior_head_hash
        || pending.record.record_hash
            != record_hash(
                pending.record.sequence,
                &pending.record.previous_hash,
                &pending.record.event,
            )?
        || pending.snapshot.ledger_schema_version != LEDGER_SCHEMA_VERSION
        || pending.snapshot.record_count != expected_count
        || pending.snapshot.head_hash != pending.record.record_hash
    {
        return Err(LedgerError::CorruptPending);
    }
    Ok(pending)
}

fn write_pending(directory: &Path, pending: &PendingAppend) -> Result<(), LedgerError> {
    let mut payload = serde_json::to_vec(pending).map_err(LedgerError::json)?;
    payload.push(b'\n');
    write_atomic(&directory.join(PENDING_FILE_NAME), &payload)
}

fn clear_pending(directory: &Path) -> Result<(), LedgerError> {
    match fs::remove_file(directory.join(PENDING_FILE_NAME)) {
        Ok(()) => sync_directory(directory),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(LedgerError::io(error)),
    }
}

fn recover_pending(directory: &Path) -> Result<bool, LedgerError> {
    let Some(pending) = load_pending(directory)? else {
        return Ok(false);
    };
    let payload = read_optional(&directory.join(LEDGER_FILE_NAME))?;
    let prior_len =
        usize::try_from(pending.prior_file_len).map_err(|_| LedgerError::CorruptPending)?;
    if payload.len() < prior_len {
        return Err(LedgerError::TruncatedLedger);
    }
    let prior_records = load_records(&payload[..prior_len])?;
    if u64::try_from(prior_records.len()).map_err(|_| LedgerError::CorruptPending)?
        != pending.prior_record_count
        || records_head(&prior_records) != pending.prior_head_hash
    {
        return Err(LedgerError::CorruptPending);
    }
    let current_snapshot = load_snapshot(&directory.join(SNAPSHOT_FILE_NAME))?;
    let line = record_line(&pending.record)?;
    let tail = &payload[prior_len..];

    if tail.is_empty() {
        validate_optional_snapshot(current_snapshot.as_ref(), &prior_records)?;
        clear_pending(directory)?;
        return Ok(false);
    }
    if tail.len() < line.len() && line.starts_with(tail) {
        validate_optional_snapshot(current_snapshot.as_ref(), &prior_records)?;
        truncate_ledger(directory, pending.prior_file_len)?;
        clear_pending(directory)?;
        return Ok(false);
    }
    if tail != line {
        return Err(LedgerError::CorruptPending);
    }

    let records = load_records(&payload)?;
    if records.len() != prior_records.len() + 1
        || records.last() != Some(&pending.record)
        || records[..prior_records.len()] != prior_records
    {
        return Err(LedgerError::CorruptPending);
    }
    validate_snapshot_anchor(&pending.snapshot, &records)?;
    match current_snapshot.as_ref() {
        Some(snapshot) if snapshot == &pending.snapshot => {
            validate_snapshot_anchor(snapshot, &records)?;
        }
        snapshot => {
            validate_optional_snapshot(snapshot, &prior_records)?;
            write_snapshot(directory, &pending.snapshot)?;
        }
    }
    clear_pending(directory)?;
    Ok(true)
}

fn validate_optional_snapshot(
    snapshot: Option<&LedgerSnapshot>,
    records: &[LedgerEnvelope],
) -> Result<(), LedgerError> {
    match snapshot {
        Some(snapshot) => validate_snapshot_anchor(snapshot, records),
        None if records.is_empty() => Ok(()),
        None => Err(LedgerError::MissingSnapshot),
    }
}

fn truncate_ledger(directory: &Path, length: u64) -> Result<(), LedgerError> {
    let file = OpenOptions::new()
        .write(true)
        .open(directory.join(LEDGER_FILE_NAME))
        .map_err(LedgerError::io)?;
    file.set_len(length).map_err(LedgerError::io)?;
    file.sync_all().map_err(LedgerError::io)
}

fn records_head(records: &[LedgerEnvelope]) -> &str {
    records
        .last()
        .map_or(GENESIS_HASH, |record| record.record_hash.as_str())
}

fn record_line(record: &LedgerEnvelope) -> Result<Vec<u8>, LedgerError> {
    let mut line = serde_json::to_vec(record).map_err(LedgerError::json)?;
    line.push(b'\n');
    Ok(line)
}

fn record_hash(
    sequence: u64,
    previous_hash: &str,
    event: &LedgerEvent,
) -> Result<String, LedgerError> {
    let payload = serde_json::to_vec(&RecordHashInput {
        schema_version: LEDGER_SCHEMA_VERSION,
        sequence,
        previous_hash,
        event,
    })
    .map_err(LedgerError::json)?;
    Ok(digest_hex(&payload))
}

fn snapshot_checksum(snapshot: &LedgerSnapshot) -> Result<String, LedgerError> {
    let payload = serde_json::to_vec(&SnapshotHashInput {
        schema_version: SNAPSHOT_SCHEMA_VERSION,
        snapshot,
    })
    .map_err(LedgerError::json)?;
    Ok(digest_hex(&payload))
}

fn pending_checksum(pending: &PendingAppend) -> Result<String, LedgerError> {
    let payload = serde_json::to_vec(&PendingHashInput {
        schema_version: pending.schema_version,
        prior_file_len: pending.prior_file_len,
        prior_record_count: pending.prior_record_count,
        prior_head_hash: &pending.prior_head_hash,
        record: &pending.record,
        snapshot: &pending.snapshot,
    })
    .map_err(LedgerError::json)?;
    Ok(digest_hex(&payload))
}

fn write_snapshot(directory: &Path, snapshot: &LedgerSnapshot) -> Result<(), LedgerError> {
    let envelope = SnapshotEnvelope {
        schema_version: SNAPSHOT_SCHEMA_VERSION,
        checksum: snapshot_checksum(snapshot)?,
        snapshot: snapshot.clone(),
    };
    let mut payload = serde_json::to_vec(&envelope).map_err(LedgerError::json)?;
    payload.push(b'\n');
    write_atomic(&directory.join(SNAPSHOT_FILE_NAME), &payload)
}

fn digest_hex(payload: &[u8]) -> String {
    let digest = Sha256::digest(payload);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

fn checked_add(left: UsdcMicros, right: UsdcMicros) -> Result<UsdcMicros, LedgerError> {
    left.as_micros()
        .checked_add(right.as_micros())
        .map(UsdcMicros::from_micros)
        .ok_or(LedgerError::ArithmeticOverflow)
}

fn checked_sub(left: UsdcMicros, right: UsdcMicros) -> Result<UsdcMicros, LedgerError> {
    left.as_micros()
        .checked_sub(right.as_micros())
        .map(UsdcMicros::from_micros)
        .ok_or(LedgerError::CorruptLedger(
            "capital conservation underflowed".into(),
        ))
}

fn require_deployable(state: &ReplayState, requested: UsdcMicros) -> Result<(), LedgerError> {
    if requested <= state.deployable_usdc() {
        Ok(())
    } else {
        Err(LedgerError::InsufficientDeployableCapital)
    }
}

fn require_nonzero(value: UsdcMicros) -> Result<(), LedgerError> {
    if value.is_zero() {
        Err(LedgerError::InvalidEvent(
            "USDC amount must be positive".into(),
        ))
    } else {
        Ok(())
    }
}

fn require_hype(value: u64) -> Result<(), LedgerError> {
    if value == 0 {
        Err(LedgerError::InvalidEvent(
            "HYPE amount must be positive".into(),
        ))
    } else {
        Ok(())
    }
}

fn validate_id(name: &str, value: &str) -> Result<(), LedgerError> {
    if value.trim().is_empty() || value.trim() != value {
        Err(LedgerError::InvalidEvent(format!(
            "{name} must be non-empty and trimmed"
        )))
    } else {
        Ok(())
    }
}

fn validate_text(name: &str, value: &str) -> Result<(), LedgerError> {
    if value.trim().is_empty() {
        Err(LedgerError::InvalidEvent(format!(
            "{name} must be non-empty"
        )))
    } else {
        Ok(())
    }
}

fn read_optional(path: &Path) -> Result<Vec<u8>, LedgerError> {
    fs::read(path)
        .or_else(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                Ok(Vec::new())
            } else {
                Err(error)
            }
        })
        .map_err(LedgerError::io)
}

fn write_atomic(path: &Path, payload: &[u8]) -> Result<(), LedgerError> {
    let parent = path.parent().ok_or(LedgerError::InvalidPath)?;
    let file_name = path.file_name().ok_or(LedgerError::InvalidPath)?;
    fs::create_dir_all(parent).map_err(LedgerError::io)?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| LedgerError::Io(error.to_string()))?
        .as_nanos();
    let mut temporary_name = file_name.to_os_string();
    temporary_name.push(format!(".{}.{}.tmp", std::process::id(), nonce));
    let temporary = parent.join(temporary_name);
    let result = (|| -> Result<(), LedgerError> {
        let mut options = OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o640);
        }
        let mut file = options.open(&temporary).map_err(LedgerError::io)?;
        file.write_all(payload).map_err(LedgerError::io)?;
        file.sync_all().map_err(LedgerError::io)?;
        fs::rename(&temporary, path).map_err(LedgerError::io)?;
        sync_directory(parent)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

struct LedgerLock {
    _file: File,
}

impl LedgerLock {
    fn acquire(directory: &Path) -> Result<Self, LedgerError> {
        fs::create_dir_all(directory).map_err(LedgerError::io)?;
        let mut options = OpenOptions::new();
        options.create(true).read(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o640);
        }
        let file = options
            .open(directory.join(LOCK_FILE_NAME))
            .map_err(LedgerError::io)?;
        file.lock_exclusive().map_err(LedgerError::io)?;
        Ok(Self { _file: file })
    }
}

fn ensure_clean_directory(path: &Path) -> Result<(), LedgerError> {
    if path.exists() {
        for entry in fs::read_dir(path).map_err(LedgerError::io)? {
            let entry = entry.map_err(LedgerError::io)?;
            if entry.file_name() != LOCK_FILE_NAME {
                return Err(LedgerError::RestoreDestinationNotEmpty);
            }
        }
    } else {
        fs::create_dir_all(path).map_err(LedgerError::io)?;
    }
    Ok(())
}

fn sync_directory(path: &Path) -> Result<(), LedgerError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(LedgerError::io)
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum LedgerError {
    #[error("invalid ledger event: {0}")]
    InvalidEvent(String),
    #[error("event ID collision: {0}")]
    EventCollision(String),
    #[error("unknown authoritative deposit: {0}")]
    UnknownDeposit(String),
    #[error("admission exceeds authoritative deposit: {0}")]
    AdmissionExceedsDeposit(String),
    #[error("insufficient deployable capital")]
    InsufficientDeployableCapital,
    #[error("commitment ID collision: {0}")]
    CommitmentCollision(String),
    #[error("unknown commitment: {0}")]
    UnknownCommitment(String),
    #[error("commitment already settled: {0}")]
    CommitmentAlreadySettled(String),
    #[error("cash debit exceeds commitment: {0}")]
    DebitExceedsCommitment(String),
    #[error("ledger is truncated")]
    TruncatedLedger,
    #[error("ledger is corrupt: {0}")]
    CorruptLedger(String),
    #[error("snapshot is missing")]
    MissingSnapshot,
    #[error("snapshot checksum or encoding is corrupt")]
    CorruptSnapshot,
    #[error("snapshot does not match its journal prefix")]
    SnapshotMismatch,
    #[error("snapshot does not cover the current journal head")]
    StaleSnapshot,
    #[error("pending append transaction is corrupt")]
    CorruptPending,
    #[error("restore destination is not empty")]
    RestoreDestinationNotEmpty,
    #[error("ledger changed since it was opened")]
    ConcurrentModification,
    #[error("ledger path is invalid")]
    InvalidPath,
    #[error("ledger arithmetic overflow")]
    ArithmeticOverflow,
    #[error("ledger I/O failed: {0}")]
    Io(String),
    #[error("ledger serialization failed: {0}")]
    Json(String),
}

impl LedgerError {
    fn io(error: io::Error) -> Self {
        error.into()
    }

    fn json(error: serde_json::Error) -> Self {
        error.into()
    }
}

impl From<io::Error> for LedgerError {
    fn from(error: io::Error) -> Self {
        Self::Io(error.to_string())
    }
}

impl From<serde_json::Error> for LedgerError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error.to_string())
    }
}

#[cfg(test)]
mod transaction_tests {
    use super::*;
    use chrono::TimeZone;

    fn at(hour: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 8, 24, hour, 0, 0)
            .single()
            .expect("valid UTC fixture")
    }

    fn usd(value: u64) -> UsdcMicros {
        UsdcMicros::checked_from_whole_usdc(value).expect("small test amount")
    }

    fn deposit(id: &str, hour: u32) -> LedgerEvent {
        LedgerEvent {
            event_id: id.into(),
            occurred_at: at(hour),
            kind: LedgerEventKind::AuthoritativeDeposit {
                amount_usdc: usd(100),
            },
        }
    }

    fn observation(id: &str, hour: u32) -> LedgerEvent {
        LedgerEvent {
            event_id: id.into(),
            occurred_at: at(hour),
            kind: LedgerEventKind::BalanceObserved {
                observed_usdc: usd(100),
                observed_hype_atoms: 1,
            },
        }
    }

    #[test]
    fn restart_discards_intent_when_journal_append_never_started() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let mut ledger = DurableLedger::open(directory.path()).expect("open ledger");
        ledger
            .append(deposit("deposit-before-intent", 1))
            .expect("append deposit");
        let interrupted = observation("observation-after-intent", 2);
        {
            let _lock = LedgerLock::acquire(directory.path()).expect("acquire lock");
            ledger.ensure_current().expect("current ledger");
            let prepared = ledger
                .prepare_append(interrupted.clone())
                .expect("prepare append");
            write_pending(directory.path(), &prepared.pending).expect("persist intent");
        }

        let reopened = DurableLedger::open(directory.path()).expect("recover old head");
        assert_eq!(reopened.record_count(), 1);
        assert!(!directory.path().join(PENDING_FILE_NAME).exists());
        assert_eq!(
            ledger.append(interrupted).expect("retry append"),
            AppendOutcome::Appended
        );
    }

    #[test]
    fn same_instance_retry_rolls_forward_fsynced_record_without_snapshot() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let mut ledger = DurableLedger::open(directory.path()).expect("open ledger");
        ledger
            .append(deposit("deposit-before-record", 1))
            .expect("append deposit");
        let interrupted = observation("observation-after-record", 2);
        {
            let _lock = LedgerLock::acquire(directory.path()).expect("acquire lock");
            ledger.ensure_current().expect("current ledger");
            let prepared = ledger
                .prepare_append(interrupted.clone())
                .expect("prepare append");
            write_pending(directory.path(), &prepared.pending).expect("persist intent");
            ledger
                .write_record(&prepared.record)
                .expect("fsync journal record");
        }

        assert_eq!(
            ledger.append(interrupted).expect("recover and retry"),
            AppendOutcome::Duplicate
        );
        assert_eq!(ledger.record_count(), 2);
        assert_eq!(ledger.state().observed_hype_atoms(), 1);
        assert!(!directory.path().join(PENDING_FILE_NAME).exists());
        assert_eq!(
            DurableLedger::open(directory.path())
                .expect("reopen recovered ledger")
                .record_count(),
            2
        );
    }

    #[test]
    fn restart_truncates_an_authorized_partial_record_and_allows_retry() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let mut ledger = DurableLedger::open(directory.path()).expect("open ledger");
        ledger
            .append(deposit("deposit-before-partial", 1))
            .expect("append deposit");
        let interrupted = observation("observation-partial", 2);
        {
            let _lock = LedgerLock::acquire(directory.path()).expect("acquire lock");
            ledger.ensure_current().expect("current ledger");
            let prepared = ledger
                .prepare_append(interrupted.clone())
                .expect("prepare append");
            write_pending(directory.path(), &prepared.pending).expect("persist intent");
            let line = record_line(&prepared.record).expect("encode record");
            let mut file = OpenOptions::new()
                .append(true)
                .open(directory.path().join(LEDGER_FILE_NAME))
                .expect("open journal");
            file.write_all(&line[..line.len() / 2])
                .expect("write partial record");
            file.sync_all().expect("fsync partial record");
        }

        assert_eq!(
            DurableLedger::open(directory.path())
                .expect("recover prior head")
                .record_count(),
            1
        );
        assert_eq!(
            ledger.append(interrupted).expect("retry append"),
            AppendOutcome::Appended
        );
        assert_eq!(ledger.record_count(), 2);
    }
}
