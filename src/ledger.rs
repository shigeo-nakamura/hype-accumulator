//! Durable audit ledger with an independently protected head anchor.
//!
//! This module is deliberately transport-agnostic. It writes only to caller-
//! supplied local directories and never submits, signs, uploads, or deploys
//! anything. Tamper and rollback resistance depends on the caller keeping its
//! [`ProtectedAnchorStore`] outside the mutable ledger filesystem boundary.

use crate::pacing::UsdcMicros;
use chrono::{DateTime, NaiveDate, Utc};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    fmt::Write as _,
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use thiserror::Error;

pub const LEDGER_SCHEMA_VERSION: u8 = 1;
pub const SNAPSHOT_SCHEMA_VERSION: u8 = 1;
pub const PROTECTED_ANCHOR_SCHEMA_VERSION: u8 = 1;
pub const LEDGER_FILE_NAME: &str = "ledger.jsonl";
pub const SNAPSHOT_FILE_NAME: &str = "snapshot.json";
const LOCK_FILE_NAME: &str = ".ledger.lock";
const PENDING_FILE_NAME: &str = ".pending-append.json";
const PENDING_SCHEMA_VERSION: u8 = 1;
const RESTORE_PENDING_FILE_NAME: &str = ".pending-restore.json";
const RESTORE_PENDING_SCHEMA_VERSION: u8 = 1;
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
        commitment_id: String,
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct PurchaseDecisionReplay {
    commitment_id: String,
    planned_usdc: UsdcMicros,
    committed_usdc: UsdcMicros,
    filled_usdc: UsdcMicros,
    fee_usdc: UsdcMicros,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayState {
    deposits: BTreeMap<String, DepositReplay>,
    commitments: BTreeMap<String, CommitmentReplay>,
    decision_outcomes: BTreeMap<NaiveDate, String>,
    decision_ids: BTreeSet<String>,
    purchase_decisions: BTreeMap<String, PurchaseDecisionReplay>,
    decision_commitment_ids: BTreeSet<String>,
    orders: BTreeMap<String, String>,
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

/// Latest committed journal head held outside the mutable ledger directory.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProtectedHeadAnchor {
    pub schema_version: u8,
    pub record_count: u64,
    pub head_hash: String,
}

/// Independently protected persistence boundary for the latest committed head.
///
/// Implementations must be durable before returning success and must not be
/// writable by an actor that can modify the ledger directory. `compare_and_swap`
/// returns `Ok(false)` when `expected` is no longer current. A store instance is
/// scoped to one logical ledger; source and restore destination use distinct
/// instances.
pub trait ProtectedAnchorStore: Send + Sync {
    /// Loads the latest committed head, or `None` for an unused ledger scope.
    ///
    /// # Errors
    ///
    /// Returns a store-specific error when the protected state cannot be read.
    fn load(&self) -> Result<Option<ProtectedHeadAnchor>, String>;

    /// Durably replaces `expected` with `next` when `expected` is still current.
    ///
    /// # Errors
    ///
    /// Returns a store-specific error when the atomic durable update cannot be
    /// completed. `Ok(false)` reports a comparison conflict, not an I/O error.
    fn compare_and_swap(
        &self,
        expected: Option<&ProtectedHeadAnchor>,
        next: &ProtectedHeadAnchor,
    ) -> Result<bool, String>;
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct PendingRestore {
    schema_version: u8,
    ledger_digest: String,
    snapshot_digest: String,
    record_count: u64,
    head_hash: String,
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

#[derive(Serialize)]
struct RestorePendingHashInput<'a> {
    schema_version: u8,
    ledger_digest: &'a str,
    snapshot_digest: &'a str,
    record_count: u64,
    head_hash: &'a str,
}

pub struct DurableLedger {
    directory: PathBuf,
    journal: File,
    anchor_store: Arc<dyn ProtectedAnchorStore>,
    anchor: Option<ProtectedHeadAnchor>,
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

struct RestoreTarget {
    ledger_payload: Vec<u8>,
    snapshot_payload: Vec<u8>,
    records: Vec<LedgerEnvelope>,
    state: ReplayState,
    anchor: Option<ProtectedHeadAnchor>,
}

impl RestoreTarget {
    fn from_verified(
        ledger_payload: Vec<u8>,
        snapshot_payload: Vec<u8>,
        verified: &DurableLedger,
    ) -> Self {
        Self {
            ledger_payload,
            snapshot_payload,
            records: verified.records.clone(),
            state: verified.state.clone(),
            anchor: verified.anchor.clone(),
        }
    }

    fn head_hash(&self) -> &str {
        records_head(&self.records)
    }
}

impl DurableLedger {
    /// Opens a ledger and verifies its journal and snapshot against the
    /// independently protected latest-head anchor.
    ///
    /// `anchor_store` must be scoped to this logical ledger and protected from
    /// actors that can modify `directory`.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] for malformed, truncated, hash-invalid, or
    /// snapshot-inconsistent state.
    pub fn open(
        directory: impl AsRef<Path>,
        anchor_store: Arc<dyn ProtectedAnchorStore>,
    ) -> Result<Self, LedgerError> {
        let directory = canonical_directory(directory.as_ref())?;
        let _lock = LedgerLock::acquire(&directory)?;
        recover_pending(&directory, anchor_store.as_ref())?;
        Self::open_unlocked(directory, anchor_store)
    }

    fn open_unlocked(
        directory: PathBuf,
        anchor_store: Arc<dyn ProtectedAnchorStore>,
    ) -> Result<Self, LedgerError> {
        let mut journal = open_journal(&directory)?;
        let payload = read_journal(&mut journal)?;
        let records = load_records(&payload)?;
        let anchor = load_protected_anchor(anchor_store.as_ref())?;
        validate_protected_anchor(anchor.as_ref(), &records)?;
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
            journal,
            anchor_store,
            anchor,
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

    /// Appends one validated event and durably commits both its protected head
    /// and local journal before returning.
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
        recover_pending(&self.directory, self.anchor_store.as_ref())?;
        *self = Self::open_unlocked(self.directory.clone(), Arc::clone(&self.anchor_store))?;
        if let Some(existing) = self.events_by_id.get(&event.event_id) {
            return if existing == &event {
                Ok(AppendOutcome::Duplicate)
            } else {
                Err(LedgerError::EventCollision(event.event_id))
            };
        }
        let prepared = self.prepare_append(event)?;
        write_pending(&self.directory, &prepared.pending)?;
        let next_anchor = protected_anchor_for_snapshot(&prepared.snapshot)?;
        advance_protected_anchor(
            self.anchor_store.as_ref(),
            self.anchor.as_ref(),
            &next_anchor,
        )?;
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
        self.anchor = Some(next_anchor);
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
        if recover_pending(&self.directory, self.anchor_store.as_ref())? {
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
    /// An exact completed retry is idempotent.
    ///
    /// The source and destination anchor stores must be independently protected
    /// scopes for their respective logical ledgers. The destination scope must
    /// be unused or already contain the exact restored head.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] for a non-clean destination, missing/stale
    /// snapshot, source mutation, verification failure, or local I/O failure.
    pub fn restore_clean(
        source: impl AsRef<Path>,
        destination: impl AsRef<Path>,
        source_anchor_store: Arc<dyn ProtectedAnchorStore>,
        destination_anchor_store: Arc<dyn ProtectedAnchorStore>,
    ) -> Result<Self, LedgerError> {
        let source = canonical_directory(source.as_ref())?;
        let destination = canonical_directory(destination.as_ref())?;
        if source == destination || same_directory_identity(&source, &destination)? {
            return Err(LedgerError::RestoreDestinationNotEmpty);
        }
        let (_first_lock, _second_lock) = acquire_restore_locks(&source, &destination)?;
        recover_pending(&source, source_anchor_store.as_ref())?;
        let verified = Self::open_unlocked(source.clone(), source_anchor_store)?;
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
        let current_pending = build_pending_restore(&ledger_payload, &snapshot_payload, &verified)?;
        recover_pending_restore_temporary(&destination, &verified.records)?;
        let destination_anchor = load_protected_anchor(destination_anchor_store.as_ref())?;
        let target = match load_pending_restore(&destination)? {
            Some(existing) => {
                let target = if existing == current_pending {
                    RestoreTarget::from_verified(ledger_payload, snapshot_payload, &verified)
                } else {
                    restore_target_from_pending(&existing, &verified.records)?
                };
                remove_authenticated_atomic_temporaries(
                    &destination.join(LEDGER_FILE_NAME),
                    &target.ledger_payload,
                )?;
                remove_authenticated_atomic_temporaries(
                    &destination.join(SNAPSHOT_FILE_NAME),
                    &target.snapshot_payload,
                )?;
                if !has_only_restore_entries(&destination, true)? {
                    return Err(LedgerError::CorruptRestorePending);
                }
                // A stale local intent is authoritative only after the
                // independently protected destination scope accepted that
                // exact prefix. The current protected source head and its
                // verified hash chain separately prove the prefix ancestry.
                if target.anchor != verified.anchor
                    && (target.anchor.is_none()
                        || destination_anchor.as_ref() != target.anchor.as_ref())
                {
                    return Err(LedgerError::ProtectedAnchorMismatch);
                }
                target
            }
            None if has_only_restore_entries(&destination, false)?
                && (destination_anchor.is_none()
                    || destination_anchor.as_ref() == verified.anchor.as_ref()) =>
            {
                write_pending_restore(&destination, &current_pending)?;
                RestoreTarget::from_verified(ledger_payload, snapshot_payload, &verified)
            }
            None if restore_is_complete(
                &destination,
                &ledger_payload,
                &snapshot_payload,
                &verified,
                Arc::clone(&destination_anchor_store),
            )? =>
            {
                return Self::open_unlocked(destination, destination_anchor_store);
            }
            None => return Err(LedgerError::RestoreDestinationNotEmpty),
        };
        ensure_restore_anchor(
            destination_anchor_store.as_ref(),
            destination_anchor.as_ref(),
            target.anchor.as_ref(),
        )?;
        write_atomic(&destination.join(LEDGER_FILE_NAME), &target.ledger_payload)?;
        write_atomic(
            &destination.join(SNAPSHOT_FILE_NAME),
            &target.snapshot_payload,
        )?;
        let restored = Self::open_unlocked(destination.clone(), destination_anchor_store)?;
        if restored.records != target.records
            || restored.state != target.state
            || restored.head_hash() != target.head_hash()
        {
            return Err(LedgerError::SnapshotMismatch);
        }
        clear_pending_restore(&destination)?;
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

    fn write_record(&mut self, record: &LedgerEnvelope) -> Result<u64, LedgerError> {
        validate_journal_handle(&self.directory, &self.journal)?;
        if self.journal.metadata().map_err(LedgerError::io)?.len() != self.file_len {
            return Err(LedgerError::ConcurrentModification);
        }
        let line = record_line(record)?;
        let next_file_len = self
            .file_len
            .checked_add(
                u64::try_from(line.len())
                    .map_err(|_| LedgerError::CorruptLedger("file length overflowed".into()))?,
            )
            .ok_or_else(|| LedgerError::CorruptLedger("file length overflowed".into()))?;
        self.journal.write_all(&line).map_err(LedgerError::io)?;
        self.journal.sync_all().map_err(LedgerError::io)?;
        Ok(next_file_len)
    }

    fn ensure_current(&self) -> Result<(), LedgerError> {
        validate_journal_handle(&self.directory, &self.journal)?;
        let mut journal = self.journal.try_clone().map_err(LedgerError::io)?;
        let payload = read_journal(&mut journal)?;
        let current_len = u64::try_from(payload.len())
            .map_err(|_| LedgerError::CorruptLedger("file length overflowed".into()))?;
        let records = load_records(&payload)?;
        if current_len != self.file_len || records != self.records {
            Err(LedgerError::ConcurrentModification)
        } else {
            let anchor = load_protected_anchor(self.anchor_store.as_ref())?;
            if anchor != self.anchor {
                return Err(LedgerError::ConcurrentModification);
            }
            validate_protected_anchor(anchor.as_ref(), &records)?;
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
        } => record_deposit_admission(state, deposit_event_id, *amount_usdc)?,
        LedgerEventKind::AuthoritativeWithdrawal { amount_usdc } => {
            require_deployable(state, *amount_usdc)?;
            state.withdrawn_usdc = checked_add(state.withdrawn_usdc, *amount_usdc)?;
        }
        LedgerEventKind::CapitalCommitted {
            commitment_id,
            amount_usdc,
        } => record_capital_commitment(state, commitment_id, *amount_usdc)?,
        LedgerEventKind::CapitalSettled {
            commitment_id,
            debited_usdc,
        } => record_capital_settlement(state, commitment_id, *debited_usdc)?,
        LedgerEventKind::BalanceObserved {
            observed_usdc,
            observed_hype_atoms,
        }
        | LedgerEventKind::ReconciliationCorrection {
            observed_usdc,
            observed_hype_atoms,
            ..
        } => record_observed_balance(state, *observed_usdc, *observed_hype_atoms),
        LedgerEventKind::DailyDecision {
            decision_id,
            decision_date,
            commitment_id,
            planned_usdc,
            committed_usdc,
        } => record_purchase_decision(
            state,
            event.occurred_at.date_naive(),
            *decision_date,
            decision_id,
            commitment_id,
            *planned_usdc,
            *committed_usdc,
        )?,
        LedgerEventKind::DailySkip {
            decision_id,
            decision_date,
            ..
        } => record_daily_outcome(
            state,
            event.occurred_at.date_naive(),
            *decision_date,
            decision_id,
        )?,
        LedgerEventKind::OrderRecorded {
            order_id,
            decision_id,
        } => record_order(state, order_id, decision_id)?,
        LedgerEventKind::FillRecorded {
            order_id,
            filled_usdc,
            ..
        } => record_fill(state, order_id, *filled_usdc)?,
        LedgerEventKind::FeeRecorded { order_id, fee_usdc } => {
            record_fee(state, order_id, *fee_usdc)?;
        }
        LedgerEventKind::StakingDepositRecorded { .. }
        | LedgerEventKind::DelegationRecorded { .. }
        | LedgerEventKind::RewardRecorded { .. } => {}
    }
    Ok(())
}

fn record_deposit_admission(
    state: &mut ReplayState,
    deposit_event_id: &str,
    amount_usdc: UsdcMicros,
) -> Result<(), LedgerError> {
    let deposit = state
        .deposits
        .get_mut(deposit_event_id)
        .ok_or_else(|| LedgerError::UnknownDeposit(deposit_event_id.to_owned()))?;
    let next_admitted = checked_add(deposit.admitted_usdc, amount_usdc)?;
    if next_admitted > deposit.authoritative_usdc {
        return Err(LedgerError::AdmissionExceedsDeposit(
            deposit_event_id.to_owned(),
        ));
    }
    deposit.admitted_usdc = next_admitted;
    state.admitted_usdc = checked_add(state.admitted_usdc, amount_usdc)?;
    Ok(())
}

fn record_capital_commitment(
    state: &mut ReplayState,
    commitment_id: &str,
    amount_usdc: UsdcMicros,
) -> Result<(), LedgerError> {
    if state.commitments.contains_key(commitment_id) {
        return Err(LedgerError::CommitmentCollision(commitment_id.to_owned()));
    }
    require_deployable(state, amount_usdc)?;
    state.committed_usdc = checked_add(state.committed_usdc, amount_usdc)?;
    state.commitments.insert(
        commitment_id.to_owned(),
        CommitmentReplay {
            committed_usdc: amount_usdc,
            debited_usdc: UsdcMicros::default(),
            settled: false,
        },
    );
    Ok(())
}

fn record_capital_settlement(
    state: &mut ReplayState,
    commitment_id: &str,
    debited_usdc: UsdcMicros,
) -> Result<(), LedgerError> {
    let recorded_costs = state
        .purchase_decisions
        .values()
        .find(|decision| decision.commitment_id == commitment_id)
        .map_or(Ok(UsdcMicros::default()), |decision| {
            checked_add(decision.filled_usdc, decision.fee_usdc)
        })?;
    let commitment = state
        .commitments
        .get_mut(commitment_id)
        .ok_or_else(|| LedgerError::UnknownCommitment(commitment_id.to_owned()))?;
    if commitment.settled {
        return Err(LedgerError::CommitmentAlreadySettled(
            commitment_id.to_owned(),
        ));
    }
    if debited_usdc > commitment.committed_usdc {
        return Err(LedgerError::DebitExceedsCommitment(
            commitment_id.to_owned(),
        ));
    }
    if debited_usdc < recorded_costs {
        return Err(LedgerError::DebitBelowRecordedCosts(
            commitment_id.to_owned(),
        ));
    }
    state.committed_usdc = checked_sub(state.committed_usdc, commitment.committed_usdc)?;
    state.spent_usdc = checked_add(state.spent_usdc, debited_usdc)?;
    commitment.debited_usdc = debited_usdc;
    commitment.settled = true;
    Ok(())
}

fn record_observed_balance(
    state: &mut ReplayState,
    observed_usdc: UsdcMicros,
    observed_hype_atoms: u64,
) {
    state.observed_usdc = observed_usdc;
    state.observed_hype_atoms = observed_hype_atoms;
}

fn record_daily_outcome(
    state: &mut ReplayState,
    occurred_date: NaiveDate,
    decision_date: NaiveDate,
    decision_id: &str,
) -> Result<(), LedgerError> {
    if decision_date != occurred_date {
        return Err(LedgerError::DecisionDateMismatch {
            declared: decision_date,
            occurred: occurred_date,
        });
    }
    if state.decision_outcomes.contains_key(&decision_date) {
        return Err(LedgerError::DecisionDateCollision(decision_date));
    }
    if state.decision_ids.contains(decision_id) {
        return Err(LedgerError::DecisionIdCollision(decision_id.to_owned()));
    }
    state
        .decision_outcomes
        .insert(decision_date, decision_id.to_owned());
    state.decision_ids.insert(decision_id.to_owned());
    Ok(())
}

fn record_purchase_decision(
    state: &mut ReplayState,
    occurred_date: NaiveDate,
    decision_date: NaiveDate,
    decision_id: &str,
    commitment_id: &str,
    planned_usdc: UsdcMicros,
    committed_usdc: UsdcMicros,
) -> Result<(), LedgerError> {
    record_daily_outcome(state, occurred_date, decision_date, decision_id)?;
    if state.decision_commitment_ids.contains(commitment_id) {
        return Err(LedgerError::DecisionCommitmentCollision(
            commitment_id.to_owned(),
        ));
    }
    state
        .decision_commitment_ids
        .insert(commitment_id.to_owned());
    state.purchase_decisions.insert(
        decision_id.to_owned(),
        PurchaseDecisionReplay {
            commitment_id: commitment_id.to_owned(),
            planned_usdc,
            committed_usdc,
            filled_usdc: UsdcMicros::default(),
            fee_usdc: UsdcMicros::default(),
        },
    );
    Ok(())
}

fn record_order(
    state: &mut ReplayState,
    order_id: &str,
    decision_id: &str,
) -> Result<(), LedgerError> {
    let decision = state
        .purchase_decisions
        .get(decision_id)
        .ok_or_else(|| LedgerError::UnknownDecision(decision_id.to_owned()))?;
    let commitment = state.commitments.get(&decision.commitment_id);
    if !commitment.is_some_and(|commitment| {
        !commitment.settled && commitment.committed_usdc >= decision.committed_usdc
    }) {
        return Err(LedgerError::InsufficientDecisionBacking(
            decision_id.to_owned(),
        ));
    }
    if state.orders.contains_key(order_id) {
        return Err(LedgerError::OrderIdCollision(order_id.to_owned()));
    }
    state
        .orders
        .insert(order_id.to_owned(), decision_id.to_owned());
    Ok(())
}

fn record_fill(
    state: &mut ReplayState,
    order_id: &str,
    filled_usdc: UsdcMicros,
) -> Result<(), LedgerError> {
    let decision_id = decision_id_for_order(state, order_id)?;
    require_unsettled_decision_backing(state, &decision_id)?;
    let decision = state
        .purchase_decisions
        .get_mut(&decision_id)
        .ok_or_else(|| LedgerError::UnknownDecision(decision_id.clone()))?;
    let next_filled = checked_add(decision.filled_usdc, filled_usdc)?;
    if next_filled > decision.planned_usdc {
        return Err(LedgerError::FillExceedsDecisionPlan(decision_id.clone()));
    }
    if checked_add(next_filled, decision.fee_usdc)? > decision.committed_usdc {
        return Err(LedgerError::DecisionCostsExceedCommitment(decision_id));
    }
    decision.filled_usdc = next_filled;
    Ok(())
}

fn record_fee(
    state: &mut ReplayState,
    order_id: &str,
    fee_usdc: UsdcMicros,
) -> Result<(), LedgerError> {
    let decision_id = decision_id_for_order(state, order_id)?;
    require_unsettled_decision_backing(state, &decision_id)?;
    let decision = state
        .purchase_decisions
        .get_mut(&decision_id)
        .ok_or_else(|| LedgerError::UnknownDecision(decision_id.clone()))?;
    let next_fee = checked_add(decision.fee_usdc, fee_usdc)?;
    if checked_add(decision.filled_usdc, next_fee)? > decision.committed_usdc {
        return Err(LedgerError::DecisionCostsExceedCommitment(decision_id));
    }
    decision.fee_usdc = next_fee;
    Ok(())
}

fn decision_id_for_order(state: &ReplayState, order_id: &str) -> Result<String, LedgerError> {
    state
        .orders
        .get(order_id)
        .cloned()
        .ok_or_else(|| LedgerError::UnknownOrder(order_id.to_owned()))
}

fn require_unsettled_decision_backing(
    state: &ReplayState,
    decision_id: &str,
) -> Result<(), LedgerError> {
    let decision = state
        .purchase_decisions
        .get(decision_id)
        .ok_or_else(|| LedgerError::UnknownDecision(decision_id.to_owned()))?;
    if state
        .commitments
        .get(&decision.commitment_id)
        .is_some_and(|commitment| !commitment.settled)
    {
        Ok(())
    } else {
        Err(LedgerError::InsufficientDecisionBacking(
            decision_id.to_owned(),
        ))
    }
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
            commitment_id,
            planned_usdc,
            committed_usdc,
            ..
        } => validate_daily_decision(decision_id, commitment_id, *planned_usdc, *committed_usdc)?,
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

fn validate_daily_decision(
    decision_id: &str,
    commitment_id: &str,
    planned_usdc: UsdcMicros,
    committed_usdc: UsdcMicros,
) -> Result<(), LedgerError> {
    validate_id("decision_id", decision_id)?;
    validate_id("commitment_id", commitment_id)?;
    require_nonzero(planned_usdc)?;
    if committed_usdc < planned_usdc {
        return Err(LedgerError::InvalidEvent(
            "decision commitment is below planned notional".into(),
        ));
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
    let body = &payload[..payload.len() - 1];
    if body.is_empty() {
        return Err(LedgerError::CorruptLedger(
            "journal contains a blank record".into(),
        ));
    }
    let mut records = Vec::new();
    let mut expected_previous_hash = GENESIS_HASH.to_owned();
    for line in body.split(|byte| *byte == b'\n') {
        if line.is_empty() {
            return Err(LedgerError::CorruptLedger(
                "journal contains a blank record".into(),
            ));
        }
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
    remove_durable(directory, PENDING_FILE_NAME)
}

fn recover_pending(
    directory: &Path,
    anchor_store: &dyn ProtectedAnchorStore,
) -> Result<bool, LedgerError> {
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
    let line = record_line(&pending.record)?;
    let tail = &payload[prior_len..];
    if !tail.is_empty() && tail != line && !(tail.len() < line.len() && line.starts_with(tail)) {
        return Err(LedgerError::CorruptPending);
    }
    let prior_anchor = protected_anchor_for_records(&prior_records)?;
    let target_anchor = protected_anchor_for_snapshot(&pending.snapshot)?;
    let actual_anchor = load_protected_anchor(anchor_store)?;
    if actual_anchor.as_ref() == prior_anchor.as_ref() {
        let journal_replaced = !tail.is_empty();
        rollback_uncommitted_pending(
            directory,
            &payload[..prior_len],
            &prior_records,
            journal_replaced,
        )?;
        clear_pending(directory)?;
        return Ok(journal_replaced);
    }
    if actual_anchor.as_ref() != Some(&target_anchor) {
        return Err(LedgerError::ProtectedAnchorMismatch);
    }

    let mut records = prior_records;
    records.push(pending.record.clone());
    validate_snapshot_anchor(&pending.snapshot, &records)?;
    let mut recovered_payload = payload[..prior_len].to_vec();
    recovered_payload.extend_from_slice(&line);
    write_atomic(&directory.join(LEDGER_FILE_NAME), &recovered_payload)?;
    write_snapshot(directory, &pending.snapshot)?;
    clear_pending(directory)?;
    Ok(true)
}

fn rollback_uncommitted_pending(
    directory: &Path,
    prior_payload: &[u8],
    prior_records: &[LedgerEnvelope],
    replace_journal: bool,
) -> Result<(), LedgerError> {
    if replace_journal {
        write_atomic(&directory.join(LEDGER_FILE_NAME), prior_payload)?;
    }
    if prior_records.is_empty() {
        remove_durable(directory, SNAPSHOT_FILE_NAME)
    } else {
        let snapshot = snapshot_for_records(prior_records)?;
        write_snapshot(directory, &snapshot)
    }
}

fn snapshot_for_records(records: &[LedgerEnvelope]) -> Result<LedgerSnapshot, LedgerError> {
    let events = records
        .iter()
        .map(|record| record.event.clone())
        .collect::<Vec<_>>();
    Ok(LedgerSnapshot {
        ledger_schema_version: LEDGER_SCHEMA_VERSION,
        record_count: u64::try_from(records.len())
            .map_err(|_| LedgerError::CorruptLedger("record count overflowed".into()))?,
        head_hash: records_head(records).to_owned(),
        state: replay(&events)?,
    })
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

fn build_pending_restore(
    ledger_payload: &[u8],
    snapshot_payload: &[u8],
    verified: &DurableLedger,
) -> Result<PendingRestore, LedgerError> {
    let mut pending = PendingRestore {
        schema_version: RESTORE_PENDING_SCHEMA_VERSION,
        ledger_digest: digest_hex(ledger_payload),
        snapshot_digest: digest_hex(snapshot_payload),
        record_count: u64::try_from(verified.records.len())
            .map_err(|_| LedgerError::CorruptLedger("record count overflowed".into()))?,
        head_hash: verified.head_hash().to_owned(),
        checksum: String::new(),
    };
    pending.checksum = restore_pending_checksum(&pending)?;
    Ok(pending)
}

fn restore_target_from_pending(
    pending: &PendingRestore,
    current_records: &[LedgerEnvelope],
) -> Result<RestoreTarget, LedgerError> {
    let record_count =
        usize::try_from(pending.record_count).map_err(|_| LedgerError::CorruptRestorePending)?;
    let records = current_records
        .get(..record_count)
        .ok_or(LedgerError::CorruptRestorePending)?
        .to_vec();
    if records_head(&records) != pending.head_hash {
        return Err(LedgerError::CorruptRestorePending);
    }

    let mut ledger_payload = Vec::new();
    for record in &records {
        ledger_payload.extend(record_line(record)?);
    }
    let snapshot = snapshot_for_records(&records)?;
    let snapshot_payload = encode_snapshot(&snapshot)?;
    if digest_hex(&ledger_payload) != pending.ledger_digest
        || digest_hex(&snapshot_payload) != pending.snapshot_digest
    {
        return Err(LedgerError::CorruptRestorePending);
    }

    Ok(RestoreTarget {
        ledger_payload,
        snapshot_payload,
        state: snapshot.state,
        anchor: protected_anchor_for_records(&records)?,
        records,
    })
}

fn load_pending_restore(directory: &Path) -> Result<Option<PendingRestore>, LedgerError> {
    match fs::read(directory.join(RESTORE_PENDING_FILE_NAME)) {
        Ok(payload) if payload.is_empty() => Err(LedgerError::CorruptRestorePending),
        Ok(payload) => decode_pending_restore(&payload).map(Some),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(LedgerError::io(error)),
    }
}

fn decode_pending_restore(payload: &[u8]) -> Result<PendingRestore, LedgerError> {
    let pending: PendingRestore =
        serde_json::from_slice(payload).map_err(|_| LedgerError::CorruptRestorePending)?;
    if pending.schema_version != RESTORE_PENDING_SCHEMA_VERSION
        || pending.checksum != restore_pending_checksum(&pending)?
        || (pending.record_count == 0 && pending.head_hash != GENESIS_HASH)
    {
        return Err(LedgerError::CorruptRestorePending);
    }
    Ok(pending)
}

fn restore_pending_checksum(pending: &PendingRestore) -> Result<String, LedgerError> {
    let payload = serde_json::to_vec(&RestorePendingHashInput {
        schema_version: pending.schema_version,
        ledger_digest: &pending.ledger_digest,
        snapshot_digest: &pending.snapshot_digest,
        record_count: pending.record_count,
        head_hash: &pending.head_hash,
    })
    .map_err(LedgerError::json)?;
    Ok(digest_hex(&payload))
}

fn write_pending_restore(directory: &Path, pending: &PendingRestore) -> Result<(), LedgerError> {
    let payload = encode_pending_restore(pending)?;
    write_atomic(&directory.join(RESTORE_PENDING_FILE_NAME), &payload)
}

fn encode_pending_restore(pending: &PendingRestore) -> Result<Vec<u8>, LedgerError> {
    let mut payload = serde_json::to_vec(pending).map_err(LedgerError::json)?;
    payload.push(b'\n');
    Ok(payload)
}

fn clear_pending_restore(directory: &Path) -> Result<(), LedgerError> {
    remove_durable(directory, RESTORE_PENDING_FILE_NAME)
}

fn protected_anchor_for_snapshot(
    snapshot: &LedgerSnapshot,
) -> Result<ProtectedHeadAnchor, LedgerError> {
    if snapshot.record_count == 0 {
        return Err(LedgerError::ProtectedAnchorMismatch);
    }
    Ok(ProtectedHeadAnchor {
        schema_version: PROTECTED_ANCHOR_SCHEMA_VERSION,
        record_count: snapshot.record_count,
        head_hash: snapshot.head_hash.clone(),
    })
}

fn protected_anchor_for_records(
    records: &[LedgerEnvelope],
) -> Result<Option<ProtectedHeadAnchor>, LedgerError> {
    if records.is_empty() {
        return Ok(None);
    }
    Ok(Some(ProtectedHeadAnchor {
        schema_version: PROTECTED_ANCHOR_SCHEMA_VERSION,
        record_count: u64::try_from(records.len())
            .map_err(|_| LedgerError::CorruptLedger("record count overflowed".into()))?,
        head_hash: records_head(records).to_owned(),
    }))
}

fn load_protected_anchor(
    store: &dyn ProtectedAnchorStore,
) -> Result<Option<ProtectedHeadAnchor>, LedgerError> {
    store.load().map_err(LedgerError::ProtectedAnchorStore)
}

fn validate_protected_anchor(
    actual: Option<&ProtectedHeadAnchor>,
    records: &[LedgerEnvelope],
) -> Result<(), LedgerError> {
    let expected = protected_anchor_for_records(records)?;
    match (actual, expected.as_ref()) {
        (None, None) => Ok(()),
        (Some(anchor), Some(expected)) if anchor == expected => Ok(()),
        (Some(anchor), _) if anchor.schema_version != PROTECTED_ANCHOR_SCHEMA_VERSION => {
            Err(LedgerError::ProtectedAnchorMismatch)
        }
        (Some(anchor), Some(expected)) if anchor.record_count > expected.record_count => {
            Err(LedgerError::TruncatedLedger)
        }
        (Some(_), None) => Err(LedgerError::TruncatedLedger),
        (None, Some(_)) => Err(LedgerError::MissingProtectedAnchor),
        _ => Err(LedgerError::ProtectedAnchorMismatch),
    }
}

fn advance_protected_anchor(
    store: &dyn ProtectedAnchorStore,
    expected: Option<&ProtectedHeadAnchor>,
    next: &ProtectedHeadAnchor,
) -> Result<(), LedgerError> {
    if store
        .compare_and_swap(expected, next)
        .map_err(LedgerError::ProtectedAnchorStore)?
    {
        Ok(())
    } else {
        Err(LedgerError::ConcurrentModification)
    }
}

fn ensure_restore_anchor(
    store: &dyn ProtectedAnchorStore,
    current: Option<&ProtectedHeadAnchor>,
    target: Option<&ProtectedHeadAnchor>,
) -> Result<(), LedgerError> {
    match (current, target) {
        (None, None) => Ok(()),
        (Some(current), Some(target)) if current == target => Ok(()),
        (None, Some(target)) => advance_protected_anchor(store, None, target),
        _ => Err(LedgerError::ProtectedAnchorMismatch),
    }
}

fn write_snapshot(directory: &Path, snapshot: &LedgerSnapshot) -> Result<(), LedgerError> {
    let payload = encode_snapshot(snapshot)?;
    write_atomic(&directory.join(SNAPSHOT_FILE_NAME), &payload)
}

fn encode_snapshot(snapshot: &LedgerSnapshot) -> Result<Vec<u8>, LedgerError> {
    let envelope = SnapshotEnvelope {
        schema_version: SNAPSHOT_SCHEMA_VERSION,
        checksum: snapshot_checksum(snapshot)?,
        snapshot: snapshot.clone(),
    };
    let mut payload = serde_json::to_vec(&envelope).map_err(LedgerError::json)?;
    payload.push(b'\n');
    Ok(payload)
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

fn open_journal(directory: &Path) -> Result<File, LedgerError> {
    let path = directory.join(LEDGER_FILE_NAME);
    let mut existing_options = OpenOptions::new();
    configure_journal_options(&mut existing_options);
    let (file, created) = match existing_options.open(&path) {
        Ok(file) => (file, false),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let mut create_options = OpenOptions::new();
            configure_journal_options(&mut create_options);
            create_options.create_new(true);
            match create_options.open(&path) {
                Ok(file) => (file, true),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => (
                    existing_options.open(&path).map_err(LedgerError::io)?,
                    false,
                ),
                Err(error) => return Err(LedgerError::io(error)),
            }
        }
        Err(error) => return Err(LedgerError::io(error)),
    };
    validate_journal_handle(directory, &file)?;
    if created {
        file.sync_all().map_err(LedgerError::io)?;
        sync_directory(directory)?;
    }
    Ok(file)
}

fn configure_journal_options(options: &mut OpenOptions) {
    options.read(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(libc::O_NOFOLLOW).mode(0o640);
    }
}

fn read_journal(file: &mut File) -> Result<Vec<u8>, LedgerError> {
    file.seek(SeekFrom::Start(0)).map_err(LedgerError::io)?;
    let mut payload = Vec::new();
    file.read_to_end(&mut payload).map_err(LedgerError::io)?;
    Ok(payload)
}

fn validate_journal_handle(directory: &Path, file: &File) -> Result<(), LedgerError> {
    let path_metadata =
        fs::symlink_metadata(directory.join(LEDGER_FILE_NAME)).map_err(LedgerError::io)?;
    let file_metadata = file.metadata().map_err(LedgerError::io)?;
    if !path_metadata.file_type().is_file() || !file_metadata.file_type().is_file() {
        return Err(LedgerError::UnsafeJournalFile);
    }
    validate_journal_identity(&path_metadata, &file_metadata)
}

#[cfg(unix)]
fn validate_journal_identity(
    path_metadata: &fs::Metadata,
    file_metadata: &fs::Metadata,
) -> Result<(), LedgerError> {
    use std::os::unix::fs::MetadataExt;

    if path_metadata.dev() != file_metadata.dev()
        || path_metadata.ino() != file_metadata.ino()
        || path_metadata.nlink() != 1
        || file_metadata.nlink() != 1
    {
        return Err(LedgerError::UnsafeJournalFile);
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_journal_identity(
    _path_metadata: &fs::Metadata,
    _file_metadata: &fs::Metadata,
) -> Result<(), LedgerError> {
    Ok(())
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

fn recover_pending_restore_temporary(
    directory: &Path,
    current_records: &[LedgerEnvelope],
) -> Result<(), LedgerError> {
    let pending_path = directory.join(RESTORE_PENDING_FILE_NAME);
    match fs::symlink_metadata(&pending_path) {
        Ok(_) => return Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(LedgerError::io(error)),
    }

    let mut authenticated = Vec::new();
    for entry in fs::read_dir(directory).map_err(LedgerError::io)? {
        let entry = entry.map_err(LedgerError::io)?;
        if !is_atomic_temporary_for(&entry.file_name(), RESTORE_PENDING_FILE_NAME.as_ref()) {
            continue;
        }
        let Some(payload) = read_single_linked_regular(&entry.path())? else {
            continue;
        };
        let Ok(pending) = decode_pending_restore(&payload) else {
            continue;
        };
        if restore_target_from_pending(&pending, current_records).is_ok() {
            authenticated.push(entry.path());
        }
    }

    if authenticated.len() > 1 {
        return Err(LedgerError::CorruptRestorePending);
    }
    if let Some(temporary) = authenticated.pop() {
        fs::rename(temporary, pending_path).map_err(LedgerError::io)?;
        sync_directory(directory)?;
    }
    Ok(())
}

fn remove_authenticated_atomic_temporaries(
    target: &Path,
    expected_payload: &[u8],
) -> Result<(), LedgerError> {
    let directory = target.parent().ok_or(LedgerError::InvalidPath)?;
    let target_name = target.file_name().ok_or(LedgerError::InvalidPath)?;
    let mut removed = false;
    for entry in fs::read_dir(directory).map_err(LedgerError::io)? {
        let entry = entry.map_err(LedgerError::io)?;
        if !is_atomic_temporary_for(&entry.file_name(), target_name) {
            continue;
        }
        if read_single_linked_regular(&entry.path())?
            .is_some_and(|payload| payload == expected_payload)
        {
            fs::remove_file(entry.path()).map_err(LedgerError::io)?;
            removed = true;
        }
    }
    if removed {
        sync_directory(directory)?;
    }
    Ok(())
}

fn is_atomic_temporary_for(candidate: &std::ffi::OsStr, target: &std::ffi::OsStr) -> bool {
    let Some(candidate) = candidate.to_str() else {
        return false;
    };
    let Some(target) = target.to_str() else {
        return false;
    };
    let Some(suffix) = candidate
        .strip_prefix(target)
        .and_then(|suffix| suffix.strip_prefix('.'))
        .and_then(|suffix| suffix.strip_suffix(".tmp"))
    else {
        return false;
    };
    let mut components = suffix.split('.');
    let Some(process_id) = components.next() else {
        return false;
    };
    let Some(nonce) = components.next() else {
        return false;
    };
    components.next().is_none()
        && !process_id.is_empty()
        && process_id.bytes().all(|byte| byte.is_ascii_digit())
        && !nonce.is_empty()
        && nonce.bytes().all(|byte| byte.is_ascii_digit())
}

fn read_single_linked_regular(path: &Path) -> Result<Option<Vec<u8>>, LedgerError> {
    let path_metadata = fs::symlink_metadata(path).map_err(LedgerError::io)?;
    if !path_metadata.file_type().is_file() {
        return Ok(None);
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(libc::O_NOFOLLOW);
    }
    let mut file = options.open(path).map_err(LedgerError::io)?;
    let file_metadata = file.metadata().map_err(LedgerError::io)?;
    if !single_linked_regular_identity(&path_metadata, &file_metadata) {
        return Ok(None);
    }
    let mut payload = Vec::new();
    file.read_to_end(&mut payload).map_err(LedgerError::io)?;
    let current_path_metadata = fs::symlink_metadata(path).map_err(LedgerError::io)?;
    if !single_linked_regular_identity(&current_path_metadata, &file_metadata) {
        return Ok(None);
    }
    Ok(Some(payload))
}

#[cfg(unix)]
fn single_linked_regular_identity(
    path_metadata: &fs::Metadata,
    file_metadata: &fs::Metadata,
) -> bool {
    use std::os::unix::fs::MetadataExt;

    path_metadata.file_type().is_file()
        && file_metadata.file_type().is_file()
        && path_metadata.dev() == file_metadata.dev()
        && path_metadata.ino() == file_metadata.ino()
        && path_metadata.nlink() == 1
        && file_metadata.nlink() == 1
}

#[cfg(not(unix))]
fn single_linked_regular_identity(
    path_metadata: &fs::Metadata,
    file_metadata: &fs::Metadata,
) -> bool {
    path_metadata.file_type().is_file() && file_metadata.file_type().is_file()
}

struct LedgerLock {
    directory: PathBuf,
    file: File,
}

fn canonical_directory(path: &Path) -> Result<PathBuf, LedgerError> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        env::current_dir().map_err(LedgerError::io)?.join(path)
    };
    create_directory_all_durable(&absolute, &mut sync_directory)?;
    fs::canonicalize(absolute).map_err(LedgerError::io)
}

#[cfg(unix)]
fn same_directory_identity(left: &Path, right: &Path) -> Result<bool, LedgerError> {
    use std::os::unix::fs::MetadataExt;

    let left = fs::metadata(left).map_err(LedgerError::io)?;
    let right = fs::metadata(right).map_err(LedgerError::io)?;
    Ok(left.dev() == right.dev() && left.ino() == right.ino())
}

#[cfg(not(unix))]
fn same_directory_identity(left: &Path, right: &Path) -> Result<bool, LedgerError> {
    Ok(left == right)
}

fn create_directory_all_durable<F>(path: &Path, sync_parent: &mut F) -> Result<(), LedgerError>
where
    F: FnMut(&Path) -> Result<(), LedgerError>,
{
    let mut missing = Vec::new();
    let mut current = path;
    loop {
        match fs::metadata(current) {
            Ok(metadata) if metadata.is_dir() => break,
            Ok(_) => {
                return Err(LedgerError::io(io::Error::new(
                    io::ErrorKind::NotADirectory,
                    format!("restore path is not a directory: {}", current.display()),
                )));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                missing.push(current.to_path_buf());
                current = current.parent().ok_or(LedgerError::InvalidPath)?;
            }
            Err(error) => return Err(LedgerError::io(error)),
        }
    }

    if missing.is_empty() {
        if let Some(parent) = path.parent() {
            sync_parent(parent)?;
        }
        return Ok(());
    }

    for directory in missing.iter().rev() {
        match fs::create_dir(directory) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                if !fs::metadata(directory).map_err(LedgerError::io)?.is_dir() {
                    return Err(LedgerError::io(error));
                }
            }
            Err(error) => return Err(LedgerError::io(error)),
        }
        let parent = directory.parent().ok_or(LedgerError::InvalidPath)?;
        sync_parent(parent)?;
    }
    Ok(())
}

impl LedgerLock {
    fn open(directory: &Path) -> Result<Self, LedgerError> {
        let path = directory.join(LOCK_FILE_NAME);
        let mut options = OpenOptions::new();
        options.read(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;

            options.custom_flags(libc::O_NOFOLLOW).mode(0o640);
        }
        let file = match options.open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let mut create_options = OpenOptions::new();
                create_options.read(true).write(true).create_new(true);
                #[cfg(unix)]
                {
                    use std::os::unix::fs::OpenOptionsExt;

                    create_options.custom_flags(libc::O_NOFOLLOW).mode(0o640);
                }
                match create_options.open(&path) {
                    Ok(file) => file,
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                        options.open(&path).map_err(LedgerError::io)?
                    }
                    Err(error) => return Err(LedgerError::io(error)),
                }
            }
            Err(error) => return Err(LedgerError::io(error)),
        };
        let lock = Self {
            directory: directory.to_path_buf(),
            file,
        };
        lock.validate()?;
        Ok(lock)
    }

    fn lock(self) -> Result<Self, LedgerError> {
        self.validate()?;
        self.file.lock_exclusive().map_err(LedgerError::io)?;
        self.validate()?;
        Ok(self)
    }

    fn acquire(directory: &Path) -> Result<Self, LedgerError> {
        Self::open(directory)?.lock()
    }

    fn validate(&self) -> Result<(), LedgerError> {
        let path_metadata =
            fs::symlink_metadata(self.directory.join(LOCK_FILE_NAME)).map_err(LedgerError::io)?;
        let file_metadata = self.file.metadata().map_err(LedgerError::io)?;
        if !path_metadata.file_type().is_file() || !file_metadata.file_type().is_file() {
            return Err(LedgerError::UnsafeLockFile);
        }
        validate_lock_identity(&path_metadata, &file_metadata)
    }
}

fn acquire_restore_locks(
    source: &Path,
    destination: &Path,
) -> Result<(LedgerLock, LedgerLock), LedgerError> {
    let source_lock = LedgerLock::open(source)?;
    let destination_lock = LedgerLock::open(destination)?;
    if same_lock_identity(&source_lock.file, &destination_lock.file)? {
        return Err(LedgerError::UnsafeLockFile);
    }
    let (first, second) = if source < destination {
        (source_lock, destination_lock)
    } else {
        (destination_lock, source_lock)
    };
    Ok((first.lock()?, second.lock()?))
}

#[cfg(unix)]
fn validate_lock_identity(
    path_metadata: &fs::Metadata,
    file_metadata: &fs::Metadata,
) -> Result<(), LedgerError> {
    use std::os::unix::fs::MetadataExt;

    if path_metadata.dev() != file_metadata.dev()
        || path_metadata.ino() != file_metadata.ino()
        || path_metadata.nlink() != 1
        || file_metadata.nlink() != 1
    {
        return Err(LedgerError::UnsafeLockFile);
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_lock_identity(
    _path_metadata: &fs::Metadata,
    _file_metadata: &fs::Metadata,
) -> Result<(), LedgerError> {
    Ok(())
}

#[cfg(unix)]
fn same_lock_identity(left: &File, right: &File) -> Result<bool, LedgerError> {
    use std::os::unix::fs::MetadataExt;

    let left = left.metadata().map_err(LedgerError::io)?;
    let right = right.metadata().map_err(LedgerError::io)?;
    Ok(left.dev() == right.dev() && left.ino() == right.ino())
}

#[cfg(not(unix))]
fn same_lock_identity(_left: &File, _right: &File) -> Result<bool, LedgerError> {
    Ok(false)
}

fn remove_durable(directory: &Path, file_name: &str) -> Result<(), LedgerError> {
    match fs::remove_file(directory.join(file_name)) {
        Ok(()) => sync_directory(directory),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(LedgerError::io(error)),
    }
}

fn has_only_restore_entries(directory: &Path, pending: bool) -> Result<bool, LedgerError> {
    for entry in fs::read_dir(directory).map_err(LedgerError::io)? {
        let name = entry.map_err(LedgerError::io)?.file_name();
        if name != LOCK_FILE_NAME
            && (!pending
                || (name != RESTORE_PENDING_FILE_NAME
                    && name != LEDGER_FILE_NAME
                    && name != SNAPSHOT_FILE_NAME))
        {
            return Ok(false);
        }
    }
    if pending && !directory.join(RESTORE_PENDING_FILE_NAME).exists() {
        return Ok(false);
    }
    Ok(true)
}

fn restore_is_complete(
    directory: &Path,
    ledger_payload: &[u8],
    snapshot_payload: &[u8],
    verified: &DurableLedger,
    anchor_store: Arc<dyn ProtectedAnchorStore>,
) -> Result<bool, LedgerError> {
    for entry in fs::read_dir(directory).map_err(LedgerError::io)? {
        let name = entry.map_err(LedgerError::io)?.file_name();
        if name != LOCK_FILE_NAME && name != LEDGER_FILE_NAME && name != SNAPSHOT_FILE_NAME {
            return Ok(false);
        }
    }
    if read_optional(&directory.join(LEDGER_FILE_NAME))? != ledger_payload
        || read_optional(&directory.join(SNAPSHOT_FILE_NAME))? != snapshot_payload
    {
        return Ok(false);
    }
    match DurableLedger::open_unlocked(directory.to_path_buf(), anchor_store) {
        Ok(restored) => Ok(restored.records == verified.records
            && restored.state == verified.state
            && restored.head_hash() == verified.head_hash()),
        Err(_) => Ok(false),
    }
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
    #[error("daily decision outcome already exists for {0}")]
    DecisionDateCollision(NaiveDate),
    #[error("daily decision ID already exists: {0}")]
    DecisionIdCollision(String),
    #[error("daily decision date {declared} does not match occurrence date {occurred}")]
    DecisionDateMismatch {
        declared: NaiveDate,
        occurred: NaiveDate,
    },
    #[error("capital commitment is already linked to a decision: {0}")]
    DecisionCommitmentCollision(String),
    #[error("unknown purchase decision: {0}")]
    UnknownDecision(String),
    #[error("purchase decision lacks sufficient unsettled capital backing: {0}")]
    InsufficientDecisionBacking(String),
    #[error("order ID collision: {0}")]
    OrderIdCollision(String),
    #[error("unknown order: {0}")]
    UnknownOrder(String),
    #[error("fills exceed the purchase decision plan: {0}")]
    FillExceedsDecisionPlan(String),
    #[error("fills and fees exceed the purchase decision commitment: {0}")]
    DecisionCostsExceedCommitment(String),
    #[error("unknown commitment: {0}")]
    UnknownCommitment(String),
    #[error("commitment already settled: {0}")]
    CommitmentAlreadySettled(String),
    #[error("cash debit exceeds commitment: {0}")]
    DebitExceedsCommitment(String),
    #[error("cash debit is below recorded fills and fees for commitment: {0}")]
    DebitBelowRecordedCosts(String),
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
    #[error("protected latest-head anchor is missing")]
    MissingProtectedAnchor,
    #[error("protected latest-head anchor does not match the journal")]
    ProtectedAnchorMismatch,
    #[error("protected anchor store failed: {0}")]
    ProtectedAnchorStore(String),
    #[error("pending restore transaction is corrupt or belongs to another source")]
    CorruptRestorePending,
    #[error("journal must be a single-linked regular file at its locked path")]
    UnsafeJournalFile,
    #[error("ledger lock must be a distinct single-linked regular file")]
    UnsafeLockFile,
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
    use std::sync::Mutex;

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

    type TestAnchor = Arc<MemoryProtectedAnchorStore>;

    fn anchor_store() -> TestAnchor {
        Arc::new(MemoryProtectedAnchorStore::default())
    }

    fn open(directory: &Path, anchor: &TestAnchor) -> Result<DurableLedger, LedgerError> {
        DurableLedger::open(directory, anchor.clone())
    }

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
        let anchor = anchor_store();
        let mut ledger = open(directory.path(), &anchor).expect("open ledger");
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

        let reopened = open(directory.path(), &anchor).expect("recover old head");
        assert_eq!(reopened.record_count(), 1);
        assert!(!directory.path().join(PENDING_FILE_NAME).exists());
        assert_eq!(
            ledger.append(interrupted).expect("retry append"),
            AppendOutcome::Appended
        );
    }

    #[test]
    fn checkpoint_keeps_its_journal_handle_when_rollback_has_no_tail() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let anchor = anchor_store();
        let mut ledger = open(directory.path(), &anchor).expect("open ledger");
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

        assert_eq!(
            ledger
                .checkpoint()
                .expect("checkpoint after intent-only rollback")
                .record_count,
            1
        );
        assert!(!directory.path().join(PENDING_FILE_NAME).exists());
        assert_eq!(
            ledger.append(interrupted).expect("same handle appends"),
            AppendOutcome::Appended
        );
    }

    #[test]
    fn checkpoint_requires_reopen_after_partial_tail_rollback_replaces_journal() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let anchor = anchor_store();
        let mut ledger = open(directory.path(), &anchor).expect("open ledger");
        ledger
            .append(deposit("deposit-before-partial", 1))
            .expect("append deposit");
        let interrupted = observation("observation-partial", 2);
        {
            let _lock = LedgerLock::acquire(directory.path()).expect("acquire lock");
            ledger.ensure_current().expect("current ledger");
            let prepared = ledger.prepare_append(interrupted).expect("prepare append");
            write_pending(directory.path(), &prepared.pending).expect("persist intent");
            let line = record_line(&prepared.record).expect("encode record");
            let mut file = OpenOptions::new()
                .append(true)
                .open(directory.path().join(LEDGER_FILE_NAME))
                .expect("open journal");
            file.write_all(&line[..line.len() / 2])
                .expect("write partial tail");
            file.sync_all().expect("fsync partial tail");
        }

        assert_eq!(
            ledger.checkpoint(),
            Err(LedgerError::ConcurrentModification)
        );
        assert_eq!(
            open(directory.path(), &anchor)
                .expect("reopen after inode replacement")
                .record_count(),
            1
        );
    }

    #[test]
    fn directory_creation_syncs_each_new_parent_entry() {
        let container = tempfile::tempdir().expect("destination container");
        let first = container.path().join("first");
        let second = first.join("second");
        let destination = second.join("restored");
        let mut synced = Vec::new();

        create_directory_all_durable(&destination, &mut |parent| {
            synced.push(parent.to_path_buf());
            Ok(())
        })
        .expect("durably create nested destination");

        assert!(destination.is_dir());
        assert_eq!(
            synced,
            vec![
                container.path().to_path_buf(),
                first.clone(),
                second.clone()
            ]
        );

        synced.clear();
        create_directory_all_durable(&destination, &mut |parent| {
            synced.push(parent.to_path_buf());
            Ok(())
        })
        .expect("resync an existing destination entry on retry");
        assert_eq!(synced, vec![second]);
    }

    #[test]
    fn ledger_lock_does_not_implicitly_create_a_missing_directory() {
        let container = tempfile::tempdir().expect("directory container");
        let missing = container.path().join("missing");

        assert!(LedgerLock::acquire(&missing).is_err());
        assert!(!missing.exists());
    }

    #[cfg(unix)]
    #[test]
    fn directory_identity_detects_distinct_alias_paths() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().expect("directory");
        let container = tempfile::tempdir().expect("alias container");
        let alias = container.path().join("alias");
        symlink(directory.path(), &alias).expect("create directory alias");

        assert_ne!(directory.path(), alias);
        assert!(same_directory_identity(directory.path(), &alias).expect("compare identities"));
    }

    #[test]
    fn same_instance_retry_rolls_forward_fsynced_record_without_snapshot() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let anchor = anchor_store();
        let mut ledger = open(directory.path(), &anchor).expect("open ledger");
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
            let next_anchor =
                protected_anchor_for_snapshot(&prepared.snapshot).expect("build protected anchor");
            advance_protected_anchor(anchor.as_ref(), ledger.anchor.as_ref(), &next_anchor)
                .expect("commit protected anchor");
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
            open(directory.path(), &anchor)
                .expect("reopen recovered ledger")
                .record_count(),
            2
        );
    }

    #[test]
    fn restart_rolls_forward_an_authorized_partial_record() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let anchor = anchor_store();
        let mut ledger = open(directory.path(), &anchor).expect("open ledger");
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
            let next_anchor =
                protected_anchor_for_snapshot(&prepared.snapshot).expect("build protected anchor");
            advance_protected_anchor(anchor.as_ref(), ledger.anchor.as_ref(), &next_anchor)
                .expect("commit protected anchor");
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
            open(directory.path(), &anchor)
                .expect("recover committed head")
                .record_count(),
            2
        );
        assert_eq!(
            ledger.append(interrupted).expect("retry append"),
            AppendOutcome::Duplicate
        );
        assert_eq!(ledger.record_count(), 2);
    }

    #[test]
    fn restart_rolls_back_a_local_record_without_protected_authorization() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let anchor = anchor_store();
        let mut ledger = open(directory.path(), &anchor).expect("open ledger");
        ledger
            .append(deposit("deposit-before-local-only", 1))
            .expect("append deposit");
        let interrupted = observation("observation-local-only", 2);
        {
            let _lock = LedgerLock::acquire(directory.path()).expect("acquire lock");
            ledger.ensure_current().expect("current ledger");
            let prepared = ledger
                .prepare_append(interrupted.clone())
                .expect("prepare append");
            write_pending(directory.path(), &prepared.pending).expect("persist intent");
            ledger
                .write_record(&prepared.record)
                .expect("fsync uncommitted journal record");
        }

        assert_eq!(
            open(directory.path(), &anchor)
                .expect("roll back uncommitted local record")
                .record_count(),
            1
        );
        assert_eq!(
            ledger.append(interrupted).expect("retry append"),
            AppendOutcome::Appended
        );
    }

    #[test]
    fn settlement_cannot_understate_recorded_costs() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let anchor = anchor_store();
        let mut ledger = open(directory.path(), &anchor).expect("open ledger");
        let decision_date = at(1).date_naive();
        for event in [
            deposit("deposit-settlement-floor", 1),
            LedgerEvent {
                event_id: "admission-settlement-floor".into(),
                occurred_at: at(2),
                kind: LedgerEventKind::DepositAdmission {
                    deposit_event_id: "deposit-settlement-floor".into(),
                    amount_usdc: usd(20),
                },
            },
            LedgerEvent {
                event_id: "commit-settlement-floor".into(),
                occurred_at: at(3),
                kind: LedgerEventKind::CapitalCommitted {
                    commitment_id: "commitment-settlement-floor".into(),
                    amount_usdc: usd(12),
                },
            },
            LedgerEvent {
                event_id: "decision-settlement-floor".into(),
                occurred_at: at(4),
                kind: LedgerEventKind::DailyDecision {
                    decision_id: "purchase-settlement-floor".into(),
                    decision_date,
                    commitment_id: "commitment-settlement-floor".into(),
                    planned_usdc: usd(10),
                    committed_usdc: usd(12),
                },
            },
            LedgerEvent {
                event_id: "order-settlement-floor".into(),
                occurred_at: at(5),
                kind: LedgerEventKind::OrderRecorded {
                    order_id: "order-settlement-floor".into(),
                    decision_id: "purchase-settlement-floor".into(),
                },
            },
            LedgerEvent {
                event_id: "fill-settlement-floor".into(),
                occurred_at: at(6),
                kind: LedgerEventKind::FillRecorded {
                    order_id: "order-settlement-floor".into(),
                    filled_usdc: usd(10),
                    received_hype_atoms: 1,
                },
            },
            LedgerEvent {
                event_id: "fee-settlement-floor".into(),
                occurred_at: at(7),
                kind: LedgerEventKind::FeeRecorded {
                    order_id: "order-settlement-floor".into(),
                    fee_usdc: usd(2),
                },
            },
        ] {
            ledger.append(event).expect("append settlement fixture");
        }
        let durable_before =
            fs::read(directory.path().join(LEDGER_FILE_NAME)).expect("read ledger before reject");

        assert_eq!(
            ledger.append(LedgerEvent {
                event_id: "settlement-below-costs".into(),
                occurred_at: at(8),
                kind: LedgerEventKind::CapitalSettled {
                    commitment_id: "commitment-settlement-floor".into(),
                    debited_usdc: usd(10),
                },
            }),
            Err(LedgerError::DebitBelowRecordedCosts(
                "commitment-settlement-floor".into()
            ))
        );
        assert_eq!(
            fs::read(directory.path().join(LEDGER_FILE_NAME)).expect("read unchanged ledger"),
            durable_before
        );
        ledger
            .append(LedgerEvent {
                event_id: "settlement-at-fill".into(),
                occurred_at: at(9),
                kind: LedgerEventKind::CapitalSettled {
                    commitment_id: "commitment-settlement-floor".into(),
                    debited_usdc: usd(12),
                },
            })
            .expect("settle at the recorded cost floor");
        assert_eq!(ledger.state().spent_usdc(), usd(12));
    }

    #[test]
    fn restore_resumes_after_journal_publication_and_is_idempotent() {
        let source = tempfile::tempdir().expect("source directory");
        let container = tempfile::tempdir().expect("destination container");
        let destination = container.path().join("restored");
        let source_anchor = anchor_store();
        let destination_anchor = anchor_store();
        let mut ledger = open(source.path(), &source_anchor).expect("open source ledger");
        ledger
            .append(deposit("deposit-before-restore", 1))
            .expect("append deposit");
        drop(ledger);

        let verified = open(source.path(), &source_anchor).expect("verify source ledger");
        let ledger_payload =
            fs::read(source.path().join(LEDGER_FILE_NAME)).expect("read source journal");
        let snapshot_payload =
            fs::read(source.path().join(SNAPSHOT_FILE_NAME)).expect("read source snapshot");
        let pending = build_pending_restore(&ledger_payload, &snapshot_payload, &verified)
            .expect("build restore intent");
        {
            canonical_directory(&destination).expect("durably create destination directory");
            let _lock = LedgerLock::acquire(&destination).expect("acquire destination lock");
            write_pending_restore(&destination, &pending).expect("persist restore intent");
            write_atomic(&destination.join(LEDGER_FILE_NAME), &ledger_payload)
                .expect("publish journal before interruption");
        }

        let restored = DurableLedger::restore_clean(
            source.path(),
            &destination,
            source_anchor.clone(),
            destination_anchor.clone(),
        )
        .expect("resume interrupted restore");
        assert_eq!(restored.records, verified.records);
        assert_eq!(restored.state, verified.state);
        assert!(!destination.join(RESTORE_PENDING_FILE_NAME).exists());

        let retried = DurableLedger::restore_clean(
            source.path(),
            &destination,
            source_anchor,
            destination_anchor,
        )
        .expect("accept exact completed restore retry");
        assert_eq!(retried.records, verified.records);
        assert_eq!(retried.state, verified.state);
    }

    #[test]
    fn restore_recovers_an_authenticated_pending_temporary() {
        let source = tempfile::tempdir().expect("source directory");
        let container = tempfile::tempdir().expect("destination container");
        let destination = container.path().join("restored");
        let source_anchor = anchor_store();
        let destination_anchor = anchor_store();
        let mut ledger = open(source.path(), &source_anchor).expect("open source ledger");
        ledger
            .append(deposit("deposit-before-pending-temporary", 1))
            .expect("append deposit");
        drop(ledger);

        let verified = open(source.path(), &source_anchor).expect("verify source ledger");
        let ledger_payload =
            fs::read(source.path().join(LEDGER_FILE_NAME)).expect("read source journal");
        let snapshot_payload =
            fs::read(source.path().join(SNAPSHOT_FILE_NAME)).expect("read source snapshot");
        let pending = build_pending_restore(&ledger_payload, &snapshot_payload, &verified)
            .expect("build restore intent");
        let pending_payload = encode_pending_restore(&pending).expect("encode restore intent");
        canonical_directory(&destination).expect("durably create destination directory");
        drop(LedgerLock::acquire(&destination).expect("create destination lock"));
        let orphan = destination.join(".pending-restore.json.123.456.tmp");
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&orphan)
            .expect("create pending temporary");
        file.write_all(&pending_payload)
            .expect("write pending temporary");
        file.sync_all().expect("fsync pending temporary");
        sync_directory(&destination).expect("sync pending temporary entry");

        let restored = DurableLedger::restore_clean(
            source.path(),
            &destination,
            source_anchor,
            destination_anchor,
        )
        .expect("recover authenticated pending temporary");
        assert_eq!(restored.records, verified.records);
        assert!(!orphan.exists());
        assert!(!destination.join(RESTORE_PENDING_FILE_NAME).exists());
    }

    #[test]
    fn restore_removes_only_byte_exact_payload_temporaries() {
        let source = tempfile::tempdir().expect("source directory");
        let container = tempfile::tempdir().expect("destination container");
        let destination = container.path().join("restored");
        let source_anchor = anchor_store();
        let destination_anchor = anchor_store();
        let mut ledger = open(source.path(), &source_anchor).expect("open source ledger");
        ledger
            .append(deposit("deposit-before-payload-temporaries", 1))
            .expect("append deposit");
        drop(ledger);

        let verified = open(source.path(), &source_anchor).expect("verify source ledger");
        let ledger_payload =
            fs::read(source.path().join(LEDGER_FILE_NAME)).expect("read source journal");
        let snapshot_payload =
            fs::read(source.path().join(SNAPSHOT_FILE_NAME)).expect("read source snapshot");
        let pending = build_pending_restore(&ledger_payload, &snapshot_payload, &verified)
            .expect("build restore intent");
        canonical_directory(&destination).expect("durably create destination directory");
        {
            let _lock = LedgerLock::acquire(&destination).expect("acquire destination lock");
            write_pending_restore(&destination, &pending).expect("persist restore intent");
            for (name, payload) in [
                ("ledger.jsonl.123.456.tmp", ledger_payload.as_slice()),
                ("snapshot.json.123.456.tmp", snapshot_payload.as_slice()),
            ] {
                let path = destination.join(name);
                let mut file = OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(path)
                    .expect("create payload temporary");
                file.write_all(payload).expect("write payload temporary");
                file.sync_all().expect("fsync payload temporary");
            }
            sync_directory(&destination).expect("sync payload temporary entries");
        }

        let restored = DurableLedger::restore_clean(
            source.path(),
            &destination,
            source_anchor,
            destination_anchor,
        )
        .expect("recover byte-exact payload temporaries");
        assert_eq!(restored.records, verified.records);
        assert!(!destination.join("ledger.jsonl.123.456.tmp").exists());
        assert!(!destination.join("snapshot.json.123.456.tmp").exists());
    }

    #[test]
    fn restore_completes_an_anchored_prefix_after_the_source_advances() {
        let source = tempfile::tempdir().expect("source directory");
        let container = tempfile::tempdir().expect("destination container");
        let destination = container.path().join("restored-prefix");
        let latest_destination = container.path().join("restored-latest");
        let source_anchor = anchor_store();
        let destination_anchor = anchor_store();
        let latest_destination_anchor = anchor_store();
        let mut ledger = open(source.path(), &source_anchor).expect("open source ledger");
        ledger
            .append(deposit("deposit-before-restore", 1))
            .expect("append deposit");
        drop(ledger);

        let verified_prefix = open(source.path(), &source_anchor).expect("verify source prefix");
        let prefix_head = verified_prefix.head_hash().to_owned();
        let prefix_state = verified_prefix.state.clone();
        let ledger_payload =
            fs::read(source.path().join(LEDGER_FILE_NAME)).expect("read source journal");
        let snapshot_payload =
            fs::read(source.path().join(SNAPSHOT_FILE_NAME)).expect("read source snapshot");
        let pending = build_pending_restore(&ledger_payload, &snapshot_payload, &verified_prefix)
            .expect("build restore intent");
        {
            canonical_directory(&destination).expect("durably create destination directory");
            let _lock = LedgerLock::acquire(&destination).expect("acquire destination lock");
            write_pending_restore(&destination, &pending).expect("persist restore intent");
            ensure_restore_anchor(
                destination_anchor.as_ref(),
                None,
                verified_prefix.anchor.as_ref(),
            )
            .expect("advance destination anchor");
            write_atomic(&destination.join(LEDGER_FILE_NAME), &ledger_payload)
                .expect("publish journal before interruption");
        }

        let mut advanced = open(source.path(), &source_anchor).expect("reopen source ledger");
        advanced
            .append(observation("observation-after-restore-crash", 2))
            .expect("advance source after restore crash");
        let latest_head = advanced.head_hash().to_owned();
        let latest_state = advanced.state.clone();
        drop(advanced);

        let restored_prefix = DurableLedger::restore_clean(
            source.path(),
            &destination,
            source_anchor.clone(),
            destination_anchor,
        )
        .expect("complete the independently authorized prefix restore");
        assert_eq!(restored_prefix.record_count(), 1);
        assert_eq!(restored_prefix.head_hash(), prefix_head);
        assert_eq!(restored_prefix.state, prefix_state);
        assert!(!destination.join(RESTORE_PENDING_FILE_NAME).exists());

        let restored_latest = DurableLedger::restore_clean(
            source.path(),
            &latest_destination,
            source_anchor,
            latest_destination_anchor,
        )
        .expect("restore the latest source head separately");
        assert_eq!(restored_latest.record_count(), 2);
        assert_eq!(restored_latest.head_hash(), latest_head);
        assert_eq!(restored_latest.state, latest_state);
    }

    #[test]
    fn stale_restore_intent_without_a_destination_anchor_fails_closed() {
        let source = tempfile::tempdir().expect("source directory");
        let container = tempfile::tempdir().expect("destination container");
        let destination = container.path().join("restored");
        let source_anchor = anchor_store();
        let destination_anchor = anchor_store();
        let mut ledger = open(source.path(), &source_anchor).expect("open source ledger");
        ledger
            .append(deposit("deposit-before-restore", 1))
            .expect("append deposit");
        drop(ledger);

        let verified_prefix = open(source.path(), &source_anchor).expect("verify source prefix");
        let ledger_payload =
            fs::read(source.path().join(LEDGER_FILE_NAME)).expect("read source journal");
        let snapshot_payload =
            fs::read(source.path().join(SNAPSHOT_FILE_NAME)).expect("read source snapshot");
        let pending = build_pending_restore(&ledger_payload, &snapshot_payload, &verified_prefix)
            .expect("build restore intent");
        canonical_directory(&destination).expect("durably create destination directory");
        {
            let _lock = LedgerLock::acquire(&destination).expect("acquire destination lock");
            write_pending_restore(&destination, &pending).expect("persist restore intent");
        }

        let mut advanced = open(source.path(), &source_anchor).expect("reopen source ledger");
        advanced
            .append(observation("observation-after-unanchored-intent", 2))
            .expect("advance source after unanchored intent");
        drop(advanced);

        assert!(matches!(
            DurableLedger::restore_clean(
                source.path(),
                &destination,
                source_anchor,
                destination_anchor,
            ),
            Err(LedgerError::ProtectedAnchorMismatch)
        ));
        assert!(destination.join(RESTORE_PENDING_FILE_NAME).exists());
    }

    #[test]
    fn stale_empty_restore_intent_cannot_be_independently_authorized() {
        let source = tempfile::tempdir().expect("source directory");
        let container = tempfile::tempdir().expect("destination container");
        let destination = container.path().join("restored");
        let source_anchor = anchor_store();
        let destination_anchor = anchor_store();
        let empty = open(source.path(), &source_anchor).expect("open empty source ledger");
        empty.checkpoint().expect("checkpoint empty source");
        let ledger_payload =
            fs::read(source.path().join(LEDGER_FILE_NAME)).expect("read empty journal");
        let snapshot_payload =
            fs::read(source.path().join(SNAPSHOT_FILE_NAME)).expect("read empty snapshot");
        let pending = build_pending_restore(&ledger_payload, &snapshot_payload, &empty)
            .expect("build empty restore intent");
        drop(empty);

        canonical_directory(&destination).expect("durably create destination directory");
        {
            let _lock = LedgerLock::acquire(&destination).expect("acquire destination lock");
            write_pending_restore(&destination, &pending).expect("persist empty restore intent");
        }

        let mut advanced = open(source.path(), &source_anchor).expect("reopen source ledger");
        advanced
            .append(deposit("deposit-after-empty-restore-intent", 1))
            .expect("advance source");
        drop(advanced);

        assert!(matches!(
            DurableLedger::restore_clean(
                source.path(),
                &destination,
                source_anchor,
                destination_anchor,
            ),
            Err(LedgerError::ProtectedAnchorMismatch)
        ));
        assert!(destination.join(RESTORE_PENDING_FILE_NAME).exists());
    }

    #[test]
    fn anchored_restore_intent_for_a_different_source_chain_fails_closed() {
        let original_source = tempfile::tempdir().expect("original source directory");
        let current_source = tempfile::tempdir().expect("current source directory");
        let container = tempfile::tempdir().expect("destination container");
        let destination = container.path().join("restored");
        let original_source_anchor = anchor_store();
        let current_source_anchor = anchor_store();
        let destination_anchor = anchor_store();

        let mut original =
            open(original_source.path(), &original_source_anchor).expect("open original source");
        original
            .append(deposit("deposit-original-chain", 1))
            .expect("append original deposit");
        drop(original);
        let verified_original =
            open(original_source.path(), &original_source_anchor).expect("verify original source");
        let original_ledger =
            fs::read(original_source.path().join(LEDGER_FILE_NAME)).expect("read original journal");
        let original_snapshot = fs::read(original_source.path().join(SNAPSHOT_FILE_NAME))
            .expect("read original snapshot");
        let pending =
            build_pending_restore(&original_ledger, &original_snapshot, &verified_original)
                .expect("build original restore intent");

        canonical_directory(&destination).expect("durably create destination directory");
        {
            let _lock = LedgerLock::acquire(&destination).expect("acquire destination lock");
            write_pending_restore(&destination, &pending).expect("persist restore intent");
            ensure_restore_anchor(
                destination_anchor.as_ref(),
                None,
                verified_original.anchor.as_ref(),
            )
            .expect("advance destination anchor");
        }

        let mut current =
            open(current_source.path(), &current_source_anchor).expect("open current source");
        current
            .append(deposit("deposit-different-chain", 1))
            .expect("append different deposit");
        drop(current);

        assert!(matches!(
            DurableLedger::restore_clean(
                current_source.path(),
                &destination,
                current_source_anchor,
                destination_anchor,
            ),
            Err(LedgerError::CorruptRestorePending)
        ));
        assert!(destination.join(RESTORE_PENDING_FILE_NAME).exists());
    }
}
