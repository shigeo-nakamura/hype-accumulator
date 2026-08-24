//! Provider-neutral, point-in-time signal normalization and daily snapshots.
//!
//! This module is deliberately offline-only. It contains no provider client,
//! credential, sizing, order, or workflow integration. Both live and backtest
//! callers must pass raw provider responses through the same typed normalizer.

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{btree_map::Entry, BTreeMap},
    fmt::Write as _,
};
use thiserror::Error;

const SIGNAL_SCHEMA_VERSION: u8 = 1;
pub const NEUTRAL_MULTIPLIER_BPS: u16 = 10_000;

/// A positive market price represented in integer microunits.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct PriceMicrounits(u64);

impl PriceMicrounits {
    /// Constructs a positive exact price.
    ///
    /// # Errors
    ///
    /// Returns [`SignalError::InvalidPrice`] for zero. Using an integer input
    /// makes overflow or float-rounding impossible at this boundary.
    pub const fn new(value: u64) -> Result<Self, SignalError> {
        if value == 0 {
            Err(SignalError::InvalidPrice)
        } else {
            Ok(Self(value))
        }
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl TryFrom<u64> for PriceMicrounits {
    type Error = SignalError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl<'de> Deserialize<'de> for PriceMicrounits {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u64::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

/// Ordered point-in-time timestamps for one provider revision.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RevisionTimestamps {
    observed_at: DateTime<Utc>,
    published_at: DateTime<Utc>,
    fetched_at: DateTime<Utc>,
    first_usable_at: DateTime<Utc>,
}

#[derive(Deserialize)]
#[allow(clippy::struct_field_names)]
struct RevisionTimestampsWire {
    observed_at: DateTime<Utc>,
    published_at: DateTime<Utc>,
    fetched_at: DateTime<Utc>,
    first_usable_at: DateTime<Utc>,
}

impl RevisionTimestamps {
    /// Creates timestamps after enforcing the complete availability ordering.
    ///
    /// # Errors
    ///
    /// Returns [`SignalError::InvalidTimestampOrder`] unless
    /// `observed <= published <= fetched <= first_usable`.
    pub fn new(
        observed_at: DateTime<Utc>,
        published_at: DateTime<Utc>,
        fetched_at: DateTime<Utc>,
        first_usable_at: DateTime<Utc>,
    ) -> Result<Self, SignalError> {
        if observed_at > published_at || published_at > fetched_at || fetched_at > first_usable_at {
            return Err(SignalError::InvalidTimestampOrder);
        }
        Ok(Self {
            observed_at,
            published_at,
            fetched_at,
            first_usable_at,
        })
    }

    #[must_use]
    pub const fn observed_at(&self) -> DateTime<Utc> {
        self.observed_at
    }

    #[must_use]
    pub const fn published_at(&self) -> DateTime<Utc> {
        self.published_at
    }

    #[must_use]
    pub const fn fetched_at(&self) -> DateTime<Utc> {
        self.fetched_at
    }

    #[must_use]
    pub const fn first_usable_at(&self) -> DateTime<Utc> {
        self.first_usable_at
    }
}

impl<'de> Deserialize<'de> for RevisionTimestamps {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = RevisionTimestampsWire::deserialize(deserializer)?;
        Self::new(
            value.observed_at,
            value.published_at,
            value.fetched_at,
            value.first_usable_at,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// The authoritative identity of one revision slot.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct RevisionIdentity {
    source: String,
    source_version: String,
    series: String,
    observation_date: NaiveDate,
    revision_id: String,
}

#[derive(Deserialize)]
struct RevisionIdentityWire {
    source: String,
    source_version: String,
    series: String,
    observation_date: NaiveDate,
    revision_id: String,
}

impl RevisionIdentity {
    /// Constructs a revision identity from non-empty, trimmed components.
    ///
    /// # Errors
    ///
    /// Returns [`SignalError::InvalidIdentity`] when a text component is empty
    /// or has surrounding whitespace.
    pub fn new(
        source: impl Into<String>,
        source_version: impl Into<String>,
        series: impl Into<String>,
        observation_date: NaiveDate,
        revision_id: impl Into<String>,
    ) -> Result<Self, SignalError> {
        let value = Self {
            source: source.into(),
            source_version: source_version.into(),
            series: series.into(),
            observation_date,
            revision_id: revision_id.into(),
        };
        if invalid_component(&value.source)
            || invalid_component(&value.source_version)
            || invalid_component(&value.series)
            || invalid_component(&value.revision_id)
        {
            return Err(SignalError::InvalidIdentity);
        }
        Ok(value)
    }

    #[must_use]
    pub fn source(&self) -> &str {
        &self.source
    }

    #[must_use]
    pub fn source_version(&self) -> &str {
        &self.source_version
    }

    #[must_use]
    pub fn series(&self) -> &str {
        &self.series
    }

    #[must_use]
    pub const fn observation_date(&self) -> NaiveDate {
        self.observation_date
    }

    #[must_use]
    pub fn revision_id(&self) -> &str {
        &self.revision_id
    }
}

impl<'de> Deserialize<'de> for RevisionIdentity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = RevisionIdentityWire::deserialize(deserializer)?;
        Self::new(
            value.source,
            value.source_version,
            value.series,
            value.observation_date,
            value.revision_id,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// One normalized point-in-time revision.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SignalRevision<T> {
    identity: RevisionIdentity,
    timestamps: RevisionTimestamps,
    value: T,
}

impl<T> SignalRevision<T> {
    #[must_use]
    pub const fn new(identity: RevisionIdentity, timestamps: RevisionTimestamps, value: T) -> Self {
        Self {
            identity,
            timestamps,
            value,
        }
    }

    #[must_use]
    pub const fn identity(&self) -> &RevisionIdentity {
        &self.identity
    }

    #[must_use]
    pub const fn timestamps(&self) -> &RevisionTimestamps {
        &self.timestamps
    }

    #[must_use]
    pub const fn value(&self) -> &T {
        &self.value
    }
}

/// Checked execution and book prices captured atomically by the source.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CoreMarketData {
    execution_price: PriceMicrounits,
    reference_price: PriceMicrounits,
    bid_price: PriceMicrounits,
    ask_price: PriceMicrounits,
}

#[derive(Deserialize)]
#[allow(clippy::struct_field_names)]
struct CoreMarketDataWire {
    execution_price: PriceMicrounits,
    reference_price: PriceMicrounits,
    bid_price: PriceMicrounits,
    ask_price: PriceMicrounits,
}

impl CoreMarketData {
    /// Constructs an uncrossed positive-price market observation.
    ///
    /// # Errors
    ///
    /// Returns [`SignalError::CrossedBook`] when bid exceeds ask.
    pub const fn new(
        execution_price: PriceMicrounits,
        reference_price: PriceMicrounits,
        bid_price: PriceMicrounits,
        ask_price: PriceMicrounits,
    ) -> Result<Self, SignalError> {
        if bid_price.0 > ask_price.0 {
            return Err(SignalError::CrossedBook);
        }
        Ok(Self {
            execution_price,
            reference_price,
            bid_price,
            ask_price,
        })
    }

    #[must_use]
    pub const fn execution_price(&self) -> PriceMicrounits {
        self.execution_price
    }

    #[must_use]
    pub const fn reference_price(&self) -> PriceMicrounits {
        self.reference_price
    }

    #[must_use]
    pub const fn bid_price(&self) -> PriceMicrounits {
        self.bid_price
    }

    #[must_use]
    pub const fn ask_price(&self) -> PriceMicrounits {
        self.ask_price
    }
}

impl<'de> Deserialize<'de> for CoreMarketData {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = CoreMarketDataWire::deserialize(deserializer)?;
        Self::new(
            value.execution_price,
            value.reference_price,
            value.bid_price,
            value.ask_price,
        )
        .map_err(serde::de::Error::custom)
    }
}

/// A normalized auxiliary observation and its precomputed feature.
///
/// Both fields are exact signed integers. They are retained for audit but do
/// not affect pacing in this fixed-DCA-safe slice.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct AuxiliarySignal {
    pub raw_value_microunits: i64,
    pub feature_value_bps: i32,
}

/// A fully specified lookup that cannot silently forward-fill another date.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RevisionQuery {
    source: String,
    source_version: String,
    series: String,
    observation_date: NaiveDate,
}

impl RevisionQuery {
    /// Constructs an exact-date revision query.
    ///
    /// # Errors
    ///
    /// Returns [`SignalError::InvalidIdentity`] for invalid text components.
    pub fn new(
        source: impl Into<String>,
        source_version: impl Into<String>,
        series: impl Into<String>,
        observation_date: NaiveDate,
    ) -> Result<Self, SignalError> {
        let value = Self {
            source: source.into(),
            source_version: source_version.into(),
            series: series.into(),
            observation_date,
        };
        if invalid_component(&value.source)
            || invalid_component(&value.source_version)
            || invalid_component(&value.series)
        {
            return Err(SignalError::InvalidIdentity);
        }
        Ok(value)
    }

    #[must_use]
    pub fn source(&self) -> &str {
        &self.source
    }

    #[must_use]
    pub fn source_version(&self) -> &str {
        &self.source_version
    }

    #[must_use]
    pub fn series(&self) -> &str {
        &self.series
    }

    #[must_use]
    pub const fn observation_date(&self) -> NaiveDate {
        self.observation_date
    }

    fn is_valid(&self) -> bool {
        !invalid_component(&self.source)
            && !invalid_component(&self.source_version)
            && !invalid_component(&self.series)
    }

    fn matches(&self, identity: &RevisionIdentity) -> bool {
        identity.source == self.source
            && identity.source_version == self.source_version
            && identity.series == self.series
            && identity.observation_date == self.observation_date
    }
}

/// An exact-date query plus its strict freshness boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FreshnessRequirement {
    query: RevisionQuery,
    stale_after_seconds: u64,
}

impl FreshnessRequirement {
    /// Constructs a requirement with a positive exclusive freshness limit.
    ///
    /// # Errors
    ///
    /// Returns [`SignalError::InvalidFreshnessLimit`] for zero.
    pub fn new(query: RevisionQuery, stale_after_seconds: u64) -> Result<Self, SignalError> {
        if stale_after_seconds == 0 {
            return Err(SignalError::InvalidFreshnessLimit);
        }
        Ok(Self {
            query,
            stale_after_seconds,
        })
    }
}

/// Insert result for revision and daily snapshot replay.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InsertOutcome {
    Inserted,
    Existing,
}

/// An identity-indexed revision set with conflict-safe replay.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RevisionBook<T> {
    entries: BTreeMap<RevisionIdentity, SignalRevision<T>>,
}

impl<T> Default for RevisionBook<T> {
    fn default() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }
}

impl<T: Eq> RevisionBook<T> {
    /// Inserts a revision. Exact identity/payload replay is idempotent, while a
    /// different payload under the same identity fails closed.
    ///
    /// # Errors
    ///
    /// Returns [`SignalError::ConflictingRevision`] on identity reuse.
    pub fn insert(&mut self, revision: SignalRevision<T>) -> Result<InsertOutcome, SignalError> {
        match self.entries.entry(revision.identity.clone()) {
            Entry::Vacant(entry) => {
                entry.insert(revision);
                Ok(InsertOutcome::Inserted)
            }
            Entry::Occupied(entry) if entry.get() == &revision => Ok(InsertOutcome::Existing),
            Entry::Occupied(entry) => Err(SignalError::ConflictingRevision(
                entry.key().revision_id.clone(),
            )),
        }
    }

    /// Returns the latest exact-date revision usable at the decision time.
    #[must_use]
    pub fn select_exact(
        &self,
        query: &RevisionQuery,
        decision_at: DateTime<Utc>,
    ) -> Option<&SignalRevision<T>> {
        self.entries
            .values()
            .filter(|revision| query.matches(&revision.identity))
            .filter(|revision| revision.timestamps.first_usable_at <= decision_at)
            .max_by_key(|revision| {
                (
                    revision.timestamps.first_usable_at,
                    revision.timestamps.fetched_at,
                    revision.timestamps.published_at,
                    revision.identity.revision_id.clone(),
                )
            })
    }

    fn first_future(
        &self,
        query: &RevisionQuery,
        decision_at: DateTime<Utc>,
    ) -> Option<DateTime<Utc>> {
        self.entries
            .values()
            .filter(|revision| query.matches(&revision.identity))
            .map(|revision| revision.timestamps.first_usable_at)
            .filter(|first_usable_at| *first_usable_at > decision_at)
            .min()
    }
}

/// Purchase-gating health of core execution and book data.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "status")]
pub enum CoreHealth {
    Healthy { age_seconds: u64 },
    Missing,
    Future { first_usable_at: DateTime<Utc> },
    Stale { age_seconds: u64 },
}

/// Independent auxiliary-input health. Every variant remains neutral here.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "status")]
pub enum AuxiliaryHealth {
    Healthy { age_seconds: u64 },
    Missing,
    Future { first_usable_at: DateTime<Utc> },
    Stale { age_seconds: u64 },
}

/// Inputs needed to materialize one immutable UTC-day snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotRequest {
    decision_at: DateTime<Utc>,
    core: FreshnessRequirement,
    auxiliary: FreshnessRequirement,
}

impl SnapshotRequest {
    #[must_use]
    pub const fn new(
        decision_at: DateTime<Utc>,
        core: FreshnessRequirement,
        auxiliary: FreshnessRequirement,
    ) -> Self {
        Self {
            decision_at,
            core,
            auxiliary,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct SignalSnapshotBody {
    schema_version: u8,
    decision_date: NaiveDate,
    decision_at: DateTime<Utc>,
    core_query: RevisionQuery,
    core_stale_after_seconds: u64,
    core: Option<SignalRevision<CoreMarketData>>,
    core_health: CoreHealth,
    auxiliary_query: RevisionQuery,
    auxiliary_stale_after_seconds: u64,
    auxiliary: Option<SignalRevision<AuxiliarySignal>>,
    auxiliary_health: AuxiliaryHealth,
    pacing_multiplier_bps: u16,
    purchase_eligible: bool,
}

/// Canonical, typed daily decision input with a self-verifying SHA-256 hash.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SignalSnapshot {
    #[serde(flatten)]
    body: SignalSnapshotBody,
    snapshot_hash: String,
}

impl SignalSnapshot {
    fn from_body(body: SignalSnapshotBody) -> Result<Self, SignalError> {
        let snapshot_hash = canonical_hash(&body)?;
        Ok(Self {
            body,
            snapshot_hash,
        })
    }

    /// Parses typed JSON, validates all snapshot invariants, and verifies the
    /// canonical hash. JSON object field order is irrelevant.
    ///
    /// # Errors
    ///
    /// Returns [`SignalError`] for malformed JSON, unsupported schema,
    /// inconsistent health/eligibility fields, or a hash mismatch.
    pub fn from_json(input: &str) -> Result<Self, SignalError> {
        let snapshot: Self =
            serde_json::from_str(input).map_err(|error| SignalError::Json(error.to_string()))?;
        snapshot.validate()?;
        Ok(snapshot)
    }

    /// Serializes the typed snapshot in stable struct-field order.
    ///
    /// # Errors
    ///
    /// Returns [`SignalError::SnapshotSerialization`] on serialization failure.
    pub fn to_canonical_json(&self) -> Result<String, SignalError> {
        self.validate()?;
        serde_json::to_string(self)
            .map_err(|error| SignalError::SnapshotSerialization(error.to_string()))
    }

    /// Returns canonical JSON bytes of only the hashed fields.
    ///
    /// # Errors
    ///
    /// Returns [`SignalError::SnapshotSerialization`] on serialization failure.
    pub fn canonical_bytes_without_hash(&self) -> Result<Vec<u8>, SignalError> {
        self.validate_invariants()?;
        serde_json::to_vec(&self.body)
            .map_err(|error| SignalError::SnapshotSerialization(error.to_string()))
    }

    /// Verifies structural invariants and the SHA-256 stored with the snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`SignalError`] when the snapshot is invalid or has been changed.
    pub fn validate(&self) -> Result<(), SignalError> {
        self.validate_invariants()?;
        if self.snapshot_hash != canonical_hash(&self.body)? {
            return Err(SignalError::InvalidSnapshotHash);
        }
        Ok(())
    }

    fn validate_invariants(&self) -> Result<(), SignalError> {
        if self.body.schema_version != SIGNAL_SCHEMA_VERSION {
            return Err(SignalError::UnsupportedSchema(self.body.schema_version));
        }
        if self.body.decision_date != self.body.decision_at.date_naive()
            || !self.body.core_query.is_valid()
            || !self.body.auxiliary_query.is_valid()
            || self.body.core_stale_after_seconds == 0
            || self.body.auxiliary_stale_after_seconds == 0
            || self.body.pacing_multiplier_bps != NEUTRAL_MULTIPLIER_BPS
            || self.body.purchase_eligible
                != matches!(self.body.core_health, CoreHealth::Healthy { .. })
            || core_health_mismatch(
                self.body.core.as_ref(),
                &self.body.core_health,
                &self.body.core_query,
                self.body.decision_at,
                self.body.core_stale_after_seconds,
            )
            || auxiliary_health_mismatch(
                self.body.auxiliary.as_ref(),
                &self.body.auxiliary_health,
                &self.body.auxiliary_query,
                self.body.decision_at,
                self.body.auxiliary_stale_after_seconds,
            )
        {
            return Err(SignalError::InvalidSnapshotInvariant);
        }
        Ok(())
    }

    #[must_use]
    pub const fn decision_date(&self) -> NaiveDate {
        self.body.decision_date
    }

    #[must_use]
    pub const fn decision_at(&self) -> DateTime<Utc> {
        self.body.decision_at
    }

    #[must_use]
    pub const fn core_query(&self) -> &RevisionQuery {
        &self.body.core_query
    }

    #[must_use]
    pub const fn auxiliary_query(&self) -> &RevisionQuery {
        &self.body.auxiliary_query
    }

    #[must_use]
    pub const fn core(&self) -> Option<&SignalRevision<CoreMarketData>> {
        self.body.core.as_ref()
    }

    #[must_use]
    pub const fn core_health(&self) -> &CoreHealth {
        &self.body.core_health
    }

    #[must_use]
    pub const fn auxiliary(&self) -> Option<&SignalRevision<AuxiliarySignal>> {
        self.body.auxiliary.as_ref()
    }

    #[must_use]
    pub const fn auxiliary_health(&self) -> &AuxiliaryHealth {
        &self.body.auxiliary_health
    }

    #[must_use]
    pub const fn pacing_multiplier_bps(&self) -> u16 {
        self.body.pacing_multiplier_bps
    }

    #[must_use]
    pub const fn purchase_eligible(&self) -> bool {
        self.body.purchase_eligible
    }

    #[must_use]
    pub fn snapshot_hash(&self) -> &str {
        &self.snapshot_hash
    }
}

/// A validated provider-neutral feed used by both production and replay code.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NormalizedSignalFeed {
    core: RevisionBook<CoreMarketData>,
    auxiliary: RevisionBook<AuxiliarySignal>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSignalFixture {
    schema_version: u8,
    core_revisions: Vec<SignalRevision<CoreMarketData>>,
    auxiliary_revisions: Vec<SignalRevision<AuxiliarySignal>>,
}

impl NormalizedSignalFeed {
    fn from_raw_json(input: &str) -> Result<Self, SignalError> {
        let fixture: RawSignalFixture =
            serde_json::from_str(input).map_err(|error| SignalError::Json(error.to_string()))?;
        if fixture.schema_version != SIGNAL_SCHEMA_VERSION {
            return Err(SignalError::UnsupportedSchema(fixture.schema_version));
        }
        let mut core = RevisionBook::default();
        for revision in fixture.core_revisions {
            core.insert(revision)?;
        }
        let mut auxiliary = RevisionBook::default();
        for revision in fixture.auxiliary_revisions {
            auxiliary.insert(revision)?;
        }
        Ok(Self { core, auxiliary })
    }

    /// Produces one typed snapshot without looking ahead or forward-filling.
    ///
    /// # Errors
    ///
    /// Returns [`SignalError`] only if canonical serialization fails.
    pub fn snapshot(&self, request: &SnapshotRequest) -> Result<SignalSnapshot, SignalError> {
        let (core, core_health) = select_core(&self.core, &request.core, request.decision_at);
        let (auxiliary, auxiliary_health) =
            select_auxiliary(&self.auxiliary, &request.auxiliary, request.decision_at);
        let purchase_eligible = matches!(core_health, CoreHealth::Healthy { .. });
        SignalSnapshot::from_body(SignalSnapshotBody {
            schema_version: SIGNAL_SCHEMA_VERSION,
            decision_date: request.decision_at.date_naive(),
            decision_at: request.decision_at,
            core_query: request.core.query.clone(),
            core_stale_after_seconds: request.core.stale_after_seconds,
            core,
            core_health,
            auxiliary_query: request.auxiliary.query.clone(),
            auxiliary_stale_after_seconds: request.auxiliary.stale_after_seconds,
            auxiliary,
            auxiliary_health,
            pacing_multiplier_bps: NEUTRAL_MULTIPLIER_BPS,
            purchase_eligible,
        })
    }
}

/// Production-entry normalizer. Network fetching is intentionally out of scope.
pub struct LiveSignalNormalizer;

impl LiveSignalNormalizer {
    /// Normalizes provider-neutral raw JSON using the shared replay-safe path.
    ///
    /// # Errors
    ///
    /// Returns [`SignalError`] for malformed or conflicting input.
    pub fn normalize_json(input: &str) -> Result<NormalizedSignalFeed, SignalError> {
        NormalizedSignalFeed::from_raw_json(input)
    }
}

/// Backtest-entry normalizer. It is intentionally identical to the live path.
pub struct BacktestSignalNormalizer;

impl BacktestSignalNormalizer {
    /// Normalizes replay JSON using the exact production normalization path.
    ///
    /// # Errors
    ///
    /// Returns [`SignalError`] for malformed or conflicting input.
    pub fn normalize_json(input: &str) -> Result<NormalizedSignalFeed, SignalError> {
        NormalizedSignalFeed::from_raw_json(input)
    }
}

/// One immutable snapshot per UTC calendar date.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DailySnapshotStore {
    entries: BTreeMap<NaiveDate, SignalSnapshot>,
}

impl DailySnapshotStore {
    /// Inserts a verified snapshot. Semantic replay is idempotent, while any
    /// differing second record for the same UTC date fails closed.
    ///
    /// # Errors
    ///
    /// Returns [`SignalError`] for an invalid hash/invariant or date conflict.
    pub fn insert(&mut self, snapshot: SignalSnapshot) -> Result<InsertOutcome, SignalError> {
        snapshot.validate()?;
        match self.entries.entry(snapshot.decision_date()) {
            Entry::Vacant(entry) => {
                entry.insert(snapshot);
                Ok(InsertOutcome::Inserted)
            }
            Entry::Occupied(entry) if entry.get() == &snapshot => Ok(InsertOutcome::Existing),
            Entry::Occupied(entry) => Err(SignalError::ConflictingDailySnapshot(*entry.key())),
        }
    }

    /// Parses and inserts a typed snapshot. Shuffled JSON object fields still
    /// replay idempotently because comparison follows typed canonicalization.
    ///
    /// # Errors
    ///
    /// Returns [`SignalError`] for malformed/invalid JSON or date conflict.
    pub fn insert_json(&mut self, input: &str) -> Result<InsertOutcome, SignalError> {
        self.insert(SignalSnapshot::from_json(input)?)
    }

    #[must_use]
    pub fn get(&self, date: NaiveDate) -> Option<&SignalSnapshot> {
        self.entries.get(&date)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum SignalError {
    #[error("price microunits must be positive")]
    InvalidPrice,
    #[error("market book is crossed")]
    CrossedBook,
    #[error("timestamps must satisfy observed <= published <= fetched <= first_usable")]
    InvalidTimestampOrder,
    #[error("revision identity components must be non-empty and trimmed")]
    InvalidIdentity,
    #[error("revision identity conflicts with a different payload: {0}")]
    ConflictingRevision(String),
    #[error("freshness limit must be positive")]
    InvalidFreshnessLimit,
    #[error("unsupported signal schema version: {0}")]
    UnsupportedSchema(u8),
    #[error("invalid snapshot invariant")]
    InvalidSnapshotInvariant,
    #[error("snapshot hash does not match canonical typed contents")]
    InvalidSnapshotHash,
    #[error("UTC date already has a different immutable snapshot: {0}")]
    ConflictingDailySnapshot(NaiveDate),
    #[error("signal JSON is invalid: {0}")]
    Json(String),
    #[error("snapshot serialization failed: {0}")]
    SnapshotSerialization(String),
}

fn select_core(
    book: &RevisionBook<CoreMarketData>,
    requirement: &FreshnessRequirement,
    decision_at: DateTime<Utc>,
) -> (Option<SignalRevision<CoreMarketData>>, CoreHealth) {
    if let Some(revision) = book.select_exact(&requirement.query, decision_at) {
        let age_seconds = age_seconds(decision_at, revision.timestamps.observed_at);
        return match age_seconds {
            Some(age_seconds) if age_seconds < requirement.stale_after_seconds => {
                (Some(revision.clone()), CoreHealth::Healthy { age_seconds })
            }
            Some(age_seconds) => (Some(revision.clone()), CoreHealth::Stale { age_seconds }),
            None => (
                None,
                CoreHealth::Future {
                    first_usable_at: revision.timestamps.first_usable_at,
                },
            ),
        };
    }
    match book.first_future(&requirement.query, decision_at) {
        Some(first_usable_at) => (None, CoreHealth::Future { first_usable_at }),
        None => (None, CoreHealth::Missing),
    }
}

fn select_auxiliary(
    book: &RevisionBook<AuxiliarySignal>,
    requirement: &FreshnessRequirement,
    decision_at: DateTime<Utc>,
) -> (Option<SignalRevision<AuxiliarySignal>>, AuxiliaryHealth) {
    if let Some(revision) = book.select_exact(&requirement.query, decision_at) {
        let age_seconds = age_seconds(decision_at, revision.timestamps.observed_at);
        return match age_seconds {
            Some(age_seconds) if age_seconds < requirement.stale_after_seconds => (
                Some(revision.clone()),
                AuxiliaryHealth::Healthy { age_seconds },
            ),
            Some(age_seconds) => (
                Some(revision.clone()),
                AuxiliaryHealth::Stale { age_seconds },
            ),
            None => (
                None,
                AuxiliaryHealth::Future {
                    first_usable_at: revision.timestamps.first_usable_at,
                },
            ),
        };
    }
    match book.first_future(&requirement.query, decision_at) {
        Some(first_usable_at) => (None, AuxiliaryHealth::Future { first_usable_at }),
        None => (None, AuxiliaryHealth::Missing),
    }
}

fn age_seconds(decision_at: DateTime<Utc>, observed_at: DateTime<Utc>) -> Option<u64> {
    u64::try_from(decision_at.signed_duration_since(observed_at).num_seconds()).ok()
}

fn core_health_mismatch(
    selected: Option<&SignalRevision<CoreMarketData>>,
    health: &CoreHealth,
    query: &RevisionQuery,
    decision_at: DateTime<Utc>,
    stale_after_seconds: u64,
) -> bool {
    match selected {
        Some(revision) => {
            if !query.matches(revision.identity())
                || revision.timestamps().first_usable_at() > decision_at
            {
                return true;
            }
            let Some(age_seconds) = age_seconds(decision_at, revision.timestamps().observed_at())
            else {
                return true;
            };
            let expected = if age_seconds < stale_after_seconds {
                CoreHealth::Healthy { age_seconds }
            } else {
                CoreHealth::Stale { age_seconds }
            };
            health != &expected
        }
        None => match health {
            CoreHealth::Missing => false,
            CoreHealth::Future { first_usable_at } => *first_usable_at <= decision_at,
            CoreHealth::Healthy { .. } | CoreHealth::Stale { .. } => true,
        },
    }
}

fn auxiliary_health_mismatch(
    selected: Option<&SignalRevision<AuxiliarySignal>>,
    health: &AuxiliaryHealth,
    query: &RevisionQuery,
    decision_at: DateTime<Utc>,
    stale_after_seconds: u64,
) -> bool {
    match selected {
        Some(revision) => {
            if !query.matches(revision.identity())
                || revision.timestamps().first_usable_at() > decision_at
            {
                return true;
            }
            let Some(age_seconds) = age_seconds(decision_at, revision.timestamps().observed_at())
            else {
                return true;
            };
            let expected = if age_seconds < stale_after_seconds {
                AuxiliaryHealth::Healthy { age_seconds }
            } else {
                AuxiliaryHealth::Stale { age_seconds }
            };
            health != &expected
        }
        None => match health {
            AuxiliaryHealth::Missing => false,
            AuxiliaryHealth::Future { first_usable_at } => *first_usable_at <= decision_at,
            AuxiliaryHealth::Healthy { .. } | AuxiliaryHealth::Stale { .. } => true,
        },
    }
}

fn invalid_component(value: &str) -> bool {
    value.is_empty() || value.trim() != value
}

fn canonical_hash(value: &SignalSnapshotBody) -> Result<String, SignalError> {
    let bytes = serde_json::to_vec(value)
        .map_err(|error| SignalError::SnapshotSerialization(error.to_string()))?;
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(64);
    for byte in digest {
        write!(&mut output, "{byte:02x}")
            .map_err(|error| SignalError::SnapshotSerialization(error.to_string()))?;
    }
    Ok(output)
}
