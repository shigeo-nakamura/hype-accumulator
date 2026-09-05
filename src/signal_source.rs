//! Signer-free Hyperliquid core-signal snapshot producer.
//!
//! The pacing planner only becomes purchase-eligible when the daily
//! [`SignalSnapshot`] bound to the configured UTC decision boundary carries a
//! fresh core market observation. This module produces that snapshot from the
//! public HYPE/USDC `l2Book` on Hyperliquid itself: it needs no account
//! identity, secret, or signer, and it runs the typed observation through the
//! exact [`LiveSignalNormalizer`] path the replay contract already uses.
//!
//! The auxiliary (BTC spot ETF flow) input has no production adapter yet, so
//! every produced snapshot records it as `missing`, which the fixed-DCA-safe
//! slice already treats as neutral and never blocks pacing.
//!
//! Fail-closed rules:
//!
//! - The snapshot binds to the *next* configured boundary. If that boundary is
//!   not inside the configured core freshness window, nothing is written, so a
//!   late or persistent-timer catch-up run cannot publish a stale snapshot.
//! - A crossed or empty book, a non-exact price, or a venue clock ahead of the
//!   local fetch time is rejected rather than normalized.
//! - An existing snapshot already bound to the same boundary is left
//!   untouched; the first snapshot per UTC day is immutable.

use crate::{
    config::UtcSchedule,
    hype_asset::HYPE_SPOT_MARKET,
    signal::{
        CoreHealth, CoreMarketData, FreshnessRequirement, LiveSignalNormalizer, PriceMicrounits,
        RevisionIdentity, RevisionQuery, RevisionTimestamps, SignalError, SignalRevision,
        SignalSnapshot, SnapshotRequest, SIGNAL_SCHEMA_VERSION,
    },
    status_io::{write_signal_snapshot_atomic, StatusIoError},
};
use chrono::{DateTime, Days, TimeZone, Utc};
use dex_connector::{HyperliquidConnector, HyperliquidConnectorConfig, OrderBookSnapshot};
use fs2::FileExt as _;
use rust_decimal::{prelude::ToPrimitive, Decimal};
use serde_json::json;
use std::{
    fs::{self, OpenOptions},
    io,
    path::{Path, PathBuf},
};
use thiserror::Error;

/// Revision identity of the core market observation.
pub const CORE_SIGNAL_SOURCE: &str = "hyperliquid";
pub const CORE_SIGNAL_SOURCE_VERSION: &str = "l2Book-v1";
pub const CORE_SIGNAL_SERIES: &str = HYPE_SPOT_MARKET;
/// Revision identity reserved for the not-yet-configured auxiliary input.
pub const AUXILIARY_SIGNAL_SOURCE: &str = "unconfigured";
pub const AUXILIARY_SIGNAL_SOURCE_VERSION: &str = "v0";
pub const AUXILIARY_SIGNAL_SERIES: &str = "btc_etf_net_flow";
/// Placeholder freshness for the always-empty auxiliary query. No revision
/// is ever inserted for [`AUXILIARY_SIGNAL_SOURCE`], so `select_auxiliary`
/// reports `Missing` (neutral) regardless of this value; it exists only so
/// `FreshnessRequirement::new` has a positive limit to validate. Deliberately
/// independent of the core freshness window: a future production auxiliary
/// adapter must choose its own value rather than inherit this one.
const AUXILIARY_PLACEHOLDER_STALE_AFTER_SECONDS: u64 = 900;

const PRICE_MICROUNITS_PER_UNIT: u64 = 1_000_000;
const TOP_OF_BOOK_DEPTH: usize = 1;

#[derive(Debug, Error)]
pub enum SignalSourceError {
    #[error("invalid UTC decision boundary")]
    InvalidBoundary,
    #[error("core freshness limit must be positive")]
    InvalidFreshnessLimit,
    #[error(
        "decision boundary {boundary} is {lead_seconds}s away, outside the {stale_after_seconds}s core freshness window"
    )]
    BoundaryOutsideFreshnessWindow {
        boundary: DateTime<Utc>,
        lead_seconds: u64,
        stale_after_seconds: u64,
    },
    #[error("Hyperliquid book read failed: {0}")]
    Connector(String),
    #[error("HYPE/USDC book has no {0} level")]
    EmptyBook(&'static str),
    #[error("book price is not an exact positive microunit value: {0}")]
    InexactPrice(Decimal),
    #[error("bid {bid} plus ask {ask} overflows the microunit midpoint computation")]
    MidpointOverflow { bid: Decimal, ask: Decimal },
    #[error("invalid venue timestamp: {0}")]
    InvalidVenueTime(u64),
    #[error("venue clock {venue} is ahead of the local fetch time {fetched}")]
    VenueClockAhead {
        venue: DateTime<Utc>,
        fetched: DateTime<Utc>,
    },
    #[error("book fetched at {fetched} is after the decision boundary {boundary}")]
    FetchedAfterBoundary {
        fetched: DateTime<Utc>,
        boundary: DateTime<Utc>,
    },
    #[error("produced snapshot is not purchase-eligible: {0:?}")]
    NotEligible(CoreHealth),
    #[error(transparent)]
    Signal(#[from] SignalError),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    StatusIo(#[from] StatusIoError),
}

/// The boundary a produced snapshot binds to and the freshness it must meet.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotPlan {
    pub decision_at: DateTime<Utc>,
    pub stale_after_seconds: u64,
}

/// Returns the first configured UTC boundary at or after `now`.
///
/// # Errors
///
/// Returns [`SignalSourceError::InvalidBoundary`] when the schedule cannot be
/// mapped onto a calendar instant.
pub fn next_decision_boundary(
    now: DateTime<Utc>,
    schedule: &UtcSchedule,
) -> Result<DateTime<Utc>, SignalSourceError> {
    let today = boundary_on(now.date_naive(), schedule)?;
    if now <= today {
        return Ok(today);
    }
    let tomorrow = now
        .date_naive()
        .checked_add_days(Days::new(1))
        .ok_or(SignalSourceError::InvalidBoundary)?;
    boundary_on(tomorrow, schedule)
}

/// Binds the next boundary and refuses any run whose observation could not be
/// healthy at that boundary.
///
/// # Errors
///
/// Returns [`SignalSourceError::InvalidFreshnessLimit`] for a zero limit,
/// [`SignalSourceError::BoundaryOutsideFreshnessWindow`] when the boundary is
/// at least `stale_after_seconds` away, or a boundary computation error.
pub fn plan_snapshot(
    now: DateTime<Utc>,
    schedule: &UtcSchedule,
    stale_after_seconds: u64,
) -> Result<SnapshotPlan, SignalSourceError> {
    if stale_after_seconds == 0 {
        return Err(SignalSourceError::InvalidFreshnessLimit);
    }
    let decision_at = next_decision_boundary(now, schedule)?;
    let lead_seconds = u64::try_from(decision_at.signed_duration_since(now).num_seconds())
        .map_err(|_| SignalSourceError::InvalidBoundary)?;
    if lead_seconds >= stale_after_seconds {
        return Err(SignalSourceError::BoundaryOutsideFreshnessWindow {
            boundary: decision_at,
            lead_seconds,
            stale_after_seconds,
        });
    }
    Ok(SnapshotPlan {
        decision_at,
        stale_after_seconds,
    })
}

/// Best bid/ask captured with Hyperliquid's own book generation time.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopOfBookObservation {
    pub bid_price: Decimal,
    pub ask_price: Decimal,
    pub venue_time_ms: u64,
    pub fetched_at: DateTime<Utc>,
}

/// Extracts the top of book from a depth-limited snapshot.
///
/// # Errors
///
/// Returns [`SignalSourceError::EmptyBook`] when either side is empty.
pub fn top_of_book(
    book: &OrderBookSnapshot,
    venue_time_ms: u64,
    fetched_at: DateTime<Utc>,
) -> Result<TopOfBookObservation, SignalSourceError> {
    let bid = book
        .bids
        .first()
        .ok_or(SignalSourceError::EmptyBook("bid"))?;
    let ask = book
        .asks
        .first()
        .ok_or(SignalSourceError::EmptyBook("ask"))?;
    Ok(TopOfBookObservation {
        bid_price: bid.price,
        ask_price: ask.price,
        venue_time_ms,
        fetched_at,
    })
}

/// Read-only public-book client. No account, signer, or nonce state exists.
pub struct HyperliquidCoreSignalSource {
    connector: HyperliquidConnector,
}

impl HyperliquidCoreSignalSource {
    /// Creates a signer-free public-info client for the configured endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`SignalSourceError::Connector`] when the HTTP client cannot be
    /// constructed.
    pub fn new(base_url: &str) -> Result<Self, SignalSourceError> {
        let connector = HyperliquidConnector::new(HyperliquidConnectorConfig {
            base_url: base_url.trim_end_matches('/').to_owned(),
            tracked_symbols: vec![HYPE_SPOT_MARKET.to_owned()],
        })
        .map_err(|error| SignalSourceError::Connector(error.to_string()))?;
        Ok(Self { connector })
    }

    /// Reads the HYPE/USDC top of book together with the venue's own clock.
    ///
    /// # Errors
    ///
    /// Returns [`SignalSourceError::Connector`] on request/parse failure or
    /// [`SignalSourceError::EmptyBook`] when a side has no level.
    pub async fn observe_top_of_book(&self) -> Result<TopOfBookObservation, SignalSourceError> {
        let venue_book = self
            .connector
            .get_order_book_with_venue_time(HYPE_SPOT_MARKET, TOP_OF_BOOK_DEPTH)
            .await
            .map_err(|error| SignalSourceError::Connector(error.to_string()))?;
        let fetched_at = Utc::now();
        top_of_book(&venue_book.book, venue_book.venue_time_ms, fetched_at)
    }
}

/// Materializes one immutable snapshot bound to the planned boundary.
///
/// `execution_price` is the best ask (the side a purchase would cross),
/// `reference_price` is the floored bid/ask midpoint, and the top of book is
/// retained verbatim. The observation, publication, fetch, and first-usable
/// timestamps come from the venue clock and the local fetch time only.
///
/// # Errors
///
/// Returns [`SignalSourceError`] for a crossed/inexact book, an inconsistent
/// clock, or a snapshot that would not be purchase-eligible at the boundary.
pub fn build_snapshot(
    plan: &SnapshotPlan,
    observation: &TopOfBookObservation,
) -> Result<SignalSnapshot, SignalSourceError> {
    let observed_at = i64::try_from(observation.venue_time_ms)
        .ok()
        .and_then(DateTime::<Utc>::from_timestamp_millis)
        .ok_or(SignalSourceError::InvalidVenueTime(
            observation.venue_time_ms,
        ))?;
    if observed_at > observation.fetched_at {
        return Err(SignalSourceError::VenueClockAhead {
            venue: observed_at,
            fetched: observation.fetched_at,
        });
    }
    if observation.fetched_at > plan.decision_at {
        return Err(SignalSourceError::FetchedAfterBoundary {
            fetched: observation.fetched_at,
            boundary: plan.decision_at,
        });
    }
    let bid = exact_price_microunits(observation.bid_price)?;
    let ask = exact_price_microunits(observation.ask_price)?;
    let mid = bid
        .get()
        .checked_add(ask.get())
        .map(|total| total / 2)
        .ok_or(SignalSourceError::MidpointOverflow {
            bid: observation.bid_price,
            ask: observation.ask_price,
        })?;
    let core = CoreMarketData::new(ask, PriceMicrounits::new(mid)?, bid, ask)?;
    let decision_date = plan.decision_at.date_naive();
    let identity = RevisionIdentity::new(
        CORE_SIGNAL_SOURCE,
        CORE_SIGNAL_SOURCE_VERSION,
        CORE_SIGNAL_SERIES,
        decision_date,
        format!("l2book-{}", observation.venue_time_ms),
    )?;
    let timestamps = RevisionTimestamps::new(
        observed_at,
        observed_at,
        observation.fetched_at,
        observation.fetched_at,
    )?;
    let raw = serde_json::to_string(&json!({
        "schema_version": SIGNAL_SCHEMA_VERSION,
        "core_revisions": [SignalRevision::new(identity, timestamps, core)],
        "auxiliary_revisions": [],
    }))
    .map_err(|error| SignalError::SnapshotSerialization(error.to_string()))?;
    let core_query = RevisionQuery::new(
        CORE_SIGNAL_SOURCE,
        CORE_SIGNAL_SOURCE_VERSION,
        CORE_SIGNAL_SERIES,
        decision_date,
    )?;
    let auxiliary_query = RevisionQuery::new(
        AUXILIARY_SIGNAL_SOURCE,
        AUXILIARY_SIGNAL_SOURCE_VERSION,
        AUXILIARY_SIGNAL_SERIES,
        decision_date,
    )?;
    let snapshot = LiveSignalNormalizer::normalize_json(&raw)?.snapshot(&SnapshotRequest::new(
        plan.decision_at,
        FreshnessRequirement::new(core_query, plan.stale_after_seconds)?,
        FreshnessRequirement::new(auxiliary_query, AUXILIARY_PLACEHOLDER_STALE_AFTER_SECONDS)?,
    ))?;
    if !snapshot.purchase_eligible() {
        return Err(SignalSourceError::NotEligible(
            snapshot.core_health().clone(),
        ));
    }
    Ok(snapshot)
}

/// Result of publishing a snapshot to the runtime input path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PublishOutcome {
    /// The snapshot was written atomically.
    Written,
    /// A valid snapshot for the same boundary already existed and was kept.
    /// Carries that snapshot's own (frozen, persisted-at-creation) core
    /// health and hash — not the just-built candidate's — so a caller
    /// reporting this outcome describes what is actually on disk.
    Existing {
        snapshot_hash: String,
        core_health: CoreHealth,
    },
}

/// Writes the snapshot unless a valid one for the same boundary already
/// exists. A snapshot for another boundary or an unparsable file is replaced.
///
/// # Errors
///
/// Returns [`SignalSourceError`] for read or atomic write failures.
pub fn publish_snapshot(
    path: &Path,
    snapshot: &SignalSnapshot,
) -> Result<PublishOutcome, SignalSourceError> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty());
    if let Some(parent) = parent {
        fs::create_dir_all(parent)?;
    }
    let lock_file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(signal_snapshot_lock_path(path))?;
    // Held for the whole read-decide-write section below (released on drop
    // at function return) so a concurrently triggered producer run — a
    // manual invocation racing the scheduled timer, or two overlapping
    // timer firings — cannot both observe "no existing snapshot for this
    // boundary" and both proceed to write, which would silently replace the
    // first published observation with a later one and break the "first
    // snapshot per UTC day is immutable" guarantee.
    // `lock_exclusive` blocks the calling thread, which would stall a tokio
    // worker if this ran inside a long-lived multi-task process; acceptable
    // only because the producer is a one-shot CLI invocation with nothing
    // else to schedule on this runtime.
    lock_file.lock_exclusive()?;
    match fs::read_to_string(path) {
        Ok(payload) => {
            if let Ok(existing) = SignalSnapshot::from_json(&payload) {
                if existing.decision_at() == snapshot.decision_at() {
                    return Ok(PublishOutcome::Existing {
                        snapshot_hash: existing.snapshot_hash().to_owned(),
                        core_health: existing.core_health().clone(),
                    });
                }
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    write_signal_snapshot_atomic(path, snapshot)?;
    Ok(PublishOutcome::Written)
}

/// Extracts the `(label, age_seconds)` pair for a snapshot's core health.
///
/// Every snapshot this producer ever persists is purchase-eligible:
/// [`build_snapshot`] itself already returns [`SignalSourceError::NotEligible`]
/// before returning one that is not. This helper exists only so a caller
/// reporting either a freshly built or a previously persisted snapshot (see
/// [`PublishOutcome::Existing`]) can format the same way; its `Missing`/
/// `Future`/`Stale` arm is defensive and intentionally redundant with that
/// earlier check, not an independent safety gate.
///
/// # Errors
///
/// Returns [`SignalSourceError::NotEligible`] if `health` is not `Healthy`.
pub fn core_health_label(health: &CoreHealth) -> Result<(&'static str, u64), SignalSourceError> {
    match health {
        CoreHealth::Healthy { age_seconds } => Ok(("healthy", *age_seconds)),
        CoreHealth::Missing | CoreHealth::Future { .. } | CoreHealth::Stale { .. } => {
            Err(SignalSourceError::NotEligible(health.clone()))
        }
    }
}

fn signal_snapshot_lock_path(path: &Path) -> PathBuf {
    let mut name = path.file_name().map_or_else(
        || std::ffi::OsString::from("signal-snapshot"),
        std::ffi::OsStr::to_os_string,
    );
    name.push(".lock");
    path.with_file_name(name)
}

fn boundary_on(
    date: chrono::NaiveDate,
    schedule: &UtcSchedule,
) -> Result<DateTime<Utc>, SignalSourceError> {
    use chrono::Datelike;
    Utc.with_ymd_and_hms(
        date.year(),
        date.month(),
        date.day(),
        u32::from(schedule.utc_hour),
        u32::from(schedule.utc_minute),
        0,
    )
    .single()
    .ok_or(SignalSourceError::InvalidBoundary)
}

fn exact_price_microunits(price: Decimal) -> Result<PriceMicrounits, SignalSourceError> {
    let scaled = price
        .checked_mul(Decimal::from(PRICE_MICROUNITS_PER_UNIT))
        .ok_or(SignalSourceError::InexactPrice(price))?;
    if !scaled.fract().is_zero() {
        return Err(SignalSourceError::InexactPrice(price));
    }
    let value = scaled
        .to_u64()
        .ok_or(SignalSourceError::InexactPrice(price))?;
    PriceMicrounits::new(value).map_err(|_| SignalSourceError::InexactPrice(price))
}

#[cfg(test)]
mod tests {
    use super::*;
    use dex_connector::OrderBookLevel;
    use std::str::FromStr;

    fn schedule() -> UtcSchedule {
        UtcSchedule {
            utc_hour: 12,
            utc_minute: 0,
            weekdays: (1..=7).collect(),
        }
    }

    fn at(value: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(value)
            .expect("rfc3339 fixture")
            .with_timezone(&Utc)
    }

    fn ms(value: DateTime<Utc>) -> u64 {
        u64::try_from(value.timestamp_millis()).expect("positive fixture")
    }

    fn price(value: &str) -> Decimal {
        Decimal::from_str(value).expect("decimal fixture")
    }

    fn observation(bid: &str, ask: &str, venue: &str, fetched: &str) -> TopOfBookObservation {
        TopOfBookObservation {
            bid_price: price(bid),
            ask_price: price(ask),
            venue_time_ms: ms(at(venue)),
            fetched_at: at(fetched),
        }
    }

    fn plan() -> SnapshotPlan {
        plan_snapshot(at("2026-09-05T11:58:30Z"), &schedule(), 900).expect("plan")
    }

    #[test]
    fn next_boundary_is_today_until_the_boundary_passes() {
        assert_eq!(
            next_decision_boundary(at("2026-09-05T03:00:00Z"), &schedule()).expect("today"),
            at("2026-09-05T12:00:00Z")
        );
        assert_eq!(
            next_decision_boundary(at("2026-09-05T12:00:00Z"), &schedule()).expect("exact"),
            at("2026-09-05T12:00:00Z")
        );
        assert_eq!(
            next_decision_boundary(at("2026-09-05T12:00:01Z"), &schedule()).expect("tomorrow"),
            at("2026-09-06T12:00:00Z")
        );
        assert_eq!(
            next_decision_boundary(at("2026-09-30T13:00:00Z"), &schedule()).expect("rollover"),
            at("2026-10-01T12:00:00Z")
        );
    }

    #[test]
    fn plan_refuses_runs_outside_the_freshness_window() {
        let plan = plan();
        assert_eq!(plan.decision_at, at("2026-09-05T12:00:00Z"));
        assert_eq!(plan.stale_after_seconds, 900);
        assert!(matches!(
            plan_snapshot(at("2026-09-05T11:45:00Z"), &schedule(), 900),
            Err(SignalSourceError::BoundaryOutsideFreshnessWindow {
                lead_seconds: 900,
                stale_after_seconds: 900,
                ..
            })
        ));
        assert!(matches!(
            plan_snapshot(at("2026-09-05T12:00:01Z"), &schedule(), 900),
            Err(SignalSourceError::BoundaryOutsideFreshnessWindow { .. })
        ));
        assert!(matches!(
            plan_snapshot(at("2026-09-05T11:59:00Z"), &schedule(), 0),
            Err(SignalSourceError::InvalidFreshnessLimit)
        ));
    }

    #[test]
    fn top_of_book_requires_both_sides() {
        let fetched = at("2026-09-05T11:58:31Z");
        let mut book = OrderBookSnapshot::default();
        assert!(matches!(
            top_of_book(&book, 1, fetched),
            Err(SignalSourceError::EmptyBook("bid"))
        ));
        book.bids.push(OrderBookLevel {
            price: price("85.008"),
            size: price("1"),
        });
        assert!(matches!(
            top_of_book(&book, 1, fetched),
            Err(SignalSourceError::EmptyBook("ask"))
        ));
        book.asks.push(OrderBookLevel {
            price: price("85.009"),
            size: price("1"),
        });
        let observation = top_of_book(&book, 1, fetched).expect("top of book");
        assert_eq!(observation.bid_price, price("85.008"));
        assert_eq!(observation.ask_price, price("85.009"));
        assert_eq!(observation.venue_time_ms, 1);
        assert_eq!(observation.fetched_at, fetched);
    }

    #[test]
    fn snapshot_binds_the_boundary_and_is_purchase_eligible() {
        let snapshot = build_snapshot(
            &plan(),
            &observation(
                "85.008",
                "85.009",
                "2026-09-05T11:58:30.250Z",
                "2026-09-05T11:58:30.750Z",
            ),
        )
        .expect("snapshot");
        assert_eq!(snapshot.decision_at(), at("2026-09-05T12:00:00Z"));
        assert!(snapshot.purchase_eligible());
        assert_eq!(
            snapshot.core_health(),
            &CoreHealth::Healthy { age_seconds: 89 }
        );
        let core = snapshot.core().expect("core revision");
        assert_eq!(core.value().execution_price().get(), 85_009_000);
        assert_eq!(core.value().reference_price().get(), 85_008_500);
        assert_eq!(core.value().bid_price().get(), 85_008_000);
        assert_eq!(core.value().ask_price().get(), 85_009_000);
        assert_eq!(core.identity().source(), CORE_SIGNAL_SOURCE);
        assert_eq!(core.identity().series(), "HYPE/USDC");
        assert_eq!(
            core.identity().observation_date(),
            at("2026-09-05T12:00:00Z").date_naive()
        );
        assert_eq!(
            core.timestamps().observed_at(),
            at("2026-09-05T11:58:30.250Z")
        );
        assert_eq!(
            core.timestamps().first_usable_at(),
            at("2026-09-05T11:58:30.750Z")
        );
        assert!(snapshot.auxiliary().is_none());
        let canonical = snapshot.to_canonical_json().expect("canonical json");
        assert_eq!(
            SignalSnapshot::from_json(&canonical).expect("round trip"),
            snapshot
        );
    }

    #[test]
    fn snapshot_rejects_bad_books_and_clocks() {
        let plan = plan();
        assert!(matches!(
            build_snapshot(
                &plan,
                &observation(
                    "85.010",
                    "85.009",
                    "2026-09-05T11:58:30Z",
                    "2026-09-05T11:58:31Z"
                )
            ),
            Err(SignalSourceError::Signal(SignalError::CrossedBook))
        ));
        assert!(matches!(
            build_snapshot(
                &plan,
                &observation(
                    "85.0000001",
                    "85.009",
                    "2026-09-05T11:58:30Z",
                    "2026-09-05T11:58:31Z"
                )
            ),
            Err(SignalSourceError::InexactPrice(_))
        ));
        assert!(matches!(
            build_snapshot(
                &plan,
                &observation(
                    "0",
                    "85.009",
                    "2026-09-05T11:58:30Z",
                    "2026-09-05T11:58:31Z"
                )
            ),
            Err(SignalSourceError::InexactPrice(_))
        ));
        assert!(matches!(
            build_snapshot(
                &plan,
                &observation(
                    "85.008",
                    "85.009",
                    "2026-09-05T11:58:32Z",
                    "2026-09-05T11:58:31Z"
                )
            ),
            Err(SignalSourceError::VenueClockAhead { .. })
        ));
        assert!(matches!(
            build_snapshot(
                &plan,
                &observation(
                    "85.008",
                    "85.009",
                    "2026-09-05T12:00:00Z",
                    "2026-09-05T12:00:01Z"
                )
            ),
            Err(SignalSourceError::FetchedAfterBoundary { .. })
        ));
    }

    #[test]
    fn core_health_label_accepts_only_healthy() {
        assert_eq!(
            core_health_label(&CoreHealth::Healthy { age_seconds: 42 }).expect("healthy"),
            ("healthy", 42)
        );
        assert!(matches!(
            core_health_label(&CoreHealth::Missing),
            Err(SignalSourceError::NotEligible(CoreHealth::Missing))
        ));
    }

    #[test]
    fn publish_keeps_the_first_snapshot_per_boundary() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let path = directory.path().join("inputs/signal-snapshot.json");
        let first = build_snapshot(
            &plan(),
            &observation(
                "85.008",
                "85.009",
                "2026-09-05T11:58:30Z",
                "2026-09-05T11:58:31Z",
            ),
        )
        .expect("first snapshot");
        assert_eq!(
            publish_snapshot(&path, &first).expect("written"),
            PublishOutcome::Written
        );
        let second = build_snapshot(
            &plan(),
            &observation(
                "85.100",
                "85.101",
                "2026-09-05T11:59:00Z",
                "2026-09-05T11:59:01Z",
            ),
        )
        .expect("second snapshot");
        assert_eq!(
            publish_snapshot(&path, &second).expect("kept"),
            PublishOutcome::Existing {
                snapshot_hash: first.snapshot_hash().to_owned(),
                core_health: first.core_health().clone(),
            }
        );
        // The kept outcome must report the persisted (first) snapshot's own
        // frozen health, not the just-built (second) candidate's — even
        // though both are Healthy here, they carry different `age_seconds`.
        assert_ne!(first.core_health(), second.core_health());
        assert_eq!(
            SignalSnapshot::from_json(&fs::read_to_string(&path).expect("payload"))
                .expect("valid file"),
            first
        );

        let next_day = build_snapshot(
            &plan_snapshot(at("2026-09-06T11:58:30Z"), &schedule(), 900).expect("plan"),
            &observation(
                "85.200",
                "85.201",
                "2026-09-06T11:58:30Z",
                "2026-09-06T11:58:31Z",
            ),
        )
        .expect("next-day snapshot");
        assert_eq!(
            publish_snapshot(&path, &next_day).expect("replaced"),
            PublishOutcome::Written
        );
        assert_eq!(
            SignalSnapshot::from_json(&fs::read_to_string(&path).expect("payload"))
                .expect("valid file"),
            next_day
        );

        fs::write(&path, "not json").expect("corrupt input");
        assert_eq!(
            publish_snapshot(&path, &next_day).expect("replaced corrupt input"),
            PublishOutcome::Written
        );
    }
}
