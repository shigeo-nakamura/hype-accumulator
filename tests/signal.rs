use chrono::{DateTime, NaiveDate, Utc};
use hype_accumulator::signal::{
    AuxiliaryHealth, BacktestSignalNormalizer, CoreHealth, CoreMarketData, DailySnapshotStore,
    FreshnessRequirement, InsertOutcome, LiveSignalNormalizer, PriceMicrounits, RevisionBook,
    RevisionIdentity, RevisionQuery, RevisionTimestamps, SignalError, SignalRevision,
    SignalSnapshot, SnapshotRequest, NEUTRAL_MULTIPLIER_BPS,
};

const RAW: &str = include_str!("../fixtures/signal-snapshots-v1/raw.json");

fn at(value: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(value)
        .unwrap()
        .with_timezone(&Utc)
}

fn day(value: &str) -> NaiveDate {
    NaiveDate::parse_from_str(value, "%Y-%m-%d").unwrap()
}

fn request(
    decision_at: &str,
    core_date: &str,
    auxiliary_date: &str,
    core_stale_after_seconds: u64,
    auxiliary_stale_after_seconds: u64,
) -> SnapshotRequest {
    let core = RevisionQuery::new("fixture-core", "v1", "hype_market", day(core_date)).unwrap();
    let auxiliary =
        RevisionQuery::new("fixture-aux", "v1", "btc_etf_net_flow", day(auxiliary_date)).unwrap();
    SnapshotRequest::new(
        at(decision_at),
        FreshnessRequirement::new(core, core_stale_after_seconds).unwrap(),
        FreshnessRequirement::new(auxiliary, auxiliary_stale_after_seconds).unwrap(),
    )
}

fn live_snapshot(request: &SnapshotRequest) -> SignalSnapshot {
    LiveSignalNormalizer::normalize_json(RAW)
        .unwrap()
        .snapshot(request)
        .unwrap()
}

#[test]
fn raw_contract_rejects_bad_time_order_zero_prices_and_crossed_books() {
    assert_eq!(
        RevisionTimestamps::new(
            at("2026-07-01T12:00:01Z"),
            at("2026-07-01T12:00:00Z"),
            at("2026-07-01T12:00:02Z"),
            at("2026-07-01T12:00:03Z"),
        ),
        Err(SignalError::InvalidTimestampOrder)
    );
    assert_eq!(PriceMicrounits::new(0), Err(SignalError::InvalidPrice));
    assert_eq!(
        CoreMarketData::new(
            PriceMicrounits::new(10).unwrap(),
            PriceMicrounits::new(10).unwrap(),
            PriceMicrounits::new(11).unwrap(),
            PriceMicrounits::new(10).unwrap(),
        ),
        Err(SignalError::CrossedBook)
    );

    let mut unordered: serde_json::Value = serde_json::from_str(RAW).unwrap();
    unordered["core_revisions"][0]["timestamps"]["published_at"] =
        serde_json::json!("2026-03-08T06:59:58Z");
    assert!(matches!(
        LiveSignalNormalizer::normalize_json(&unordered.to_string()),
        Err(SignalError::Json(_))
    ));

    let mut zero: serde_json::Value = serde_json::from_str(RAW).unwrap();
    zero["core_revisions"][0]["value"]["reference_price"] = serde_json::json!(0);
    assert!(matches!(
        LiveSignalNormalizer::normalize_json(&zero.to_string()),
        Err(SignalError::Json(_))
    ));

    for invalid in [
        serde_json::json!({
            "source": "",
            "source_version": "v1",
            "series": "hype_market",
            "observation_date": "2026-07-01"
        }),
        serde_json::json!({
            "source": "fixture-core",
            "source_version": "v1",
            "series": " hype_market ",
            "observation_date": "2026-07-01"
        }),
    ] {
        let error = serde_json::from_value::<RevisionQuery>(invalid).unwrap_err();
        assert!(error.to_string().contains("non-empty and trimmed"));
    }
}

#[test]
fn live_and_backtest_use_identical_normalization_and_snapshot_bytes() {
    let request = request(
        "2026-07-06T12:00:00Z",
        "2026-07-06",
        "2026-07-02",
        60,
        604_800,
    );
    let live = LiveSignalNormalizer::normalize_json(RAW)
        .unwrap()
        .snapshot(&request)
        .unwrap();
    let backtest = BacktestSignalNormalizer::normalize_json(RAW)
        .unwrap()
        .snapshot(&request)
        .unwrap();
    assert_eq!(live, backtest);
    assert_eq!(
        live.to_canonical_json().unwrap(),
        backtest.to_canonical_json().unwrap()
    );
    assert_eq!(live.snapshot_hash().len(), 64);
    assert_eq!(live.core_query().source(), "fixture-core");
    assert_eq!(live.core_query().observation_date(), day("2026-07-06"));
    assert_eq!(live.auxiliary_query().series(), "btc_etf_net_flow");
    assert_eq!(live.auxiliary_query().observation_date(), day("2026-07-02"));
}

#[test]
fn us_holiday_and_weekend_do_not_forward_fill_auxiliary_data() {
    for (decision_at, date, expected_core_age) in [
        ("2026-07-03T12:00:00Z", "2026-07-03", 30),
        ("2026-07-04T12:00:00Z", "2026-07-04", 20),
    ] {
        let snapshot = live_snapshot(&request(decision_at, date, date, 31, 604_800));
        assert_eq!(
            snapshot.core_health(),
            &CoreHealth::Healthy {
                age_seconds: expected_core_age
            }
        );
        assert_eq!(snapshot.auxiliary_health(), &AuxiliaryHealth::Missing);
        assert!(snapshot.auxiliary().is_none());
        assert!(snapshot.purchase_eligible());
        assert_eq!(snapshot.pacing_multiplier_bps(), NEUTRAL_MULTIPLIER_BPS);
    }
}

#[test]
fn delayed_publication_is_invisible_until_first_usable() {
    let before = live_snapshot(&request(
        "2026-07-06T11:59:59Z",
        "2026-07-06",
        "2026-07-02",
        60,
        604_800,
    ));
    assert_eq!(
        before.auxiliary_health(),
        &AuxiliaryHealth::Future {
            first_usable_at: at("2026-07-06T12:00:00Z")
        }
    );
    assert!(before.auxiliary().is_none());
    assert_eq!(before.pacing_multiplier_bps(), NEUTRAL_MULTIPLIER_BPS);

    let usable = live_snapshot(&request(
        "2026-07-06T12:00:00Z",
        "2026-07-06",
        "2026-07-02",
        60,
        604_800,
    ));
    assert_eq!(
        usable.auxiliary().unwrap().identity().revision_id(),
        "initial"
    );
}

#[test]
fn late_revision_is_visible_only_to_a_later_day() {
    let day_n = live_snapshot(&request(
        "2026-07-06T12:00:00Z",
        "2026-07-06",
        "2026-07-02",
        60,
        604_800,
    ));
    let day_n_plus_one = live_snapshot(&request(
        "2026-07-07T12:00:00Z",
        "2026-07-07",
        "2026-07-02",
        60,
        604_800,
    ));
    assert_eq!(
        day_n.auxiliary().unwrap().identity().revision_id(),
        "initial"
    );
    assert_eq!(
        day_n_plus_one.auxiliary().unwrap().identity().revision_id(),
        "corrected"
    );
    assert_ne!(day_n.snapshot_hash(), day_n_plus_one.snapshot_hash());
}

#[test]
fn exact_observation_date_selection_never_forward_fills() {
    let exact_missing = live_snapshot(&request(
        "2026-07-06T12:00:00Z",
        "2026-07-06",
        "2026-07-03",
        60,
        604_800,
    ));
    assert_eq!(exact_missing.auxiliary_health(), &AuxiliaryHealth::Missing);
    assert!(exact_missing.auxiliary().is_none());
}

#[test]
fn core_outage_blocks_and_recovery_reenables_without_auxiliary_coupling() {
    let outage = live_snapshot(&request(
        "2026-07-08T12:00:00Z",
        "2026-07-08",
        "2026-07-08",
        60,
        604_800,
    ));
    assert_eq!(outage.core_health(), &CoreHealth::Missing);
    assert_eq!(outage.auxiliary_health(), &AuxiliaryHealth::Missing);
    assert!(!outage.purchase_eligible());

    let recovered = live_snapshot(&request(
        "2026-07-09T12:00:00Z",
        "2026-07-09",
        "2026-07-09",
        60,
        604_800,
    ));
    assert_eq!(
        recovered.core_health(),
        &CoreHealth::Healthy { age_seconds: 20 }
    );
    assert_eq!(recovered.auxiliary_health(), &AuxiliaryHealth::Missing);
    assert!(recovered.purchase_eligible());
    assert_eq!(recovered.pacing_multiplier_bps(), NEUTRAL_MULTIPLIER_BPS);
}

#[test]
fn core_age_at_limit_and_future_core_both_block() {
    let at_limit = live_snapshot(&request(
        "2026-07-03T12:00:00Z",
        "2026-07-03",
        "2026-07-03",
        30,
        604_800,
    ));
    assert_eq!(
        at_limit.core_health(),
        &CoreHealth::Stale { age_seconds: 30 }
    );
    assert!(!at_limit.purchase_eligible());

    let future = live_snapshot(&request(
        "2026-07-10T12:00:05Z",
        "2026-07-10",
        "2026-07-10",
        60,
        604_800,
    ));
    assert_eq!(
        future.core_health(),
        &CoreHealth::Future {
            first_usable_at: at("2026-07-10T12:00:10Z")
        }
    );
    assert!(!future.purchase_eligible());
}

#[test]
fn stale_auxiliary_is_explicit_but_core_remains_eligible_and_neutral() {
    let snapshot = live_snapshot(&request(
        "2026-07-07T12:00:00Z",
        "2026-07-07",
        "2026-07-02",
        60,
        403_200,
    ));
    assert_eq!(
        snapshot.core_health(),
        &CoreHealth::Healthy { age_seconds: 20 }
    );
    assert_eq!(
        snapshot.auxiliary_health(),
        &AuxiliaryHealth::Stale {
            age_seconds: 403_200
        }
    );
    assert!(snapshot.purchase_eligible());
    assert_eq!(snapshot.pacing_multiplier_bps(), NEUTRAL_MULTIPLIER_BPS);
}

#[test]
fn daylight_saving_offsets_normalize_to_utc_without_calendar_drift() {
    let spring = live_snapshot(&request(
        "2026-03-08T07:00:30Z",
        "2026-03-08",
        "2026-03-07",
        60,
        86_400,
    ));
    assert_eq!(spring.decision_date(), day("2026-03-08"));
    assert_eq!(
        spring.core().unwrap().timestamps().observed_at(),
        at("2026-03-08T06:59:59Z")
    );
    assert_eq!(
        spring.core_health(),
        &CoreHealth::Healthy { age_seconds: 31 }
    );

    let fall = live_snapshot(&request(
        "2026-11-01T06:15:30Z",
        "2026-11-01",
        "2026-10-31",
        3_000,
        86_400,
    ));
    assert_eq!(fall.decision_date(), day("2026-11-01"));
    assert_eq!(
        fall.core().unwrap().timestamps().observed_at(),
        at("2026-11-01T05:30:00Z")
    );
    assert_eq!(
        fall.core_health(),
        &CoreHealth::Healthy { age_seconds: 2_730 }
    );
}

#[test]
fn duplicate_revision_identity_is_idempotent_or_conflicts_fail_closed() {
    let identity =
        RevisionIdentity::new("fixture-core", "v1", "hype_market", day("2026-07-11"), "r1")
            .unwrap();
    let timestamps = RevisionTimestamps::new(
        at("2026-07-11T11:59:50Z"),
        at("2026-07-11T11:59:51Z"),
        at("2026-07-11T11:59:52Z"),
        at("2026-07-11T11:59:53Z"),
    )
    .unwrap();
    let market = CoreMarketData::new(
        PriceMicrounits::new(40_002_000).unwrap(),
        PriceMicrounits::new(40_000_000).unwrap(),
        PriceMicrounits::new(39_998_000).unwrap(),
        PriceMicrounits::new(40_002_000).unwrap(),
    )
    .unwrap();
    let revision = SignalRevision::new(identity.clone(), timestamps.clone(), market);
    let mut book = RevisionBook::default();
    assert_eq!(book.insert(revision.clone()), Ok(InsertOutcome::Inserted));
    assert_eq!(book.insert(revision), Ok(InsertOutcome::Existing));

    let conflicting_market = CoreMarketData::new(
        PriceMicrounits::new(41_002_000).unwrap(),
        PriceMicrounits::new(41_000_000).unwrap(),
        PriceMicrounits::new(40_998_000).unwrap(),
        PriceMicrounits::new(41_002_000).unwrap(),
    )
    .unwrap();
    assert_eq!(
        book.insert(SignalRevision::new(
            identity,
            timestamps,
            conflicting_market
        )),
        Err(SignalError::ConflictingRevision("r1".to_owned()))
    );
}

#[test]
fn raw_duplicate_payload_replays_but_conflicting_payload_is_rejected() {
    let mut duplicate: serde_json::Value = serde_json::from_str(RAW).unwrap();
    let row = duplicate["core_revisions"][0].clone();
    duplicate["core_revisions"]
        .as_array_mut()
        .unwrap()
        .push(row);
    assert!(LiveSignalNormalizer::normalize_json(&duplicate.to_string()).is_ok());

    let mut conflict = duplicate;
    let final_row = conflict["core_revisions"]
        .as_array_mut()
        .unwrap()
        .last_mut()
        .unwrap();
    final_row["value"]["execution_price"] = serde_json::json!(999_000_000);
    assert_eq!(
        LiveSignalNormalizer::normalize_json(&conflict.to_string()),
        Err(SignalError::ConflictingRevision("spring-dst".to_owned()))
    );
}

#[test]
fn normalization_rejects_ambiguous_authoritative_revision_timestamps() {
    let mut ambiguous: serde_json::Value = serde_json::from_str(RAW).unwrap();
    let mut row = ambiguous["auxiliary_revisions"][0].clone();
    row["identity"]["source_version"] = serde_json::json!("v2");
    row["identity"]["revision_id"] = serde_json::json!("same-slot-different-id");
    row["value"]["raw_value_microunits"] = serde_json::json!(999_000_000);
    ambiguous["auxiliary_revisions"]
        .as_array_mut()
        .unwrap()
        .push(row);

    assert_eq!(
        LiveSignalNormalizer::normalize_json(&ambiguous.to_string()),
        Err(SignalError::AmbiguousRevisionOrder(day("2026-07-02")))
    );
}

#[test]
fn canonical_hash_ignores_json_field_order_and_excludes_hash_field() {
    let snapshot = live_snapshot(&request(
        "2026-07-06T12:00:00Z",
        "2026-07-06",
        "2026-07-02",
        60,
        604_800,
    ));
    let canonical = snapshot.to_canonical_json().unwrap();
    let value: serde_json::Value = serde_json::from_str(&canonical).unwrap();
    let shuffled = serde_json::to_string_pretty(&value).unwrap();
    let reparsed = SignalSnapshot::from_json(&shuffled).unwrap();
    let directly_deserialized: SignalSnapshot = serde_json::from_str(&shuffled).unwrap();
    assert_eq!(snapshot, reparsed);
    assert_eq!(snapshot, directly_deserialized);
    assert_eq!(snapshot.snapshot_hash(), reparsed.snapshot_hash());
    assert!(
        !String::from_utf8(snapshot.canonical_bytes_without_hash().unwrap())
            .unwrap()
            .contains("snapshot_hash")
    );

    let mut inconsistent_health = value.clone();
    inconsistent_health["body"]["core_health"]["age_seconds"] = serde_json::json!(21);
    assert_eq!(
        SignalSnapshot::from_json(&inconsistent_health.to_string()),
        Err(SignalError::InvalidSnapshotInvariant)
    );
    let direct_inconsistent =
        serde_json::from_value::<SignalSnapshot>(inconsistent_health).unwrap_err();
    assert!(direct_inconsistent
        .to_string()
        .contains("invalid snapshot invariant"));

    let mut mismatched_query = value.clone();
    mismatched_query["body"]["core_query"]["observation_date"] = serde_json::json!("2026-07-05");
    assert_eq!(
        SignalSnapshot::from_json(&mismatched_query.to_string()),
        Err(SignalError::InvalidSnapshotInvariant)
    );

    let mut unknown_top_level = value.clone();
    unknown_top_level["unhashed_note"] = serde_json::json!("injected");
    assert!(matches!(
        SignalSnapshot::from_json(&unknown_top_level.to_string()),
        Err(SignalError::Json(_))
    ));

    let mut unknown_nested = value.clone();
    unknown_nested["body"]["core"]["unhashed_note"] = serde_json::json!("injected");
    assert!(matches!(
        SignalSnapshot::from_json(&unknown_nested.to_string()),
        Err(SignalError::Json(_))
    ));

    let mut tampered = value;
    tampered["body"]["core"]["value"]["execution_price"] = serde_json::json!(40_503_000);
    assert_eq!(
        SignalSnapshot::from_json(&tampered.to_string()),
        Err(SignalError::InvalidSnapshotHash)
    );
    let direct_tampered = serde_json::from_value::<SignalSnapshot>(tampered).unwrap_err();
    assert!(direct_tampered
        .to_string()
        .contains("snapshot hash does not match"));
}

#[test]
fn daily_store_is_semantically_idempotent_and_conflict_safe() {
    let first = live_snapshot(&request(
        "2026-07-03T12:00:00Z",
        "2026-07-03",
        "2026-07-03",
        31,
        604_800,
    ));
    let mut store = DailySnapshotStore::default();
    assert_eq!(store.insert(first.clone()), Ok(InsertOutcome::Inserted));
    assert_eq!(store.insert(first.clone()), Ok(InsertOutcome::Existing));

    let shuffled_value: serde_json::Value =
        serde_json::from_str(&first.to_canonical_json().unwrap()).unwrap();
    assert_eq!(
        store.insert_json(&serde_json::to_string_pretty(&shuffled_value).unwrap()),
        Ok(InsertOutcome::Existing)
    );
    assert_eq!(store.len(), 1);
    assert_eq!(store.get(day("2026-07-03")), Some(&first));

    let different_same_day = live_snapshot(&request(
        "2026-07-03T12:00:01Z",
        "2026-07-03",
        "2026-07-03",
        60,
        604_800,
    ));
    assert_eq!(
        store.insert(different_same_day),
        Err(SignalError::ConflictingDailySnapshot(day("2026-07-03")))
    );
}
