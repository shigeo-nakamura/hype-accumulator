use chrono::{DateTime, NaiveDate, TimeDelta, TimeZone, Utc};
use hype_accumulator::{
    config::{CarryOverPolicy, Config, ConfigError},
    pacing::{
        AdmissionStatus, CapitalEvent, DecisionInput, DecisionReason, DecisionResult, DepositEvent,
        DryRunDecisionReport, PacingAlert, PacingError, PacingLimits, PacingState, UsdcMicros,
        WithdrawalEvent,
    },
    signal::{
        AuxiliaryHealth, FreshnessRequirement, LiveSignalNormalizer, RevisionQuery, SignalSnapshot,
        SnapshotRequest, NEUTRAL_MULTIPLIER_BPS,
    },
};
use proptest::prelude::*;
use std::collections::{BTreeSet, HashMap};

const RAW_SIGNALS: &str = include_str!("../fixtures/signal-snapshots-v1/raw.json");

fn at(year: i32, month: u32, day: u32, hour: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(year, month, day, hour, 0, 0)
        .single()
        .expect("valid UTC fixture")
}

fn usd(value: u64) -> UsdcMicros {
    UsdcMicros::checked_from_whole_usdc(value).expect("small test amount")
}

fn limits() -> PacingLimits {
    PacingLimits {
        min_deposit_confirmations: 2,
        max_automatically_admitted_usdc: usd(10_000),
        yearly_admission_cap_usdc: usd(10_000),
        cumulative_admission_cap_usdc: usd(20_000),
        deposit_cooldown_seconds: 1,
        min_order_usdc: usd(1),
        max_daily_notional_usdc: usd(25),
        fixed_reserve_usdc: usd(0),
        fee_spread_reserve_bps: 0,
        final_catch_up_days: 7,
        carry_over_policy: CarryOverPolicy::HoldForApproval,
        utc_hour: 12,
        utc_minute: 0,
        weekdays: (1..=7).collect::<BTreeSet<_>>(),
    }
}

fn deposit(id: impl Into<String>, amount: u64, received_at: DateTime<Utc>) -> CapitalEvent {
    CapitalEvent::Deposit(DepositEvent {
        event_id: id.into(),
        amount_usdc: usd(amount),
        received_at,
        confirmed_at: Some(received_at),
        confirmation_count: 2,
        admission_approved_at: Some(received_at),
    })
}

fn unapproved_deposit(
    id: impl Into<String>,
    amount: u64,
    received_at: DateTime<Utc>,
) -> CapitalEvent {
    CapitalEvent::Deposit(DepositEvent {
        event_id: id.into(),
        amount_usdc: usd(amount),
        received_at,
        confirmed_at: Some(received_at),
        confirmation_count: 2,
        admission_approved_at: None,
    })
}

fn withdrawal(id: impl Into<String>, amount: u64, occurred_at: DateTime<Utc>) -> CapitalEvent {
    CapitalEvent::Withdrawal(WithdrawalEvent {
        event_id: id.into(),
        amount_usdc: usd(amount),
        occurred_at,
        reconciled_at: occurred_at,
    })
}

fn input(at: DateTime<Utc>, observed: u64) -> DecisionInput {
    DecisionInput {
        at,
        observed_spot_usdc: usd(observed),
        capital_history_complete: true,
        manual_pause: false,
    }
}

fn day(value: &str) -> NaiveDate {
    NaiveDate::parse_from_str(value, "%Y-%m-%d").expect("valid fixture date")
}

fn signal_snapshot(
    decision_at: DateTime<Utc>,
    core_date: &str,
    auxiliary_date: &str,
) -> SignalSnapshot {
    let core = RevisionQuery::new("fixture-core", "v1", "hype_market", day(core_date))
        .expect("core query");
    let auxiliary =
        RevisionQuery::new("fixture-aux", "v1", "btc_etf_net_flow", day(auxiliary_date))
            .expect("auxiliary query");
    LiveSignalNormalizer::normalize_json(RAW_SIGNALS)
        .expect("normalized fixture")
        .snapshot(&SnapshotRequest::new(
            decision_at,
            FreshnessRequirement::new(core, 60).expect("core freshness"),
            FreshnessRequirement::new(auxiliary, 604_800).expect("auxiliary freshness"),
        ))
        .expect("signal snapshot")
}

#[test]
fn signal_bound_decision_emits_complete_dry_run_attribution() {
    let limits = limits();
    let decision_at = at(2026, 7, 6, 12);
    let mut state = PacingState::default();
    state
        .reconcile_capital(
            &[deposit("deposit-signal-bound", 100, at(2026, 7, 6, 8))],
            at(2026, 7, 6, 10),
            &limits,
        )
        .expect("capital admitted");
    let snapshot = signal_snapshot(decision_at, "2026-07-06", "2026-07-03");
    assert!(snapshot.purchase_eligible());
    assert_eq!(snapshot.auxiliary_health(), &AuxiliaryHealth::Missing);
    assert_eq!(snapshot.pacing_multiplier_bps(), NEUTRAL_MULTIPLIER_BPS);

    let result = state
        .decide_with_signal(&input(decision_at, 1_000), &limits, &snapshot)
        .expect("signal-bound decision");
    let decision = result.decision();
    assert!(result.is_new());
    assert_eq!(decision.reason, DecisionReason::Planned);
    assert_eq!(decision.input_snapshot_hash, snapshot.snapshot_hash());
    assert!(decision.planned_usdc > UsdcMicros::default());
    assert_eq!(decision.allocations[0].tranche_id, "deposit-signal-bound");

    let report = DryRunDecisionReport::new(&result, &snapshot).expect("dry-run report");
    assert!(!report.signed_action_created());
    assert_eq!(report.signal_snapshot(), &snapshot);
    assert_eq!(report.decision(), decision);
    let json: serde_json::Value = serde_json::from_str(
        &report
            .to_canonical_json()
            .expect("canonical dry-run report"),
    )
    .expect("valid report JSON");
    assert_eq!(json["schema_version"], 1);
    assert_eq!(json["signed_action_created"], false);
    assert_eq!(
        json["signal_snapshot"]["snapshot_hash"],
        snapshot.snapshot_hash()
    );
    assert_eq!(
        json["decision"]["allocations"][0]["tranche_id"],
        "deposit-signal-bound"
    );
    assert!(json["decision"]["explanation"]["daily_cap_usdc"].is_number());
}

#[test]
fn unavailable_core_is_a_durable_skip_and_signal_conflicts_fail_closed() {
    let limits = limits();
    let decision_at = at(2026, 7, 8, 12);
    let mut state = PacingState::default();
    state
        .reconcile_capital(
            &[deposit("deposit-core-outage", 100, at(2026, 7, 8, 8))],
            at(2026, 7, 8, 10),
            &limits,
        )
        .expect("capital admitted");
    let outage = signal_snapshot(decision_at, "2026-07-08", "2026-07-08");
    assert!(!outage.purchase_eligible());

    let input = input(decision_at, 1_000);
    let first = state
        .decide_with_signal(&input, &limits, &outage)
        .expect("durable outage skip");
    assert!(first.is_new());
    assert_eq!(
        first.decision().reason,
        DecisionReason::CoreSignalUnavailable
    );
    assert_eq!(first.decision().planned_usdc, UsdcMicros::default());
    assert_eq!(first.decision().input_snapshot_hash, outage.snapshot_hash());

    let replay = state
        .decide_with_signal(&input, &limits, &outage)
        .expect("exact same-day replay");
    assert!(matches!(replay, DecisionResult::Existing(_)));

    let conflicting = signal_snapshot(decision_at, "2026-07-08", "2026-07-02");
    assert_eq!(
        state.decide_with_signal(&input, &limits, &conflicting),
        Err(PacingError::ConflictingSignalSnapshot)
    );
    assert_eq!(state.decisions().len(), 1);

    let next_day = signal_snapshot(at(2026, 7, 9, 12), "2026-07-09", "2026-07-09");
    assert_eq!(
        state.decide_with_signal(&input, &limits, &next_day),
        Err(PacingError::SignalDecisionMismatch)
    );
    assert_eq!(state.decisions().len(), 1);
}

#[test]
fn arbitrary_time_events_repace_only_future_decisions() {
    let limits = limits();
    let mut state = PacingState::default();
    let first_received = at(2026, 1, 1, 8);
    state
        .reconcile_capital(
            &[deposit("deposit-before", 365, first_received)],
            at(2026, 1, 1, 10),
            &limits,
        )
        .expect("first deposit admitted");
    let first = state
        .decide(&input(at(2026, 1, 1, 12), 1_000), &limits)
        .expect("first decision");
    assert_eq!(first.decision().planned_usdc, usd(1));
    state
        .settle_decision(&first.decision().decision_id, usd(1), usd(1))
        .expect("first fill");

    let after = at(2026, 1, 1, 18);
    state
        .reconcile_capital(
            &[
                deposit("deposit-after", 365, after),
                withdrawal("withdraw-after", 65, at(2026, 1, 1, 19)),
            ],
            at(2026, 1, 1, 20),
            &limits,
        )
        .expect("after-decision events reconcile");

    let same_day = state
        .decide(&input(at(2026, 1, 1, 21), 1_000), &limits)
        .expect("same-day replay");
    assert!(matches!(same_day, DecisionResult::Existing(_)));
    assert_eq!(same_day.decision().planned_usdc, usd(1));
    assert_eq!(state.decisions().len(), 1);

    let next_day = state
        .decide(&input(at(2026, 1, 2, 12), 1_000), &limits)
        .expect("next-day decision");
    assert!(next_day.is_new());
    assert!(next_day.decision().planned_usdc > usd(1));
    assert!(next_day
        .decision()
        .allocations
        .iter()
        .any(|row| row.tranche_id == "deposit-after"));
    assert!(state.withdrawals()["withdraw-after"].applied);
}

#[test]
fn future_deposit_cannot_retroactively_fund_an_earlier_withdrawal() {
    let limits = limits();
    let mut state = PacingState::default();
    let withdrawn_at = at(2026, 1, 1, 8);
    let deposited_at = at(2026, 1, 1, 9);
    let reconciled_at = at(2026, 1, 1, 10);
    let result = state.reconcile_capital(
        &[
            CapitalEvent::Withdrawal(WithdrawalEvent {
                event_id: "earlier-withdrawal".to_owned(),
                amount_usdc: usd(10),
                occurred_at: withdrawn_at,
                reconciled_at,
            }),
            deposit("later-deposit", 100, deposited_at),
        ],
        at(2026, 1, 1, 11),
        &limits,
    );
    assert!(matches!(
        result,
        Err(hype_accumulator::pacing::PacingError::WithdrawalExceedsFreeCapital(id))
            if id == "earlier-withdrawal"
    ));
    assert!(state.deposits().is_empty());
    assert!(state.withdrawals().is_empty());
}

#[test]
fn late_reconciliation_replays_prior_admissions_before_allocating_withdrawal() {
    let limits = limits();
    let mut state = PacingState::default();
    let withdrawn_at = at(2026, 1, 1, 8);
    let deposited_at = at(2026, 1, 1, 9);
    state
        .reconcile_capital(
            &[deposit("later-deposit", 100, deposited_at)],
            at(2026, 1, 1, 10),
            &limits,
        )
        .expect("later deposit admitted by the first call");
    assert_eq!(state.deposits()["later-deposit"].admitted_usdc, usd(100));

    let result = state.reconcile_capital(
        &[CapitalEvent::Withdrawal(WithdrawalEvent {
            event_id: "late-earlier-withdrawal".to_owned(),
            amount_usdc: usd(10),
            occurred_at: withdrawn_at,
            reconciled_at: at(2026, 1, 1, 11),
        })],
        at(2026, 1, 1, 12),
        &limits,
    );
    assert!(matches!(
        result,
        Err(hype_accumulator::pacing::PacingError::WithdrawalExceedsFreeCapital(id))
            if id == "late-earlier-withdrawal"
    ));
    assert_eq!(state.deposits()["later-deposit"].admitted_usdc, usd(100));
    assert!(state.withdrawals().is_empty());
}

#[test]
fn retroactive_withdrawal_never_reuses_committed_or_invested_capital() {
    let limits = limits();
    let mut committed_state = PacingState::default();
    committed_state
        .reconcile_capital(
            &[deposit("deposit", 100, at(2026, 1, 1, 7))],
            at(2026, 1, 1, 10),
            &limits,
        )
        .expect("deposit admitted");
    let decision = committed_state
        .decide(&input(at(2026, 1, 1, 12), 1_000), &limits)
        .expect("capital committed");
    assert!(!decision.decision().planned_usdc.is_zero());

    let late_withdrawal = |id: &str| {
        CapitalEvent::Withdrawal(WithdrawalEvent {
            event_id: id.to_owned(),
            amount_usdc: usd(100),
            occurred_at: at(2026, 1, 1, 11),
            reconciled_at: at(2026, 1, 1, 13),
        })
    };
    let committed_result = committed_state.reconcile_capital(
        &[late_withdrawal("withdraw-committed")],
        at(2026, 1, 1, 14),
        &limits,
    );
    assert!(matches!(
        committed_result,
        Err(hype_accumulator::pacing::PacingError::WithdrawalExceedsFreeCapital(id))
            if id == "withdraw-committed"
    ));
    assert!(committed_state.withdrawals().is_empty());

    let mut invested_state = committed_state;
    invested_state
        .settle_decision(
            &decision.decision().decision_id,
            decision.decision().planned_usdc,
            decision.decision().planned_usdc,
        )
        .expect("commitment invested");
    let invested_result = invested_state.reconcile_capital(
        &[late_withdrawal("withdraw-invested")],
        at(2026, 1, 1, 14),
        &limits,
    );
    assert!(matches!(
        invested_result,
        Err(hype_accumulator::pacing::PacingError::WithdrawalExceedsFreeCapital(id))
            if id == "withdraw-invested"
    ));
    assert!(invested_state.withdrawals().is_empty());
}

#[test]
fn late_earlier_deposit_cannot_displace_existing_fill_backing() {
    let mut limits = limits();
    limits.max_automatically_admitted_usdc = usd(100);
    limits.yearly_admission_cap_usdc = usd(100);
    limits.cumulative_admission_cap_usdc = usd(100);
    let mut state = PacingState::default();
    state
        .reconcile_capital(
            &[deposit("later-filled", 100, at(2026, 1, 1, 9))],
            at(2026, 1, 1, 10),
            &limits,
        )
        .expect("later tranche initially consumes the cap");
    let decision = state
        .decide(&input(at(2026, 1, 1, 12), 1_000), &limits)
        .expect("decision from later tranche");
    let filled = decision.decision().planned_usdc;
    state
        .settle_decision(&decision.decision().decision_id, filled, filled)
        .expect("later tranche filled");

    state
        .reconcile_capital(
            &[deposit("delayed-earlier", 100, at(2026, 1, 1, 8))],
            at(2026, 1, 1, 13),
            &limits,
        )
        .expect("delayed event uses only uncommitted admission capacity");

    let earlier = &state.deposits()["delayed-earlier"];
    let later = &state.deposits()["later-filled"];
    assert_eq!(later.invested_usdc, filled);
    assert_eq!(later.admitted_usdc, filled);
    assert_eq!(
        earlier.admitted_usdc,
        UsdcMicros::from_micros(usd(100).as_micros() - filled.as_micros())
    );
    assert_eq!(
        earlier.admitted_usdc.as_micros() + later.admitted_usdc.as_micros(),
        usd(100).as_micros()
    );
    state
        .validate_invariants()
        .expect("fill remains fully backed");
}

#[test]
fn stale_reconciliation_time_cannot_restore_applied_withdrawal_capital() {
    let limits = limits();
    let mut state = PacingState::default();
    state
        .reconcile_capital(
            &[
                deposit("deposit", 100, at(2026, 1, 1, 8)),
                CapitalEvent::Withdrawal(WithdrawalEvent {
                    event_id: "withdrawal".to_owned(),
                    amount_usdc: usd(20),
                    occurred_at: at(2026, 1, 1, 9),
                    reconciled_at: at(2026, 1, 1, 10),
                }),
            ],
            at(2026, 1, 1, 10),
            &limits,
        )
        .expect("withdrawal applied");
    let before = state.clone();

    let result = state.reconcile_capital(&[], at(2026, 1, 1, 9), &limits);
    assert!(matches!(
        result,
        Err(hype_accumulator::pacing::PacingError::ReconciliationTimeRegressed)
    ));
    assert_eq!(state, before);
    assert!(state.withdrawals()["withdrawal"].applied);
    assert_eq!(state.deposits()["deposit"].residual_usdc(), usd(80));

    state
        .decide(&input(at(2026, 1, 1, 12), 1_000), &limits)
        .expect("later planning retains the withdrawal");
    assert!(state.withdrawals()["withdrawal"].applied);
    assert_eq!(state.capital_reconciled_through(), Some(at(2026, 1, 1, 12)));
}

#[test]
fn decision_replays_withdrawals_that_became_ready_after_last_reconciliation() {
    let limits = limits();
    let mut state = PacingState::default();
    state
        .reconcile_capital(
            &[
                deposit("deposit", 100, at(2026, 1, 1, 8)),
                CapitalEvent::Withdrawal(WithdrawalEvent {
                    event_id: "future-ready".to_owned(),
                    amount_usdc: usd(20),
                    occurred_at: at(2026, 1, 1, 9),
                    reconciled_at: at(2026, 1, 1, 11),
                }),
            ],
            at(2026, 1, 1, 10),
            &limits,
        )
        .expect("future-ready withdrawal retained but not applied");
    assert!(!state.withdrawals()["future-ready"].applied);

    state
        .decide(&input(at(2026, 1, 1, 12), 1_000), &limits)
        .expect("decision advances capital replay");
    assert!(state.withdrawals()["future-ready"].applied);
    assert_eq!(state.deposits()["deposit"].withdrawn_usdc, usd(20));
}

#[test]
fn restored_state_revalidates_authoritative_admission_fields() {
    let limits = limits();
    let mut state = PacingState::default();
    state
        .reconcile_capital(
            &[unapproved_deposit("unapproved", 100, at(2026, 1, 1, 8))],
            at(2026, 1, 1, 10),
            &limits,
        )
        .expect("unapproved event retained without admission");
    let original = serde_json::to_value(&state).expect("serialize state");

    let mut admitted = original.clone();
    admitted["deposits"]["unapproved"]["admitted_usdc"] = serde_json::json!(1_000_000);
    let admitted: PacingState = serde_json::from_value(admitted).expect("restore tampered state");
    assert!(admitted.validate_invariants().is_ok());
    assert!(matches!(
        admitted.validate_for_limits(&limits),
        Err(hype_accumulator::pacing::PacingError::CorruptState)
    ));

    let mut usable = original.clone();
    usable["deposits"]["unapproved"]["first_usable_at"] = serde_json::json!("2026-01-01T08:00:01Z");
    let usable: PacingState = serde_json::from_value(usable).expect("restore tampered state");
    assert!(matches!(
        usable.validate_for_limits(&limits),
        Err(hype_accumulator::pacing::PacingError::CorruptState)
    ));

    let mut status = original;
    status["deposits"]["unapproved"]["status"] = serde_json::json!("admitted");
    let mut status: PacingState = serde_json::from_value(status).expect("restore tampered state");
    assert!(matches!(
        status.decide(&input(at(2026, 1, 1, 12), 1_000), &limits),
        Err(hype_accumulator::pacing::PacingError::CorruptState)
    ));
}

#[test]
fn unadmitted_or_balance_only_capital_fails_closed() {
    let limits = limits();
    let mut balance_only = PacingState::default();
    let no_event = balance_only
        .decide(&input(at(2026, 1, 1, 12), 50_000), &limits)
        .expect("durable no-capital decision");
    assert_eq!(no_event.decision().planned_usdc, usd(0));
    assert_eq!(
        no_event.decision().reason,
        DecisionReason::NoAdmittedCapital
    );

    let mut unadmitted = PacingState::default();
    let received = at(2026, 1, 2, 8);
    unadmitted
        .reconcile_capital(
            &[unapproved_deposit("unapproved", 100, received)],
            at(2026, 1, 2, 10),
            &limits,
        )
        .expect("record unapproved event");
    let decision = unadmitted
        .decide(&input(at(2026, 1, 2, 12), 50_000), &limits)
        .expect("fail-closed decision");
    assert_eq!(decision.decision().planned_usdc, usd(0));
    assert_eq!(
        unadmitted.deposits()["unapproved"].status,
        AdmissionStatus::AwaitingApproval
    );
    assert_eq!(
        decision.decision().alerts,
        vec![PacingAlert::UnadmittedCapital {
            amount_usdc: usd(100)
        }]
    );
}

#[test]
fn admission_minimum_daily_cap_and_reserve_are_enforced() {
    let mut capped = limits();
    capped.max_automatically_admitted_usdc = usd(30);
    capped.yearly_admission_cap_usdc = usd(30);
    capped.max_daily_notional_usdc = usd(10);
    capped.fee_spread_reserve_bps = 100;
    let received = at(2026, 6, 1, 8);
    let mut state = PacingState::default();
    state
        .reconcile_capital(
            &[deposit("capped", 100, received)],
            at(2026, 6, 1, 10),
            &capped,
        )
        .expect("partially admit against cap");
    let tranche = &state.deposits()["capped"];
    assert_eq!(tranche.admitted_usdc, usd(30));
    assert_eq!(tranche.unadmitted_usdc(), usd(70));
    assert_eq!(tranche.status, AdmissionStatus::PartiallyAdmitted);
    let decision = state
        .decide(&input(at(2026, 6, 1, 12), 10_000), &capped)
        .expect("capped decision");
    assert!(decision.decision().planned_usdc <= usd(10));
    assert!(decision
        .decision()
        .alerts
        .iter()
        .any(|alert| matches!(alert, PacingAlert::UnadmittedCapital { .. })));

    let mut minimum = limits();
    minimum.min_order_usdc = usd(5);
    let mut dust = PacingState::default();
    dust.reconcile_capital(
        &[deposit("dust", 4, at(2026, 6, 2, 8))],
        at(2026, 6, 2, 10),
        &minimum,
    )
    .expect("admit dust");
    let skipped = dust
        .decide(&input(at(2026, 6, 2, 12), 100), &minimum)
        .expect("minimum skip");
    assert_eq!(skipped.decision().planned_usdc, usd(0));
    assert_eq!(
        skipped.decision().reason,
        DecisionReason::BelowExchangeMinimum
    );

    let mut reserved = limits();
    reserved.max_daily_notional_usdc = usd(100);
    reserved.fee_spread_reserve_bps = 1_000;
    let mut reserve_state = PacingState::default();
    reserve_state
        .reconcile_capital(
            &[deposit("reserved", 100, at(2026, 12, 31, 8))],
            at(2026, 12, 31, 10),
            &reserved,
        )
        .expect("reserve deposit");
    let reserve_decision = reserve_state
        .decide(&input(at(2026, 12, 31, 12), 10_000), &reserved)
        .expect("reserve-capped decision");
    assert_eq!(reserve_decision.decision().planned_usdc, usd(90));
    assert_eq!(
        reserve_decision
            .decision()
            .explanation
            .admitted_budget_after_reserve_usdc,
        usd(90)
    );
}

#[test]
fn fixed_reserve_is_deducted_before_proportional_reserve() {
    let mut reserved = limits();
    reserved.max_daily_notional_usdc = usd(100);
    reserved.fixed_reserve_usdc = usd(10);
    reserved.fee_spread_reserve_bps = 1_000;
    let received = at(2026, 12, 31, 8);
    let mut state = PacingState::default();
    state
        .reconcile_capital(
            &[deposit("fixed-reserve", 100, received)],
            at(2026, 12, 31, 10),
            &reserved,
        )
        .expect("reserve deposit");

    let decision = state
        .decide(&input(at(2026, 12, 31, 12), 100), &reserved)
        .expect("reserve-aware decision");
    assert_eq!(decision.decision().planned_usdc, usd(81));
    assert_eq!(decision.decision().committed_usdc, usd(90));
    assert_eq!(state.deposits()["fixed-reserve"].residual_usdc(), usd(10));

    let mut blocked = PacingState::default();
    blocked
        .reconcile_capital(
            &[deposit("observed-floor", 100, received)],
            at(2026, 12, 31, 10),
            &reserved,
        )
        .expect("reserve deposit");
    let blocked_decision = blocked
        .decide(&input(at(2026, 12, 31, 12), 10), &reserved)
        .expect("balance at reserve fails closed");
    assert_eq!(blocked_decision.decision().planned_usdc, usd(0));
    assert_eq!(
        blocked_decision.decision().reason,
        DecisionReason::InsufficientObservedBalance
    );
}

#[test]
fn fixed_reserve_is_unavailable_to_withdrawals() {
    let mut reserved = limits();
    reserved.max_daily_notional_usdc = usd(100);
    reserved.fixed_reserve_usdc = usd(10);
    let received = at(2026, 12, 31, 8);
    let mut state = PacingState::default();
    state
        .reconcile_capital(
            &[deposit("withdrawal-reserve", 100, received)],
            at(2026, 12, 31, 10),
            &reserved,
        )
        .expect("reserve deposit");
    let decision = state
        .decide(&input(at(2026, 12, 31, 12), 100), &reserved)
        .expect("reserve-aware decision");
    assert_eq!(decision.decision().planned_usdc, usd(90));
    state
        .settle_decision(&decision.decision().decision_id, usd(90), usd(90))
        .expect("purchase settles above reserve");

    let before = state.clone();
    assert!(matches!(
        state.reconcile_capital(
            &[withdrawal("reserve-withdrawal", 10, at(2026, 12, 31, 15))],
            at(2026, 12, 31, 16),
            &reserved,
        ),
        Err(hype_accumulator::pacing::PacingError::WithdrawalExceedsFreeCapital(id))
            if id == "reserve-withdrawal"
    ));
    assert_eq!(state, before);
    assert_eq!(
        state.deposits()["withdrawal-reserve"].residual_usdc(),
        usd(10)
    );

    reserved.fixed_reserve_usdc = reserved.max_automatically_admitted_usdc;
    assert_eq!(
        reserved.validate(),
        Err(hype_accumulator::pacing::PacingError::InvalidLimits)
    );
}

#[test]
fn fixed_reserve_is_global_across_expired_and_active_horizons() {
    let mut reserved = limits();
    reserved.max_daily_notional_usdc = usd(100);
    reserved.fixed_reserve_usdc = usd(10);
    let mut state = PacingState::default();
    state
        .reconcile_capital(
            &[deposit("prior-reserve", 10, at(2026, 12, 31, 8))],
            at(2026, 12, 31, 10),
            &reserved,
        )
        .expect("prior reserve admitted");
    let prior = state
        .decide(&input(at(2026, 12, 31, 12), 10), &reserved)
        .expect("prior reserve held");
    assert_eq!(prior.decision().planned_usdc, usd(0));

    state
        .reconcile_capital(
            &[deposit("new-horizon", 100, at(2027, 12, 31, 8))],
            at(2027, 12, 31, 10),
            &reserved,
        )
        .expect("new horizon admitted");
    let next = state
        .decide(&input(at(2027, 12, 31, 12), 110), &reserved)
        .expect("existing reserve applies globally");
    assert_eq!(next.decision().planned_usdc, usd(100));
    assert_eq!(state.deposits()["prior-reserve"].residual_usdc(), usd(10));
    assert_eq!(state.deposits()["new-horizon"].committed_usdc, usd(100));
}

#[test]
fn automatic_admission_limit_is_per_deposit() {
    let mut limits = limits();
    limits.max_automatically_admitted_usdc = usd(100);
    limits.yearly_admission_cap_usdc = usd(500);
    limits.cumulative_admission_cap_usdc = usd(1_000);
    let mut state = PacingState::default();
    state
        .reconcile_capital(
            &[
                deposit("first", 60, at(2026, 1, 1, 8)),
                deposit("second", 60, at(2026, 1, 1, 9)),
            ],
            at(2026, 1, 1, 10),
            &limits,
        )
        .expect("both sub-limit deposits admitted");
    assert_eq!(state.deposits()["first"].admitted_usdc, usd(60));
    assert_eq!(state.deposits()["second"].admitted_usdc, usd(60));
    assert_eq!(
        state
            .deposits()
            .values()
            .map(|tranche| tranche.admitted_usdc.as_micros())
            .sum::<u64>(),
        usd(120).as_micros()
    );
}

#[test]
fn unsettled_commitment_encumbers_fee_spread_reserve() {
    let mut limits = limits();
    limits.max_daily_notional_usdc = usd(100);
    limits.fee_spread_reserve_bps = 1_000;
    let mut state = PacingState::default();
    state
        .reconcile_capital(
            &[deposit("reserved", 100, at(2026, 12, 31, 8))],
            at(2026, 12, 31, 10),
            &limits,
        )
        .expect("deposit admitted");
    let decision = state
        .decide(&input(at(2026, 12, 31, 12), 1_000), &limits)
        .expect("reserve-aware decision");
    assert_eq!(decision.decision().planned_usdc, usd(90));
    assert_eq!(decision.decision().committed_usdc, usd(100));
    assert_eq!(state.deposits()["reserved"].committed_usdc, usd(100));

    let before = state.clone();
    let blocked_withdrawal = CapitalEvent::Withdrawal(WithdrawalEvent {
        event_id: "blocked-withdrawal".to_owned(),
        amount_usdc: usd(1),
        occurred_at: at(2026, 12, 31, 13),
        reconciled_at: at(2026, 12, 31, 14),
    });
    assert!(matches!(
        state.reconcile_capital(&[blocked_withdrawal], at(2026, 12, 31, 14), &limits),
        Err(hype_accumulator::pacing::PacingError::WithdrawalExceedsFreeCapital(id))
            if id == "blocked-withdrawal"
    ));
    assert_eq!(state, before);

    assert!(matches!(
        state.settle_decision(&decision.decision().decision_id, usd(90), usd(89)),
        Err(hype_accumulator::pacing::PacingError::DebitBelowFill)
    ));
    assert!(matches!(
        state.settle_decision(&decision.decision().decision_id, usd(90), usd(101)),
        Err(hype_accumulator::pacing::PacingError::DebitExceedsCommitment)
    ));
    assert_eq!(state, before);

    state
        .settle_decision(&decision.decision().decision_id, usd(90), usd(95))
        .expect("settlement retains the actual fee debit");
    assert_eq!(state.deposits()["reserved"].invested_usdc, usd(95));
    assert_eq!(state.deposits()["reserved"].committed_usdc, usd(0));
    assert_eq!(state.deposits()["reserved"].residual_usdc(), usd(5));
    assert_eq!(
        state.decisions()[&at(2026, 12, 31, 12).date_naive()].debited_usdc,
        usd(95)
    );
    state
        .settle_decision(&decision.decision().decision_id, usd(90), usd(95))
        .expect("the exact fill and debit replay is idempotent");
    assert!(matches!(
        state.settle_decision(&decision.decision().decision_id, usd(90), usd(96)),
        Err(hype_accumulator::pacing::PacingError::ConflictingSettlement)
    ));
    let settled = state.clone();
    assert!(matches!(
        state.reconcile_capital(
            &[withdrawal("fee-not-reusable", 6, at(2026, 12, 31, 15))],
            at(2026, 12, 31, 16),
            &limits,
        ),
        Err(hype_accumulator::pacing::PacingError::WithdrawalExceedsFreeCapital(id))
            if id == "fee-not-reusable"
    ));
    assert_eq!(state, settled);
}

#[test]
fn reserve_commitment_can_span_tranches_without_changing_fill_attribution() {
    let mut limits = limits();
    limits.max_daily_notional_usdc = usd(50);
    limits.fee_spread_reserve_bps = 1_000;
    let mut state = PacingState::default();
    state
        .reconcile_capital(
            &[
                deposit("first", 50, at(2026, 12, 31, 8)),
                deposit("second", 50, at(2026, 12, 31, 9)),
            ],
            at(2026, 12, 31, 10),
            &limits,
        )
        .expect("both tranches admitted");
    let decision = state
        .decide(&input(at(2026, 12, 31, 12), 1_000), &limits)
        .expect("reserve spans the active tranches");
    assert_eq!(decision.decision().planned_usdc, usd(50));
    assert!(decision.decision().committed_usdc > usd(50));
    assert!(decision
        .decision()
        .allocations
        .iter()
        .any(
            |allocation| allocation.planned_usdc.is_zero() && !allocation.committed_usdc.is_zero()
        ));

    state
        .settle_decision(&decision.decision().decision_id, usd(50), usd(55))
        .expect("settlement releases reserve-only tranche slice");
    assert_eq!(state.deposits()["first"].invested_usdc, usd(50));
    assert_eq!(state.deposits()["second"].invested_usdc, usd(5));
    assert_eq!(state.deposits()["second"].committed_usdc, usd(0));
}

#[test]
fn late_year_infeasibility_holds_residual_across_rollover() {
    let limits = limits();
    let mut state = PacingState::default();
    state
        .reconcile_capital(
            &[deposit("late", 100, at(2026, 12, 31, 8))],
            at(2026, 12, 31, 10),
            &limits,
        )
        .expect("late deposit admitted");
    let final_day = state
        .decide(&input(at(2026, 12, 31, 12), 100), &limits)
        .expect("final-day decision");
    assert_eq!(final_day.decision().planned_usdc, usd(25));
    assert!(final_day.decision().alerts.iter().any(|alert| matches!(
        alert,
        PacingAlert::HorizonInfeasible {
            residual_usdc,
            remaining_capacity_usdc,
            ..
        } if *residual_usdc == usd(100) && *remaining_capacity_usdc == usd(25)
    )));
    state
        .settle_decision(&final_day.decision().decision_id, usd(25), usd(25))
        .expect("settle final-day fill");

    let rollover = state
        .decide(&input(at(2027, 1, 1, 12), 75), &limits)
        .expect("rollover hold decision");
    assert_eq!(rollover.decision().planned_usdc, usd(0));
    assert_eq!(
        rollover.decision().reason,
        DecisionReason::HorizonInfeasible
    );
    assert_ne!(
        rollover.decision().decision_id,
        final_day.decision().decision_id
    );
    assert_eq!(state.deposits()["late"].residual_usdc(), usd(75));
}

#[test]
fn long_outage_repaces_remaining_days_without_breaking_the_daily_cap() {
    let limits = limits();
    let mut state = PacingState::default();
    state
        .reconcile_capital(
            &[deposit("before-outage", 100, at(2026, 1, 1, 8))],
            at(2026, 1, 1, 10),
            &limits,
        )
        .expect("pre-outage deposit");

    let resumed = state
        .decide(&input(at(2026, 12, 30, 12), 100), &limits)
        .expect("decision after long outage");
    assert_eq!(resumed.decision().planned_usdc, usd(25));
    assert!(resumed.decision().alerts.iter().any(|alert| matches!(
        alert,
        PacingAlert::HorizonInfeasible {
            residual_usdc,
            remaining_capacity_usdc,
            ..
        } if *residual_usdc == usd(100) && *remaining_capacity_usdc == usd(50)
    )));
    state
        .settle_decision(&resumed.decision().decision_id, usd(25), usd(25))
        .expect("settle resumed purchase");
    let final_day = state
        .decide(&input(at(2026, 12, 31, 12), 75), &limits)
        .expect("final capped decision");
    assert_eq!(final_day.decision().planned_usdc, usd(25));
}

#[test]
fn restart_is_deterministic_and_durable_skip_is_not_retried() {
    let limits = limits();
    let mut original = PacingState::default();
    original
        .reconcile_capital(
            &[deposit("restart", 365, at(2026, 1, 1, 8))],
            at(2026, 1, 1, 10),
            &limits,
        )
        .expect("deposit admitted");
    let encoded = serde_json::to_vec(&original).expect("serialize durable state");
    let mut restored: PacingState =
        serde_json::from_slice(&encoded).expect("restore durable state");
    let decision_input = input(at(2026, 1, 1, 12), 365);
    let first = original
        .decide(&decision_input, &limits)
        .expect("original decision");
    let restored_first = restored
        .decide(&decision_input, &limits)
        .expect("restored decision");
    assert_eq!(first, restored_first);
    assert_eq!(original, restored);
    assert_eq!(first.decision().capital_snapshot_hash.len(), 64);
    assert_eq!(first.decision().input_snapshot_hash.len(), 64);

    let restarted_bytes = serde_json::to_vec(&original).expect("persist decision");
    let mut restarted: PacingState =
        serde_json::from_slice(&restarted_bytes).expect("restart after decision");
    let replay = restarted
        .decide(&decision_input, &limits)
        .expect("same-day replay");
    assert!(matches!(replay, DecisionResult::Existing(_)));

    let mut skipped = PacingState::default();
    skipped
        .reconcile_capital(
            &[deposit("skip", 100, at(2026, 1, 2, 8))],
            at(2026, 1, 2, 10),
            &limits,
        )
        .expect("deposit admitted");
    let mut incomplete = input(at(2026, 1, 2, 12), 100);
    incomplete.capital_history_complete = false;
    let durable_skip = skipped
        .decide(&incomplete, &limits)
        .expect("missing-history skip");
    assert_eq!(
        durable_skip.decision().reason,
        DecisionReason::MissingCapitalHistory
    );
    let later = skipped
        .decide(&input(at(2026, 1, 2, 20), 100), &limits)
        .expect("skip replay");
    assert!(matches!(later, DecisionResult::Existing(_)));
    assert_eq!(later.decision().planned_usdc, usd(0));
}

#[test]
fn mid_order_deposit_does_not_create_an_overlapping_next_day_intent() {
    let limits = limits();
    let mut state = PacingState::default();
    state
        .reconcile_capital(
            &[deposit("initial", 100, at(2026, 12, 29, 8))],
            at(2026, 12, 29, 10),
            &limits,
        )
        .expect("initial deposit");
    let open = state
        .decide(&input(at(2026, 12, 29, 12), 100), &limits)
        .expect("open commitment");
    assert!(!open.decision().planned_usdc.is_zero());

    state
        .reconcile_capital(
            &[deposit("mid-order", 100, at(2026, 12, 29, 18))],
            at(2026, 12, 29, 20),
            &limits,
        )
        .expect("mid-order deposit");
    let same_day = state
        .decide(&input(at(2026, 12, 29, 21), 200), &limits)
        .expect("same day is idempotent");
    assert!(matches!(same_day, DecisionResult::Existing(_)));

    let next_day = state
        .decide(&input(at(2026, 12, 30, 12), 200), &limits)
        .expect("unsettled guard");
    assert_eq!(next_day.decision().planned_usdc, usd(0));
    assert_eq!(
        next_day.decision().reason,
        DecisionReason::PriorDecisionUnsettled
    );
    assert_eq!(state.deposits()["mid-order"].invested_usdc, usd(0));
}

#[test]
fn partial_fill_releases_commitment_without_losing_attribution() {
    let limits = limits();
    let mut state = PacingState::default();
    state
        .reconcile_capital(
            &[deposit("fill", 100, at(2026, 12, 31, 8))],
            at(2026, 12, 31, 10),
            &limits,
        )
        .expect("deposit admitted");
    let decision = state
        .decide(&input(at(2026, 12, 31, 12), 100), &limits)
        .expect("planned decision");
    state
        .settle_decision(&decision.decision().decision_id, usd(7), usd(7))
        .expect("partial fill");
    let tranche = &state.deposits()["fill"];
    assert_eq!(tranche.invested_usdc, usd(7));
    assert_eq!(tranche.committed_usdc, usd(0));
    assert_eq!(tranche.residual_usdc(), usd(93));
    state
        .settle_decision(&decision.decision().decision_id, usd(7), usd(7))
        .expect("idempotent settlement");
}

#[test]
fn unsafe_caps_and_impossible_schedule_are_rejected_by_config() {
    let mut config = Config::from_toml(include_str!("fixtures/safe.toml")).expect("fixture");
    config.capital.max_automatically_deployable_usdc = 600.0;
    assert!(matches!(
        config.validate(&HashMap::new()),
        Err(ConfigError::Invalid(_))
    ));

    let mut impossible = Config::from_toml(include_str!("fixtures/safe.toml")).expect("fixture");
    impossible.capital.max_automatically_deployable_usdc = 10.0;
    impossible.capital.yearly_deployment_cap_usdc = 10.0;
    impossible.capital.cumulative_deployment_cap_usdc = 10.0;
    impossible.pacing.target_horizon_days = 1;
    impossible.pacing.final_catch_up_days = 1;
    impossible.pacing.min_order_usdc = 1.0;
    impossible.pacing.max_order_usdc = 5.0;
    impossible.execution.max_order_usdc = 5.0;
    impossible.schedule.weekdays = vec![1];
    assert!(matches!(
        impossible.validate(&HashMap::new()),
        Err(ConfigError::Invalid(message)) if message.contains("cannot fit")
    ));
}

#[test]
fn exact_microunit_schedule_capacity_avoids_float_drift() {
    let mut config = Config::from_toml(include_str!("fixtures/safe.toml")).expect("fixture");
    config.capital.max_automatically_deployable_usdc = 8_199.571_2;
    config.capital.yearly_deployment_cap_usdc = 8_199.571_2;
    config.capital.cumulative_deployment_cap_usdc = 8_199.571_2;
    config.pacing.max_order_usdc = 22.403_2;
    config.execution.max_order_usdc = 22.403_2;
    config.pacing.target_horizon_days = 366;
    config.schedule.weekdays = vec![1, 2, 3, 4, 5, 6, 7];
    config
        .validate(&HashMap::new())
        .expect("366 slots exactly fit an integer-microunit capital cap");
}

#[test]
fn six_decimal_monetary_values_use_decimal_conversion() {
    let mut config = Config::from_toml(include_str!("fixtures/safe.toml")).expect("fixture");
    config.capital.max_automatically_deployable_usdc = 16_890.122_169;
    config.capital.yearly_deployment_cap_usdc = 16_890.122_169;
    config.capital.cumulative_deployment_cap_usdc = 16_890.122_169;
    config.pacing.max_order_usdc = 200.0;
    config.execution.max_order_usdc = 200.0;
    config
        .validate(&HashMap::new())
        .expect("valid six-decimal microunit value is accepted");

    config.capital.max_automatically_deployable_usdc = 100.000_000_1;
    assert!(matches!(
        config.validate(&HashMap::new()),
        Err(ConfigError::SecurityPolicy(_))
    ));
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn property_arbitrary_capital_crash_restart_sequences_never_overspend_or_repeat(
        operations in prop::collection::vec(
            (1_u16..100, any::<bool>(), any::<bool>(), 0_u16..50, any::<bool>(), any::<bool>()),
            1..45,
        )
    ) {
        let limits = limits();
        let mut state = PacingState::default();
        for (index, (amount, after_decision, approved, withdrawal_amount, partial_fill, restart))
            in operations.into_iter().enumerate()
        {
            let day = at(2026, 1, 1, 0)
                .checked_add_signed(TimeDelta::days(i64::try_from(index).expect("small index")))
                .expect("fixture date range");
            let deposit_at = day + TimeDelta::hours(if after_decision { 18 } else { 8 });
            let event_id = format!("deposit-{index}");
            let event = if approved {
                deposit(event_id, u64::from(amount), deposit_at)
            } else {
                unapproved_deposit(event_id, u64::from(amount), deposit_at)
            };
            if !after_decision {
                state
                    .reconcile_capital(&[event.clone()], day + TimeDelta::hours(10), &limits)
                    .expect("morning event");
            }

            let decision_input = input(day + TimeDelta::hours(12), 50_000);
            let mut deterministic_copy: PacingState = serde_json::from_slice(
                &serde_json::to_vec(&state).expect("serialize pre-decision state")
            ).expect("restore pre-decision state");
            let first = state.decide(&decision_input, &limits).expect("daily decision");
            let copy = deterministic_copy.decide(&decision_input, &limits).expect("deterministic copy");
            prop_assert_eq!(&first, &copy);
            prop_assert_eq!(&state, &deterministic_copy);

            let replay = state
                .decide(&input(day + TimeDelta::hours(13), 100_000), &limits)
                .expect("same-day replay");
            prop_assert!(matches!(replay, DecisionResult::Existing(_)));
            if !first.decision().planned_usdc.is_zero() {
                let planned = first.decision().planned_usdc.as_micros();
                let fill = if partial_fill { planned / 2 } else { planned };
                state
                    .settle_decision(
                        &first.decision().decision_id,
                        UsdcMicros::from_micros(fill),
                        UsdcMicros::from_micros(fill),
                    )
                    .expect("bounded settlement");
            }

            if after_decision {
                state
                    .reconcile_capital(&[event], day + TimeDelta::hours(20), &limits)
                    .expect("evening event");
                let after_event_replay = state
                    .decide(&input(day + TimeDelta::hours(21), 100_000), &limits)
                    .expect("after-event same-day replay");
                prop_assert!(matches!(after_event_replay, DecisionResult::Existing(_)));
            }

            let free_micros = state
                .deposits()
                .values()
                .map(|tranche| tranche.residual_usdc().as_micros())
                .sum::<u64>();
            let requested = u64::from(withdrawal_amount) * 1_000_000;
            let withdraw_micros = requested.min(free_micros);
            if withdraw_micros != 0 {
                let event = CapitalEvent::Withdrawal(WithdrawalEvent {
                    event_id: format!("withdrawal-{index}"),
                    amount_usdc: UsdcMicros::from_micros(withdraw_micros),
                    occurred_at: day + TimeDelta::hours(22),
                    reconciled_at: day + TimeDelta::hours(22),
                });
                state
                    .reconcile_capital(&[event], day + TimeDelta::hours(23), &limits)
                    .expect("bounded withdrawal");
            }

            if restart {
                state = serde_json::from_slice(
                    &serde_json::to_vec(&state).expect("serialize restart state")
                ).expect("restore restart state");
            }
            state.validate_invariants().expect("state conservation");
        }

        let admitted = state
            .deposits()
            .values()
            .map(|tranche| tranche.admitted_usdc.as_micros())
            .sum::<u64>();
        let used = state
            .deposits()
            .values()
            .map(|tranche| {
                tranche.invested_usdc.as_micros()
                    + tranche.committed_usdc.as_micros()
                    + tranche.withdrawn_usdc.as_micros()
            })
            .sum::<u64>();
        prop_assert!(used <= admitted);
        prop_assert!(admitted <= limits.cumulative_admission_cap_usdc.as_micros());
        prop_assert!(state.deposits().values().all(|tranche|
            tranche.admitted_usdc <= limits.max_automatically_admitted_usdc
        ));
        let economic_days = state
            .decisions()
            .values()
            .filter(|decision| !decision.planned_usdc.is_zero())
            .map(|decision| decision.decision_date)
            .collect::<BTreeSet<_>>();
        let economic_count = state
            .decisions()
            .values()
            .filter(|decision| !decision.planned_usdc.is_zero())
            .count();
        prop_assert_eq!(economic_days.len(), economic_count);
    }
}
