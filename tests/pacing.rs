use chrono::{DateTime, TimeDelta, TimeZone, Utc};
use hype_accumulator::{
    config::{CarryOverPolicy, Config, ConfigError},
    pacing::{
        AdmissionStatus, CapitalEvent, DecisionInput, DecisionReason, DecisionResult, DepositEvent,
        PacingAlert, PacingLimits, PacingState, UsdcMicros, WithdrawalEvent,
    },
};
use proptest::prelude::*;
use std::collections::{BTreeSet, HashMap};

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
        .settle_decision(&first.decision().decision_id, usd(1))
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
        .settle_decision(&final_day.decision().decision_id, usd(25))
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
        .settle_decision(&decision.decision().decision_id, usd(7))
        .expect("partial fill");
    let tranche = &state.deposits()["fill"];
    assert_eq!(tranche.invested_usdc, usd(7));
    assert_eq!(tranche.committed_usdc, usd(0));
    assert_eq!(tranche.residual_usdc(), usd(93));
    state
        .settle_decision(&decision.decision().decision_id, usd(7))
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
        prop_assert!(admitted <= limits.max_automatically_admitted_usdc.as_micros());
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
