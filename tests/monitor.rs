use chrono::{TimeZone, Utc};
use hype_accumulator::{
    config::UtcSchedule,
    monitor::{
        reconcile_status, trade_cadence_label, BalanceObservation, HypeAttribution,
        StakingObservation,
    },
};

fn at(hour: u32) -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 8, 24, hour, 0, 0)
        .single()
        .unwrap()
}

#[test]
fn reconciliation_includes_only_ledger_attributed_hype() {
    let status = reconcile_status(
        &BalanceObservation {
            spot_usdc: 25.0,
            spot_hype: 2.0,
            hype_price_usdc: 40.0,
        },
        &StakingObservation {
            delegated_hype: 3.0,
            undelegated_hype: 0.5,
            pending_withdrawal_hype: 0.25,
            delegation_rows_hype: 3.0,
        },
        &HypeAttribution::Reconciled {
            hype: 5.75,
            last_trade_at: Some(at(10)),
            transferred_out_hype: 0.0,
            custodian: None,
        },
        at(12),
        "Mon/Wed/Fri at 12:00 UTC",
    )
    .unwrap();

    assert!((status.usdc_balance() - 25.0).abs() < f64::EPSILON);
    assert!((status.hype_balance() - 5.75).abs() < f64::EPSILON);
    assert!((status.total_equity_usdc() - 255.0).abs() < f64::EPSILON);
    assert!(status.is_healthy());
    assert_eq!(status.last_trade_at(), Some(&at(10)));
}

#[test]
fn delegation_summary_mismatch_is_degraded_not_hidden() {
    let status = reconcile_status(
        &BalanceObservation {
            spot_usdc: 25.0,
            spot_hype: 2.0,
            hype_price_usdc: 40.0,
        },
        &StakingObservation {
            delegated_hype: 3.0,
            undelegated_hype: 0.0,
            pending_withdrawal_hype: 0.0,
            delegation_rows_hype: 2.0,
        },
        &HypeAttribution::Reconciled {
            hype: 5.0,
            last_trade_at: None,
            transferred_out_hype: 0.0,
            custodian: None,
        },
        at(12),
        "daily",
    )
    .unwrap();

    assert!(!status.is_healthy());
    assert_eq!(
        status.health_reason(),
        Some("staking delegation total does not match delegator summary")
    );
}

#[test]
fn unavailable_attribution_excludes_account_hype_and_degrades() {
    let status = reconcile_status(
        &BalanceObservation {
            spot_usdc: 25.0,
            spot_hype: 2.0,
            hype_price_usdc: 40.0,
        },
        &StakingObservation {
            delegated_hype: 3.0,
            undelegated_hype: 0.5,
            pending_withdrawal_hype: 0.25,
            delegation_rows_hype: 3.0,
        },
        &HypeAttribution::Unavailable,
        at(12),
        "daily",
    )
    .unwrap();

    assert!(status.hype_balance().abs() < f64::EPSILON);
    assert!((status.total_equity_usdc() - 25.0).abs() < f64::EPSILON);
    assert_eq!(status.last_trade_at(), None);
    assert!(!status.is_healthy());
    assert_eq!(
        status.health_reason(),
        Some("HYPE attribution unavailable; account holdings excluded")
    );
}

#[test]
fn unattributed_hype_is_excluded_and_visible_as_degraded() {
    let status = reconcile_status(
        &BalanceObservation {
            spot_usdc: 25.0,
            spot_hype: 2.0,
            hype_price_usdc: 40.0,
        },
        &StakingObservation {
            delegated_hype: 3.0,
            undelegated_hype: 0.5,
            pending_withdrawal_hype: 0.25,
            delegation_rows_hype: 3.0,
        },
        &HypeAttribution::Reconciled {
            hype: 4.0,
            last_trade_at: Some(at(10)),
            transferred_out_hype: 0.0,
            custodian: None,
        },
        at(12),
        "daily",
    )
    .unwrap();

    assert!((status.hype_balance() - 4.0).abs() < f64::EPSILON);
    assert!((status.total_equity_usdc() - 185.0).abs() < f64::EPSILON);
    assert_eq!(status.last_trade_at(), Some(&at(10)));
    assert!(!status.is_healthy());
    assert_eq!(
        status.health_reason(),
        Some("unattributed HYPE account holdings excluded")
    );
}

#[test]
fn attribution_above_observed_hype_is_degraded_not_a_dropped_observation() {
    // The ledger says 6.0 HYPE is bot-owned but the account only holds 5.75:
    // bot-owned HYPE has left the account. The dashboard must still be
    // published — refusing to produce a document here would hide exactly the
    // situation that needs attention (bot-strategy#929).
    let status = reconcile_status(
        &BalanceObservation {
            spot_usdc: 25.0,
            spot_hype: 2.0,
            hype_price_usdc: 40.0,
        },
        &StakingObservation {
            delegated_hype: 3.0,
            undelegated_hype: 0.5,
            pending_withdrawal_hype: 0.25,
            delegation_rows_hype: 3.0,
        },
        &HypeAttribution::Reconciled {
            hype: 6.0,
            last_trade_at: Some(at(10)),
            transferred_out_hype: 0.0,
            custodian: None,
        },
        at(12),
        "daily",
    )
    .unwrap();

    // Zero, not the account total: the total includes whatever else the
    // account holds, and `hype_balance` must never include holdings that are
    // not the bot's.
    assert!(status.hype_balance().abs() < f64::EPSILON);
    assert!((status.total_equity_usdc() - 25.0).abs() < f64::EPSILON);
    assert!(!status.is_healthy());
    assert!(status.attribution_exceeds_holdings());
    assert_eq!(
        status.health_reason(),
        Some(
            "attributed HYPE exceeds observed account holdings; bot-owned HYPE has left the \
             account"
        )
    );
    assert_eq!(status.last_trade_at(), Some(&at(10)));
}

#[test]
fn attribution_within_tolerance_of_observed_hype_stays_healthy() {
    // Float arithmetic on the observed side must not turn an exactly-matching
    // ledger into a divergence report.
    let status = reconcile_status(
        &BalanceObservation {
            spot_usdc: 25.0,
            spot_hype: 2.0,
            hype_price_usdc: 40.0,
        },
        &StakingObservation {
            delegated_hype: 3.0,
            undelegated_hype: 0.5,
            pending_withdrawal_hype: 0.25,
            delegation_rows_hype: 3.0,
        },
        &HypeAttribution::Reconciled {
            hype: 5.75 + 1e-12,
            last_trade_at: None,
            transferred_out_hype: 0.0,
            custodian: None,
        },
        at(12),
        "daily",
    )
    .unwrap();

    assert!(status.is_healthy());
}

#[test]
fn a_last_trade_after_the_balance_read_degrades_instead_of_dropping_the_status() {
    // A clock that stepped backwards (NTP correction, restored backup) must
    // not stop the status document being written: the attributed fill time is
    // dropped and the disagreement is reported (bot-strategy#929).
    let status = reconcile_status(
        &BalanceObservation {
            spot_usdc: 25.0,
            spot_hype: 2.0,
            hype_price_usdc: 40.0,
        },
        &StakingObservation {
            delegated_hype: 0.0,
            undelegated_hype: 0.0,
            pending_withdrawal_hype: 0.0,
            delegation_rows_hype: 0.0,
        },
        &HypeAttribution::Reconciled {
            hype: 2.0,
            last_trade_at: Some(at(14)),
            transferred_out_hype: 0.0,
            custodian: None,
        },
        at(12),
        "daily",
    )
    .unwrap();

    assert_eq!(status.last_trade_at(), None);
    assert!(!status.is_healthy());
    assert_eq!(
        status.health_reason(),
        Some(
            "last attributed fill is after the balance observation; clock or history is \
             inconsistent"
        )
    );
}

#[test]
fn cadence_label_is_stable_and_deduplicated() {
    assert_eq!(
        trade_cadence_label(&UtcSchedule {
            utc_hour: 12,
            utc_minute: 5,
            weekdays: vec![5, 1, 3, 3],
        }),
        "Mon/Wed/Fri at 12:05 UTC"
    );
    assert_eq!(
        trade_cadence_label(&UtcSchedule {
            utc_hour: 0,
            utc_minute: 0,
            weekdays: vec![1, 2, 3, 4, 5, 6, 7],
        }),
        "Daily at 00:00 UTC"
    );
}

/// bot-strategy#929 slice C: HYPE that left the account by an explained
/// movement is reported beside `hype_balance`, never inside it, and does not
/// change how the divergence check bounds the held claim by the holdings.
#[test]
fn transferred_out_hype_is_reported_beside_the_held_balance() {
    let balances = BalanceObservation {
        spot_usdc: 25.0,
        spot_hype: 0.7,
        hype_price_usdc: 40.0,
    };
    let staking = StakingObservation {
        delegated_hype: 0.0,
        undelegated_hype: 0.0,
        pending_withdrawal_hype: 0.0,
        delegation_rows_hype: 0.0,
    };
    // Bought 1.2, 0.5 moved out through the ledger: the account holds 0.7.
    let status = reconcile_status(
        &balances,
        &staking,
        &HypeAttribution::Reconciled {
            hype: 0.7,
            last_trade_at: Some(at(10)),
            transferred_out_hype: 0.5,
            custodian: None,
        },
        at(12),
        "daily",
    )
    .unwrap();
    assert!(status.is_healthy());
    assert!((status.hype_balance() - 0.7).abs() < f64::EPSILON);
    assert_eq!(status.hype_transferred_out(), Some(0.5));
    assert!(
        (status.total_equity_usdc() - 53.0).abs() < 1e-9,
        "equity counts held HYPE only"
    );

    // The ledger still claims more than the account holds (a further 0.2
    // left by a path the ledger does not show): degraded, claim zeroed, the
    // explained part still reported.
    let degraded = reconcile_status(
        &balances,
        &staking,
        &HypeAttribution::Reconciled {
            hype: 0.9,
            last_trade_at: Some(at(10)),
            transferred_out_hype: 0.5,
            custodian: None,
        },
        at(12),
        "daily",
    )
    .unwrap();
    assert!(degraded.attribution_exceeds_holdings());
    assert!((degraded.hype_balance()).abs() < f64::EPSILON);
    assert_eq!(degraded.hype_transferred_out(), Some(0.5));

    // Without an attribution there is nothing to report as transferred out.
    let unavailable = reconcile_status(
        &balances,
        &staking,
        &HypeAttribution::Unavailable,
        at(12),
        "daily",
    )
    .unwrap();
    assert_eq!(unavailable.hype_transferred_out(), None);

    // A negative or non-finite figure is refused like every other amount.
    assert!(reconcile_status(
        &balances,
        &staking,
        &HypeAttribution::Reconciled {
            hype: 0.7,
            last_trade_at: None,
            transferred_out_hype: -0.1,
            custodian: None,
        },
        at(12),
        "daily",
    )
    .is_err());
}

/// bot-strategy#847: with a staking custodian named, the ledger's custodian
/// view and the custodian's own staking balances are published beside the
/// held balance; a transfer the custodian has not staked yet degrades the
/// status with its shortfall; a failed custodian read degrades instead of
/// failing the observation; and the divergence halt zeroes the eligible
/// figure while keeping the transferred record.
#[test]
#[allow(clippy::too_many_lines)]
fn custodian_view_is_published_and_an_unstaked_transfer_degrades() {
    use hype_accumulator::{
        monitor::{reconcile_status_with_custodian, CustodianAttribution, CustodianObservation},
        status::CUSTODIAN_STAKING_SHORTFALL,
    };
    let balances = BalanceObservation {
        spot_usdc: 25.0,
        spot_hype: 0.7,
        hype_price_usdc: 40.0,
    };
    let staking = StakingObservation {
        delegated_hype: 0.0,
        undelegated_hype: 0.0,
        pending_withdrawal_hype: 0.0,
        delegation_rows_hype: 0.0,
    };
    // Bought 1.2; 0.5 left, 0.4 of it to the custodian; residual 0.1 keeps
    // 0.6 eligible. The custodian has 0.4 in staking (0.3 delegated, 0.1
    // still undelegated after cDeposit).
    let attribution = HypeAttribution::Reconciled {
        hype: 0.7,
        last_trade_at: Some(at(10)),
        transferred_out_hype: 0.5,
        custodian: Some(CustodianAttribution {
            transferred_hype: 0.4,
            eligible_for_transfer_hype: 0.6,
        }),
    };
    let staked = CustodianObservation::Observed {
        delegated_hype: 0.3,
        undelegated_hype: 0.1,
        pending_withdrawal_hype: 0.0,
    };
    let status = reconcile_status_with_custodian(
        &balances,
        &staking,
        &staked,
        &attribution,
        at(12),
        "daily",
    )
    .unwrap();
    assert!(status.is_healthy(), "{:?}", status.health_reason());
    assert!((status.hype_balance() - 0.7).abs() < f64::EPSILON);
    assert_eq!(status.hype_transferred_out(), Some(0.5));
    assert_eq!(status.hype_transferred_to_custodian(), Some(0.4));
    assert_eq!(status.hype_eligible_for_transfer(), Some(0.6));
    let custodian_staking = status.custodian_staking().expect("custodian staking");
    assert!((custodian_staking.delegated_hype - 0.3).abs() < f64::EPSILON);
    assert!((custodian_staking.undelegated_hype - 0.1).abs() < f64::EPSILON);
    assert_eq!(custodian_staking.shortfall_hype, Some(0.0));
    assert!(!status.custodian_staking_shortfall());
    assert!(
        (status.total_equity_usdc() - 53.0).abs() < 1e-9,
        "equity counts held HYPE only; the custodian's staking is not the bot's"
    );

    // The custodian holds less in staking than was transferred: the owner
    // moved HYPE but did not stake it. Degraded, with the shortfall.
    let unstaked = CustodianObservation::Observed {
        delegated_hype: 0.25,
        undelegated_hype: 0.0,
        pending_withdrawal_hype: 0.0,
    };
    let degraded = reconcile_status_with_custodian(
        &balances,
        &staking,
        &unstaked,
        &attribution,
        at(12),
        "daily",
    )
    .unwrap();
    assert!(!degraded.is_healthy());
    assert!(degraded.custodian_staking_shortfall());
    assert!(degraded
        .health_reason()
        .is_some_and(|reason| reason.contains(CUSTODIAN_STAKING_SHORTFALL)));
    let shortfall = degraded
        .custodian_staking()
        .and_then(|staking| staking.shortfall_hype)
        .expect("shortfall");
    assert!((shortfall - 0.15).abs() < 1e-9, "{shortfall}");
    // Still a status document with every other figure intact.
    assert!((degraded.hype_balance() - 0.7).abs() < f64::EPSILON);
    assert_eq!(degraded.hype_eligible_for_transfer(), Some(0.6));

    // A custodian read that failed degrades; nothing else is lost.
    let unavailable = reconcile_status_with_custodian(
        &balances,
        &staking,
        &CustodianObservation::Unavailable,
        &attribution,
        at(12),
        "daily",
    )
    .unwrap();
    assert!(!unavailable.is_healthy());
    assert!(unavailable
        .health_reason()
        .is_some_and(|reason| reason.contains("staking custodian read unavailable")));
    assert_eq!(unavailable.custodian_staking(), None);
    assert_eq!(unavailable.hype_transferred_to_custodian(), Some(0.4));

    // No custodian named: none of the custodian fields exist, whatever the
    // attribution says about transfers.
    let plain = reconcile_status(
        &balances,
        &staking,
        &HypeAttribution::Reconciled {
            hype: 0.7,
            last_trade_at: Some(at(10)),
            transferred_out_hype: 0.5,
            custodian: None,
        },
        at(12),
        "daily",
    )
    .unwrap();
    assert_eq!(plain.hype_transferred_to_custodian(), None);
    assert_eq!(plain.hype_eligible_for_transfer(), None);
    assert_eq!(plain.custodian_staking(), None);

    // The divergence halt: the eligible figure is derived from a claim this
    // read refused, so it is zeroed; the transferred record stays.
    let halted = reconcile_status_with_custodian(
        &balances,
        &staking,
        &staked,
        &HypeAttribution::Reconciled {
            hype: 0.9,
            last_trade_at: Some(at(10)),
            transferred_out_hype: 0.5,
            custodian: Some(CustodianAttribution {
                transferred_hype: 0.4,
                eligible_for_transfer_hype: 0.8,
            }),
        },
        at(12),
        "daily",
    )
    .unwrap();
    assert!(halted.attribution_exceeds_holdings());
    assert_eq!(halted.hype_transferred_to_custodian(), Some(0.4));
    assert_eq!(halted.hype_eligible_for_transfer(), Some(0.0));

    // A custodian part larger than the whole transferred-out figure is not
    // an attribution this reconciliation will publish.
    assert!(reconcile_status_with_custodian(
        &balances,
        &staking,
        &staked,
        &HypeAttribution::Reconciled {
            hype: 0.7,
            last_trade_at: None,
            transferred_out_hype: 0.5,
            custodian: Some(CustodianAttribution {
                transferred_hype: 0.6,
                eligible_for_transfer_hype: 0.6,
            }),
        },
        at(12),
        "daily",
    )
    .is_err());
}
