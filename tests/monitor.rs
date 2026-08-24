use chrono::{TimeZone, Utc};
use hype_accumulator::{
    config::UtcSchedule,
    monitor::{reconcile_status, trade_cadence_label, BalanceObservation, StakingObservation},
};

fn at(hour: u32) -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 8, 24, hour, 0, 0)
        .single()
        .unwrap()
}

#[test]
fn reconciliation_includes_all_owned_staking_states() {
    let status = reconcile_status(
        &BalanceObservation {
            spot_usdc: 25.0,
            spot_hype: 2.0,
            hype_price_usdc: 40.0,
            last_trade_at: Some(at(10)),
        },
        &StakingObservation {
            delegated_hype: 3.0,
            undelegated_hype: 0.5,
            pending_withdrawal_hype: 0.25,
            delegation_rows_hype: 3.0,
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
            last_trade_at: None,
        },
        &StakingObservation {
            delegated_hype: 3.0,
            undelegated_hype: 0.0,
            pending_withdrawal_hype: 0.0,
            delegation_rows_hype: 2.0,
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
