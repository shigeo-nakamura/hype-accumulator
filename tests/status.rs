use chrono::{TimeZone, Utc};
use hype_accumulator::status::{AccumulatorStatus, DashboardStatus, StatusError};

fn at(hour: u32) -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 8, 24, hour, 0, 0)
        .single()
        .unwrap()
}

#[test]
fn dashboard_status_derives_equity_and_serializes_activity() {
    let accumulator = AccumulatorStatus::new(
        25.0,
        2.5,
        40.0,
        at(12),
        Some(at(10)),
        "Mon/Wed/Fri at 12:00 UTC",
        None,
    )
    .unwrap();
    let status = DashboardStatus::new(at(12), at(8), true, accumulator);
    let value: serde_json::Value = serde_json::from_str(&status.to_json().unwrap()).unwrap();

    assert_eq!(value["schema_version"], 1);
    assert_eq!(value["dex"], "hyperliquid");
    assert_eq!(value["dry_run"], true);
    assert_eq!(value["accumulator"]["total_equity_usdc"], 125.0);
    assert_eq!(value["accumulator"]["usdc_balance"], 25.0);
    assert_eq!(value["accumulator"]["hype_balance"], 2.5);
    assert_eq!(value["accumulator"]["healthy"], true);
    assert_eq!(
        value["accumulator"]["trade_cadence"],
        "Mon/Wed/Fri at 12:00 UTC"
    );
    assert_eq!(
        value["accumulator"]["last_trade_at"],
        "2026-08-24T10:00:00Z"
    );
    assert!(value["accumulator"].get("health_reason").is_none());
}

#[test]
fn degraded_status_requires_a_nonempty_reason() {
    let degraded = AccumulatorStatus::new(
        10.0,
        1.0,
        40.0,
        at(12),
        None,
        "daily",
        Some("account reconciliation delayed".to_owned()),
    )
    .unwrap();
    assert!(!degraded.is_healthy());
    assert_eq!(
        degraded.health_reason(),
        Some("account reconciliation delayed")
    );

    assert_eq!(
        AccumulatorStatus::new(
            10.0,
            1.0,
            40.0,
            at(12),
            None,
            "daily",
            Some("   ".to_owned()),
        ),
        Err(StatusError::EmptyHealthReason)
    );
}

#[test]
fn invalid_or_future_measurements_fail_closed() {
    assert_eq!(
        AccumulatorStatus::new(f64::NAN, 1.0, 40.0, at(12), None, "daily", None),
        Err(StatusError::NonFinite("usdc_balance"))
    );
    assert_eq!(
        AccumulatorStatus::new(1.0, -1.0, 40.0, at(12), None, "daily", None),
        Err(StatusError::Negative("hype_balance"))
    );
    assert_eq!(
        AccumulatorStatus::new(1.0, 1.0, 0.0, at(12), None, "daily", None),
        Err(StatusError::NonPositivePrice)
    );
    assert_eq!(
        AccumulatorStatus::new(1.0, 1.0, 40.0, at(11), Some(at(12)), "daily", None),
        Err(StatusError::FutureLastTrade)
    );
}
