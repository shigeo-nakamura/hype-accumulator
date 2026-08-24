use chrono::{TimeZone, Utc};
use hype_accumulator::{
    status::{AccumulatorStatus, DashboardStatus},
    status_io::write_status_atomic,
};

#[test]
fn atomic_writer_replaces_status_without_temp_files() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("status.json");
    let observed_at = Utc
        .with_ymd_and_hms(2026, 8, 24, 12, 0, 0)
        .single()
        .unwrap();
    let accumulator =
        AccumulatorStatus::new(25.0, 2.5, 40.0, observed_at, None, "daily", None).unwrap();
    let status = DashboardStatus::new(observed_at, observed_at, true, accumulator);

    write_status_atomic(&path, &status).unwrap();
    write_status_atomic(&path, &status).unwrap();

    let value: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    assert_eq!(value["accumulator"]["total_equity_usdc"], 125.0);
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
}
