#[derive(Clone, Debug, Default, PartialEq)]
pub struct MetricsSnapshot {
    pub observed_spot_usdc: f64,
    pub admitted_deposits_usdc: f64,
    pub deployable_usdc: f64,
    pub dry_run_actions: u64,
}
