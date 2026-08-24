use crate::{account::CapitalSnapshot, config::CapitalConfig};

#[must_use]
pub fn automatically_deployable(snapshot: &CapitalSnapshot, limits: &CapitalConfig) -> f64 {
    if [
        snapshot.observed_spot_usdc,
        snapshot.confirmed_deposits_usdc,
        snapshot.admitted_deposits_usdc,
        snapshot.deployed_this_year_usdc,
        snapshot.deployed_cumulative_usdc,
    ]
    .iter()
    .any(|value| !value.is_finite() || *value < 0.0)
    {
        return 0.0;
    }

    let admitted_remaining = (snapshot
        .admitted_deposits_usdc
        .min(snapshot.confirmed_deposits_usdc)
        - snapshot.deployed_cumulative_usdc)
        .max(0.0)
        .min(snapshot.observed_spot_usdc);
    let yearly = (limits.yearly_deployment_cap_usdc - snapshot.deployed_this_year_usdc).max(0.0);
    let cumulative =
        (limits.cumulative_deployment_cap_usdc - snapshot.deployed_cumulative_usdc).max(0.0);
    admitted_remaining.min(yearly).min(cumulative).max(0.0)
}
