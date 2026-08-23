use crate::{account::CapitalSnapshot, config::CapitalConfig};

#[must_use]
pub fn automatically_deployable(snapshot: &CapitalSnapshot, limits: &CapitalConfig) -> f64 {
    let admitted = snapshot
        .admitted_deposits_usdc
        .min(snapshot.confirmed_deposits_usdc)
        .min(snapshot.observed_spot_usdc);
    let yearly = (limits.yearly_deployment_cap_usdc - snapshot.deployed_this_year_usdc).max(0.0);
    let cumulative =
        (limits.cumulative_deployment_cap_usdc - snapshot.deployed_cumulative_usdc).max(0.0);
    admitted
        .min(yearly)
        .min(cumulative)
        .min(limits.max_automatically_deployable_usdc)
        .max(0.0)
}
