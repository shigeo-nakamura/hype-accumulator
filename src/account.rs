#[derive(Clone, Debug, Default, PartialEq)]
pub struct CapitalSnapshot {
    pub observed_spot_usdc: f64,
    pub confirmed_deposits_usdc: f64,
    pub admitted_deposits_usdc: f64,
    pub deployed_this_year_usdc: f64,
    pub deployed_cumulative_usdc: f64,
}
