use chrono::{DateTime, Utc};
use serde::Serialize;
use thiserror::Error;

pub const DASHBOARD_SCHEMA_VERSION: u8 = 1;

/// Dashboard-safe HYPE accumulation measurements.
///
/// `hype_balance` is the reconciled total owned by the configured account,
/// including spot, staking, and delegated balances. Unattributed holdings must
/// not be included. `total_equity_usdc` is always derived by this type.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct AccumulatorStatus {
    pub total_equity_usdc: f64,
    pub usdc_balance: f64,
    pub hype_balance: f64,
    pub hype_price_usdc: f64,
    pub balance_observed_at: DateTime<Utc>,
    pub last_trade_at: Option<DateTime<Utc>>,
    pub trade_cadence: String,
    pub healthy: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub health_reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct DashboardStatus {
    pub schema_version: u8,
    pub ts: i64,
    pub updated_at: DateTime<Utc>,
    pub process_started_at: i64,
    pub dex: &'static str,
    pub dry_run: bool,
    pub accumulator: AccumulatorStatus,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum StatusError {
    #[error("{0} must be finite")]
    NonFinite(&'static str),
    #[error("{0} must not be negative")]
    Negative(&'static str),
    #[error("hype_price_usdc must be positive")]
    NonPositivePrice,
    #[error("trade cadence must not be empty")]
    EmptyCadence,
    #[error("health reason must not be empty")]
    EmptyHealthReason,
    #[error("last_trade_at must not be after balance_observed_at")]
    FutureLastTrade,
}

impl AccumulatorStatus {
    /// Constructs a validated balance snapshot and derives total equity.
    ///
    /// A missing health reason means healthy. Supplying a non-empty reason
    /// marks the snapshot degraded while preserving the last reconciled values.
    ///
    /// # Errors
    ///
    /// Returns [`StatusError`] for non-finite or negative balances, a
    /// non-positive mark, invalid activity timestamps, or blank labels.
    pub fn new(
        usdc_balance: f64,
        hype_balance: f64,
        hype_price_usdc: f64,
        balance_observed_at: DateTime<Utc>,
        last_trade_at: Option<DateTime<Utc>>,
        trade_cadence: impl Into<String>,
        health_reason: Option<String>,
    ) -> Result<Self, StatusError> {
        finite_non_negative("usdc_balance", usdc_balance)?;
        finite_non_negative("hype_balance", hype_balance)?;
        if !hype_price_usdc.is_finite() {
            return Err(StatusError::NonFinite("hype_price_usdc"));
        }
        if hype_price_usdc <= 0.0 {
            return Err(StatusError::NonPositivePrice);
        }
        if last_trade_at.is_some_and(|value| value > balance_observed_at) {
            return Err(StatusError::FutureLastTrade);
        }
        let trade_cadence = trade_cadence.into();
        if trade_cadence.trim().is_empty() {
            return Err(StatusError::EmptyCadence);
        }
        let health_reason = health_reason
            .map(|reason| reason.trim().to_owned())
            .transpose_empty()?;
        let total_equity_usdc = usdc_balance + hype_balance * hype_price_usdc;
        if !total_equity_usdc.is_finite() {
            return Err(StatusError::NonFinite("total_equity_usdc"));
        }
        Ok(Self {
            total_equity_usdc,
            usdc_balance,
            hype_balance,
            hype_price_usdc,
            balance_observed_at,
            last_trade_at,
            trade_cadence,
            healthy: health_reason.is_none(),
            health_reason,
        })
    }
}

impl DashboardStatus {
    #[must_use]
    pub fn new(
        updated_at: DateTime<Utc>,
        process_started_at: DateTime<Utc>,
        dry_run: bool,
        accumulator: AccumulatorStatus,
    ) -> Self {
        Self {
            schema_version: DASHBOARD_SCHEMA_VERSION,
            ts: updated_at.timestamp(),
            updated_at,
            process_started_at: process_started_at.timestamp(),
            dex: "hyperliquid",
            dry_run,
            accumulator,
        }
    }

    /// Serializes the public dashboard payload without account identity or
    /// secret material.
    ///
    /// # Errors
    ///
    /// Returns [`serde_json::Error`] if JSON serialization fails.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }
}

fn finite_non_negative(name: &'static str, value: f64) -> Result<(), StatusError> {
    if !value.is_finite() {
        return Err(StatusError::NonFinite(name));
    }
    if value < 0.0 {
        return Err(StatusError::Negative(name));
    }
    Ok(())
}

trait TransposeEmpty {
    fn transpose_empty(self) -> Result<Option<String>, StatusError>;
}

impl TransposeEmpty for Option<String> {
    fn transpose_empty(self) -> Result<Option<String>, StatusError> {
        if self.as_ref().is_some_and(String::is_empty) {
            Err(StatusError::EmptyHealthReason)
        } else {
            Ok(self)
        }
    }
}
