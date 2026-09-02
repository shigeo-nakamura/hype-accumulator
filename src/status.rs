use crate::metrics::MetricsSnapshot;
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
    total_equity_usdc: f64,
    usdc_balance: f64,
    hype_balance: f64,
    hype_price_usdc: f64,
    #[serde(skip)]
    balance_observation_started_at: DateTime<Utc>,
    balance_observed_at: DateTime<Utc>,
    last_trade_at: Option<DateTime<Utc>>,
    trade_cadence: String,
    healthy: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    health_reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct DashboardStatus {
    schema_version: u8,
    ts: i64,
    updated_at: DateTime<Utc>,
    process_started_at: i64,
    dex: &'static str,
    dry_run: bool,
    accumulator: AccumulatorStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    operations: Option<MetricsSnapshot>,
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
    #[error("balance observation start must not be after completion")]
    InvalidBalanceObservationWindow,
    #[error("operations observation must not be after status update")]
    FutureOperations,
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
        Self::new_with_balance_window(
            usdc_balance,
            hype_balance,
            hype_price_usdc,
            balance_observed_at,
            balance_observed_at,
            last_trade_at,
            trade_cadence,
            health_reason,
        )
    }

    /// Constructs a validated balance snapshot whose value was obtained within
    /// a closed request window. The start is retained only for point-in-time
    /// reconciliation and is not emitted in public status JSON.
    ///
    /// # Errors
    ///
    /// Returns [`StatusError`] for an inverted observation window or any error
    /// accepted by [`Self::new`].
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_balance_window(
        usdc_balance: f64,
        hype_balance: f64,
        hype_price_usdc: f64,
        balance_observation_started_at: DateTime<Utc>,
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
        if balance_observation_started_at > balance_observed_at {
            return Err(StatusError::InvalidBalanceObservationWindow);
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
            balance_observation_started_at,
            balance_observed_at,
            last_trade_at,
            trade_cadence,
            healthy: health_reason.is_none(),
            health_reason,
        })
    }

    #[must_use]
    pub const fn total_equity_usdc(&self) -> f64 {
        self.total_equity_usdc
    }

    #[must_use]
    pub const fn usdc_balance(&self) -> f64 {
        self.usdc_balance
    }

    #[must_use]
    pub const fn hype_balance(&self) -> f64 {
        self.hype_balance
    }

    #[must_use]
    pub const fn hype_price_usdc(&self) -> f64 {
        self.hype_price_usdc
    }

    #[must_use]
    pub const fn balance_observed_at(&self) -> &DateTime<Utc> {
        &self.balance_observed_at
    }

    #[must_use]
    pub const fn balance_observation_started_at(&self) -> &DateTime<Utc> {
        &self.balance_observation_started_at
    }

    #[must_use]
    pub const fn last_trade_at(&self) -> Option<&DateTime<Utc>> {
        self.last_trade_at.as_ref()
    }

    #[must_use]
    pub fn trade_cadence(&self) -> &str {
        &self.trade_cadence
    }

    #[must_use]
    pub const fn is_healthy(&self) -> bool {
        self.healthy
    }

    #[must_use]
    pub fn health_reason(&self) -> Option<&str> {
        self.health_reason.as_deref()
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
            operations: None,
        }
    }

    /// Attaches an identifier-free operational projection.
    ///
    /// # Errors
    ///
    /// Returns [`StatusError::FutureOperations`] if the projection is newer
    /// than this status update.
    pub fn with_operations(mut self, operations: MetricsSnapshot) -> Result<Self, StatusError> {
        if operations.observed_at > self.updated_at {
            return Err(StatusError::FutureOperations);
        }
        self.operations = Some(operations);
        Ok(self)
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
