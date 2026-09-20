use crate::metrics::MetricsSnapshot;
use chrono::{DateTime, Utc};
use serde::Serialize;
use thiserror::Error;

pub const DASHBOARD_SCHEMA_VERSION: u8 = 1;

/// Health reason reported when the workflow ledger claims more bot-owned HYPE
/// than the account holds (bot-strategy#929).
///
/// A decision signal, not only display text: anything that can still act
/// economically must stop doing so while it holds. Read it through
/// [`AccumulatorStatus::attribution_exceeds_holdings`], never by re-matching
/// the text at a call site.
pub const ATTRIBUTION_EXCEEDS_HOLDINGS: &str =
    "attributed HYPE exceeds observed account holdings; bot-owned HYPE has left the account";

/// Health reason reported when more bot-acquired HYPE was transferred to the
/// staking custodian than the custodian holds in staking at all
/// (bot-strategy#847): a transfer the owner has not staked yet. Advisory —
/// no capital decision reads it.
pub const CUSTODIAN_STAKING_SHORTFALL: &str =
    "HYPE transferred to the staking custodian exceeds the custodian's staking balance";

/// The staking custodian's own staking balances (bot-strategy#847): an upper
/// bound on bot HYPE staked there, since the custodian commingles other
/// holdings. Dashboard-safe; carries no address.
#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
pub struct CustodianStakingStatus {
    pub delegated_hype: f64,
    pub undelegated_hype: f64,
    pub pending_withdrawal_hype: f64,
    /// `hype_transferred_to_custodian` less the custodian's whole staking
    /// balance when positive, else zero; absent when attribution is
    /// unavailable and there is nothing to compare against.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shortfall_hype: Option<f64>,
}

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
    /// Bot-acquired HYPE that left the account by a transfer the venue's
    /// ledger records (bot-strategy#929 slice C). Not part of
    /// `hype_balance`, which counts only what the account still holds, and
    /// absent when attribution is unavailable.
    #[serde(skip_serializing_if = "Option::is_none")]
    hype_transferred_out: Option<f64>,
    /// The part of `hype_transferred_out` that went to the staking
    /// custodian (bot-strategy#847). Present only when the policy names one.
    #[serde(skip_serializing_if = "Option::is_none")]
    hype_transferred_to_custodian: Option<f64>,
    /// Bot-acquired HYPE the owner may transfer to the custodian now:
    /// `hype_balance` less the policy's residual buffer, zero while
    /// attribution is degraded. Advisory; nothing acts on it.
    #[serde(skip_serializing_if = "Option::is_none")]
    hype_eligible_for_transfer: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    custodian_staking: Option<CustodianStakingStatus>,
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
    #[error("custodian attribution requires hype_transferred_out")]
    CustodianWithoutTransferredOut,
    #[error("hype_transferred_to_custodian must not exceed hype_transferred_out")]
    CustodianExceedsTransferredOut,
}

impl AccumulatorStatus {
    /// Whether a last-trade instant can be reported against a balance read
    /// taken at `balance_observed_at`: the one predicate behind this type's
    /// own validation and the observer's degrade-instead-of-error filter, so
    /// the two cannot disagree and drop an observation.
    #[must_use]
    pub fn last_trade_is_plausible(
        last_trade_at: DateTime<Utc>,
        balance_observed_at: DateTime<Utc>,
    ) -> bool {
        last_trade_at <= balance_observed_at
    }

    /// Whether this observation reports that the workflow ledger claims more
    /// bot-owned HYPE than the account holds (bot-strategy#929). While true,
    /// nothing may commit capital or prepare an order; the recurring cycle
    /// and `prepare` both stop on it.
    #[must_use]
    pub fn attribution_exceeds_holdings(&self) -> bool {
        self.health_reason
            .as_deref()
            .is_some_and(|reason| reason.contains(ATTRIBUTION_EXCEEDS_HOLDINGS))
    }

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
        if last_trade_at
            .is_some_and(|value| !Self::last_trade_is_plausible(value, balance_observed_at))
        {
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
            hype_transferred_out: None,
            hype_transferred_to_custodian: None,
            hype_eligible_for_transfer: None,
            custodian_staking: None,
        })
    }

    /// Records how much bot-acquired HYPE left the account by explained
    /// movements. Reported beside `hype_balance`, never folded into it.
    ///
    /// # Errors
    ///
    /// Returns [`StatusError`] when the amount is not finite or is negative.
    pub fn with_hype_transferred_out(mut self, hype: f64) -> Result<Self, StatusError> {
        finite_non_negative("hype_transferred_out", hype)?;
        self.hype_transferred_out = Some(hype);
        Ok(self)
    }

    #[must_use]
    pub const fn hype_transferred_out(&self) -> Option<f64> {
        self.hype_transferred_out
    }

    /// Records the staking-custodian view of the ledger (bot-strategy#847):
    /// how much of the transferred-out HYPE went to the custodian and how
    /// much may be transferred next. Requires `with_hype_transferred_out`
    /// first, and the custodian part may not exceed the whole.
    ///
    /// # Errors
    ///
    /// Returns [`StatusError`] for a non-finite or negative amount, or a
    /// custodian part larger than the recorded transferred-out total.
    pub fn with_custodian_attribution(
        mut self,
        transferred_to_custodian: f64,
        eligible_for_transfer: f64,
    ) -> Result<Self, StatusError> {
        finite_non_negative("hype_transferred_to_custodian", transferred_to_custodian)?;
        finite_non_negative("hype_eligible_for_transfer", eligible_for_transfer)?;
        let transferred_out = self
            .hype_transferred_out
            .ok_or(StatusError::CustodianWithoutTransferredOut)?;
        if transferred_to_custodian > transferred_out + 1e-8_f64.max(transferred_out * 1e-10) {
            return Err(StatusError::CustodianExceedsTransferredOut);
        }
        self.hype_transferred_to_custodian = Some(transferred_to_custodian);
        self.hype_eligible_for_transfer = Some(eligible_for_transfer);
        Ok(self)
    }

    /// Records the custodian's own staking balances (bot-strategy#847).
    ///
    /// # Errors
    ///
    /// Returns [`StatusError`] for a non-finite or negative amount.
    pub fn with_custodian_staking(
        mut self,
        staking: CustodianStakingStatus,
    ) -> Result<Self, StatusError> {
        finite_non_negative("custodian delegated_hype", staking.delegated_hype)?;
        finite_non_negative("custodian undelegated_hype", staking.undelegated_hype)?;
        finite_non_negative(
            "custodian pending_withdrawal_hype",
            staking.pending_withdrawal_hype,
        )?;
        if let Some(shortfall) = staking.shortfall_hype {
            finite_non_negative("custodian shortfall_hype", shortfall)?;
        }
        self.custodian_staking = Some(staking);
        Ok(self)
    }

    #[must_use]
    pub const fn hype_transferred_to_custodian(&self) -> Option<f64> {
        self.hype_transferred_to_custodian
    }

    #[must_use]
    pub const fn hype_eligible_for_transfer(&self) -> Option<f64> {
        self.hype_eligible_for_transfer
    }

    #[must_use]
    pub const fn custodian_staking(&self) -> Option<CustodianStakingStatus> {
        self.custodian_staking
    }

    /// Whether this observation reports a transfer to the custodian that its
    /// staking balance does not cover (bot-strategy#847).
    #[must_use]
    pub fn custodian_staking_shortfall(&self) -> bool {
        self.health_reason
            .as_deref()
            .is_some_and(|reason| reason.contains(CUSTODIAN_STAKING_SHORTFALL))
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
