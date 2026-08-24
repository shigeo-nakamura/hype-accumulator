use crate::{
    config::UtcSchedule,
    status::{AccumulatorStatus, StatusError},
};
use chrono::{DateTime, Utc};
use dex_connector::{
    create_hyperliquid_account_connector, DexConnector, HyperliquidAccountConfig,
    HyperliquidConnectorConfig,
};
use reqwest::Client;
use rust_decimal::{prelude::ToPrimitive, Decimal};
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::{json, Value};
use std::{path::PathBuf, str::FromStr, time::Duration};
use thiserror::Error;

const HYPE_SPOT_SYMBOL: &str = "HYPE/USDC";

#[derive(Clone, Debug, PartialEq)]
pub struct BalanceObservation {
    pub spot_usdc: f64,
    pub spot_hype: f64,
    pub hype_price_usdc: f64,
    pub last_trade_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StakingObservation {
    pub delegated_hype: f64,
    pub undelegated_hype: f64,
    pub pending_withdrawal_hype: f64,
    pub delegation_rows_hype: f64,
}

#[derive(Debug, Error)]
pub enum MonitorError {
    #[error("invalid observation account")]
    InvalidAccount,
    #[error("Hyperliquid account read failed: {0}")]
    Connector(String),
    #[error("Hyperliquid info read failed: {0}")]
    Http(String),
    #[error("invalid Hyperliquid response: {0}")]
    InvalidResponse(String),
    #[error(transparent)]
    Status(#[from] StatusError),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DelegatorSummaryWire {
    delegated: String,
    undelegated: String,
    total_pending_withdrawal: String,
}

#[derive(Debug, Deserialize)]
struct DelegationWire {
    amount: String,
}

pub struct HyperliquidObserver {
    connector: Box<dyn DexConnector>,
    client: Client,
    info_url: String,
    account: String,
}

impl HyperliquidObserver {
    /// Creates a read-only observer. No signer or nonce state is constructed.
    ///
    /// # Errors
    ///
    /// Returns [`MonitorError`] for malformed account identity, HTTP client
    /// construction failures, or connector configuration failures.
    pub fn new(base_url: &str, account: &str) -> Result<Self, MonitorError> {
        let account = canonical_account(account)?;
        let base_url = base_url.trim_end_matches('/').to_owned();
        let is_mainnet = !base_url.to_ascii_lowercase().contains("testnet");
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(15))
            .build()
            .map_err(|error| MonitorError::Http(error.to_string()))?;
        let connector = create_hyperliquid_account_connector(
            HyperliquidConnectorConfig {
                base_url: base_url.clone(),
                tracked_symbols: vec![HYPE_SPOT_SYMBOL.to_owned()],
            },
            HyperliquidAccountConfig {
                account_address: account.clone(),
                signer_private_key: None,
                vault_address: None,
                is_mainnet,
                nonce_state_path: None::<PathBuf>,
                max_taker_notional: None,
                max_taker_slippage_bps: None,
                max_taker_book_age_ms: 0,
            },
        )
        .map_err(|error| MonitorError::Connector(error.to_string()))?;
        Ok(Self {
            connector,
            client,
            info_url: format!("{base_url}/info"),
            account,
        })
    }

    /// Reads spot balances, current HYPE mark, fills, staking summary, and
    /// staking delegations, then produces the dashboard-safe status block.
    ///
    /// # Errors
    ///
    /// Returns [`MonitorError`] when any required read or reconciliation fails.
    pub async fn observe(
        &self,
        trade_cadence: impl Into<String>,
    ) -> Result<AccumulatorStatus, MonitorError> {
        let combined = self
            .connector
            .get_combined_balance()
            .await
            .map_err(|error| MonitorError::Connector(error.to_string()))?;
        let ticker = self
            .connector
            .get_ticker(HYPE_SPOT_SYMBOL, None)
            .await
            .map_err(|error| MonitorError::Connector(error.to_string()))?;
        let fills = self
            .connector
            .get_filled_orders(HYPE_SPOT_SYMBOL)
            .await
            .map_err(|error| MonitorError::Connector(error.to_string()))?;
        let summary: DelegatorSummaryWire = self
            .post_info(json!({"type": "delegatorSummary", "user": self.account}))
            .await?;
        let delegations: Vec<DelegationWire> = self
            .post_info(json!({"type": "delegations", "user": self.account}))
            .await?;

        let last_trade_at = fills
            .orders
            .iter()
            .filter(|fill| !fill.is_rejected)
            .filter_map(|fill| fill.filled_ts_ms)
            .max()
            .map(|timestamp| {
                DateTime::from_timestamp_millis(timestamp).ok_or_else(|| {
                    MonitorError::InvalidResponse("fill timestamp is out of range".to_owned())
                })
            })
            .transpose()?;
        let balances = BalanceObservation {
            spot_usdc: spot_total(&combined.spot_assets, "USDC")?,
            spot_hype: spot_total(&combined.spot_assets, "HYPE")?,
            hype_price_usdc: decimal_to_f64(ticker.price, "HYPE mark")?,
            last_trade_at,
        };
        let staking = StakingObservation {
            delegated_hype: parse_amount(&summary.delegated, "delegated HYPE")?,
            undelegated_hype: parse_amount(&summary.undelegated, "undelegated HYPE")?,
            pending_withdrawal_hype: parse_amount(
                &summary.total_pending_withdrawal,
                "pending-withdrawal HYPE",
            )?,
            delegation_rows_hype: delegations.iter().try_fold(
                0.0,
                |total, row| -> Result<f64, MonitorError> {
                    Ok(total + parse_amount(&row.amount, "delegation HYPE")?)
                },
            )?,
        };
        reconcile_status(&balances, &staking, Utc::now(), trade_cadence)
    }

    async fn post_info<T: DeserializeOwned>(&self, body: Value) -> Result<T, MonitorError> {
        self.client
            .post(&self.info_url)
            .json(&body)
            .send()
            .await
            .map_err(|error| MonitorError::Http(error.to_string()))?
            .error_for_status()
            .map_err(|error| MonitorError::Http(error.to_string()))?
            .json()
            .await
            .map_err(|error| MonitorError::InvalidResponse(error.to_string()))
    }
}

/// Reconciles spot and staking observations into one dashboard balance.
/// Delegation row mismatches remain visible as a fresh degraded status.
///
/// # Errors
///
/// Returns [`MonitorError`] when any amount is non-finite/negative or the
/// resulting dashboard status violates its timestamp/value invariants.
pub fn reconcile_status(
    balances: &BalanceObservation,
    staking: &StakingObservation,
    observed_at: DateTime<Utc>,
    trade_cadence: impl Into<String>,
) -> Result<AccumulatorStatus, MonitorError> {
    for (label, value) in [
        ("spot USDC", balances.spot_usdc),
        ("spot HYPE", balances.spot_hype),
        ("delegated HYPE", staking.delegated_hype),
        ("undelegated HYPE", staking.undelegated_hype),
        ("pending-withdrawal HYPE", staking.pending_withdrawal_hype),
        ("delegation row HYPE", staking.delegation_rows_hype),
    ] {
        finite_nonnegative(label, value)?;
    }
    let mismatch = (staking.delegated_hype - staking.delegation_rows_hype).abs();
    let tolerance = 1e-8_f64.max(staking.delegated_hype.abs() * 1e-10);
    let health_reason = (mismatch > tolerance)
        .then(|| "staking delegation total does not match delegator summary".to_owned());
    let total_hype = balances.spot_hype
        + staking.delegated_hype
        + staking.undelegated_hype
        + staking.pending_withdrawal_hype;
    AccumulatorStatus::new(
        balances.spot_usdc,
        total_hype,
        balances.hype_price_usdc,
        observed_at,
        balances.last_trade_at,
        trade_cadence,
        health_reason,
    )
    .map_err(MonitorError::from)
}

#[must_use]
pub fn trade_cadence_label(schedule: &UtcSchedule) -> String {
    const DAYS: [&str; 7] = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
    let mut weekdays = schedule.weekdays.clone();
    weekdays.sort_unstable();
    weekdays.dedup();
    let frequency = if weekdays == vec![1, 2, 3, 4, 5, 6, 7] {
        "Daily".to_owned()
    } else {
        weekdays
            .into_iter()
            .filter_map(|day| DAYS.get(usize::from(day.saturating_sub(1))))
            .copied()
            .collect::<Vec<_>>()
            .join("/")
    };
    format!(
        "{frequency} at {:02}:{:02} UTC",
        schedule.utc_hour, schedule.utc_minute
    )
}

fn canonical_account(value: &str) -> Result<String, MonitorError> {
    let value = value.trim();
    if value.len() != 42
        || !value.starts_with("0x")
        || !value[2..]
            .chars()
            .all(|character| character.is_ascii_hexdigit())
    {
        return Err(MonitorError::InvalidAccount);
    }
    Ok(value.to_ascii_lowercase())
}

fn spot_total(
    balances: &[dex_connector::SpotAssetBalance],
    symbol: &str,
) -> Result<f64, MonitorError> {
    let total = balances
        .iter()
        .filter(|balance| balance.symbol.eq_ignore_ascii_case(symbol))
        .map(|balance| balance.balance)
        .sum();
    decimal_to_f64(total, "spot balance")
}

fn parse_amount(value: &str, label: &str) -> Result<f64, MonitorError> {
    let decimal = Decimal::from_str(value)
        .map_err(|_| MonitorError::InvalidResponse(format!("{label} is not a decimal number")))?;
    decimal_to_f64(decimal, label)
}

fn decimal_to_f64(value: Decimal, label: &str) -> Result<f64, MonitorError> {
    value
        .to_f64()
        .filter(|value| value.is_finite())
        .ok_or_else(|| MonitorError::InvalidResponse(format!("{label} is not finite")))
}

fn finite_nonnegative(label: &str, value: f64) -> Result<(), MonitorError> {
    if value.is_finite() && value >= 0.0 {
        Ok(())
    } else {
        Err(MonitorError::InvalidResponse(format!(
            "{label} must be finite and nonnegative"
        )))
    }
}
