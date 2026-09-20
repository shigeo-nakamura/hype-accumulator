use crate::{
    config::UtcSchedule,
    hype_asset::HYPE_SPOT_MARKET,
    status::{AccumulatorStatus, CustodianStakingStatus, StatusError, CUSTODIAN_STAKING_SHORTFALL},
};
use chrono::{DateTime, Utc};
use dex_connector::{
    DexConnector, HyperliquidAccountConfig, HyperliquidAccountMovement, HyperliquidConnector,
    HyperliquidConnectorConfig,
};
use reqwest::Client;
use rust_decimal::{prelude::ToPrimitive, Decimal};
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::{json, Value};
use std::{path::PathBuf, str::FromStr, time::Duration};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq)]
pub struct BalanceObservation {
    pub spot_usdc: f64,
    pub spot_hype: f64,
    pub hype_price_usdc: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StakingObservation {
    pub delegated_hype: f64,
    pub undelegated_hype: f64,
    pub pending_withdrawal_hype: f64,
    pub delegation_rows_hype: f64,
}

pub use crate::status::ATTRIBUTION_EXCEEDS_HOLDINGS;

/// Authoritative accumulator-ledger attribution for account-level observations.
///
/// Account balances and fills alone cannot distinguish accumulator activity
/// from direct transfers, pre-existing holdings, or manual staking actions.
#[derive(Clone, Debug, PartialEq)]
pub enum HypeAttribution {
    Unavailable,
    Reconciled {
        /// Bot-acquired HYPE the ledger says the account still holds.
        hype: f64,
        last_trade_at: Option<DateTime<Utc>>,
        /// Bot-acquired HYPE that left the account by a movement the venue's
        /// ledger records (bot-strategy#929 slice C); reported, not held.
        transferred_out_hype: f64,
        /// The staking-custodian view (bot-strategy#847), present only when
        /// the policy names a custodian.
        custodian: Option<CustodianAttribution>,
    },
}

/// What the ledger says about the staking custodian (bot-strategy#847).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CustodianAttribution {
    /// The part of `transferred_out_hype` whose destination was the
    /// custodian.
    pub transferred_hype: f64,
    /// Bot-acquired HYPE still held less the policy's residual buffer: the
    /// amount the owner may transfer next. Advisory.
    pub eligible_for_transfer_hype: f64,
}

/// The custodian's own staking balances, read from the public info endpoint
/// for the custodian account (bot-strategy#847). An upper bound on bot HYPE
/// staked there — the custodian commingles other holdings — never an exact
/// attribution.
#[derive(Clone, Debug, PartialEq)]
pub enum CustodianObservation {
    /// The policy names no custodian; nothing was read.
    NotConfigured,
    /// The read failed; the status degrades instead of going stale.
    Unavailable,
    Observed {
        delegated_hype: f64,
        undelegated_hype: f64,
        pending_withdrawal_hype: f64,
    },
}

/// One venue read of the account, taken within a closed window and not yet
/// reconciled against an attribution.
#[derive(Clone, Debug, PartialEq)]
pub struct AccountObservation {
    pub balances: BalanceObservation,
    pub staking: StakingObservation,
    pub custodian: CustodianObservation,
    pub balance_observation_started_at: DateTime<Utc>,
    pub balance_observed_at: DateTime<Utc>,
}

impl AccountObservation {
    /// Produces the fail-closed dashboard status block for this read under
    /// `attribution`.
    ///
    /// # Errors
    ///
    /// Returns [`MonitorError`] when reconciliation fails.
    pub fn reconcile(
        &self,
        attribution: &HypeAttribution,
        trade_cadence: impl Into<String>,
    ) -> Result<AccumulatorStatus, MonitorError> {
        reconcile_status_with_balance_window(
            &self.balances,
            &self.staking,
            &self.custodian,
            attribution,
            self.balance_observation_started_at,
            self.balance_observed_at,
            trade_cadence,
        )
    }
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
    connector: HyperliquidConnector,
    client: Client,
    info_url: String,
    account: String,
    /// The staking custodian whose public staking balances are read beside
    /// the execution account's (bot-strategy#847), canonical lowercased.
    custodian: Option<String>,
}

impl HyperliquidObserver {
    /// The execution account this observer watches, in the exact canonical
    /// form the connector holds it in.
    ///
    /// Anything that must agree with a value the connector derived from the
    /// account — the execution-identity hash bound into every workflow
    /// journal, in particular — has to start from this string, not from the
    /// configured value: the connector canonicalizes on construction, and a
    /// checksummed (mixed-case) address in the environment would otherwise
    /// hash to a different identity than the one the journals carry.
    ///
    /// # Errors
    ///
    /// Returns [`MonitorError::Connector`] if the connector has no account,
    /// which `new` makes impossible.
    pub fn execution_account(&self) -> Result<&str, MonitorError> {
        self.connector
            .execution_account_address()
            .map_err(|error| MonitorError::Connector(error.to_string()))
    }

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
        let connector = HyperliquidConnector::new(HyperliquidConnectorConfig {
            base_url: base_url.clone(),
            tracked_symbols: vec![HYPE_SPOT_MARKET.to_owned()],
        })
        .and_then(|connector| {
            connector.with_account(HyperliquidAccountConfig {
                account_address: account.clone(),
                signer_private_key: None,
                vault_address: None,
                is_mainnet,
                nonce_state_path: None::<PathBuf>,
                max_taker_notional: None,
                max_taker_slippage_bps: None,
                max_taker_book_age_ms: 0,
            })
        })
        .map_err(|error| MonitorError::Connector(error.to_string()))?;
        Ok(Self {
            connector,
            client,
            info_url: format!("{base_url}/info"),
            account,
            custodian: None,
        })
    }

    /// Also reads the staking custodian's public staking balances on every
    /// account observation (bot-strategy#847). Read-only: the custodian's
    /// signer is never involved, and this observer never holds one.
    ///
    /// # Errors
    ///
    /// Returns [`MonitorError::InvalidAccount`] for a malformed custodian or
    /// one equal to the execution account, which could not be a custodian.
    pub fn with_custodian(mut self, custodian: Option<&str>) -> Result<Self, MonitorError> {
        self.custodian = match custodian {
            Some(custodian) => {
                let custodian = canonical_account(custodian)?;
                if custodian == self.account {
                    return Err(MonitorError::InvalidAccount);
                }
                Some(custodian)
            }
            None => None,
        };
        Ok(self)
    }

    /// Reads normalized authoritative account movements without constructing
    /// a signer. The caller supplies a closed millisecond range and persists
    /// its own overlap cursor for idempotent replay.
    ///
    /// # Errors
    ///
    /// Returns [`MonitorError::Connector`] when the read-only history query or
    /// normalization fails.
    pub async fn account_movements(
        &self,
        start_time_ms: u64,
        end_time_ms: u64,
    ) -> Result<Vec<HyperliquidAccountMovement>, MonitorError> {
        self.connector
            .get_account_movements(start_time_ms, Some(end_time_ms))
            .await
            .map_err(|error| MonitorError::Connector(error.to_string()))
    }

    /// Reads spot balances, current HYPE mark, staking summary, and staking
    /// delegations, then produces a fail-closed dashboard status block.
    ///
    /// # Errors
    ///
    /// Returns [`MonitorError`] when any required read or reconciliation fails.
    pub async fn observe(
        &self,
        attribution: &HypeAttribution,
        trade_cadence: impl Into<String>,
    ) -> Result<AccumulatorStatus, MonitorError> {
        let observation = self.observe_account().await?;
        observation.reconcile(attribution, trade_cadence)
    }

    /// Reads spot balances, current HYPE mark, staking summary, and staking
    /// delegations, without reconciling them against an attribution yet.
    ///
    /// Split from [`Self::observe`] so a caller can take the account read
    /// first and decide the attribution afterwards — the recurring cycle
    /// nets HYPE movements from the same scan it is about to commit before
    /// it judges divergence (bot-strategy#929 slice C) — while the balance
    /// window the status reports stays the one this read took.
    ///
    /// # Errors
    ///
    /// Returns [`MonitorError`] when any required read fails.
    pub async fn observe_account(&self) -> Result<AccountObservation, MonitorError> {
        let balance_observation_started_at = Utc::now();
        let combined = self
            .connector
            .get_combined_balance()
            .await
            .map_err(|error| MonitorError::Connector(error.to_string()))?;
        let balance_observed_at = Utc::now();
        let ticker = self
            .connector
            .get_ticker(HYPE_SPOT_MARKET, None)
            .await
            .map_err(|error| MonitorError::Connector(error.to_string()))?;
        let summary: DelegatorSummaryWire = self
            .post_info(json!({"type": "delegatorSummary", "user": self.account}))
            .await?;
        let delegations: Vec<DelegationWire> = self
            .post_info(json!({"type": "delegations", "user": self.account}))
            .await?;

        let balances = BalanceObservation {
            spot_usdc: spot_total(&combined.spot_assets, "USDC")?,
            spot_hype: spot_total(&combined.spot_assets, "HYPE")?,
            hype_price_usdc: decimal_to_f64(ticker.price, "HYPE mark")?,
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
        let custodian = match &self.custodian {
            None => CustodianObservation::NotConfigured,
            Some(custodian) => self.observe_custodian(custodian).await,
        };
        Ok(AccountObservation {
            balances,
            staking,
            custodian,
            balance_observation_started_at,
            balance_observed_at,
        })
    }

    /// The custodian's `delegatorSummary`, degraded rather than failed: the
    /// execution account's own read is the one this cycle must not lose,
    /// and a custodian figure is advisory (bot-strategy#847).
    async fn observe_custodian(&self, custodian: &str) -> CustodianObservation {
        let Ok(summary) = self
            .post_info::<DelegatorSummaryWire>(
                json!({"type": "delegatorSummary", "user": custodian}),
            )
            .await
        else {
            return CustodianObservation::Unavailable;
        };
        match (
            parse_amount(&summary.delegated, "custodian delegated HYPE"),
            parse_amount(&summary.undelegated, "custodian undelegated HYPE"),
            parse_amount(
                &summary.total_pending_withdrawal,
                "custodian pending-withdrawal HYPE",
            ),
        ) {
            (Ok(delegated_hype), Ok(undelegated_hype), Ok(pending_withdrawal_hype)) => {
                CustodianObservation::Observed {
                    delegated_hype,
                    undelegated_hype,
                    pending_withdrawal_hype,
                }
            }
            _ => CustodianObservation::Unavailable,
        }
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

/// Reconciles account observations with authoritative accumulator attribution.
/// Unattributed account HYPE is excluded, and unavailable attribution produces
/// a fresh degraded status with zero reported HYPE instead of silently claiming
/// pre-existing, transferred, or manually staked holdings.
///
/// # Errors
///
/// Returns [`MonitorError`] when any amount is non-finite/negative or the
/// resulting dashboard status violates its timestamp/value invariants.
pub fn reconcile_status(
    balances: &BalanceObservation,
    staking: &StakingObservation,
    attribution: &HypeAttribution,
    observed_at: DateTime<Utc>,
    trade_cadence: impl Into<String>,
) -> Result<AccumulatorStatus, MonitorError> {
    reconcile_status_with_balance_window(
        balances,
        staking,
        &CustodianObservation::NotConfigured,
        attribution,
        observed_at,
        observed_at,
        trade_cadence,
    )
}

/// [`reconcile_status`] with the staking custodian's read
/// (bot-strategy#847).
///
/// # Errors
///
/// As [`reconcile_status`].
pub fn reconcile_status_with_custodian(
    balances: &BalanceObservation,
    staking: &StakingObservation,
    custodian: &CustodianObservation,
    attribution: &HypeAttribution,
    observed_at: DateTime<Utc>,
    trade_cadence: impl Into<String>,
) -> Result<AccumulatorStatus, MonitorError> {
    reconcile_status_with_balance_window(
        balances,
        staking,
        custodian,
        attribution,
        observed_at,
        observed_at,
        trade_cadence,
    )
}

fn reconcile_status_with_balance_window(
    balances: &BalanceObservation,
    staking: &StakingObservation,
    custodian: &CustodianObservation,
    attribution: &HypeAttribution,
    balance_observation_started_at: DateTime<Utc>,
    balance_observed_at: DateTime<Utc>,
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
    let observed_hype = balances.spot_hype
        + staking.delegated_hype
        + staking.undelegated_hype
        + staking.pending_withdrawal_hype;
    let attribution_tolerance = 1e-8_f64.max(observed_hype.abs() * 1e-10);
    let delegation_tolerance = 1e-8_f64.max(staking.delegated_hype.abs() * 1e-10);
    let mismatch = (staking.delegated_hype - staking.delegation_rows_hype).abs();
    let mut health_reasons = Vec::new();
    if mismatch > delegation_tolerance {
        health_reasons.push("staking delegation total does not match delegator summary");
    }
    let (attributed_hype, last_trade_at, transferred_out_hype, custodian_attribution) =
        match attribution {
            HypeAttribution::Unavailable => {
                health_reasons.push("HYPE attribution unavailable; account holdings excluded");
                (0.0, None, None, None)
            }
            HypeAttribution::Reconciled {
                hype,
                last_trade_at,
                transferred_out_hype,
                custodian: custodian_attribution,
            } => {
                finite_nonnegative("attributed HYPE", *hype)?;
                finite_nonnegative("transferred-out HYPE", *transferred_out_hype)?;
                validate_custodian_attribution(
                    custodian_attribution.as_ref(),
                    *transferred_out_hype,
                    attribution_tolerance,
                )?;
                if *hype > observed_hype + attribution_tolerance {
                    // The workflow ledger says this account should still hold
                    // more bot-owned HYPE than it does: HYPE the bot acquired has
                    // left the account (an external sale, a transfer, or a
                    // staking movement no workflow recorded).
                    //
                    // Deliberately a health failure rather than an error
                    // (bot-strategy#929): refusing to produce a status document
                    // would take the dashboard down — and stall the recurring
                    // cycle that publishes it — in exactly the situation that
                    // most needs to be visible. What is reported as bot-owned is
                    // zero, the same as `Unavailable`: the ledger's claim would
                    // overstate, and the account total includes whatever else
                    // the account holds, which `hype_balance` must never
                    // include. The reason carries the fact; the number does not
                    // guess.
                    health_reasons.push(ATTRIBUTION_EXCEEDS_HOLDINGS);
                    // The eligible figure is derived from a claim this read just
                    // refused; publishing it would invite a transfer of HYPE the
                    // account may not hold. The transferred figure is a record
                    // of the past and stays.
                    (
                        0.0,
                        *last_trade_at,
                        Some(*transferred_out_hype),
                        custodian_attribution.map(|custodian| CustodianAttribution {
                            eligible_for_transfer_hype: 0.0,
                            ..custodian
                        }),
                    )
                } else {
                    if observed_hype - *hype > attribution_tolerance {
                        health_reasons.push("unattributed HYPE account holdings excluded");
                    }
                    (
                        *hype,
                        *last_trade_at,
                        Some(*transferred_out_hype),
                        *custodian_attribution,
                    )
                }
            }
        };
    let custodian_staking =
        custodian_staking_status(custodian, custodian_attribution, &mut health_reasons)?;
    // A last-trade timestamp after the balance read is rejected by
    // `AccumulatorStatus`, and now that a real journal-derived fill time is
    // attributed (bot-strategy#929) a clock that steps backwards — an NTP
    // correction, a restored backup — would make that rejection abort the
    // whole observation and stop the status document being written at all.
    // Same judgment as the divergence branch above: degrade loudly, keep
    // publishing.
    let last_trade_at = last_trade_at.filter(|value| {
        let plausible = AccumulatorStatus::last_trade_is_plausible(*value, balance_observed_at);
        if !plausible {
            health_reasons.push(
                "last attributed fill is after the balance observation; clock or history is \
                 inconsistent",
            );
        }
        plausible
    });
    let health_reason = (!health_reasons.is_empty()).then(|| health_reasons.join("; "));
    let status = AccumulatorStatus::new_with_balance_window(
        balances.spot_usdc,
        attributed_hype,
        balances.hype_price_usdc,
        balance_observation_started_at,
        balance_observed_at,
        last_trade_at,
        trade_cadence,
        health_reason,
    )?;
    with_transfer_figures(
        status,
        transferred_out_hype,
        custodian_attribution,
        custodian_staking,
    )
}

/// Attaches the transfer and custodian figures (each optional) to a
/// validated status.
fn with_transfer_figures(
    status: AccumulatorStatus,
    transferred_out_hype: Option<f64>,
    custodian_attribution: Option<CustodianAttribution>,
    custodian_staking: Option<CustodianStakingStatus>,
) -> Result<AccumulatorStatus, MonitorError> {
    let status = match transferred_out_hype {
        Some(hype) => status.with_hype_transferred_out(hype)?,
        None => status,
    };
    let status = match custodian_attribution {
        Some(custodian) => status.with_custodian_attribution(
            custodian.transferred_hype,
            custodian.eligible_for_transfer_hype,
        )?,
        None => status,
    };
    Ok(match custodian_staking {
        Some(staking) => status.with_custodian_staking(staking)?,
        None => status,
    })
}

/// The custodian view of an attribution is a part of its transferred-out
/// total (bot-strategy#847); anything else is not an attribution this
/// reconciliation will publish.
fn validate_custodian_attribution(
    custodian: Option<&CustodianAttribution>,
    transferred_out_hype: f64,
    tolerance: f64,
) -> Result<(), MonitorError> {
    let Some(custodian) = custodian else {
        return Ok(());
    };
    finite_nonnegative("transferred-to-custodian HYPE", custodian.transferred_hype)?;
    finite_nonnegative(
        "eligible-for-transfer HYPE",
        custodian.eligible_for_transfer_hype,
    )?;
    if custodian.transferred_hype > transferred_out_hype + tolerance {
        return Err(MonitorError::InvalidResponse(
            "transferred-to-custodian HYPE exceeds transferred-out HYPE".to_owned(),
        ));
    }
    Ok(())
}

/// The custodian's staking balances bound what the owner staked there from
/// above (they commingle other HYPE), so the only thing this can say is that
/// a transfer has *not* been staked yet: transferred more than the custodian
/// holds in staking at all (bot-strategy#847). Alert only — no capital
/// decision depends on it.
fn custodian_staking_status(
    custodian: &CustodianObservation,
    custodian_attribution: Option<CustodianAttribution>,
    health_reasons: &mut Vec<&'static str>,
) -> Result<Option<CustodianStakingStatus>, MonitorError> {
    Ok(match custodian {
        CustodianObservation::NotConfigured => None,
        CustodianObservation::Unavailable => {
            health_reasons.push("staking custodian read unavailable");
            None
        }
        CustodianObservation::Observed {
            delegated_hype,
            undelegated_hype,
            pending_withdrawal_hype,
        } => {
            for (label, value) in [
                ("custodian delegated HYPE", *delegated_hype),
                ("custodian undelegated HYPE", *undelegated_hype),
                (
                    "custodian pending-withdrawal HYPE",
                    *pending_withdrawal_hype,
                ),
            ] {
                finite_nonnegative(label, value)?;
            }
            let staked_hype = delegated_hype + undelegated_hype + pending_withdrawal_hype;
            let shortfall_hype = custodian_attribution.map(|custodian| {
                let shortfall = custodian.transferred_hype - staked_hype;
                let tolerance = 1e-8_f64.max(custodian.transferred_hype.abs() * 1e-10);
                if shortfall > tolerance {
                    health_reasons.push(CUSTODIAN_STAKING_SHORTFALL);
                    shortfall
                } else {
                    0.0
                }
            });
            Some(CustodianStakingStatus {
                delegated_hype: *delegated_hype,
                undelegated_hype: *undelegated_hype,
                pending_withdrawal_hype: *pending_withdrawal_hype,
                shortfall_hype,
            })
        }
    })
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
