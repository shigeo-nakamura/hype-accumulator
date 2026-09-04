//! One-shot, operator-supervised HYPE/USDC spot live-probe binary.
//!
//! Two subcommands split "compute and durably prepare an order" from
//! "actually submit it", so an operator can review the exact parameters
//! (quantity, price, expiry, signer/execution identity) before any signed
//! action reaches the venue:
//!
//! - `prepare`: opens the signer-free runtime, computes today's pacing
//!   decision if one is due, assembles the order envelope from live market
//!   state, and durably prepares the order in a `DurableWorkflow` journal.
//!   Prints the exact prepared order and exits. **Submits nothing.**
//! - `submit`: re-opens the same durably prepared workflow (never
//!   recomputing it — see `live_decision.rs`'s crash-safety design) and
//!   submits the exact order `prepare` printed, then reconciles the fill.
//!   Requires `--confirm <client_order_id>` to match the prepared order's
//!   own client order ID exactly, so an operator must have actually read
//!   `prepare`'s output before this proceeds.
//!
//! This binary only supports an account's first-ever live economic action
//! (see `live_decision.rs`'s module doc for why). It is feature-gated
//! behind `live-probe` and is not built by default.

use chrono::Utc;
use dex_connector::{HyperliquidAccountConfig, HyperliquidConnector, HyperliquidConnectorConfig};
use hype_accumulator::{
    config::{Config, ProcessEnvironment},
    live_decision::prepare_first_live_order_workflow,
    live_probe::{HyperliquidLiveProbe, LiveProbeBinding},
    monitor::{trade_cadence_label, HypeAttribution, HyperliquidObserver},
    order_envelope::OrderEnvelopeFreshnessPolicy,
    pacing::PacingLimits,
    runtime::{AdmissionApprovals, RuntimeConfig, RuntimeCycleInput, SignerFreeRuntime},
    signal::SignalSnapshot,
    signer::resolve_signer_private_key,
    workflow::{
        DurableWorkflow, EligibilityPolicyBinding, ExchangeOrderOwnerStore,
        FileExchangeOrderOwnerStore, FileProtectedWorkflowHeadStore, HypeAtoms,
        ProtectedWorkflowHeadStore,
    },
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::{env, fs, path::PathBuf, process, str::FromStr, sync::Arc};

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("live probe rejected: {error}");
        process::exit(2);
    }
}

/// Operational parameters `SecurityPolicy` has no field for (see
/// `EffectiveLiveOrderPolicy`'s doc comment): venue/network selection and
/// taker execution bounds, plus this probe's own timing knobs. Read from a
/// dedicated file, not baked into the binary, so an operator can review
/// exact values before every invocation without a rebuild.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OperationalParams {
    is_mainnet: bool,
    max_taker_notional_usdc: String,
    max_taker_slippage_bps: u32,
    max_taker_book_age_ms: u64,
    order_timeout_seconds: u64,
    order_book_depth: usize,
}

impl OperationalParams {
    fn from_toml(input: &str) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(toml::from_str(input)?)
    }
}

/// Durably records exactly which venue endpoint and network `prepare`
/// resolved its quantity/price/inventory from, at the same path every
/// `submit` for this journal must independently re-derive and match.
///
/// Nothing in `DecisionBinding`/`OrderEnvelopeBinding`/`InventoryBaseline`
/// records network selection — `config.toml`'s `hyperliquid.endpoint` and
/// `operational.toml`'s `is_mainnet` are read completely independently by
/// each subcommand invocation. Without this check, an operator could
/// `prepare` against testnet, then edit either file to point at mainnet
/// before running `submit` for the *same already-fsynced journal*, and the
/// mainnet-priced submission would silently use testnet-derived quantity,
/// price, and inventory — never re-validated against the network it is
/// actually about to hit.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct NetworkBinding {
    endpoint: String,
    is_mainnet: bool,
}

impl NetworkBinding {
    fn path(journal_path: &str) -> PathBuf {
        let mut path = PathBuf::from(journal_path);
        path.set_extension("network-binding.json");
        path
    }

    fn resolved(config: &Config, operational: &OperationalParams) -> Self {
        Self {
            endpoint: config.hyperliquid.endpoint.clone(),
            is_mainnet: operational.is_mainnet,
        }
    }

    /// Durably writes this binding, refusing to silently overwrite a
    /// different one already recorded for this journal (that would defeat
    /// the entire point: it must be fixed at `prepare` time and never
    /// change underneath a later `submit`).
    fn write_once(&self, journal_path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let path = Self::path(journal_path);
        if let Ok(existing) = fs::read_to_string(&path) {
            let existing: Self = serde_json::from_str(&existing)?;
            if existing != *self {
                return Err(format!(
                    "network binding already recorded for this journal ({existing:?}) does not \
                     match this prepare attempt ({self:?}); a journal's network selection is \
                     fixed at first prepare and must never change"
                )
                .into());
            }
            return Ok(());
        }
        fs::write(&path, serde_json::to_string_pretty(self)?)?;
        Ok(())
    }

    fn verify(journal_path: &str, current: &Self) -> Result<(), Box<dyn std::error::Error>> {
        let path = Self::path(journal_path);
        let recorded: Self = serde_json::from_str(&fs::read_to_string(&path).map_err(|_| {
            format!(
                "no network binding recorded at {}; run `prepare` first",
                path.display()
            )
        })?)?;
        if recorded != *current {
            return Err(format!(
                "config.toml/operational.toml now resolve to {current:?}, but this journal was \
                 prepared against {recorded:?}; refusing to submit a testnet-derived (or \
                 otherwise different-network) order against a different network. Re-run \
                 `prepare` with the network you intend to submit to."
            )
            .into());
        }
        Ok(())
    }
}

const USAGE: &str = "usage:\n  hype-live-probe prepare <config.toml> <security-policy.toml> \
     <runtime-config.toml> <operational.toml> <journal.jsonl>\n  hype-live-probe submit \
     <config.toml> <security-policy.toml> <operational.toml> <journal.jsonl> --confirm \
     <client_order_id>";

#[derive(Debug, Eq, PartialEq)]
enum Invocation {
    Prepare {
        config_path: String,
        security_policy_path: String,
        runtime_config_path: String,
        operational_params_path: String,
        journal_path: String,
    },
    Submit {
        config_path: String,
        security_policy_path: String,
        operational_params_path: String,
        journal_path: String,
        confirm_client_order_id: String,
    },
}

fn invocation<I>(args: I) -> Result<Invocation, &'static str>
where
    I: Iterator<Item = String>,
{
    let args = args.collect::<Vec<_>>();
    match args.as_slice() {
        [command, config_path, security_policy_path, runtime_config_path, operational_params_path, journal_path]
            if command == "prepare" =>
        {
            Ok(Invocation::Prepare {
                config_path: config_path.clone(),
                security_policy_path: security_policy_path.clone(),
                runtime_config_path: runtime_config_path.clone(),
                operational_params_path: operational_params_path.clone(),
                journal_path: journal_path.clone(),
            })
        }
        [command, config_path, security_policy_path, operational_params_path, journal_path, confirm_flag, confirm_client_order_id]
            if command == "submit" && confirm_flag == "--confirm" =>
        {
            Ok(Invocation::Submit {
                config_path: config_path.clone(),
                security_policy_path: security_policy_path.clone(),
                operational_params_path: operational_params_path.clone(),
                journal_path: journal_path.clone(),
                confirm_client_order_id: confirm_client_order_id.clone(),
            })
        }
        _ => Err(USAGE),
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    match invocation(env::args().skip(1))? {
        Invocation::Prepare {
            config_path,
            security_policy_path,
            runtime_config_path,
            operational_params_path,
            journal_path,
        } => {
            prepare(
                &config_path,
                &security_policy_path,
                &runtime_config_path,
                &operational_params_path,
                &journal_path,
            )
            .await
        }
        Invocation::Submit {
            config_path,
            security_policy_path,
            operational_params_path,
            journal_path,
            confirm_client_order_id,
        } => {
            submit(
                &config_path,
                &security_policy_path,
                &operational_params_path,
                &journal_path,
                &confirm_client_order_id,
            )
            .await
        }
    }
}

async fn prepare(
    config_path: &str,
    security_policy_path: &str,
    runtime_config_path: &str,
    operational_params_path: &str,
    journal_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let now = Utc::now();
    let config = load_config(config_path, security_policy_path)?;
    config.validate_at(&ProcessEnvironment, now)?;
    let operational = OperationalParams::from_toml(&fs::read_to_string(operational_params_path)?)?;
    // Fixes the network this journal is bound to before anything else reads
    // `config`/`operational` for a network-dependent value: refuses to
    // silently re-bind an already-prepared journal to a different network on
    // a later `prepare` retry.
    NetworkBinding::resolved(&config, &operational).write_once(journal_path)?;

    // Decrypts the signer now, even though the signal-free
    // `SignerFreeRuntime::apply_cycle` below (inside
    // `prepare_first_live_order_workflow`) might still find no decision
    // due. This can't be deferred further: envelope assembly's nonce
    // reservation runs inside that same call, and splitting the signer-free
    // and signer-requiring halves of that flow is out of this binary's
    // scope (it would mean revisiting `live_decision.rs`, already merged).
    let connector = build_signed_connector(&config, &operational, journal_path).await?;

    // Re-reads the clock here, after the KMS-backed signer decrypt above
    // (a network round trip whose latency is outside this binary's control)
    // rather than reusing the `now` captured before it. Every freshness/
    // expiry computation below — policy acknowledgement validity, the
    // movement-scan window, signal evidence, and the order envelope's
    // `signed_expiry_at` — should be judged against a clock read as close as
    // practical to the decision it gates, not one that already has an
    // unbounded KMS round trip baked into its staleness.
    let now = Utc::now();
    let effective = config.effective_live_order_policy(&ProcessEnvironment, now)?;
    let policy_version = config.effective_security_policy_digest(&ProcessEnvironment)?;
    let envelope_policy = OrderEnvelopeFreshnessPolicy {
        max_venue_clock_lag_ms: effective.max_venue_clock_lag_ms,
        venue_clock_evidence_stale_after_seconds: effective
            .venue_clock_evidence_stale_after_seconds,
        book_stale_after_seconds: effective.book_stale_after_seconds,
        account_history_stale_after_seconds: effective.account_history_stale_after_seconds,
        fee_schedule_stale_after_seconds: effective.fee_schedule_stale_after_seconds,
        signal_stale_after_seconds: effective.signal_stale_after_seconds,
        order_timeout_seconds: operational.order_timeout_seconds,
        max_slippage_bps: effective.max_slippage_bps,
        order_book_depth: operational.order_book_depth,
    };
    let eligibility_policy = EligibilityPolicyBinding {
        policy_version,
        fill_registration_deadline_seconds: effective.fill_registration_deadline_seconds,
        lot_eligibility_max_age_seconds: effective.lot_eligibility_max_age_seconds,
    };
    let configured_residual_hype_atoms = HypeAtoms::from_atoms(effective.residual_hype_wei);

    let runtime_config = RuntimeConfig::from_toml(&fs::read_to_string(runtime_config_path)?)?;
    let limits = PacingLimits::from_config(&config)?;
    let mut runtime = SignerFreeRuntime::open(runtime_config.clone(), limits)?;

    let approvals = AdmissionApprovals::from_json(&fs::read_to_string(
        runtime_config.admission_approvals_path(),
    )?)?;
    let signal = match fs::read_to_string(runtime_config.signal_snapshot_path()) {
        Ok(payload) => SignalSnapshot::from_json(&payload).ok(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    let account = config.observation_account(&ProcessEnvironment)?;
    let observer = HyperliquidObserver::new(&config.hyperliquid.endpoint, &account)?;
    let accumulator = observer
        .observe(
            &HypeAttribution::Unavailable,
            trade_cadence_label(&config.schedule),
        )
        .await?;
    let scan_end_ms = u64::try_from(now.timestamp_millis())?;
    let scan_start_ms = runtime.next_scan_start_ms();
    let (movements, capital_history_complete, api_errors) =
        match observer.account_movements(scan_start_ms, scan_end_ms).await {
            Ok(movements) => (movements, true, 0),
            Err(_) => (Vec::new(), false, 1),
        };
    let cycle_input = RuntimeCycleInput {
        observed_at: now,
        scan_start_ms,
        scan_end_ms,
        movements: &movements,
        approvals: &approvals,
        signal: signal.as_ref(),
        accumulator,
        capital_history_complete,
        manual_pause: config.manual_halt,
        api_errors,
    };

    let (protected_head_store, owner_store) = build_stores(journal_path)?;
    let journal = PathBuf::from(journal_path);

    // `signal_evidence_valid_through_at` is not `policy_acknowledgement_valid_through_at`
    // (an unrelated quantity that happens to also be a `DateTime<Utc>`) — no
    // per-signal timestamp is available here, so this mirrors the same
    // judgment call `order_envelope.rs`'s `decision_valid_through_at` already
    // makes and was reviewed for (PR #27): treat the signal as fresh as of
    // `now` and bound it by the same `signal_stale_after_seconds` window.
    let signal_evidence_valid_through_at =
        now + chrono::TimeDelta::seconds(i64::try_from(effective.signal_stale_after_seconds)?);
    let workflow = prepare_first_live_order_workflow(
        &connector,
        &mut runtime,
        cycle_input,
        signal_evidence_valid_through_at,
        effective.policy_acknowledgement_valid_through_at,
        &envelope_policy,
        eligibility_policy,
        configured_residual_hype_atoms,
        &journal,
        protected_head_store,
        owner_store,
        now,
    )
    .await?;

    let action = workflow.pending_prepared_order()?;
    println!("mode=prepared journal={journal_path}");
    println!("{action:#?}");
    println!(
        "\nReview the values above carefully. To submit this exact order, run:\n  hype-live-probe submit {config_path} {security_policy_path} {operational_params_path} {journal_path} --confirm {}",
        workflow.state().client_order_id()
    );
    Ok(())
}

async fn submit(
    config_path: &str,
    security_policy_path: &str,
    operational_params_path: &str,
    journal_path: &str,
    confirm_client_order_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let now = Utc::now();
    let config = load_config(config_path, security_policy_path)?;
    config.validate_at(&ProcessEnvironment, now)?;
    let effective = config.effective_live_order_policy(&ProcessEnvironment, now)?;
    let operational = OperationalParams::from_toml(&fs::read_to_string(operational_params_path)?)?;
    // Refuses to submit if `config.toml`/`operational.toml` now resolve to a
    // different network than the one this journal was `prepare`d against —
    // see `NetworkBinding`'s doc comment for the exact danger this closes.
    NetworkBinding::verify(
        journal_path,
        &NetworkBinding::resolved(&config, &operational),
    )?;

    let binding = DurableWorkflow::peek_committed_binding(journal_path)?
        .ok_or("no prepared order found at this journal path; run `prepare` first")?;
    let (protected_head_store, owner_store) = build_stores(journal_path)?;
    let workflow =
        DurableWorkflow::open_or_create(journal_path, &binding, protected_head_store, owner_store)?;
    let action = workflow.pending_prepared_order()?;
    let client_order_id = workflow.state().client_order_id();
    if client_order_id != confirm_client_order_id {
        return Err(format!(
            "--confirm {confirm_client_order_id} does not match the prepared order's client_order_id {client_order_id}; re-run `prepare` and copy its exact confirmation command"
        )
        .into());
    }
    println!("mode=submitting journal={journal_path}");
    println!("{action:#?}");

    let connector = build_signed_connector(&config, &operational, journal_path).await?;
    // Must be the market-metadata digest `prepare` bound into the action
    // (`hype_asset::hype_usdc_market_metadata_digest`), NOT
    // `effective_security_policy_digest` — a different, policy-fingerprint
    // concept. Using the wrong one here would make every submission fail
    // `PreparedIocOrder::from_action`'s binding-match check.
    let probe_binding = LiveProbeBinding::from_connector(
        &connector,
        hype_accumulator::hype_asset::hype_usdc_market_metadata_digest(),
    )?;
    let probe =
        HyperliquidLiveProbe::new(connector, probe_binding, effective.max_purchase_fee_bps)?;

    let submission = probe.submit(&workflow, now).await?;
    println!("mode=submitted {submission:#?}");
    let reconciliation = probe.reconcile(&workflow).await?;
    println!("mode=reconciled {reconciliation:#?}");
    Ok(())
}

fn load_config(
    config_path: &str,
    security_policy_path: &str,
) -> Result<Config, Box<dyn std::error::Error>> {
    let runtime = fs::read_to_string(config_path)?;
    let policy = fs::read_to_string(security_policy_path)?;
    Ok(Config::from_toml_with_security_policy(&runtime, &policy)?)
}

type WorkflowStores = (
    Arc<dyn ProtectedWorkflowHeadStore>,
    Arc<dyn ExchangeOrderOwnerStore>,
);

fn build_stores(journal_path: &str) -> Result<WorkflowStores, Box<dyn std::error::Error>> {
    let mut head_path = PathBuf::from(journal_path);
    head_path.set_extension("protected-head.json");
    let protected_head_store: Arc<dyn ProtectedWorkflowHeadStore> =
        Arc::new(FileProtectedWorkflowHeadStore::new(head_path)?);
    // Deliberately outside the per-journal path: this store must be shared
    // across every workflow for this execution identity, never scoped to
    // one decision's journal (see `FileExchangeOrderOwnerStore`'s doc
    // comment and bot-strategy#845 PR #28's review note on this exact risk).
    let owner_store_path = PathBuf::from(journal_path)
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .join("exchange-order-owners.json");
    let owner_store: Arc<dyn ExchangeOrderOwnerStore> =
        Arc::new(FileExchangeOrderOwnerStore::new(owner_store_path)?);
    Ok((protected_head_store, owner_store))
}

fn box_error<E: std::error::Error + 'static>(error: E) -> Box<dyn std::error::Error> {
    Box::new(error)
}

async fn build_signed_connector(
    config: &Config,
    operational: &OperationalParams,
    journal_path: &str,
) -> Result<HyperliquidConnector, Box<dyn std::error::Error>> {
    let account_address = config.observation_account(&ProcessEnvironment)?;
    let signer_private_key =
        resolve_signer_private_key(&ProcessEnvironment, &config.hyperliquid.signing_key_env)
            .await
            .map_err(|error| error.to_string())?;
    let max_taker_notional = Decimal::from_str(&operational.max_taker_notional_usdc)?;
    let mut nonce_state_path = PathBuf::from(journal_path);
    nonce_state_path.set_extension("nonce-state.json");
    // A subaccount/vault execution account requires dex-connector's
    // vault_address to be set (and equal to account_address) so the signed
    // action's vaultAddress field routes it there — see
    // Config::requires_vault_address_routing's doc comment.
    let vault_address = config
        .requires_vault_address_routing()?
        .then(|| account_address.clone());
    let connector = HyperliquidConnector::new(HyperliquidConnectorConfig {
        base_url: config.hyperliquid.endpoint.clone(),
        tracked_symbols: Vec::new(),
    })
    .map_err(box_error)?
    .with_account(HyperliquidAccountConfig {
        account_address,
        signer_private_key: Some(signer_private_key),
        vault_address,
        is_mainnet: operational.is_mainnet,
        nonce_state_path: Some(nonce_state_path),
        max_taker_notional: Some(max_taker_notional),
        max_taker_slippage_bps: Some(operational.max_taker_slippage_bps),
        max_taker_book_age_ms: operational.max_taker_book_age_ms,
    })
    .map_err(box_error)?;
    Ok(connector)
}

#[cfg(test)]
mod tests {
    use super::{invocation, Invocation};

    fn args(values: &[&str]) -> impl Iterator<Item = String> {
        values
            .iter()
            .map(|value| (*value).to_owned())
            .collect::<Vec<_>>()
            .into_iter()
    }

    #[test]
    fn parses_a_complete_prepare_invocation() {
        assert_eq!(
            invocation(args(&[
                "prepare",
                "config.toml",
                "security-policy.toml",
                "runtime.toml",
                "operational.toml",
                "journal.jsonl",
            ])),
            Ok(Invocation::Prepare {
                config_path: "config.toml".to_owned(),
                security_policy_path: "security-policy.toml".to_owned(),
                runtime_config_path: "runtime.toml".to_owned(),
                operational_params_path: "operational.toml".to_owned(),
                journal_path: "journal.jsonl".to_owned(),
            })
        );
    }

    #[test]
    fn parses_a_complete_submit_invocation() {
        assert_eq!(
            invocation(args(&[
                "submit",
                "config.toml",
                "security-policy.toml",
                "operational.toml",
                "journal.jsonl",
                "--confirm",
                "0xabc123",
            ])),
            Ok(Invocation::Submit {
                config_path: "config.toml".to_owned(),
                security_policy_path: "security-policy.toml".to_owned(),
                operational_params_path: "operational.toml".to_owned(),
                journal_path: "journal.jsonl".to_owned(),
                confirm_client_order_id: "0xabc123".to_owned(),
            })
        );
    }

    #[test]
    fn rejects_submit_without_the_literal_confirm_flag() {
        // A caller must pass the `--confirm` flag literally, not just any
        // 7-argument submit invocation — this is the one thing standing
        // between an operator and an actual signed submission.
        assert!(invocation(args(&[
            "submit",
            "config.toml",
            "security-policy.toml",
            "operational.toml",
            "journal.jsonl",
            "--yes",
            "0xabc123",
        ]))
        .is_err());
    }

    #[test]
    fn rejects_missing_arguments() {
        assert!(invocation(args(&["prepare", "config.toml"])).is_err());
        assert!(invocation(args(&[])).is_err());
        assert!(invocation(args(&["unknown-command"])).is_err());
    }
}
