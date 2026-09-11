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

use chrono::{DateTime, Utc};
use dex_connector::{HyperliquidAccountConfig, HyperliquidConnector, HyperliquidConnectorConfig};
use hype_accumulator::{
    config::{Config, EffectiveLiveOrderPolicy, ProcessEnvironment},
    live_decision::{bound_decision_identity, prepare_first_live_order_workflow},
    live_probe::{
        execution_identity_hash_for, reconcile_prepared_order, HyperliquidLiveProbe,
        LiveProbeBinding,
    },
    monitor::{trade_cadence_label, HyperliquidObserver, ATTRIBUTION_EXCEEDS_HOLDINGS},
    order_envelope::OrderEnvelopeFreshnessPolicy,
    pacing::{DailyDecision, PacingLimits, UsdcMicros},
    runtime::{
        AdmissionApprovals, DecisionMode, LiveDecisionIdentity, LiveHypeAcquisition, RuntimeConfig,
        RuntimeCycleInput, SignerFreeRuntime,
    },
    signal::SignalSnapshot,
    signer::resolve_signer_private_key,
    workflow::{
        DisabledStakingProof, DurableWorkflow, EligibilityPolicyBinding, ExchangeOrderOwnerStore,
        FileExchangeOrderOwnerStore, FileProtectedWorkflowHeadStore, HypeAtoms,
        ProtectedWorkflowHeadStore, WorkflowError, WorkflowStage, WorkflowState,
    },
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::{
    cell::RefCell,
    collections::BTreeSet,
    env, fs,
    io::Write,
    path::{Path, PathBuf},
    process,
    str::FromStr,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

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
    // Every journal for this account must live directly in this one
    // directory — never derived from a per-invocation `journal_path`, which
    // a differently-shaped future caller (e.g. a date-partitioned daily
    // scheduler) could vary without ever meaning to change where history is
    // aggregated from. Fixed here, in a file the operator reviews and edits
    // deliberately, not inferred.
    //
    // Optional, not required, even though only `prepare` ever reads it:
    // `submit`/`reconcile` parse this same shared struct out of an
    // operational.toml that may predate this field, to recover an in-flight
    // order after an upgrade. A hard-required field here would make those
    // two commands fail to even parse a still-otherwise-valid legacy file,
    // forcing an operator to edit unrelated configuration before recovery —
    // see `prepare`'s explicit `ok_or` below for where this is actually
    // enforced.
    history_directory: Option<String>,
}

impl OperationalParams {
    fn from_toml(input: &str) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(toml::from_str(input)?)
    }
}

/// Durably records exactly which venue endpoint/network and vault-address
/// routing decision `prepare` resolved its quantity/price/inventory/signed
/// action shape from, at the same path every `submit` for this journal must
/// independently re-derive and match.
///
/// Nothing in `DecisionBinding`/`OrderEnvelopeBinding`/`InventoryBaseline`/
/// `LiveProbeBinding` records either fact — `config.toml`'s
/// `hyperliquid.endpoint` and `custody.execution_account_kind`, and
/// `operational.toml`'s `is_mainnet`, are read completely independently by
/// each subcommand invocation:
///
/// - Without the network half of this check, an operator could `prepare`
///   against testnet, then edit either file to point at mainnet before
///   running `submit` for the *same already-fsynced journal*, and the
///   mainnet-priced submission would silently use testnet-derived quantity,
///   price, and inventory — never re-validated against the network it is
///   actually about to hit.
/// - Without the routing half, `build_signed_connector` re-derives
///   `vault_address` from `config.requires_vault_address_routing()` fresh at
///   `submit` time. If `execution_account_kind` changes between `prepare`
///   and `submit` (e.g. an operator edits it to fix an unrelated custody
///   setting), the signed action's `vaultAddress` field silently follows the
///   *new* setting while `LiveProbeBinding`'s hash — derived only from the
///   unchanged account/signer addresses — cannot detect the change. The
///   prepared IOC would then route to the wrong subaccount/vault/master
///   account with every other binding check still passing.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct PrepareTimeBinding {
    endpoint: String,
    is_mainnet: bool,
    // A journal's binding file written before this field existed predates
    // vault-address routing entirely, so its absence deserializes as the
    // historically correct value (no routing) rather than failing to parse
    // and permanently blocking `verify`/`write_once` for that journal. A
    // legacy journal whose *current* policy now requires vault routing
    // still correctly fails `verify`/`write_once` below: `false` will not
    // equal the freshly resolved `true`.
    #[serde(default)]
    requires_vault_address_routing: bool,
}

impl PrepareTimeBinding {
    fn path(journal_path: &str) -> PathBuf {
        let mut path = PathBuf::from(journal_path);
        path.set_extension("network-binding.json");
        path
    }

    fn new(endpoint: String, is_mainnet: bool, requires_vault_address_routing: bool) -> Self {
        Self {
            endpoint,
            is_mainnet,
            requires_vault_address_routing,
        }
    }

    fn resolved(
        config: &Config,
        operational: &OperationalParams,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self::new(
            config.hyperliquid.endpoint.clone(),
            operational.is_mainnet,
            config.requires_vault_address_routing()?,
        ))
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
                    "prepare-time binding already recorded for this journal ({existing:?}) does \
                     not match this prepare attempt ({self:?}); a journal's network selection \
                     and vault-address routing mode are fixed at first prepare and must never \
                     change"
                )
                .into());
            }
            return Ok(());
        }
        fs::write(&path, serde_json::to_string_pretty(self)?)?;
        Ok(())
    }

    fn read(journal_path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let path = Self::path(journal_path);
        Ok(serde_json::from_str(&fs::read_to_string(&path).map_err(
            |_| {
                format!(
                    "no prepare-time binding recorded at {}; run `prepare` first",
                    path.display()
                )
            },
        )?)?)
    }

    fn verify(journal_path: &str, current: &Self) -> Result<(), Box<dyn std::error::Error>> {
        let recorded = Self::read(journal_path)?;
        if recorded != *current {
            return Err(format!(
                "config.toml/operational.toml now resolve to {current:?}, but this journal was \
                 prepared against {recorded:?}; refusing to submit against a different network \
                 or a changed vault-address routing mode. Re-run `prepare` with the network and \
                 custody settings you intend to submit with."
            )
            .into());
        }
        Ok(())
    }

    /// Unlike [`Self::verify`] (exact equality, used before ever submitting
    /// a signed action against a specific endpoint), historical-journal
    /// aggregation only cares about the stable network/routing identity: a
    /// benign endpoint change (URL migration, failover) must not
    /// permanently exclude every already-completed journal's residual just
    /// because their immutable, write-once bindings recorded the old URL.
    const fn same_network_and_routing_as(&self, other: &Self) -> bool {
        self.is_mainnet == other.is_mainnet
            && self.requires_vault_address_routing == other.requires_vault_address_routing
    }
}

/// Durably records which `operational.toml` `history_directory` the first
/// `prepare` for this `operational_params_path` resolved, at a path
/// derived from that stable file path (not from `history_directory`
/// itself, which is exactly the value this exists to protect against
/// silently drifting). `operational_params_path` is assumed operator-
/// controlled and stable across invocations (fixed in a systemd unit or
/// cron job), the same trust `config_path`/`security_policy_path` already
/// get elsewhere in this binary — unlike `journal_path`, which varies day
/// to day.
///
/// Without this, an operator editing `operational.toml`'s
/// `history_directory` between `prepare` runs would go undetected:
/// `validate_journal_path` only checks the *current* invocation's
/// `journal_path` against the *current* `history_directory`, and
/// `aggregate_terminal_residual_hype` then silently scans only the new,
/// likely-empty directory — the live-balance upper bound cannot detect
/// this undercount, since it only ever rejects a total that's too large.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HistoryInitialization {
    FirstEver,
    AlreadyInitialized,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct HistoryDirectoryBinding {
    history_directory: String,
    /// File names of every `.jsonl` journal a verified scan for this
    /// `operational_params_path` has found in `history_directory`
    /// (bot-strategy#944). Names, not a count: a count would let each newly
    /// created journal silently substitute for a lost older one.
    ///
    /// `serde(default)` so a binding written before this field existed
    /// reads as empty — the check it feeds is "every recorded journal is
    /// still present", so an unmigrated binding is simply permissive until
    /// the first verified scan records one, never a hard failure on an
    /// account that predates the field. An *older* binary reading a binding
    /// that has one silently drops it on any write it makes (serde ignores
    /// unknown fields), which is why only
    /// [`Self::record_validated_journals`] — never `persist_first_ever` —
    /// ever rewrites an existing binding, and why it refuses to forget a
    /// journal already recorded.
    #[serde(default)]
    recorded_journals: BTreeSet<String>,
}

/// What [`HistoryDirectoryBinding::check`] found on disk: whether history
/// for this `operational_params_path` was already initialized, and the
/// journals recorded as present in it (empty for a first-ever prepare, or
/// for a binding written before the field existed).
#[derive(Debug, Clone, PartialEq, Eq)]
struct HistoryBindingState {
    initialization: HistoryInitialization,
    recorded_journals: BTreeSet<String>,
}

impl HistoryDirectoryBinding {
    /// Keyed by `operational_params_path` itself, not by the execution
    /// account it protects — the cleaner key, `execution_identity_hash`,
    /// isn't available until after the KMS-decrypted connector is built,
    /// several steps later in `prepare()`. This means copying or renaming
    /// the operational file gives the same account a fresh, empty binding
    /// namespace, undetected. Deliberately deferred (bot-strategy#943,
    /// same class of gap as bot-strategy#942): `operational_params_path` is
    /// operator-controlled infrastructure (systemd unit / cron job), not
    /// attacker-reachable, and hype-accumulator has no live capital today.
    fn path(operational_params_path: &str) -> PathBuf {
        let mut path = PathBuf::from(operational_params_path);
        path.set_extension("history-directory-binding.json");
        path
    }

    /// Read-only: determines whether history for this
    /// `operational_params_path` was already initialized, verifying an
    /// existing binding still matches `history_directory` without writing
    /// anything. Split out from [`Self::write_once_and_verify`] so a caller
    /// that still needs to create `history_directory` (a genuinely
    /// first-ever `prepare`) can do so *before* [`Self::persist_first_ever`]
    /// durably commits the binding — persisting the binding first would let
    /// a transient directory-creation failure's retry observe
    /// `AlreadyInitialized` against a directory that was never actually
    /// created, requiring manual directory creation or binding-file surgery
    /// to recover.
    fn check(
        operational_params_path: &str,
        history_directory: &str,
    ) -> Result<HistoryBindingState, Box<dyn std::error::Error>> {
        let path = Self::path(operational_params_path);
        // A read failure other than "no binding yet" (permission denied, a
        // truncated or non-UTF-8 file after a crash) must not be treated as
        // a first-ever prepare: `persist_first_ever` would then silently
        // replace whatever evidence the file held, exactly the undetected
        // history_directory reset this binding exists to prevent.
        match fs::read_to_string(&path) {
            Ok(existing) => {
                let existing: Self = serde_json::from_str(&existing)?;
                if existing.history_directory != history_directory {
                    return Err(format!(
                        "operational.toml's history_directory is now {:?}, but the first prepare \
                         for this operational_params_path recorded {:?}; history_directory must \
                         never change once an account has any completed journals. Restore the \
                         original value, or start a genuinely new account with a fresh \
                         operational_params_path.",
                        history_directory, existing.history_directory
                    )
                    .into());
                }
                Ok(HistoryBindingState {
                    initialization: HistoryInitialization::AlreadyInitialized,
                    recorded_journals: existing.recorded_journals,
                })
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(HistoryBindingState {
                initialization: HistoryInitialization::FirstEver,
                recorded_journals: BTreeSet::new(),
            }),
            Err(err) => Err(format!(
                "failed to read history-directory binding at {}: {err}; refusing to treat an \
                 unreadable or corrupt binding as a first-ever prepare.",
                path.display()
            )
            .into()),
        }
    }

    /// Durably persists the binding for a first-ever prepare. Callers must
    /// only invoke this after `history_directory` itself has been created
    /// and verified available — see [`Self::check`]'s doc for why the order
    /// matters.
    ///
    /// Writes through a `create_new` temporary file, fsyncs it, then
    /// publishes it to `path` with a hard link (not a rename): both paths
    /// are siblings by construction, so the link is an atomic, no-replace
    /// publish primitive on the same filesystem — mirroring
    /// `backup.rs::publish_file_noreplace`'s durability pattern. This gives
    /// the binding true first-writer-wins semantics (a second, concurrent
    /// first-ever `prepare` for the same `operational_params_path` fails
    /// with `AlreadyExists` instead of silently overwriting the first
    /// binding) and crash safety (a crash between the temporary write and
    /// the link never leaves a partial file at `path`; a crash after the
    /// link is indistinguishable from a completed write).
    fn persist_first_ever(
        operational_params_path: &str,
        history_directory: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let path = Self::path(operational_params_path);
        let current = Self {
            history_directory: history_directory.to_owned(),
            recorded_journals: BTreeSet::new(),
        };
        let (parent, temporary) = current.write_temporary(&path)?;
        let link_result = fs::hard_link(&temporary, &path);
        let _ = fs::remove_file(&temporary);
        link_result?;
        #[cfg(unix)]
        fs::File::open(&parent)?.sync_all()?;
        Ok(())
    }

    /// Records the journals a verified scan just found in the bound
    /// `history_directory` (bot-strategy#944), preserving
    /// `history_directory` itself exactly as it was recorded.
    ///
    /// Unlike [`Self::persist_first_ever`] this publishes with a rename,
    /// which replaces — the binding's write-once property protects
    /// `history_directory`, not the journal set, and that set has to grow.
    /// To keep that property intact the existing binding is re-read here
    /// and its `history_directory` is what gets written back, so no code
    /// path can rewrite the bound directory through this method. Refusing
    /// to forget a journal already recorded makes the write monotonic,
    /// which in turn makes two concurrent `prepare`s harmless: both observe
    /// the same directory, so either order of their (identical) writes
    /// leaves the same set, and a stale writer that observed *fewer*
    /// journals is rejected before it can write at all.
    fn record_validated_journals(
        operational_params_path: &str,
        journals: &BTreeSet<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let path = Self::path(operational_params_path);
        // Held across the whole read-check-publish below. Two `prepare`s
        // sharing this operational config are not otherwise serialized —
        // their runtime locks are per state directory, and this file lives
        // next to the operational config, not in either — so without it the
        // slower writer's rename could replace a larger recorded set with
        // its own stale, smaller one, and a journal only that larger set
        // named would stop being missed when it was lost. Same lock-sibling
        // posture as `FileProtectedWorkflowHeadStore`.
        let _lock = Self::lock(&path)?;
        let existing: Self = serde_json::from_str(&fs::read_to_string(&path)?)?;
        let forgotten: Vec<&str> = existing
            .recorded_journals
            .iter()
            .map(String::as_str)
            .filter(|name| !journals.contains(*name))
            .collect();
        if !forgotten.is_empty() {
            return Err(format!(
                "refusing to drop {} journal(s) already recorded at {} ({})",
                forgotten.len(),
                path.display(),
                forgotten.join(", ")
            )
            .into());
        }
        let current = Self {
            history_directory: existing.history_directory,
            recorded_journals: journals.clone(),
        };
        let (parent, temporary) = current.write_temporary(&path)?;
        let rename_result = fs::rename(&temporary, &path);
        if rename_result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        rename_result?;
        #[cfg(unix)]
        fs::File::open(&parent)?.sync_all()?;
        Ok(())
    }

    /// Opens and exclusively locks the `<binding>.lock` sibling, held for as
    /// long as the returned handle lives. `O_NOFOLLOW` and the hard-link
    /// check mirror this crate's posture for every other security-relevant
    /// file open: a lock that can be aimed elsewhere, or that a second name
    /// also points at, is not a lock.
    fn lock(path: &Path) -> Result<fs::File, Box<dyn std::error::Error>> {
        let mut lock_name = path
            .file_name()
            .ok_or("history-directory binding has no file name")?
            .to_os_string();
        lock_name.push(".lock");
        let lock_path = path
            .parent()
            .ok_or("history-directory binding has no parent")?
            .join(lock_name);
        let mut options = fs::OpenOptions::new();
        options.create(true).read(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
        }
        let lock = options.open(&lock_path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if lock.metadata()?.nlink() != 1 {
                return Err(format!(
                    "history-directory binding lock {} has more than one name",
                    lock_path.display()
                )
                .into());
            }
        }
        fs2::FileExt::lock_exclusive(&lock)?;
        Ok(lock)
    }

    /// Serializes `self` into a fsynced, 0600, `create_new` sibling of
    /// `path`, ready to be published by whichever primitive the caller
    /// needs (a no-replace hard link, or a replacing rename). Returns
    /// `path`'s parent directory — the one to fsync after publishing — and
    /// the temporary file's path. The temporary is removed on any write
    /// failure; a successful return hands ownership of it to the caller.
    fn write_temporary(
        &self,
        path: &Path,
    ) -> Result<(PathBuf, PathBuf), Box<dyn std::error::Error>> {
        let payload = serde_json::to_string_pretty(self)?;
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("history-directory-binding.json");
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(std::io::Error::other)?
            .as_nanos();
        let temporary = parent.join(format!(".{file_name}.{}.{nonce}.tmp", process::id()));
        let write_result: Result<(), std::io::Error> = (|| {
            let mut options = fs::OpenOptions::new();
            options.create_new(true).write(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options.open(&temporary)?;
            file.write_all(payload.as_bytes())?;
            file.sync_all()?;
            Ok(())
        })();
        if let Err(error) = write_result {
            let _ = fs::remove_file(&temporary);
            return Err(error.into());
        }
        Ok((parent, temporary))
    }

    /// Durably writes the binding on first use, or verifies an existing one
    /// still matches.
    /// Returns whether history for this `operational_params_path` was
    /// already initialized before this call, so the caller can tell a
    /// genuinely first-ever `prepare` (no directory yet is normal) apart
    /// from one that already succeeded before (no directory now means it
    /// was lost, not that it never existed).
    ///
    /// Convenience wrapper over [`Self::check`] + [`Self::persist_first_ever`]
    /// for callers (and tests) with nothing to create in between; `prepare`
    /// itself must call the two steps separately around directory creation
    /// instead of using this.
    #[cfg(test)]
    fn write_once_and_verify(
        operational_params_path: &str,
        history_directory: &str,
    ) -> Result<HistoryInitialization, Box<dyn std::error::Error>> {
        match Self::check(operational_params_path, history_directory)?.initialization {
            HistoryInitialization::AlreadyInitialized => {
                Ok(HistoryInitialization::AlreadyInitialized)
            }
            HistoryInitialization::FirstEver => {
                Self::persist_first_ever(operational_params_path, history_directory)?;
                Ok(HistoryInitialization::FirstEver)
            }
        }
    }
}

const USAGE: &str = "usage:\n  hype-live-probe prepare <config.toml> <security-policy.toml> \
     <runtime-config.toml> <operational.toml> <journal.jsonl>\n  hype-live-probe submit \
     <config.toml> <security-policy.toml> <runtime-config.toml> <operational.toml> \
     <journal.jsonl> --confirm <client_order_id>\n  hype-live-probe reconcile <config.toml> \
     <security-policy.toml> <runtime-config.toml> <operational.toml> <journal.jsonl>\n  \
     hype-live-probe release <config.toml> <security-policy.toml> <runtime-config.toml> \
     <operational.toml>\n  hype-live-probe backfill-attribution <config.toml> \
     <security-policy.toml> <runtime-config.toml> <operational.toml>";

#[derive(Debug, Eq, PartialEq)]
enum Invocation {
    Reconcile {
        config_path: String,
        security_policy_path: String,
        runtime_config_path: String,
        operational_params_path: String,
        journal_path: String,
    },
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
        runtime_config_path: String,
        operational_params_path: String,
        journal_path: String,
        confirm_client_order_id: String,
    },
    Release {
        config_path: String,
        security_policy_path: String,
        runtime_config_path: String,
        operational_params_path: String,
    },
    BackfillAttribution {
        config_path: String,
        security_policy_path: String,
        runtime_config_path: String,
        operational_params_path: String,
    },
}

fn invocation<I>(args: I) -> Result<Invocation, &'static str>
where
    I: Iterator<Item = String>,
{
    let args = args.collect::<Vec<_>>();
    match args.as_slice() {
        [command, config_path, security_policy_path, runtime_config_path, operational_params_path, journal_path]
            if command == "reconcile" =>
        {
            Ok(Invocation::Reconcile {
                config_path: config_path.clone(),
                security_policy_path: security_policy_path.clone(),
                runtime_config_path: runtime_config_path.clone(),
                operational_params_path: operational_params_path.clone(),
                journal_path: journal_path.clone(),
            })
        }

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
        [command, config_path, security_policy_path, runtime_config_path, operational_params_path, journal_path, confirm_flag, confirm_client_order_id]
            if command == "submit" && confirm_flag == "--confirm" =>
        {
            Ok(Invocation::Submit {
                config_path: config_path.clone(),
                security_policy_path: security_policy_path.clone(),
                runtime_config_path: runtime_config_path.clone(),
                operational_params_path: operational_params_path.clone(),
                journal_path: journal_path.clone(),
                confirm_client_order_id: confirm_client_order_id.clone(),
            })
        }
        [command, config_path, security_policy_path, runtime_config_path, operational_params_path]
            if command == "release" =>
        {
            Ok(Invocation::Release {
                config_path: config_path.clone(),
                security_policy_path: security_policy_path.clone(),
                runtime_config_path: runtime_config_path.clone(),
                operational_params_path: operational_params_path.clone(),
            })
        }
        [command, config_path, security_policy_path, runtime_config_path, operational_params_path]
            if command == "backfill-attribution" =>
        {
            Ok(Invocation::BackfillAttribution {
                config_path: config_path.clone(),
                security_policy_path: security_policy_path.clone(),
                runtime_config_path: runtime_config_path.clone(),
                operational_params_path: operational_params_path.clone(),
            })
        }
        _ => Err(USAGE),
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    match invocation(env::args().skip(1))? {
        Invocation::Reconcile {
            config_path,
            security_policy_path,
            runtime_config_path,
            operational_params_path,
            journal_path,
        } => {
            reconcile(
                &config_path,
                &security_policy_path,
                &runtime_config_path,
                &operational_params_path,
                &journal_path,
            )
            .await
        }

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
            runtime_config_path,
            operational_params_path,
            journal_path,
            confirm_client_order_id,
        } => {
            submit(
                &config_path,
                &security_policy_path,
                &runtime_config_path,
                &operational_params_path,
                &journal_path,
                &confirm_client_order_id,
            )
            .await
        }
        Invocation::Release {
            config_path,
            security_policy_path,
            runtime_config_path,
            operational_params_path,
        } => release(
            &config_path,
            &security_policy_path,
            &runtime_config_path,
            &operational_params_path,
        ),

        Invocation::BackfillAttribution {
            config_path,
            security_policy_path,
            runtime_config_path,
            operational_params_path,
        } => backfill_attribution(
            &config_path,
            &security_policy_path,
            &runtime_config_path,
            &operational_params_path,
        ),
    }
}

/// `prepare`'s history-directory preamble: binds `history_directory` for
/// this `operational_params_path`, refuses a journal path that could never
/// be rediscovered, verifies the directory is both present and still holds
/// the journals an earlier run recorded, and creates it on a genuinely
/// first-ever run. Returns the directory and its canonical form (the shape
/// the runtime's hash-chained namespace binding uses, so an alias of the
/// same directory cannot dodge it).
///
/// Extracted from `prepare` only to keep that function readable; the order
/// of the steps below is load-bearing and each one's comment says why.
fn bind_history_directory_for_prepare(
    operational_params_path: &str,
    history_directory: &str,
    journal_path: &str,
) -> Result<(PathBuf, PathBuf, HistoryBindingState), Box<dyn std::error::Error>> {
    // Durably binds history_directory to the first value ever read for this
    // operational_params_path, before trusting it for anything: an
    // operator later editing operational.toml's history_directory would
    // otherwise go undetected, and the next prepare would silently
    // aggregate from an empty new location instead of failing closed.
    let history_binding =
        HistoryDirectoryBinding::check(operational_params_path, history_directory)?;
    let journal_directory = PathBuf::from(history_directory);
    // Validated before anything below acts on `journal_directory`, in
    // particular before `persist_first_ever` durably (and irreversibly)
    // commits history_directory below: on a genuinely first-ever prepare
    // with a mismatched journal_path, an operator fixing the mistake would
    // otherwise still find every retry rejected as a "directory change"
    // against a binding that was written for an invocation that never
    // actually created a workflow, requiring manual binding-file surgery to
    // recover. This check is a pure path comparison with no side effects,
    // so running it first costs nothing.
    validate_journal_path(journal_path, &journal_directory)?;
    ensure_history_directory_available(history_binding.initialization, &journal_directory)?;
    // Reached only when the directory already exists (the `AlreadyInitialized`
    // case above fails closed otherwise) or this is a genuinely first-ever
    // prepare, so creating it here is a no-op in the former case and turns
    // the latter's "missing is normal" into "present before anything below
    // — the journal write, `PrepareTimeBinding`'s sidecar, protected-head
    // store — tries to write into it.
    fs::create_dir_all(&journal_directory)?;
    // Persisted only now that the directory demonstrably exists: doing this
    // before `create_dir_all` would let a transient creation failure's retry
    // observe `AlreadyInitialized` against a directory that was never
    // actually created (see `HistoryDirectoryBinding::check`'s doc).
    if history_binding.initialization == HistoryInitialization::FirstEver {
        HistoryDirectoryBinding::persist_first_ever(operational_params_path, history_directory)?;
    }
    // Canonical so the runtime's hash-chained namespace binding cannot be
    // dodged with an alias of the same directory.
    let live_history_directory = fs::canonicalize(&journal_directory)?;
    Ok((journal_directory, live_history_directory, history_binding))
}

#[allow(clippy::too_many_lines)]
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
    // The only place `history_directory` is required: `submit`/`reconcile`
    // never read it (see the field's doc comment), so a legacy
    // operational.toml that predates this field parses fine for them, but
    // `prepare` cannot proceed without a directory to bind history to.
    let history_directory = operational
        .history_directory
        .as_deref()
        .ok_or("operational.toml is missing history_directory, required by `prepare`")?;
    let (journal_directory, live_history_directory, history_binding) =
        bind_history_directory_for_prepare(
            operational_params_path,
            history_directory,
            journal_path,
        )?;
    // Fixes the network this journal is bound to before anything else reads
    // `config`/`operational` for a network-dependent value: refuses to
    // silently re-bind an already-prepared journal to a different network on
    // a later `prepare` retry.
    let prepare_time_binding = PrepareTimeBinding::resolved(&config, &operational)?;
    prepare_time_binding.write_once(journal_path)?;

    // Decrypts the signer now, even though the signal-free
    // `SignerFreeRuntime::apply_cycle` below (inside
    // `prepare_first_live_order_workflow`) might still find no decision
    // due. This can't be deferred further: envelope assembly's nonce
    // reservation runs inside that same call, and splitting the signer-free
    // and signer-requiring halves of that flow is out of this binary's
    // scope (it would mean revisiting `live_decision.rs`, already merged).
    let connector = build_signed_connector(&config, &operational, journal_path).await?;

    // Observes the account *before* the cycle clock is read below, in the
    // same order as `main.rs`'s `--dry-run-cycle`: `validate_cycle_range`
    // rejects a cycle whose `accumulator.balance_observed_at()` is later
    // than its `observed_at` ("account observation is after the runtime
    // cycle"). Reading `now` first and observing afterwards fails that
    // check deterministically — found on the first real mainnet `prepare`
    // (bot-strategy#845, 2026-09-07), a path no offline fixture exercises.
    let account = config.observation_account(&ProcessEnvironment)?;
    let observer = HyperliquidObserver::new(&config.hyperliquid.endpoint, &account)?;
    // Opened before the observation, not after it as everything else in this
    // function is ordered, because the cycle below republishes the status
    // document: observing with `Unavailable` here would overwrite the
    // dashboard's attributed HYPE with zero for as long as the recurring
    // cycle stays stopped, which on a probe day is the whole probe
    // (bot-strategy#929). The exclusive state lock is held across the
    // observation as a result; a probe day has the recurring timer stopped,
    // so nothing else contends for it.
    let runtime_config = RuntimeConfig::from_toml(&fs::read_to_string(runtime_config_path)?)?
        .with_parent_funding_route(config.parent_funding_route(&ProcessEnvironment)?);
    let limits = PacingLimits::from_config(&config)?;
    let mut runtime = SignerFreeRuntime::open(runtime_config.clone(), limits)?;
    let attribution = runtime.attributed_hype().to_attribution();
    let accumulator = observer
        .observe(&attribution, trade_cadence_label(&config.schedule))
        .await?;
    // The recurring cycle halts on this (see `main.rs`), and this path — the
    // only one that commits real capital — must halt harder: refuse before a
    // decision is committed or an order is printed, while HYPE the ledger
    // says the bot owns is unaccounted for (bot-strategy#929).
    if accumulator.attribution_exceeds_holdings() {
        return Err(format!(
            "refusing to prepare an order: {ATTRIBUTION_EXCEEDS_HOLDINGS}. Reconcile the \
             account's HYPE against the workflow journals first."
        )
        .into());
    }

    // Re-reads the clock here, after the KMS-backed signer decrypt and the
    // account observation above (network round trips whose latency is
    // outside this binary's control) rather than reusing the `now` captured
    // before them. Every freshness/expiry computation below — policy
    // acknowledgement validity, the movement-scan window, signal evidence,
    // and the order envelope's `signed_expiry_at` — should be judged against
    // a clock read as close as practical to the decision it gates, not one
    // that already has an unbounded KMS round trip baked into its staleness.
    // This is also what keeps `observed_at >= balance_observed_at` and
    // `scan_end_ms == observed_at` for `validate_cycle_range`.
    let now = Utc::now();
    let effective = config.effective_live_order_policy(&ProcessEnvironment, now)?;
    let policy_version = config.effective_security_policy_digest(&ProcessEnvironment, now)?;
    let (envelope_policy, eligibility_policy) = build_prepare_policies(
        &effective,
        &operational,
        policy_version,
        config.staking_policy_digest()?,
    );
    let configured_residual_hype_atoms = HypeAtoms::from_atoms(effective.residual_hype_wei);

    let approvals = AdmissionApprovals::from_json(&fs::read_to_string(
        runtime_config.admission_approvals_path(),
    )?)?;
    let signal = match fs::read_to_string(runtime_config.signal_snapshot_path()) {
        Ok(payload) => SignalSnapshot::from_json(&payload).ok(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
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
        decision_mode: decision_mode_for(&config, live_history_directory),
    };

    let (protected_head_store, owner_store) = build_stores(Path::new(journal_path))?;
    let journal = PathBuf::from(journal_path);

    // `signal_evidence_valid_through_at` is not `policy_acknowledgement_valid_through_at`
    // (an unrelated quantity that happens to also be a `DateTime<Utc>`) — no
    // per-signal timestamp is available here, so this mirrors the same
    // judgment call `order_envelope.rs`'s `decision_valid_through_at` already
    // makes and was reviewed for (PR #27): treat the signal as fresh as of
    // `now` and bound it by the same `signal_stale_after_seconds` window.
    let signal_evidence_valid_through_at =
        now + chrono::TimeDelta::seconds(i64::try_from(effective.signal_stale_after_seconds)?);
    let network_routing_admissible = network_routing_admissible_for(&prepare_time_binding);
    // The aggregation below calls this once per journal it accepts, and any
    // journal that fails any later check aborts the whole scan — so a
    // successful return means this collected exactly the journals the
    // verified scan validated. That is the only set the durable record may be
    // written from: a malformed, foreign or half-restored `.jsonl` would
    // otherwise be recorded before the scan rejected it, and removing that
    // file to repair the directory would then wedge every later run against a
    // journal that never belonged there (Codex review, hype-accumulator#54).
    let validated = RefCell::new(BTreeSet::new());
    let admissible = collecting_journal_admissible(network_routing_admissible, &validated);
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
        &journal_directory,
        &history_binding.recorded_journals,
        &historical_protected_head_store_for,
        &admissible,
        &|| record_validated_history_journals(operational_params_path, &validated),
        protected_head_store,
        owner_store,
        now,
    )
    .await?;

    print_prepared_order(
        &workflow,
        config_path,
        security_policy_path,
        runtime_config_path,
        operational_params_path,
        journal_path,
    )
}

/// Prints the prepared (but unsigned, unsent) order and the exact `submit`
/// command that would send it — the operator's review step, and the only
/// place a `submit` command line is ever produced.
fn print_prepared_order(
    workflow: &DurableWorkflow,
    config_path: &str,
    security_policy_path: &str,
    runtime_config_path: &str,
    operational_params_path: &str,
    journal_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let action = workflow.pending_prepared_order()?;
    println!("mode=prepared journal={journal_path}");
    println!("{action:#?}");
    println!(
        "\nReview the values above carefully. To submit this exact order, run:\n  hype-live-probe submit {config_path} {security_policy_path} {runtime_config_path} {operational_params_path} {journal_path} --confirm {}",
        workflow.state().client_order_id()
    );
    Ok(())
}

async fn submit(
    config_path: &str,
    security_policy_path: &str,
    runtime_config_path: &str,
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
    // different network, or a changed vault-address routing mode, than what
    // this journal was `prepare`d against — see `PrepareTimeBinding`'s doc
    // comment for the exact danger this closes.
    PrepareTimeBinding::verify(
        journal_path,
        &PrepareTimeBinding::resolved(&config, &operational)?,
    )?;

    let binding = DurableWorkflow::peek_committed_binding(journal_path)?
        .ok_or("no prepared order found at this journal path; run `prepare` first")?;
    let (protected_head_store, owner_store) = build_stores(Path::new(journal_path))?;
    let mut workflow =
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

    // Before any economic action (and before decrypting the signer): the
    // runtime this order will settle into must be reachable, unlocked, hold
    // this journal's decision unsettled, and have declared this journal for
    // it. A settlement-side failure discovered only after the venue accepted
    // the order would leave the commitment unsettled behind a live fill.
    preflight_settlement_runtime(&config, runtime_config_path, &workflow, journal_path)?;

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

    // KMS decryption may have consumed the prepared order's short lifetime.
    // Never authorize submission using the clock captured before that I/O.
    let submission = probe.submit(&workflow, Utc::now()).await;
    match &submission {
        Ok(receipt) => println!("mode=submitted {receipt:#?}"),
        Err(_) => eprintln!(
            "submission failed or is ambiguous; reconciling the durable CLOID; do not resubmit"
        ),
    }
    // Even a transport error may follow venue acceptance. Recovery must run
    // after every attempt, without allowing a second economic request.
    // Same proof `reconcile` passes (bot-strategy#993). `submit` already
    // required a valid acknowledgement above, so a failure here would be a
    // clock race; withholding the proof then is the fail-closed choice (the
    // next `reconcile` completes the workflow).
    let now = Utc::now();
    let disabled_staking = disabled_staking_proof(&config, now);
    let reconciliation = probe
        .reconcile(
            &mut workflow,
            Path::new(journal_path),
            disabled_staking.as_ref(),
            now,
        )
        .await;
    let settlement = if let Ok(observation) = &reconciliation {
        print_observation(observation)?;
        // Closes the loop from terminal fill evidence back to the capital
        // ledger (#901 item 5). Runs after every reconciliation attempt that
        // reached the venue; a failure here leaves the order's own evidence
        // intact and is retried by the signer-free `reconcile`.
        settle_finalized_decision(&config, runtime_config_path, &workflow, observation)
    } else {
        eprintln!(
            "reconciliation unavailable; run the signer-free reconcile command; do not resubmit"
        );
        Ok(())
    };
    submission?;
    reconciliation?;
    settlement?;
    Ok(())
}

/// Settles the workflow's pacing decision in the signer-free runtime from the
/// durable terminal fill evidence, once the order is final. Before finality
/// nothing is written: the decision stays committed-but-unsettled, which
/// blocks every later decision day (`PriorDecisionUnsettled`) until the
/// signer-free `reconcile` observes the terminal state and settles it.
/// Idempotent — repeating it after a successful settlement writes nothing.
/// Live policy (`dry_run = false`): the new planned decision is left
/// unsettled so the execution workflow can bind it, and the runtime binds
/// itself to `history_directory` (the namespace the journal is created in).
/// A `DRY_RUN` pair keeps zero-settling it in-cycle, so `prepare` still fails
/// closed at the binding.
fn decision_mode_for(config: &Config, history_directory: PathBuf) -> DecisionMode {
    if config.dry_run {
        DecisionMode::DryRun
    } else {
        DecisionMode::Live { history_directory }
    }
}

fn settle_finalized_decision(
    config: &Config,
    runtime_config_path: &str,
    workflow: &DurableWorkflow,
    observation: &hype_accumulator::live_probe::ProbeReconciliation,
) -> Result<(), Box<dyn std::error::Error>> {
    let identity = bound_decision_identity(workflow.state().binding());
    let decision_id = identity.decision_id.clone();
    if !observation.durable_finality {
        println!("mode=settlement-deferred decision={decision_id} durable_finality=false");
        return Ok(());
    }
    // A terminal result already on the journal is only settleable from a
    // reconciliation that could see every fill row: with the venue's bounded
    // recent-fill window aged out, `record_reconciliation` records nothing
    // new (no contradictory evidence, no ManualReview) while the authoritative
    // cumulative quantity may already exceed what the journal froze.
    if !observation.fills_complete {
        println!(
            "mode=settlement-deferred decision={decision_id} fills_complete=false (rerun \
             reconcile once the order's fill rows are fully visible)"
        );
        return Ok(());
    }
    // Holds the journal's append lock through the runtime commit and first
    // re-verifies this instance is not stale: a concurrent `submit`/
    // `reconcile` that has meanwhile appended (a late fill, ManualReview)
    // fails closed here instead of letting the earlier totals settle.
    workflow.with_frozen_state(|state| -> Result<(), Box<dyn std::error::Error>> {
        // Fresh late venue evidence that contradicts a terminal result moves
        // the workflow to ManualReview; its recorded totals are then
        // contested and must not be written into the capital ledger (see the
        // runbook: a settlement already made from the earlier totals cannot
        // be corrected here — bot-strategy#901).
        if state.stage() == WorkflowStage::ManualReview {
            return Err(format!(
                "decision {decision_id}: workflow is in ManualReview (contradictory late venue \
                 evidence); refusing to settle contested totals — resolve the review first"
            )
            .into());
        }
        // The authoritative quantity this reconciliation observed must be the
        // one the journal's terminal result was frozen from; anything else is
        // late evidence the journal has not absorbed yet.
        if observation.filled_hype != state.matched_hype() {
            return Err(format!(
                "decision {decision_id}: venue reports {} HYPE atoms matched but the journal's \
                 terminal result holds {}; refusing to settle stale totals — resolve as manual \
                 review",
                observation.filled_hype.as_atoms(),
                state.matched_hype().as_atoms()
            )
            .into());
        }
        // And the credited quantity — what the account actually holds after
        // any fee charged in HYPE (bot-strategy#998) — must be the one the
        // journal's terminal result was frozen from, for the same reason.
        if observation.credited_hype != Some(state.purchased_hype()) {
            return Err(format!(
                "decision {decision_id}: venue fills credit {:?} HYPE atoms but the journal's \
                 terminal result holds {}; refusing to settle stale totals — resolve as manual \
                 review",
                observation.credited_hype.map(HypeAtoms::as_atoms),
                state.purchased_hype().as_atoms()
            )
            .into());
        }
        let mut runtime = open_signer_free_runtime(config, runtime_config_path)?;
        let filled_usdc = state.filled_usdc();
        let debited_usdc = state.debited_usdc();
        // The inventory side of the same settlement, read from exactly the
        // frozen terminal state the cash figures come from: HYPE actually
        // credited (net of a HYPE-denominated fee), never matched
        // (bot-strategy#929/#998).
        let acquisition =
            LiveHypeAcquisition::from_finalized_workflow(state, workflow.journal_path())?;
        let outcome = runtime.settle_live_decision(
            &identity,
            filled_usdc,
            debited_usdc,
            &acquisition,
            Utc::now(),
        )?;
        println!(
            "mode=settled decision={decision_id} filled_usdc={} debited_usdc={} outcome={outcome:?}",
            filled_usdc.as_micros(),
            debited_usdc.as_micros()
        );
        Ok(())
    })
}

/// Opens the runtime `submit` will later settle into and checks, before the
/// order is sent, that the settlement cannot fail for a reason that was
/// knowable up front: the runtime opens (path readable, not locked, funding
/// route matches), it holds exactly the decision this journal is bound to
/// (full identity match), that decision is still unsettled, and the runtime
/// declared this very journal for it. The lock is released again before the
/// venue call; settlement re-verifies everything under its own lock.
fn preflight_settlement_runtime(
    config: &Config,
    runtime_config_path: &str,
    workflow: &DurableWorkflow,
    journal_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let identity = bound_decision_identity(workflow.state().binding());
    let runtime = open_signer_free_runtime(config, runtime_config_path)?;
    let decision_id = identity.decision_id.as_str();
    match runtime.decision_identity(decision_id) {
        Some(held) if held == identity => {}
        Some(_) => {
            return Err(format!(
                "runtime holds decision {decision_id} but its identity differs from this \
                 journal's binding; refusing to submit an order that could not be settled"
            )
            .into())
        }
        None => {
            return Err(format!(
                "runtime does not hold decision {decision_id}; refusing to submit an order that \
                 could not be settled"
            )
            .into())
        }
    }
    if !runtime
        .unsettled_planned_decisions()
        .iter()
        .any(|decision| decision.decision_id == decision_id)
    {
        return Err(format!(
            "decision {decision_id} is already settled in the runtime; refusing to submit"
        )
        .into());
    }
    let declared = runtime.live_journal_intent(decision_id);
    let this_journal = fs::canonicalize(journal_path)?;
    if declared.map(fs::canonicalize).transpose()?.as_deref() != Some(this_journal.as_path()) {
        return Err(format!(
            "runtime declared journal {} for decision {decision_id}, not {journal_path}; \
             refusing to submit",
            declared.map_or_else(|| "<none>".to_owned(), |path| path.display().to_string())
        )
        .into());
    }
    println!("mode=settlement-preflight-ok decision={decision_id}");
    Ok(())
}

fn open_signer_free_runtime(
    config: &Config,
    runtime_config_path: &str,
) -> Result<SignerFreeRuntime, Box<dyn std::error::Error>> {
    let runtime_config = RuntimeConfig::from_toml(&fs::read_to_string(runtime_config_path)?)?
        .with_parent_funding_route(config.parent_funding_route(&ProcessEnvironment)?);
    let limits = PacingLimits::from_config(config)?;
    Ok(SignerFreeRuntime::open(runtime_config, limits)?)
}

/// Releases live planned decisions that provably never reached a signer.
///
/// `prepare` commits the day's decision in the runtime cycle *before* the
/// execution workflow journal is created, so a failure or crash between the
/// two (envelope assembly, inventory aggregation, journal I/O) leaves capital
/// committed with no order that could ever settle it — and every later
/// decision day fails closed as `PriorDecisionUnsettled`. Signing is only
/// possible through `submit`, which requires a committed workflow binding in
/// the operational config's bound `history_directory`; so a decision that no
/// journal in that directory binds can never have produced a venue action,
/// and settling it at zero is safe. A decision that *is* bound by a journal
/// is deliberately refused here: its order may exist at the venue, and only
/// durable finality (`reconcile`) or gap-free conclusive-absence evidence
/// (not constructible yet, bot-strategy#929) may resolve it.
fn release(
    config_path: &str,
    security_policy_path: &str,
    runtime_config_path: &str,
    operational_params_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // The exclusive runtime lock is taken BEFORE the journal scan and held
    // through settlement: `prepare` opens the runtime before it creates its
    // journal, so while this process holds the lock no new journal can
    // appear, and the scan below cannot go stale between reading the
    // directory and releasing a decision.
    let BoundSignerFreeRuntime {
        config,
        operational,
        journal_directory,
        history_binding,
        mut runtime,
    } = open_bound_signer_free_runtime(
        config_path,
        security_policy_path,
        runtime_config_path,
        operational_params_path,
        "release",
    )?;
    let unsettled = runtime.unsettled_planned_decisions();
    if unsettled.is_empty() {
        println!("mode=nothing-to-release");
        return Ok(());
    }
    // The runtime records (hash-chained, write-once) the directory its live
    // decisions were prepared into. Scanning any other directory — a renamed
    // or copied operational config binds a fresh, empty namespace — would
    // "prove" absence of a journal that exists elsewhere.
    let _bound_history_directory = ensure_runtime_prepared_into(
        &runtime,
        &journal_directory,
        "treat absence from the wrong directory as proof of non-submission",
    )?;
    // Same protected-history verification `prepare`'s aggregation applies:
    // symlinks, orphaned protected heads, rolled-back/truncated/empty
    // journals and duplicate bindings all fail closed, and every journal must
    // pass the same network/routing admissibility check.
    let prepare_time_binding = PrepareTimeBinding::resolved(&config, &operational)?;
    let network_routing_admissible = network_routing_admissible_for(&prepare_time_binding);
    let bound = DurableWorkflow::bound_decision_ids(
        &journal_directory,
        &historical_protected_head_store_for,
        &network_routing_admissible,
        &history_binding.recorded_journals,
    )?
    .ok_or_else(|| {
        format!(
            "history_directory {} does not exist although it was already initialized; \
             refusing to treat a missing directory as proof of absence",
            journal_directory.display()
        )
    })?;
    for decision in unsettled {
        // Recorded by `prepare` before it created the journal: this decision
        // was (or was about to be) bound, so absence from the directory —
        // including a directory deleted and recreated empty, or an unmounted
        // mount point — is never proof it was not submitted.
        if let Some(intent) = runtime.live_journal_intent(&decision.decision_id) {
            return Err(format!(
                "decision {} declared workflow journal {} before it was created; refusing to \
                 release committed capital by absence. If that journal is present, run \
                 `reconcile` on it — after the prepared order's expiry that records conclusive \
                 absence and settles the decision at zero; if it is missing, the history \
                 directory was lost — restore it from backup (bot-strategy#944) first.",
                decision.decision_id,
                intent.display()
            )
            .into());
        }
        if let Some(journal_path) = bound.get(&decision.decision_id) {
            return Err(format!(
                "decision {} is bound by workflow journal {}; refusing to release committed \
                 capital while an order may exist at the venue. Run `reconcile` on that journal: \
                 it reaches durable finality from a fill, or — once the prepared order has \
                 expired and the venue's complete order and fill history contain its client \
                 order ID nowhere — records conclusive absence and settles at zero.",
                decision.decision_id,
                journal_path.display()
            )
            .into());
        }
        let outcome = runtime.settle_live_decision(
            &LiveDecisionIdentity::of(&decision),
            UsdcMicros::default(),
            UsdcMicros::default(),
            // Checked above and re-checked by the runtime: this decision
            // never bound a journal, so no order and no HYPE can exist.
            &LiveHypeAcquisition::NoWorkflow,
            Utc::now(),
        )?;
        println!(
            "mode=released decision={} committed_usdc={} outcome={outcome:?}",
            decision.decision_id,
            decision.committed_usdc.as_micros()
        );
    }
    Ok(())
}

/// Records the HYPE acquisition of purchases that were settled before this
/// runtime recorded inventory (bot-strategy#929).
///
/// Signer-free and economically inert: it commits no capital, prepares no
/// order, and touches no venue. Each decision's own journal is opened the way
/// `reconcile` opens it and read under its append lock; the gates, the
/// required user, and the expected output are in
/// `docs/runbooks/live-probe-recovery.md` ("Backfilling attribution").
fn backfill_attribution(
    config_path: &str,
    security_policy_path: &str,
    runtime_config_path: &str,
    operational_params_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let BoundSignerFreeRuntime {
        config,
        operational,
        journal_directory,
        history_binding: _,
        mut runtime,
    } = open_bound_signer_free_runtime(
        config_path,
        security_policy_path,
        runtime_config_path,
        operational_params_path,
        "backfill-attribution",
    )?;
    // One predicate drives the work list, the early exit and the exit code:
    // the set of settled purchases with no acquisition row.
    let missing = runtime.settled_purchases_without_acquisition();
    if missing.is_empty() {
        let attributed = runtime.attributed_hype();
        println!(
            "mode=nothing-to-backfill attributed_hype_atoms={} settled_purchases={}",
            attributed.credited_hype_atoms, attributed.settled_purchases_with_evidence
        );
        return Ok(());
    }
    // Reading evidence out of any other directory would attribute one
    // account's history to another.
    let bound_history_directory = ensure_runtime_prepared_into(
        &runtime,
        &journal_directory,
        "attribute evidence from the wrong directory",
    )?;
    let prepare_time_binding = PrepareTimeBinding::resolved(&config, &operational)?;
    let network_routing_admissible = network_routing_admissible_for(&prepare_time_binding);
    // From the connector's canonical form of the account, never the raw
    // configured value: every journal's identity was hashed from the former,
    // and a checksummed address in the environment hashes differently.
    let observer = HyperliquidObserver::new(
        &config.hyperliquid.endpoint,
        &config.observation_account(&ProcessEnvironment)?,
    )?;
    let expected_execution_identity = execution_identity_hash_for(observer.execution_account()?);
    let recorded_at = Utc::now();

    // Every decision is attempted: each is verified against its own journal
    // and committed on its own, so one permanently refused journal must not
    // stop the others from being recorded. All failures are reported.
    let mut failures = Vec::new();
    for decision in &missing {
        if let Err(error) = backfill_one_decision(
            &mut runtime,
            decision,
            &bound_history_directory,
            &expected_execution_identity,
            &network_routing_admissible,
            recorded_at,
        ) {
            eprintln!("decision {}: {error}", decision.decision_id);
            failures.push(decision.decision_id.clone());
        }
    }

    let after = runtime.attributed_hype();
    let still_missing = runtime.settled_purchases_without_acquisition();
    println!(
        "mode=backfill-complete attributed_hype_atoms={} settled_purchases={} missing={} \
         complete={}",
        after.credited_hype_atoms,
        after.settled_purchases_with_evidence,
        still_missing.len(),
        still_missing.is_empty()
    );
    // Attribution is withheld unless every settled purchase has its evidence,
    // so a run that leaves any behind must not report success: a caller
    // chaining on this command would otherwise believe the dashboard was
    // fixed.
    if !failures.is_empty() {
        return Err(format!(
            "{} settled purchase(s) could not be attributed ({}); attribution stays unavailable \
             until each is resolved",
            failures.len(),
            failures.join(", ")
        )
        .into());
    }
    Ok(())
}

/// What every signer-free, directory-bound command starts from.
struct BoundSignerFreeRuntime {
    config: Config,
    operational: OperationalParams,
    journal_directory: PathBuf,
    history_binding: HistoryBindingState,
    runtime: SignerFreeRuntime,
}

/// The preamble `release` and `backfill-attribution` share: the operational
/// config's write-once `history_directory` binding, the directory's
/// availability, and the exclusive runtime lock — in that order, so the two
/// commands cannot diverge on which directory they trust.
fn open_bound_signer_free_runtime(
    config_path: &str,
    security_policy_path: &str,
    runtime_config_path: &str,
    operational_params_path: &str,
    command: &str,
) -> Result<BoundSignerFreeRuntime, Box<dyn std::error::Error>> {
    let config = load_config(config_path, security_policy_path)?;
    let operational = OperationalParams::from_toml(&fs::read_to_string(operational_params_path)?)?;
    let history_directory = operational.history_directory.as_deref().ok_or_else(|| {
        format!("operational.toml is missing history_directory, required by `{command}`")
    })?;
    // Same write-once binding `prepare` enforces: the directory read below
    // must be the one every prepare for this operational config wrote to.
    let history_binding =
        HistoryDirectoryBinding::check(operational_params_path, history_directory)?;
    if history_binding.initialization == HistoryInitialization::FirstEver {
        return Err(format!(
            "history_directory was never initialized for this operational config; no prepare \
             ever ran through it, so there is nothing for `{command}` to work from"
        )
        .into());
    }
    let journal_directory = PathBuf::from(history_directory);
    ensure_history_directory_available(history_binding.initialization, &journal_directory)?;
    let runtime = open_signer_free_runtime(&config, runtime_config_path)?;
    Ok(BoundSignerFreeRuntime {
        config,
        operational,
        journal_directory,
        history_binding,
        runtime,
    })
}

/// Refuses to proceed unless `journal_directory` is the directory this
/// runtime's live decisions were prepared into (hash-chained, write-once).
/// `refused_action` completes "refusing to …" in the error.
fn ensure_runtime_prepared_into(
    runtime: &SignerFreeRuntime,
    journal_directory: &Path,
    refused_action: &str,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let bound_history_directory = fs::canonicalize(journal_directory)?;
    match runtime.live_history_directory() {
        Some(recorded) if recorded == bound_history_directory => Ok(bound_history_directory),
        Some(recorded) => Err(format!(
            "this runtime's live decisions were prepared into {} but this operational config \
             binds {}; refusing to {refused_action}",
            recorded.display(),
            bound_history_directory.display()
        )
        .into()),
        None => Err(format!(
            "this runtime never recorded a live history directory, so it has no journals of \
             its own; refusing to {refused_action}"
        )
        .into()),
    }
}

/// Reads one settled purchase's journal and commits its acquisition
/// evidence, under that journal's own append lock.
fn backfill_one_decision(
    runtime: &mut SignerFreeRuntime,
    decision: &DailyDecision,
    bound_history_directory: &Path,
    expected_execution_identity: &str,
    network_routing_admissible: &impl Fn(&Path) -> Result<(), WorkflowError>,
    recorded_at: DateTime<Utc>,
) -> Result<(), Box<dyn std::error::Error>> {
    let decision_id = decision.decision_id.as_str();
    let journal = runtime
        .live_journal_intent(decision_id)
        .ok_or(
            "settled a purchase but this runtime never recorded a journal for it; there is no \
             evidence to attribute it from",
        )?
        .to_path_buf();
    // A journal this runtime declared but that is no longer there is lost
    // history, not an empty journal: the operator needs the restore path
    // (bot-strategy#944), not a "crashed before its first append" story.
    // Only a genuine absence is that; an unreadable journal (wrong user,
    // root-owned after an earlier incident) is its own error.
    if let Err(error) = fs::metadata(&journal) {
        if error.kind() == std::io::ErrorKind::NotFound {
            return Err(format!(
                "declared journal {} is missing; recorded history is incomplete — restore it \
                 from backup (bot-strategy#944) before attributing anything",
                journal.display()
            )
            .into());
        }
        return Err(format!("journal {}: {error}", journal.display()).into());
    }
    // The intent is stored as `prepare` spelled it, which may be relative;
    // resolved from this process's working directory it must still land in
    // the directory the runtime bound, or the evidence is being read from
    // somewhere the runtime never wrote to.
    let resolved = fs::canonicalize(&journal)?;
    if resolved.parent() != Some(bound_history_directory) {
        return Err(format!(
            "declared journal {} resolves to {}, outside the bound history directory {}; \
             refusing to read evidence from a directory the runtime never prepared into",
            journal.display(),
            resolved.display(),
            bound_history_directory.display()
        )
        .into());
    }
    network_routing_admissible(&journal).map_err(box_error)?;
    let binding = DurableWorkflow::peek_committed_binding(&journal)?.ok_or_else(|| {
        format!(
            "journal {} has no committed binding; its outcome is unknown and must not be \
             attributed",
            journal.display()
        )
    })?;
    let (protected_head_store, owner_store) = build_stores(&journal)?;
    let workflow =
        DurableWorkflow::open_or_create(&journal, &binding, protected_head_store, owner_store)?;
    // Reads the evidence and commits it under the journal's own append
    // lock, exactly as `settle_finalized_decision` does: a concurrent
    // `reconcile` appending late contradictory evidence (moving the
    // workflow to `ManualReview`) must fail this closed rather than let
    // contested figures become an immutable attribution row.
    workflow.with_frozen_state(|state| -> Result<(), Box<dyn std::error::Error>> {
        let acquisition =
            verified_attribution_evidence(decision, &journal, state, expected_execution_identity)?;
        // The journal's own view of which decision it serves, so the runtime's
        // field-by-field `mismatch_against` does the decision check and names
        // the disagreeing field — the same shape `settle_finalized_decision`
        // uses, rather than a second, weaker comparison here.
        let identity = bound_decision_identity(state.binding());
        runtime.backfill_hype_acquisition(&identity, &acquisition, recorded_at)?;
        println!(
            "mode=backfilled decision={decision_id} workflow={} credited_hype_atoms={} journal={}",
            state.workflow_id(),
            state.purchased_hype().as_atoms(),
            journal.display()
        );
        Ok(())
    })
}

/// Shapes the acquisition evidence one settled decision's journal proves, or
/// fails closed.
///
/// `state` is the journal's state read under its own append lock by the
/// caller, so nothing can append between this verification and the commit
/// that follows it. Two gates are this path's own — the journal must belong
/// to the configured execution account, and its cash figures must be exactly
/// what the decision settled with — and the shared one, durable finality,
/// lives in [`LiveHypeAcquisition::from_finalized_workflow`]. Which decision
/// the journal serves is checked by the runtime against the journal's own
/// binding, not here.
fn verified_attribution_evidence(
    decision: &DailyDecision,
    journal: &Path,
    state: &WorkflowState,
    expected_execution_identity: &str,
) -> Result<LiveHypeAcquisition, Box<dyn std::error::Error>> {
    // The one check the network/routing predicate cannot make: that predicate
    // compares network and vault-routing *mode*, so a different account on
    // the same network passes it. `aggregate_terminal_residual_hype` refuses
    // a foreign execution identity explicitly, and this is a write path, so
    // it refuses one too.
    if state.binding().inventory_before.execution_identity_hash != expected_execution_identity {
        return Err(format!(
            "journal {} belongs to a different execution account than the one configured; \
             refusing to attribute another account's HYPE to this one",
            journal.display()
        )
        .into());
    }
    let acquisition = LiveHypeAcquisition::from_finalized_workflow(state, journal)?;
    if state.filled_usdc() != decision.filled_usdc || state.debited_usdc() != decision.debited_usdc
    {
        return Err(format!(
            "journal {} holds filled={} debited={} but the decision settled filled={} \
             debited={}; refusing to attribute inventory the capital ledger does not agree with",
            journal.display(),
            state.filled_usdc().as_micros(),
            state.debited_usdc().as_micros(),
            decision.filled_usdc.as_micros(),
            decision.debited_usdc.as_micros()
        )
        .into());
    }
    Ok(acquisition)
}

fn print_observation(
    observation: &hype_accumulator::live_probe::ProbeReconciliation,
) -> Result<(), serde_json::Error> {
    println!(
        "mode=reconciled durable_finality={} retry_authorized=false",
        observation.durable_finality
    );
    println!("{}", serde_json::to_string(observation)?);
    Ok(())
}

async fn reconcile(
    config_path: &str,
    security_policy_path: &str,
    runtime_config_path: &str,
    operational_params_path: &str,
    journal_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = load_config(config_path, security_policy_path)?;
    let operational = OperationalParams::from_toml(&fs::read_to_string(operational_params_path)?)?;
    PrepareTimeBinding::verify(
        journal_path,
        &PrepareTimeBinding::resolved(&config, &operational)?,
    )?;
    let binding = DurableWorkflow::peek_committed_binding(journal_path)?
        .ok_or("no committed workflow; reconciliation never prepares a new order")?;
    let (protected_head_store, owner_store) = build_stores(Path::new(journal_path))?;
    let mut workflow =
        DurableWorkflow::open_or_create(journal_path, &binding, protected_head_store, owner_store)?;
    // Deliberately do not validate live approval or load/decrypt the signer.
    // An expired approval and a revoked key must not prevent read-only recovery.
    let connector = build_read_only_connector(&config, &operational, &ProcessEnvironment)?;
    let now = Utc::now();
    let disabled_staking = disabled_staking_proof(&config, now);
    let observation = reconcile_prepared_order(
        &connector,
        &mut workflow,
        Path::new(journal_path),
        disabled_staking.as_ref(),
        now,
    )
    .await?;
    print_observation(&observation)?;
    // Signer-free like everything else here: settling the pacing decision
    // needs only the durable fill evidence and the runtime ledger.
    settle_finalized_decision(&config, runtime_config_path, &workflow, &observation)?;
    Ok(())
}

fn build_read_only_connector<E: hype_accumulator::config::Environment>(
    config: &Config,
    operational: &OperationalParams,
    environment: &E,
) -> Result<HyperliquidConnector, Box<dyn std::error::Error>> {
    let account_address = config.observation_account(environment)?;
    HyperliquidConnector::new(HyperliquidConnectorConfig {
        base_url: config.hyperliquid.endpoint.clone(),
        tracked_symbols: Vec::new(),
    })
    .map_err(box_error)?
    .with_account(HyperliquidAccountConfig {
        account_address,
        signer_private_key: None,
        vault_address: None,
        is_mainnet: operational.is_mainnet,
        nonce_state_path: None,
        max_taker_notional: None,
        max_taker_slippage_bps: None,
        max_taker_book_age_ms: operational.max_taker_book_age_ms,
    })
    .map_err(box_error)
}

/// Proof, for the workflow, that the policy in force disables staking
/// (bot-strategy#993) — or `None`, which leaves a real purchase at
/// `OrderFinalized` rather than attesting anything.
///
/// Gated on the **full** live-contract validation (`effective_live_order_
/// policy`): validation refuses `staking.enabled = true`, and it also checks
/// the configured acknowledgement against the policy's expected digest, so a
/// cleared or mismatched acknowledgement withholds the proof even while its
/// expiry lies in the future. Read-only recovery must not depend on any of
/// this, so a failure only withholds the proof: lookup, fill recording and
/// settlement still run. The staking-section digest survives an
/// acknowledgement renewal; the whole-policy version does not, and only
/// matters for decisions bound before the digest existed.
fn disabled_staking_proof(config: &Config, now: DateTime<Utc>) -> Option<DisabledStakingProof> {
    let validated = config
        .effective_live_order_policy(&ProcessEnvironment, now)
        .and_then(|_| config.effective_security_policy_digest(&ProcessEnvironment, now))
        .and_then(|validated_policy_version| {
            Ok(DisabledStakingProof {
                staking_policy_digest: config.staking_policy_digest()?,
                validated_policy_version,
            })
        });
    match validated {
        Ok(proof) => Some(proof),
        Err(error) => {
            eprintln!(
                "note: security policy is not live-valid ({error}); a real purchase cannot \
                 complete its workflow until it is (bot-strategy#993)"
            );
            None
        }
    }
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

fn build_stores(journal_path: &Path) -> Result<WorkflowStores, Box<dyn std::error::Error>> {
    let head_path = DurableWorkflow::protected_head_path_for(journal_path);
    let protected_head_store: Arc<dyn ProtectedWorkflowHeadStore> =
        Arc::new(FileProtectedWorkflowHeadStore::new(head_path)?);
    // Deliberately outside the per-journal path: this store must be shared
    // across every workflow for this execution identity, never scoped to
    // one decision's journal (see `FileExchangeOrderOwnerStore`'s doc
    // comment and bot-strategy#845 PR #28's review note on this exact risk).
    let owner_store_path = journal_path
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .join("exchange-order-owners.json");
    let owner_store: Arc<dyn ExchangeOrderOwnerStore> =
        Arc::new(FileExchangeOrderOwnerStore::new(owner_store_path)?);
    Ok((protected_head_store, owner_store))
}

fn build_prepare_policies(
    effective: &EffectiveLiveOrderPolicy,
    operational: &OperationalParams,
    policy_version: String,
    staking_policy_digest: String,
) -> (OrderEnvelopeFreshnessPolicy, EligibilityPolicyBinding) {
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
        staking_policy_digest: Some(staking_policy_digest),
    };
    (envelope_policy, eligibility_policy)
}

/// A missing `journal_directory` is only ever the normal state before this
/// account's genuinely first-ever `prepare` (aggregation then correctly
/// treats it as zero historical journals). Once history has been
/// initialized before, its disappearance — deleted, unmounted, unavailable
/// storage — must fail closed: silently proceeding would let
/// `open_or_create` recreate an empty directory and every prior residual
/// allocation vanish from aggregation without a trace, since the
/// live-balance check only ever rejects a total that's too large, never
/// one that's suspiciously small.
///
/// Existence alone is not enough, and never was (bot-strategy#944): an
/// unmounted journal filesystem can leave an ordinary, empty mount-point
/// directory behind, and a directory deleted then recreated empty passes
/// `is_dir()` exactly the same way a genuinely-preserved one would. That
/// half is closed by `recorded_journals`, the durable journal record from
/// `HistoryDirectoryBinding` (which lives outside `journal_directory` and
/// so survives its loss), enforced inside the verified scans themselves — `aggregate_terminal_residual_hype` and
/// `bound_decision_ids` — so history cannot go missing between the check
/// and the scan that depends on it. This check stays separate because it
/// has to be answered *before* `prepare`'s `create_dir_all` makes the
/// directory exist either way.
fn ensure_history_directory_available(
    history_initialization: HistoryInitialization,
    journal_directory: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    if history_initialization == HistoryInitialization::AlreadyInitialized
        && !journal_directory.is_dir()
    {
        return Err(format!(
            "history_directory {} was already initialized for this account but does not exist \
             (deleted, unmounted, or unavailable?); refusing to silently start aggregating from \
             an empty directory. Restore it before running prepare again.",
            journal_directory.display()
        )
        .into());
    }
    Ok(())
}

/// `DurableWorkflow::aggregate_terminal_residual_hype` only ever
/// rediscovers a `.jsonl` journal directly inside one stable directory. A
/// journal with any other extension would complete normally today but
/// become permanently invisible to every later `prepare`'s aggregation —
/// and its own protected-head sidecar would then look orphaned, blocking
/// the account entirely. Likewise, a journal placed outside
/// `history_directory` (e.g. a caller date-partitioning `journal_path` by
/// subdirectory, with no durable registry or configured root requiring
/// every invocation to share one parent) would itself never be
/// rediscovered by a later run's aggregation, silently omitting whatever
/// residual it goes on to record. Both are refused up front, before
/// anything is written, rather than letting either the extension or the
/// directory choice quietly break history discovery later.
fn validate_journal_path(
    journal_path: &str,
    history_directory: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let path = Path::new(journal_path);
    if path.extension().and_then(std::ffi::OsStr::to_str) != Some("jsonl") {
        return Err(format!(
            "journal_path must end in exactly \".jsonl\" (lowercase) so later aggregation can \
             rediscover it, got {journal_path:?}"
        )
        .into());
    }
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    if parent != history_directory {
        return Err(format!(
            "journal_path's directory ({}) does not match operational.toml's configured \
             history_directory ({}); every journal for this account must live directly in the \
             same configured history_directory, or later aggregation will never rediscover it",
            parent.display(),
            history_directory.display()
        )
        .into());
    }
    Ok(())
}

fn historical_protected_head_store_for(
    path: &Path,
) -> Result<Arc<dyn ProtectedWorkflowHeadStore>, WorkflowError> {
    let head_path = DurableWorkflow::protected_head_path_for(path);
    FileProtectedWorkflowHeadStore::new(head_path)
        .map(|store| Arc::new(store) as Arc<dyn ProtectedWorkflowHeadStore>)
        .map_err(WorkflowError::ProtectedHead)
}

/// `execution_identity_hash` alone does not distinguish testnet from
/// mainnet, or one vault-address routing mode from another, for the same
/// address — this checks each historical journal's own `PrepareTimeBinding`
/// sidecar against `current`, so a foreign-network or foreign-routing
/// journal sharing this directory is rejected by
/// `aggregate_terminal_residual_hype` rather than silently aggregated.
///
/// Known residual risk, deliberately out of scope here: this reads the
/// sidecar `.network-binding.json` written by plain `fs::write`, not a
/// journal's own hash-chained, protected-head-anchored content — an
/// attacker with write access to just this file (not the journal itself)
/// could still relabel a testnet journal's residual as mainnet-admissible
/// without invalidating the journal's own protected head. Closing this
/// fully means either folding network/routing identity into
/// `live_probe.rs`'s already-merged `execution_identity_hash` computation,
/// or building an independent protection mechanism for this sidecar —
/// both out of scope for this aggregator-foundation PR. Tracked in
/// bot-strategy#942.
/// Records the journals a verified history scan just validated.
///
/// Called by the aggregation itself, through [`HistoryScanRecorder`], the
/// moment that scan succeeds and before this run's own journal exists — the
/// only point where the set is known and a failed write can still be
/// retried. The existing-binding retry path never scans history and so
/// never reaches this: it reuses the binding already on disk, computes no
/// inventory from history, and must leave whatever earlier runs recorded
/// exactly as it is.
///
/// Only `prepare` records. `release` deliberately does not: its own scan
/// does not exclude the journal of the day being released, so it would
/// record a journal whose decision may still be released, and a set is only
/// ever allowed to grow.
fn record_validated_history_journals(
    operational_params_path: &str,
    validated: &RefCell<BTreeSet<String>>,
) -> Result<(), WorkflowError> {
    HistoryDirectoryBinding::record_validated_journals(operational_params_path, &validated.borrow())
        .map_err(|error| WorkflowError::HistoryRecordWrite(error.to_string()))
}

/// Wraps a journal-admissibility check so a caller can learn *which*
/// journals a verified scan accepted, without rescanning the directory
/// itself.
///
/// Both verified scans call this check once per journal, before every other
/// per-journal check, and abort the whole scan on the first failure of any
/// of them — so when a scan returns `Ok`, the set holds exactly the
/// journals that scan validated. That is the only set the durable record
/// may be written from: collecting the directory separately would record
/// files a scan is about to reject, and the operator removing such a file
/// to repair the directory would then wedge every later run against a
/// journal that never belonged there.
fn collecting_journal_admissible<'a>(
    inner: impl Fn(&Path) -> Result<(), WorkflowError> + 'a,
    accepted: &'a RefCell<BTreeSet<String>>,
) -> impl Fn(&Path) -> Result<(), WorkflowError> + 'a {
    move |path: &Path| {
        inner(path)?;
        let name = path
            .file_name()
            .and_then(std::ffi::OsStr::to_str)
            .ok_or_else(|| {
                WorkflowError::CorruptJournal(format!(
                    "{}: journal path has no valid UTF-8 file name",
                    path.display()
                ))
            })?;
        accepted.borrow_mut().insert(name.to_owned());
        Ok(())
    }
}

fn network_routing_admissible_for(
    current: &PrepareTimeBinding,
) -> impl Fn(&Path) -> Result<(), WorkflowError> + '_ {
    move |path: &Path| {
        let path_str = path.to_str().ok_or_else(|| {
            WorkflowError::CorruptJournal(format!(
                "{}: journal path is not valid UTF-8",
                path.display()
            ))
        })?;
        let recorded = PrepareTimeBinding::read(path_str)
            .map_err(|error| WorkflowError::CorruptJournal(error.to_string()))?;
        if !recorded.same_network_and_routing_as(current) {
            return Err(WorkflowError::CorruptJournal(format!(
                "{path_str}: journal was prepared under a different network or vault-address \
                 routing mode ({recorded:?}) than the account currently being aggregated for \
                 ({current:?})"
            )));
        }
        Ok(())
    }
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
    use super::{
        invocation, network_routing_admissible_for, validate_journal_path, HistoryDirectoryBinding,
        HistoryInitialization, Invocation, PrepareTimeBinding,
    };
    use std::cell::RefCell;
    use std::collections::BTreeSet;
    use std::path::Path;

    #[test]
    fn history_directory_binding_distinguishes_first_ever_from_already_initialized() {
        let directory = tempfile::tempdir().expect("temp dir");
        let operational_params_path = directory.path().join("operational.toml");
        let operational_params_path = operational_params_path.to_str().expect("utf8 path");
        let first = HistoryDirectoryBinding::write_once_and_verify(
            operational_params_path,
            "/opt/hype-accumulator/journals",
        )
        .expect("first write");
        assert_eq!(first, HistoryInitialization::FirstEver);

        let second = HistoryDirectoryBinding::write_once_and_verify(
            operational_params_path,
            "/opt/hype-accumulator/journals",
        )
        .expect("matching re-verify");
        assert_eq!(second, HistoryInitialization::AlreadyInitialized);
    }

    #[test]
    fn history_directory_binding_rejects_a_changed_directory() {
        let directory = tempfile::tempdir().expect("temp dir");
        let operational_params_path = directory.path().join("operational.toml");
        let operational_params_path = operational_params_path.to_str().expect("utf8 path");
        HistoryDirectoryBinding::write_once_and_verify(
            operational_params_path,
            "/opt/hype-accumulator/journals",
        )
        .expect("first write");

        // Simulates an operator editing operational.toml's
        // history_directory after journals already exist under the
        // original one.
        assert!(HistoryDirectoryBinding::write_once_and_verify(
            operational_params_path,
            "/opt/hype-accumulator/journals-v2",
        )
        .is_err());

        // The original value still verifies.
        HistoryDirectoryBinding::write_once_and_verify(
            operational_params_path,
            "/opt/hype-accumulator/journals",
        )
        .expect("original value still verifies");
    }

    #[test]
    fn history_directory_binding_fails_closed_on_an_unreadable_binding() {
        let directory = tempfile::tempdir().expect("temp dir");
        let operational_params_path = directory.path().join("operational.toml");
        let operational_params_path = operational_params_path.to_str().expect("utf8 path");

        // A binding file that exists but isn't valid JSON (truncated write,
        // corruption after a crash) must never be treated as "no binding
        // yet" — that would silently let a changed history_directory
        // through as though this were a first-ever prepare.
        let binding_path = HistoryDirectoryBinding::path(operational_params_path);
        std::fs::write(&binding_path, b"not valid json").expect("write corrupt binding");

        assert!(HistoryDirectoryBinding::write_once_and_verify(
            operational_params_path,
            "/opt/hype-accumulator/journals",
        )
        .is_err());

        // The corrupt file must be left alone, not silently overwritten.
        assert_eq!(
            std::fs::read_to_string(&binding_path).expect("binding still present"),
            "not valid json"
        );
    }

    #[test]
    fn persist_first_ever_never_overwrites_an_existing_binding() {
        let directory = tempfile::tempdir().expect("temp dir");
        let operational_params_path = directory.path().join("operational.toml");
        let operational_params_path = operational_params_path.to_str().expect("utf8 path");

        HistoryDirectoryBinding::persist_first_ever(
            operational_params_path,
            "/opt/hype-accumulator/journals",
        )
        .expect("first persist");

        // Simulates two concurrent first-ever `prepare` invocations for the
        // same operational_params_path both observing `check` == FirstEver
        // before either has persisted: the second `persist_first_ever` must
        // fail rather than silently replacing the first writer's binding
        // with a different (or even identical) value.
        assert!(HistoryDirectoryBinding::persist_first_ever(
            operational_params_path,
            "/opt/hype-accumulator/journals-v2",
        )
        .is_err());

        // The first writer's binding is untouched, and no stray temporary
        // file is left behind in the directory.
        let binding_path = HistoryDirectoryBinding::path(operational_params_path);
        let persisted: HistoryDirectoryBinding =
            serde_json::from_str(&std::fs::read_to_string(&binding_path).expect("binding present"))
                .expect("valid json");
        assert_eq!(
            persisted.history_directory,
            "/opt/hype-accumulator/journals"
        );
        let leftover_temp_files = std::fs::read_dir(directory.path())
            .expect("read temp dir")
            .filter_map(Result::ok)
            .filter(|entry| entry.path() != binding_path)
            .count();
        assert_eq!(leftover_temp_files, 0);
    }

    #[test]
    fn ensure_history_directory_available_only_requires_the_directory_when_already_initialized() {
        use super::ensure_history_directory_available;

        let temp = tempfile::tempdir().expect("temp dir");
        let missing = temp.path().join("does-not-exist");

        // A genuinely first-ever prepare finding no directory yet is normal.
        assert!(
            ensure_history_directory_available(HistoryInitialization::FirstEver, &missing).is_ok()
        );

        // Once history was already initialized before, the same missing
        // directory must fail closed rather than silently restart empty.
        assert!(ensure_history_directory_available(
            HistoryInitialization::AlreadyInitialized,
            &missing,
        )
        .is_err());

        // An existing directory always passes, regardless of
        // initialization state.
        assert!(ensure_history_directory_available(
            HistoryInitialization::AlreadyInitialized,
            temp.path(),
        )
        .is_ok());
    }

    #[test]
    fn collecting_journal_admissible_collects_only_accepted_journals() {
        use super::collecting_journal_admissible;
        use hype_accumulator::workflow::WorkflowError;

        let accepted = RefCell::new(BTreeSet::new());
        let collecting = collecting_journal_admissible(
            |path: &Path| {
                if path.ends_with("foreign.jsonl") {
                    return Err(WorkflowError::CorruptJournal("foreign".into()));
                }
                Ok(())
            },
            &accepted,
        );

        collecting(Path::new("/journals/day-1.jsonl")).expect("admissible");
        collecting(Path::new("/journals/day-2.jsonl")).expect("admissible");

        // A rejected journal must not be collected — the scan aborts here,
        // so it must never be recorded as if it had been validated.
        assert!(collecting(Path::new("/journals/foreign.jsonl")).is_err());
        assert_eq!(
            *accepted.borrow(),
            BTreeSet::from(["day-1.jsonl".to_owned(), "day-2.jsonl".to_owned()])
        );
    }

    #[test]
    fn a_concurrent_record_waits_for_the_binding_lock() {
        use super::record_validated_history_journals;
        use std::{sync::mpsc, thread, time::Duration};

        let directory = tempfile::tempdir().expect("temp dir");
        let operational_params_path = directory.path().join("operational.toml");
        let operational_params_path = operational_params_path.to_str().expect("utf8 path");
        let journals = directory.path().join("journals");
        std::fs::create_dir(&journals).expect("create journals");
        let journals_str = journals.to_str().expect("utf8 path");
        HistoryDirectoryBinding::persist_first_ever(operational_params_path, journals_str)
            .expect("persist binding");
        let recorded = RefCell::new(BTreeSet::from(["day-1.jsonl".to_owned()]));
        record_validated_history_journals(operational_params_path, &recorded).expect("record");

        // Stands in for a second `prepare` that is between reading this
        // binding and publishing its own set: it holds the same lock.
        let binding_path = HistoryDirectoryBinding::path(operational_params_path);
        let held = HistoryDirectoryBinding::lock(&binding_path).expect("lock");

        let owned_path = operational_params_path.to_owned();
        let (finished, has_finished) = mpsc::channel();
        let writer = thread::spawn(move || {
            let scanned = RefCell::new(BTreeSet::from([
                "day-1.jsonl".to_owned(),
                "day-2.jsonl".to_owned(),
            ]));
            let result = record_validated_history_journals(&owned_path, &scanned);
            finished.send(()).expect("signal");
            result
        });

        // The write it is about to make would succeed on its own, so if it
        // completes while the lock is held, the read-check-publish is not
        // serialized at all and a slower writer could publish a stale set
        // over a larger one.
        thread::sleep(Duration::from_millis(200));
        assert!(
            has_finished.try_recv().is_err(),
            "a second writer must wait for the binding lock"
        );

        drop(held);
        writer
            .join()
            .expect("join")
            .expect("record after the lock is released");
        assert_eq!(
            HistoryDirectoryBinding::check(operational_params_path, journals_str)
                .expect("check binding")
                .recorded_journals,
            BTreeSet::from(["day-1.jsonl".to_owned(), "day-2.jsonl".to_owned()])
        );
    }

    #[test]
    fn a_scan_that_forgets_a_recorded_journal_is_refused() {
        use super::record_validated_history_journals;
        use hype_accumulator::workflow::WorkflowError;

        let directory = tempfile::tempdir().expect("temp dir");
        let operational_params_path = directory.path().join("operational.toml");
        let operational_params_path = operational_params_path.to_str().expect("utf8 path");
        let journals = directory.path().join("journals");
        std::fs::create_dir(&journals).expect("create journals");
        let journals_str = journals.to_str().expect("utf8 path");
        HistoryDirectoryBinding::persist_first_ever(operational_params_path, journals_str)
            .expect("persist binding");

        let first = RefCell::new(BTreeSet::from([
            "day-1.jsonl".to_owned(),
            "day-2.jsonl".to_owned(),
        ]));
        record_validated_history_journals(operational_params_path, &first).expect("record");

        // A later scan that no longer sees `day-1.jsonl` must fail loudly
        // rather than quietly rewriting the record: the record is the only
        // surviving evidence that journal was ever there. A replacement
        // journal keeping the count the same must not paper over it.
        let regressed = RefCell::new(BTreeSet::from([
            "day-2.jsonl".to_owned(),
            "day-3.jsonl".to_owned(),
        ]));
        let error = record_validated_history_journals(operational_params_path, &regressed)
            .expect_err("a forgotten journal must not be recorded");
        assert!(
            matches!(&error, WorkflowError::HistoryRecordWrite(message) if message.contains("day-1.jsonl")),
            "unexpected error: {error}"
        );

        let grown = RefCell::new(BTreeSet::from([
            "day-1.jsonl".to_owned(),
            "day-2.jsonl".to_owned(),
            "day-3.jsonl".to_owned(),
        ]));
        record_validated_history_journals(operational_params_path, &grown).expect("record");
        assert_eq!(
            HistoryDirectoryBinding::check(operational_params_path, journals_str)
                .expect("check binding")
                .recorded_journals,
            *grown.borrow(),
            "a grown journal set is recorded, and the earlier ones survive"
        );

        // Advancing preserves the bound directory rather than rewriting it:
        // the write-once property protects `history_directory`, not the set.
        let persisted: HistoryDirectoryBinding = serde_json::from_str(
            &std::fs::read_to_string(HistoryDirectoryBinding::path(operational_params_path))
                .expect("binding present"),
        )
        .expect("valid json");
        assert_eq!(persisted.history_directory, journals_str);
    }

    #[test]
    fn a_binding_written_before_the_journal_record_existed_still_reads() {
        let directory = tempfile::tempdir().expect("temp dir");
        let operational_params_path = directory.path().join("operational.toml");
        let operational_params_path = operational_params_path.to_str().expect("utf8 path");

        // Exactly the shape deployed before bot-strategy#944: no mark field.
        std::fs::write(
            HistoryDirectoryBinding::path(operational_params_path),
            br#"{"history_directory": "/opt/hype-accumulator/journals"}"#,
        )
        .expect("legacy binding");

        let state = HistoryDirectoryBinding::check(
            operational_params_path,
            "/opt/hype-accumulator/journals",
        )
        .expect("legacy binding reads");
        assert_eq!(
            state.initialization,
            HistoryInitialization::AlreadyInitialized
        );
        assert!(state.recorded_journals.is_empty());
    }

    #[test]
    fn journal_path_extension_must_be_exactly_lowercase_jsonl() {
        let history = Path::new("/opt/hype-accumulator/journals");
        assert!(
            validate_journal_path("/opt/hype-accumulator/journals/journal.jsonl", history).is_ok()
        );
        assert!(
            validate_journal_path("/opt/hype-accumulator/journals/journal.JSONL", history).is_err()
        );
        assert!(
            validate_journal_path("/opt/hype-accumulator/journals/journal.jsonl.bak", history)
                .is_err()
        );
        assert!(validate_journal_path("/opt/hype-accumulator/journals/journal", history).is_err());
        assert!(
            validate_journal_path("/opt/hype-accumulator/journals/journal.json", history).is_err()
        );
    }

    #[test]
    fn journal_path_must_live_directly_in_the_configured_history_directory() {
        let history = Path::new("/opt/hype-accumulator/journals");
        assert!(
            validate_journal_path("/opt/hype-accumulator/journals/day-1.jsonl", history).is_ok()
        );
        // A date-partitioned subdirectory would make this journal
        // permanently invisible to later aggregation, which only ever
        // scans `history_directory` itself.
        assert!(validate_journal_path(
            "/opt/hype-accumulator/journals/2026-09-07/day.jsonl",
            history
        )
        .is_err());
        assert!(validate_journal_path("/some/other/place/day-1.jsonl", history).is_err());
    }

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
                "runtime.toml",
                "operational.toml",
                "journal.jsonl",
                "--confirm",
                "0xabc123",
            ])),
            Ok(Invocation::Submit {
                config_path: "config.toml".to_owned(),
                security_policy_path: "security-policy.toml".to_owned(),
                runtime_config_path: "runtime.toml".to_owned(),
                operational_params_path: "operational.toml".to_owned(),
                journal_path: "journal.jsonl".to_owned(),
                confirm_client_order_id: "0xabc123".to_owned(),
            })
        );
    }

    #[test]
    fn recovery_takes_the_runtime_config_but_no_confirmation() {
        assert_eq!(
            invocation(args(&[
                "reconcile",
                "config.toml",
                "policy.toml",
                "runtime.toml",
                "operational.toml",
                "journal.jsonl"
            ])),
            Ok(Invocation::Reconcile {
                config_path: "config.toml".to_owned(),
                security_policy_path: "policy.toml".to_owned(),
                runtime_config_path: "runtime.toml".to_owned(),
                operational_params_path: "operational.toml".to_owned(),
                journal_path: "journal.jsonl".to_owned(),
            })
        );
        // The pre-settlement 5-argument shape must be rejected rather than
        // silently reinterpreted with a shifted argument.
        assert!(invocation(args(&[
            "reconcile",
            "config.toml",
            "policy.toml",
            "operational.toml",
            "journal.jsonl"
        ]))
        .is_err());
        assert!(invocation(args(&[
            "reconcile",
            "config.toml",
            "policy.toml",
            "runtime.toml",
            "operational.toml",
            "journal.jsonl",
            "--confirm",
            "0xabc123"
        ]))
        .is_err());
    }

    #[test]
    fn read_only_recovery_never_reads_signing_material_or_requires_live_approval() {
        struct AccountOnly;
        impl hype_accumulator::config::Environment for AccountOnly {
            fn get(&self, name: &str) -> Option<String> {
                assert_eq!(
                    name, "HYPE_ACCOUNT_ID",
                    "recovery tried to read signing material"
                );
                Some("0x1111111111111111111111111111111111111111".to_owned())
            }
        }
        let config =
            hype_accumulator::config::Config::from_toml(include_str!("../../config/example.toml"))
                .unwrap();
        assert!(config.manual_halt);
        assert!(!config.live_approved);
        let operational = super::OperationalParams::from_toml(include_str!(
            "../../config/live-probe-operational.example.toml"
        ))
        .unwrap();
        let connector =
            super::build_read_only_connector(&config, &operational, &AccountOnly).unwrap();
        assert!(connector.api_wallet_address().is_err());
        assert_eq!(
            connector.execution_account_address().unwrap(),
            "0x1111111111111111111111111111111111111111"
        );
    }

    #[test]
    fn operational_params_without_history_directory_still_parses_for_submit_and_reconcile() {
        // A legacy operational.toml written before `history_directory`
        // existed must still deserialize: `submit`/`reconcile` never read
        // that field (only `prepare` does, and enforces its presence
        // itself), so requiring it here would block recovering an
        // already-prepared or already-submitted order after an upgrade.
        let legacy_toml = r#"
            is_mainnet = false
            max_taker_notional_usdc = "25.0"
            max_taker_slippage_bps = 20
            max_taker_book_age_ms = 5000
            order_timeout_seconds = 10
            order_book_depth = 5
        "#;
        let operational = super::OperationalParams::from_toml(legacy_toml)
            .expect("legacy operational.toml without history_directory must still parse");
        assert!(operational.history_directory.is_none());
    }

    #[test]
    fn parses_a_release_invocation_without_a_journal() {
        assert_eq!(
            invocation(args(&[
                "release",
                "config.toml",
                "policy.toml",
                "runtime.toml",
                "operational.toml",
            ])),
            Ok(Invocation::Release {
                config_path: "config.toml".to_owned(),
                security_policy_path: "policy.toml".to_owned(),
                runtime_config_path: "runtime.toml".to_owned(),
                operational_params_path: "operational.toml".to_owned(),
            })
        );
        // A journal argument is not accepted: release scans the bound
        // history directory itself and must never be pointed at one file.
        assert!(invocation(args(&[
            "release",
            "config.toml",
            "policy.toml",
            "runtime.toml",
            "operational.toml",
            "journal.jsonl",
        ]))
        .is_err());
    }

    #[test]
    fn parses_a_backfill_attribution_invocation_without_a_journal() {
        assert_eq!(
            invocation(args(&[
                "backfill-attribution",
                "config.toml",
                "policy.toml",
                "runtime.toml",
                "operational.toml",
            ])),
            Ok(Invocation::BackfillAttribution {
                config_path: "config.toml".to_owned(),
                security_policy_path: "policy.toml".to_owned(),
                runtime_config_path: "runtime.toml".to_owned(),
                operational_params_path: "operational.toml".to_owned(),
            })
        );
        // Like `release`, it works from the decisions the runtime itself
        // holds and must never be pointed at one journal file.
        assert!(invocation(args(&[
            "backfill-attribution",
            "config.toml",
            "policy.toml",
            "runtime.toml",
            "operational.toml",
            "journal.jsonl",
        ]))
        .is_err());
        // And it is not `release`: the two five-argument commands must not
        // be confusable.
        assert_ne!(
            invocation(args(&[
                "backfill-attribution",
                "config.toml",
                "policy.toml",
                "runtime.toml",
                "operational.toml",
            ])),
            invocation(args(&[
                "release",
                "config.toml",
                "policy.toml",
                "runtime.toml",
                "operational.toml",
            ]))
        );
    }

    #[test]
    fn rejects_submit_without_the_literal_confirm_flag() {
        // A caller must pass the `--confirm` flag literally, not just any
        // 8-argument submit invocation — this is the one thing standing
        // between an operator and an actual signed submission.
        assert!(invocation(args(&[
            "submit",
            "config.toml",
            "security-policy.toml",
            "runtime.toml",
            "operational.toml",
            "journal.jsonl",
            "--yes",
            "0xabc123",
        ]))
        .is_err());
        // The pre-settlement 7-argument shape (no runtime config) is rejected
        // outright instead of being parsed with shifted paths.
        assert!(invocation(args(&[
            "submit",
            "config.toml",
            "security-policy.toml",
            "operational.toml",
            "journal.jsonl",
            "--confirm",
            "0xabc123",
        ]))
        .is_err());
    }

    #[test]
    fn prepare_time_binding_write_once_is_idempotent_for_the_same_binding() {
        let directory = tempfile::tempdir().expect("temp dir");
        let journal_path = directory.path().join("journal.jsonl");
        let journal_path = journal_path.to_str().expect("utf8 path");
        let binding = PrepareTimeBinding::new("https://api.hyperliquid.xyz".to_owned(), true, true);
        binding.write_once(journal_path).expect("first write");
        binding
            .write_once(journal_path)
            .expect("identical replay is a no-op");
    }

    #[test]
    fn prepare_time_binding_write_once_rejects_a_changed_binding() {
        let directory = tempfile::tempdir().expect("temp dir");
        let journal_path = directory.path().join("journal.jsonl");
        let journal_path = journal_path.to_str().expect("utf8 path");
        PrepareTimeBinding::new("https://api.hyperliquid.xyz".to_owned(), true, true)
            .write_once(journal_path)
            .expect("first write");
        // Same endpoint/network, but the routing mode this journal was
        // prepared with (subaccount/vault) no longer matches — this is
        // exactly the case a config edit between `prepare` and a re-run of
        // `prepare` (or `submit`) must not silently pass through.
        let changed =
            PrepareTimeBinding::new("https://api.hyperliquid.xyz".to_owned(), true, false);
        assert!(changed.write_once(journal_path).is_err());
    }

    #[test]
    fn prepare_time_binding_verify_rejects_a_routing_mode_that_changed_since_prepare() {
        let directory = tempfile::tempdir().expect("temp dir");
        let journal_path = directory.path().join("journal.jsonl");
        let journal_path = journal_path.to_str().expect("utf8 path");
        let prepared_with_vault_routing =
            PrepareTimeBinding::new("https://api.hyperliquid.xyz".to_owned(), true, true);
        prepared_with_vault_routing
            .write_once(journal_path)
            .expect("prepare records vault routing");

        // Same endpoint/network as `submit` would independently re-derive,
        // but `execution_account_kind` was edited back to a dedicated
        // master account before `submit` ran.
        let now_resolves_to_master =
            PrepareTimeBinding::new("https://api.hyperliquid.xyz".to_owned(), true, false);
        assert!(
            PrepareTimeBinding::verify(journal_path, &now_resolves_to_master).is_err(),
            "submit must refuse when the vault-address routing mode no longer matches prepare"
        );

        // The unchanged binding still verifies.
        PrepareTimeBinding::verify(journal_path, &prepared_with_vault_routing)
            .expect("matching binding verifies");
    }

    #[test]
    fn network_routing_admissible_for_rejects_a_different_network_or_routing_mode() {
        let directory = tempfile::tempdir().expect("temp dir");
        let mainnet_path = directory.path().join("mainnet.jsonl");
        let mainnet_path_str = mainnet_path.to_str().expect("utf8 path");
        let mainnet =
            PrepareTimeBinding::new("https://api.hyperliquid.xyz".to_owned(), true, false);
        mainnet
            .write_once(mainnet_path_str)
            .expect("mainnet journal prepared");

        let testnet_path = directory.path().join("testnet.jsonl");
        let testnet_path_str = testnet_path.to_str().expect("utf8 path");
        let testnet = PrepareTimeBinding::new(
            "https://api.hyperliquid-testnet.xyz".to_owned(),
            false,
            false,
        );
        testnet
            .write_once(testnet_path_str)
            .expect("testnet journal prepared");

        let vault_routed_path = directory.path().join("vault.jsonl");
        let vault_routed_path_str = vault_routed_path.to_str().expect("utf8 path");
        let vault_routed =
            PrepareTimeBinding::new("https://api.hyperliquid.xyz".to_owned(), true, true);
        vault_routed
            .write_once(vault_routed_path_str)
            .expect("vault-routed journal prepared");

        let admissible_for_mainnet = network_routing_admissible_for(&mainnet);
        assert!(
            admissible_for_mainnet(&mainnet_path).is_ok(),
            "a journal prepared under the same network and routing mode is admissible"
        );
        assert!(
            admissible_for_mainnet(&testnet_path).is_err(),
            "a testnet journal must not be aggregated into a mainnet run"
        );
        assert!(
            admissible_for_mainnet(&vault_routed_path).is_err(),
            "a different vault-address routing mode must not be aggregated together"
        );
    }

    #[test]
    fn network_routing_admissible_for_tolerates_a_benign_endpoint_change() {
        // A completed journal's write-once binding can never be updated;
        // an endpoint URL migration or failover on the same network must
        // not permanently exclude every already-completed journal, unlike
        // `PrepareTimeBinding::verify`'s exact-equality check (correct for
        // its own submit-safety purpose, wrong for aggregation).
        let directory = tempfile::tempdir().expect("temp dir");
        let path = directory.path().join("day-1.jsonl");
        let path_str = path.to_str().expect("utf8 path");
        PrepareTimeBinding::new("https://api.hyperliquid.xyz".to_owned(), true, false)
            .write_once(path_str)
            .expect("journal prepared against the old endpoint");

        let current_after_endpoint_migration =
            PrepareTimeBinding::new("https://api2.hyperliquid.xyz".to_owned(), true, false);
        let admissible = network_routing_admissible_for(&current_after_endpoint_migration);
        assert!(
            admissible(&path).is_ok(),
            "a same-network, same-routing journal remains admissible across an endpoint change"
        );
    }

    #[test]
    fn prepare_time_binding_reads_a_legacy_binding_missing_the_routing_field() {
        let directory = tempfile::tempdir().expect("temp dir");
        let journal_path = directory.path().join("journal.jsonl");
        let binding_path = directory.path().join("journal.network-binding.json");
        // A journal written before requires_vault_address_routing existed.
        std::fs::write(
            &binding_path,
            r#"{"endpoint":"https://api.hyperliquid.xyz","is_mainnet":true}"#,
        )
        .expect("legacy binding file");
        let journal_path = journal_path.to_str().expect("utf8 path");

        // The historical (pre-vault-routing) config still verifies.
        let unchanged =
            PrepareTimeBinding::new("https://api.hyperliquid.xyz".to_owned(), true, false);
        PrepareTimeBinding::verify(journal_path, &unchanged)
            .expect("legacy binding deserializes and matches the historical no-routing value");

        // A config that now requires vault routing still correctly refuses:
        // the legacy journal's absent field must not silently satisfy it.
        let now_requires_routing =
            PrepareTimeBinding::new("https://api.hyperliquid.xyz".to_owned(), true, true);
        assert!(PrepareTimeBinding::verify(journal_path, &now_requires_routing).is_err());

        // A retry of `prepare` against the same unchanged config is also
        // still accepted (write_once must not choke on the legacy file
        // either).
        unchanged
            .write_once(journal_path)
            .expect("retrying prepare against an unchanged legacy binding succeeds");
    }

    #[test]
    fn prepare_time_binding_verify_requires_a_prior_prepare() {
        let directory = tempfile::tempdir().expect("temp dir");
        let journal_path = directory.path().join("journal.jsonl");
        let journal_path = journal_path.to_str().expect("utf8 path");
        let current =
            PrepareTimeBinding::new("https://api.hyperliquid.xyz".to_owned(), true, false);
        assert!(PrepareTimeBinding::verify(journal_path, &current).is_err());
    }

    #[test]
    fn rejects_missing_arguments() {
        assert!(invocation(args(&["prepare", "config.toml"])).is_err());
        assert!(invocation(args(&[])).is_err());
        assert!(invocation(args(&["unknown-command"])).is_err());
    }
}
