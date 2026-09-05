use chrono::Utc;
use hype_accumulator::{
    backup::{create_ledger_backup, restore_ledger_backup, verify_ledger_backup},
    bootstrap,
    config::{Config, ProcessEnvironment},
    exchange::UnavailableLiveExchange,
    monitor::{trade_cadence_label, HypeAttribution, HyperliquidObserver},
    pacing::PacingLimits,
    runtime::{AdmissionApprovals, RuntimeConfig, RuntimeCycleInput, SignerFreeRuntime},
    signal::{CoreHealth, SignalSnapshot},
    signal_source::{
        build_snapshot, plan_snapshot, publish_snapshot, HyperliquidCoreSignalSource,
        PublishOutcome,
    },
};
use std::{
    env, fs,
    path::{Path, PathBuf},
    process,
};

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("startup rejected: {error}");
        process::exit(2);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    match invocation(env::args().skip(1))? {
        Invocation::Startup {
            config_path,
            security_policy_path,
        } => {
            let config = load_config(&config_path, security_policy_path.as_deref())?;
            let exchange = bootstrap(&config, &ProcessEnvironment, |_| {
                Box::new(UnavailableLiveExchange)
            })?;
            println!("mode={} ready", exchange.mode());
        }
        Invocation::InstallPreflight {
            config_path,
            security_policy_path,
        } => {
            let config = load_config(&config_path, Some(&security_policy_path))?;
            config.validate_offline_install(&ProcessEnvironment)?;
            println!("mode=dry-run halted install-ready");
        }
        Invocation::DryRunCycle {
            config_path,
            security_policy_path,
            runtime_config_path,
        } => {
            run_dry_run_cycle(&config_path, &security_policy_path, &runtime_config_path).await?;
        }
        Invocation::SignalSnapshot {
            config_path,
            security_policy_path,
            runtime_config_path,
        } => {
            run_signal_snapshot(&config_path, &security_policy_path, &runtime_config_path).await?;
        }
        Invocation::LedgerBackupCreate {
            ledger_directory,
            source_anchor_path,
            bundle_directory,
            anchor_export_path,
        } => {
            let manifest = create_ledger_backup(
                ledger_directory,
                source_anchor_path,
                bundle_directory,
                anchor_export_path,
                Utc::now(),
            )?;
            println!(
                "backup_id={} records={} head={}",
                manifest.backup_id, manifest.record_count, manifest.head_hash
            );
        }
        Invocation::LedgerBackupVerify {
            bundle_directory,
            anchor_export_path,
        } => {
            let manifest = verify_ledger_backup(bundle_directory, anchor_export_path)?;
            println!(
                "backup_id={} records={} head={} verified",
                manifest.backup_id, manifest.record_count, manifest.head_hash
            );
        }
        Invocation::LedgerBackupRestore {
            bundle_directory,
            anchor_export_path,
            destination_directory,
            destination_anchor_path,
        } => {
            let manifest = restore_ledger_backup(
                bundle_directory,
                anchor_export_path,
                destination_directory,
                destination_anchor_path,
            )?;
            println!(
                "backup_id={} records={} head={} restored",
                manifest.backup_id, manifest.record_count, manifest.head_hash
            );
        }
    }
    Ok(())
}

#[derive(Debug, Eq, PartialEq)]
enum Invocation {
    Startup {
        config_path: PathBuf,
        security_policy_path: Option<PathBuf>,
    },
    InstallPreflight {
        config_path: PathBuf,
        security_policy_path: PathBuf,
    },
    DryRunCycle {
        config_path: PathBuf,
        security_policy_path: PathBuf,
        runtime_config_path: PathBuf,
    },
    SignalSnapshot {
        config_path: PathBuf,
        security_policy_path: PathBuf,
        runtime_config_path: PathBuf,
    },
    LedgerBackupCreate {
        ledger_directory: PathBuf,
        source_anchor_path: PathBuf,
        bundle_directory: PathBuf,
        anchor_export_path: PathBuf,
    },
    LedgerBackupVerify {
        bundle_directory: PathBuf,
        anchor_export_path: PathBuf,
    },
    LedgerBackupRestore {
        bundle_directory: PathBuf,
        anchor_export_path: PathBuf,
        destination_directory: PathBuf,
        destination_anchor_path: PathBuf,
    },
}

fn invocation<I>(args: I) -> Result<Invocation, &'static str>
where
    I: Iterator<Item = String>,
{
    let args = args.collect::<Vec<_>>();
    match args.as_slice() {
        [] => Ok(Invocation::Startup {
            config_path: PathBuf::from("config.toml"),
            security_policy_path: None,
        }),
        [config_path] if !config_path.starts_with("--") => Ok(Invocation::Startup {
            config_path: PathBuf::from(config_path),
            security_policy_path: None,
        }),
        [config_path, security_policy_path] if !config_path.starts_with("--") => {
            Ok(Invocation::Startup {
                config_path: PathBuf::from(config_path),
                security_policy_path: Some(PathBuf::from(security_policy_path)),
            })
        }
        [command, config_path, security_policy_path] if command == "--install-preflight" => {
            Ok(Invocation::InstallPreflight {
                config_path: PathBuf::from(config_path),
                security_policy_path: PathBuf::from(security_policy_path),
            })
        }
        [command, config_path, security_policy_path, runtime_config_path]
            if command == "--dry-run-cycle" =>
        {
            Ok(Invocation::DryRunCycle {
                config_path: PathBuf::from(config_path),
                security_policy_path: PathBuf::from(security_policy_path),
                runtime_config_path: PathBuf::from(runtime_config_path),
            })
        }
        [command, config_path, security_policy_path, runtime_config_path]
            if command == "--signal-snapshot" =>
        {
            Ok(Invocation::SignalSnapshot {
                config_path: PathBuf::from(config_path),
                security_policy_path: PathBuf::from(security_policy_path),
                runtime_config_path: PathBuf::from(runtime_config_path),
            })
        }
        [command, ledger_directory, source_anchor_path, bundle_directory, anchor_export_path]
            if command == "--ledger-backup-create" =>
        {
            Ok(Invocation::LedgerBackupCreate {
                ledger_directory: PathBuf::from(ledger_directory),
                source_anchor_path: PathBuf::from(source_anchor_path),
                bundle_directory: PathBuf::from(bundle_directory),
                anchor_export_path: PathBuf::from(anchor_export_path),
            })
        }
        [command, bundle_directory, anchor_export_path]
            if command == "--ledger-backup-verify" =>
        {
            Ok(Invocation::LedgerBackupVerify {
                bundle_directory: PathBuf::from(bundle_directory),
                anchor_export_path: PathBuf::from(anchor_export_path),
            })
        }
        [
            command,
            bundle_directory,
            anchor_export_path,
            destination_directory,
            destination_anchor_path,
        ] if command == "--ledger-backup-restore" => Ok(Invocation::LedgerBackupRestore {
            bundle_directory: PathBuf::from(bundle_directory),
            anchor_export_path: PathBuf::from(anchor_export_path),
            destination_directory: PathBuf::from(destination_directory),
            destination_anchor_path: PathBuf::from(destination_anchor_path),
        }),
        _ => Err(
            "usage: hype-accumulator [config.toml] [security-policy.toml] | --install-preflight config.toml security-policy.toml | --dry-run-cycle config.toml security-policy.toml runtime.toml | --signal-snapshot config.toml security-policy.toml runtime.toml | --ledger-backup-create LEDGER_DIR SOURCE_ANCHOR BUNDLE_DIR ANCHOR_EXPORT | --ledger-backup-verify BUNDLE_DIR ANCHOR_EXPORT | --ledger-backup-restore BUNDLE_DIR ANCHOR_EXPORT DESTINATION_DIR DESTINATION_ANCHOR",
        ),
    }
}

async fn run_dry_run_cycle(
    config_path: &Path,
    security_policy_path: &Path,
    runtime_config_path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = load_config(config_path, Some(security_policy_path))?;
    config.validate_signer_free_runtime(&ProcessEnvironment)?;
    let limits = PacingLimits::from_config(&config)?;
    let runtime_config = RuntimeConfig::from_toml(&fs::read_to_string(runtime_config_path)?)?;
    let approvals_path = runtime_config.admission_approvals_path().to_path_buf();
    let signal_path = runtime_config.signal_snapshot_path().to_path_buf();
    let mut runtime = SignerFreeRuntime::open(runtime_config, limits)?;
    let approvals = AdmissionApprovals::from_json(&fs::read_to_string(approvals_path)?)?;
    let signal = match fs::read_to_string(signal_path) {
        Ok(payload) => {
            if let Ok(signal) = SignalSnapshot::from_json(&payload) {
                Some(signal)
            } else {
                eprintln!("signal snapshot invalid; recording fail-closed unavailable state");
                None
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            eprintln!("signal snapshot absent; recording fail-closed unavailable state");
            None
        }
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
    let observed_at = Utc::now();
    let scan_end_ms = u64::try_from(observed_at.timestamp_millis())?;
    let scan_start_ms = runtime.next_scan_start_ms();
    let (movements, capital_history_complete, api_errors) =
        if let Ok(movements) = observer.account_movements(scan_start_ms, scan_end_ms).await {
            (movements, true, 0)
        } else {
            eprintln!(
                "account movement history unavailable; cursor retained and decision fails closed"
            );
            (Vec::new(), false, 1)
        };
    let report = runtime.apply_cycle(RuntimeCycleInput {
        observed_at,
        scan_start_ms,
        scan_end_ms,
        movements: &movements,
        approvals: &approvals,
        signal: signal.as_ref(),
        accumulator,
        capital_history_complete,
        manual_pause: config.manual_halt,
        api_errors,
    })?;
    let disposition = if report.decision().is_none() {
        "not-due"
    } else if report.is_new_decision() {
        "new"
    } else {
        "existing"
    };
    println!(
        "mode=dry-run cycle={disposition} economic_action_suppressed=true signed_action_created=false"
    );
    Ok(())
}

async fn run_signal_snapshot(
    config_path: &Path,
    security_policy_path: &Path,
    runtime_config_path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = load_config(config_path, Some(security_policy_path))?;
    config.validate_signer_free_runtime(&ProcessEnvironment)?;
    let runtime_config = RuntimeConfig::from_toml(&fs::read_to_string(runtime_config_path)?)?;
    let plan = plan_snapshot(
        Utc::now(),
        &config.schedule,
        runtime_config.signal_snapshot_stale_after_seconds(),
    )?;
    let source = HyperliquidCoreSignalSource::new(&config.hyperliquid.endpoint)?;
    let observation = source.observe_top_of_book().await?;
    let snapshot = build_snapshot(&plan, &observation)?;
    let (core_health_label, core_age_seconds) = match snapshot.core_health() {
        CoreHealth::Healthy { age_seconds } => ("healthy", *age_seconds),
        CoreHealth::Missing | CoreHealth::Future { .. } | CoreHealth::Stale { .. } => {
            return Err("produced snapshot is not purchase-eligible".into());
        }
    };
    let (outcome, snapshot_hash) =
        match publish_snapshot(runtime_config.signal_snapshot_path(), &snapshot)? {
            PublishOutcome::Written => ("written", snapshot.snapshot_hash().to_owned()),
            PublishOutcome::Existing { snapshot_hash } => ("existing", snapshot_hash),
        };
    println!(
        "mode=signal-snapshot decision_at={} core_health={core_health_label} core_age_seconds={core_age_seconds} outcome={outcome} snapshot_hash={snapshot_hash} signed_action_created=false",
        plan.decision_at.to_rfc3339(),
    );
    Ok(())
}

fn load_config(
    config_path: &Path,
    security_policy_path: Option<&Path>,
) -> Result<Config, Box<dyn std::error::Error>> {
    let runtime = fs::read_to_string(config_path)?;
    match security_policy_path {
        Some(path) => {
            let policy = fs::read_to_string(path)?;
            Ok(Config::from_toml_with_security_policy(&runtime, &policy)?)
        }
        None => Ok(Config::from_toml(&runtime)?),
    }
}

#[cfg(test)]
mod tests {
    use super::{invocation, load_config, Invocation};
    use std::{fs, path::PathBuf};

    #[test]
    fn install_preflight_requires_both_explicit_documents() {
        assert_eq!(
            invocation(
                [
                    "--install-preflight",
                    "runtime.toml",
                    "security-policy.toml",
                ]
                .into_iter()
                .map(str::to_owned)
            ),
            Ok(Invocation::InstallPreflight {
                config_path: PathBuf::from("runtime.toml"),
                security_policy_path: PathBuf::from("security-policy.toml"),
            })
        );
        assert!(invocation(
            ["--install-preflight", "runtime.toml"]
                .into_iter()
                .map(str::to_owned)
        )
        .is_err());
    }

    #[test]
    fn dry_run_cycle_requires_all_three_explicit_documents() {
        assert_eq!(
            invocation(
                [
                    "--dry-run-cycle",
                    "runtime-policy.toml",
                    "security-policy.toml",
                    "runtime-paths.toml",
                ]
                .into_iter()
                .map(str::to_owned)
            ),
            Ok(Invocation::DryRunCycle {
                config_path: PathBuf::from("runtime-policy.toml"),
                security_policy_path: PathBuf::from("security-policy.toml"),
                runtime_config_path: PathBuf::from("runtime-paths.toml"),
            })
        );
        assert!(invocation(
            [
                "--dry-run-cycle",
                "runtime-policy.toml",
                "security-policy.toml",
            ]
            .into_iter()
            .map(str::to_owned)
        )
        .is_err());
    }

    #[test]
    fn signal_snapshot_requires_all_three_explicit_documents() {
        assert_eq!(
            invocation(
                [
                    "--signal-snapshot",
                    "config.toml",
                    "security-policy.toml",
                    "runtime.toml",
                ]
                .into_iter()
                .map(str::to_owned)
            ),
            Ok(Invocation::SignalSnapshot {
                config_path: PathBuf::from("config.toml"),
                security_policy_path: PathBuf::from("security-policy.toml"),
                runtime_config_path: PathBuf::from("runtime.toml"),
            })
        );
        assert!(invocation(
            ["--signal-snapshot", "config.toml", "security-policy.toml"]
                .into_iter()
                .map(str::to_owned)
        )
        .is_err());
    }

    #[test]
    fn ledger_backup_commands_require_explicit_separate_paths() {
        assert_eq!(
            invocation(
                [
                    "--ledger-backup-create",
                    "/state/ledger",
                    "/anchors/source.json",
                    "/backups/bundle",
                    "/protected/anchor.json",
                ]
                .into_iter()
                .map(str::to_owned)
            ),
            Ok(Invocation::LedgerBackupCreate {
                ledger_directory: PathBuf::from("/state/ledger"),
                source_anchor_path: PathBuf::from("/anchors/source.json"),
                bundle_directory: PathBuf::from("/backups/bundle"),
                anchor_export_path: PathBuf::from("/protected/anchor.json"),
            })
        );
        assert_eq!(
            invocation(
                [
                    "--ledger-backup-verify",
                    "/backups/bundle",
                    "/protected/anchor.json",
                ]
                .into_iter()
                .map(str::to_owned)
            ),
            Ok(Invocation::LedgerBackupVerify {
                bundle_directory: PathBuf::from("/backups/bundle"),
                anchor_export_path: PathBuf::from("/protected/anchor.json"),
            })
        );
        assert_eq!(
            invocation(
                [
                    "--ledger-backup-restore",
                    "/backups/bundle",
                    "/protected/anchor.json",
                    "/restore/ledger",
                    "/restore-protected/anchor.json",
                ]
                .into_iter()
                .map(str::to_owned)
            ),
            Ok(Invocation::LedgerBackupRestore {
                bundle_directory: PathBuf::from("/backups/bundle"),
                anchor_export_path: PathBuf::from("/protected/anchor.json"),
                destination_directory: PathBuf::from("/restore/ledger"),
                destination_anchor_path: PathBuf::from("/restore-protected/anchor.json"),
            })
        );
        assert!(invocation(
            ["--ledger-backup-verify", "/backups/bundle"]
                .into_iter()
                .map(str::to_owned)
        )
        .is_err());
    }

    #[test]
    fn cli_loads_and_validates_the_explicit_security_policy_document() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let config_path = directory.path().join("config.toml");
        let policy_path = directory.path().join("security-policy.toml");
        fs::write(&config_path, include_str!("../tests/fixtures/safe.toml"))
            .expect("runtime fixture");
        fs::write(
            &policy_path,
            include_str!("../config/security-policy.example.toml"),
        )
        .expect("policy fixture");
        load_config(&config_path, Some(&policy_path)).expect("typed policy is loaded");

        let invalid_policy = format!(
            "unknown_top_level = true\n{}",
            include_str!("../config/security-policy.example.toml")
        );
        fs::write(&policy_path, invalid_policy).expect("invalid policy fixture");
        assert!(load_config(&config_path, Some(&policy_path)).is_err());
    }
}
