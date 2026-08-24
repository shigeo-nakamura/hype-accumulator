use chrono::Utc;
use hype_accumulator::{
    config::{Config, ProcessEnvironment},
    monitor::{trade_cadence_label, HypeAttribution, HyperliquidObserver},
    status::DashboardStatus,
    status_io::write_status_atomic,
};
use std::{env, fs, path::PathBuf, process};

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("status observation failed: {error}");
        process::exit(2);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = env::args().skip(1);
    let config_path = args.next().unwrap_or_else(|| "config.toml".to_owned());
    let output_path = args
        .next()
        .map_or_else(|| PathBuf::from("status.json"), PathBuf::from);
    if args.next().is_some() {
        return Err("usage: hype-status [config.toml] [status.json]".into());
    }
    let config = Config::from_toml(&fs::read_to_string(config_path)?)?;
    let account = config.observation_account(&ProcessEnvironment)?;
    let process_started_at = Utc::now();
    let observer = HyperliquidObserver::new(&config.hyperliquid.endpoint, &account)?;
    let accumulator = observer
        .observe(
            &HypeAttribution::Unavailable,
            trade_cadence_label(&config.schedule),
        )
        .await?;
    let updated_at = Utc::now();
    let status = DashboardStatus::new(updated_at, process_started_at, config.dry_run, accumulator);
    write_status_atomic(&output_path, &status)?;
    println!("status observation written");
    Ok(())
}
