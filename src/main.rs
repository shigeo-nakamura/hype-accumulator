use hype_accumulator::{bootstrap, config::Config, exchange::UnavailableLiveExchange};
use std::{env, fs, process};

fn main() {
    if let Err(error) = run() {
        eprintln!("startup rejected: {error}");
        process::exit(2);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let path = env::args()
        .nth(1)
        .unwrap_or_else(|| "config.toml".to_owned());
    let config = Config::from_toml(&fs::read_to_string(path)?)?;
    let exchange = bootstrap(
        config,
        &hype_accumulator::config::ProcessEnvironment,
        |_| Box::new(UnavailableLiveExchange),
    )?;
    println!("mode={} ready", exchange.mode());
    Ok(())
}
