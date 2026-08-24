use hype_accumulator::{bootstrap, config::Config, exchange::UnavailableLiveExchange};
use std::{
    env, fs,
    path::{Path, PathBuf},
    process,
};

fn main() {
    if let Err(error) = run() {
        eprintln!("startup rejected: {error}");
        process::exit(2);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let (config_path, security_policy_path) = config_paths(env::args().skip(1))?;
    let config = load_config(&config_path, security_policy_path.as_deref())?;
    let exchange = bootstrap(
        &config,
        &hype_accumulator::config::ProcessEnvironment,
        |_| Box::new(UnavailableLiveExchange),
    )?;
    println!("mode={} ready", exchange.mode());
    Ok(())
}

fn config_paths<I>(mut args: I) -> Result<(PathBuf, Option<PathBuf>), &'static str>
where
    I: Iterator<Item = String>,
{
    let config_path = args
        .next()
        .map_or_else(|| PathBuf::from("config.toml"), PathBuf::from);
    let security_policy_path = args.next().map(PathBuf::from);
    if args.next().is_some() {
        return Err("usage: hype-accumulator [config.toml] [security-policy.toml]");
    }
    Ok((config_path, security_policy_path))
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
    use super::load_config;
    use std::fs;

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
