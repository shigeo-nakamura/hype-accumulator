use hype_accumulator::{
    bootstrap,
    config::{Config, ProcessEnvironment},
    exchange::UnavailableLiveExchange,
};
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
        [config_path] => Ok(Invocation::Startup {
            config_path: PathBuf::from(config_path),
            security_policy_path: None,
        }),
        [config_path, security_policy_path] if config_path != "--install-preflight" => {
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
        _ => Err(
            "usage: hype-accumulator [config.toml] [security-policy.toml] | --install-preflight config.toml security-policy.toml",
        ),
    }
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
