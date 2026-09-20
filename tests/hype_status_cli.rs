//! Offline checks of the `hype-status` command line: which positional
//! arguments it takes and that a third one is read as the security policy
//! before the observer (and any network or environment access) exists.

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

fn example(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("config")
        .join(name)
}

fn run(args: &[&str]) -> (i32, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_hype-status"))
        .args(args)
        .env_remove("HYPE_ACCOUNT_ID")
        .output()
        .expect("hype-status runs");
    (
        output.status.code().expect("exit code"),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

#[test]
fn a_fourth_positional_argument_is_rejected() {
    let (code, stderr) = run(&[
        "config.toml",
        "status.json",
        "security-policy.toml",
        "excess",
    ]);
    assert_eq!(code, 2);
    assert!(
        stderr.contains("usage: hype-status [config.toml] [status.json] [security-policy.toml]"),
        "{stderr}"
    );
}

#[test]
fn the_third_argument_is_the_security_policy_and_is_read_before_the_observer() {
    let config = example("example.toml");
    let status = std::env::temp_dir().join(format!(
        "hype-status-cli-{}-status.json",
        std::process::id()
    ));
    let status_arg = status.to_string_lossy().into_owned();
    let config_arg = config.to_string_lossy().into_owned();

    // A policy path that does not exist fails on the policy, not on the
    // missing observation account: the policy is attached before the
    // account is resolved.
    let missing = std::env::temp_dir().join("hype-status-cli-no-such-policy.toml");
    let (code, stderr) = run(&[&config_arg, &status_arg, &missing.to_string_lossy()]);
    assert_eq!(code, 2);
    assert!(
        !stderr.contains("HYPE_ACCOUNT_ID"),
        "policy must be read before the account is resolved: {stderr}"
    );

    // A malformed policy is a policy parse error, again before the account.
    let malformed = std::env::temp_dir().join(format!(
        "hype-status-cli-{}-malformed-policy.toml",
        std::process::id()
    ));
    fs::write(&malformed, "this is not = [ a policy").expect("write malformed policy");
    let (code, stderr) = run(&[&config_arg, &status_arg, &malformed.to_string_lossy()]);
    fs::remove_file(&malformed).ok();
    assert_eq!(code, 2);
    assert!(
        !stderr.contains("HYPE_ACCOUNT_ID"),
        "policy must be parsed before the account is resolved: {stderr}"
    );

    // With the example policy attached, the run proceeds to the account
    // resolution and stops there without the environment value — the
    // policy was accepted.
    let policy = example("security-policy.example.toml");
    let (code, stderr) = run(&[&config_arg, &status_arg, &policy.to_string_lossy()]);
    assert_eq!(code, 2);
    assert!(
        stderr.contains("HYPE_ACCOUNT_ID"),
        "the example policy must attach cleanly: {stderr}"
    );
    assert!(!status.exists(), "no status document is written on failure");
}
