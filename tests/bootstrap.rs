use hype_accumulator::{
    account::CapitalSnapshot,
    bootstrap,
    capital::automatically_deployable,
    config::{Config, ConfigError},
    exchange::{DryRunExchange, Exchange, OrderIntent, Submission},
};
use std::{cell::Cell, collections::HashMap, rc::Rc};
fn fixture() -> Config {
    Config::from_toml(include_str!("fixtures/safe.toml")).expect("fixture must parse")
}

#[test]
fn safe_defaults_are_dry_run_and_halted() {
    let input = include_str!("fixtures/safe.toml")
        .replace("dry_run = true\n", "")
        .replace("manual_halt = true\n", "");
    let config = Config::from_toml(&input).expect("safe defaults must parse");
    assert!(config.dry_run);
    assert!(config.manual_halt);
    assert!(!config.live_approved);
}

#[test]
fn dry_run_never_constructs_live_exchange() {
    let called = Rc::new(Cell::new(false));
    let marker = Rc::clone(&called);
    let mut exchange = bootstrap(&fixture(), &HashMap::new(), move |_| {
        marker.set(true);
        panic!("live exchange must not be constructed")
    })
    .expect("safe config");
    assert_eq!(
        exchange.submit(&OrderIntent {
            notional_usdc: 10.0,
            max_slippage_bps: 10
        }),
        Ok(Submission::Simulated)
    );
    assert!(!called.get());
}

#[test]
fn incomplete_live_config_fails_before_exchange_construction() {
    let mut config = fixture();
    config.dry_run = false;
    config.manual_halt = false;
    config.live_approved = true;
    config.validator_allowlist.push("validator-a".into());
    let called = Rc::new(Cell::new(false));
    let marker = Rc::clone(&called);
    let result = bootstrap(&config, &HashMap::new(), move |_| {
        marker.set(true);
        Box::new(DryRunExchange::default())
    });
    assert!(matches!(result, Err(ConfigError::MissingLiveSecret(_))));
    assert!(!called.get());
}

#[test]
fn capital_is_limited_by_admitted_not_observed_balance() {
    let snapshot = CapitalSnapshot {
        observed_spot_usdc: 900.0,
        confirmed_deposits_usdc: 300.0,
        admitted_deposits_usdc: 80.0,
        deployed_this_year_usdc: 20.0,
        deployed_cumulative_usdc: 50.0,
    };
    let deployable = automatically_deployable(&snapshot, &fixture().capital);
    assert!((deployable - 30.0).abs() < f64::EPSILON);
}

#[test]
fn fully_deployed_admitted_capital_cannot_be_reused() {
    let snapshot = CapitalSnapshot {
        observed_spot_usdc: 900.0,
        confirmed_deposits_usdc: 80.0,
        admitted_deposits_usdc: 80.0,
        deployed_this_year_usdc: 80.0,
        deployed_cumulative_usdc: 80.0,
    };
    let deployable = automatically_deployable(&snapshot, &fixture().capital);
    assert!(deployable.abs() < f64::EPSILON);
}

#[test]
fn dry_run_exchange_only_simulates() {
    let mut exchange = DryRunExchange::default();
    let intent = OrderIntent {
        notional_usdc: 12.0,
        max_slippage_bps: 5,
    };
    assert_eq!(exchange.submit(&intent), Ok(Submission::Simulated));
    assert_eq!(exchange.simulated(), &[intent]);
}
