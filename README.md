# HYPE accumulator

Rust bot skeleton for deposit-aware HYPE accumulation through `dex-connector`,
without pairtrade strategy assumptions.

## Safety boundary

`dry_run` defaults to `true`; this path never constructs a live exchange or
signer. Live startup fails before creating network-capable objects unless manual
halt is off, explicit live approval is on, the validator allowlist is non-empty,
all limits validate, and account/signing environment variables exist. Changing
those values is an explicit operator approval boundary.

Never commit credentials, addresses, ciphertext, host aliases, or production
paths. `config/example.toml` is safe and non-routable.

## Architecture

- `config`: typed configuration and fail-closed validation
- `account` / `capital`: observed, confirmed, and admitted capital reconciliation
- `signal`: read-only signal inputs
- `clock` / `exchange`: deterministic boundaries and test doubles
- `execution`: order limits and exchange workflow
- `ledger`: durable-ledger interface
- `metrics`: stable metrics snapshot types

The live adapter and persistent ledger backend are intentionally outside this
bootstrap. The crate pins tagged `dex-connector` with `hyperliquid-sdk` only.

## Local development

```text
cp config/example.toml config.local.toml
cargo run --locked -- config.local.toml
cargo fmt --all --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --all-targets --locked
cargo build --locked
```

The first command reports `mode=dry-run ready`. CI runs the same gates.

## Research

The standard-library-only point-in-time research harness compares deposit-aware daily and weekly DCA with explicitly bounded adaptive pacing. It enforces capital admission times, one purchase per UTC day, execution limits, and data revision/publication rules.

Run `make test` and `make research`. The reproducible report is written to `build/research-report.json`. Committed inputs are synthetic fixtures, so the report deliberately returns `no-go` rather than claiming an economic timing edge. See `docs/data-contract.md` and `docs/research-method.md`.
