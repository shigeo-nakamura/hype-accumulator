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
- `pacing`: exact-USDC deposit admission and deterministic fixed-DCA decisions
- `clock` / `exchange`: deterministic boundaries and test doubles
- `execution`: order limits and exchange workflow
- `ledger`: durable-ledger interface
- `metrics`: stable metrics snapshot types
- `status`: validated, dashboard-safe balance and activity payload

The live adapter and persistent ledger backend are intentionally outside this
bootstrap. The crate pins tagged `dex-connector` with `hyperliquid-sdk` only.

## Offline fixed-DCA fallback

The `pacing` state machine is an offline planner. It accepts uniquely identified
authoritative deposit and withdrawal events, admits deposits only after the
configured confirmations, cooldown, explicit admission approval, and admission
caps. The automatic limit applies independently to each deposit; yearly and
cumulative limits aggregate all admissions. Each admitted tranche receives a 31
December receipt-year horizon.
Unmatched balance changes and unadmitted deposit residual never become
deployable capital.

Each eligible UTC date produces one durable decision ID. A skip is durable too;
same-day replay after a restart or an after-decision deposit returns the existing
audit record and no new economic intent. Plans use exact USDC microunits, preserve
per-tranche committed/filled/withdrawn attribution, reserve fee/spread capacity,
and encumber that reserve as part of the unsettled maximum cash commitment.
Plans respect exchange minimum and daily caps and hold an infeasible horizon
residual for explicit approval. The configured final catch-up window adds daily
eligible slots but never relaxes the daily cap.

Terminal settlement records both filled notional and the authoritative total
cash debit. Charged fees consume the reserved commitment permanently; only
unused headroom returns to tranche residual capital.

This fallback deliberately has no market-signal multiplier, signer, exchange
submission, state-file backend, or live configuration. The caller must durably
persist a new decision before any separately approved execution integration.

The read-only status observer therefore treats HYPE attribution as unavailable:
it reports zero HYPE with degraded health until an authoritative accumulator
ledger supplies reconciled holdings and last-trade identity. Raw account HYPE
and account-wide fills are never presented as accumulator activity.

## Dashboard status contract

The accumulator publishes a nested `accumulator` status block for
`debot-dashboard`. It contains total equity in USDC, reconciled USDC and HYPE
balances, the HYPE mark used for valuation, balance observation time, last
trade time, configured cadence, and health. `total_equity_usdc` is derived as
`usdc_balance + hype_balance * hype_price_usdc`; callers cannot provide a
different total. `hype_balance` means all reconciled holdings owned by the
configured account (spot plus staking/delegated), excluding unattributed
external changes.

The payload deliberately excludes account addresses, signing material,
ciphertext, signed requests, and production topology. The read-only
`hype-status` binary queries spot balances, the HYPE mark, and staking
summary/delegations without constructing a signer, then atomically writes one
local status snapshot. Last-trade identity remains unavailable until supplied
by the authoritative accumulator ledger:

```text
HYPE_ACCOUNT_ID=0x... cargo run --locked --bin hype-status -- config.local.toml status.json
```

This networked one-shot path is suitable for signer-free, read-only DRY_RUN
verification. S3 emission and deployed scheduling remain gated by the
observability rollout.

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
