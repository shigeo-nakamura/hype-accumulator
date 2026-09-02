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
- `backup`: checksummed ledger backup verification and clean restore
- `metrics`: stable metrics snapshot types
- `status`: validated, dashboard-safe balance and activity payload

The live adapter and persistent ledger backend are intentionally outside this
bootstrap. Release branches pin tagged `dex-connector` with
`hyperliquid-sdk` only; dependent development changes may temporarily pin the
reviewed connector commit and must return to a tag before merge.

The optional `live-probe` library feature is a narrow integration seam for the
rollout probe. It can reserve one persistent API-wallet nonce, translate only a
currently pending `SubmitOrder` read directly from a `DurableWorkflow` into an
exact IOC request, and perform read-only reconciliation using that same
journal-backed action's CLOID and identity bindings. The adapter never accepts
a caller-constructed or cloned action for submission.
It has no CLI, scheduler, config or secret loader, retry loop, staking action,
deployment path, or live authorization. The default build does not expose the
module. After any submission attempt, including any error, the durable action
is reconciliation-only and must never be submitted again.

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

## Offline staking workflow fault injection

The optional `offline-staking-simulation` feature exercises the durable
post-purchase mechanics without adding a signer or exchange client to the
default/release build. A simulation binding is versioned and fixed to the
execution-account identity hash, eligibility policy, policy acknowledgement,
one validator address, and its validator-summary evidence hash. Staking and
delegation action IDs additionally bind the exact content-addressed eligibility
workflow, so an old eligibility-only journal cannot become action-capable after
restart.

The simulation covers write-ahead preparation, ambiguous responses,
reconciliation-only restart behavior, exact account/transaction evidence,
validator-bound delegation, duplicate replay, and completion only after the
delegated amount matches the recorded staking target:

```text
cargo test --test workflow --features offline-staking-simulation --locked
```

The public `prepare_staking_deposit` and `prepare_delegation` methods remain
hard-disabled in every build. Simulation evidence does not prove venue signer
capability or safe acceptance timing and does not authorize testnet/mainnet
submission, automatic staking, secret installation, or live operation.

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

The optional `operations` block is derived from mutually consistent pacing and
durable-ledger state plus identifier-free workflow observations. It reports
confirmed/admitted/unallocated capital, deployable/committed/spent amounts,
per-horizon pace, last activity timestamps, attributed unstaked/delegated HYPE,
stale signals, API error counters, and pending workflow age. Projection fails
closed if capital totals disagree or observation time regresses. The same typed
snapshot renders Prometheus text and can be written atomically; no wallet,
action, order, signing, or secret identifier is used as a field or label. A
nonterminal workflow older than the configured threshold emits
`hype_accumulator_workflow_stuck 1` and the stable `stuck_workflow` halt label.
Alert routing and deployed scheduling remain rollout-gated.

For recurring signer-free planning, `hype-accumulator --dry-run-cycle`
combines the read-only account observer with a durable overlapping movement
cursor, admission approvals, UTC-bound signal snapshots, one-decision-per-day
pacing, and a protected crash-recovery ledger. It refuses live mode and a
populated signing-key environment variable and never constructs an economic
action. See `docs/runbooks/signer-free-runtime.md` for the closed input and
persistence contract. Service installation or start remains a separate
approval gate.

The signer-free maintenance CLI can checkpoint the append-only ledger into a
checksummed payload bundle and a separate protected-head export, verify a
downloaded pair, and restore it only into a clean state/anchor scope. The Rust
CLI has no S3, signer, exchange, service-lifecycle, or deployment capability.
The separate operator tool `scripts/ledger_backup_transfer.py` can pin those
outputs to exact version IDs in two versioned, KMS-encrypted S3 buckets and
download only the recorded versions. It performs no restore or service action;
actual AWS use remains an explicit operator gate. See
`docs/runbooks/ledger-backup.md`.

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

## ARM64 release compatibility

CI builds and tests release artifacts on a native ARM64 runner inside a
versioned, digest-pinned Amazon Linux 2023 container. Before packaging, both
binaries must resolve every shared library with `ldd` in that container. The
main binary also validates the non-routable example configuration, while the
status binary is exercised only through its argument-validation path so the
check cannot contact an exchange or require a signer.

Each archive includes checksummed build provenance and an `AL2023-ABI.txt`
report containing the image digest, OS/libc identity, dynamic dependencies, and
required glibc symbol versions. This artifact workflow does not deploy, start a
service, install configuration or credentials, or enable live behavior.

The offline installer verifies the attested artifact's outer and inner
checksums, provenance, ABI, immutable image and lock digests, and an explicit
halted dry-run policy before staging or atomically selecting a release. It has
no network, AWS, secret, signer, or service-lifecycle capability. See
`docs/runbooks/release-install.md` for the exact stage/activate/rollback contract.

## Research

The standard-library-only point-in-time research harness compares deposit-aware daily and weekly DCA with explicitly bounded adaptive pacing. It enforces capital admission times, one purchase per UTC day, execution limits, and data revision/publication rules.

Run `make test` and `make research`. The reproducible report is written to `build/research-report.json`. Committed inputs are synthetic fixtures, so the report deliberately returns `no-go` rather than claiming an economic timing edge. See `docs/data-contract.md` and `docs/research-method.md`.
