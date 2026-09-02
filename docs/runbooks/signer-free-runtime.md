# Signer-free recurring DRY_RUN runtime

`hype-accumulator --dry-run-cycle` performs one exclusively locked,
crash-safe read-only cycle. A systemd timer may invoke the same command
repeatedly; durable UTC decisions suppress a second same-day action after a
restart.

The command requires three separately reviewed documents:

```text
hype-accumulator --dry-run-cycle \
  /etc/hype-accumulator/config.toml \
  /etc/hype-accumulator/security-policy.toml \
  /etc/hype-accumulator/runtime.toml
```

The runtime refuses non-DRY_RUN configuration, live approval, a missing
security policy, or a populated signing-key environment variable. It creates
no signer, order, staking payload, nonce, or submission client.

The staged install preflight and the scheduled planner intentionally use
different runtime halt settings. Keep `manual_halt=true` while staging and
running `--install-preflight`. After the signer-free artifact is separately
approved for recurring observation, use a reviewed runtime config copy with
`manual_halt=false` for `--dry-run-cycle`; otherwise every due day is durably
recorded as a `manual_pause` skip. The attached security policy remains
`dry_run=true` and `manual_halt=true`, and the scheduled-runtime validator
still requires `dry_run=true`, `live_approved=false`, and an empty signing-key
environment. Clearing the runtime planning pause never enables an economic
action.

## Capital and decision behavior

- Only normalized external USDC deposits and withdrawals enter capital state.
- A deposit remains confirmed-but-unallocated until its exact movement event
  ID appears in the separately reviewed admission artifact with confirmation
  and approval timestamps. An approval for an unknown event fails the cycle.
- Newly admitted capital is journaled at its first usable timestamp, after its
  authoritative deposit and before any later withdrawal that depends on it.
  This ordering is preserved when both movements are discovered in one scan.
- Journaled admission is append-only per deposit event ID. Approving an older
  tranche later can use only remaining admission capacity; it cannot move
  admission already journaled for another tranche.
- The movement cursor re-reads a 24-hour overlap by default; ledger event IDs
  make replay idempotent. An incomplete history query never advances the
  cursor and forces the day's decision to fail closed.
- Each cycle captures the read-only balance first, fixes an observation
  boundary, and then closes movement history through that boundary. A balance
  older than `account_observation_max_age_seconds` is rejected.
- A missing, invalid, or boundary-mismatched signal snapshot produces a
  durable `core_signal_unavailable` skip. A stale snapshot left from another
  decision day cannot wedge the movement cursor, and a later same-day snapshot
  cannot replace the recorded skip. Signal and boundary-balance availability
  are authenticated decision-time evidence and remain unchanged on same-day
  recurring cycles.
- A valid signal snapshot must bind to that day's configured UTC hour/minute,
  with seconds set to zero. Delayed timer execution still records the decision
  at that boundary. The balance API request start and response time form a
  closed uncertainty window. With complete movement coverage and no USDC
  movement inside that window, an observation before or after the boundary is
  adjusted to the boundary by applying or reversing only normalized external
  USDC deposits and withdrawals. An internal, trading-related, unknown, or
  request-window movement makes the boundary balance unavailable and durably
  skips with `missing_capital_history`; the runtime never substitutes an
  unadjusted current balance. Deposits, withdrawals, confirmations, and
  approvals after the boundary are reconciled only after the boundary decision
  and become available no earlier than the next eligible day.
- Every planned DRY_RUN amount is recorded and counted, but the cycle report
  always states `economic_action_suppressed=true` and
  `signed_action_created=false`. The simulated commitment is then settled with
  zero fill and zero debit in the same crash-safe cycle, releasing all capital
  for a later scheduled plan without recording spend.

## Persistence boundaries

The runtime state directory is reserved exclusively for the pacing snapshot,
transaction proof files, lock, and append-only audit ledger. Every configured
input/output file must be outside that directory, including after resolving
its parent. Relative paths, `.`/`..` components, configured-file symlinks, and
parent symlink aliases into the state directory are rejected. Resolved parent
plus filename identities must also be distinct, so two lexical paths cannot
alias one input or output through a symlinked parent.

`protected_anchor_path` also requires its own filesystem/IAM boundary; a second
path on the same mutable boundary is not credited as rollback protection.
Anchor symlinks and hard links are rejected. Before the first protected cycle,
an empty ledger accepts only the exact pristine runtime state. Runtime state,
pending transaction, committed proof, protected anchor, and private cycle
report files are written with mode `0600` on Unix.

The private cycle report contains correlation IDs and tranche allocation. The
public `status.json` and Prometheus output are identifier-free. Do not place an
account address, encrypted key, ciphertext, signed payload, host alias, or
production secret in repository configuration or public output.

If the movement-history request fails, the cycle increments the API error
counter and retains the cursor; when a decision is due, it persists a
fail-closed `missing_capital_history` skip. If the account balance/status
request itself fails, the command exits non-zero before the cycle commit; the
previously published status remains stale for the external service monitor to
alert on.

Artifact installation, timer/unit creation, host start/restart, secret
installation, funding, signing, submission, and live enablement remain
separate explicit approval gates.
