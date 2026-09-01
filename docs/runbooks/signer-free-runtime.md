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

## Capital and decision behavior

- Only normalized external USDC deposits and withdrawals enter capital state.
- A deposit remains confirmed-but-unallocated until its exact movement event
  ID appears in the separately reviewed admission artifact with confirmation
  and approval timestamps. An approval for an unknown event fails the cycle.
- The movement cursor re-reads a 24-hour overlap by default; ledger event IDs
  make replay idempotent. An incomplete history query never advances the
  cursor and forces the day's decision to fail closed.
- Each cycle captures the read-only balance first, fixes an observation
  boundary, and then closes movement history through that boundary. A balance
  older than `account_observation_max_age_seconds` is rejected.
- A missing or invalid signal snapshot produces a durable
  `core_signal_unavailable` skip. A later same-day snapshot cannot replace it.
- A valid signal snapshot must bind to that day's configured UTC hour/minute,
  with seconds set to zero. Delayed timer execution still records the decision
  at that boundary. Deposits, withdrawals, confirmations, and approvals after
  the boundary are reconciled only after the decision and become available no
  earlier than the next eligible day.
- Every planned DRY_RUN amount is recorded and counted, but the cycle report
  always states `economic_action_suppressed=true` and
  `signed_action_created=false`.

## Persistence boundaries

The runtime state directory contains the pacing snapshot and append-only audit
ledger. `protected_anchor_path` must be outside that directory. On a deployed
host, protect the anchor parent with a distinct filesystem/IAM boundary; a
second path on the same mutable boundary is not credited as rollback
protection. Relative paths, `.`/`..` components, anchor symlinks, hard-linked
anchors, and anchor-parent symlink aliases into the state directory are
rejected. Runtime state, pending transaction, protected anchor, and private
cycle report files are written with mode `0600` on Unix.

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
