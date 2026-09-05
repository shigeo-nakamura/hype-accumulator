# Recognizing main-to-subaccount funding

`designated_parent_funding` recognizes positive USDC internal transfers from
one explicitly designated main account into the configured subaccount. It is
an **account-local funding boundary**, not inheritance of a parent ledger's
admission allocations. Annual and cumulative admission ceilings apply to funds
admitted by this subaccount. Do not aggregate its admitted-capital counters with
its parent's counters and call the result system-wide capital.

The default `external_deposit_only` is unchanged. The existing
`traced_parent_transfer` contract is unchanged and is not implemented by this
new mode. No runtime path relabels an internal transfer as an external deposit.

## Policy binding

In the protected security policy, select:

```toml
[custody]
# Other mandatory custody fields are still required.
execution_account_kind = "subaccount"
funding_mode = "designated_parent_funding"
allow_traced_parent_transfer_admission = false
admitted_parent_account_env = "HYPE_PARENT_ACCOUNT"
```

Resolve `HYPE_PARENT_ACCOUNT` and the existing execution-account variable from
protected public-identity configuration. The addresses must be valid and must
differ. The actual parent address and funding mode are bound into the effective
live-policy digest, not just the environment variable name. This does not
validate signer delegation or enable live trading.

Both the signer-free runtime and supervised live-probe prepare command obtain the route through the typed config and binds it
into authenticated runtime state on its first committed cycle. A subsequent
parent change, execution-account change, enablement, or disablement is rejected.
Use a separately approved migration with a fresh state directory and protected
anchor; preserve prior ledgers and reconcile existing admitted balances first.
Do not reset a funded ledger to recover annual admission room. Full-history
backfill is needed so previously ignored transfers are not lost behind a cursor.

## Recognition, approval, and replay

- Positive USDC `InternalTransfer` movements with the exact approved counterparty
  become `AuthoritativeParentFunding` events. The private journal retains the
  stable movement ID, amount, time, source, and destination.
- Recognized funding appears in the existing public capital metrics. The legacy
  `confirmed_deposits_usdc`/`unallocated_deposits_usdc` names now include this
  explicitly enabled source; no account address is added to public status.
- Recognition alone does **not** admit capital. The existing protected admission
  artifact must provide the movement's confirmation and approval evidence. The
  minimum confirmation count, cooldown, per-funding, annual, and cumulative caps
  apply exactly as they do for other admitted tranches. No confirmations are
  fabricated from a balance change or a successful polling request.
- Duplicate movement IDs, overlapping polls, and restarts cannot admit the same
  funding twice. Conflicting normalized movements within one scan are rejected
  before persistence. The complete planned ledger batch is also checked against
  durable history before any pending marker or event is written, so a corrected
  scan can resume without a permanently uncommitted cycle.
- Incoming USDC transfers from other accounts, unknown senders, zero/self
  transfers fail the history gate in this mode. They are not approved funding.
- Negative USDC internal transfers with a valid distinct counterparty are
  recorded separately as `AuthoritativeTransferWithdrawal`. They reduce
  available capital and do not refund annual/lifetime admission room. Returns
  first consume available unadmitted funding in receipt-time/ID order, then free
  admitted residual. The unadmitted allocations are frozen in durable state and
  the ledger; later approval/cooldown expiry cannot admit returned capital.
  Public unallocated funding excludes these returns. Missing or malformed
  counterparties fail closed.
- New funding after the daily decision cannot cause a second purchase that day.
  Current DRY_RUN still suppresses all signed actions.

## Remaining live requirements

This adapter supplies funding recognition and uses existing admission evidence;
it does not automate that evidence producer, implement parent-ledger allocation
inheritance, finish order finality/settlement, reconcile cross-workflow HYPE, or
enable staking. Those gates remain explicit prerequisites for scheduled live.
