# Recover an interrupted first-live probe

A submission error can occur after the venue accepted the prepared IOC.
Never rerun `submit`, create another journal, or prepare a replacement order
to resolve that ambiguity. The persisted CLOID remains the only order to query.

The `live-probe` feature includes an unsigned recovery command:

```text
hype-live-probe reconcile config.local.toml security-policy.local.toml operational.local.toml journal.jsonl
```

Use the same reviewed endpoint, network selection, execution account, routing
mode, and journal that `prepare` used. Only the public execution-account
environment variable is needed. Do not source a signer environment file.
The command never loads signing material, decrypts a key, reserves a nonce,
submits an exchange action, or recomputes a daily decision. It checks the
prepare-time network/routing binding and protected workflow journal, then
checks the account and market against the durable prepared action before
querying the exact CLOID. Halted operation, revoked keys, and expired live
acknowledgements do not prevent this read-only lookup.

The output starts with:

```text
mode=reconciled durable_finality=<true|false> retry_authorized=false
```

`durable_finality` reflects what this call actually recorded, not a constant:
`true` only once the workflow durably holds a terminal finalization for this
order (bot-strategy#901).

The following JSON contains the CLOID, exchange order ID if found, venue status,
filled/remaining HYPE atoms, `fills_complete`, and `durable_finality`. Preserve
this output with the operator's private probe evidence. The adapter's
account-scoped lookup reads recent fills and then order status; the returned
`filled_hype`/`remaining_hype` are always authoritative (from `orderStatus`
itself), but the underlying fill *rows* can be a truncated view of a bounded,
account-wide recent-fill window. When `fills_complete` is `false`, this call
could not durably record cumulative USDC and skipped fill/finality recording
entirely rather than persisting an understated total — rerun `reconcile` once
the order's fills are no longer competing with other account activity for that
window. An `unknownOid` response is unresolved evidence, never permission to
resubmit, and records nothing durably either (conclusive-absence recording is
still unimplemented — see below).

`submit` now attempts this lookup after both a successful response and a
submission error. If submission failed, it still exits unsuccessfully even when
a subsequent lookup succeeds. If both calls fail, retain the journal and use
`reconcile` once connectivity recovers. Prepared-envelope expiry is checked using the current clock at submission,
after key loading.

## Required follow-up before scheduled live

This recovery command now builds and records `AuthenticatedOrderSubmission`
from raw dex-connector evidence (`orderStatus`/`spotClearinghouseState`
response bodies) and advances `DurableWorkflow` through fill observation to
finalization, once fills are gap-free for the order (bot-strategy#901). It
still deliberately does not manufacture `OrderBoundEligibilityEvidence`
(the staking side) from this reduced reconciliation result — that remains
gated on the separately approved staking custody design. Remaining
requirements:

- Recover order-submission evidence idempotently after crashes, including an
  ambiguous submit landing between the venue accepting the order and this
  command's next successful call — exercised by
  `reconciliation_survives_being_called_again_after_acceptance_is_recorded` in
  `src/live_probe.rs`, but not yet rehearsed end-to-end through a real signer.
- Durably record conclusive absence for an `unknownOid` order
  (`DurableWorkflow::record_order_submission_absent`) — needs gap-free order/
  fill history watermark evidence this binary does not construct yet.
- Settle the capital ledger once and carry attributable spot/residual/staking
  balances across daily workflows before enabling any scheduled purchase.
- Rehearse backup/restore and halt with unresolved orders; a restore must not
  reauthorize a consumed order or capital allocation.
- Complete the separately approved staking custody design and venue capability
  gates. The current policy still rejects automatic staking.

No output of this command is a scheduled-live approval or a staking approval.
