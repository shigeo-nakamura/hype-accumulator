# Recover an interrupted first-live probe

A submission error can occur after the venue accepted the prepared IOC.
Never rerun `submit`, create another journal, or prepare a replacement order
to resolve that ambiguity. The persisted CLOID remains the only order to query.

The `live-probe` feature includes an unsigned recovery command:

```text
hype-live-probe reconcile config.local.toml security-policy.local.toml runtime.local.toml operational.local.toml journal.jsonl
```

Use the same reviewed endpoint, network selection, execution account, routing
mode, runtime config, and journal that `prepare` used. Only the public
execution-account environment variable is needed. Do not source a signer
environment file. The command never loads signing material, decrypts a key,
reserves a nonce, submits an exchange action, or recomputes a daily decision.
Once the order is durably final it does settle the pacing decision that
`prepare` committed in the signer-free runtime (`mode=settled ...`), from the
same durable fill evidence, so the capital ledger's commitment is released or
converted to spend exactly once; before finality it prints
`mode=settlement-deferred` and every later decision day stays blocked as
`PriorDecisionUnsettled` until this command is rerun. Settlement is bound to
the runtime that produced the decision: the journal's durable copy of the
decision (date, `decided_at`, capital/input snapshot hashes, planned and
committed amounts, tranche allocations) must match the runtime's own decision
field-for-field, so pointing the command at a different `runtime.toml` fails
closed instead of settling someone else's same-dated decision. It checks the
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

## Releasing a decision that never reached a signer

`prepare` commits the day's pacing decision in the runtime cycle *before* the
workflow journal exists. If it then fails or crashes before the journal is
created (envelope assembly, inventory aggregation, journal I/O), the capital
stays committed with no order that could ever settle it, and every later
decision day fails closed as `PriorDecisionUnsettled`. Release it with:

```text
hype-live-probe release config.local.toml security-policy.local.toml runtime.local.toml operational.local.toml
```

It takes no journal argument on purpose. The runtime records, in its
hash-chained committed state, the canonical `history_directory` its live
decisions were prepared into (first live `prepare` binds it; a later live
`prepare` naming another directory fails closed), and `release` refuses to
run unless the operational config's bound `history_directory` is that same
directory — so a renamed or copied operational config, which would bind a
fresh, empty history namespace, cannot be used to "prove" absence of a
journal that exists elsewhere. It then scans that directory with the same protected-history
verification `prepare`'s aggregation uses (symlinks and non-regular entries
rejected, orphaned protected heads, empty or rolled-back/truncated journals,
duplicate bindings and inadmissible journals all fail the whole scan closed)
and reads each journal's committed binding. Independently of the directory,
`prepare` records the journal path it is about to create in the runtime's
hash-chained state *before* creating it (`live_journal_intents`), after every
fallible network read; a decision with such a record is never released by
absence — if its journal is present, `reconcile` it; if it is missing, the
history directory was lost (deleted and recreated empty, unmounted) and must
be restored from backup first (bot-strategy#944). Only a decision with no
intent record **and** no journal in the verified directory is released. A decision that **no** journal binds can never have
produced a venue action — signing is only reachable through `submit`, which
needs a committed binding in that directory — so it is settled at zero
(`mode=released ...`), releasing the commitment. A decision that **is** bound
by a journal is refused, with the journal path in the error: its order may
exist at the venue, and only `reconcile` reaching durable finality (or
gap-free conclusive-absence evidence, which this binary cannot construct yet
— bot-strategy#929) may resolve it. An unreadable journal fails the whole
scan closed. The exclusive runtime lock is held for the entire scan-and-
release, so a concurrent `prepare` cannot slip a new journal in between.

A prepared-but-never-submitted order (journal exists, operator declined) is
therefore *not* releasable today; do not run `prepare` unless you intend to
submit, and treat that state as manual review until #929 lands.

## Settlement is final; late contradictory evidence is a manual review

`submit`/`reconcile` settle the pacing decision once the workflow holds a
durable terminal result. If fresh venue evidence later contradicts that
result (a fill discovered after a canceled/unfilled finalization), the
workflow moves itself to `ManualReview`; `reconcile` then refuses to settle
the contested totals (`workflow is in ManualReview`), and a settlement that
was already written from the earlier totals cannot be corrected by this
binary — `settle_live_decision` rejects a conflicting replay rather than
silently overwriting the ledger. Resolving that state needs a durable
settlement-correction event across pacing/ledger/runtime, which is
bot-strategy#901's remaining scope; until then treat it as a manual review
with the journal, the reconcile output, and the runtime state preserved.
