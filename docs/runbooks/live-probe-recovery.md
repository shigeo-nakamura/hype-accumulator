# Recover an interrupted first-live probe

A submission error can occur after the venue accepted the prepared IOC.
Never rerun `submit`, create another journal, or prepare a replacement order
to resolve that ambiguity. The persisted CLOID remains the only order to query.

The `live-probe` feature includes an unsigned recovery command:

```text
hype-live-probe reconcile config.local.toml security-policy.local.toml runtime.local.toml operational.local.toml journal.jsonl
```

Use the same reviewed endpoint, network selection, execution account, routing
mode, runtime config, and journal that `prepare` used. The public
execution-account environment variable is needed, and — because the command
settles the pacing decision in the signer-free runtime once the order is
final — so is the parent-account variable named by the policy's
`admitted_parent_account_env` when `funding_mode = "designated_parent_funding"`
(the runtime refuses to open under a different funding route). Source the
observer environment file, never a signer environment file. The command
never loads signing material, decrypts a key,
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
window. An `unknownOid` response is never permission to resubmit. Before the prepared
order's `effective_expiry_at` it is unresolved evidence and nothing is
recorded. After it, the order can no longer be accepted, and this command
resolves it: it reads the account's complete order history and its complete
retained fill history, and if the prepared client order ID appears in
neither, durably records conclusive absence — the zero-fill terminal outcome
that releases the prepared intent (`absence_recorded` in the JSON, and
`durable_finality` then true), after which the same run settles the pacing
decision at zero. A workflow that bought no HYPE is also driven all the way
to `Complete` (`workflow_completed`): `aggregate_terminal_residual_hype`
treats anything short of that as fail-closed, so a journal left at
`OrderFinalized` would block every later `prepare` for this account. Only a
zero-purchase workflow is completed this way — one holding HYPE needs the
separately approved staking custody design to classify residual versus
eligible. Either history being truncated, or the client order ID
appearing in one of them, fails closed and records nothing.

The order the venue reports may carry a slightly *smaller* quantity than the
envelope authorized: HYPE spot trades on a venue size lot (`szDecimals`) while
the envelope is derived at wei precision, so an authorized 0.30798790 HYPE is
accepted as 0.3. Reconciliation accepts a quantity that is at or below the
authorized one and rejects anything larger (or zero), because every later
cumulative cap is bounded by the authorized quantity and the venue must never
be able to enlarge it. Spend is bounded independently by the envelope's
`max_debit_usdc`, and recorded USDC totals come from the fills themselves. The
practical effect is that a probe can under-spend its budget by up to one lot's
notional; deriving the quantity on the venue's lot grid up front is
bot-strategy#991.

The venue also normalizes the order's *price* onto its own grid (at most five
significant figures for spot), so an authorized 81.172020 USDC per HYPE comes
back as 81.172. The order is a buy, so the authorized price is a ceiling:
reconciliation accepts a venue price at or below it — strictly within the
authorization, and it can only lower the maximum spend — and rejects one above
it, or a non-positive one. Order identity comes from the client order ID, not
from the price. Deriving the price on the venue's grid up front is part of
bot-strategy#991.

Hyperliquid spot charges a **buy's** taker fee in the token being bought: the
2026-09-10 fill matched 0.3 HYPE and carried `fee: 0.00021, feeToken: HYPE`,
so the account was credited 0.29979 HYPE and paid exactly the 24.3063 USDC
notional. The journal keeps the two quantities apart (bot-strategy#998):
`matched_hype` (0.3) is the order-level figure — what "completely filled"
means, and what the fill cap bounds — while `purchased_hype` (0.29979) is
what the account actually holds and is what residual/eligibility inventory
is built from. Recording the matched size as purchased would claim HYPE the
account does not hold, and `aggregate_terminal_residual_hype`'s live-balance
bound would then fail every later `prepare` closed, permanently. The fee is
counted once, on the HYPE side: `debited_usdc` includes only a fee the venue
charged in USDC, never the quote-equivalent of one charged in HYPE. The
per-fill accumulator next to the journal is schema version 2 for this; a
version-1 file (which never captured the fee asset) is refused rather than
upgraded. What to do depends on the journal it belongs to. If the journal
holds no fill observation yet (the earlier run merged fills but failed before
`observe_order_fill`), remove the file and `reconcile` rebuilds it from the
venue. If the journal already holds a fill observation, that observation was
recorded under the old fee semantics: with a fee charged in USDC its totals
are unchanged and a rebuilt reconcile replays it identically (the new field
is written only when a HYPE fee makes credited differ from matched); with a
fee charged in HYPE its totals were wrong, the rebuilt evidence contradicts
them, and the workflow moves to `ManualReview` — treat it as the
settlement-correction case above, never by editing the journal. No such
journal exists on the production host: no fill was ever recorded by a
version-1 binary.

That accepted quantity is durably recorded with the order-submission evidence,
and it — not the authorized quantity — is what a *complete* fill has to
reconcile to. Without that, a venue-rounded order that fills entirely still
looks partial, `filled` finality is rejected as contradictory, and the journal
lands in `ManualReview` instead of settling. It is recorded once, bounded above
by the authorized quantity, and also caps cumulative fills from then on.

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
- ~~Durably record conclusive absence for an `unknownOid` order~~ — done
  (bot-strategy#982): `reconcile` builds the gap-free order/fill watermarks
  from dex-connector's `historical_orders_window` /
  `retained_fills_through` and records
  `DurableWorkflow::record_order_submission_absent`. It fails closed once an
  account's lifetime fills pass the venue's per-request row cap, which is
  when this evidence needs a paginated format.
- Settle the capital ledger once and carry attributable spot/residual/staking
  balances across daily workflows before enabling any scheduled purchase.
- Rehearse backup/restore and halt with unresolved orders; a restore must not
  reauthorize a consumed order or capital allocation.
- Complete the separately approved staking custody design and venue capability
  gates. The current policy still rejects automatic staking.

### Completing a real purchase while staking is disabled (bot-strategy#993)

Recording staking eligibility for an accepted order used to require
signer-side `OrderBoundEligibilityEvidence`, which nothing produces while
staking is disabled — so every real purchase stopped at `OrderFinalized`, and
`aggregate_terminal_residual_hype` (which accepts only `Complete`) then failed
every later `prepare` closed. Auto-staking is deliberately not implemented
(owner decision, 2026-09-10: immaterial yield at this size, a master-key
custody escalation since an API wallet cannot sign staking actions, and a
7-day unbonding delay).

Instead, `reconcile` and `submit` complete such a workflow under a
**staking-disabled attestation**, produced only from a policy that passed the
full live-contract validation (`effective_live_order_policy`, which refuses
`staking.enabled = true` and checks the configured acknowledgement against the
policy's expected digest — a cleared or mismatched acknowledgement withholds
it even while its expiry lies in the future). Which basis a journal accepts is
fixed by its decision binding, never chosen by the operator:

- A binding that carries `eligibility_policy.staking_policy_digest` (every
  decision prepared from this release on) accepts only that digest — the
  fingerprint of the policy's staking section alone, which survives a live
  acknowledgement renewal. Such a decision can be completed under any later
  valid acknowledgement.
- A binding written before that field existed (2026-09-10) accepts only the
  whole-policy `policy_version` it was bound under. That fingerprint covers
  the acknowledgement expiry, so **complete such a decision before renewing
  the acknowledgement**; after a renewal it stays at `OrderFinalized` with no
  attestation that can match it, and needs the settlement-correction path.

An attestation naming any other digest or version fails closed on every open.
The residual/eligible split is computed exactly as before. When the policy is
not live-valid, lookup, fill recording and settlement still run (read-only
recovery must not depend on it), the command prints a note, and the workflow
stays at `OrderFinalized` until it is and `reconcile` is rerun. When staking
is enabled later, the evidence producer is added then; workflows completed
under an attestation remain distinguishable in the journal.

No output of this command is a scheduled-live approval or a staking approval.

## Backfilling attribution for purchases settled before the inventory ledger

Attribution is withheld entirely while any settled purchase lacks its
acquisition evidence (bot-strategy#929): the dashboard reports zero HYPE with
`HYPE attribution unavailable` rather than a partial sum that would understate
bot-owned inventory without saying so. Purchases settled by a build older than
that ledger have no evidence, so they need a one-time migration:

```text
hype-live-probe backfill-attribution config.local.toml security-policy.local.toml runtime.local.toml operational.local.toml
```

**Run it exactly the way a probe-day `reconcile` is run**: same user, same
environment. On the current host that means as root with the observer
environment sourced (the account variables must be present — `sudo -u` drops
them and the command then fails on a missing account), the recurring timer
stopped, and `fix-anchor-ownership.sh` run afterwards, because a root run
leaves runtime state and lock files root-owned just as a probe does
(bot-strategy#972). The protected-head sidecars are owner-only, which is why
the read-only observer can never do this itself.

It is signer-free and economically inert — no capital moves, no order is
prepared, no venue is contacted — and idempotent: a second run prints
`mode=nothing-to-backfill`. It takes no journal argument, for the same reason
`release` does not: it works from the decisions the runtime itself holds, and
refuses to read evidence out of any directory other than the one its live
decisions were prepared into (each declared journal must also resolve into
that directory).

For each settled purchase with no evidence it opens that decision's own
journal **the way `reconcile` opens it** — which includes the exchange-order
owner reconciliation; an owner conflict moves the journal to `ManualReview`
exactly as a `reconcile` would, and that is then refused below — and, under
the journal's append lock, verifies before recording anything:

* the journal is admissible for this network and vault-routing mode;
* its binding's execution identity is the configured account's (derived from
  the connector's canonical form of the address, so a checksummed address in
  the environment is fine);
* the order reached durable finality — `OrderFinalized` or later, never
  `ManualReview`, never a stage at which a restored journal could still be
  missing its finalization;
* it is bound to that very decision (checked by the runtime against the
  journal's own binding, which names the disagreeing field);
* it holds exactly the filled and debited USDC the decision settled with.

Deliberately *not* required: that the workflow reached `Complete`. Reaching
`Complete` needs the staking-disabled attestation, which is withheld while the
policy acknowledgement is expired — requiring it would block the migration on
exactly the hosts that need it.

One thing the journal cannot prove: a fill event written before
bot-strategy#998 carried no credited quantity and replays as the matched one,
which in the journal looks exactly like a modern fee-free fill. Settlement
guards this with the venue's own credited figure; a venue-free backfill
cannot. A pre-#998 journal whose fee was charged in HYPE is therefore
attributed the fee too, and that shows up as attribution above holdings — the
divergence halt — not as a silently accepted number. (The only such journal on
the current host, 2026-09-10, had its terminal events rewritten after #998
and carries the credited quantity.)

A missing journal is reported as lost history with the restore path
(bot-strategy#944); an unreadable one as its own error. **Every decision is
attempted**: each is verified against its own journal and committed on its
own, so one refused journal does not stop the others from being recorded.
Failures are printed per decision and the command exits non-zero if any
remain, so a caller chaining on it cannot mistake a partial run for a fixed
dashboard; re-running records only what is still missing.

Expected output on a host whose only real purchase is 2026-09-10:

```text
mode=backfilled decision=fixed-dca:2026-09-10 workflow=… credited_hype_atoms=29979000 journal=…
mode=backfill-complete attributed_hype_atoms=29979000 settled_purchases=1 missing=0 complete=true
```

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
be restored from backup first (bot-strategy#944). The same records act as a
manifest for `prepare`: before it touches the venue or aggregates history,
every recorded intent must resolve to a present journal bound to exactly
that decision (`JournalIntentUnresolved` otherwise), so lost history blocks
new orders instead of silently aggregating to zero. Only a decision with no
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

A prepared-but-never-submitted order (journal exists, operator declined or
the run was interrupted before `submit`) is not released by this command —
it is resolved by `reconcile` once the prepared order has expired, as
described above, which records conclusive absence and settles the decision
at zero. `release` stays reserved for a decision that never got a journal at
all.

## A history directory that lost its journals

`prepare` and `release` both refuse to read a `history_directory` that no
longer holds a journal an earlier run recorded there:

```text
history directory lost journals: /opt/hype-accumulator/journals no longer
holds 1 journal(s) an earlier run recorded there (2026-09-09.jsonl); journals
are only ever added, so history has been lost (unmounted, deleted and
recreated, or different underlying storage?). Restore it before running this
command again.
```

The record lives in `<operational>.history-directory-binding.json`, next to
the operational config and outside `history_directory`, so it survives that
directory's loss. It catches what an existence check cannot: a journal
filesystem unmounted while leaving its ordinary mount-point directory behind,
or a directory deleted and recreated empty — both of which still pass
`is_dir()`, aggregate to less than they should, and would otherwise read
exactly like a smaller account (the live-balance bound only ever rejects a
total that is too *large*). Unlike `live_journal_intents`, which protects the
decision currently in flight, this protects every already-settled day's
journal.

Journals are recorded by name, not counted: with a count, each newly created
journal would silently substitute for a lost older one and the loss would
never surface. Each recorded journal is re-verified against its own protected
head on every scan, so a name that is still present cannot have had its
contents swapped.

The comparison runs *inside* the verified scan it protects — the same scan
that aggregates residual, or that `release` reads bindings from — so history
cannot disappear between the check and its use. The record is written by that
scan the moment it succeeds and before the run's own journal exists, so a
failed write leaves no journal behind and the retry simply scans and records
again. Only `prepare` records; `release` checks the record but never writes
it.

Recovery is to restore the journals — from the off-host ledger backup, or the
host's own backup of `history_directory` — and rerun the command; the error
names the journals that are missing, and a partial restore keeps failing until
all of them are back. **Restore the newest backup**: each journal's protected
head lives beside the journal itself, so an older journal and its own sidecar
are self-consistent and this check cannot tell them from the current pair
(bot-strategy#974). After any restore, `reconcile` each restored journal
before running `prepare` again. Never "fix" this by deleting or editing the binding
file: that discards the only evidence that the missing journals ever existed,
and the next `prepare` would then treat still-unstaked HYPE from those
journals as a fresh, staking-eligible fill. If history genuinely has to be
abandoned (a decommissioned account), start a new operational config path
instead, which binds a new directory and a new record.

## Settlement is final; late contradictory evidence is a manual review

`submit`/`reconcile` settle the pacing decision once the workflow holds a
durable terminal result. If fresh venue evidence later contradicts that
result (a fill discovered after a canceled/unfilled finalization), the
workflow moves itself to `ManualReview`; `reconcile` then refuses to settle
the contested totals (`workflow is in ManualReview`). The settlement itself
runs with the journal's append lock held and only after re-verifying that
the journal has not advanced since this invocation loaded it, so an
overlapping `submit`/`reconcile` cannot slip a late fill in between the
check and the runtime commit (it fails with `ConcurrentModification` and is
simply rerun). A settlement that
was already written from the earlier totals cannot be corrected by this
binary — `settle_live_decision` rejects a conflicting replay rather than
silently overwriting the ledger. Resolving that state needs a durable
settlement-correction event across pacing/ledger/runtime, which is
bot-strategy#901's remaining scope; until then treat it as a manual review
with the journal, the reconcile output, and the runtime state preserved.
