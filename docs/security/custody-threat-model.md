# Custody and capital-control threat model

Status: design gate; production custody is **not approved**.

This document defines the security boundary for the HYPE accumulator. It does
not authorize installing a key, admitting a deposit, selecting a validator, or
enabling live actions.

## Trust boundaries

The service is split into four capabilities. A single process must not silently
gain a capability from the presence of a private key.

| Capability | Production principal | Allowed actions | Explicitly denied |
| --- | --- | --- | --- |
| Read/reconcile | unsigned account address | metadata, book, balances, orders, fills, movements, staking state | every exchange action |
| Spot execution | dedicated, named API wallet used by one process | venue-authorized trading actions for its assigned account; service policy submits only capped HYPE/USDC orders and cancels | user-signed transfers, staking, withdrawals, agent approval |
| Staking automation (disabled) | no runtime principal or master key | unsigned eligibility and audit records only | `cDeposit`, `tokenDelegate`, `cWithdraw`, undelegation, transfers, orders, agent approval |
| Recovery | offline master-wallet procedure | revoke/replace agent and recover funded account | unattended service access |

Hyperliquid account queries use the actual master or subaccount address, never
the API-wallet address. Nonces belong to the signer, so every bot process uses a
new, dedicated API wallet and a durable monotonic allocator. A deregistered or
expired API-wallet address is never reused.

An API wallet is modeled as having full trading authority over its assigned
account. Service-side asset, notional, slippage, and hot-balance checks do not
constrain a leaked key and are defense in depth only. The leaked-key maximum loss
is therefore the complete marked-to-market value reachable by that wallet, not
the configured `max_hot_trading_balance_microusd`.

A dedicated execution master account, subaccount, or vault with no unrelated
funds is necessary but is not by itself a security cap. Its typed
`execution_account_kind` is bound into the effective-policy acknowledgement.
Production may claim a bounded maximum loss only after evidence demonstrates
either a venue/account-level hard bound or a separately controlled custody
mechanism outside the API wallet's authority that moves accumulating or
appreciated assets beyond that authority before the bound can be exceeded. The
mechanism must cover `hold_in_spot`, price appreciation, partial fills, retry
ambiguity, and outages, and must enforce a lower admission threshold with measured
worst-case headroom. Any custody mover that expands the action allowlist requires
a separate design and explicit approval; none is authorized here. Until such
enforcement is proven, `hot_balance_enforcement` remains `unapproved`, live mode
is rejected, and the configured threshold is only an operational halt/alert.
Venue-enforced agent restrictions receive credit only after capability tests.
Accepting full authority over the account's actual uncapped value instead of
claiming a cap is the separate explicit policy mode
`hot_balance_enforcement = "accepted_uncapped_authority"`. It claims no bound:
the leaked-key maximum loss is the execution account's complete marked-to-market
value at the time of compromise, including later deposits and appreciation. The
mode is live-valid only when the sweep threshold, worst-case headroom, and
enforcement-evidence digest are all absent (zero or empty — a value there would
assert an enforcement that does not exist), the change-record reference names
the private record of the user's explicit acceptance, and
`max_hot_trading_balance_microusd` is positive as the declared operational alert
threshold, not a cap. The mode string, threshold, and change record are bound
into the effective-policy digest, so an acknowledgement issued for bounded
enforcement never validates an uncapped policy or vice versa. No other gate is
weakened by this mode. Because nothing bounds the hot balance mechanically, the
operator bounds it procedurally: keep only near-term deployable capital in the
execution account and top it up from cold custody as the deposit-aware pacing
admits and re-paces each new tranche. An automated sweep is not an alternative —
moving funds out requires the user-signed path this model keeps offline.

The effective-policy digest binds the enforcement mode, hard maximum, sweep
threshold, worst-case outage headroom, lowercase SHA-256 evidence digest, opaque
approved change-record reference, and the canonical daily/yearly limit-period
schemes. It also binds the aggregate purchase-fee ceiling, mandatory
venue-enforced signed-request-expiry mode, maximum verified venue-clock lag,
venue-clock evidence freshness limit, and the invariant
`staking.enabled = false`. A bounded live mode requires positive finite values,
`sweep threshold + headroom <= hard maximum`, and non-empty valid evidence fields;
`unapproved`, unknown modes or period schemes, a disabled signed expiry, malformed
hashes, `staking.enabled = true`, and inconsistent combinations fail closed.
Changing any field invalidates the acknowledgement.

`cDeposit` and `tokenDelegate` use the user-signed EIP-712 scheme and do not
support a venue-enforced acceptance deadline such as `expiresAfter`, while spot
orders use the L1 action scheme. A request can stall after a local pre-submit
check and reach the venue after its balance, limit, or validator evidence expires.
Direct signer submission and a local time margin do not bound that in-flight
delay. Automatic staking is therefore unavailable for every execution-account
kind: `staking.enabled` must remain `false`, no master signer is installed in the
runtime service, and purchased HYPE remains in spot. A future staking path
requires a venue-enforced deadline bound into the signed action, a revised threat
model, fresh evidence, and explicit user approval. A child-to-master transfer path
would likewise require a separate design and approval; this service does not
infer or perform one.

## Required process isolation

Before any HYPE purchase order is submitted, an independent signer-side
authorizer authenticates the caller, independently obtains the authoritative
account and strategy inputs, and either recomputes the deterministic approved
decision or verifies an authenticated approval artifact issued outside the trading
service. It checks admitted-uncommitted capital, reserve, daily room, signal
freshness, the authoritative fee schedule, slippage, and hot-exposure enforcement
against its own immutable policy anchor. It also obtains independently
authenticated venue-clock evidence whose non-negative age is strictly below the
positive acknowledged `venue_clock_evidence_stale_after_seconds` and verifies
that the conservative venue lag, including sampling uncertainty and maximum
drift through the authorization horizon, is no greater than the acknowledged
`max_venue_clock_lag_ms`. Missing, stale, future, malformed, or above-bound
evidence rejects live authorization. The authenticated decision contains an
immutable canonical decision-chain ID and digest plus exact maximum base quantity
`Q_D` and purchase notional `N_D` in checked integer units; missing, non-positive,
or unrepresentable decision caps reject authorization. For the requested exact
base quantity `Q`, it computes maximum purchase notional
`N = ceil(limit price * quantity in micro-USDC)`, maximum fee
`F = ceil(N * max_purchase_fee_bps / 10000)`, and maximum total cash debit
`C = N + F`. The configured fee ceiling must be an independently verified upper
bound for every venue tier or schedule change possible through effective expiry,
plus any builder, referral, or other proportional execution fee in the exact order
envelope. A current schedule below that bound does not lower `F`. If no such hard
upper bound is available, live purchase is rejected. Unknown, stale,
non-proportional, differently denominated, or above-ceiling fees reject the
purchase; arithmetic overflow or unrepresentable rounding also fails closed.

One durable atomic transaction against a single strongly consistent ledger, using
that ledger's authoritative UTC database clock, inserts or locks the unique
`(execution account, canonical decision-chain ID)` row. That row stores `Q_D`,
`N_D`, permanently consumed fill quantity/notional, the active authorization ID,
and its reserved `Q` and `N`. The transaction requires no unresolved predecessor
and atomically reserves `Q` and `N` no greater than both decision remainders; a
decision can have only one active authorization. The same transaction first
canonicalizes the CLOID to its exact venue byte representation and inserts an
append-only unique `(execution account, canonical CLOID bytes)` ownership row
pointing to this authorization ID and decision chain. An existing row rejects the
authorization even when its former authorization is expired, conclusively absent,
terminal, or restored from backup; a committed CLOID owner is never deleted or
reassigned. If any later reservation fails, the entire transaction, including the
new CLOID claim, rolls back because no authorization was issued.

In that same transaction, the authorizer deterministically selects
admitted-uncommitted tranche slices in stable `(confirmation time, movement ID)`
order and moves exactly `C` from their `uncommitted` state to `committed` while
preserving the configured cash reserve. This is an amount-conserving state
transition inside admission allocations already charged to their originating
yearly and lifetime ceilings; it does not reserve `C` against either ceiling a
second time. It separately reserves `N` against daily purchase-notional room and
uses the appropriate full-asset exposure for the hot limit. It then commits the
one-time pre-purchase authorization. If any complete decision, tranche, or policy
reservation is unavailable, neither a partial transition nor a record is created.
Concurrent authorizer instances acquire CLOID-owner, decision, selected-tranche,
and limit-ledger rows in one documented deterministic order; authorizations for
different decisions but the same account/CLOID therefore cannot both commit. A
process-local clock or ledger snapshot cannot authorize a purchase.
The record contains:

- an authorization ID, canonical decision-chain ID and digest, `Q_D`, `N_D`,
  requested `Q` and `N`, the predecessor authorization ID or explicit null marker,
  and decision room before and after reservation;
- the actual execution account, policy version, market, buy side, mandatory IOC
  TIF, exact client order ID (CLOID), quantity, limit price, and L1 nonce;
- `N`, the independently checked fee schedule and maximum fee `F`, maximum total
  cash debit `C`, maximum slippage, each originating admission allocation and
  tranche slice moved from `uncommitted` to `committed`, the amount reserved in
  each named notional and exposure ledger, canonical daily period IDs and exact
  half-open UTC boundaries, checked room before and after reservation, and any
  exact HYPE quantity reserved to fill the current `residual_hype_wei` deficit
  before staking eligibility is constructed; and
- issue time, integer `effective_expiry_ms` no later than the earliest deadline for
  the policy acknowledgement, decision, signal, book, account, fee-schedule, or
  venue-clock evidence, the evidence timestamp and digest, verified
  `max_venue_clock_lag_ms = L`, exact
  `expiresAfter_ms = effective_expiry_ms - L - 1`, and the authorizer's
  authenticated record digest.

Live order placement requires Hyperliquid's venue-enforced `expiresAfter` field.
The checked subtraction above must not underflow and must produce a positive
representable integer millisecond. The authorizer binds `L`, its evidence, and
that exact value into both its record and the canonical unsigned
exchange-request-template digest. The executor passes the same value into the L1
action hash before signing and sends the byte-identical `action`, nonce,
`vaultAddress`, `expiresAfter`, and signature payload. An omitted, changed,
recomputed, or unsupported expiry or lag bound fails envelope validation or
signature verification. Because the venue compares `expiresAfter_ms` with its own
clock, a venue clock may lag the ledger clock by `L`. Subtracting both `L` and
one millisecond guarantees that when the ledger reaches `effective_expiry_ms`,
the slowest permitted venue clock is already past the signed expiry. If no
positive evidence-backed bound covers uncertainty and drift through that horizon,
live order authorization is unavailable. Local pre-submit checks remain defense
in depth, not the expiry enforcement boundary.

The only live period schemes are `utc_calendar_day_v1` and
`utc_calendar_year_v1`. For a non-negative UTC POSIX second `t` obtained inside
the ledger transaction, the day is the half-open interval
`[floor(t / 86400) * 86400, (floor(t / 86400) + 1) * 86400)` with durable ID
`utc-day-v1:<integer day index>`. The year is the proleptic-Gregorian UTC interval
`[YYYY-01-01T00:00:00Z, (YYYY+1)-01-01T00:00:00Z)` with durable ID
`utc-year-v1:<four-digit YYYY>`. Boundary equality belongs to the next period;
process timezone, locale, daylight-saving rules, and host clock do not participate.
Each ledger row has a uniqueness constraint on
`(execution account, limit kind, period scheme, period ID)` and stores the exact
start/end seconds. Period opening is an insert-or-lock operation in the same
serializable reservation transaction. A conflicting boundary, overlapping or
duplicate row, unsupported timestamp, or restore/replay mismatch fails closed.
Each daily purchase-notional row has checked `reserved`, `spent`, and
`mirrored_encumbrance` counters and enforces
`reserved + spent + mirrored_encumbrance <= limit`. Authorization-ID-keyed
entries make every increment and later transition idempotent. Before a
current-day row can authorize a new purchase or claim an older authorization,
the serializable transaction locks active records in stable authorization-ID
order. It terminalizes expired `authorized` records and materializes a full-`N`
`mirrored_encumbrance` keyed by `(authorization ID, current period ID)` for every
unexpired `authorized` record and every unresolved `submission_claimed` or
`order_bound` record whose originating day is earlier, regardless of whether its
acceptance expiry has passed. Existing entries are verified, not added twice.
These carry-forward entries commit even when the requested new authorization is
denied; they are existing obligations, not a partial reservation for the rejected
request. Expiry alone can terminalize only a never-claimed `authorized` record. A
claimed or bound record keeps its full-`N` encumbrance in every newly prepared day
until authoritative terminal reconciliation proves and settles all fills or proves
conclusive absence. Concurrent authorization, claim, expiry, period opening, and
reconciliation transactions serialize on the same rows.
The same schemes assign authoritative fill timestamps to execution periods.

The record is committed before the API wallet signs the canonical unsigned
exchange-request template. The executor must atomically move exactly one record
from `authorized` to `submission_claimed` with that template digest before
signing. It may append only the resulting signature before submitting the exact
payload. The same transaction uses the ledger's authoritative UTC clock to require
`now_ms < expiresAfter_ms < effective_expiry_ms`, rechecks that the effective
expiry does not exceed any bound input's freshness horizon, independently
revalidates fresh venue-clock evidence at or below the bound `L`, validates the
exact signed `expiresAfter_ms`, and performs the current-day carry-forward
operation above.
If the claim day differs from the originating day, this record's full-`N` mirror
must exist and fit before the state transition commits. Missing room or a failed
claim leaves the record `authorized` and forbids signing. An expired record still
in `authorized` moves
atomically to a terminal unused state, returns each committed cash slice to
`uncommitted` in its same originating admission allocation, subtracts its full
`N` from the originating daily row's `reserved` counter and every existing
mirror, releases every other policy and decision reservation, and clears the
decision's active slot. A
transport-ambiguous submission stays claimed until authoritative CLOID
reconciliation and never releases room or retries blindly.

The approved live policy authorizes IOC plus the signed request expiry only: the
venue rejects late acceptance and immediately cancels every unfilled remainder of
an accepted request, so the order cannot be accepted or rest across the
authorization horizon. GTC, ALO, and any other resting TIF fail configuration and
envelope validation because this venue path provides no approved per-order expiry
at that horizon. A process-local cancel timer is not credited as containment.
Adding a resting order requires a separate design with a venue-enforced expiry no
later than the authorization horizon, mandatory authoritative cancellation, and
explicit approval. Any authoritative fill timestamped at or beyond effective
expiry proves a bypass, mismatch, or venue invariant failure: the purchase remains
visible and charged to capital but is permanently ineligible for automatic
staking and triggers halt/manual review.

An exact CLOID query returns only a candidate, not proof that the service submitted
the order. Before any binding, the reconciler locks and verifies the CLOID-owner
row and obtains the candidate's immutable original envelope and provenance from
authoritative account-scoped venue history. Using the same canonicalization as the
authorization, it requires exact equality for execution account, market, side,
TIF, original quantity, limit price, canonical CLOID bytes, L1 nonce, signed
`expiresAfter_ms`, and every venue-exposed signer/agent or request-digest field.
The authoritative acceptance timestamp must be present and, after applying the
acknowledged venue-clock lag bound and measurement uncertainty, must neither
predate the durable `submission_claimed` timestamp nor reach
`effective_expiry_ms`. A field needed for that comparison that is absent,
ambiguous, mutable, or not authenticated makes live binding unapprovable.

Only an exact envelope and provenance match may atomically insert or verify an
append-only unique `(execution account, stable venue order ID)` ownership row
pointing to the same authorization and CLOID and move `submission_claimed` to
`order_bound`. A mismatched candidate remains unbound, retains every reservation,
halts for account-compromise reconciliation, and may be cancelled only through
the unknown-order containment path; it is never settled against the authorization
or treated as conclusively absent. An existing identical owner is an idempotent
replay; an owner for a different authorization or CLOID likewise halts with every
reservation intact. Later polls validate the immutable envelope, provenance, and
both ownership bindings without repeating the transition. Before any fill affects
settlement, the same rule inserts or verifies
an append-only unique `(execution account, stable venue fill ID)` owner tied to the
bound authorization and order. A conflicting fill owner likewise halts without
settlement, so one venue order or fill cannot discharge two authorizations. All
identifiers come from authoritative account-scoped venue history, never a caller.
A conclusively absent claim moves to a terminal unused state, retains its permanent
CLOID ownership tombstone, and returns each committed cash slice to `uncommitted`
within the same originating admission allocation, subtracts its full `N` from the
originating daily row's `reserved` counter and every later mirrored encumbrance,
releases
every other policy and decision reservation, and clears the decision's active
slot in the same transaction; no terminal authorization is reusable. This is not
an exchange action and grants no generic master-signer
capability. A retry or residual reissue requires a new CLOID and authorization
after authoritative reconciliation of its predecessor. That reconciliation
locks the originating daily row, every later mirrored row, and every canonical
execution-day row. For each execution day `d`, let `N_f,d` be the checked sum of
authoritative fill notional in that day and let `N_f = sum_d(N_f,d) <= N`. One
atomic transition subtracts the full `N` from the originating `reserved` counter,
removes every full-`N` mirrored encumbrance, and adds each `N_f,d` exactly once to
that day's `spent` counter. If origin and execution are the same row, these are
coalesced into `reserved -= N; spent += N_f`; if no fill occurred, `N_f = 0`.
Thus `N - N_f` is released and `N_f` is never left reserved or charged twice.
Underflow, an inconsistent mirror, `N_f > N`, or an execution-day limit breach
fails closed without a partial transition, retains the full reservations, and
halts for reconciliation; the authoritative raw fill evidence remains durable.

The same transaction moves actual consideration plus every authoritative fee
from `committed` to `spent` in the originating admission allocations. It returns
only conclusively unfilled cash and unused fee headroom from `committed` to
`uncommitted` in those same allocations; ambiguous fee or fill state retains the
full cash commitment and full daily-notional reservation. Neither transition
changes the yearly or lifetime admission-allocation counters. Actual filled
base quantity `Q_f` and executed notional `N_f` are permanently consumed from the
decision row, and only the proven `Q - Q_f` and `N - N_f` remainders are released
before the active authorization is cleared. An ambiguous predecessor retains its
full decision reservation and active slot. Only then may a new CLOID reserve no
more than both remaining decision caps; changing the daily-decision ID or
supplying a fresh approval wrapper cannot create a new decision chain. A
caller-supplied decision, authorization record, or policy snapshot is correlation
data only.

An unexpired `authorized` or any unresolved `submission_claimed` or `order_bound`
reservation never disappears at a daily or yearly boundary. Its `N` remains
charged to its originating daily period and is also deducted as the conservative
authorization-ID-keyed encumbrance from every later daily row prepared before its
terminal reconciliation. For a claimed or bound record this continues after
`expiresAfter_ms` and `effective_expiry_ms` while authoritative order/fill history
is unavailable, stale, or lacks the required gap-free watermark. Thus a
pre-midnight authorization competes for next-day room before either a next-day
authorization or its own claim can commit, and an expired ambiguous claim cannot
open room before a delayed fill is discovered. Its `C` remains `committed` inside
the same admission allocations, whose
originating yearly and lifetime ceiling charges never roll over or repeat; an old
admitted slice can be committed after a year boundary without consuming the new
year's admission room. There is no independent process-local rollover job.
Hot-exposure reservations remain continuously charged. Terminal reconciliation
atomically charges actual notional
to the execution-day periods with the reserved-to-spent transition above, moves
actual cash debit including fees to `spent`, returns only proven unused `C` to the
same slices' `uncommitted` state, and preserves the conserved audit trail; a
boundary alone cannot release a commitment or restore admission room.

The following lot bookkeeping is dormant, unsigned eligibility accounting. It
does not authorize, sign, or submit a staking action and cannot override the
mandatory disabled gate.

An independent reconciler advances a durable
monotonic cursor over authoritative fills. When a purchase workflow becomes
terminal and its canonical order/fill set is final, and before it can become
eligible in that dormant ledger, the reconciler requires a distinct signer-side
`order_bound` authorization for every order in the set. Every account,
decision, CLOID, canonical order envelope, limit, and expiry must match exactly,
and all authorizations must belong to the same decision chain. It atomically binds
the terminal fill evidence to each existing immutable order binding and records
the sorted authorization, order, and fill IDs, every exact purchased quantity,
the authorizer-bound residual reservation, each deterministic residual/staking
allocation, their checked total, policy version, and a canonical
content-addressed workflow ID derived from that complete evidence. Every fill
must be registered within a configured deadline after its first authoritative
observation.
A cursor gap, late or absent authorization, non-IOC or mismatched order or fill,
a claim recorded at or after effective expiry or beyond an input freshness
horizon, a fill executed at or beyond effective expiry, a historical fill first
presented after the deadline, or a fill absent from this purchase-time ledger
makes the entire workflow permanently ineligible for automatic staking and sends
it to manual review; neither the reconciler nor staking request path can backfill
or override that state.
Before constructing the staking-eligible fill allocation, the authorizer
atomically reserves the positive deficit between `residual_hype_wei` and already
reconciled, unconsumed `residual_spot`. While that positive top-up reservation is
unresolved, every later purchase authorization fails closed; concurrent requests
cannot reserve or rely on the same deficit. At terminal fill reconciliation, the
reconciler assigns
`min(reserved deficit, total exact fill quantity)` to `residual_spot` and only the
remainder to `eligible_spot`, consuming fills in canonical
`(execution time, order ID, fill ID)` order. It releases any unfilled residual
reservation only with the corresponding terminal order reconciliation. If fills
do not exceed the reserved deficit, the workflow has no staking-eligible amount
and emits no intent. This is the sole permitted split of a purchased fill: both
allocations and their source fill IDs are immutable, remain in one workflow, and
must satisfy exactly
`purchased = residual_spot + eligible_spot`. A caller cannot choose or revise the
split, and a residual allocation can never later become staking-eligible.

The dormant eligibility model is full eligible-allocation only: a candidate
amount must equal
the sum of every exact `eligible_spot` allocation for its mapped authoritative
fills after the residual carve-out. The bookkeeping does not create further
sub-lots or accept a caller-chosen partial eligible amount.

The reconciler maintains amount-conserving lot states: `residual_spot`,
`eligible_spot`, and terminal spent, moved, expired, or ineligible states. There
is no deposit or delegation reservation state and no transition from an eligible
lot into staking. A separately approved offline manual staking movement is
observed as an external movement and makes the affected lot moved and ineligible;
it is never attributed to an automatic workflow. A terminal lot never becomes
eligible again because fungible spot later increases. Before any eligibility
classification, the authoritative fill and movement cursors must be caught up
through a fresh common watermark; a gap or concurrent state that cannot be ordered
against that watermark fails closed.

Non-workflow sales and transfers debit registered spot allocations
deterministically in ascending
`(authoritative fill time, stable order ID, stable fill ID, allocation kind)`
order, with `residual_spot` before `eligible_spot` for the final tie-break. Any
partial consumption of an eligible allocation makes the workflow
ineligible for automatic staking: the consumed quantity enters its terminal state
and the remainder cannot be enrolled as a sub-lot. Consumed `residual_spot`
reopens a deficit but cannot promote any old allocation; only a new
pre-authorized purchase may refill it. No staking reservation exists.
`lot_eligibility_max_age_seconds` makes a remaining lot eligible only while
`now < authoritative fill time + max age`; the boundary itself is expired. The
only supported
`lot_consumption_policy` is `oldest_authoritative_fill_first`; zero expiry,
unknown policies, missing stable tie-breakers, and arithmetic remainder fail
closed rather than selecting an implementation-dependent lot.

The service stops at that unsigned bookkeeping boundary. It has no staking intent
endpoint, staking signer, master-key material, or outbound `cDeposit` or
`tokenDelegate` client. Configuration validation accepts only
`staking.enabled = false` in dry-run and live configurations; `true`, a missing
value, or an unknown value fails startup before any live capability is enabled.
No account kind, validator policy, local expiry check, dispatch margin, retry
rule, or direct-submission implementation may bypass this invariant.

The reason is an uncloseable acceptance-time gap in the current venue protocol.
Once a user-signed staking request has entered a proxy, TCP buffer, or adversarial
network, the service cannot withdraw it or prove that the venue accepted it before
the evidence horizon. Destroying a local payload, blocking retry, and reconciling
an ambiguous result limit duplicates but cannot prevent a stale first acceptance.
Those controls therefore receive no safety credit for automatic staking.

Re-enabling automatic staking requires evidence that the venue rejects acceptance
at or beyond an expiry cryptographically bound into the signed staking action.
That capability must cover both `cDeposit` and `tokenDelegate` and be tested by
delaying an already signed request beyond the horizon. Any future design must
also restore independent evidence validation, one-time phase consumption, and
ambiguity reconciliation in a new reviewed threat model with explicit user
approval. Until then, staking is an offline manual operation outside this
service. Storing a general-purpose master private key in the bot process remains
prohibited.

## Capital admission and action gates

All gates are conjunctive and fail closed:

1. `dry_run` is true by default. Live mode requires a versioned acknowledgement
   that binds an effective-policy digest: the canonical approval-relevant config
   (excluding the acknowledgement value itself), execution account and its typed
   kind, signer mode, normalized validator set, expiry, and the normalized resolved
   parent-account value or an explicit null marker. Environment-variable names
   alone are not part of the approval boundary. Any resolved-value change or
   resolution failure invalidates the acknowledgement and fails closed.
   `staking.enabled = false` is mandatory for every account kind and both dry-run
   and live configuration; there is no automatic-staking acknowledgement or
   signer mode.
2. System-wide deployable capital originates from authoritative external USDC
   movement history with a stable event ID. Raw balance delta, order
   holds/releases, fills, fees, internal transfers, dust, and reconciliation
   corrections never create new admitted capital.
3. An authoritative transfer into an approved isolated execution account may
   create a child tranche only by inheriting an already confirmed and admitted
   parent-account deposit. The ledger binds stable parent-deposit and transfer
   IDs, approved source and destination accounts, and an amount no greater than
   the parent tranche's admitted-uncommitted residual. It atomically moves that
   residual, including its originating admission-allocation IDs, to the child's
   `uncommitted` state exactly once; replay is idempotent, no admission counter is
   incremented, and system-wide admitted capital is unchanged.
4. An untraced, mismatched, duplicate, or excess internal transfer remains visible
   but unallocated and halts new purchases pending reconciliation. Parent
   inheritance is disabled unless the approved funding mode and parent-account
   identity are explicitly configured. At startup the configured environment
   name is resolved to a canonical validated account address and included in the
   effective-policy digest before the acknowledgement is checked. Valid live
   combinations are
   `external_deposit_only` with inheritance disabled and no parent, or
   `traced_parent_transfer` with inheritance enabled and a non-empty approved
   parent-account environment name; every other combination is rejected.
5. Admission is the only transition that consumes yearly or lifetime
   deployable-capital room. One serializable transaction locks a confirmed
   external movement's stable ID and the current `utc_calendar_year_v1` plus
   lifetime admission-allocation rows, moves an exact amount from
   `confirmed_unallocated` to a tranche's `uncommitted` state, and increments
   both allocation counters by that amount exactly once. The move requires the
   resulting counters to remain at or below their acknowledged ceilings. A
   deposit above the per-deposit limit remains visible but unallocated until a
   separately recorded operator admission performs this same transaction; any
   remainder beyond yearly room stays `confirmed_unallocated` until the next
   canonical year or a newly acknowledged ceiling, and any remainder beyond
   lifetime room requires a newly acknowledged ceiling. Operator admission,
   replay, purchase authorization, fill settlement, release, rebate, withdrawal,
   or internal transfer cannot increment twice, decrement, or otherwise restore
   either admission-allocation counter.
6. Each admitted allocation obeys the checked integer conservation invariant
   `admitted = uncommitted + committed + spent + withdrawn`. Authorization moves
   cash only from `uncommitted` to `committed`; settlement moves actual
   consideration and authoritative fees to `spent` and returns proven unused
   headroom to `uncommitted`; a reconciled withdrawal moves available cash to
   `withdrawn`. A reconciled fee rebate may move value from `spent` back to
   `uncommitted` but never changes `admitted`. The system-wide sum of committed
   maximum cash debit `C` plus spent cash cannot exceed admitted deposits minus
   reconciled withdrawals and the configured reserve. The separate daily
   purchase-notional ledger excludes fees by definition; `C` and `spent` include
   them. Therefore admitting 100 units under 100-unit yearly and lifetime
   ceilings leaves zero admission room but 100 units `uncommitted`: a purchase
   may commit those units, while a second deposit cannot be admitted. Requiring
   fresh admission room at purchase authorization is invalid.
7. A purchase requires fresh book/account data and a trusted decision-signal
   generation timestamp whose non-negative age is strictly less than the positive
   configured `signal_stale_after_seconds`. A missing, malformed, future, or
   expired signal timestamp rejects the purchase even when book and account data
   remain fresh. Independently obtained fee-schedule data must likewise have a
   non-negative age strictly below the positive
   `fee_schedule_stale_after_seconds`; missing, malformed, future, or age-at-limit
   data rejects the purchase. Independently authenticated venue-clock evidence
   must have a non-negative age strictly below the positive
   `venue_clock_evidence_stale_after_seconds` and prove, after measurement
   uncertainty and horizon-drift headroom, a lag no greater than the positive
   `max_venue_clock_lag_ms`. An unavailable, stale, future, malformed, unbounded,
   or above-bound clock observation rejects live purchase. It also requires no
   unknown movement, no balance
   mismatch, no halt, no active predecessor and sufficient remaining `Q_D` and
   `N_D` in the authenticated decision chain, enough admitted-uncommitted cash
   after the reserve, available daily purchase-notional room, available slippage
   room, and independently enforced post-purchase hot-exposure room. A
   service-side threshold alone cannot satisfy the final condition. Before order
   submission, the signer-side
   authorizer must durably bind the independently verified decision, exact CLOID
   and canonical unsigned request template, policy version, fee ceiling, effective
   expiry, clock evidence and lag bound `L`, exact signed
   `expiresAfter = effective_expiry_ms - L - 1`, and remaining limits while
   atomically moving `C` from the
   selected admitted allocations' `uncommitted` state to `committed` and
   reserving `N` against daily purchase-notional room. The executor must lock and
   verify that the append-only account/CLOID owner still names this exact
   authorization before claim, signing, or submission. It must reject a
   claim at or beyond the earlier signed expiry or beyond any input freshness
   horizon. Only IOC with the venue-enforced
   signed request expiry is authorized; every resting TIF, omitted or altered
   expiry, and delayed venue acceptance is rejected. No matching authorization
   means no service order; any bypass or fill at or beyond effective expiry is
   permanently ineligible for automatic staking and halts live action. Unresolved
   notional reservations reduce new daily room until terminal settlement;
   unresolved `C` stays committed inside its already charged admission allocations
   and never
   consumes a second yearly or lifetime allocation.
8. Automatic staking is disabled. Because the signed staking actions lack a
   venue-enforced acceptance deadline, the service never creates a deposit or
   delegation reservation, intent, signature, or outbound request. Dormant
   `eligible_spot` accounting remains unsigned and purchased HYPE remains in spot;
   no account type, validator state, continuation policy, local time margin, or
   operator acknowledgement permits `cDeposit` or `tokenDelegate`.
9. An ambiguous exposure-creating action response moves the workflow to
   reconciliation or manual review; it never causes a blind retry.
10. A manual-halt request uses one serializable ledger transaction to move
    `running` to `halt_draining`, record its cutoff, reject every subsequent
    authorization, claim, or signature, and atomically move each unclaimed
    `authorized` record to terminal unused while returning its `C` slices to the
    same allocations' `uncommitted` state, subtracting its full `N` from the
    originating `reserved` counter and every mirror, releasing its other policy
    and decision reservations, and clearing its decision active slot. The same
    transaction snapshots every `submission_claimed` and `order_bound` record with
    its CLOID, signed `expiresAfter_ms`, `effective_expiry_ms`, lag bound, and
    evidence digest. A claim is treated as possibly signed even if local
    persistence says otherwise. The service must not report `halted` or assert
    that service-originated placement has stopped while any snapshot record is
    unresolved.

    Unsigned reconciliation and a mandatory cancel-only execution path remain
    active in both `halt_draining` and `halted`. That path independently queries
    the configured account and outstanding CLOIDs and may sign only cancellations
    for exact, currently open order IDs returned by the authoritative query; it
    cannot accept caller-supplied order identities, place or amend an order,
    perform a staking or transfer action, or clear either halt state. A claimed
    request not yet visible remains pending and is re-queried because it may
    surface before its venue-enforced expiry. If it becomes visible, the
    reconciler binds it, cancels it if open, and accounts for every fill before
    marking it terminal.

    A lagging venue may still accept after the ledger has passed the smaller signed
    `expiresAfter_ms`. Only after the ledger clock has passed the snapshotted
    `effective_expiry_ms`, where the lag-adjusted proof guarantees venue expiry,
    and authoritative order/fill histories have a gap-free watermark later than
    that effective expiry may a still-invisible CLOID be atomically marked
    conclusively absent and terminal unused, return its `C` slices to the same
    allocations' `uncommitted` state, subtract its full `N` from the
    originating `reserved` counter and every mirror, release its other policy and
    decision reservations,
    and clear its decision active slot. It remains unresolved before that point.
    After every claimed/bound record is terminal and a fresh account query
    returns no open order, one transaction may move `halt_draining` to `halted`.
    A delayed request
    discovered during that drain is handled before the transition. Lost responses
    are re-queried; unavailable or stale histories, an unresolved claim, an open
    order, or an unavailable cancel-only signer keeps the system visibly
    `halt_draining`, raises an alert, and escalates to the offline recovery
    procedure. Runtime configuration has no bypass.

Cash and quantity limits are positive, finite integer minor units. Basis-point
fields are non-negative bounded integers, and duration fields are positive bounded
integers. Zero disables a capability or limit and never means unlimited;
`max_purchase_fee_bps = 0` is a strict zero-fee ceiling and permits live purchase
only when authoritative inputs independently prove no fee. Production
configuration must set all of these explicitly:

- maximum automatically admitted deposit;
- maximum daily purchase notional;
- maximum yearly and lifetime admission allocations of deployable capital;
- exact `utc_calendar_day_v1` and `utc_calendar_year_v1` limit-period schemes;
- maximum order slippage;
- aggregate maximum purchase-fee rate, mandatory venue-enforced signed expiry,
  positive verified venue-clock lag bound, and clock-evidence freshness limit;
- minimum reserve and residual HYPE buffer;
- market/book, account-history, fee-schedule, and signal staleness limits;
- purchase-fill registration deadline plus deterministic lot allocation and
  expiration policy;
- mandatory `staking.enabled = false` with no runtime staking signer or client;
- externally enforced hot-balance mode, limit, sweep threshold, and worst-case
  headroom evidence, or the explicit `accepted_uncapped_authority` mode with
  its private acceptance change record and operational alert threshold;
- mandatory cancel-only containment throughout `halt_draining` and `halted`;
- execution-account kind and funding mode, parent-account identity, and whether
  traced transfer admission inheritance is enabled;
- validator allowlist and live acknowledgement expiry.

## Threat analysis

| Threat | Prevent | Detect / recover |
| --- | --- | --- |
| Co-host compromise | isolated Unix user, read-only config, no master key in trading process, least-privilege IAM, signer action allowlist | revoke API wallet from an offline master path; halt; reconcile from authoritative history |
| Leaked API key | full trading authority assumed; dedicated execution account with no unrelated funds; no loss-cap credit without external enforcement proof; one named agent per process and no address reuse | alert on unknown signer/order or operational threshold breach; halt, cancel, revoke, reconcile, and generate a new address |
| Replay or nonce pruning | durable atomic nonce, unique signer per process, bounded expiry where supported, append-only unique account/CLOID, account/order-ID, and account/fill-ID owners, lot consumption, never reuse deregistered/expired agent | reconcile by CLOID/history; halt on any ownership conflict; rotate signer; never resend an unknown action blindly |
| Unauthorized API-wallet order | signer-side durable pre-purchase decision and globally owned CLOID authorization before execution; bind only after the authoritative original account, envelope, nonce, signer/request provenance, and acceptance-time bounds exactly match | a same-CLOID mismatch remains unbound and cannot consume the authorization; unmatched, mismatched, or multiply claimed orders/fills retain reservations, halt, and require trading-account compromise reconciliation |
| Concurrent decision reuse | unique decision-chain row; one active authorization; atomic `Q` and `N` decision reservations with global policy reservations | ambiguous predecessor retains the active slot; terminal reconciliation consumes fills and releases only proven remainder before reissue |
| Delayed or stale order | signed `expiresAfter = effective_expiry_ms - verified venue lag L - 1` plus IOC-only live policy; missing/stale/unbounded clock evidence, GTC/ALO, and changed expiry rejected | even the slowest permitted venue clock is past signed expiry at the effective horizon; any fill at or beyond that horizon halts live action, remains charged, and is ineligible for automatic staking |
| Delayed staking acceptance | automatic staking disabled because user-signed staking actions lack a venue-enforced acceptance deadline; no runtime signer, endpoint, or client | configuration rejects any enabled value before live capability; HYPE remains in spot and staking is manual/offline |
| Fee under-reservation | checked `C = N + ceil(N * aggregate fee bps / 10000)` moves admitted-uncommitted cash to `committed` while preserving the reserve; daily notional separately reserves `N` and admission ceilings are not charged twice | authoritative fee reconciliation retains the full commitment while ambiguous and halts on an unknown, stale, differently denominated, or above-ceiling fee |
| Malicious validator selection | no runtime staking capability; validator data is advisory for offline manual review only | keep HYPE in spot and require a separately approved offline operation |
| Dependency compromise | lockfile, checksums, minimal signing interface, CI audit/review gate | artifact provenance and rollback; rotate signer if signing material may have been exposed |
| State rollback/truncation | hash-chained append-only ledger, atomic snapshot, versioned off-host backup | replay and checksum verification; fail closed on divergence |
| Deposit spoofing or dust | authoritative external movement IDs plus confirmation/admission policy | expose observed vs confirmed vs admitted totals separately; manual classification correction |
| Capital-event misclassification | typed movement categories; transfers inherit only from a traced admitted parent residual and never increase system-wide admission | invariant checks against parent/child account histories, idempotent transfer IDs, and the conserved capital equation |
| Operator error | schema validation, effective-policy digest acknowledgement, live gate, mandatory disabled-staking invariant, exact notional display | `halt_draining` until all signed claims expire/reconcile and open orders cancel; immutable audit trail, rehearsed restore and key rotation |

## Validator governance

Validator metadata and the allowlist are advisory inputs for unsigned research
and offline manual review only. Runtime code may remove a validator from that
view but may never add or switch one. `no_eligible_validator_policy` accepts only
`hold_in_spot`; every alternative and unknown value fails configuration
validation and cannot authorize a deposit. Retained or appreciated HYPE in spot
does not provide a leaked-key balance cap: if it could exceed the independently
enforced bound, no further purchase is authorized and production remains
unapproved until an external containment design is proven.

Automatic `cWithdraw` and undelegation are absent from the service. Staking to
spot has a seven-day queue and limited pending withdrawals; recovery procedures
must account for that delay rather than adding an emergency automatic action.

## Approval gates

Before production secrets or funds are present, attach evidence for:

- deterministic L1 order-signature vectors and signer-capability tests on
  testnet/replay;
- a dry-run key install/rotate/revoke rehearsal using non-production material;
- ledger restore and stale/ambiguous-response fault tests;
- halt-transition tests proving the cutoff atomically blocks new
  authorizations/claims/signatures but remains visibly `halt_draining` while a
  pre-cutoff `submission_claimed` request is withheld. Inject its acceptance just
  before `expiresAfter_ms` and prove discovery, binding, cancellation, and fill
  reconciliation occur before `halted`; with the venue clock at maximum permitted
  lag, absent claims require the ledger to pass `effective_expiry_ms` and a
  gap-free history watermark later than that horizon. Merely passing the smaller
  signed expiry is insufficient. Lost responses, stale history, restart, restore,
  and unavailable signer must preserve draining state and escalation;
- IAM and filesystem permission review;
- proof of venue-enforced agent restrictions or dedicated-account balance
  isolation, including whether a hard venue bound or separately controlled
  custody mechanism keeps purchases, retained HYPE, appreciation, and outages
  below the claimed maximum loss; bind its sweep threshold, worst-case headroom,
  evidence SHA-256, and approved change-record reference into the policy digest;
- conservation, idempotency, source/destination, and excess-transfer tests when
  parent-admission inheritance is enabled;
- automatic-staking boundary tests proving every attempt to enable staking,
  including `staking.enabled = true`, missing or unknown values, every account
  kind, and every validator/continuation setting, fails before any live capability
  is enabled; build and deployment artifacts must contain no runtime staking
  signer, endpoint, master key, `cDeposit`, `tokenDelegate`, or direct-submission
  client;
- amount-conservation tests starting from zero HYPE and covering a fill below, at,
  and above the residual deficit, partial fills, concurrent residual reservations,
  deterministic multi-fill allocation, consumed residual, and restore; every case
  must prove exact `purchased = residual_spot + eligible_spot` accounting and
  preserve the configured positive spot residual, with later authorizations
  denied while a residual top-up is unresolved;
- purchase-time registration and adversarial replay tests proving an unmapped or
  consumed historical fill cannot be enrolled under fresh workflow or daily
  decision IDs, including after a later purchase replenishes balances;
- deterministic multi-lot partial-sale/transfer tests, including identical
  timestamps, stable-ID tie-breaking, expiry boundaries, and replay after restore;
- yearly and lifetime boundary tests proving direct and operator admission share
  one serializable `confirmed_unallocated -> uncommitted` transition, increment
  both admission-allocation counters exactly once, and cannot bypass either
  acknowledged ceiling. Race admissions at the exact boundary; prove only a new
  half-open year restores yearly admission room, lifetime room never resets, and
  purchase, release, settlement, rebate, withdrawal, transfer, replay, restart,
  and restore never change those counters;
- signal-freshness boundary tests proving a missing, malformed, future, or
  age-at-limit decision timestamp rejects a purchase while book/account data is
  otherwise fresh;
- delayed-acceptance tests documenting that local pre-submit checks, direct
  submission, no-retry rules, and dispatch margins cannot prevent an already
  signed staking request from stalling in flight; none may be accepted as a
  substitute for a venue-enforced signed acceptance deadline;
- adversarial pre-authorization tests proving a forged decision, unknown or reused
  CLOID, changed order envelope or limits, late authorization, unmatched fill, and
  residual reissue without a new authorization remain permanently ineligible for
  automatic staking across retry and restore; fault injection must cover the
  `authorized`, `submission_claimed`, and `order_bound` transitions;
- identifier-ownership tests racing separate authorizer instances with different
  decision chains but the same execution account and canonical CLOID while every
  policy cap has room. Exactly one complete authorization may commit; the loser
  creates no authorization or reservation. Terminalize the winner and prove the
  CLOID remains rejected after restart and restore. Also attempt to bind one stable
  venue order ID, then one stable fill ID, to a second authorization and prove each
  conflict retains all reservations, halts before settlement, and cannot double
  charge or discharge capital. Pre-submit a smaller and otherwise altered order
  from another actor under the winner's CLOID; vary market, side, TIF, price,
  quantity, nonce, signer/request provenance, and acceptance time independently,
  and prove every mismatch or unavailable authoritative field prevents
  `order_bound`, retains reservations across restart and restore, and cannot enter
  settlement;
- concurrent-authorization tests proving exact tranche slices totaling `C` move
  atomically from `uncommitted` to `committed` while preserving the reserve,
  without changing yearly or lifetime admission allocations; `N` is separately
  reserved against daily notional and appropriate exposure is reserved against
  the hot limit. Terminal reconciliation must move actual cash to `spent` and
  return only conclusively unused headroom to the same slices, preserving
  `admitted = uncommitted + committed + spent + withdrawn`. Zero, partial, full,
  and cross-day fill cases must also atomically subtract reserved `N`, add `N_f`
  exactly once to canonical execution-day spend, release `N - N_f`, and remove
  mirrors without underflow or double charge;
- decision-concurrency tests racing distinct CLOIDs from separate authorizer
  instances against one authenticated decision while global caps have room;
  exactly one may reserve the unique decision row and create a record. Partial
  fill reconciliation must permanently consume actual `Q_f` and `N_f`, release
  only the proven remainder, and permit a new-CLOID reissue only within both
  remaining decision caps; ambiguity, altered daily-decision IDs, fresh wrappers,
  restart, and restore must not clear the active slot or create another decision
  chain;
- fee-boundary tests covering ceiling equality, one microunit below cash room,
  upward rounding, partial fills, fee rebates, stale or above-ceiling schedules,
  builder/referral fees, overflow, unknown fixed or non-USDC fees, and ambiguous
  fee responses without treating fees as purchase notional or admitted capital;
- canonical-period tests running multiple authorizers under different host
  timezones across one second before, exactly at, and one second after UTC day and
  Gregorian-year boundaries; all instances and clean-directory restores must
  derive identical IDs and half-open bounds, reuse one unique row, preserve prior
  daily spend, admission allocations, and unresolved encumbrances, and reject
  host-clock, overlap, duplicate, unsupported-schema, and boundary-tampering
  cases. A pre-boundary `authorized` record must be mirrored idempotently before
  either a later-day claim or new authorization can commit;
- authorization-expiry tests proving effective expiry is capped by every input
  freshness horizon, independently authenticated venue-clock evidence is fresh,
  and its conservative lag `L` covers measurement uncertainty and maximum drift
  through that horizon. Missing, stale, future, malformed, unbounded, and
  above-policy lag evidence must fail live operation. Prove
  `expiresAfter_ms = effective_expiry_ms - L - 1` with checked arithmetic and bind
  `L`, its evidence digest, and that exact expiry into the authorization, L1
  action hash, and canonical unsigned request-template digest. Claims at the
  signed-expiry boundary and omitted or altered fields must fail; with the venue
  clock exactly `L` behind the ledger, delay a valid signed payload to the
  effective horizon and prove venue rejection with no fill;
- TIF and rollover tests proving GTC/ALO/resting envelopes are rejected, IOC fills
  cannot be accepted at or beyond effective expiry, and every unexpired
  `authorized` plus every unresolved `submission_claimed` or `order_bound` record,
  even after expiry, reduces each later daily period before a new authorization or
  later-day claim can commit. Race a
  pre-midnight `authorized` record's claim against a full-limit next-day
  authorization and prove their authorization-ID-keyed mirrors serialize without
  omission or duplication, including when the new request is denied, across
  restart and restore. Also claim an authorization before midnight, accept and fill
  it after midnight but before expiry, delay authoritative fill history until
  after expiry, and prove its full `N` continues to mirror into every prepared day
  and blocks a full-limit purchase until one atomic terminal reconciliation
  replaces all mirrors with exact execution-day spend. Ambiguous `C` remains in
  its originating commitment
  without consuming a later year's admission room. Terminal settlement must
  convert the originating reservation and every mirror to actual execution-day
  spend in one transaction, leaving no unused `N - N_f` charged;
- acknowledgement tests proving a changed, missing, malformed, or differently
  normalized parent-account value invalidates the effective-policy digest;
- the user's explicit choice of custody option, host, limits, and exact small-probe
  configuration; no approval can enable automatic staking under this model.

## Sources

- Hyperliquid, [Nonces and API wallets](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/nonces-and-api-wallets)
- Hyperliquid, [Signing](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/signing)
- Hyperliquid, [Exchange endpoint](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint)
- Hyperliquid, [Staking](https://hyperliquid.gitbook.io/hyperliquid-docs/hypercore/staking)
- Hyperliquid, [Sub-accounts](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/sub-accounts)
