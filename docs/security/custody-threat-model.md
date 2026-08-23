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
claiming a cap requires a separate explicit policy mode and user approval; this
document does not grant it.

The effective-policy digest binds the enforcement mode, hard maximum, sweep
threshold, worst-case outage headroom, lowercase SHA-256 evidence digest, opaque
approved change-record reference, and the canonical daily/yearly limit-period
schemes. It also binds the aggregate purchase-fee ceiling, mandatory
venue-enforced signed-request-expiry mode, and the invariant
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
service. It checks admitted capital, reserve, daily/yearly/cumulative room, signal
freshness, the authoritative fee schedule, slippage, and hot-exposure enforcement
against its own immutable policy anchor. The authenticated decision contains an
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
decision can have only one active authorization.

In that same transaction, the authorizer reserves `C` against the admitted tranche
while preserving the configured cash reserve and against yearly and cumulative
deployable-capital room. It separately reserves `N` against daily
purchase-notional room and uses the appropriate full-asset exposure for the hot
limit. It then commits the one-time pre-purchase authorization. If any complete
decision or policy reservation is unavailable, neither a partial reservation nor
a record is created. Concurrent authorizer instances serialize on both the
decision row and limit-ledger rows; a process-local clock or ledger snapshot
cannot authorize a purchase.
The record contains:

- an authorization ID, canonical decision-chain ID and digest, `Q_D`, `N_D`,
  requested `Q` and `N`, the predecessor authorization ID or explicit null marker,
  and decision room before and after reservation;
- the actual execution account, policy version, market, buy side, mandatory IOC
  TIF, exact client order ID (CLOID), quantity, limit price, and L1 nonce;
- `N`, the independently checked fee schedule and maximum fee `F`, maximum total
  cash debit `C`, maximum slippage, the amount reserved in each named cash,
  notional, and exposure ledger, canonical daily/yearly period IDs and exact
  half-open UTC boundaries, checked room before and after reservation, and any
  exact HYPE quantity reserved to fill the current `residual_hype_wei` deficit
  before staking eligibility is constructed; and
- issue time, integer `effective_expiry_ms` no later than the earliest deadline for
  the policy acknowledgement, decision, signal, book, account, or fee-schedule
  data, exact `expiresAfter_ms = effective_expiry_ms - 1`, and the authorizer's
  authenticated record digest.

Live order placement requires Hyperliquid's venue-enforced `expiresAfter` field.
The checked subtraction above must produce a positive representable integer
millisecond. The authorizer binds that exact value into both its record and the
canonical unsigned exchange-request-template digest. The executor passes the same
value into the L1 action hash before signing and sends the byte-identical `action`,
nonce, `vaultAddress`, `expiresAfter`, and signature payload. An omitted, changed,
recomputed, or unsupported expiry fails envelope validation or signature
verification. Because the venue rejects the request after `expiresAfter_ms`,
choosing one millisecond before `effective_expiry_ms` prevents acceptance at or
beyond the authorization horizon even if a valid signed request is delayed or
withheld in transit. Local pre-submit checks remain defense in depth, not the
expiry enforcement boundary.

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
The same schemes assign authoritative fill timestamps to execution periods.

The record is committed before the API wallet signs the canonical unsigned
exchange-request template. The executor must atomically move exactly one record
from `authorized` to `submission_claimed` with that template digest before
signing. It may append only the resulting signature before submitting the exact
payload. The same transaction uses the ledger's authoritative UTC clock to require
`now_ms < effective_expiry_ms`, rechecks that the effective expiry does not exceed
any bound input's freshness horizon, and validates the exact signed
`expiresAfter_ms`. The equality boundary, a future/missing input timestamp, or a
failed claim forbids submission. An expired record still in `authorized` moves
atomically to a terminal unused state, releases every policy and decision
reservation, and clears the decision's active slot. A transport-ambiguous
submission stays claimed until authoritative CLOID reconciliation and never
releases room or retries blindly.

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

As soon as an exact CLOID query finds the order, even before it is terminal, the
reconciler atomically binds the stable venue order ID and moves
`submission_claimed` to `order_bound`. Later polls validate that immutable binding
without repeating the transition. A conclusively absent claim moves to a terminal
unused state, releases every policy and decision reservation, and clears the
decision's active slot in the same transaction; no terminal authorization is
reusable. This is not an exchange action and grants no generic master-signer
capability. A retry or residual reissue requires a new CLOID and authorization
after authoritative reconciliation of its predecessor. That reconciliation
atomically charges actual executed notional to the notional ledger and actual
consideration plus every authoritative fee to the cash ledgers. It releases only
conclusively unfilled notional and unused fee headroom from `C`; ambiguous fee or
fill state retains the full reservation. In the same transaction, actual filled
base quantity `Q_f` and executed notional `N_f` are permanently consumed from the
decision row, and only the proven `Q - Q_f` and `N - N_f` remainders are released
before the active authorization is cleared. An ambiguous predecessor retains its
full decision reservation and active slot. Only then may a new CLOID reserve no
more than both remaining decision caps; changing the daily-decision ID or
supplying a fresh approval wrapper cannot create a new decision chain. A
caller-supplied decision, authorization record, or policy snapshot is correlation
data only.

An unresolved `submission_claimed` or `order_bound` reservation never disappears
at a daily or yearly boundary. It remains charged to its recorded originating
period IDs and is also deducted as a conservative cross-period encumbrance from
each canonical period row opened by a later reservation transaction until terminal
reconciliation. There is no independent process-local rollover job.
Admitted-capital, cumulative, and hot-exposure reservations remain continuously
charged. Terminal reconciliation atomically charges the actual fill to the
canonical execution-period IDs derived from its authoritative timestamp, records
actual notional separately from actual cash debit including fees, releases only
the proven unfilled remainder, unused fee headroom, and mirrored encumbrances, and
preserves a conserved audit trail; a boundary alone cannot make room reusable.

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
   the parent tranche's unallocated residual. It atomically debits that residual
   and credits the child exactly once; replay is idempotent and system-wide
   admitted capital is unchanged.
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
5. A deposit above the per-deposit limit remains visible but unallocated until a
   separately recorded operator admission. Capital beyond yearly room remains
   unallocated until the next canonical `utc_calendar_year_v1` period begins or a
   newly acknowledged policy raises the ceiling; capital beyond the lifetime
   cumulative room requires such a newly acknowledged policy. Operator admission
   alone never overrides either ceiling.
6. Committed maximum cash debit `C` plus spent order consideration and
   authoritative fees cannot exceed admitted deposits minus reconciled withdrawals
   and the configured reserve. The separate daily purchase-notional ledger excludes
   fees by definition; no cash-capital, yearly, or cumulative deployable ledger
   does. Fee rebates are reconciliation events and never pre-credit authorization
   room.
7. A purchase requires fresh book/account data and a trusted decision-signal
   generation timestamp whose non-negative age is strictly less than the positive
   configured `signal_stale_after_seconds`. A missing, malformed, future, or
   expired signal timestamp rejects the purchase even when book and account data
   remain fresh. Independently obtained fee-schedule data must likewise have a
   non-negative age strictly below the positive
   `fee_schedule_stale_after_seconds`; missing, malformed, future, or age-at-limit
   data rejects the purchase. It also requires no unknown movement, no balance
   mismatch, no halt, no active predecessor and sufficient remaining `Q_D` and
   `N_D` in the authenticated decision chain, available daily purchase-notional
   room, available yearly and cumulative deployable-capital room, available
   slippage room, and independently enforced post-purchase hot-exposure room. A
   service-side threshold alone cannot satisfy the final condition. Before order
   submission, the signer-side
   authorizer must durably bind the independently verified decision, exact CLOID
   and canonical unsigned request template, policy version, fee ceiling, effective
   expiry, exact signed
   `expiresAfter`, and remaining limits while atomically reserving `C` against
   admitted, reserve, yearly, and cumulative cash room and `N` against daily
   purchase-notional room. The executor must reject a claim at or beyond that
   expiry or beyond any input freshness horizon. Only IOC with the venue-enforced
   signed request expiry is authorized; every resting TIF, omitted or altered
   expiry, and delayed venue acceptance is rejected. No matching authorization
   means no service order; any bypass or impossible post-expiry fill is permanently
   ineligible for automatic staking and halts live action. Unresolved reservations
   carry into and reduce new daily/yearly room until terminal settlement.
8. Automatic staking is disabled. Because the signed staking actions lack a
   venue-enforced acceptance deadline, the service never creates a deposit or
   delegation reservation, intent, signature, or outbound request. Dormant
   `eligible_spot` accounting remains unsigned and purchased HYPE remains in spot;
   no account type, validator state, continuation policy, local time margin, or
   operator acknowledgement permits `cDeposit` or `tokenDelegate`.
9. An ambiguous exposure-creating action response moves the workflow to
   reconciliation or manual review; it never causes a blind retry.
10. Engaging manual halt atomically denies new order placement, staking deposit,
    delegation, and every other exposure-increasing signed action before
    cancellation begins. Unsigned reconciliation and a mandatory cancel-only
    execution path remain active. That path independently queries the configured
    account and may sign only cancellations for exact, currently open order IDs
    returned by the authoritative query; it cannot accept caller-supplied order
    identities, place or amend an order, perform a staking or transfer action, or
    clear the halt. After every cancellation response it re-queries before
    retrying and continues until no open orders remain. An unavailable signer or
    unresolved cancellation raises an alert and escalates to the offline recovery
    procedure without re-enabling exposure. All other recovery actions require
    that separate procedure; runtime configuration has no general halt bypass.

Cash and quantity limits are positive, finite integer minor units. Basis-point
fields are non-negative bounded integers, and duration fields are positive bounded
integers. Zero disables a capability or limit and never means unlimited;
`max_purchase_fee_bps = 0` is a strict zero-fee ceiling and permits live purchase
only when authoritative inputs independently prove no fee. Production
configuration must set all of these explicitly:

- maximum automatically admitted deposit;
- maximum daily purchase notional;
- maximum yearly and cumulative deployable capital;
- exact `utc_calendar_day_v1` and `utc_calendar_year_v1` limit-period schemes;
- maximum order slippage;
- aggregate maximum purchase-fee rate and mandatory venue-enforced signed expiry;
- minimum reserve and residual HYPE buffer;
- market/book, account-history, fee-schedule, and signal staleness limits;
- purchase-fill registration deadline plus deterministic lot allocation and
  expiration policy;
- mandatory `staking.enabled = false` with no runtime staking signer or client;
- externally enforced hot-balance mode, limit, sweep threshold, and worst-case
  headroom evidence, or separately approved uncapped-authority acceptance;
- mandatory cancel-only containment while halted;
- execution-account kind and funding mode, parent-account identity, and whether
  traced transfer admission inheritance is enabled;
- validator allowlist and live acknowledgement expiry.

## Threat analysis

| Threat | Prevent | Detect / recover |
| --- | --- | --- |
| Co-host compromise | isolated Unix user, read-only config, no master key in trading process, least-privilege IAM, signer action allowlist | revoke API wallet from an offline master path; halt; reconcile from authoritative history |
| Leaked API key | full trading authority assumed; dedicated execution account with no unrelated funds; no loss-cap credit without external enforcement proof; one named agent per process and no address reuse | alert on unknown signer/order or operational threshold breach; halt, cancel, revoke, reconcile, and generate a new address |
| Replay or nonce pruning | durable atomic nonce, unique signer per process, bounded expiry where supported, purchase-time one-fill-to-workflow mapping, lot consumption, one-time authorization/order claims, never reuse deregistered/expired agent | reconcile by CLOID/history; block ambiguous order claims; rotate signer; never resend an unknown action blindly |
| Unauthorized API-wallet order | signer-side durable pre-purchase decision and CLOID authorization before execution; exact order-envelope binding | unmatched or mismatched fills remain permanently ineligible for automatic staking; halt and reconcile the trading-account compromise |
| Concurrent decision reuse | unique decision-chain row; one active authorization; atomic `Q` and `N` decision reservations with global policy reservations | ambiguous predecessor retains the active slot; terminal reconciliation consumes fills and releases only proven remainder before reissue |
| Delayed or stale order | signed `expiresAfter = effective_expiry_ms - 1` plus IOC-only live policy; GTC/ALO and missing/changed expiry rejected; no process-local expiry credit | venue rejects delayed acceptance; any impossible post-expiry fill halts live action, remains charged, and is ineligible for automatic staking |
| Delayed staking acceptance | automatic staking disabled because user-signed staking actions lack a venue-enforced acceptance deadline; no runtime signer, endpoint, or client | configuration rejects any enabled value before live capability; HYPE remains in spot and staking is manual/offline |
| Fee under-reservation | checked `C = N + ceil(N * aggregate fee bps / 10000)` reserves admitted, reserve, yearly, and cumulative cash room while daily notional separately reserves `N` | authoritative fee reconciliation retains full reservation while ambiguous and halts on an unknown, stale, differently denominated, or above-ceiling fee |
| Malicious validator selection | no runtime staking capability; validator data is advisory for offline manual review only | keep HYPE in spot and require a separately approved offline operation |
| Dependency compromise | lockfile, checksums, minimal signing interface, CI audit/review gate | artifact provenance and rollback; rotate signer if signing material may have been exposed |
| State rollback/truncation | hash-chained append-only ledger, atomic snapshot, versioned off-host backup | replay and checksum verification; fail closed on divergence |
| Deposit spoofing or dust | authoritative external movement IDs plus confirmation/admission policy | expose observed vs confirmed vs admitted totals separately; manual classification correction |
| Capital-event misclassification | typed movement categories; transfers inherit only from a traced admitted parent residual and never increase system-wide admission | invariant checks against parent/child account histories, idempotent transfer IDs, and the conserved capital equation |
| Operator error | schema validation, effective-policy digest acknowledgement, live gate, mandatory disabled-staking invariant, exact notional display | manual halt with authoritative cancel-only containment, immutable audit trail, rehearsed restore and key rotation |

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
- halt-transition tests proving new exposure is denied before exact authoritative
  open-order cancellations begin, including lost-response re-query and
  unavailable-signer escalation;
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
- yearly and lifetime boundary tests proving an operator admission cannot bypass
  either acknowledged deployable-capital ceiling;
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
- concurrent-authorization tests proving `C` is atomically reserved against
  admitted, reserve, yearly, and cumulative cash room while `N` is separately
  reserved against daily notional and appropriate exposure is reserved against the
  hot limit; release occurs only for conclusively unfilled or never-submitted
  terminal records with authoritative fee reconciliation;
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
  spend and unresolved encumbrances, and reject host-clock, overlap, duplicate,
  unsupported-schema, and boundary-tampering cases;
- authorization-expiry tests proving effective expiry is capped by every input
  freshness horizon, `expiresAfter_ms` is exactly one millisecond earlier and
  included in the L1 action hash and canonical unsigned request-template digest,
  and the claim rejects the exact boundary, stale inputs, and omitted or changed
  expiry; delay a valid signed payload beyond the horizon and prove venue rejection
  with no fill;
- TIF and rollover tests proving GTC/ALO/resting envelopes are rejected, IOC fills
  cannot be accepted at or beyond effective expiry, and ambiguous reservations
  reduce each new daily/yearly period until terminal settlement without double
  release;
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
