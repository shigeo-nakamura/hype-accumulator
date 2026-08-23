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
| Staking approval | separately controlled master-wallet signer, pending user approval | durable off-exchange purchase-authorization records plus `cDeposit` and `tokenDelegate`, for a reconciled workflow | `cWithdraw`, undelegation, transfers, orders, agent approval |
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
threshold, worst-case outage headroom, lowercase SHA-256 evidence digest, and
opaque approved change-record reference. A bounded live mode requires positive
finite values, `sweep threshold + headroom <= hard maximum`, and non-empty valid
evidence fields; `unapproved`, unknown modes, malformed hashes, and inconsistent
combinations fail closed. Changing any field invalidates the acknowledgement.

`cDeposit` and `tokenDelegate` use the user-signed EIP-712 scheme, while spot
orders use the L1 action scheme. The production design therefore treats staking
as master-authority until a testnet or explicitly approved probe demonstrates a
narrower supported authority. An API wallet succeeding at order placement is
not evidence that it can authorize staking. These approved staking actions contain
no child-account target, so automatic staking is available only when
`execution_account_kind = "dedicated_master"` and the execution account exactly
matches the account recovered from the master signer. A subaccount or vault must
keep `staking.enabled = false`; purchased HYPE remains in spot. A child-to-master
transfer path would expand the signer capability and requires a separate design,
threat model, and explicit approval; this service does not infer or perform one.

## Required process isolation

Before any HYPE purchase order is submitted, an independent signer-side
authorizer authenticates the caller, independently obtains the authoritative
account and strategy inputs, and either recomputes the deterministic approved
decision or verifies an authenticated approval artifact issued outside the trading
service. It checks admitted capital, reserve, daily/yearly/cumulative room, signal
freshness, slippage, and hot-exposure enforcement against its own immutable policy
anchor. One durable atomic transaction both reserves the order's worst-case
notional against its admitted-capital tranche and every daily, yearly, cumulative,
and hot-exposure limit and commits the one-time pre-purchase authorization. If the
complete reservation is unavailable, neither a partial reservation nor a record is
created. Concurrent authorization requests serialize on the same limit ledger.
The record contains:

- an authorization ID and canonical daily-decision ID and digest;
- the actual execution account, policy version, market, buy side, mandatory IOC
  TIF, exact client order ID (CLOID), quantity, and limit price;
- the maximum notional and slippage, the amount reserved in each named ledger,
  checked room before and after reservation, and any exact HYPE quantity reserved
  to fill the current `residual_hype_wei` deficit before staking eligibility is
  constructed; and
- issue time and an effective expiry no later than the earliest deadline for the
  policy acknowledgement, decision, signal, book, or account data, plus the
  authorizer's authenticated record digest.

The record is committed before the API wallet signs or submits the byte-identical
order envelope. The executor must atomically move exactly one record from
`authorized` to `submission_claimed` with that envelope digest before signing. The
same transaction uses a trusted clock to require `now < effective expiry` and
rechecks that the effective expiry does not exceed any bound input's freshness
horizon; the equality boundary, a future/missing input timestamp, or a failed
claim forbids submission. An expired record still in `authorized` moves atomically
to a terminal unused state and releases its reservation. A transport-ambiguous
submission stays claimed until authoritative CLOID reconciliation and never
releases room or retries blindly.

The approved live policy authorizes IOC only: the venue immediately cancels every
unfilled remainder, so the order cannot rest across the authorization horizon.
GTC, ALO, and any other resting TIF fail configuration and envelope validation
because this venue path provides no approved per-order expiry at that horizon. A
process-local cancel timer is not credited as containment. Adding a resting order
requires a separate design with a venue-enforced expiry no later than the
authorization horizon, mandatory authoritative cancellation, and explicit
approval. Reconciliation also rejects any fill whose authoritative execution
timestamp is at or beyond the authorization's effective expiry; the purchase
remains visible and charged to capital but is permanently ineligible for automatic
staking.

As soon as an exact CLOID query finds the order, even before it is terminal, the
reconciler atomically binds the stable venue order ID and moves
`submission_claimed` to `order_bound`. Later polls validate that immutable binding
without repeating the transition. A conclusively absent claim moves to a terminal
unused state and releases its reservation; no terminal authorization is reusable.
This is not an exchange action and grants no generic master-signer capability. A
retry or residual reissue requires a new CLOID and authorization after authoritative
reconciliation of its predecessor. That reconciliation atomically charges the
filled amount, releases only conclusively unfilled reserved room, and debits any
reissue from the remaining decision and policy room. A caller-supplied decision,
authorization record, or policy snapshot is correlation data only.

An unresolved `submission_claimed` or `order_bound` reservation never disappears
at a daily or yearly rollover. It remains charged to its originating period and is
also deducted as a conservative cross-period encumbrance from each newly opened
daily/yearly room until terminal reconciliation. Admitted-capital, cumulative, and
hot-exposure reservations remain continuously charged. Terminal reconciliation
atomically charges the actual fill to its authoritative execution period, releases
only the proven unfilled remainder and mirrored encumbrances, and preserves a
conserved audit trail; rollover alone cannot make room reusable.

An independent signer-side reconciler, not the intent caller, advances a durable
monotonic cursor over authoritative fills. When a purchase workflow becomes
terminal and its canonical order/fill set is final, and before it can become
staking-eligible, the reconciler requires a distinct signer-side
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

Automatic staking is full eligible-allocation only: the intent amount must equal
the sum of every exact `eligible_spot` allocation for its mapped authoritative
fills after the residual carve-out. The signer does not create further sub-lots,
accept a partial eligible amount, or split one workflow across multiple deposit
or delegation phase keys.

The reconciler maintains amount-conserving lot states: `residual_spot`,
`eligible_spot`,
`deposit_reserved`, `deposited_undelegated`, `delegation_reserved`, `delegated`,
and terminal spent, moved, expired, or ineligible states. A deposit reservation
moves the entire exact mapped eligible allocation from `eligible_spot` to
`deposit_reserved` while leaving `residual_spot` in spot;
authoritative `cDeposit` completion moves that same quantity to
`deposited_undelegated`.
A delegation reservation starts only from `deposited_undelegated`, moves it to
`delegation_reserved`, and authoritative `tokenDelegate` completion moves it to
`delegated`. Delegation never debits or reserves the original spot quantity again.
Each transition is atomic. A terminal lot never becomes eligible again because
fungible spot or undelegated balance later increases. Before any reservation, the
authoritative fill and movement cursors must be caught up through a fresh common
watermark; a gap or concurrent state that cannot be ordered against that watermark
fails closed.

Non-workflow sales and transfers debit registered spot allocations
deterministically in ascending
`(authoritative fill time, stable order ID, stable fill ID, allocation kind)`
order, with `residual_spot` before `eligible_spot` for the final tie-break. Any
partial consumption of an eligible allocation makes the workflow
ineligible for automatic staking: the consumed quantity enters its terminal state
and the remainder cannot be enrolled as a sub-lot. Consumed `residual_spot`
reopens a deficit but cannot promote any old allocation; only a new
pre-authorized purchase may refill it. Staking reservations consume the entire
exact mapped eligible allocation in their workflow.
`lot_eligibility_max_age_seconds` makes a remaining lot eligible only while
`now < authoritative fill time + max age`; the boundary itself is expired. The
only supported
`lot_consumption_policy` is `oldest_authoritative_fill_first`; zero expiry,
unknown policies, missing stable tie-breakers, and arithmetic remainder fail
closed rather than selecting an implementation-dependent lot.

The trading service emits a content-addressed staking intent after an
authoritative fill and balance reconciliation. A separate signer accepts only a
fully specified intent containing:

- workflow and daily decision IDs plus the stable authoritative order and fill
  IDs and signer-side pre-purchase authorization IDs for the purchase;
- account, action phase, asset, and exact integer `wei` amount; a delegation
  intent also contains exactly one validator, while a deposit intent contains no
  validator field;
- hashes of the admitted-capital snapshot, order/fill evidence, and current
  staking state;
- an expiry and a fresh monotonic nonce;
- the remaining approved daily, yearly, and cumulative limits.

The signer has no generic JSON/action endpoint. It uses separate strict schemas
for staking deposit and delegation and rejects unknown fields. In particular,
`cDeposit` neither accepts nor infers a validator, while `tokenDelegate` requires
the exact validator address. Intent-supplied hashes are correlation identifiers,
not proof that the referenced state or limits are genuine.

For every initial request and retry, the signer authenticates the caller and
independently queries authoritative order, fill, spot-balance, and staking state
using the actual account address. It verifies that the canonical stable order/fill
set, workflow ID, residual carve-out, and eligible amount exactly match the
purchase-time mapping instead of trusting caller-supplied identifiers or aggregate
balance. For `cDeposit`, it
requires every durable pre-purchase authorization and venue order binding to
remain authentic, consistent, `order_bound` to this workflow, and unused by any
other workflow. It verifies
the intent amount equals the entire mapped eligible allocation, tracked unconsumed
`residual_spot` is at least `residual_hype_wei`, and authoritative spot balance
after subtracting the intent amount remains at least that same positive buffer.
For `tokenDelegate`, it instead
verifies a completed reconciled deposit predecessor and that the same full amount
remains in `deposited_undelegated` with sufficient authoritative undelegated
staking balance; it never requires or reserves `eligible_spot` again. It also
verifies residual balance, intent expiry, and daily, yearly, and cumulative room
against an independently loaded approved policy and immutable ledger anchor.
For a deposit under the default `hold_in_spot` policy, it independently queries
the complete allowlist and requires at least one currently eligible validator,
without accepting or selecting a validator in the intent. Missing or stale
validator state fails closed. Only the explicitly approved
`hold_undelegated_in_staking` policy skips that availability requirement for
`cDeposit`. For delegation, it queries the specified validator and verifies it is
allowlisted, active, not jailed, and not undelegate-only. Missing, stale, consumed,
ambiguous, or mismatched evidence is rejected into manual review without signing.

Before producing a signature, one durable atomic transaction verifies every fill
is already mapped to this workflow, performs the action-specific reservation, and
claims the unique `(account, workflow ID, action phase)` key with the immutable
fill allocations, canonical intent digest, and nonce. The deposit phase must
atomically move the full mapped eligible allocation from `eligible_spot` to
`deposit_reserved`. The
delegation phase must atomically verify its reconciled deposit predecessor and move
the same mapped quantity from `deposited_undelegated` to
`delegation_reserved`; it cannot reserve `eligible_spot`. The signer never creates
or changes the fill-to-workflow mapping while processing an intent. All fills in
an aggregate purchase succeed or fail as a unit. Deposit and delegation are
separate, ordered phases, and each may be claimed only once. Authoritative success
finalizes the reservation into `deposited_undelegated` or `delegated` respectively.
A conflicting state, unmapped fill set, amount, digest, or nonce is rejected. A
duplicate of a completed phase may return only the previously stored byte-identical
result, never a fresh signature or nonce. A crash or write ambiguity between
reservation, signing, and result persistence leaves the action-specific
reservation and phase blocked for authoritative reconciliation and manual review.
Neither may be cleared by caller retry, process restart, snapshot restore, or a
later balance increase. The purchase-time mapping, lot lifecycle, and
consumed-phase ledger are included in the hash chain and off-host restore checks.

Until this boundary is implemented and rehearsed, automatic staking remains
disabled. Storing a general-purpose master private key in the bot process is not
an acceptable default.

## Capital admission and action gates

All gates are conjunctive and fail closed:

1. `dry_run` is true by default. Live mode requires a versioned acknowledgement
   that binds an effective-policy digest: the canonical approval-relevant config
   (excluding the acknowledgement value itself), execution account and its typed
   kind, signer mode, normalized validator set, expiry, and the normalized resolved
   parent-account value or an explicit null marker. Environment-variable names
   alone are not part of the approval boundary. Any resolved-value change or
   resolution failure invalidates the acknowledgement and fails closed. Automatic
   staking additionally requires the execution account to be the dedicated master
   account recovered from the approved signer; subaccount, vault, unknown, and
   mismatched combinations fail closed.
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
   unallocated until the approved period rolls over or a newly acknowledged policy
   raises the ceiling; capital beyond the lifetime cumulative room requires such a
   newly acknowledged policy. Operator admission alone never overrides either
   ceiling.
6. Committed plus spent USDC cannot exceed admitted deposits minus reconciled
   withdrawals and reserves.
7. A purchase requires fresh book/account data and a trusted decision-signal
   generation timestamp whose non-negative age is strictly less than the positive
   configured `signal_stale_after_seconds`. A missing, malformed, future, or
   expired signal timestamp rejects the purchase even when book and account data
   remain fresh. It also requires no unknown movement, no balance mismatch, no
   halt, available daily purchase-notional room, available yearly and cumulative
   deployable-capital room, available slippage room, and independently enforced
   post-purchase hot-exposure room. A service-side threshold alone cannot satisfy
   the final condition. Before order submission, the signer-side authorizer must
   durably bind the independently verified decision, exact CLOID and canonical
   order envelope, policy version, effective expiry, and remaining limits while
   atomically reserving the worst-case notional against every applicable room
   ledger. The executor must reject a claim at or beyond that expiry or beyond any
   input freshness horizon. Only IOC is authorized; every resting TIF is rejected.
   No matching authorization means no service order; any bypass or post-expiry fill
   is permanently ineligible for automatic staking. Unresolved reservations carry
   into and reduce new daily/yearly room until terminal settlement.
8. A staking deposit requires reconciled newly purchased HYPE in `eligible_spot`
   after the authorizer-reserved residual deficit has been carved out into
   `residual_spot`. On an initially empty account, serial terminal purchases must
   first accumulate the configured positive `residual_hype_wei` in spot before
   any remainder can become staking-eligible; a fill no larger than the current
   deficit produces no staking intent. The signer independently requires the
   tracked and post-deposit authoritative residual to meet the configured buffer.
   When no validator is eligible, a deposit is permitted only by the separately
   approved `hold_undelegated_in_staking` policy; the request has no validator
   field.
   Delegation requires the same mapped eligible quantity in
   `deposited_undelegated` after authoritative deposit reconciliation, plus an
   explicitly specified, allowlisted active validator that is neither jailed nor
   undelegate-only.
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

Limits are positive, finite integer minor units. Zero means disabled, never
unlimited. Production configuration must set all of these explicitly:

- maximum automatically admitted deposit;
- maximum daily purchase notional;
- maximum yearly and cumulative deployable capital;
- maximum order slippage;
- minimum reserve and residual HYPE buffer;
- market/book, account-history, and signal staleness limits;
- purchase-fill registration deadline plus deterministic lot allocation and
  expiration policy;
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
| Replay or nonce pruning | durable atomic nonce, unique signer per process, bounded expiry where supported, purchase-time one-fill-to-workflow mapping, lot consumption, signer-side one-time workflow/action-phase claims, never reuse deregistered/expired agent | reconcile by CLOID/history; block ambiguous lot and phase claims; rotate signer; never resend an unknown action blindly |
| Unauthorized API-wallet order | signer-side durable pre-purchase decision and CLOID authorization before execution; exact order-envelope binding | unmatched or mismatched fills remain permanently ineligible for automatic staking; halt and reconcile the trading-account compromise |
| Stale resting order | IOC-only live policy; GTC/ALO rejected; no process-local expiry credit | post-expiry fills are charged but ineligible for automatic staking; unresolved reservations encumber new ledger periods until terminal |
| Malicious validator selection | exact-address allowlist, no yield-based auto-switch, active/not-jailed/not-undelegate-only checks | stop new delegation and require allowlist-owner review |
| Dependency compromise | lockfile, checksums, minimal signing interface, CI audit/review gate | artifact provenance and rollback; rotate signer if signing material may have been exposed |
| State rollback/truncation | hash-chained append-only ledger, atomic snapshot, versioned off-host backup | replay and checksum verification; fail closed on divergence |
| Deposit spoofing or dust | authoritative external movement IDs plus confirmation/admission policy | expose observed vs confirmed vs admitted totals separately; manual classification correction |
| Capital-event misclassification | typed movement categories; transfers inherit only from a traced admitted parent residual and never increase system-wide admission | invariant checks against parent/child account histories, idempotent transfer IDs, and the conserved capital equation |
| Operator error | schema validation, effective-policy digest acknowledgement, two-step live/staking gates, exact validator/notional display | manual halt with authoritative cancel-only containment, immutable audit trail, rehearsed restore and key rotation |

## Validator governance

The allowlist owner is an operator independent from runtime policy. Changes are
reviewed at least monthly and immediately after a jailed, inactive,
undelegate-only, or commission-change alert. Runtime code may remove a validator
from eligibility but may never add or switch one. When no approved validator is
eligible, the typed
`no_eligible_validator_policy` controls the next step and raises an alert. The
default `hold_in_spot` value forbids `cDeposit`. The only alternative,
`hold_undelegated_in_staking`, requires separate explicit approval and permits
only a validator-free `cDeposit`, never delegation; the signer skips validator
lookup and eligibility checks for that action while retaining every purchase,
lot, balance, limit, expiry, and phase-consumption check. Operators must accept
its seven-day return queue. Under `hold_in_spot`, a deposit remains validator-free
but requires a fresh signer-side scan proving at least one allowlisted validator is
eligible; no eligible validator or unavailable state rejects the deposit. Unknown
policy values fail configuration validation. `hold_in_spot`
does not provide a leaked-key balance cap: if retained or appreciated HYPE could
exceed the independently enforced bound, no further purchase is authorized and
production remains unapproved until an external containment design is proven.

Automatic `cWithdraw` and undelegation are absent from the service. Staking to
spot has a seven-day queue and limited pending withdrawals; recovery procedures
must account for that delay rather than adding an emergency automatic action.

## Approval gates

Before production secrets or funds are present, attach evidence for:

- deterministic signature vectors and signer-capability tests on testnet/replay;
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
- signer crash/retry/restore tests proving each workflow action phase is consumed
  before signing and cannot be reauthorized with a different nonce;
- action-specific lot-state tests proving deposit and delegation reserve
  `eligible_spot` and `deposited_undelegated` respectively, including ambiguous
  responses, restore, and rejection of double reservation;
- amount-conservation tests starting from zero HYPE and covering a fill below, at,
  and above the residual deficit, partial fills, concurrent residual reservations,
  deterministic multi-fill allocation, consumed residual, and restore; every case
  must prove exact `purchased = residual_spot + eligible_spot` accounting and
  preserve the configured post-deposit residual, with later authorizations denied
  while a residual top-up is unresolved;
- full-eligible-allocation tests proving undersized, oversized, partially
  consumed, and attempted caller-chosen sub-lot intents are rejected without
  creating another phase key;
- purchase-time registration and adversarial replay tests proving an unmapped or
  consumed historical fill cannot be enrolled under fresh workflow or daily
  decision IDs, including after a later purchase replenishes balances;
- deterministic multi-lot partial-sale/transfer tests, including identical
  timestamps, stable-ID tie-breaking, expiry boundaries, and replay after restore;
- action-schema tests proving deposit intents reject a validator field, delegation
  intents require one, `hold_in_spot` deposits require fresh allowlist
  availability, and approved deposit-only continuation succeeds without validator
  availability while preserving every non-validator gate;
- yearly and lifetime boundary tests proving an operator admission cannot bypass
  either acknowledged deployable-capital ceiling;
- signal-freshness boundary tests proving a missing, malformed, future, or
  age-at-limit decision timestamp rejects a purchase while book/account data is
  otherwise fresh;
- account-target tests proving automatic staking accepts only a dedicated master
  execution account that exactly matches the signer and rejects subaccount, vault,
  unknown, and master-account mismatch configurations;
- adversarial pre-authorization tests proving a forged decision, unknown or reused
  CLOID, changed order envelope or limits, late authorization, unmatched fill, and
  residual reissue without a new authorization remain permanently ineligible for
  automatic staking across retry and restore; fault injection must cover the
  `authorized`, `submission_claimed`, and `order_bound` transitions;
- concurrent-authorization tests proving worst-case notional is atomically reserved
  against admitted, daily, yearly, cumulative, and hot-exposure room and released
  only for conclusively unfilled or never-submitted terminal records;
- authorization-expiry tests proving effective expiry is capped by every input
  freshness horizon and the claim rejects the exact boundary, stale inputs, and
  post-expiry execution;
- TIF and rollover tests proving GTC/ALO/resting envelopes are rejected, IOC fills
  at or beyond effective expiry are ineligible, and ambiguous reservations reduce
  each new daily/yearly period until terminal settlement without double release;
- acknowledgement tests proving a changed, missing, malformed, or differently
  normalized parent-account value invalidates the effective-policy digest;
- the user's explicit choice of custody option, host, validator, limits, and
  exact small-probe configuration.

## Sources

- Hyperliquid, [Nonces and API wallets](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/nonces-and-api-wallets)
- Hyperliquid, [Signing](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/signing)
- Hyperliquid, [Exchange endpoint](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint)
- Hyperliquid, [Staking](https://hyperliquid.gitbook.io/hyperliquid-docs/hypercore/staking)
- Hyperliquid, [Sub-accounts](https://hyperliquid.gitbook.io/hyperliquid-docs/trading/sub-accounts)
