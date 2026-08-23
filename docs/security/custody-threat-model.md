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
| Staking approval | separately controlled master-wallet signer, pending user approval | `cDeposit` and `tokenDelegate` only, for a reconciled workflow | `cWithdraw`, undelegation, transfers, orders, agent approval |
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

An independent signer-side reconciler, not the intent caller, advances a durable
monotonic cursor over authoritative fills. When a purchase workflow becomes
terminal and its canonical order/fill set is final, and before it can become
staking-eligible, the reconciler atomically records the stable account, sorted
order/fill IDs, every exact purchased quantity and their checked total, policy
version, and a canonical content-addressed workflow ID derived from that complete
evidence. Every fill must be registered within a configured deadline after its
first authoritative observation. A cursor gap, a historical fill first presented
after the deadline, or a fill absent from this purchase-time ledger makes the
entire workflow ineligible for automatic staking and sends it to manual review;
the staking request path never backfills it.
Automatic staking is full-fill-set only: the intent amount must equal the sum of
the exact registered quantities for its mapped authoritative fills. The signer
does not create sub-lots, accept a partial amount, or split one workflow across
multiple deposit or delegation phase keys.

The reconciler maintains amount-conserving lot states: `eligible_spot`,
`deposit_reserved`, `deposited_undelegated`, `delegation_reserved`, `delegated`,
and terminal spent, moved, expired, or ineligible states. A deposit reservation
moves the entire exact mapped `eligible_spot` quantity to `deposit_reserved`;
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

Non-workflow sales and transfers debit registered lots deterministically in
ascending `(authoritative fill time, stable order ID, stable fill ID)` order. Any
partial consumption makes the workflow ineligible for automatic staking: the
consumed quantity enters its terminal state and the remainder cannot be enrolled
as a sub-lot. Staking reservations consume the entire exact mapped fill set in
their workflow.
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
  IDs for the purchase;
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
set, workflow ID, and amount exactly match the purchase-time mapping instead of
trusting caller-supplied identifiers or aggregate balance. For `cDeposit`, it
verifies the intent amount equals the entire mapped `eligible_spot` quantity and
that authoritative spot balance covers it. For `tokenDelegate`, it instead
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
claims the unique `(account, workflow ID, action phase)` key with the fill set,
canonical intent digest, and nonce. The deposit phase must atomically move
the full mapped quantity from `eligible_spot` to `deposit_reserved`. The
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
   the final condition.
8. A staking deposit requires reconciled newly purchased HYPE in `eligible_spot`
   and a configured residual buffer. When no validator is eligible it is permitted
   only by the separately approved `hold_undelegated_in_staking` policy; the
   deposit request has no validator field. Delegation requires the same mapped
   quantity in `deposited_undelegated` after authoritative deposit reconciliation,
   plus an explicitly specified, allowlisted active validator that is neither
   jailed nor undelegate-only.
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
- full-fill-set tests proving undersized, oversized, partially consumed, and
  attempted sub-lot intents are rejected without creating another phase key;
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
