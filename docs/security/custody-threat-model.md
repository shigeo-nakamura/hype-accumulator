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
account. Service-side asset, notional, and slippage checks do not constrain a
leaked key and are defense in depth only. Production therefore requires a
dedicated execution master account, subaccount, or vault whose available trading
balance is independently bounded by the approved hot-balance cap and contains no
unrelated funds. Reconciliation halts before signing if that balance exceeds the
cap. Any venue-enforced agent restrictions must be proven by capability tests
before receiving credit; none are assumed by this design. If account isolation
is incompatible with the approved staking flow, custody remains unapproved unless
the user explicitly accepts full trading authority over the funded account and
its stated maximum loss.

`cDeposit` and `tokenDelegate` use the user-signed EIP-712 scheme, while spot
orders use the L1 action scheme. The production design therefore treats staking
as master-authority until a testnet or explicitly approved probe demonstrates a
narrower supported authority. An API wallet succeeding at order placement is
not evidence that it can authorize staking.

## Required process isolation

The trading service emits a content-addressed staking intent after an
authoritative fill and balance reconciliation. A separate signer accepts only a
fully specified intent containing:

- workflow and daily decision IDs plus the stable authoritative order and fill
  IDs for the purchase;
- account, validator, asset, and exact integer `wei` amount;
- hashes of the admitted-capital snapshot, order/fill evidence, and current
  staking state;
- an expiry and a fresh monotonic nonce;
- the remaining approved daily and cumulative limits.

The signer has no generic JSON/action endpoint. It supports only staking deposit
and delegation and rejects unknown fields. Intent-supplied hashes are correlation
identifiers, not proof that the referenced state or limits are genuine.

For every initial request and retry, the signer authenticates the caller and
independently queries authoritative order, fill, spot-balance, staking, and
validator state using the actual account address. It verifies the newly purchased
HYPE amount and derives the canonical set of stable order/fill IDs rather than
trusting caller-supplied identifiers. It also verifies residual balance,
validator eligibility, intent expiry, and daily and cumulative room against an
independently loaded approved policy and immutable ledger anchor. Missing, stale,
ambiguous, or mismatched evidence is rejected into manual review without signing.

Before producing a signature, one durable atomic transaction establishes an
immutable mapping from every authoritative fill ID in the canonical purchase set
to exactly one account, workflow ID, and purchased amount, then claims the unique
`(account, workflow ID, action phase)` key with that fill set, the canonical
intent digest, and nonce. All fills in an aggregate purchase succeed or fail as a
unit; a fill already mapped to any workflow rejects the entire request even when
the caller supplies a fresh workflow, daily decision, or nonce. Deposit and
delegation are separate, ordered phases, and each may be claimed only once; a
later phase also requires authoritative reconciliation of its predecessor. A
conflicting fill set, amount, digest, or nonce is rejected. A duplicate of a
completed phase may return only the previously stored byte-identical result,
never a fresh signature or nonce. A crash or write ambiguity between claim,
signing, and result persistence leaves the fill mapping and phase blocked for
authoritative reconciliation and manual review. Neither may be cleared by caller
retry, process restart, snapshot restore, or a later balance increase. The fill
mapping and consumed-phase ledger are included in the hash chain and off-host
restore checks.

Until this boundary is implemented and rehearsed, automatic staking remains
disabled. Storing a general-purpose master private key in the bot process is not
an acceptable default.

## Capital admission and action gates

All gates are conjunctive and fail closed:

1. `dry_run` is true by default. Live mode requires a versioned acknowledgement
   that binds an effective-policy digest: the canonical approval-relevant config
   (excluding the acknowledgement value itself), execution account, signer mode,
   normalized validator set, expiry, and the normalized resolved parent-account
   value or an explicit null marker. Environment-variable names alone are not
   part of the approval boundary. Any resolved-value change or resolution failure
   invalidates the acknowledgement and fails closed.
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
5. A deposit above the per-deposit limit or beyond yearly/cumulative room remains
   visible but unallocated until a separately recorded operator admission.
6. Committed plus spent USDC cannot exceed admitted deposits minus reconciled
   withdrawals and reserves.
7. A purchase requires fresh book/account data, no unknown movement, no balance
   mismatch, no halt, and available daily/cumulative notional and slippage room.
8. Delegation requires reconciled newly purchased HYPE, a configured residual
   buffer, and an allowlisted active validator that is neither jailed nor
   undelegate-only.
9. An ambiguous action response moves the workflow to reconciliation or manual
   review; it never causes a blind retry.
10. Manual halt denies every new signed action, including staking deposit and
    delegation, while unsigned reconciliation remains active. Recovery actions
    require the separate offline recovery procedure; runtime configuration has no
    halt bypass.

Limits are positive, finite integer minor units. Zero means disabled, never
unlimited. Production configuration must set all of these explicitly:

- maximum automatically admitted deposit;
- maximum daily purchase notional;
- maximum yearly and cumulative deployable capital;
- maximum order slippage;
- minimum reserve and residual HYPE buffer;
- market/book, account-history, and signal staleness limits;
- execution-account funding mode, parent-account identity, and whether traced
  transfer admission inheritance is enabled;
- validator allowlist and live acknowledgement expiry.

## Threat analysis

| Threat | Prevent | Detect / recover |
| --- | --- | --- |
| Co-host compromise | isolated Unix user, read-only config, no master key in trading process, least-privilege IAM, signer action allowlist | revoke API wallet from an offline master path; halt; reconcile from authoritative history |
| Leaked API key | full trading authority assumed; dedicated balance-bounded execution account, one named agent per process, no unrelated funds or address reuse | alert on unknown signer/order or hot-balance breach; halt, revoke, reconcile, and generate a new address |
| Replay or nonce pruning | durable atomic nonce, unique signer per process, bounded expiry where supported, immutable one-fill-to-workflow mapping, signer-side one-time workflow/action-phase claims, never reuse deregistered/expired agent | reconcile by CLOID/history; block ambiguous fill and phase claims; rotate signer; never resend an unknown action blindly |
| Malicious validator selection | exact-address allowlist, no yield-based auto-switch, active/not-jailed/not-undelegate-only checks | stop new delegation and require allowlist-owner review |
| Dependency compromise | lockfile, checksums, minimal signing interface, CI audit/review gate | artifact provenance and rollback; rotate signer if signing material may have been exposed |
| State rollback/truncation | hash-chained append-only ledger, atomic snapshot, versioned off-host backup | replay and checksum verification; fail closed on divergence |
| Deposit spoofing or dust | authoritative external movement IDs plus confirmation/admission policy | expose observed vs confirmed vs admitted totals separately; manual classification correction |
| Capital-event misclassification | typed movement categories; transfers inherit only from a traced admitted parent residual and never increase system-wide admission | invariant checks against parent/child account histories, idempotent transfer IDs, and the conserved capital equation |
| Operator error | schema validation, effective-policy digest acknowledgement, two-step live/staking gates, exact validator/notional display | manual halt, immutable audit trail, rehearsed restore and key rotation |

## Validator governance

The allowlist owner is an operator independent from runtime policy. Changes are
reviewed at least monthly and immediately after a jailed, inactive,
undelegate-only, or commission-change alert. Runtime code may remove a validator
from eligibility but may never add or switch one. When no approved validator is
eligible, the typed
`no_eligible_validator_policy` controls the next step and raises an alert. The
default `hold_in_spot` value forbids `cDeposit`. The only alternative,
`hold_undelegated_in_staking`, requires separate explicit approval and permits
only `cDeposit`, never delegation; operators must accept its seven-day return
queue. Unknown policy values fail configuration validation.

Automatic `cWithdraw` and undelegation are absent from the service. Staking to
spot has a seven-day queue and limited pending withdrawals; recovery procedures
must account for that delay rather than adding an emergency automatic action.

## Approval gates

Before production secrets or funds are present, attach evidence for:

- deterministic signature vectors and signer-capability tests on testnet/replay;
- a dry-run key install/rotate/revoke rehearsal using non-production material;
- ledger restore and stale/ambiguous-response fault tests;
- IAM and filesystem permission review;
- proof of venue-enforced agent restrictions or dedicated-account balance
  isolation, including the maximum hot balance and breach behavior;
- conservation, idempotency, source/destination, and excess-transfer tests when
  parent-admission inheritance is enabled;
- signer crash/retry/restore tests proving each workflow action phase is consumed
  before signing and cannot be reauthorized with a different nonce;
- adversarial replay tests proving the same authoritative fill set cannot be
  remapped under fresh workflow or daily decision IDs, including after a later
  purchase replenishes balances;
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
