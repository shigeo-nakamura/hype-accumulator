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
| Spot execution | dedicated, named API wallet used by one process | capped HYPE/USDC order and cancel | transfers, staking, withdrawals, agent approval |
| Staking approval | separately controlled master-wallet signer, pending user approval | `cDeposit` and `tokenDelegate` only, for a reconciled workflow | `cWithdraw`, undelegation, transfers, orders, agent approval |
| Recovery | offline master-wallet procedure | revoke/replace agent and recover funded account | unattended service access |

Hyperliquid account queries use the actual master or subaccount address, never
the API-wallet address. Nonces belong to the signer, so every bot process uses a
new, dedicated API wallet and a durable monotonic allocator. A deregistered or
expired API-wallet address is never reused.

`cDeposit` and `tokenDelegate` use the user-signed EIP-712 scheme, while spot
orders use the L1 action scheme. The production design therefore treats staking
as master-authority until a testnet or explicitly approved probe demonstrates a
narrower supported authority. An API wallet succeeding at order placement is
not evidence that it can authorize staking.

## Required process isolation

The trading service emits a content-addressed staking intent after an
authoritative fill and balance reconciliation. A separate signer accepts only a
fully specified intent containing:

- workflow and daily decision IDs;
- account, validator, asset, and exact integer `wei` amount;
- hashes of the admitted-capital snapshot, order/fill evidence, and current
  staking state;
- an expiry and a fresh monotonic nonce;
- the remaining approved daily and cumulative limits.

The signer has no generic JSON/action endpoint. It supports only staking deposit
and delegation, rejects unknown fields, and re-queries state before signing an
ambiguous retry. Until this boundary is implemented and rehearsed, automatic
staking remains disabled. Storing a general-purpose master private key in the
bot process is not an acceptable default.

## Capital admission and action gates

All gates are conjunctive and fail closed:

1. `dry_run` is true by default. Live mode requires a versioned acknowledgement
   that binds the config hash, account, signer mode, validator set, and expiry.
2. A deployable tranche originates from authoritative external USDC movement
   history and has a stable event ID. Raw balance delta, order holds/releases,
   fills, fees, internal transfers, dust, and reconciliation corrections never
   create deployable capital.
3. A deposit above the per-deposit limit or beyond yearly/cumulative room remains
   visible but unallocated until a separately recorded operator admission.
4. Committed plus spent USDC cannot exceed admitted deposits minus reconciled
   withdrawals and reserves.
5. A purchase requires fresh book/account data, no unknown movement, no balance
   mismatch, no halt, and available daily/cumulative notional and slippage room.
6. Delegation requires reconciled newly purchased HYPE, a configured residual
   buffer, and an allowlisted active validator that is neither jailed nor
   undelegate-only.
7. An ambiguous action response moves the workflow to reconciliation or manual
   review; it never causes a blind retry.

Limits are positive, finite integer minor units. Zero means disabled, never
unlimited. Production configuration must set all of these explicitly:

- maximum automatically admitted deposit;
- maximum daily purchase notional;
- maximum yearly and cumulative deployable capital;
- maximum order slippage;
- minimum reserve and residual HYPE buffer;
- market/book, account-history, and signal staleness limits;
- validator allowlist and live acknowledgement expiry.

## Threat analysis

| Threat | Prevent | Detect / recover |
| --- | --- | --- |
| Co-host compromise | isolated Unix user, read-only config, no master key in trading process, least-privilege IAM, signer action allowlist | revoke API wallet from an offline master path; halt; reconcile from authoritative history |
| Leaked API key | dedicated named agent per process, low action/notional limits, no reuse | alert on unknown signer/order; revoke and generate a new address |
| Replay or nonce pruning | durable atomic nonce, unique signer per process, bounded expiry where supported, never reuse deregistered/expired agent | reconcile by CLOID/history; rotate signer; never resend an unknown action blindly |
| Malicious validator selection | exact-address allowlist, no yield-based auto-switch, active/not-jailed/not-undelegate-only checks | stop new delegation and require allowlist-owner review |
| Dependency compromise | lockfile, checksums, minimal signing interface, CI audit/review gate | artifact provenance and rollback; rotate signer if signing material may have been exposed |
| State rollback/truncation | hash-chained append-only ledger, atomic snapshot, versioned off-host backup | replay and checksum verification; fail closed on divergence |
| Deposit spoofing or dust | authoritative external movement IDs plus confirmation/admission policy | expose observed vs confirmed vs admitted totals separately; manual classification correction |
| Capital-event misclassification | typed movement categories; balance alone cannot admit funds | invariant checks against account history and total capital equation |
| Operator error | schema validation, config hash acknowledgement, two-step live/staking gates, exact validator/notional display | manual halt, immutable audit trail, rehearsed restore and key rotation |

## Validator governance

The allowlist owner is an operator independent from runtime policy. Changes are
reviewed at least monthly and immediately after a jailed, inactive,
undelegate-only, or commission-change alert. Runtime code may remove a validator
from eligibility but may never add or switch one. When no approved validator is
eligible, purchased HYPE remains reconciled in spot or the staking account per
the configured continuation policy and raises an alert.

Automatic `cWithdraw` and undelegation are absent from the service. Staking to
spot has a seven-day queue and limited pending withdrawals; recovery procedures
must account for that delay rather than adding an emergency automatic action.

## Approval gates

Before production secrets or funds are present, attach evidence for:

- deterministic signature vectors and signer-capability tests on testnet/replay;
- a dry-run key install/rotate/revoke rehearsal using non-production material;
- ledger restore and stale/ambiguous-response fault tests;
- IAM and filesystem permission review;
- the user's explicit choice of custody option, host, validator, limits, and
  exact small-probe configuration.

## Sources

- Hyperliquid, [Nonces and API wallets](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/nonces-and-api-wallets)
- Hyperliquid, [Signing](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/signing)
- Hyperliquid, [Exchange endpoint](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint)
- Hyperliquid, [Staking](https://hyperliquid.gitbook.io/hyperliquid-docs/hypercore/staking)

