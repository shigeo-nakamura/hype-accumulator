# Signing-key lifecycle runbook

This is a rehearsal template. It deliberately contains no wallet address,
ciphertext, host alias, or production filesystem path.

## Preconditions

- Live mode and automatic staking are disabled.
- The selected custody option, typed execution-account kind, and host have explicit
  user approval. No approval enables automatic staking under this model.
- The operator has recorded the expected account, fresh agent public address,
  canonical config, normalized validator allowlist, acknowledgement expiry, and
  every startup-resolved policy identity through the private change record. For
  traced parent funding, independently verify the resolved parent account and
  record the resulting effective-policy digest rather than approving only the
  environment-variable name.
- Backups have passed checksum and clean-directory restore tests.

## Install

1. Generate a new dedicated API-wallet key; never reuse a former agent address.
2. Approve it as a named, expiring agent from the offline master path.
3. Encrypt secret material with the approved KMS key and bind decrypt permission
   to the service role. Do not place plaintext or ciphertext in source control.
4. Install with owner-only permissions while the service is stopped or in a
   read-only validation mode.
5. Verify the account query uses the actual account address and returns the
   expected read-only state. Do not submit an action. Confirm automatic staking is
   rejected for every account kind and the service has no implicit child-to-master
   transfer path.
6. Confirm the service rejects live config without a current acknowledgement and
   rejects a missing, malformed, or changed resolved policy identity. Confirm
   `staking.enabled = true`, a missing value, and unknown values fail startup in
   both dry-run and live configurations.
7. Before order submission, verify the signer-side authorizer independently
   validates the decision and policy inputs and durably binds the exact account,
   decision, CLOID, unsigned request template, L1 nonce, signed `expiresAfter`,
   fee ceiling, `N`/`F`/`C` values, ledger limits, and expiry. Submit an
   unauthorized order with non-production material and confirm its fills remain
   permanently ineligible for automatic staking. Repeat with a forged decision,
   changed price or quantity, reused/unknown CLOID, expired or late authorization,
   and residual reissue lacking a new authorization; none may be backfilled after
   restart or restore. Drop the submission response after the authorization claim
   and verify it remains `submission_claimed` until authoritative CLOID
   reconciliation. When
   the order first appears, verify exactly one transition to `order_bound` and
   ensure terminal enrollment accepts that existing binding without transitioning
   it again. Compute maximum notional `N`, upward-rounded worst-case fee `F`, and
   cash debit `C = N + F` from an independently fetched fee schedule. Race two
   authorizations whose combined `C` exceeds admitted-uncommitted cash after the
   reserve, or whose combined `N` exceeds daily notional room, and verify one
   fails without a partial tranche transition or record. Verify the winner moves
   `C` to `committed` without changing either yearly or lifetime admission
   allocation. With global caps deliberately left sufficient, race two authorizer
   instances using the same authenticated decision and distinct CLOIDs;
   exactly one may reserve the decision's `Q` and `N` and create an active record.
   Reconcile zero, partial, and full fills and verify one atomic daily-ledger
   transition subtracts the full reserved `N`, adds actual `N_f` exactly once to
   the canonical execution-day `spent` counter, and releases `N - N_f`. For a
   cross-day fill, it must also remove every full-`N` mirror without double
   charging `N_f`. Verify a new-CLOID reissue can reserve only the remaining `Q_D`
   and `N_D`. An ambiguous predecessor, changed daily-decision ID, fresh wrapper,
   restart, or restore must retain the full notional reservation, active slot, and
   reject a second authorization. Repeat at exact and one-microunit fee/cash
   boundaries,
   with partial fills, builder fees, rebates, overflow, stale
   or above-ceiling schedules, unknown fixed/non-USDC fees, and an ambiguous fee
   response; cash room must retain `C` while daily notional retains only `N`.
   Run authorizer instances under different host timezones at one second before,
   exactly at, and one second after UTC day and Gregorian-year boundaries. Verify
   they use the shared ledger clock, derive identical half-open bounds and durable
   period IDs, and insert or lock one unique row. A clean-directory restore must
   reproduce those rows and reject a changed scheme, host-clock input, duplicate,
   overlap, or altered boundary without restoring room.
   At claim, test every input freshness horizon and the exact effective-expiry
   boundary; neither an expired `authorized` record nor an ambiguous claimed record
   may return to a reusable state. Verify the exact
   `expiresAfter_ms = effective_expiry_ms - 1` is in the authorization, canonical
   unsigned request-template digest, L1 action hash, and submitted payload.
   Omission or alteration must fail; hold a valid signed IOC request until the
   horizon and prove the venue rejects it without a fill. Confirm GTC, ALO, and
   every resting TIF fail live validation and only the exact IOC-plus-expiry
   envelope can be claimed. Carry an ambiguous IOC claim across daily and yearly
   boundaries and verify `N` reduces each later daily ledger until terminal
   settlement while `C` remains committed in its originating admitted slices
   without charging the new year's admission room. At terminal settlement, verify
   each mirror is removed in the same reserved-to-spent transition and no daily
   row retains unused `N - N_f`. An impossible fill at or beyond effective expiry
   must remain charged, halt live action, and be automatic-staking ineligible.
8. Prove the automatic-staking boundary is absent, not merely guarded at runtime.
   Build and deployment artifacts must contain no staking signer process,
   master-key credential, staking intent endpoint, or outbound `cDeposit` or
   `tokenDelegate` client. Attempts to enable staking for every account kind,
   validator state, or continuation policy must fail before any live capability
   starts. Confirm `hold_in_spot` is the only accepted continuation value and
   never produces an action. Review a delayed-delivery fault model showing that a
   locally valid signed request can stall in a proxy or TCP buffer until after its
   evidence horizon; do not accept local margins, direct submission, payload
   destruction, or no-retry reconciliation as substitutes for a venue-enforced
   deadline in the signed staking action. Confirm purchase lots and residual
   amounts remain unsigned bookkeeping and HYPE remains in spot across restart
   and restore.
9. Rehearse the `running` to `halt_draining` to `halted` state machine. Race the
   halt request against authorization and claim transactions; the cutoff must
   reject every post-cutoff authorization, claim, and signature and terminalize
   unclaimed `authorized` records atomically. Hold a valid signed request after it
   reaches `submission_claimed` and confirm the service reports
   `halt_draining`, never `halted`. Submit it just before `expiresAfter_ms`;
   the cancel-only path must discover and bind it, cancel it if open, and reconcile
   every fill. For a withheld request that never appears, require the ledger clock
   to pass expiry plus a gap-free authoritative order/fill watermark later than
   expiry. Only after every snapshotted claim is terminal and a fresh query shows
   no open orders may the service report `halted`. Drop cancellation and query
   responses, restart, restore, stale the history watermark, and remove the
   cancel-only signer; every case must remain visibly `halt_draining`, re-query
   when possible, alert, and escalate. Staking signatures remain unavailable in
   every state.
10. Prove the claimed hot-balance enforcement outside the API-wallet process.
   Include retained HYPE appreciation, partial fills, delayed custody movement,
   and enforcement outage. Treat `max_hot_trading_balance_microusd` only as an
   operational alert and reject live mode while enforcement is `unapproved`.
   Verify the sweep threshold plus worst-case headroom does not exceed the hard
   maximum, and verify the acknowledged evidence SHA-256 and private change-record
   reference before enabling live mode.
11. At the exact `utc_calendar_year_v1` and lifetime admission-allocation
    boundaries, race direct and separately recorded operator admissions and
    verify the exact admitted amount moves once from `confirmed_unallocated` to
    `uncommitted` while both counters increment once. No admission may exceed
    either ceiling. The half-open UTC year boundary opens only fresh yearly
    admission room; it does not reset lifetime room. Existing admitted slices may
    later move to `committed` without consuming that fresh room, and release,
    settlement, rebate, withdrawal, transfer, replay, restart, or restore must
    never restore either counter. Exceeding an acknowledged ceiling requires a
    newly acknowledged policy.
12. Keep book and account feeds fresh while supplying a missing, malformed, future,
    exactly-at-limit, and expired decision-signal timestamp. Confirm every case
    rejects purchase and that only a non-negative age strictly below the configured
    positive signal-staleness threshold passes this gate.

## Rotate

1. Request manual halt and keep reconciliation/cancel-only containment active
   until the service reaches `halted`; do not treat `halt_draining` as complete.
2. Resolve all ambiguous orders and externally observed manual staking movements.
   Rotation is blocked while an external action is not conclusively reconciled.
3. Generate and approve a new named agent with a new address.
4. Atomically switch the encrypted reference and nonce namespace.
5. Complete read-only health checks, then deregister the old agent.
6. Mark the old address permanently retired and archive the audit evidence.

## Revoke after suspected API-wallet compromise

1. Request manual halt; it may remain `halt_draining` while pre-cutoff claims
   expire or reconcile. Do not use a suspected signer for the cancel-only path.
2. From an independent device, deregister the agent. Do not register a new key
   at the same address.
3. Query authoritative orders, fills, movements, staking state, and delegation
   history for the actual account.
4. Cancel only known open orders through an approved recovery path; contradictory
   state goes to manual review.
5. Rotate affected credentials and restore state from the last verified backup,
   then replay the authoritative history through the incident cutoff.
6. Resume only after the capital equation and spot/staking balances reconcile and
   the user approves a new config acknowledgement.

## Contain suspected offline master-key compromise

An exfiltrated master-wallet key cannot be revoked or rotated in place. API-wallet
deregistration does not contain it. Treat the funded master account as compromised
and execute this procedure only from a clean, independent recovery environment:

1. Request manual halt and keep its claim drain visible until `halted`. Verify no
   runtime staking signer or master credential is deployed. Revoke any accidentally
   present host, IAM, KMS, and network access; do not use the suspected key or
   environment again.
2. Establish a fresh master account under the approved offline recovery process.
   Record its policy and account identifiers in the private change record before
   moving value.
3. Query authoritative orders, fills, balances, movements, staking state, pending
   withdrawals, delegations, and rewards for the compromised account from the
   clean environment. Preserve an incident cutoff and continue monitoring for
   competing actions.
4. If trustworthy control remains, cancel only known orders and migrate immediately
   transferable assets to the pre-approved fresh account using explicit offline
   recovery transactions. The unattended service performs none of these actions.
5. Recover locked or delegated HYPE only through the separately approved manual
   undelegation and withdrawal path, observing protocol locks and the seven-day
   return queue before migration. If exclusive control cannot be established,
   stop and escalate to account-level incident response rather than racing blindly.
6. Retire the compromised master account and every derived API wallet permanently.
   Create new nonce namespaces, credentials, approvals, and ledger migration
   corrections; never reuse an old address as a service signer.
7. Resume on the fresh account only after old and new account histories, capital
   equations, spot/staking balances, and migration records reconcile and the user
   explicitly approves a new configuration acknowledgement.

## Funded-account recovery

The service never initiates funding, withdrawal, undelegation, or `cWithdraw`.
Recovery uses the offline master procedure. Account for the staking lock and
withdrawal queue, record every manual action as an external correction, and do
not convert a recovery into an unattended runtime capability.

## Rehearsal evidence

Record timestamps from the system clock, operator identity, config/artifact
hashes, redacted command outcomes, ledger sequence/hash before and after, and
PASS/FAIL for install, rotate, API-wallet revoke, master-signer containment,
funded-account migration, restore, and read-only restart. Never copy
private keys, signed payloads, wallet addresses, or encrypted blobs into CI logs
or issues.
