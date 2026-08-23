# Signing-key lifecycle runbook

This is a rehearsal template. It deliberately contains no wallet address,
ciphertext, host alias, or production filesystem path.

## Preconditions

- Live mode and automatic staking are disabled.
- The selected custody option, typed execution-account kind, and host have explicit
  user approval. Automatic staking uses only a dedicated master execution account
  that exactly matches the separately controlled signer.
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
   rejected for subaccount, vault, unknown, and master-signer/account mismatch
   configurations; the service has no implicit child-to-master transfer path.
6. Confirm the service rejects live config without a current acknowledgement and
   rejects a missing, malformed, or changed resolved policy identity. Confirm it
   rejects staking when the separate signer is unavailable.
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
   authorizations whose combined `C` exceeds admitted, reserve, yearly, or
   cumulative cash room, or whose combined `N` exceeds daily notional room, and
   verify one fails without a partial record. Repeat at exact and one-microunit
   fee/cash boundaries, with partial fills, builder fees, rebates, overflow, stale
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
   boundaries and verify its `C` and `N` reservations reduce each appropriate
   new-period ledger until terminal settlement; an impossible fill at or beyond
   effective expiry must remain charged, halt live action, and be
   automatic-staking ineligible.
8. Fault-test the signer around claim, signature, and result persistence. A
   consumed or ambiguous `(account, workflow ID, action phase)` must remain
   blocked across caller retry, restart, restore, and a retry with a new nonce.
   Confirm `cDeposit` and `tokenDelegate` use only `signer_submit_once`: the
   signer owns the exchange client, and callers, logs, and the durable ledger
   receive no signature or action payload. At the exact
   `submission_min_remaining_ms` boundary, verify rejection occurs before
   signing. Delay dispatch past intent expiry and verify the signer destroys the
   in-memory payload, performs no write, and blocks the phase for manual
   reconciliation. Inject a partial or unknown write and withhold the response
   beyond expiry; verify no retry is possible, the caller sees only opaque status,
   and a late authoritative success is reconciled. Crash at claim, sign, write,
   and result persistence and verify the consumed phase and submission state
   survive restore.
   Confirm purchase fills are mapped by the independent reconciler at first
   authoritative observation, not by the staking request. Sell or otherwise
   consume a registered lot, replenish aggregate balance, then repackage the old
   fill under new workflow and daily decision IDs; the lot ledger must reject it.
   An old fill missing the purchase-time mapping must also remain ineligible.
   Delay sale/movement ingestion and confirm staking fails until both cursors
   reach a fresh common watermark. Create multiple lots, partially consume them,
   and verify replay selects the same oldest lot using fill time, order ID, then
   fill ID; verify expired lots cannot be reserved. With no eligible validator,
   verify `hold_in_spot` rejects `cDeposit`, while the separately approved
   `hold_undelegated_in_staking` mode accepts a validator-free deposit intent and
   still rejects delegation. Deposit intents containing a validator and delegation
   intents missing one must fail schema validation. Under `hold_in_spot`, stale or
   unavailable validator state must also reject the deposit. Complete a deposit
   from a zero-HYPE account: a terminal total fill below or equal to the atomically
   reserved residual deficit must produce only `residual_spot` and no staking
   intent, while a total fill above the deficit, including a partial fill of a
   larger requested order, must conserve the exact purchase as
   `residual_spot + eligible_spot` using the canonical fill order. Race two
   purchases and verify every later authorization fails while the first residual
   top-up remains unresolved. Consume residual spot and confirm no old eligible
   allocation is promoted and no deposit proceeds until a new authorized purchase
   refills the deficit. Then
   verify the full eligible allocation moves from `eligible_spot` through
   `deposit_reserved` to `deposited_undelegated` while the configured positive
   residual remains in authoritative spot; delegation must reserve only that
   undelegated state, never debit `eligible_spot` again. Submit undersized,
   oversized, and caller-chosen sub-lot amounts and partially consume an eligible
   allocation; every case must reject automatic staking without creating another
   phase key.
   Exercise ambiguous responses, restart, and restore at both reservation
   boundaries.
9. Rehearse manual halt with a resting test order. Confirm new order and staking
   signatures stop before the cancel-only path independently discovers and
   cancels the exact open order. Drop the cancellation response and verify it
   re-queries authoritative state before retrying; signer loss must alert and
   escalate without clearing the halt.
10. Prove the claimed hot-balance enforcement outside the API-wallet process.
   Include retained HYPE appreciation, partial fills, delayed custody movement,
   and enforcement outage. Treat `max_hot_trading_balance_microusd` only as an
   operational alert and reject live mode while enforcement is `unapproved`.
   Verify the sweep threshold plus worst-case headroom does not exceed the hard
   maximum, and verify the acknowledged evidence SHA-256 and private change-record
   reference before enabling live mode.
11. At the exact `utc_calendar_year_v1` and lifetime cumulative capital
    boundaries, verify that neither direct admission nor a separately recorded
    operator admission permits another purchase. Only the half-open UTC boundary
    may restore yearly room; exceeding either acknowledged ceiling requires a
    newly acknowledged policy.
12. Keep book and account feeds fresh while supplying a missing, malformed, future,
    exactly-at-limit, and expired decision-signal timestamp. Confirm every case
    rejects purchase and that only a non-negative age strictly below the configured
    positive signal-staleness threshold passes this gate.

## Rotate

1. Halt new decisions, cancel authoritatively discovered open orders through the
   cancel-only path, and keep reconciliation active.
2. Resolve all ambiguous orders and staking intents. Rotation is blocked while
   an external action is not conclusively reconciled.
3. Generate and approve a new named agent with a new address.
4. Atomically switch the encrypted reference and nonce namespace.
5. Complete read-only health checks, then deregister the old agent.
6. Mark the old address permanently retired and archive the audit evidence.

## Revoke after suspected API-wallet compromise

1. Engage manual halt and deny new exposure-increasing signing requests. Do not
   use a suspected signer for the cancel-only path.
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

## Contain suspected master-signer compromise

An exfiltrated master-wallet key cannot be revoked or rotated in place. API-wallet
deregistration does not contain it. Treat the funded master account as compromised
and execute this procedure only from a clean, independent recovery environment:

1. Engage manual halt, disconnect the staking signer, and revoke its host, IAM,
   KMS, and network access. Do not send another request through the suspected
   signer or host.
2. Establish a fresh master account and separately controlled signer under the
   approved offline recovery process. Record its policy and account identifiers
   in the private change record before moving value.
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
