# Signing-key lifecycle runbook

This is a rehearsal template. It deliberately contains no wallet address,
ciphertext, host alias, or production filesystem path.

## Preconditions

- Live mode and automatic staking are disabled.
- The selected custody option and host have explicit user approval.
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
   expected read-only state. Do not submit an action.
6. Confirm the service rejects live config without a current acknowledgement and
   rejects a missing, malformed, or changed resolved policy identity. Confirm it
   rejects staking when the separate signer is unavailable.
7. Fault-test the signer around claim, signature, and result persistence. A
   consumed or ambiguous `(account, workflow ID, action phase)` must remain
   blocked across caller retry, restart, restore, and a retry with a new nonce.
   Repackage the same authoritative fill IDs under new workflow and daily
   decision IDs and confirm the durable one-fill-to-workflow mapping rejects it.
8. Rehearse manual halt with a resting test order. Confirm new order and staking
   signatures stop before the cancel-only path independently discovers and
   cancels the exact open order. Drop the cancellation response and verify it
   re-queries authoritative state before retrying; signer loss must alert and
   escalate without clearing the halt.

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
