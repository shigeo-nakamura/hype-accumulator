# Signing-key lifecycle runbook

This is a rehearsal template. It deliberately contains no wallet address,
ciphertext, host alias, or production filesystem path.

## Preconditions

- Live mode and automatic staking are disabled.
- The selected custody option and host have explicit user approval.
- The operator has recorded the expected account, fresh agent public address,
  config hash, validator allowlist hash, and expiry through the private change
  record.
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
   rejects staking when the separate signer is unavailable.

## Rotate

1. Halt new decisions but keep reconciliation active.
2. Resolve all ambiguous orders and staking intents. Rotation is blocked while
   an external action is not conclusively reconciled.
3. Generate and approve a new named agent with a new address.
4. Atomically switch the encrypted reference and nonce namespace.
5. Complete read-only health checks, then deregister the old agent.
6. Mark the old address permanently retired and archive the audit evidence.

## Revoke after suspected compromise

1. Engage manual halt and deny new signing requests.
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

## Funded-account recovery

The service never initiates funding, withdrawal, undelegation, or `cWithdraw`.
Recovery uses the offline master procedure. Account for the staking lock and
withdrawal queue, record every manual action as an external correction, and do
not convert a recovery into an unattended runtime capability.

## Rehearsal evidence

Record timestamps from the system clock, operator identity, config/artifact
hashes, redacted command outcomes, ledger sequence/hash before and after, and
PASS/FAIL for install, rotate, revoke, restore, and read-only restart. Never copy
private keys, signed payloads, wallet addresses, or encrypted blobs into CI logs
or issues.

