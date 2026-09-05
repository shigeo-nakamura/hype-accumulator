# Recover an interrupted first-live probe

A submission error can occur after the venue accepted the prepared IOC.
Never rerun `submit`, create another journal, or prepare a replacement order
to resolve that ambiguity. The persisted CLOID remains the only order to query.

The `live-probe` feature includes an unsigned recovery command:

```text
hype-live-probe reconcile config.local.toml security-policy.local.toml operational.local.toml journal.jsonl
```

Use the same reviewed endpoint, network selection, execution account, routing
mode, and journal that `prepare` used. Only the public execution-account
environment variable is needed. Do not source a signer environment file.
The command never loads signing material, decrypts a key, reserves a nonce,
submits an exchange action, or recomputes a daily decision. It checks the
prepare-time network/routing binding and protected workflow journal, then
checks the account and market against the durable prepared action before
querying the exact CLOID. Halted operation, revoked keys, and expired live
acknowledgements do not prevent this read-only lookup.

The output starts with:

```text
mode=reconciled durable_finality=false retry_authorized=false
```

The following JSON contains the CLOID, exchange order ID if found, venue status,
and filled/remaining HYPE atoms. Preserve this output with the operator's private
probe evidence. The adapter's account-scoped lookup reads recent fills and then
order status; returned quantity can include fills whose detailed rows are no
longer in the recent window. It therefore does **not** prove complete fill/fee
history, conclusive absence, final capital debit, or staking eligibility.
An `unknownOid` response is unresolved evidence, never permission to resubmit.

`submit` now attempts this lookup after both a successful response and a
submission error. If submission failed, it still exits unsuccessfully even when
a subsequent lookup succeeds. If both calls fail, retain the journal and use
`reconcile` once connectivity recovers. Prepared-envelope expiry is checked using the current clock at submission,
after key loading.

## Required follow-up before scheduled live

This recovery command addresses operator visibility, not the durable-finality
work in bot-strategy#901. It deliberately does not manufacture
`AuthenticatedOrderSubmission` or `OrderBoundEligibilityEvidence` from the
connector's reduced reconciliation result. Remaining requirements are:

- Persist account-bound order-envelope evidence, exact acceptance time, fills,
  fees, and their provenance before advancing `DurableWorkflow` to finality.
- Recover that evidence idempotently after crashes, including ambiguous submit.
- Settle the capital ledger once and carry attributable spot/residual/staking
  balances across daily workflows before enabling any scheduled purchase.
- Rehearse backup/restore and halt with unresolved orders; a restore must not
  reauthorize a consumed order or capital allocation.
- Complete the separately approved staking custody design and venue capability
  gates. The current policy still rejects automatic staking.

No output of this command is a scheduled-live approval or a staking approval.
