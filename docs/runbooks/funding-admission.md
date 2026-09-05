# Amount-bounded funding admission

Recognized funding remains unallocated until the protected admission artifact
supplies independently established confirmation evidence and an operator
approval. This applies to external deposits and designated-parent funding.

An approval may include `max_admitted_usdc`, a positive integer number of
**USDC microunits** (1 USDC = 1,000,000 units). It limits the **total** capital
admitted from that event, not an additional amount per scan. For example, an
explicitly approved total of 10,000 USDC is `10000000000`. The ceiling must not
exceed the original authoritative movement amount. A zero, negative, fractional,
or string value is rejected.

If the field is absent or null, the existing per-deposit automatic admission
limit still applies. If present, this exact operator ceiling replaces the
per-deposit automatic limit and can be either smaller or larger. It never
replaces the shared yearly or lifetime admission ceilings. Actual admission is
bounded by remaining event capital, including prior unadmitted returns, and
remaining yearly/lifetime capacity. Existing confirmation requirements, approval
time, cooldown, reserves, daily purchase cap, and halt gates still apply.

The approval's event ID, original confirmation/approval times and optional amount
are retained in authenticated runtime state. Once approval has been recorded,
changing, adding, or removing its amount is rejected before cycle persistence.
Omitting the entire entry on later scans preserves its durable approval; it does
not revoke already admitted capital. This interface does not implement approval
revision or revocation. Do not rewrite the artifact or reset a ledger to enlarge
an existing approval or recover capacity. A legacy approval without an amount
cannot later be upgraded through this field.

An approval after a daily decision boundary cannot retroactively fund that
decision. It becomes available for future decisions, after all readiness gates.
The new field is omitted when absent in serialized capital state, preserving
legacy snapshot/proof serialization. Old binaries reject the new artifact field;
once new approvals are recorded, use a compatible release for recovery.

## Evidence and deployment

This change consumes evidence; it does not manufacture confirmations or generate
an automatic approval producer. Repeated REST polls, balance changes, or elapsed
wall-clock time are not confirmation counts. The documented Hyperliquid
[ledger update response](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions)
contains time, hash and a movement delta, without a confirmation-count field.
[API-server execution semantics](https://hyperliquid.gitbook.io/hyperliquid-docs/hypercore/api-servers)
do not establish a second confirmation simply because an observer reads again.
Verify an authoritative evidence source and its chain-specific interpretation
before filling `confirmed_at` or `confirmation_count`; never copy fixture values
into a production approval.

Keep the artifact outside runtime-writable state and public outputs. Review the
exact movement, account route, independently obtained evidence, total amount and
policy before installation. Installing it can admit funds on the next cycle,
even while trading is halted. Preserve the prior artifact and ledger evidence.
Daily caps constrain purchase plans; the current yearly/lifetime caps count
capital admission, not a separate calendar-year purchase-notional counter.
This change alone does not enable live orders or staking.
