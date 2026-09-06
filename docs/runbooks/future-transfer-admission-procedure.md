# Operator procedure: verifying and admitting a new designated-parent transfer

This is the concrete, step-by-step procedure for the "provide a repeatable protected
confirmation/admission producer or explicit operator procedure for future main-to-subaccount
transfers" requirement (bot-strategy#928). It formalizes the manual steps used for the 2026-09-04
transfer's admission. There is no automated evidence producer yet; this is an explicit human
procedure using public, unauthenticated Hyperliquid endpoints. Approving a transfer this way does
not require code changes and does not by itself enable live trading — `dry_run`, `manual_halt`, and
`live_approved` remain independent gates.

Each transfer is a distinct on-chain event with its own hash/`event_id`. A previously approved
parent account does **not** pre-authorize a future transfer from that same account — repeat this
whole procedure for every new transfer, and get a fresh explicit operator decision on the amount
each time.

## Why this evidence model, not "N block confirmations"

HyperCore's consensus (HyperBFT, a HotStuff-family BFT protocol) gives deterministic finality at
commit — there is no PoW-style probabilistic finality that accrues with more elapsed blocks or
polls. The `min_deposit_confirmations` / `DepositAdmissionApproval.confirmation_count` fields should
be read as **the number of independent verification checks that passed**, not blocks elapsed or
repeated polls of the same source (`docs/runbooks/funding-admission.md` already forbids the latter).
Step 1 and step 2 below are the two checks that count toward `confirmation_count`.

## Step 1 — transaction-history lookup (primary evidence)

Query the public, unauthenticated Hyperliquid info endpoint for the execution account's ledger
history, bracketing the expected receipt window:

```
POST https://api.hyperliquid.xyz/info
{"type":"userNonFundingLedgerUpdates","user":"<execution_account>","startTime":<ms>,"endTime":<ms>}
```

Find the entry whose `delta.type == "send"`, `delta.user` equals the exact designated parent account
(case-insensitive), `delta.destination` equals the exact execution account, `delta.token == "USDC"`,
and `delta.amount`/`delta.usdcValue` equal the expected amount. Reject and stop if any of these do
not match exactly — do not admit a similar-looking or partial movement. Record the entry's `hash`
(this becomes the ledger's `event_id`) and its `time` in milliseconds (this becomes `confirmed_at`,
converted to UTC ISO 8601 — it is the on-chain time, not wall-clock poll time).

## Step 2 — balance cross-check (second, structurally different check)

Query a different endpoint over the same account to independently corroborate the balance change:

```
POST https://api.hyperliquid.xyz/info
{"type":"spotClearinghouseState","user":"<execution_account>"}
```

Confirm the reported USDC `total` is consistent with the expected post-transfer balance, and that
`hold == "0.0"` for the newly arrived funds (not already encumbered by an open order). A mismatch
here — even if step 1 looked correct — means stop and investigate before proceeding.

Two independent, structurally different reads that agree (transaction log + balance snapshot) is
`confirmation_count = 2`, consistent with the current `min_deposit_confirmations = 2` policy value.

## Step 3 — operator admission decision

Confirmation evidence only proves the transfer happened; it does not authorize admitting it. Get an
explicit decision from the account owner on:

- The amount to admit for this event (`max_admitted_usdc`, in integer USDC micros — 1 USDC =
  1,000,000 units). It must not exceed the transfer's own amount, and actual admission is further
  bounded by the shared yearly/lifetime caps in `config.toml` (see `docs/runbooks/parent-funding.md`
  and the capital-cap semantics: the yearly cap resets by the deposit's UTC calendar receipt year;
  the cumulative cap is a true lifetime total and never resets).
- Whether any prerequisite from `docs/runbooks/parent-funding.md`'s "Remaining live requirements"
  section has changed since the last transfer.

## Step 4 — install the approval

Add one entry to `admission-approvals.json` (schema in `src/runtime.rs::DepositAdmissionApproval`):

```json
{
  "schema_version": 1,
  "approvals": [
    {
      "max_admitted_usdc": <integer micros, or omit to use the automatic per-deposit ceiling>,
      "event_id": "<hash from step 1>",
      "confirmed_at": "<on-chain time from step 1, UTC ISO 8601>",
      "confirmation_count": 2,
      "approved_at": "<UTC time of this operator decision>"
    }
  ]
}
```

Install it the same way as any other production config change on this host: a halted rollout that
pauses the HYPE timers, backs up the existing file, validates the new config offline (see
`docs/runbooks/release-install.md`'s `--install-preflight` pattern), writes atomically, restarts the
observer/dry-run services once to confirm the expected `admitted_usdc` figure, and rolls back
automatically on any mismatch. `dry_run`/`manual_halt`/`live_approved` are not touched by this step.

## Step 5 — verify, independently

After installing, verify with a command that is not part of the rollout script's own assertions
(a second, independent read):

- `admission-approvals.json` has exactly the new entry, with the expected `event_id` and
  `max_admitted_usdc`.
- The runtime ledger (`/var/lib/hype-accumulator/runtime/<current-route>/runtime-state.json`) shows
  the deposit's `admitted_usdc` matching the expected admitted amount (capped by whatever the
  current yearly/lifetime caps allow — it is normal for this to be less than the full transfer
  amount if caps are the binding constraint; the remainder auto-admits on a later cycle once caps
  allow, with no new approval needed).
- `dry_run` is still `true`, `signed_action_created` is `false`, and no unrelated systemd unit
  changed state.

## What this procedure does not do

It does not automate evidence collection, does not relabel a return/withdrawal as new funding, does
not pre-authorize any future transfer, and does not itself enable live trading. Durable order/fill
finality, cross-workflow HYPE attribution, and staking remain separate, still-open prerequisites
tracked under bot-strategy#901, #929, and #844/#847 respectively.
