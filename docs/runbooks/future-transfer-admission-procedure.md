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

## Step 2 — sender-side ledger lookup (second, genuinely independent check)

A current balance is an aggregate figure: it cannot be attributed to one specific event, and
`docs/runbooks/parent-funding.md` already forbids treating a balance change (or a repeated poll) as
a confirmation. The second confirmation instead comes from checking the **other side of the same
on-chain transaction** — query the same endpoint, but for the designated parent account:

```
POST https://api.hyperliquid.xyz/info
{"type":"userNonFundingLedgerUpdates","user":"<parent_account>","startTime":<ms>,"endTime":<ms>}
```

Find the entry with the exact same `hash` as step 1. Confirm its `delta.type == "send"`,
`delta.user` equals the parent account itself, `delta.destination` equals the exact execution
account, and `delta.amount`/`delta.usdcValue` match step 1 exactly. Reject and stop on any
mismatch. This is a genuinely independent corroboration — the same committed transaction observed
from both the source and destination ledgers — not a repeated poll of the same query or an
unrelated aggregate metric.

Two independent, structurally matching reads of the same event (destination-side ledger lookup +
source-side ledger lookup) is `confirmation_count = 2`, consistent with the current
`min_deposit_confirmations = 2` policy value.

As an additional sanity check (not counted toward `confirmation_count`), querying
`{"type":"spotClearinghouseState","user":"<execution_account>"}` and confirming the current USDC
`total`/`hold` are consistent with the expected post-transfer balance is still worth doing — it just
does not by itself establish or add to confirmation evidence for this specific event.

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

`--install-preflight` (`docs/runbooks/release-install.md`) only validates `config.toml` and
`security-policy.toml` — it does not parse or validate `admission-approvals.json`. There is no
dedicated CLI subcommand that validates this artifact alone without opening the live runtime state
(`--dry-run-cycle` does parse it via the real `AdmissionApprovals::from_json`, but it also opens
`SignerFreeRuntime` against the configured, real `state_directory`, so it is not side-effect-free to
run ad hoc before a backup exists). Before installing, run an offline check that reproduces the
artifact's actual closed schema and typed-timestamp validation precisely — not a loose string
comparison, which would wrongly accept unknown fields, non-integer numeric fields, or two identical
non-date strings for `confirmed_at`/`approved_at`:

```python
import json
from datetime import datetime, timezone

def parse_rfc3339_utc(value):
    if not isinstance(value, str):
        raise ValueError("timestamp must be a JSON string")
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    return dt.astimezone(timezone.utc)

data = json.load(open("staged-admission-approvals.json"))  # raises on malformed JSON
assert set(data.keys()) == {"schema_version", "approvals"}, "unknown top-level field"
assert data["schema_version"] == 1
seen = set()
for a in data["approvals"]:
    allowed = {"max_admitted_usdc", "event_id", "confirmed_at", "confirmation_count", "approved_at"}
    assert set(a.keys()) <= allowed, "unknown field in approval entry"
    assert isinstance(a["event_id"], str) and a["event_id"].strip() == a["event_id"] and a["event_id"]
    assert a["event_id"] not in seen
    seen.add(a["event_id"])
    assert isinstance(a["confirmation_count"], int) and not isinstance(a["confirmation_count"], bool)
    assert a["confirmation_count"] != 0
    if "max_admitted_usdc" in a and a["max_admitted_usdc"] is not None:
        assert isinstance(a["max_admitted_usdc"], int) and not isinstance(a["max_admitted_usdc"], bool)
        assert a["max_admitted_usdc"] > 0
    confirmed_at = parse_rfc3339_utc(a["confirmed_at"])
    approved_at = parse_rfc3339_utc(a["approved_at"])
    assert confirmed_at <= approved_at
```

This narrows, but does not eliminate, the gap with the real parser (it does not re-derive
`UsdcMicros`' exact integer-overflow/bounds behavior, for instance). Treat a pass here as "safe to
proceed to the halted rollout," not as a substitute for the runtime's own validation on first load.

Then install it the same way as any other production config change on this host: a halted rollout
that pauses the HYPE timers, backs up the existing file, runs the config/policy `--install-preflight`
check above alongside the admission-artifact check above, writes atomically, restarts the
observer/dry-run services once, and rolls back automatically if anything **other than** the expected
cooldown-pending state is wrong (see step 5 for what "expected" means when the transfer is recent).
`dry_run`/`manual_halt`/`live_approved` are not touched by this step.

## Step 5 — verify, independently

After installing, verify with a command that is not part of the rollout script's own assertions
(a second, independent read):

- `admission-approvals.json` has exactly the new entry, with the expected `event_id` and
  `max_admitted_usdc`.
- The runtime ledger — `runtime-state.json` under the `state_directory` configured in the deployed
  `runtime.toml` (`config/runtime.example.toml` documents this field; do not assume a fixed path,
  it is a distinct directory per funding route on hosts that have migrated routes) — shows the
  deposit's `admitted_usdc` matching the expected admitted amount, **unless** the transfer's
  `received_at` is still within `pacing.deposit_cooldown_seconds` of the current time. Admission is
  correctly gated on `first_usable_at = received_at + deposit_cooldown_seconds`, so a fresh transfer
  checked soon after arrival will legitimately show `admitted_usdc = 0` (or a partial amount) on the
  very first post-install cycle — this is expected, not a failure, and does not mean the approval or
  caps are wrong. Re-check after the cooldown elapses. Only treat a mismatch as a real problem if it
  persists once both the cooldown has elapsed and the yearly/lifetime caps have enough remaining
  capacity for the expected amount.
- `dry_run` is still `true`, `signed_action_created` is `false`, and no unrelated systemd unit
  changed state.

## What this procedure does not do

It does not automate evidence collection, does not relabel a return/withdrawal as new funding, does
not pre-authorize any future transfer, and does not itself enable live trading. Durable order/fill
finality, cross-workflow HYPE attribution, and staking remain separate, still-open prerequisites
tracked under bot-strategy#901, #929, and #844/#847 respectively.
