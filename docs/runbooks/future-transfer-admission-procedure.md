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
Independence means a genuinely different failure domain, not just a different parameter to the same
API — step 1 and step 3 below are the two checks that count toward `confirmation_count`; step 2 is a
same-source consistency check only.

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

## Step 2 — sender-side ledger lookup (consistency check, not an independent source)

Query the same endpoint again, but for the designated parent account:

```
POST https://api.hyperliquid.xyz/info
{"type":"userNonFundingLedgerUpdates","user":"<parent_account>","startTime":<ms>,"endTime":<ms>}
```

Find the entry with the exact same `hash` as step 1. Confirm its `delta.type == "send"`,
`delta.user` equals the parent account itself, `delta.destination` equals the exact execution
account, and `delta.amount`/`delta.usdcValue` match step 1 exactly. Reject and stop on any mismatch
— this catches a wrong sender/route/amount that a single-sided lookup could miss.

**This does not, by itself, satisfy an independent second confirmation.** Both this lookup and
step 1 hit the same `api.hyperliquid.xyz` operator: if that API returns an incorrect or compromised
record, changing which account is queried does not create a different failure domain — it is one
source asked twice. Neither this step nor a balance snapshot
(`{"type":"spotClearinghouseState","user":"<execution_account>"}`, which `docs/runbooks/parent-funding.md`
already forbids treating as a confirmation) may be counted toward `confirmation_count`. Both remain
worth doing as consistency checks — they catch typos, wrong routes, and wrong amounts cheaply — but
step 3 below is what actually earns the second confirmation.

## Step 3 — independent human verification (the genuine second source)

A public, independently-operated block explorer not run by `api.hyperliquid.xyz`'s operator (for
example `hypurrscan.io`) gives a different codebase, infrastructure, and organization reading the
same underlying committed state — a real second failure domain, unlike another parameter on the same
API. As of this writing its documented JSON API did not return usable per-address transfer data for
a fresh address during this research (`/addressDetails/{address}` returned an empty object;
`/transfers/{fromTimestamp}/{toTimestamp}` requires a JWT this procedure does not have), and it is a
JavaScript application, so it cannot be checked by an unattended script fetch. This step is therefore
manual: **a human operator opens such an explorer in a real browser**, searches for the transaction
hash from step 1 (or the execution/parent account), and visually confirms the same hash, amount, and
route appear. Record who performed this check and when. If no independent explorer can be reached or
shows the transaction, do not record `confirmation_count = 2` — treat the transfer as
single-source-confirmed only, which does not meet `min_deposit_confirmations = 2`, and escalate
rather than proceeding.

This defends against a bug, cache, or compromise specific to the automated fetch path in steps 1–2;
it does not defend against the underlying HyperCore consensus itself producing wrong committed state,
which is a materially different and out-of-scope threat for this procedure.

## Step 4 — operator admission decision

Confirmation evidence only proves the transfer happened; it does not authorize admitting it. Get an
explicit decision from the account owner on:

- The amount to admit for this event (`max_admitted_usdc`, in integer USDC micros — 1 USDC =
  1,000,000 units). It must not exceed the transfer's own amount, and actual admission is further
  bounded by the shared yearly/lifetime caps in `config.toml` (see `docs/runbooks/parent-funding.md`
  and the capital-cap semantics: the yearly cap resets by the deposit's UTC calendar receipt year;
  the cumulative cap is a true lifetime total and never resets).
- Whether any prerequisite from `docs/runbooks/parent-funding.md`'s "Remaining live requirements"
  section has changed since the last transfer.

## Step 5 — install the approval

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

`confirmation_count = 2` here means steps 1 and 3 both passed (the destination-side lookup and the
independent human explorer check) — do not fill this in without step 3 having actually been done.

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


def check(condition, message):
    # Deliberately not `assert`: assertions are stripped entirely when Python
    # runs with -O or PYTHONOPTIMIZE is set, which would silently disable
    # every check below.
    if not condition:
        raise ValueError(message)


def parse_rfc3339_utc(value):
    check(isinstance(value, str), "timestamp must be a JSON string")
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    check(dt.tzinfo is not None, "timestamp must be timezone-aware")
    return dt.astimezone(timezone.utc)


data = json.load(open("staged-admission-approvals.json"))  # raises on malformed JSON
check(set(data.keys()) == {"schema_version", "approvals"}, "unknown top-level field")
check(data["schema_version"] == 1, "unsupported schema_version")
seen = set()
for a in data["approvals"]:
    allowed = {"max_admitted_usdc", "event_id", "confirmed_at", "confirmation_count", "approved_at"}
    check(set(a.keys()) <= allowed, "unknown field in approval entry")
    check(isinstance(a["event_id"], str) and a["event_id"].strip() == a["event_id"] and a["event_id"],
          "invalid event_id")
    check(a["event_id"] not in seen, "duplicate event_id")
    seen.add(a["event_id"])
    check(isinstance(a["confirmation_count"], int) and not isinstance(a["confirmation_count"], bool),
          "confirmation_count must be an integer")
    check(0 < a["confirmation_count"] <= 0xFFFFFFFF, "confirmation_count must fit an unsigned 32-bit "
          "integer (DepositAdmissionApproval.confirmation_count is a u32; the real parser rejects "
          "negative or over-range values that a bare nonzero check would miss)")
    if "max_admitted_usdc" in a and a["max_admitted_usdc"] is not None:
        check(isinstance(a["max_admitted_usdc"], int) and not isinstance(a["max_admitted_usdc"], bool),
              "max_admitted_usdc must be an integer")
        check(0 < a["max_admitted_usdc"] <= 0xFFFFFFFFFFFFFFFF,
              "max_admitted_usdc must fit an unsigned 64-bit integer (UsdcMicros wraps a u64)")
    confirmed_at = parse_rfc3339_utc(a["confirmed_at"])
    approved_at = parse_rfc3339_utc(a["approved_at"])
    check(confirmed_at <= approved_at, "confirmed_at must not be after approved_at")
```

Run this exactly as written (no `-O`/`PYTHONOPTIMIZE`, though the checks above do not rely on that
flag being unset). This narrows, but does not eliminate, the gap with the real parser — it does not
re-derive `UsdcMicros`' exact integer-overflow/bounds behavior, for instance. Treat a pass here as
"safe to proceed to the halted rollout," not as a substitute for the runtime's own validation on
first load.

Then install it the same way as any other production config change on this host: a halted rollout
that pauses the HYPE timers, backs up the existing file, runs the config/policy `--install-preflight`
check above alongside the admission-artifact check above, writes atomically, restarts the
observer/dry-run services once, and rolls back automatically if anything **other than** the expected
cooldown-or-returns-pending state is wrong (see step 6 for what "expected" means). `dry_run`,
`manual_halt`, and `live_approved` are not touched by this step.

## Step 6 — verify, independently

After installing, verify with a command that is not part of the rollout script's own assertions
(a second, independent read):

- `admission-approvals.json` has exactly the new entry, with the expected `event_id` and
  `max_admitted_usdc`.
- The runtime ledger — `runtime-state.json` under the `state_directory` configured in the deployed
  `runtime.toml` (`config/runtime.example.toml` documents this field; do not assume a fixed path,
  it is a distinct directory per funding route on hosts that have migrated routes) — shows the
  deposit's `admitted_usdc`. The expected figure is `min(event_ceiling, remaining event capital
  after any prior returns/withdrawals against this event, remaining yearly capacity, remaining
  lifetime capacity)`, where `event_ceiling` is the approval's `max_admitted_usdc` **if the entry
  set one** — if it was omitted (the schema explicitly permits this, falling back to the automatic
  per-deposit ceiling), `event_ceiling` is instead `config.toml`'s
  `capital.max_automatically_deployable_usdc` (`DepositTranche::admission_limit` in `src/pacing.rs`
  makes the same substitution). Do not assume the un-capped transfer amount is the ceiling. A prior
  partial return against this same event also legitimately reduces admissible capital even with
  cooldown elapsed and caps otherwise unconstrained; check the event's recorded returns before
  treating a shortfall as a failure.
  Separately, if the transfer's `received_at` is still within `pacing.deposit_cooldown_seconds` of
  the current time, `first_usable_at = received_at + deposit_cooldown_seconds` has not passed yet
  and `admitted_usdc = 0` on this first post-install cycle is expected, not a failure — re-check
  after cooldown. Only treat a mismatch as a real problem once cooldown has elapsed and the computed
  expected figure above is still not reached.
- `dry_run` is still `true`, `signed_action_created` is `false`, and no unrelated systemd unit
  changed state.

## What this procedure does not do

It does not automate evidence collection, does not relabel a return/withdrawal as new funding, does
not pre-authorize any future transfer, and does not itself enable live trading. Durable order/fill
finality, cross-workflow HYPE attribution, and staking remain separate, still-open prerequisites
tracked under bot-strategy#901, #929, and #844/#847 respectively.
