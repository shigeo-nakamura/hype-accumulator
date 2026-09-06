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
(this becomes the ledger's `event_id`) and its `time` in milliseconds (this becomes `confirmed_at`)
— it is the on-chain time, not wall-clock poll time.

Convert `time` to UTC ISO 8601 **without losing its millisecond precision** — truncating to whole
seconds can make `confirmed_at` appear earlier than the runtime's own millisecond-precise
`received_at` for the same movement, which `validate_deposit` rejects outright:

```python
from datetime import datetime, timezone
ms = 1788542589551  # replace with the entry's actual "time" value
confirmed_at = datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") \
    + f"{ms % 1000:03d}Z"
# confirmed_at == "2026-09-04T17:23:09.551Z"
```

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

A different UI is not automatically a different failure domain: several public Hyperliquid
"explorers" are themselves thin clients over `api.hyperliquid.xyz` and would reproduce, not
corroborate, an error or compromise at that API. Per Hyperliquid's own architecture, "API servers
listen to updates from a node" — the design explicitly allows multiple parties to run their own node
and API server reading the L1 network directly, so a genuinely independent explorer is one that does
this, not one that calls `api.hyperliquid.xyz` on the operator's behalf. This research did not
establish which is true for any specific public explorer (for example `hypurrscan.io`'s documented
JSON API returned no usable per-address data for a fresh address, and it is a JavaScript application
that could not be inspected further here) — **do not assume a candidate explorer qualifies without
checking.**

Before relying on an explorer for this step:

1. Confirm the explorer's own documentation, "about"/infrastructure page, or operator statement
   describes it as running its own Hyperliquid node/validator connection, not proxying the official
   API. If this cannot be established, it does not count as an independent source — pick a different
   explorer or escalate; do not fall back to re-checking `api.hyperliquid.xyz` and calling it done.
2. **A human operator opens the confirmed-independent explorer in a real browser**, searches for the
   transaction hash from step 1 (or the execution/parent account), and visually confirms the same
   hash, amount, and route appear. Record who performed this check, when, which explorer was used,
   and the provenance basis from step 1 above.

If no independent-with-confirmed-provenance explorer can be reached or shows the transaction, do not
record `confirmation_count = 2` — treat the transfer as single-source-confirmed only, which does not
meet `min_deposit_confirmations = 2`, and escalate rather than proceeding.

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
`security-policy.toml` — it does not parse or validate `admission-approvals.json`. Hand-reproducing
`AdmissionApprovals::from_json`'s exact closed schema and RFC 3339 grammar in another language is a
moving target — Serde's typed `DateTime<Utc>` parser, `u32`/`u64` bounds, and `deny_unknown_fields`
are implementation details that can drift out of sync with any reimplementation. Invoke the real
parser instead, isolated from live state:

`--dry-run-cycle config.toml security-policy.toml runtime.toml` parses the approvals artifact via
the actual `AdmissionApprovals::from_json` and opens `SignerFreeRuntime` against whatever
`state_directory`/`admission_approvals_path`/etc. the given `runtime.toml` names — it is exactly the
code `hype-accumulator-dryrun.service` runs. Point it at a **scratch copy** of `runtime.toml` so it
never touches production paths:

```sh
set -eu
# Absolute path to the staged artifact under review (the new
# admission-approvals.json you are about to install, not the live one).
STAGED_APPROVALS=/absolute/path/to/staged-admission-approvals.json
SCRATCH=$(mktemp -d)
trap 'rm -rf "$SCRATCH"' EXIT
# The host's system python3 is 3.9 (no stdlib tomllib), and runtime.toml here is
# flat key = "value" / key = integer lines with no nested tables — a line-based
# rewrite avoids needing a TOML parser at all.
python3 - "$SCRATCH" "$STAGED_APPROVALS" <<'PY'
import sys, re, pathlib

# `if not count == 1: raise`, not `assert` — an assertion silently stripped
# under -O/PYTHONOPTIMIZE would leave the substitution unapplied, and the
# dry-run-cycle below would then read/write the REAL production paths
# instead of the scratch directory. This check is what makes the isolation
# a guarantee rather than a hope.
def require_exactly_one(count, key):
    if count != 1:
        raise ValueError(f"expected exactly one {key} line, found {count}")

scratch = pathlib.Path(sys.argv[1])
staged_approvals = pathlib.Path(sys.argv[2])
if not staged_approvals.is_absolute():
    raise ValueError("STAGED_APPROVALS must be an absolute path")
text = pathlib.Path("/etc/hype-accumulator/runtime.toml").read_text()
for key in ("state_directory", "protected_anchor_path", "signal_snapshot_path",
            "status_path", "metrics_path", "cycle_report_path"):
    pattern = re.compile(rf'^{key}\s*=\s*".*"\s*$', re.M)
    replacement = f'{key} = "{scratch / key}"'
    text, count = pattern.subn(replacement, text, count=1)
    require_exactly_one(count, key)
# Point this at the STAGED artifact under review, not the live one — an
# absolute path, so this does not depend on the invoking shell's cwd.
pattern = re.compile(r'^admission_approvals_path\s*=\s*".*"\s*$', re.M)
text, count = pattern.subn(f'admission_approvals_path = "{staged_approvals}"', text, count=1)
require_exactly_one(count, "admission_approvals_path")
(scratch / "runtime.toml").write_text(text)
PY

set -a; source /etc/hype-accumulator/observer.env; set +a  # public account identifiers only, no secret
# The dry-run cycle also best-effort mirrors its local status write to S3
# (src/status_io.rs::mirror_status_to_s3) whenever STATUS_S3_BUCKET and
# STATUS_S3_KEY_PREFIX resolve from the environment — unconditionally unset
# them so this disposable scratch run can never publish anywhere outside
# $SCRATCH, regardless of what the shell or service environment sets.
unset STATUS_S3_BUCKET STATUS_S3_KEY_PREFIX
/opt/hype-accumulator/current/hype-accumulator --dry-run-cycle \
  /etc/hype-accumulator/config.toml /etc/hype-accumulator/security-policy.toml \
  "$SCRATCH/runtime.toml"
```

A zero exit means the real parser accepted the staged `admission-approvals.json`. This is what makes
`--dry-run-cycle` more than a pure offline artifact check: after parsing the approvals file, the same
process also performs a **live, read-only Hyperliquid account observation** (`observer.observe(...)`
in `src/main.rs`, the same call the real dry-run service makes every cycle) and exits nonzero if that
call errors — which can happen from a transient venue or network issue that has nothing to do with
the artifact's validity. Do not treat every nonzero exit as an artifact rejection:

- The admission-approvals.json parse happens **before** that network call. A parse/schema rejection
  therefore fails fast, and its stderr message names the artifact/parsing problem specifically (for
  example mentioning "admission" or a JSON/schema error) rather than the network or the Hyperliquid
  endpoint.
- If stderr instead points at the account observation or the Hyperliquid endpoint (a timeout,
  connection error, or similar), that is unrelated to the artifact — retry the whole check once
  network access is confirmed healthy rather than concluding the artifact is invalid.
- Only treat the artifact as rejected, and skip the install, when the failure is specifically
  attributable to parsing/validating `admission-approvals.json`.

`dry_run=true` in the real `config.toml` and the signer-free runtime's own design (no signing key is
ever loaded by this path) mean nothing here can place, sign, or submit an order regardless of outcome.
With the S3 mirror explicitly disabled above, the only local state this touches is the disposable
`$SCRATCH` directory, which is deleted immediately after (network calls to Hyperliquid's public API
are read-only). Treat a zero exit as "safe to proceed to the halted rollout," not as proof the
*values* (amount, confirmation evidence) are correct — steps 1–4 above are what establish that.

Then install it the same way as any other production config change on this host: a halted rollout
that pauses the HYPE timers, backs up the existing file, runs the config/policy `--install-preflight`
check above alongside the admission-artifact check above, writes atomically, restarts the
observer/dry-run services once, and rolls back automatically if `admitted_usdc` differs from step 6's
full expected-value formula — not merely from `max_admitted_usdc` — so a legitimate cooldown-pending,
returns-reduced, or shared-cap-limited amount is never mistaken for a failure. `dry_run`,
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
  after any prior returns/withdrawals against this event, yearly capacity remaining to *other*
  events, lifetime capacity remaining to *other* events)` — compute the yearly/lifetime remaining
  capacity from admissions **excluding this event's own** (for example from the pre-install ledger
  snapshot, or by summing every other tranche's `admitted_usdc`), not from the post-install ledger.
  Checking post-install remaining capacity is self-referential: if this event's admission correctly
  exhausts the last of a cap, the *post*-install remaining capacity is zero even though the
  correctly admitted amount is positive, and comparing against zero would flag a correct rollout as
  a failure. `event_ceiling` is the approval's `max_admitted_usdc` **if the entry set one** — if it
  was omitted (the schema explicitly permits this, falling back to the automatic per-deposit
  ceiling), `event_ceiling` is instead `config.toml`'s `capital.max_automatically_deployable_usdc`
  (`DepositTranche::admission_limit` in `src/pacing.rs` makes the same substitution). Do not assume
  the un-capped transfer amount is the ceiling. A prior partial return against this same event also
  legitimately reduces admissible capital even with cooldown elapsed and caps otherwise
  unconstrained; check the event's recorded returns before
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
