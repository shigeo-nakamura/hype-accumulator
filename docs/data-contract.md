# Point-in-time data contract

Every run is anchored by an immutable manifest `as_of` timestamp and SHA-256 hashes. All timestamps are UTC. A market observation is usable only when `available_at <= decision_at`; if revisions exist, the latest revision visible at that decision wins. Future revisions never rewrite an earlier decision. A provider-specific publication lag is applied in addition to `available_at` (the ETF-flow feature uses one full observation-day lag).

Capital events require an authoritative, unique `event_id`, `occurred_at`, `confirmed_at`, and `first_usable_at`. Only `first_usable_at <= decision_at` admits a deposit or withdrawal. Thus a deposit confirmed after the UTC decision participates on the next eligible decision. Withdrawals consume admitted, uninvested cohorts FIFO and cannot consume invested or future capital.

## Source registry

| Series | Committed source | Time semantics | Revisions | Outage behavior | License/cost |
|---|---|---|---|---|---|
| HYPE execution price | Synthetic fixture | UTC decision timestamp | Immutable fixture | Missing day is not synthesized | Test fixture, $0 |
| BTC ETF net flow | Synthetic fixture | Observation day; publication in `available_at`; extra 1-day lag | Latest visible revision | Fixed fallback or skip | Test fixture, $0 |
| HYPE trend | Synthetic fixture | Prior UTC close, published 00:05 UTC | Immutable fixture | Same stale rule | Derived fixture, $0 |

The fixture is not evidence about HYPE returns. Before real evaluation, replace each row with a licensed provider, exact publication schedule, revision/archive policy, outage SLA, timezone, and cost. Coinbase premium, MVRV/NUPL, funding, and open interest are intentionally absent until those rules are documented.

## Execution and horizon contract

There is at most one purchase per UTC date. Daily pace is admitted cash divided by remaining eligible decisions; weekly uses Mondays. Every admitted event recomputes pace. Minimum/maximum order, fee, half-spread, and slippage are explicit. Adaptive multiplier bounds apply before the execution cap. Cash left at the horizon is reported as carry-over/infeasible; a late deposit is never forced into an oversized order.
