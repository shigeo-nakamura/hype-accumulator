from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from math import sqrt
from typing import Any

from .contracts import CapitalEvent, PointInTimeView, PriceBar, capital_event_sort_key


@dataclass
class Cohort:
    event_id: str
    admitted_usd: float
    cash_usd: float
    invested_usd: float = 0.0
    withdrawn_usd: float = 0.0


@dataclass
class Ledger:
    cohorts: list[Cohort] = field(default_factory=list)

    @property
    def cash(self) -> float:
        return sum(item.cash_usd for item in self.cohorts)

    def deposit(self, event: CapitalEvent) -> None:
        self.cohorts.append(Cohort(event.event_id, event.amount_usd, event.amount_usd))

    def take(self, amount: float, purpose: str) -> None:
        if amount > self.cash + 1e-8:
            raise ValueError(f"{purpose} exceeds admitted uninvested capital")
        remaining = amount
        for cohort in self.cohorts:
            used = min(cohort.cash_usd, remaining)
            cohort.cash_usd -= used
            if purpose == "purchase":
                cohort.invested_usd += used
            else:
                cohort.withdrawn_usd += used
            remaining -= used
            if remaining <= 1e-8:
                return


def remaining_dates(decision_date: date, horizon: date, cadence: str) -> int:
    if cadence != "weekly":
        return max((horizon - decision_date).days + 1, 1)
    first_monday = decision_date + timedelta(days=(7 - decision_date.weekday()) % 7)
    if first_monday > horizon:
        return 1
    return 1 + (horizon - first_monday).days // 7


def feature_score(view: PointInTimeView, decision_at: datetime, enabled: set[str], stale_after_days: int) -> tuple[float, bool]:
    scores: list[float] = []
    stale = False
    for series in sorted(enabled):
        history = view.history(series, decision_at, publication_lag_days=1 if series == "btc_etf_flow_usd" else 0)
        if len(history) < 2:
            stale = True
            continue
        stale |= (decision_at.date() - history[-1].observation_date).days > stale_after_days
        values = [item.value for item in history[-5:]]
        mean = sum(values) / len(values)
        scale = sqrt(sum((x - mean) ** 2 for x in values) / max(len(values) - 1, 1))
        scores.append(0.0 if scale == 0 else max(-2.0, min(2.0, (values[-1] - mean) / scale)))
    return (sum(scores) / len(scores) if scores else 0.0), stale


def run_backtest(
    bars: list[PriceBar],
    events: list[CapitalEvent],
    view: PointInTimeView,
    policy: dict[str, Any],
    execution: dict[str, Any],
    as_of: datetime,
) -> dict[str, Any]:
    ledger = Ledger()
    pending = sorted(
        (event for event in events if event.first_usable_at <= as_of),
        key=capital_event_sort_key,
    )
    event_index = 0
    units = spend = fees = turnover = 0.0
    peak_value = max_drawdown = 0.0
    trades: list[dict[str, Any]] = []
    skipped: dict[str, int] = {}
    last_trade_date: date | None = None
    final_price = 0.0
    horizon = date.fromisoformat(policy["horizon"])
    cost_rate = (execution["fee_bps"] + execution["half_spread_bps"] + execution["slippage_bps"]) / 10_000
    cadence = policy.get("cadence", "daily")
    for bar in bars:
        if bar.decision_at > as_of or bar.decision_at.date() > horizon:
            break
        final_price = bar.price_usd
        while event_index < len(pending) and pending[event_index].first_usable_at <= bar.decision_at:
            event = pending[event_index]
            if event.kind == "deposit":
                ledger.deposit(event)
            else:
                ledger.take(event.amount_usd, "withdrawal")
            event_index += 1
        inventory_value = units * bar.price_usd
        peak_value = max(peak_value, inventory_value)
        if peak_value:
            max_drawdown = max(max_drawdown, (peak_value - inventory_value) / peak_value)
        if cadence == "weekly" and bar.decision_at.weekday() != 0:
            continue
        if last_trade_date == bar.decision_at.date():
            skipped["duplicate_decision_day"] = skipped.get("duplicate_decision_day", 0) + 1
            continue
        if ledger.cash <= 1e-8:
            skipped["no_cash"] = skipped.get("no_cash", 0) + 1
            continue
        slots = remaining_dates(bar.decision_at.date(), horizon, cadence)
        base = ledger.cash / slots
        multiplier = 1.0
        score, stale = feature_score(view, bar.decision_at, set(policy.get("features", [])), policy.get("stale_after_days", 3))
        if policy["kind"] == "adaptive":
            stale_behavior = policy.get("stale_behavior", "fixed")
            if stale and stale_behavior == "skip":
                skipped["stale_features"] = skipped.get("stale_features", 0) + 1
                continue
            if stale and stale_behavior != "fixed":
                raise ValueError(f"unsupported stale behavior: {stale_behavior}")
            if not stale:
                raw = 1.0 - policy["sensitivity"] * score
                multiplier = max(policy["min_multiplier"], min(policy["max_multiplier"], raw))
        order = min(ledger.cash, base * multiplier, execution["max_trade_usd"])
        if order < execution["min_trade_usd"]:
            skipped["below_minimum"] = skipped.get("below_minimum", 0) + 1
            continue
        ledger.take(order, "purchase")
        acquired = order / (bar.price_usd * (1 + cost_rate))
        units += acquired
        peak_value = max(peak_value, units * bar.price_usd)
        spend += order
        cost = order * cost_rate / (1 + cost_rate)
        fees += cost
        turnover += order
        trades.append({"decision_at": bar.decision_at.isoformat().replace("+00:00", "Z"), "spend_usd": round(order, 8), "price_usd": bar.price_usd, "units": round(acquired, 10), "multiplier": round(multiplier, 6), "feature_score": round(score, 6)})
        last_trade_date = bar.decision_at.date()
    cohort_rows = [{"event_id": c.event_id, "admitted_usd": round(c.admitted_usd, 8), "invested_usd": round(c.invested_usd, 8), "withdrawn_usd": round(c.withdrawn_usd, 8), "remaining_usd": round(c.cash_usd, 8), "utilization": round(c.invested_usd / c.admitted_usd, 8)} for c in ledger.cohorts]
    infeasible = ledger.cash > 1e-8 and any(e.kind == "deposit" for e in events if e.first_usable_at.date() <= horizon)
    return {
        "policy": policy["name"], "trade_count": len(trades), "acquisition_vwap_usd": round(spend / units, 8) if units else None,
        "invested_usd": round(spend, 8), "remaining_cash_usd": round(ledger.cash, 8), "units": round(units, 10),
        "ending_inventory_usd": round(units * final_price, 8), "max_inventory_drawdown": round(max_drawdown, 8),
        "turnover_usd": round(turnover, 8), "cost_usd": round(fees, 8), "horizon_complete": not infeasible,
        "horizon_infeasible": infeasible, "skipped_days": skipped, "capital_cohorts": cohort_rows, "trades": trades,
    }
