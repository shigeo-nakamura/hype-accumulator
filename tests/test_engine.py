from __future__ import annotations

import unittest
from datetime import date, datetime, timezone
from pathlib import Path

from hype_research.cli import run_experiment
from hype_research.contracts import CapitalEvent, PointInTimeView, PriceBar, Revision
from hype_research.engine import run_backtest


ROOT = Path(__file__).resolve().parents[1]
REPORT = run_experiment(ROOT / "fixtures" / "experiment.json")


def capital_path(name: str) -> dict:
    return next(row for row in REPORT["results"] if row["capital_path"] == name)


def policy(path: dict, name: str) -> dict:
    return next(row for row in path["policies"] if row["policy"] == name)


class EngineInvariantTests(unittest.TestCase):
    def test_no_deposit_path_never_trades(self) -> None:
        for result in capital_path("no-deposits")["policies"]:
            self.assertEqual(result["trade_count"], 0)
            self.assertEqual(result["invested_usd"], 0)

    def test_after_decision_deposit_affects_only_future_days(self) -> None:
        daily = policy(
            capital_path("frequent-before-after-multiple-withdrawal-late"),
            "deposit-aware-equal-daily",
        )

        self.assertFalse(
            any(row["decision_at"].startswith("2025-12-21") for row in daily["trades"])
        )
        cohort = next(row for row in daily["capital_cohorts"] if row["event_id"] == "dep-after")
        self.assertGreater(cohort["invested_usd"], 0)
        self.assertTrue(
            any(row["decision_at"].startswith("2025-12-22") for row in daily["trades"])
        )

    def test_execution_caps_and_late_deposit_infeasibility(self) -> None:
        adaptive = policy(
            capital_path("frequent-before-after-multiple-withdrawal-late"),
            "bounded-adaptive",
        )

        self.assertTrue(adaptive["horizon_infeasible"])
        self.assertLessEqual(max(row["spend_usd"] for row in adaptive["trades"]), 25)
        late = next(row for row in adaptive["capital_cohorts"] if row["event_id"] == "dep-late")
        self.assertGreater(late["remaining_usd"], 0)

    def test_adaptive_multiplier_is_bounded(self) -> None:
        for path in REPORT["results"]:
            adaptive = policy(path, "bounded-adaptive")
            for trade in adaptive["trades"]:
                self.assertGreaterEqual(trade["multiplier"], 0.5)
                self.assertLessEqual(trade["multiplier"], 1.5)

    def test_at_most_one_purchase_per_utc_day(self) -> None:
        for path in REPORT["results"]:
            for result in path["policies"]:
                days = [row["decision_at"][:10] for row in result["trades"]]
                self.assertEqual(len(days), len(set(days)))

    def test_capital_conservation(self) -> None:
        for path in REPORT["results"]:
            for result in path["policies"]:
                admitted = sum(row["admitted_usd"] for row in result["capital_cohorts"])
                invested = sum(row["invested_usd"] for row in result["capital_cohorts"])
                withdrawn = sum(row["withdrawn_usd"] for row in result["capital_cohorts"])
                remaining = sum(row["remaining_usd"] for row in result["capital_cohorts"])
                self.assertAlmostEqual(admitted, invested + withdrawn + remaining, places=7)
                self.assertAlmostEqual(result["invested_usd"], invested, places=7)

    def test_duplicate_intraday_bars_trade_once_and_ignore_future_valuation(self) -> None:
        utc = timezone.utc
        bars = [
            PriceBar(datetime(2025, 1, 1, 12, tzinfo=utc), 10.0),
            PriceBar(datetime(2025, 1, 1, 18, tzinfo=utc), 11.0),
            PriceBar(datetime(2025, 1, 2, 12, tzinfo=utc), 100.0),
        ]
        deposit = CapitalEvent(
            "deposit",
            "deposit",
            100.0,
            datetime(2025, 1, 1, tzinfo=utc),
            datetime(2025, 1, 1, tzinfo=utc),
            datetime(2025, 1, 1, tzinfo=utc),
        )
        result = run_backtest(
            bars,
            [deposit],
            PointInTimeView([], datetime(2025, 1, 1, 23, tzinfo=utc)),
            {
                "name": "daily",
                "kind": "fixed",
                "cadence": "daily",
                "horizon": "2025-01-01",
                "features": [],
            },
            {
                "min_trade_usd": 1.0,
                "max_trade_usd": 100.0,
                "fee_bps": 0.0,
                "half_spread_bps": 0.0,
                "slippage_bps": 0.0,
            },
            datetime(2025, 1, 1, 23, tzinfo=utc),
        )

        self.assertEqual(result["trade_count"], 1)
        self.assertEqual(result["skipped_days"]["duplicate_decision_day"], 1)
        self.assertEqual(result["ending_inventory_usd"], 110.0)

    def test_post_horizon_capital_events_are_not_admitted(self) -> None:
        utc = timezone.utc
        first = datetime(2025, 1, 1, 12, tzinfo=utc)
        after_horizon = datetime(2025, 1, 2, 12, tzinfo=utc)
        events = [
            CapitalEvent("initial", "deposit", 100.0, first, first, first),
            CapitalEvent("future-deposit", "deposit", 25.0, after_horizon, after_horizon, after_horizon),
            CapitalEvent("future-withdrawal", "withdrawal", 200.0, after_horizon, after_horizon, after_horizon),
        ]
        result = run_backtest(
            [PriceBar(first, 10.0), PriceBar(after_horizon, 100.0)],
            events,
            PointInTimeView([], after_horizon),
            {
                "name": "daily",
                "kind": "fixed",
                "cadence": "daily",
                "horizon": "2025-01-01",
                "features": [],
            },
            {
                "min_trade_usd": 1.0,
                "max_trade_usd": 100.0,
                "fee_bps": 0.0,
                "half_spread_bps": 0.0,
                "slippage_bps": 0.0,
            },
            after_horizon,
        )

        self.assertEqual([row["event_id"] for row in result["capital_cohorts"]], ["initial"])
        self.assertEqual(result["ending_inventory_usd"], 100.0)

    def test_stale_fixed_fallback_disables_adaptive_multiplier(self) -> None:
        utc = timezone.utc
        revisions = [
            Revision("feature", date(2024, 12, 1), 0.0, datetime(2024, 12, 1, tzinfo=utc), "a"),
            Revision("feature", date(2024, 12, 2), 10.0, datetime(2024, 12, 2, tzinfo=utc), "b"),
        ]
        at = datetime(2025, 1, 1, 12, tzinfo=utc)
        deposit = CapitalEvent("deposit", "deposit", 20.0, at, at, at)
        result = run_backtest(
            [PriceBar(at, 10.0)],
            [deposit],
            PointInTimeView(revisions, at),
            {
                "name": "adaptive",
                "kind": "adaptive",
                "cadence": "daily",
                "horizon": "2025-01-01",
                "features": ["feature"],
                "sensitivity": 0.5,
                "min_multiplier": 0.5,
                "max_multiplier": 1.5,
                "stale_after_days": 3,
                "stale_behavior": "fixed",
            },
            {
                "min_trade_usd": 1.0,
                "max_trade_usd": 100.0,
                "fee_bps": 0.0,
                "half_spread_bps": 0.0,
                "slippage_bps": 0.0,
            },
            at,
        )

        self.assertEqual(result["trades"][0]["multiplier"], 1.0)

    def test_missing_feature_obeys_skip_fallback(self) -> None:
        utc = timezone.utc
        at = datetime(2025, 1, 1, 12, tzinfo=utc)
        deposit = CapitalEvent("deposit", "deposit", 20.0, at, at, at)
        result = run_backtest(
            [PriceBar(at, 10.0)],
            [deposit],
            PointInTimeView([], at),
            {
                "name": "adaptive",
                "kind": "adaptive",
                "cadence": "daily",
                "horizon": "2025-01-01",
                "features": ["missing-feature"],
                "sensitivity": 0.5,
                "min_multiplier": 0.5,
                "max_multiplier": 1.5,
                "stale_after_days": 3,
                "stale_behavior": "skip",
            },
            {
                "min_trade_usd": 1.0,
                "max_trade_usd": 100.0,
                "fee_bps": 0.0,
                "half_spread_bps": 0.0,
                "slippage_bps": 0.0,
            },
            at,
        )

        self.assertEqual(result["trade_count"], 0)
        self.assertEqual(result["skipped_days"]["stale_features"], 1)

    def test_fixture_cannot_produce_economic_go_recommendation(self) -> None:
        self.assertTrue(REPORT["fixture_only"])
        self.assertEqual(REPORT["recommendation"], "no-go")
        self.assertEqual(REPORT["walk_forward"]["status"], "not_economically_evaluated")


if __name__ == "__main__":
    unittest.main()
