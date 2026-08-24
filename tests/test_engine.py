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

    def test_usable_capital_after_final_price_is_reported(self) -> None:
        utc = timezone.utc
        first = datetime(2025, 1, 1, 12, tzinfo=utc)
        after_horizon = datetime(2025, 1, 3, tzinfo=utc)
        late = datetime(2025, 1, 2, 18, tzinfo=utc)
        result = run_backtest(
            [PriceBar(first, 10.0)],
            [CapitalEvent("late", "deposit", 50.0, late, late, late)],
            PointInTimeView([], after_horizon),
            {
                "name": "daily",
                "kind": "fixed",
                "cadence": "daily",
                "horizon": "2025-01-02",
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

        self.assertEqual(result["remaining_cash_usd"], 50.0)
        self.assertEqual([row["event_id"] for row in result["capital_cohorts"]], ["late"])
        self.assertEqual(result["horizon_status"], "infeasible")

    def test_future_horizon_is_reported_in_progress(self) -> None:
        utc = timezone.utc
        first = datetime(2025, 1, 1, 12, tzinfo=utc)
        deposit = CapitalEvent("deposit", "deposit", 100.0, first, first, first)
        result = run_backtest(
            [PriceBar(first, 10.0)],
            [deposit],
            PointInTimeView([], first),
            {
                "name": "daily",
                "kind": "fixed",
                "cadence": "daily",
                "horizon": "2025-01-02",
                "features": [],
            },
            {
                "min_trade_usd": 1.0,
                "max_trade_usd": 100.0,
                "fee_bps": 0.0,
                "half_spread_bps": 0.0,
                "slippage_bps": 0.0,
            },
            first,
        )

        self.assertEqual(result["horizon_status"], "in_progress")
        self.assertFalse(result["horizon_complete"])
        self.assertFalse(result["horizon_infeasible"])

    def test_horizon_day_before_decision_cutoff_is_still_in_progress(self) -> None:
        utc = timezone.utc
        prior = datetime(2025, 1, 1, 16, tzinfo=utc)
        before_cutoff = datetime(2025, 1, 2, 1, tzinfo=utc)
        cutoff = datetime(2025, 1, 2, 16, tzinfo=utc)
        deposit = CapitalEvent("deposit", "deposit", 100.0, prior, prior, prior)
        result = run_backtest(
            [PriceBar(prior, 10.0), PriceBar(cutoff, 10.0)],
            [deposit],
            PointInTimeView([], before_cutoff),
            {
                "name": "daily",
                "kind": "fixed",
                "cadence": "daily",
                "horizon": "2025-01-02",
                "features": [],
            },
            {
                "min_trade_usd": 1.0,
                "max_trade_usd": 25.0,
                "fee_bps": 0.0,
                "half_spread_bps": 0.0,
                "slippage_bps": 0.0,
            },
            before_cutoff,
        )

        self.assertEqual(result["horizon_status"], "in_progress")
        self.assertFalse(result["horizon_complete"])
        self.assertFalse(result["horizon_infeasible"])

    def test_horizon_day_before_capital_admission_is_still_in_progress(self) -> None:
        utc = timezone.utc
        decision = datetime(2025, 1, 2, 16, tzinfo=utc)
        before_admission = datetime(2025, 1, 2, 17, tzinfo=utc)
        admission = datetime(2025, 1, 2, 18, tzinfo=utc)
        result = run_backtest(
            [PriceBar(decision, 10.0)],
            [CapitalEvent("late", "deposit", 50.0, admission, admission, admission)],
            PointInTimeView([], before_admission),
            {
                "name": "daily",
                "kind": "fixed",
                "cadence": "daily",
                "horizon": "2025-01-02",
                "features": [],
            },
            {
                "min_trade_usd": 1.0,
                "max_trade_usd": 100.0,
                "fee_bps": 0.0,
                "half_spread_bps": 0.0,
                "slippage_bps": 0.0,
            },
            before_admission,
        )

        self.assertEqual(result["horizon_status"], "in_progress")
        self.assertFalse(result["horizon_complete"])
        self.assertFalse(result["horizon_infeasible"])
        self.assertEqual(result["capital_cohorts"], [])

    def test_horizon_day_price_outage_does_not_close_before_utc_cutoff(self) -> None:
        utc = timezone.utc
        prior = datetime(2025, 1, 1, 16, tzinfo=utc)
        during_outage = datetime(2025, 1, 2, 17, tzinfo=utc)
        result = run_backtest(
            [PriceBar(prior, 10.0)],
            [CapitalEvent("deposit", "deposit", 100.0, prior, prior, prior)],
            PointInTimeView([], during_outage),
            {
                "name": "daily",
                "kind": "fixed",
                "cadence": "daily",
                "horizon": "2025-01-02",
                "features": [],
            },
            {
                "min_trade_usd": 1.0,
                "max_trade_usd": 25.0,
                "fee_bps": 0.0,
                "half_spread_bps": 0.0,
                "slippage_bps": 0.0,
            },
            during_outage,
        )

        self.assertEqual(result["horizon_status"], "in_progress")
        self.assertFalse(result["horizon_complete"])
        self.assertFalse(result["horizon_infeasible"])

    def test_simultaneous_deposit_precedes_withdrawal_regardless_of_id(self) -> None:
        utc = timezone.utc
        at = datetime(2025, 1, 1, 12, tzinfo=utc)
        events = [
            CapitalEvent("a-withdrawal", "withdrawal", 40.0, at, at, at),
            CapitalEvent("z-deposit", "deposit", 100.0, at, at, at),
        ]
        result = run_backtest(
            [PriceBar(at, 10.0)],
            events,
            PointInTimeView([], at),
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
            at,
        )

        self.assertEqual(result["invested_usd"], 60.0)
        self.assertEqual(result["capital_cohorts"][0]["withdrawn_usd"], 40.0)

    def test_inventory_peak_includes_units_acquired_by_prior_purchases(self) -> None:
        utc = timezone.utc
        first = datetime(2025, 1, 1, 12, tzinfo=utc)
        second = datetime(2025, 1, 2, 12, tzinfo=utc)
        third = datetime(2025, 1, 3, 12, tzinfo=utc)
        deposit = CapitalEvent("deposit", "deposit", 200.0, first, first, first)
        result = run_backtest(
            [PriceBar(first, 100.0), PriceBar(second, 100.0), PriceBar(third, 50.0)],
            [deposit],
            PointInTimeView([], third),
            {
                "name": "daily",
                "kind": "fixed",
                "cadence": "daily",
                "horizon": "2025-01-03",
                "features": [],
            },
            {
                "min_trade_usd": 1.0,
                "max_trade_usd": 100.0,
                "fee_bps": 0.0,
                "half_spread_bps": 0.0,
                "slippage_bps": 0.0,
            },
            third,
        )

        self.assertEqual(result["trade_count"], 3)
        self.assertEqual(result["max_inventory_drawdown"], 0.5)

    def test_missing_future_price_day_does_not_change_current_pace(self) -> None:
        utc = timezone.utc
        first = datetime(2025, 1, 1, 12, tzinfo=utc)
        third = datetime(2025, 1, 3, 12, tzinfo=utc)
        deposit = CapitalEvent("deposit", "deposit", 90.0, first, first, first)
        result = run_backtest(
            [PriceBar(first, 100.0), PriceBar(third, 100.0)],
            [deposit],
            PointInTimeView([], third),
            {
                "name": "daily",
                "kind": "fixed",
                "cadence": "daily",
                "horizon": "2025-01-03",
                "features": [],
            },
            {
                "min_trade_usd": 1.0,
                "max_trade_usd": 100.0,
                "fee_bps": 0.0,
                "half_spread_bps": 0.0,
                "slippage_bps": 0.0,
            },
            third,
        )

        self.assertEqual(result["trades"][0]["spend_usd"], 30.0)
        self.assertEqual(result["trades"][1]["spend_usd"], 60.0)

    def test_missing_valuation_date_price_makes_ending_value_unavailable(self) -> None:
        utc = timezone.utc
        first = datetime(2025, 1, 1, 12, tzinfo=utc)
        horizon = datetime(2025, 1, 2, 12, tzinfo=utc)
        deposit = CapitalEvent("deposit", "deposit", 100.0, first, first, first)
        result = run_backtest(
            [PriceBar(first, 10.0)],
            [deposit],
            PointInTimeView([], horizon),
            {
                "name": "daily",
                "kind": "fixed",
                "cadence": "daily",
                "horizon": "2025-01-02",
                "features": [],
            },
            {
                "min_trade_usd": 1.0,
                "max_trade_usd": 100.0,
                "fee_bps": 0.0,
                "half_spread_bps": 0.0,
                "slippage_bps": 0.0,
            },
            horizon,
        )

        self.assertIsNone(result["ending_inventory_usd"])

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
