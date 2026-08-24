from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from hype_research.cli import run_experiment


ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "fixtures"


class ExperimentContractTests(unittest.TestCase):
    @staticmethod
    def make_paths_absolute(experiment: dict) -> None:
        experiment["dataset_manifest"] = str(FIXTURES / "dataset-manifest.json")
        for capital_path in experiment["capital_paths"]:
            capital_path["manifest"] = str(FIXTURES / capital_path["manifest"])

    def test_experiment_rejects_unknown_policy_kind(self) -> None:
        experiment = json.loads((FIXTURES / "experiment.json").read_text(encoding="utf-8"))
        experiment["policies"][0]["kind"] = "adaptve"

        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "experiment.json"
            manifest.write_text(json.dumps(experiment), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "unsupported policy kind"):
                run_experiment(manifest)

    def test_experiment_rejects_unknown_policy_cadence(self) -> None:
        experiment = json.loads((FIXTURES / "experiment.json").read_text(encoding="utf-8"))
        experiment["policies"][0]["cadence"] = "weeky"

        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "experiment.json"
            manifest.write_text(json.dumps(experiment), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "unsupported policy cadence"):
                run_experiment(manifest)

    def test_experiment_rejects_nonpositive_execution_limits(self) -> None:
        experiment = json.loads((FIXTURES / "experiment.json").read_text(encoding="utf-8"))
        experiment["execution"]["min_trade_usd"] = -20
        experiment["execution"]["max_trade_usd"] = -10

        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "experiment.json"
            manifest.write_text(json.dumps(experiment), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "execution trade limits"):
                run_experiment(manifest)

    def test_experiment_rejects_inverted_adaptive_bounds(self) -> None:
        experiment = json.loads((FIXTURES / "experiment.json").read_text(encoding="utf-8"))
        adaptive = next(policy for policy in experiment["policies"] if policy["kind"] == "adaptive")
        adaptive["min_multiplier"] = 2.0
        adaptive["max_multiplier"] = 1.0

        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "experiment.json"
            manifest.write_text(json.dumps(experiment), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "adaptive multiplier bounds"):
                run_experiment(manifest)

    def test_experiment_rejects_nonfinite_adaptive_sensitivities(self) -> None:
        for field in ("policy", "grid"):
            with self.subTest(field=field):
                experiment = json.loads((FIXTURES / "experiment.json").read_text(encoding="utf-8"))
                if field == "policy":
                    adaptive = next(
                        policy for policy in experiment["policies"] if policy["kind"] == "adaptive"
                    )
                    adaptive["sensitivity"] = float("nan")
                else:
                    experiment["sensitivity"]["adaptive_sensitivity"][0] = float("nan")

                with tempfile.TemporaryDirectory() as directory:
                    manifest = Path(directory) / "experiment.json"
                    manifest.write_text(json.dumps(experiment), encoding="utf-8")

                    with self.assertRaisesRegex(ValueError, "must be finite"):
                        run_experiment(manifest)

    def test_experiment_rejects_unknown_stale_behavior(self) -> None:
        experiment = json.loads((FIXTURES / "experiment.json").read_text(encoding="utf-8"))
        adaptive = next(policy for policy in experiment["policies"] if policy["kind"] == "adaptive")
        adaptive["stale_behavior"] = "fiixed"

        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "experiment.json"
            manifest.write_text(json.dumps(experiment), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "unsupported stale behavior"):
                run_experiment(manifest)

    def test_experiment_rejects_negative_staleness_window(self) -> None:
        experiment = json.loads((FIXTURES / "experiment.json").read_text(encoding="utf-8"))
        adaptive = next(policy for policy in experiment["policies"] if policy["kind"] == "adaptive")
        adaptive["stale_after_days"] = -1

        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "experiment.json"
            manifest.write_text(json.dumps(experiment), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "stale_after_days"):
                run_experiment(manifest)

    def test_experiment_rejects_unknown_policy_and_ablation_features(self) -> None:
        for location in ("policy", "ablation"):
            with self.subTest(location=location):
                experiment = json.loads((FIXTURES / "experiment.json").read_text(encoding="utf-8"))
                if location == "policy":
                    adaptive = next(
                        policy for policy in experiment["policies"] if policy["kind"] == "adaptive"
                    )
                    adaptive["features"] = ["hype_trned"]
                else:
                    experiment["ablations"] = [["hype_trned"]]
                self.make_paths_absolute(experiment)

                with tempfile.TemporaryDirectory() as directory:
                    manifest = Path(directory) / "experiment.json"
                    manifest.write_text(json.dumps(experiment), encoding="utf-8")

                    with self.assertRaisesRegex(ValueError, "unknown feature names"):
                        run_experiment(manifest)

    def test_experiment_cannot_advance_past_dataset_snapshot(self) -> None:
        experiment = json.loads((FIXTURES / "experiment.json").read_text(encoding="utf-8"))
        experiment["as_of"] = "2026-01-01T00:00:00Z"
        self.make_paths_absolute(experiment)

        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "experiment.json"
            manifest.write_text(json.dumps(experiment), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "experiment as_of exceeds dataset snapshot boundary"):
                run_experiment(manifest)

    def test_experiment_requires_exactly_one_adaptive_policy(self) -> None:
        experiment = json.loads((FIXTURES / "experiment.json").read_text(encoding="utf-8"))
        adaptive = next(policy for policy in experiment["policies"] if policy["kind"] == "adaptive")
        duplicate = dict(adaptive)
        duplicate["name"] = "second-adaptive"
        experiment["policies"].append(duplicate)

        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "experiment.json"
            manifest.write_text(json.dumps(experiment), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "exactly one adaptive policy"):
                run_experiment(manifest)

    def test_experiment_requires_unique_nonempty_policy_names(self) -> None:
        for name in ("deposit-aware-equal-daily", ""):
            with self.subTest(name=name):
                experiment = json.loads(
                    (FIXTURES / "experiment.json").read_text(encoding="utf-8")
                )
                experiment["policies"][-1]["name"] = name

                with tempfile.TemporaryDirectory() as directory:
                    manifest = Path(directory) / "experiment.json"
                    manifest.write_text(json.dumps(experiment), encoding="utf-8")

                    with self.assertRaisesRegex(ValueError, "policy names must be"):
                        run_experiment(manifest)

    def test_analysis_capital_path_is_named_and_order_independent(self) -> None:
        experiment = json.loads((FIXTURES / "experiment.json").read_text(encoding="utf-8"))
        self.make_paths_absolute(experiment)

        with tempfile.TemporaryDirectory() as directory:
            first_manifest = Path(directory) / "first.json"
            first_manifest.write_text(json.dumps(experiment), encoding="utf-8")
            first = run_experiment(first_manifest)

            experiment["capital_paths"].reverse()
            second_manifest = Path(directory) / "second.json"
            second_manifest.write_text(json.dumps(experiment), encoding="utf-8")
            second = run_experiment(second_manifest)

        self.assertEqual(first["sensitivity"], second["sensitivity"])
        self.assertEqual(first["signal_ablations"], second["signal_ablations"])
        for result in first["sensitivity"] + first["signal_ablations"]:
            self.assertEqual(
                result["capital_path"],
                "frequent-before-after-multiple-withdrawal-late",
            )
            self.assertEqual(result["source_policy"], "bounded-adaptive")

    def test_analysis_capital_path_must_be_declared(self) -> None:
        experiment = json.loads((FIXTURES / "experiment.json").read_text(encoding="utf-8"))
        experiment["analysis_capital_path"] = "missing-path"

        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "experiment.json"
            manifest.write_text(json.dumps(experiment), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "analysis_capital_path is not declared"):
                run_experiment(manifest)


if __name__ == "__main__":
    unittest.main()
