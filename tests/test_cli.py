from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from hype_research.cli import run_experiment


ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "fixtures"


class ExperimentContractTests(unittest.TestCase):
    def test_experiment_cannot_advance_past_dataset_snapshot(self) -> None:
        experiment = json.loads((FIXTURES / "experiment.json").read_text(encoding="utf-8"))
        experiment["as_of"] = "2026-01-01T00:00:00Z"
        experiment["dataset_manifest"] = str(FIXTURES / "dataset-manifest.json")
        for capital_path in experiment["capital_paths"]:
            capital_path["manifest"] = str(FIXTURES / capital_path["manifest"])

        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "experiment.json"
            manifest.write_text(json.dumps(experiment), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "experiment as_of exceeds dataset snapshot boundary"):
                run_experiment(manifest)


if __name__ == "__main__":
    unittest.main()
