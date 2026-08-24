from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from hype_research.contracts import (
    PointInTimeView,
    digest,
    load_capital_events,
    load_dataset,
    timestamp,
)


ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "fixtures"


class PointInTimeContractTests(unittest.TestCase):
    def test_revision_is_not_visible_before_publication(self) -> None:
        _, revisions, _ = load_dataset(FIXTURES / "dataset-manifest.json")
        view = PointInTimeView(revisions, timestamp("2025-12-31T23:59:59Z"))

        before_revision = view.history(
            "btc_etf_flow_usd",
            timestamp("2025-12-21T23:00:00Z"),
            publication_lag_days=1,
        )
        still_before_revision = view.history(
            "btc_etf_flow_usd",
            timestamp("2025-12-23T23:00:00Z"),
            publication_lag_days=1,
        )
        after_revision = view.history(
            "btc_etf_flow_usd",
            timestamp("2025-12-25T23:00:00Z"),
            publication_lag_days=1,
        )

        self.assertEqual(before_revision[-1].observation_date.isoformat(), "2025-12-20")
        self.assertEqual(before_revision[-1].value, -80)
        pre_revision = next(
            row
            for row in still_before_revision
            if row.observation_date.isoformat() == "2025-12-20"
        )
        self.assertEqual(pre_revision.value, -80)
        revised = next(row for row in after_revision if row.observation_date.isoformat() == "2025-12-20")
        self.assertEqual(revised.value, 40)
        self.assertEqual(revised.revision_id, "revised")

    def test_deposit_after_decision_is_first_usable_later(self) -> None:
        events, _ = load_capital_events(FIXTURES / "capital-varied-manifest.json")
        deposit = next(row for row in events if row.event_id == "dep-after")
        decision = timestamp("2025-12-21T16:00:00Z")

        self.assertGreater(deposit.first_usable_at, decision)

    def test_view_as_of_excludes_later_revision(self) -> None:
        _, revisions, _ = load_dataset(FIXTURES / "dataset-manifest.json")
        view = PointInTimeView(revisions, timestamp("2025-12-23T23:59:59Z"))

        history = view.history(
            "btc_etf_flow_usd",
            timestamp("2025-12-31T23:00:00Z"),
            publication_lag_days=1,
        )

        observation = next(
            row for row in history if row.observation_date.isoformat() == "2025-12-20"
        )
        self.assertEqual(observation.value, -80)
        self.assertEqual(observation.revision_id, "initial")

    def test_manifest_checksum_detects_tampering(self) -> None:
        original = json.loads((FIXTURES / "capital-one-manifest.json").read_text(encoding="utf-8"))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "events.csv").write_text(
                (FIXTURES / "capital-one.csv").read_text(encoding="utf-8") + "tampered\n",
                encoding="utf-8",
            )
            original["files"]["events"]["path"] = "events.csv"
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps(original), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "checksum mismatch"):
                load_capital_events(manifest)

    def test_nonfinite_observation_is_rejected(self) -> None:
        original = json.loads((FIXTURES / "dataset-manifest.json").read_text(encoding="utf-8"))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            prices = root / "prices.csv"
            observations = root / "observations.csv"
            prices.write_bytes((FIXTURES / original["files"]["prices"]["path"]).read_bytes())
            observations.write_text(
                (FIXTURES / original["files"]["observations"]["path"])
                .read_text(encoding="utf-8")
                .replace(",100,", ",NaN,", 1),
                encoding="utf-8",
            )
            original["files"]["prices"] = {"path": prices.name, "sha256": digest(prices)}
            original["files"]["observations"] = {
                "path": observations.name,
                "sha256": digest(observations),
            }
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps(original), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "observations must be finite"):
                load_dataset(manifest)

    def test_revision_timestamp_tie_is_rejected(self) -> None:
        original = json.loads((FIXTURES / "dataset-manifest.json").read_text(encoding="utf-8"))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            prices = root / "prices.csv"
            observations = root / "observations.csv"
            prices.write_bytes((FIXTURES / original["files"]["prices"]["path"]).read_bytes())
            source = (FIXTURES / original["files"]["observations"]["path"]).read_text(
                encoding="utf-8"
            )
            first_revision = source.splitlines()[1].rsplit(",", 1)[0]
            observations.write_text(
                source + f"{first_revision},same-time-different-id\n",
                encoding="utf-8",
            )
            original["files"]["prices"] = {"path": prices.name, "sha256": digest(prices)}
            original["files"]["observations"] = {
                "path": observations.name,
                "sha256": digest(observations),
            }
            manifest = root / "manifest.json"
            manifest.write_text(json.dumps(original), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "revision availability timestamps must be unambiguous"):
                load_dataset(manifest)


if __name__ == "__main__":
    unittest.main()
