from __future__ import annotations

import hashlib
import json
import re
import unittest
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
EVIDENCE_DIR = ROOT / "docs" / "evidence"
HISTORICAL_EVIDENCE = EVIDENCE_DIR / "stage1-offline-2026-08-25.json"
LATEST_EVIDENCE = EVIDENCE_DIR / "stage1-offline-2026-09-01.json"
EVIDENCE_FILES = tuple(sorted(EVIDENCE_DIR.glob("stage1-offline-*.json")))
EXPECTED_GATES = {
    "deterministic_strategy_and_pacing",
    "full_workflow_fault_injection",
    "spot_execution_vectors_and_reconciliation",
    "staking_vectors_validator_and_reconciliation",
    "protected_ledger_restore",
    "fail_closed_configuration",
    "observability_and_durable_suppression",
    "staking_release_and_active_callers",
    "immutable_aggregate_evidence",
}
SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")
FORBIDDEN_KEYS = {
    "wallet_address",
    "ciphertext",
    "host_alias",
    "production_path",
    "signed_payload",
    "secret",
    "secret_value",
}


def load_evidence(path: Path = LATEST_EVIDENCE) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def nested_keys(value: Any) -> set[str]:
    if isinstance(value, dict):
        return set(value) | {key for child in value.values() for key in nested_keys(child)}
    if isinstance(value, list):
        return {key for child in value for key in nested_keys(child)}
    return set()


class Stage1EvidenceTests(unittest.TestCase):
    def assert_cargo_command_is_offline(self, command: str) -> None:
        if command.startswith("cargo ") and not command.startswith("cargo fmt "):
            self.assertIn("--offline", command)

    def test_companion_digest_matches_exact_bytes(self) -> None:
        self.assertEqual(EVIDENCE_FILES, (HISTORICAL_EVIDENCE, LATEST_EVIDENCE))
        for evidence_path in EVIDENCE_FILES:
            with self.subTest(evidence=evidence_path.name):
                expected = hashlib.sha256(evidence_path.read_bytes()).hexdigest()
                digest_path = evidence_path.with_suffix(evidence_path.suffix + ".sha256")
                self.assertEqual(
                    digest_path.read_text(encoding="utf-8"),
                    f"{expected}  {evidence_path.name}\n",
                )

    def test_gate_set_and_aggregate_status_are_closed(self) -> None:
        for evidence_path in EVIDENCE_FILES:
            with self.subTest(evidence=evidence_path.name):
                evidence = load_evidence(evidence_path)
                gates = evidence["required_gates"]
                gate_ids = [gate["id"] for gate in gates]

                self.assertEqual(
                    evidence["schema_version"], "hype-stage1-offline-evidence/v1"
                )
                self.assertEqual(len(gate_ids), len(set(gate_ids)))
                self.assertEqual(set(gate_ids), EXPECTED_GATES)
                self.assertEqual(
                    {gate["status"] for gate in gates} - {"PASS", "FAIL"}, set()
                )

                failed = {gate["id"] for gate in gates if gate["status"] == "FAIL"}
                self.assertEqual(set(evidence["blocking_gate_ids"]), failed)
                self.assertEqual(evidence["scope"]["overall_status"], "PARTIAL")
                self.assertFalse(evidence["scope"]["stage1_accepted"])
                self.assertTrue(
                    all(gate["summary"] and gate["evidence"] for gate in gates)
                )

                workflow_gate = next(
                    gate
                    for gate in gates
                    if gate["id"] == "full_workflow_fault_injection"
                )
                self.assertEqual(workflow_gate["status"], "FAIL")
                self.assertIn(
                    "venue-enforced acceptance deadline", workflow_gate["summary"]
                )
                self.assertIn("cannot prove", workflow_gate["summary"])
                self.assertTrue(
                    any(
                        "cannot change the full-workflow gate to PASS" in item["action"]
                        for item in evidence["next_actions"]
                    )
                )

    def test_source_state_proves_release_delivery_is_incomplete(self) -> None:
        evidence = load_evidence(HISTORICAL_EVIDENCE)
        source = evidence["source_state"]
        release = source["dex_connector"]["release_candidate"]

        for commit in (
            source["hype_accumulator"]["commit"],
            source["dex_connector"]["master_commit"],
            release["version_pr_head_commit"],
            release["version_merge_commit"],
            source["pairtrade"]["commit"],
        ):
            self.assertRegex(commit, SHA_PATTERN)
        self.assertNotEqual(
            release["version_pr_head_commit"], release["version_merge_commit"]
        )
        self.assertEqual(release["version"], "4.7.14")
        self.assertFalse(release["tag_present"])
        self.assertEqual(source["hype_accumulator"]["dex_connector_ref"], "v4.7.12")
        self.assertEqual(source["pairtrade"]["dex_connector_ref"], "v4.7.13")
        self.assertNotEqual(source["hype_accumulator"]["dex_connector_ref"], "v4.7.14")
        self.assertNotEqual(source["pairtrade"]["dex_connector_ref"], "v4.7.14")

    def test_latest_source_state_records_unmerged_release_candidates(self) -> None:
        evidence = load_evidence(LATEST_EVIDENCE)
        source = evidence["source_state"]
        release = source["dex_connector"]["release_candidate"]

        self.assertTrue(release["tag_present"])
        self.assertRegex(release["tag_commit"], SHA_PATTERN)
        self.assertEqual(release["tag_commit"], release["version_pr_head_commit"])
        self.assertEqual(source["hype_accumulator"]["dex_connector_ref"], "v4.7.14")
        self.assertEqual(source["pairtrade"]["dex_connector_ref"], "v4.7.14")
        self.assertNotEqual(source["hype_accumulator"]["ref"], "master")
        self.assertNotEqual(source["pairtrade"]["ref"], "master")

        verified_commits = {run["commit"] for run in evidence["verification_runs"]}
        self.assertIn(source["hype_accumulator"]["commit"], verified_commits)
        self.assertIn(source["pairtrade"]["commit"], verified_commits)

        release_gate = next(
            gate
            for gate in evidence["required_gates"]
            if gate["id"] == "staking_release_and_active_callers"
        )
        self.assertEqual(release_gate["status"], "FAIL")
        self.assertIn("unmerged", release_gate["summary"])
        open_pr_heads = {
            item["repository"]: item["head_commit"]
            for item in release_gate["evidence"]
            if item["kind"] == "github_open_pr"
        }
        self.assertEqual(
            open_pr_heads[source["hype_accumulator"]["repository"]],
            source["hype_accumulator"]["commit"],
        )
        self.assertEqual(
            open_pr_heads[source["pairtrade"]["repository"]],
            source["pairtrade"]["commit"],
        )

    def test_references_and_verification_results_are_well_formed(self) -> None:
        for evidence_path in EVIDENCE_FILES:
            with self.subTest(evidence=evidence_path.name):
                evidence = load_evidence(evidence_path)
                timestamp = evidence["captured_at"]
                self.assertTrue(timestamp.endswith("Z"))
                self.assertEqual(
                    datetime.fromisoformat(timestamp.replace("Z", "+00:00")).tzname(),
                    "UTC",
                )

                for run in evidence["verification_runs"]:
                    self.assertEqual(run["status"], "PASS")
                    self.assertRegex(run["commit"], SHA_PATTERN)
                    self.assertTrue(run["command"])
                    self.assertTrue(run["result"])
                    self.assert_cargo_command_is_offline(run["command"])

                for gate in evidence["required_gates"]:
                    for item in gate["evidence"]:
                        if item["kind"] == "verification_run":
                            self.assert_cargo_command_is_offline(item["command"])
                        if item["kind"] == "github_open_pr":
                            self.assertRegex(item["head_commit"], SHA_PATTERN)
                            self.assertNotIn("merge_commit", item)
                        elif item["kind"] == "github_pr":
                            self.assertRegex(item["head_commit"], SHA_PATTERN)
                            self.assertRegex(item["merge_commit"], SHA_PATTERN)
                        else:
                            continue
                        self.assertEqual(
                            item["url"],
                            f"https://github.com/{item['repository']}/pull/{item['number']}",
                        )

    def test_sensitive_operational_fields_are_absent(self) -> None:
        for evidence_path in EVIDENCE_FILES:
            with self.subTest(evidence=evidence_path.name):
                self.assertEqual(
                    nested_keys(load_evidence(evidence_path)) & FORBIDDEN_KEYS, set()
                )


if __name__ == "__main__":
    unittest.main()
