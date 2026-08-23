from __future__ import annotations

import argparse
import json
from copy import deepcopy
from datetime import datetime
from pathlib import Path

from .contracts import PointInTimeView, load_capital_events, load_dataset, resolve_manifest, timestamp
from .engine import run_backtest


def run_experiment(path: Path) -> dict:
    experiment = resolve_manifest(path)
    root = experiment["_root"]
    as_of = timestamp(experiment["as_of"])
    prices, revisions, dataset_manifest = load_dataset((root / experiment["dataset_manifest"]).resolve())
    view = PointInTimeView(revisions, as_of)
    results = []
    for capital_spec in experiment["capital_paths"]:
        events, _ = load_capital_events((root / capital_spec["manifest"]).resolve())
        policy_results = []
        for policy in experiment["policies"]:
            policy_results.append(run_backtest(prices, events, view, policy, experiment["execution"], as_of))
        results.append({"capital_path": capital_spec["name"], "policies": policy_results})
    sensitivity = []
    adaptive = next(policy for policy in experiment["policies"] if policy["kind"] == "adaptive")
    events, _ = load_capital_events((root / experiment["capital_paths"][-1]["manifest"]).resolve())
    for value in experiment["sensitivity"]["adaptive_sensitivity"]:
        policy = deepcopy(adaptive)
        policy["name"] = f"adaptive-sensitivity-{value}"
        policy["sensitivity"] = value
        result = run_backtest(prices, events, view, policy, experiment["execution"], as_of)
        sensitivity.append({key: result[key] for key in ("policy", "acquisition_vwap_usd", "invested_usd", "remaining_cash_usd", "trade_count")})
    ablations = []
    for features in experiment["ablations"]:
        policy = deepcopy(adaptive)
        policy["name"] = "adaptive-ablation-" + ("none" if not features else "+".join(features))
        policy["features"] = features
        result = run_backtest(prices, events, view, policy, experiment["execution"], as_of)
        ablations.append({key: result[key] for key in ("policy", "acquisition_vwap_usd", "invested_usd", "remaining_cash_usd", "trade_count")})
    return {
        "schema_version": 1, "generated_from_as_of": experiment["as_of"], "dataset_id": dataset_manifest["dataset_id"],
        "fixture_only": dataset_manifest.get("fixture_only", False),
        "recommendation": "no-go", "recommendation_reason": "Synthetic fixtures validate mechanics only; licensed point-in-time market data and walk-forward out-of-sample evidence are required for an economic recommendation.",
        "results": results, "sensitivity": sensitivity, "signal_ablations": ablations,
        "walk_forward": {"status": "not_economically_evaluated", "reason": "The committed dataset is synthetic and too short; the harness exposes fixed parameters without selecting an in-sample optimum."},
    }


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Reproducible HYPE accumulation research harness")
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run")
    run.add_argument("--manifest", type=Path, required=True)
    run.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    report = run_experiment(args.manifest.resolve())
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    else:
        print(rendered, end="")


if __name__ == "__main__":
    main()
