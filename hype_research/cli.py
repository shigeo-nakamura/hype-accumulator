from __future__ import annotations

import argparse
import json
from copy import deepcopy
from datetime import datetime
from math import isfinite
from pathlib import Path

from .contracts import PointInTimeView, load_capital_events, load_dataset, resolve_manifest, timestamp
from .engine import run_backtest


def require_finite_number(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not isfinite(value):
        raise ValueError(f"{label} must be finite")
    return float(value)


def require_valid_execution(execution: dict) -> None:
    minimum = require_finite_number(execution["min_trade_usd"], "execution min_trade_usd")
    maximum = require_finite_number(execution["max_trade_usd"], "execution max_trade_usd")
    if minimum <= 0 or maximum <= 0 or minimum > maximum:
        raise ValueError("execution trade limits must be positive with min_trade_usd <= max_trade_usd")
    for field in ("fee_bps", "half_spread_bps", "slippage_bps"):
        if require_finite_number(execution[field], f"execution {field}") < 0:
            raise ValueError(f"execution {field} must not be negative")


def require_supported_policy_values(experiment: dict) -> None:
    for index, policy in enumerate(experiment["policies"]):
        kind = policy.get("kind")
        if kind not in {"fixed", "adaptive"}:
            raise ValueError(f"unsupported policy kind at policies[{index}]: {kind!r}")
        cadence = policy.get("cadence", "daily")
        if cadence not in {"daily", "weekly"}:
            raise ValueError(f"unsupported policy cadence at policies[{index}]: {cadence!r}")
        if kind == "adaptive":
            minimum = require_finite_number(policy["min_multiplier"], f"policies[{index}] min_multiplier")
            maximum = require_finite_number(policy["max_multiplier"], f"policies[{index}] max_multiplier")
            if minimum < 0 or minimum > maximum:
                raise ValueError(
                    f"adaptive multiplier bounds at policies[{index}] must satisfy "
                    "0 <= min_multiplier <= max_multiplier"
                )


def require_snapshot_at_least(child_manifest: dict, experiment_as_of: datetime, label: str) -> None:
    child_as_of = timestamp(child_manifest["as_of"])
    if experiment_as_of > child_as_of:
        raise ValueError(
            f"experiment as_of exceeds {label} snapshot boundary: "
            f"{experiment_as_of.isoformat()} > {child_as_of.isoformat()}"
        )


def run_experiment(path: Path) -> dict:
    experiment = resolve_manifest(path)
    require_supported_policy_values(experiment)
    require_valid_execution(experiment["execution"])
    root = experiment["_root"]
    as_of = timestamp(experiment["as_of"])
    prices, revisions, dataset_manifest = load_dataset((root / experiment["dataset_manifest"]).resolve())
    require_snapshot_at_least(dataset_manifest, as_of, "dataset")
    view = PointInTimeView(revisions, as_of)
    results = []
    for capital_spec in experiment["capital_paths"]:
        events, capital_manifest = load_capital_events((root / capital_spec["manifest"]).resolve())
        require_snapshot_at_least(capital_manifest, as_of, f"capital path {capital_spec['name']}")
        policy_results = []
        for policy in experiment["policies"]:
            policy_results.append(run_backtest(prices, events, view, policy, experiment["execution"], as_of))
        results.append({"capital_path": capital_spec["name"], "policies": policy_results})
    sensitivity = []
    adaptive = next(policy for policy in experiment["policies"] if policy["kind"] == "adaptive")
    events, capital_manifest = load_capital_events((root / experiment["capital_paths"][-1]["manifest"]).resolve())
    require_snapshot_at_least(capital_manifest, as_of, "sensitivity capital path")
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
