from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from math import isfinite
from pathlib import Path
from typing import Any


UTC = timezone.utc


def timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise ValueError(f"timestamp must include UTC offset: {value}")
    return parsed.astimezone(UTC)


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def resolve_manifest(path: Path) -> dict[str, Any]:
    manifest = load_json(path)
    if manifest.get("schema_version") != 1:
        raise ValueError(f"unsupported manifest schema: {manifest.get('schema_version')}")
    timestamp(manifest["as_of"])
    manifest["_root"] = path.parent.resolve()
    return manifest


def checked_file(root: Path, spec: dict[str, Any]) -> Path:
    path = (root / spec["path"]).resolve()
    if root not in path.parents:
        raise ValueError(f"path escapes manifest directory: {spec['path']}")
    actual = digest(path)
    if spec["sha256"] != actual:
        raise ValueError(f"checksum mismatch for {path}: expected {spec['sha256']}, got {actual}")
    return path


@dataclass(frozen=True)
class Revision:
    series: str
    observation_date: date
    value: float
    available_at: datetime
    revision_id: str


@dataclass(frozen=True)
class PriceBar:
    decision_at: datetime
    price_usd: float


@dataclass(frozen=True)
class CapitalEvent:
    event_id: str
    kind: str
    amount_usd: float
    occurred_at: datetime
    confirmed_at: datetime
    first_usable_at: datetime


def capital_event_sort_key(event: CapitalEvent) -> tuple[datetime, int, str]:
    """Order simultaneous deposits before withdrawals, then use ID for stability."""
    kind_order = 0 if event.kind == "deposit" else 1
    return event.first_usable_at, kind_order, event.event_id


def load_dataset(manifest_path: Path) -> tuple[list[PriceBar], list[Revision], dict[str, Any]]:
    manifest = resolve_manifest(manifest_path)
    root = manifest["_root"]
    prices_path = checked_file(root, manifest["files"]["prices"])
    observations_path = checked_file(root, manifest["files"]["observations"])
    prices: list[PriceBar] = []
    with prices_path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            prices.append(PriceBar(timestamp(row["decision_at"]), float(row["price_usd"])))
    revisions: list[Revision] = []
    with observations_path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            revisions.append(Revision(row["series"], date.fromisoformat(row["observation_date"]), float(row["value"]), timestamp(row["available_at"]), row["revision_id"]))
    if prices != sorted(prices, key=lambda item: item.decision_at) or len({p.decision_at for p in prices}) != len(prices):
        raise ValueError("prices must be strictly ordered and unique")
    if any(not isfinite(item.price_usd) or item.price_usd <= 0 for item in prices):
        raise ValueError("prices must be positive")
    if any(not isfinite(item.value) for item in revisions):
        raise ValueError("observations must be finite")
    revision_slots = [
        (item.series, item.observation_date, item.available_at)
        for item in revisions
    ]
    if len(set(revision_slots)) != len(revision_slots):
        raise ValueError("revision availability timestamps must be unambiguous")
    return prices, revisions, manifest


def load_capital_events(manifest_path: Path) -> tuple[list[CapitalEvent], dict[str, Any]]:
    manifest = resolve_manifest(manifest_path)
    path = checked_file(manifest["_root"], manifest["files"]["events"])
    events: list[CapitalEvent] = []
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            event = CapitalEvent(row["event_id"], row["kind"], float(row["amount_usd"]), timestamp(row["occurred_at"]), timestamp(row["confirmed_at"]), timestamp(row["first_usable_at"]))
            if event.kind not in {"deposit", "withdrawal"} or not isfinite(event.amount_usd) or event.amount_usd <= 0:
                raise ValueError(f"invalid capital event: {event}")
            if not event.occurred_at <= event.confirmed_at <= event.first_usable_at:
                raise ValueError(f"invalid capital event timing: {event.event_id}")
            events.append(event)
    if any(not event.event_id or event.event_id != event.event_id.strip() for event in events):
        raise ValueError("capital event IDs must be nonempty without surrounding whitespace")
    if len({e.event_id for e in events}) != len(events):
        raise ValueError("capital event IDs must be unique and authoritative")
    deposit_slots = [event.first_usable_at for event in events if event.kind == "deposit"]
    if len(set(deposit_slots)) != len(deposit_slots):
        raise ValueError("deposit first_usable_at timestamps must be unique")
    return sorted(events, key=capital_event_sort_key), manifest


class PointInTimeView:
    """Select the latest revision actually available at a decision timestamp."""

    def __init__(self, revisions: list[Revision], as_of: datetime):
        self.revisions = [item for item in revisions if item.available_at <= as_of]

    def history(self, series: str, decision_at: datetime, publication_lag_days: int = 0) -> list[Revision]:
        latest: dict[date, Revision] = {}
        cutoff = decision_at.date().toordinal() - publication_lag_days
        for item in self.revisions:
            if item.series != series or item.available_at > decision_at or item.observation_date.toordinal() > cutoff:
                continue
            previous = latest.get(item.observation_date)
            if previous is None or (item.available_at, item.revision_id) > (previous.available_at, previous.revision_id):
                latest[item.observation_date] = item
        return [latest[key] for key in sorted(latest)]
