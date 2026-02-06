"""Helpers for loading rule metadata."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import yaml


@dataclass(frozen=True)
class Threshold:
    warning: float
    fail: float


@dataclass(frozen=True)
class RootCause:
    probable_cause: str
    recommended_fix: str


@dataclass(frozen=True)
class CheckRule:
    id: str
    table: str
    dimension: str
    description: str
    columns: list[str] | None
    column_regex: str | None
    rule_type: str
    rule_args: list[str]
    severity: int
    weight: float
    threshold: Threshold
    metadata: dict[str, Any]
    root_causes: tuple[RootCause, ...]


def _parse_rule(rule_text: str) -> tuple[str, list[str]]:
    rule_text = rule_text.strip()
    if "(" in rule_text and rule_text.endswith(")"):
        name, arg_text = rule_text.split("(", 1)
        args = [arg.strip() for arg in arg_text[:-1].split(",")]
        return name, [arg for arg in args if arg]
    return rule_text, []


def load_rules(path: Path) -> tuple[list[CheckRule], float, float]:
    raw = yaml.safe_load(path.read_text())
    root_cause_map = load_root_causes()
    checks: dict[str, Iterable[dict[str, Any]]] = raw.get("checks") or {}
    rules: list[CheckRule] = []
    for dimension, entries in checks.items():
        for entry in entries or []:
            rule_type, rule_args = _parse_rule(entry["rule"])
            column = entry.get("column")
            columns = entry.get("columns")
            if not columns and column:
                columns = [column]
            threshold_data = entry.get("threshold") or {}
            threshold = Threshold(
                warning=float(threshold_data.get("warning", 0.0)),
                fail=float(threshold_data.get("fail", 0.0)),
            )
            rules.append(
                CheckRule(
                    id=entry["id"],
                    table=entry["table"],
                    dimension=dimension,
                    description=entry.get("description", ""),
                    columns=columns,
                    column_regex=entry.get("column_regex"),
                    rule_type=rule_type,
                    rule_args=rule_args,
                    severity=int(entry.get("severity", 1)),
                    weight=float(entry.get("weight", 1.0)),
                    threshold=threshold,
                    metadata=entry,
                    root_causes=root_cause_map.get(entry["id"], ()),
                )
            )
    score_cfg: dict[str, Any] = raw.get("score") or {}
    baseline = float(score_cfg.get("baseline", 100.0))
    minimum = float(score_cfg.get("min", 0.0))
    return rules, baseline, minimum


ROOT_CAUSES_PATH = Path(__file__).resolve().parents[2] / "dq" / "config" / "root_causes.yml"


def load_root_causes(path: Path | None = None) -> dict[str, tuple[RootCause, ...]]:
    source = path or ROOT_CAUSES_PATH
    if not source.exists():
        return {}
    raw = yaml.safe_load(source.read_text()) or {}
    entries = raw.get("checks") if isinstance(raw, dict) else raw
    if not isinstance(entries, dict):
        return {}
    root_causes: dict[str, tuple[RootCause, ...]] = {}
    for check_name, payload in entries.items():
        if isinstance(payload, list):
            rows = payload
        elif isinstance(payload, dict):
            rows = [payload]
        else:
            continue
        parsed: list[RootCause] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            cause = row.get("probable_cause")
            fix = row.get("recommended_fix")
            if not cause or not fix:
                continue
            parsed.append(RootCause(probable_cause=str(cause), recommended_fix=str(fix)))
        if parsed:
            root_causes[check_name] = tuple(parsed)
    return root_causes
