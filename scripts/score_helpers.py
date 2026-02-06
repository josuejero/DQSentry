"""Shared helpers for `scripts/score.py`."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_MARTS_BASE = REPO_ROOT / "data" / "marts"
CHECK_RESULTS_BASE = DATA_MARTS_BASE / "dq_check_results"
ISSUE_LOG_BASE = DATA_MARTS_BASE / "dq_issue_log"
RUN_HISTORY_PATH = DATA_MARTS_BASE / "dq_run_history" / "run_history.parquet"
SCORE_HISTORY_BASE = DATA_MARTS_BASE / "score_history"
LATEST_REPORT_DIR = REPO_ROOT / "reports" / "latest"
ISSUE_PREVIEW_SIZE = 12
ISSUE_LOG_COLUMNS = [
    "run_id",
    "run_ts",
    "dataset_name",
    "table_name",
    "check_name",
    "dimension",
    "issue_type",
    "severity",
    "affected_rows",
    "affected_pct",
    "sample_bad_rows_json",
    "probable_root_cause",
    "recommended_fix",
    "root_cause_candidates",
]


def parse_iso_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        if value.endswith("Z"):
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                return None
        return None


def read_run_history(run_id: str) -> tuple[str | None, str | None]:
    if not RUN_HISTORY_PATH.exists():
        return None, None
    history = pd.read_parquet(RUN_HISTORY_PATH)
    if history.empty:
        return None, None
    match = history[history["run_id"] == run_id]
    if match.empty:
        return None, None
    record = match.iloc[-1]
    return record.get("run_ts"), record.get("dataset_name")


def format_record(value: Any) -> Any:
    if pd.isna(value):
        return "" if isinstance(value, str) else None
    if isinstance(value, (float, int, str, bool)):
        return value
    return str(value)


def build_check_summary(checks: pd.DataFrame) -> list[dict[str, Any]]:
    columns = [
        "check_id",
        "table_name",
        "dimension",
        "description",
        "status",
        "failure_rate",
        "threshold_warning",
        "threshold_fail",
        "severity",
        "weight",
        "penalty",
        "issue_type",
    ]
    ordered = checks.sort_values(["status", "failure_rate"], ascending=[True, False])
    records: list[dict[str, Any]] = []
    for _, row in ordered.iterrows():
        entry = {key: format_record(row.get(key)) for key in columns}
        entry["failure_rate"] = float(entry["failure_rate"] or 0.0)
        entry["threshold_warning"] = float(entry["threshold_warning"] or 0.0)
        entry["threshold_fail"] = float(entry["threshold_fail"] or 0.0)
        entry["severity"] = int(entry["severity"] or 0)
        entry["weight"] = float(entry["weight"] or 0.0)
        entry["penalty"] = float(entry["penalty"] or 0.0)
        records.append(entry)
    return records


def build_issue_preview(issues: pd.DataFrame) -> list[dict[str, Any]]:
    if issues.empty:
        return []
    ordered = issues.sort_values(
        ["severity", "affected_pct"], ascending=[False, False]
    ).head(ISSUE_PREVIEW_SIZE)
    preview: list[dict[str, Any]] = []
    for _, row in ordered.iterrows():
        preview.append(
            {
                "run_id": row.get("run_id"),
                "table_name": row.get("table_name"),
                "check_name": row.get("check_name"),
                "issue_type": row.get("issue_type"),
                "severity": int(row.get("severity") or 0),
                "affected_pct": float(row.get("affected_pct") or 0.0),
                "probable_root_cause": row.get("probable_root_cause") or "",
                "recommended_fix": row.get("recommended_fix") or "",
            }
        )
    return preview


def append_score_history(record: dict[str, Any]) -> None:
    SCORE_HISTORY_BASE.mkdir(parents=True, exist_ok=True)
    history_path = SCORE_HISTORY_BASE / "score_history.parquet"
    frame = pd.DataFrame([record])
    if history_path.exists():
        existing = pd.read_parquet(history_path)
        frame = pd.concat([existing, frame], ignore_index=True)
    frame.to_parquet(history_path, index=False)
    run_dir = SCORE_HISTORY_BASE / f"run_id={record['run_id']}"
    run_dir.mkdir(parents=True, exist_ok=True)
    frame.iloc[-1:].to_parquet(run_dir / "score_summary.parquet", index=False)


def compute_scores_from_checks(
    checks: pd.DataFrame, baseline: float, minimum: float
) -> tuple[float, dict[str, float]]:
    total_weight = float(checks["weight"].sum())
    total_penalty = float(checks["penalty"].sum())
    normalized = total_penalty / total_weight if total_weight else 0.0
    score = max(minimum, baseline - 100 * normalized)
    dims: dict[str, float] = {}
    grouped = checks.groupby("dimension", as_index=True).agg(
        penalty=("penalty", "sum"), weight=("weight", "sum")
    )
    for dimension, row in grouped.iterrows():
        weight = float(row["weight"])
        penalty = float(row["penalty"])
        if not weight:
            dims[dimension] = float(baseline)
            continue
        dims[dimension] = max(minimum, baseline - 100 * (penalty / weight))
    return score, dims
