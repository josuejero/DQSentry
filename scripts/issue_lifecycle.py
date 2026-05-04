#!/usr/bin/env python3
"""Build issue lifecycle table from DQSentry issue history."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_MARTS = REPO_ROOT / "data" / "marts"
HISTORY_PATH = DATA_MARTS / "dq_issue_history" / "issue_history.parquet"
OUTPUT_DIR = DATA_MARTS / "dq_issue_lifecycle"
OUTPUT_PATH = OUTPUT_DIR / "issue_lifecycle.parquet"

LIFECYCLE_COLUMNS = [
    "issue_key",
    "first_seen_run_ts",
    "last_seen_run_ts",
    "first_seen_run_id",
    "last_seen_run_id",
    "occurrence_count",
    "table_name",
    "check_name",
    "dimension",
    "issue_type",
    "severity",
    "max_affected_pct",
    "median_affected_pct",
    "probable_root_cause",
    "recommended_fix",
    "status",
]


def main() -> None:
    parser = argparse.ArgumentParser(description="Build issue lifecycle metrics.")
    parser.add_argument("--history-path", type=Path, default=HISTORY_PATH)
    parser.add_argument("--output-path", type=Path, default=OUTPUT_PATH)
    args = parser.parse_args()

    lifecycle = build_issue_lifecycle(args.history_path)
    args.output_path.parent.mkdir(parents=True, exist_ok=True)
    lifecycle.to_parquet(args.output_path, index=False)
    print(f"Wrote {args.output_path}")


def build_issue_lifecycle(history_path: Path) -> pd.DataFrame:
    if not history_path.exists():
        return _empty_lifecycle()
    history = pd.read_parquet(history_path)
    if history.empty:
        return _empty_lifecycle()
    return _build_lifecycle(history)


def _build_lifecycle(history: pd.DataFrame) -> pd.DataFrame:
    working = history.copy()
    defaults: dict[str, object] = {
        "run_id": "",
        "run_ts": pd.NaT,
        "table_name": "",
        "check_name": "",
        "dimension": "",
        "issue_type": "",
        "severity": 0,
        "affected_pct": 0.0,
        "probable_root_cause": "",
        "recommended_fix": "",
    }
    for column, default in defaults.items():
        if column not in working.columns:
            working[column] = default
    working["run_ts"] = pd.to_datetime(working["run_ts"], errors="coerce", utc=True)
    working = working.sort_values(["run_ts", "run_id"], kind="stable")
    working["issue_key"] = (
        working["table_name"].astype(str)
        + "|"
        + working["check_name"].astype(str)
        + "|"
        + working["issue_type"].astype(str)
    )
    latest_run_ts = working["run_ts"].max()
    if pd.isna(latest_run_ts):
        return _empty_lifecycle()
    grouped = working.groupby("issue_key", as_index=False).agg(
        first_seen_run_ts=("run_ts", "min"),
        last_seen_run_ts=("run_ts", "max"),
        first_seen_run_id=("run_id", "first"),
        last_seen_run_id=("run_id", "last"),
        occurrence_count=("run_id", "count"),
        table_name=("table_name", "last"),
        check_name=("check_name", "last"),
        dimension=("dimension", "last"),
        issue_type=("issue_type", "last"),
        severity=("severity", "max"),
        max_affected_pct=("affected_pct", "max"),
        median_affected_pct=("affected_pct", "median"),
        probable_root_cause=("probable_root_cause", "last"),
        recommended_fix=("recommended_fix", "last"),
    )
    grouped["status"] = grouped["last_seen_run_ts"].apply(
        lambda ts: "open" if ts == latest_run_ts else "not_seen_in_latest_run"
    )
    grouped["first_seen_run_ts"] = grouped["first_seen_run_ts"].apply(_format_ts)
    grouped["last_seen_run_ts"] = grouped["last_seen_run_ts"].apply(_format_ts)
    grouped["status_order"] = grouped["status"].map({"open": 0}).fillna(1)
    grouped = grouped.sort_values(
        ["status_order", "severity", "occurrence_count"],
        ascending=[True, False, False],
    )
    return grouped[LIFECYCLE_COLUMNS].reset_index(drop=True)


def _format_ts(value: object) -> str:
    if pd.isna(value):
        return ""
    ts = pd.Timestamp(value)
    return ts.isoformat()


def _empty_lifecycle() -> pd.DataFrame:
    return pd.DataFrame(columns=LIFECYCLE_COLUMNS)


if __name__ == "__main__":
    main()
