"""Persistence helpers for validation output artifacts."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from dq.validate.metadata import StageMetadata
from dq.validate.paths import DATA_MARTS_BASE


def persist_dataframe(df: pd.DataFrame, run_id: str, folder: str) -> Path:
    dest = DATA_MARTS_BASE / folder / f"run_id={run_id}"
    dest.mkdir(parents=True, exist_ok=True)
    filename = "check_results.parquet" if folder == "dq_check_results" else "issue_log.parquet"
    path = dest / filename
    df.to_parquet(path, index=False)
    return path


def append_run_history(
    run_id: str, run_ts: datetime, dataset_name: str, metadata: StageMetadata
) -> Path:
    counts = {
        table.removeprefix("staging_"): metadata.table_counts.get(table, 0)
        for table in metadata.table_counts
    }
    entry = pd.DataFrame(
        [
            {
                "run_id": run_id,
                "run_ts": run_ts.isoformat(),
                "dataset_name": dataset_name,
                "total_rows_by_table": json.dumps(counts, ensure_ascii=False),
            }
        ]
    )
    return _append_history_table(entry, "dq_run_history", "run_history.parquet")


def append_issue_history(issue_log: pd.DataFrame) -> Path:
    return _append_history_table(issue_log, "dq_issue_history", "issue_history.parquet")


def persist_recurrence_summary(df: pd.DataFrame, run_id: str) -> Path:
    dest = DATA_MARTS_BASE / "dq_issue_recurrence" / f"run_id={run_id}"
    dest.mkdir(parents=True, exist_ok=True)
    path = dest / "top_recurring_issues.parquet"
    df.to_parquet(path, index=False)
    return path


def _append_history_table(df: pd.DataFrame, folder: str, filename: str) -> Path:
    dest_dir = DATA_MARTS_BASE / folder
    dest_dir.mkdir(parents=True, exist_ok=True)
    path = dest_dir / filename
    if path.exists():
        existing = pd.read_parquet(path)
        df = pd.concat([existing, df], ignore_index=True)
    df.to_parquet(path, index=False)
    return path
