"""Recurrence metrics helpers for validation output."""

from __future__ import annotations

import pandas as pd

from dq.validate.paths import DATA_MARTS_BASE


def compute_recurrence_metrics(limit: int = 10) -> pd.DataFrame:
    history_path = DATA_MARTS_BASE / "dq_issue_history" / "issue_history.parquet"
    if not history_path.exists():
        return _empty_recurrence_df()
    history = pd.read_parquet(history_path)
    if history.empty:
        return _empty_recurrence_df()
    working = history.copy()
    working["run_ts"] = pd.to_datetime(working["run_ts"])
    grouped = (
        working.groupby(["check_name", "table_name", "issue_type"], as_index=False)
        .agg(
            occurrences=("run_id", "count"),
            median_affected_pct=("affected_pct", "median"),
            last_seen=("run_ts", "max"),
            probable_root_cause=("probable_root_cause", "last"),
            recommended_fix=("recommended_fix", "last"),
        )
        .sort_values(["occurrences", "last_seen"], ascending=[False, False])
        .head(limit)
    )
    grouped["last_seen"] = grouped["last_seen"].apply(
        lambda ts: ts.isoformat() if pd.notna(ts) else ""
    )
    columns = [
        "check_name",
        "table_name",
        "issue_type",
        "occurrences",
        "median_affected_pct",
        "last_seen",
        "probable_root_cause",
        "recommended_fix",
    ]
    return grouped[columns]


def _empty_recurrence_df() -> pd.DataFrame:
    columns = [
        "check_name",
        "table_name",
        "issue_type",
        "occurrences",
        "median_affected_pct",
        "last_seen",
        "probable_root_cause",
        "recommended_fix",
    ]
    return pd.DataFrame(columns=columns)
