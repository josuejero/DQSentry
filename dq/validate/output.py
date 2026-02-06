"""Build output tables and persist results."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd

from dq.validate.metadata import StageMetadata
from dq.validate.models import CheckResult
from dq.validate.paths import DATA_MARTS_BASE


def build_check_results(
    results: Iterable[CheckResult], run_id: str, dataset_name: str
) -> pd.DataFrame:
    records: list[dict[str, object]] = []
    for result in results:
        records.append(
            {
                "run_id": run_id,
                "dataset_name": dataset_name,
                "table_name": result.table,
                "stage_table": result.stage_table,
                "check_id": result.rule.id,
                "dimension": result.rule.dimension,
                "description": result.rule.description,
                "rule_type": result.rule.rule_type,
                "columns": result.rule.columns or [],
                "column_regex": result.rule.column_regex,
                "severity": result.rule.severity,
                "weight": result.rule.weight,
                "threshold_warning": result.rule.threshold.warning,
                "threshold_fail": result.rule.threshold.fail,
                "failure_rate": result.failure_rate,
                "failure_count": result.failure_count,
                "total_rows": result.total_rows,
                "status": result.status,
                "penalty": result.penalty,
            }
        )
    return pd.DataFrame(records)


def build_issue_log(
    results: Iterable[CheckResult], run_id: str, dataset_name: str, run_ts: datetime
) -> pd.DataFrame:
    records: list[dict[str, object]] = []
    columns = [
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
    for result in results:
        if not result.failure_count:
            continue
        root_candidates = [
            {
                "probable_cause": rc.probable_cause,
                "recommended_fix": rc.recommended_fix,
            }
            for rc in result.rule.root_causes
        ]
        probable_root_cause = root_candidates[0]["probable_cause"] if root_candidates else result.rule.description
        recommended_fix = root_candidates[0]["recommended_fix"] if root_candidates else f"Enforce {result.rule.rule_type} for {result.rule.table}"
        records.append(
            {
                "run_id": run_id,
                "run_ts": run_ts.isoformat(),
                "dataset_name": dataset_name,
                "table_name": result.table,
                "check_name": result.rule.id,
                "dimension": result.rule.dimension,
                "issue_type": result.issue_type,
                "severity": result.rule.severity,
                "affected_rows": result.failure_count,
                "affected_pct": result.failure_rate,
                "sample_bad_rows_json": json.dumps(result.samples, ensure_ascii=False),
                "probable_root_cause": probable_root_cause,
                "recommended_fix": recommended_fix,
                "root_cause_candidates": json.dumps(root_candidates, ensure_ascii=False),
            }
        )
    return pd.DataFrame(records, columns=columns)


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
