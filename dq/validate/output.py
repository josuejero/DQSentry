"""Build output tables and persist results."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd

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
    for result in results:
        if not result.failure_count:
            continue
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
                "probable_root_cause": result.rule.description,
                "recommended_fix": f"Enforce {result.rule.rule_type} for {result.rule.table}",
            }
        )
    return pd.DataFrame(records)


def persist_dataframe(df: pd.DataFrame, run_id: str, folder: str) -> Path:
    dest = DATA_MARTS_BASE / folder / f"run_id={run_id}"
    dest.mkdir(parents=True, exist_ok=True)
    filename = "check_results.parquet" if folder == "dq_check_results" else "issue_log.parquet"
    path = dest / filename
    df.to_parquet(path, index=False)
    return path
