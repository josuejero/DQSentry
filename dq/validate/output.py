"""Build output tables and persist results."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Iterable

import pandas as pd

from dq.validate.models import CheckResult

from dq.validate.output_persistence import (
    append_issue_history,
    append_run_history,
    persist_dataframe,
    persist_recurrence_summary,
)
from dq.validate.output_recurrence import compute_recurrence_metrics


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


__all__ = [
    "append_issue_history",
    "append_run_history",
    "build_check_results",
    "build_issue_log",
    "compute_recurrence_metrics",
    "persist_dataframe",
    "persist_recurrence_summary",
]
