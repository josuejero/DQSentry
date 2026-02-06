"""Data models for the validation runner."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from dq.validate.config import CheckRule


@dataclass
class CheckResult:
    rule: CheckRule
    table: str
    stage_table: str
    failure_count: int
    total_rows: int
    failure_rate: float
    status: str
    penalty: float
    issue_type: str
    samples: list[dict[str, Any]]


@dataclass
class ValidationSummary:
    run_id: str
    run_ts: datetime
    dataset_name: str
    score: float
    subscores: dict[str, float]
    check_results_path: Path
    issue_log_path: Path
    expectation_suite_path: Path
    validation_result_path: Path
    run_history_path: Path
    issue_history_path: Path
    recurrence_summary_path: Path
