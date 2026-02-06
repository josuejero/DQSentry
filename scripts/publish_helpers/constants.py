"""Shared constants used by the publish helpers."""

from __future__ import annotations

from pathlib import Path

from dq.validate.paths import DATA_MARTS_BASE

REPO_ROOT = Path(__file__).resolve().parents[1]
REPORTS_BASE = REPO_ROOT / "reports"
TEMPLATE_DIR = REPORTS_BASE / "templates"
LATEST_REPORT_DIR = REPORTS_BASE / "latest"
RUNS_REPORT_DIR = REPORTS_BASE / "runs"
ISSUE_HISTORY_PATH = DATA_MARTS_BASE / "dq_issue_history" / "issue_history.parquet"

SVG_WIDTH = 900
SVG_HEIGHT = 320
SVG_MARGIN = 50
CHART_COLORS = [
    "#1f77b4",
    "#ff7f0e",
    "#2ca02c",
    "#d62728",
    "#9467bd",
    "#8c564b",
    "#e377c2",
    "#7f7f7f",
]
