"""Shared helpers for `scripts/publish.py`."""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

import pandas as pd
from jinja2 import Environment, FileSystemLoader, select_autoescape

from dq.validate.output import compute_recurrence_metrics
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


def load_score_payload(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"Missing score payload at {path}")
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def read_issue_history() -> pd.DataFrame:
    if not ISSUE_HISTORY_PATH.exists():
        return pd.DataFrame()
    try:
        history = pd.read_parquet(ISSUE_HISTORY_PATH)
        return history
    except Exception as exc:
        print(f"Warning: failed to read issue history ({exc}), trends will be skipped.")
        return pd.DataFrame()


def build_issue_totals(history: pd.DataFrame) -> list[dict[str, Any]]:
    if history.empty:
        return []
    if "issue_type" not in history.columns:
        return []
    totals = (
        history["issue_type"]
        .fillna("Unknown")
        .value_counts()
        .rename_axis("issue_type")
        .reset_index(name="count")
    )
    return [
        {"issue_type": row["issue_type"], "count": int(row["count"])}
        for _, row in totals.iterrows()
    ]


def build_trend_chart(history: pd.DataFrame) -> tuple[str, list[dict[str, str]]]:
    if history.empty:
        return (
            "<div class=\"chart-empty\">No issue history is available yet.</div>",
            [],
        )
    working = history.copy()
    working["issue_type"] = working["issue_type"].fillna("Unknown")
    working["run_ts_parsed"] = pd.to_datetime(working["run_ts"], errors="coerce")
    working["run_ts_parsed"] = working["run_ts_parsed"].fillna(pd.Timestamp("1970-01-01"))
    runs = (
        working[["run_id", "run_ts_parsed"]]
        .drop_duplicates(subset="run_id")
        .sort_values("run_ts_parsed")
    )
    if runs.empty:
        return (
            "<div class=\"chart-empty\">Issue history exists but run timestamps are missing.</div>",
            [],
        )
    counts = (
        working.groupby(["run_id", "issue_type"], dropna=False)
        .size()
        .reset_index(name="count")
    )
    totals = counts.groupby("issue_type")["count"].sum().sort_values(ascending=False)
    if totals.empty:
        return (
            "<div class=\"chart-empty\">Issue history exists but no issue types were detected.</div>",
            [],
        )
    top_types = totals.head(len(CHART_COLORS)).index.tolist()
    run_ids = runs["run_id"].tolist()
    count_map = {
        (row["run_id"], row["issue_type"]): int(row["count"])
        for _, row in counts.iterrows()
    }

    def y_position(value: float, max_value: float) -> float:
        span = SVG_HEIGHT - SVG_MARGIN * 2
        return SVG_HEIGHT - SVG_MARGIN - (value / max_value) * span if span else SVG_HEIGHT / 2

    max_value = max(
        (count_map.get((run_id, issue_type), 0) for run_id in run_ids for issue_type in top_types),
        default=0,
    )
    max_value = max(max_value, 1)
    if len(run_ids) == 1:
        x_positions = [SVG_WIDTH / 2]
    else:
        step = (SVG_WIDTH - SVG_MARGIN * 2) / (len(run_ids) - 1)
        x_positions = [SVG_MARGIN + idx * step for idx in range(len(run_ids))]

    axis_lines = [
        f'<line x1="{SVG_MARGIN}" y1="{SVG_MARGIN}" x2="{SVG_MARGIN}" y2="{SVG_HEIGHT - SVG_MARGIN}" stroke="#333" stroke-width="1.2"/>',
        f'<line x1="{SVG_MARGIN}" y1="{SVG_HEIGHT - SVG_MARGIN}" x2="{SVG_WIDTH - SVG_MARGIN}" y2="{SVG_HEIGHT - SVG_MARGIN}" stroke="#333" stroke-width="1.2"/>',
    ]
    grid_lines: list[str] = []
    y_labels: list[str] = []
    for idx in range(5):
        value = (max_value * idx) / 4
        y = y_position(value, max_value)
        grid_lines.append(
            f'<line x1="{SVG_MARGIN}" y1="{y:.1f}" x2="{SVG_WIDTH - SVG_MARGIN}" y2="{y:.1f}" stroke="#ddd" stroke-dasharray="4,4"/>'
        )
        y_labels.append(
            f'<text x="{SVG_MARGIN - 10}" y="{y + 4:.1f}" text-anchor="end">{int(round(value))}</text>'
        )

    polylines: list[str] = []
    legend: list[dict[str, str]] = []
    for series_idx, issue_type in enumerate(top_types):
        values = [
            count_map.get((run_id, issue_type), 0) for run_id in run_ids
        ]
        color = CHART_COLORS[series_idx % len(CHART_COLORS)]
        legend.append({"label": issue_type, "color": color})
        points = " ".join(
            f"{x:.1f},{y_position(value, max_value):.1f}"
            for x, value in zip(x_positions, values)
        )
        polylines.append(
            f'<polyline fill="none" stroke="{color}" stroke-width="2" points="{points}" />'
        )

    x_labels: list[str] = []
    for idx, run_id in enumerate(run_ids):
        label_ts = runs.loc[runs["run_id"] == run_id, "run_ts_parsed"].iloc[0]
        label = f"{run_id} · {label_ts.strftime('%Y-%m-%d')}"
        x = x_positions[idx]
        x_labels.append(
            f'<text x="{x:.1f}" y="{SVG_HEIGHT - SVG_MARGIN + 20}" text-anchor="middle">{label}</text>'
        )

    chart_elements = "\n".join(
        grid_lines + axis_lines + y_labels + polylines + x_labels
    )
    chart_svg = (
        f'<svg width="{SVG_WIDTH}" height="{SVG_HEIGHT}" viewBox="0 0 {SVG_WIDTH} {SVG_HEIGHT}" '
        'role="img" aria-label="Issue trends by type over recent runs">'
        f"{chart_elements}"
        "</svg>"
    )
    return chart_svg, legend


def mutate_context(score_payload: dict[str, Any]) -> dict[str, Any]:
    chart_history = read_issue_history()
    chart_svg, legend = build_trend_chart(chart_history)
    issue_totals = build_issue_totals(chart_history)
    recurrence = compute_recurrence_metrics()
    return {
        "score_data": score_payload,
        "check_rows": score_payload.get("check_summary", [])[:30],
        "issue_rows": score_payload.get("issue_preview", []),
        "chart_svg": chart_svg,
        "chart_legend": legend,
        "recurring_issues": recurrence.to_dict(orient="records")
        if not recurrence.empty
        else [],
        "issue_totals": issue_totals,
    }


def render_scorecard(context: dict[str, Any]) -> str:
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATE_DIR)),
        autoescape=select_autoescape(["html"]),
    )
    template = env.get_template("scorecard.html.jinja")
    return template.render(context)


def copy_to_run_directory(run_id: str) -> None:
    run_dir = RUNS_REPORT_DIR / f"run_id={run_id}"
    run_dir.mkdir(parents=True, exist_ok=True)
    for artifact in ("index.html", "score.json", "issues.csv"):
        src = LATEST_REPORT_DIR / artifact
        if not src.exists():
            continue
        shutil.copy(src, run_dir / artifact)
