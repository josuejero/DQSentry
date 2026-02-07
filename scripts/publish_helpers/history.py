"""Trend/history helpers for the publish helpers package."""

from __future__ import annotations

from typing import Any

import math
import pandas as pd
from dq.validate.output import compute_recurrence_metrics

from .constants import (
    CHART_COLORS,
    ISSUE_HISTORY_PATH,
    MAX_RUN_LABELS,
    SVG_HEIGHT,
    SVG_MARGIN,
    SVG_WIDTH,
    X_AXIS_LABEL_OFFSET,
    X_AXIS_LABEL_ROTATION,
)


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
    if history.empty or "issue_type" not in history.columns:
        return []
    totals = (
        history["issue_type"]
        .fillna("Unknown")
        .value_counts()
        .rename_axis("issue_type")
        .reset_index(name="count")
    )
    return [
        {"issue_type": row["issue_type"], "count": int(row["count"]) }
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
    working["run_id"] = working["run_id"].fillna("unknown-run")
    working["run_event_id"] = (
        working["run_id"].astype(str) + "|" + working["run_ts_parsed"].astype(str)
    )
    runs = (
        working[["run_event_id", "run_id", "run_ts_parsed"]]
        .drop_duplicates()
        .sort_values("run_ts_parsed")
        .reset_index(drop=True)
    )
    if runs.empty:
        return (
            "<div class=\"chart-empty\">Issue history exists but run timestamps are missing.</div>",
            [],
        )
    counts = (
        working.groupby(["run_event_id", "issue_type"], dropna=False)
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
    run_keys = runs["run_event_id"].tolist()
    count_map = {
        (row["run_event_id"], row["issue_type"]): int(row["count"])
        for _, row in counts.iterrows()
    }

    def y_position(value: float, max_value: float) -> float:
        span = SVG_HEIGHT - SVG_MARGIN * 2
        return SVG_HEIGHT - SVG_MARGIN - (value / max_value) * span if span else SVG_HEIGHT / 2

    max_value = max(
        (
            count_map.get((run_key, issue_type), 0)
            for run_key in run_keys
            for issue_type in top_types
        ),
        default=0,
    )
    max_value = max(max_value, 1)
    if len(run_keys) == 1:
        x_positions = [SVG_WIDTH / 2]
    else:
        step = (SVG_WIDTH - SVG_MARGIN * 2) / (len(run_keys) - 1)
        x_positions = [SVG_MARGIN + idx * step for idx in range(len(run_keys))]

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
            count_map.get((run_key, issue_type), 0) for run_key in run_keys
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

    run_index = runs.set_index("run_event_id")
    include_run_id_label = run_index["run_id"].nunique() > 1

    label_stride = 1
    if len(run_keys) > MAX_RUN_LABELS and MAX_RUN_LABELS > 0:
        label_stride = math.ceil(len(run_keys) / MAX_RUN_LABELS)
    label_y = SVG_HEIGHT - SVG_MARGIN + X_AXIS_LABEL_OFFSET

    x_labels: list[str] = []
    for idx, run_key in enumerate(run_keys):
        if len(run_keys) > MAX_RUN_LABELS and idx % label_stride != 0 and idx != len(run_keys) - 1:
            continue
        row = run_index.loc[run_key]
        label_ts = row["run_ts_parsed"]
        label_run_id = row["run_id"]
        if pd.isna(label_ts):
            formatted_ts = "Unknown run time"
        else:
            formatted_ts = label_ts.strftime("%Y-%m-%d %H:%M")
        if include_run_id_label:
            label = f"{label_run_id} · {formatted_ts}"
        else:
            label = formatted_ts
        x = x_positions[idx]
        if X_AXIS_LABEL_ROTATION < 0:
            text_anchor = "end"
        elif X_AXIS_LABEL_ROTATION > 0:
            text_anchor = "start"
        else:
            text_anchor = "middle"
        rotation_attr = (
            f' transform="rotate({X_AXIS_LABEL_ROTATION} {x:.1f} {label_y:.1f})"'
            if X_AXIS_LABEL_ROTATION != 0
            else ""
        )
        x_labels.append(
            f'<text x="{x:.1f}" y="{label_y:.1f}" text-anchor="{text_anchor}"{rotation_attr}>{label}</text>'
        )

    chart_elements = "\n".join(grid_lines + axis_lines + y_labels + polylines + x_labels)
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
