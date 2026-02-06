"""Lightweight anomaly detection on run-level metrics."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from itertools import chain
from pathlib import Path
from statistics import median
from typing import Iterable

import duckdb
import pandas as pd

from scripts.profile_utils import quote_ident
from dq.validate.paths import DATA_MARTS_BASE

METRICS_HISTORY_PATH = DATA_MARTS_BASE / "dq_metrics_history" / "metrics.parquet"
ANOMALIES_BASE = DATA_MARTS_BASE / "dq_anomalies"

METRICS_COLUMNS = [
    "run_id",
    "run_ts",
    "dataset_name",
    "event_volume",
    "completion_count",
    "completion_rate",
    "event_type_counts",
    "event_type_distribution",
]

EVENT_VOLUME_THRESHOLD = 3.0
COMPLETION_RATE_THRESHOLD = 3.0
DISTRIBUTION_SHIFT_THRESHOLD = 0.15


def run_anomaly_detection(
    run_id: str,
    dataset_name: str,
    run_ts: str,
    duckdb_path: Path,
) -> None:
    """Collect metrics for the current run, detect anomalies, and persist the results."""
    metrics = _collect_run_metrics(duckdb_path)
    record = {
        "run_id": run_id,
        "run_ts": run_ts,
        "dataset_name": dataset_name,
        "event_volume": metrics.event_volume,
        "completion_count": metrics.completion_count,
        "completion_rate": metrics.completion_rate,
        "event_type_counts": json.dumps(metrics.event_type_counts, ensure_ascii=False),
        "event_type_distribution": json.dumps(
            metrics.event_type_distribution, ensure_ascii=False
        ),
    }

    history = _load_metrics_history().query("dataset_name == @dataset_name")
    anomalies = _detect_anomalies(history, metrics, run_ts, dataset_name)

    _append_metrics_history(record)
    if anomalies:
        _persist_anomalies(run_id, anomalies)


def _collect_run_metrics(duckdb_path: Path) -> "RunMetrics":
    with duckdb.connect(str(duckdb_path)) as con:
        table = quote_ident("staging_events")
        event_volume = int(
            con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        )
        completion_count = int(
            con.execute(
                f"SELECT COUNT(*) FROM {table} WHERE event_type = 'complete'"
            ).fetchone()[0]
        )
        distribution_rows = con.execute(
            f"SELECT event_type, COUNT(*) FROM {table} GROUP BY event_type"
        ).fetchall()

    event_type_counts: dict[str, int] = {}
    event_type_distribution: dict[str, float] = {}
    if event_volume:
        for event_type, count in distribution_rows:
            key = event_type or ""
            event_type_counts[key] = int(count)
            event_type_distribution[key] = float(count) / event_volume
    completion_rate = (
        float(completion_count) / event_volume if event_volume else 0.0
    )
    return RunMetrics(
        event_volume=event_volume,
        completion_count=completion_count,
        completion_rate=completion_rate,
        event_type_counts=event_type_counts,
        event_type_distribution=event_type_distribution,
    )


def _load_metrics_history() -> pd.DataFrame:
    if not METRICS_HISTORY_PATH.exists():
        return pd.DataFrame(columns=METRICS_COLUMNS)
    return pd.read_parquet(METRICS_HISTORY_PATH)


def _append_metrics_history(record: dict[str, object]) -> None:
    METRICS_HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame([record])
    if METRICS_HISTORY_PATH.exists():
        existing = pd.read_parquet(METRICS_HISTORY_PATH)
        frame = pd.concat([existing, frame], ignore_index=True)
    frame.to_parquet(METRICS_HISTORY_PATH, index=False)


def _detect_anomalies(
    history: pd.DataFrame,
    current: "RunMetrics",
    run_ts: str,
    dataset_name: str,
) -> list[dict[str, object]]:
    if history.empty:
        return []
    records: list[dict[str, object]] = []
    event_volume_series = history["event_volume"].dropna().astype(float)
    completion_rate_series = history["completion_rate"].dropna().astype(float)

    volume_anomaly = _check_value_anomaly(
        "event_volume",
        current.event_volume,
        event_volume_series,
        EVENT_VOLUME_THRESHOLD,
        direction="both",
        run_ts=run_ts,
        dataset_name=dataset_name,
    )
    if volume_anomaly:
        records.append(volume_anomaly)

    completion_anomaly = _check_value_anomaly(
        "completion_rate",
        current.completion_rate,
        completion_rate_series,
        COMPLETION_RATE_THRESHOLD,
        direction="down",
        run_ts=run_ts,
        dataset_name=dataset_name,
    )
    if completion_anomaly:
        records.append(completion_anomaly)

    distribution_anomaly = _check_distribution_shift(
        history, current, run_ts, dataset_name
    )
    if distribution_anomaly:
        records.append(distribution_anomaly)

    return records


def _check_value_anomaly(
    metric: str,
    current_value: float,
    history: pd.Series,
    threshold: float,
    direction: str,
    run_ts: str,
    dataset_name: str,
) -> dict[str, object] | None:
    if history.empty:
        return None
    median_value = history.median()
    mad = (history - median_value).abs().median()
    if mad == 0:
        if current_value == median_value:
            return None
        notes = (
            f"{metric} deviated from median {median_value:.2f} without variation."
        )
        return _build_record(
            metric,
            current_value,
            median_value,
            mad,
            None,
            threshold,
            direction,
            notes,
            details={
                "median": median_value,
                "current": current_value,
                "mad": mad,
            },
            run_ts=run_ts,
            dataset_name=dataset_name,
        )
    z_score = (current_value - median_value) / mad
    if direction == "down" and z_score > 0:
        return None
    if abs(z_score) < threshold:
        return None
    notes = (
        f"{metric} moved {direction} to {current_value:.4f} (median {median_value:.4f}, mad {mad:.4f})."
    )
    return _build_record(
        metric,
        current_value,
        median_value,
        mad,
        z_score,
        threshold,
        direction,
        notes,
        details={
            "median": median_value,
            "mad": mad,
            "z_score": z_score,
        },
        run_ts=run_ts,
        dataset_name=dataset_name,
    )


def _check_distribution_shift(
    history: pd.DataFrame,
    current: "RunMetrics",
    run_ts: str,
    dataset_name: str,
) -> dict[str, object] | None:
    dist_column = history["event_type_distribution"].dropna()
    if dist_column.empty or not current.event_type_distribution:
        return None
    baseline = _aggregate_median_distribution(dist_column)
    if not baseline:
        return None
    current_dist = current.event_type_distribution
    keys = set(baseline) | set(current_dist)
    deltas = {
        key: abs(current_dist.get(key, 0.0) - baseline.get(key, 0.0))
        for key in keys
    }
    key, delta = max(deltas.items(), key=lambda item: item[1])
    if delta < DISTRIBUTION_SHIFT_THRESHOLD:
        return None
    notes = (
        f"Event type '{key or 'missing'}' shifted by {delta:.2%} relative to baseline."
    )
    details = {
        "category": key,
        "baseline": baseline.get(key, 0.0),
        "current": current_dist.get(key, 0.0),
        "delta": delta,
    }
    return _build_record(
        "event_type_distribution",
        delta,
        baseline.get(key, 0.0),
        DISTRIBUTION_SHIFT_THRESHOLD,
        None,
        DISTRIBUTION_SHIFT_THRESHOLD,
        "shift",
        notes,
        details,
        run_ts=run_ts,
        dataset_name=dataset_name,
    )


def _aggregate_median_distribution(series: Iterable[str]) -> dict[str, float]:
    buckets: dict[str, list[float]] = defaultdict(list)
    for raw in series:
        try:
            values = json.loads(raw)
        except json.JSONDecodeError:
            continue
        for key, value in values.items():
            buckets[key].append(float(value))
    return {key: median(vals) for key, vals in buckets.items() if vals}


def _build_record(
    metric: str,
    metric_value: float,
    baseline_value: float,
    baseline_spread: float,
    z_score: float | None,
    threshold: float,
    direction: str,
    notes: str,
    details: dict[str, object],
    run_ts: str,
    dataset_name: str,
) -> dict[str, object]:
    return {
        "metric": metric,
        "metric_value": float(metric_value),
        "baseline_value": float(baseline_value),
        "baseline_spread": float(baseline_spread),
        "z_score": float(z_score) if z_score is not None else None,
        "threshold": float(threshold),
        "direction": direction,
        "notes": notes,
        "details": json.dumps(details, ensure_ascii=False),
        "run_ts": run_ts,
        "dataset_name": dataset_name,
    }


def _persist_anomalies(run_id: str, records: list[dict[str, object]]) -> None:
    payload = []
    for record in records:
        payload.append(
            {
                "run_id": run_id,
                "metric": record["metric"],
                "metric_value": record["metric_value"],
                "baseline_value": record["baseline_value"],
                "baseline_spread": record["baseline_spread"],
                "z_score": record["z_score"],
                "threshold": record["threshold"],
                "direction": record["direction"],
                "notes": record["notes"],
                "details": record["details"],
                "dataset_name": record.get("dataset_name"),
                "run_ts": record.get("run_ts"),
            }
        )
    payload_frame = pd.DataFrame(payload)
    ANOMALIES_BASE.mkdir(parents=True, exist_ok=True)
    run_dir = ANOMALIES_BASE / f"run_id={run_id}"
    run_dir.mkdir(parents=True, exist_ok=True)
    payload_frame.to_parquet(run_dir / "anomalies.parquet", index=False)

    summary_path = ANOMALIES_BASE / "anomalies.parquet"
    if summary_path.exists():
        existing = pd.read_parquet(summary_path)
        payload_frame = pd.concat([existing, payload_frame], ignore_index=True)
    payload_frame.to_parquet(summary_path, index=False)


def _export_dataset(record: dict[str, object]) -> dict[str, object]:
    """Ensure dataset_name is added to anomaly records."""
    return record


@dataclass(frozen=True)
class RunMetrics:
    event_volume: int
    completion_count: int
    completion_rate: float
    event_type_counts: dict[str, int]
    event_type_distribution: dict[str, float]
