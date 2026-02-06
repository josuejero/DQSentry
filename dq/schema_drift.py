"""Detect schema drift between deployed schema and inferred staging tables."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Iterable

import duckdb
import pandas as pd
import yaml

from dq.validate.paths import DATA_MARTS_BASE

SCHEMA_CONFIG_PATH = Path(__file__).resolve().parents[1] / "dq" / "config" / "schema.yml"
SCHEMA_DRIFT_BASE = DATA_MARTS_BASE / "dq_schema_drift"

TYPE_CANONICAL: dict[str, str] = {
    "uuid": "varchar",
    "email": "varchar",
    "enum": "varchar",
    "state_code": "varchar",
    "string": "varchar",
    "timestamp": "timestamp",
    "int": "integer",
    "integer": "integer",
    "float": "float",
    "decimal": "float",
}


def run_schema_drift_detection(
    run_id: str,
    dataset_name: str,
    run_ts: str,
    stage_path: Path,
    duckdb_path: Path,
) -> None:
    """Persist schema drift observations for the current run."""
    expected = _load_expected_schema()
    actual = _collect_inferred_schema(duckdb_path)

    records = _compare(expected, actual)
    if not records:
        return

    _persist_schema_drift(run_id, dataset_name, run_ts, records)


def _load_expected_schema() -> dict[str, dict[str, str]]:
    if not SCHEMA_CONFIG_PATH.exists():
        return {}
    raw = yaml.safe_load(SCHEMA_CONFIG_PATH.read_text()) or {}
    tables = raw.get("tables") or {}
    schema: dict[str, dict[str, str]] = {}
    for table_name, payload in tables.items():
        columns = payload.get("columns") or {}
        schema[table_name] = {
            column_name: str(column_data.get("type", ""))
            if isinstance(column_data, dict)
            else str(column_data)
            for column_name, column_data in columns.items()
        }
    return schema


def _collect_inferred_schema(duckdb_path: Path) -> dict[str, dict[str, str]]:
    with duckdb.connect(str(duckdb_path)) as con:
        rows = con.execute(
            """
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'main'
              AND table_name LIKE 'staging_%'
            ORDER BY table_name, ordinal_position
            """
        ).fetchall()

    schema: dict[str, dict[str, str]] = {}
    for table_name, column_name, data_type in rows:
        logical = table_name.removeprefix("staging_")
        schema.setdefault(logical, {})[column_name] = data_type.lower()
    return schema


def _compare(
    expected: dict[str, dict[str, str]],
    actual: dict[str, dict[str, str]],
) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    expected_tables = set(expected)
    actual_tables = set(actual)

    for table in sorted(expected_tables - actual_tables):
        records.append(
            _build_record(
                table,
                missing_columns=sorted(expected.get(table, {}).keys()),
                new_columns=[],
                type_changes=[],
                notes="Expected staging table was not created.",
            )
        )

    for table in sorted(actual_tables - expected_tables):
        cols = sorted(actual.get(table, {}).keys())
        records.append(
            _build_record(
                table,
                missing_columns=[],
                new_columns=cols,
                type_changes=[],
                notes="Unexpected staging table appeared.",
            )
        )

    for table in sorted(expected_tables & actual_tables):
        expected_cols = expected.get(table, {})
        actual_cols = actual.get(table, {})
        missing = sorted(set(expected_cols) - set(actual_cols))
        new = sorted(set(actual_cols) - set(expected_cols))
        type_changes = []
        for column in sorted(set(expected_cols) & set(actual_cols)):
            expected_type = _normalize_config_type(expected_cols[column])
            actual_type = _normalize_actual_type(actual_cols[column])
            if expected_type and actual_type and expected_type != actual_type:
                type_changes.append(
                    {
                        "column": column,
                        "expected": expected_type,
                        "actual": actual_type,
                    }
                )
        if missing or new or type_changes:
            notes_parts: list[str] = []
            if missing:
                notes_parts.append(f"Missing columns: {', '.join(missing)}")
            if new:
                notes_parts.append(f"New columns: {', '.join(new)}")
            if type_changes:
                diffs = ", ".join(
                    f"{d['column']} ({d['expected']}→{d['actual']})" for d in type_changes
                )
                notes_parts.append(f"Type changes: {diffs}")
            records.append(
                _build_record(
                    table,
                    missing_columns=missing,
                    new_columns=new,
                    type_changes=type_changes,
                    notes="; ".join(notes_parts),
                )
            )
    return records


def _normalize_config_type(value: str | None) -> str | None:
    if not value:
        return None
    key = value.strip().lower()
    return TYPE_CANONICAL.get(key, key)


def _normalize_actual_type(value: str | None) -> str | None:
    if not value:
        return None
    return value.strip().lower()


def _build_record(
    table: str,
    missing_columns: Iterable[str],
    new_columns: Iterable[str],
    type_changes: Iterable[dict[str, str]],
    notes: str,
) -> dict[str, object]:
    return {
        "table_name": table,
        "missing_columns": json.dumps(list(missing_columns), ensure_ascii=False),
        "new_columns": json.dumps(list(new_columns), ensure_ascii=False),
        "type_changes": json.dumps(list(type_changes), ensure_ascii=False),
        "notes": notes,
    }


def _persist_schema_drift(
    run_id: str,
    dataset_name: str,
    run_ts: str,
    records: list[dict[str, object]],
) -> None:
    payload = []
    for record in records:
        payload.append(
            {
                "run_id": run_id,
                "run_ts": run_ts,
                "dataset_name": dataset_name,
                **record,
            }
        )
    frame = pd.DataFrame(payload)
    run_dir = SCHEMA_DRIFT_BASE / f"run_id={run_id}"
    run_dir.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(run_dir / "drift.parquet", index=False)

    summary_path = SCHEMA_DRIFT_BASE / "drift.parquet"
    if summary_path.exists():
        existing = pd.read_parquet(summary_path)
        frame = pd.concat([existing, frame], ignore_index=True)
    frame.to_parquet(summary_path, index=False)
