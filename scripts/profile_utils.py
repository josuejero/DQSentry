"""Utility helpers for profile scripts."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb

STAGING_BASE = Path(__file__).resolve().parents[1] / "data" / "staging"
NUMERIC_KEYWORDS = ("INT", "FLOAT", "DOUBLE", "REAL", "NUMERIC", "DECIMAL", "MONEY")
TEXT_KEYWORDS = ("CHAR", "CLOB", "TEXT")
TEMPORAL_KEYWORDS = ("TIMESTAMP", "DATETIME", "DATE", "TIME")


def quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def stringify_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def resolve_stage_path(dataset: str, seed: int, override: Path | None) -> Path:
    if override:
        return override
    return STAGING_BASE / dataset / str(seed)


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def collect_tables(con: duckdb.DuckDBPyConnection) -> list[str]:
    rows = con.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
          AND table_type = 'BASE TABLE'
          AND table_name LIKE 'staging_%'
        ORDER BY table_name
        """
    ).fetchall()
    return [row[0] for row in rows]


def is_numeric_type(type_name: str) -> bool:
    return any(keyword in type_name.upper() for keyword in NUMERIC_KEYWORDS)


def is_text_type(type_name: str) -> bool:
    return any(keyword in type_name.upper() for keyword in TEXT_KEYWORDS)


def is_temporal_type(type_name: str) -> bool:
    return any(keyword in type_name.upper() for keyword in TEMPORAL_KEYWORDS)


Record = dict[str, Any]
