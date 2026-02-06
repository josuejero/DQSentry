"""Collect metadata about staging tables."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

import duckdb

from scripts.profile_utils import quote_ident


@dataclass
class StageMetadata:
    table_map: Dict[str, str]
    table_columns: Dict[str, List[str]]
    table_counts: Dict[str, int]


def collect_stage_metadata(con: duckdb.DuckDBPyConnection) -> StageMetadata:
    table_map: Dict[str, str] = {}
    table_columns: Dict[str, List[str]] = {}
    table_counts: Dict[str, int] = {}

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

    for (table,) in rows:
        logical = table.removeprefix("staging_")
        table_map[logical] = table
        columns = [
            row[0]
            for row in con.execute(
                f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'main'
                  AND table_name = '{table}'
                ORDER BY ordinal_position
                """
            ).fetchall()
        ]
        table_columns[table] = columns
        table_counts[table] = con.execute(
            f"SELECT COUNT(*) FROM {quote_ident(table)}"
        ).fetchone()[0]

    return StageMetadata(
        table_map=table_map,
        table_columns=table_columns,
        table_counts=table_counts,
    )
