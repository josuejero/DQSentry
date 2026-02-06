"""Profile collector that orchestrates column profiling."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

from scripts.profile_table import profile_table
from scripts.profile_utils import Record, collect_tables


@dataclass
class ProfileConfig:
    dataset_name: str
    seed: int
    stage_path: Path
    duckdb_path: Path
    run_metadata: dict[str, Any]
    run_id: str
    top_n: int


@dataclass
class ProfileResult:
    column_records: list[Record]
    top_value_records: list[Record]
    type_issue_records: list[Record]
    table_summary_records: list[Record]
    table_aggregates: dict[str, Record]


class ProfileCollector:
    def __init__(self, config: ProfileConfig) -> None:
        self.config = config

    def collect(self) -> ProfileResult:
        column_records: list[Record] = []
        top_value_records: list[Record] = []
        type_issue_records: list[Record] = []
        table_aggregates: dict[str, Record] = {}

        with duckdb.connect(str(self.config.duckdb_path)) as con:
            tables = collect_tables(con)
            if not tables:
                raise SystemExit("No staging tables were detected in the DuckDB file.")
            for table in tables:
                profile_table(
                    con,
                    table,
                    self.config.run_id,
                    self.config.dataset_name,
                    self.config.top_n,
                    column_records,
                    top_value_records,
                    type_issue_records,
                    table_aggregates,
                )

        table_summary_records = [
            {
                "run_id": self.config.run_id,
                "dataset_name": self.config.dataset_name,
                "table_name": entry["table_name"],
                "row_count": entry["row_count"],
                "column_count": entry["column_count"],
                "nulliest_column": entry["nulliest_column"],
                "max_null_rate": entry["max_null_rate"],
                "high_cardinality_column": entry["high_cardinality_column"],
                "max_cardinality": entry["max_cardinality"],
            }
            for entry in table_aggregates.values()
        ]
        return ProfileResult(
            column_records=column_records,
            top_value_records=top_value_records,
            type_issue_records=type_issue_records,
            table_summary_records=table_summary_records,
            table_aggregates=table_aggregates,
        )
