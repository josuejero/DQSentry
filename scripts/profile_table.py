"""Table-level profiling helpers."""

from __future__ import annotations

from typing import Any

import duckdb

from scripts.profile_utils import (
    Record,
    is_numeric_type,
    is_text_type,
    is_temporal_type,
    quote_ident,
    quote_literal,
    stringify_value,
)


def profile_table(
    con: duckdb.DuckDBPyConnection,
    table: str,
    run_id: str,
    dataset_name: str,
    top_n: int,
    column_records: list[Record],
    top_value_records: list[Record],
    type_issue_records: list[Record],
    table_aggregates: dict[str, Record],
) -> None:
    row_count = con.execute(f"SELECT COUNT(*) FROM {quote_ident(table)}").fetchone()[0]
    schema = con.execute(
        f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'main'
          AND table_name = {quote_literal(table)}
        ORDER BY ordinal_position
        """
    ).fetchall()
    logical_table = table.removeprefix("staging_")
    table_entry = table_aggregates.setdefault(
        logical_table,
        {
            "table_name": logical_table,
            "row_count": row_count,
            "column_count": len(schema),
            "max_null_rate": 0.0,
            "nulliest_column": None,
            "max_cardinality": 0,
            "high_cardinality_column": None,
        },
    )

    for column_name, column_type in schema:
        column_expr = quote_ident(column_name)
        null_count = con.execute(
            f"SELECT COUNT(*) FROM {quote_ident(table)} WHERE {column_expr} IS NULL"
        ).fetchone()[0]
        distinct_count = con.execute(
            f"SELECT COUNT(DISTINCT {column_expr}) FROM {quote_ident(table)}"
        ).fetchone()[0]

        min_value: Any = None
        max_value: Any = None
        if is_numeric_type(column_type) or is_temporal_type(column_type):
            min_value, max_value = con.execute(
                f"SELECT MIN({column_expr}), MAX({column_expr}) FROM {quote_ident(table)}"
            ).fetchone()

        null_rate = None
        if row_count:
            null_rate = null_count / row_count

        column_records.append(
            {
                "run_id": run_id,
                "dataset_name": dataset_name,
                "table_name": logical_table,
                "column_name": column_name,
                "column_type": column_type,
                "row_count": row_count,
                "null_count": null_count,
                "null_rate": null_rate,
                "distinct_count": distinct_count,
                "min_value": stringify_value(min_value),
                "max_value": stringify_value(max_value),
            }
        )

        if null_rate is not None and null_rate > table_entry["max_null_rate"]:
            table_entry["max_null_rate"] = null_rate
            table_entry["nulliest_column"] = column_name

        if distinct_count > table_entry["max_cardinality"]:
            table_entry["max_cardinality"] = distinct_count
            table_entry["high_cardinality_column"] = column_name

        if is_text_type(column_type) and row_count:
            numeric_count = con.execute(
                f"""
                SELECT COUNT(*)
                FROM {quote_ident(table)}
                WHERE {column_expr} IS NOT NULL
                  AND REGEXP_MATCHES(TRIM({column_expr}), '^[+-]?\\d+(\\.\\d+)?$')
                """
            ).fetchone()[0]
            non_numeric = row_count - null_count - numeric_count
            if numeric_count and non_numeric:
                type_issue_records.append(
                    {
                        "run_id": run_id,
                        "dataset_name": dataset_name,
                        "table_name": logical_table,
                        "column_name": column_name,
                        "issue": (
                            f"Mixed numeric/text values ({non_numeric} of {row_count - null_count} "
                            "non-numeric rows)."
                        ),
                        "details": {
                            "numeric_values": numeric_count,
                            "non_numeric_values": non_numeric,
                        },
                    }
                )

        top_rows = con.execute(
            f"""
            SELECT {column_expr} AS value, COUNT(*) AS frequency
            FROM {quote_ident(table)}
            GROUP BY {column_expr}
            ORDER BY frequency DESC, value NULLS LAST
            LIMIT {top_n}
            """
        ).fetchall()
        for rank, (value, frequency) in enumerate(top_rows, start=1):
            top_value_records.append(
                {
                    "run_id": run_id,
                    "dataset_name": dataset_name,
                    "table_name": logical_table,
                    "column_name": column_name,
                    "rank": rank,
                    "value": stringify_value(value),
                    "frequency": frequency,
                    "relative_frequency": frequency / row_count if row_count else None,
                }
            )
