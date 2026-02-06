"""DuckDB execution helpers for rule evaluation."""

from __future__ import annotations

from typing import Any

import duckdb

from dq.validate.config import CheckRule
from dq.validate.constants import ISSUE_TYPE_MAP, SAMPLE_LIMIT
from dq.validate.models import CheckResult
from scripts.profile_utils import quote_ident, stringify_value


class QueryExecutorMixin:
    def _execute_condition(
        self, con: duckdb.DuckDBPyConnection, rule: CheckRule, stage_table: str, condition: str
    ) -> CheckResult:
        query = f"SELECT * FROM {quote_ident(stage_table)} WHERE {condition}"
        return self._execute_query_result(con, rule, stage_table, query)

    def _execute_query_result(
        self, con: duckdb.DuckDBPyConnection, rule: CheckRule, stage_table: str, query: str
    ) -> CheckResult:
        failure_count = self._count_matches(con, query)
        samples = self._fetch_samples(con, query, SAMPLE_LIMIT)
        total_rows = getattr(self, "_table_counts", {}).get(stage_table, 0)
        failure_rate = failure_count / total_rows if total_rows else 0.0
        return CheckResult(
            rule=rule,
            table=rule.table,
            stage_table=stage_table,
            failure_count=failure_count,
            total_rows=total_rows,
            failure_rate=failure_rate,
            penalty=self._compute_penalty(rule, failure_rate),
            status=self._determine_status(rule, failure_rate),
            issue_type=ISSUE_TYPE_MAP.get(rule.rule_type, "invalid"),
            samples=samples,
        )

    @staticmethod
    def _count_matches(con: duckdb.DuckDBPyConnection, query: str) -> int:
        return int(con.execute(f"SELECT COUNT(*) FROM ({query}) AS dq_failures").fetchone()[0])

    @staticmethod
    def _fetch_samples(
        con: duckdb.DuckDBPyConnection, query: str, limit: int
    ) -> list[dict[str, Any]]:
        cursor = con.execute(f"SELECT * FROM ({query}) AS dq_samples LIMIT {limit}")
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        return [
            {col: stringify_value(value) for col, value in zip(columns, row)}
            for row in cursor.fetchall()
        ]
