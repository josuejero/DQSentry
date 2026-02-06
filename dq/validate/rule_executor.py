"""DuckDB rule evaluator for validation checks."""

from __future__ import annotations

import re
from typing import Any

import duckdb

from dq.validate.config import CheckRule
from dq.validate.constants import ISSUE_TYPE_MAP, SAMPLE_LIMIT
from dq.validate.metadata import StageMetadata
from dq.validate.models import CheckResult
from dq.validate.penalty_utils import PenaltyMixin
from dq.validate.query_utils import QueryExecutorMixin
from dq.validate.stage_utils import StageResolverMixin
from scripts.profile_utils import quote_ident, quote_literal


class RuleEvaluator(
    StageResolverMixin, QueryExecutorMixin, PenaltyMixin
):
    def __init__(self, metadata: StageMetadata) -> None:
        self._table_map = metadata.table_map
        self._table_columns = metadata.table_columns
        self._table_counts = metadata.table_counts

    def evaluate(
        self, con: duckdb.DuckDBPyConnection, rule: CheckRule
    ) -> CheckResult:
        handler = {
            "NULL_PERCENTAGE": self._handle_null_percentage,
            "PATTERN": self._handle_pattern,
            "DATE_RANGE": self._handle_date_range,
            "NON_NEGATIVE_COUNTS": self._handle_non_negative_counts,
            "TIMESTAMP_ORDER": self._handle_timestamp_order,
            "ENUM": self._handle_enum,
            "UNIQUE_MAPPING": self._handle_unique_mapping,
            "DUPLICATE_PERCENTAGE": self._handle_duplicate_percentage,
            "FK": self._handle_foreign_key,
        }.get(rule.rule_type.upper())
        if not handler:
            raise RuntimeError(f"No handler for rule {rule.rule_type}")
        return handler(con, rule)

    def _handle_null_percentage(
        self, con: duckdb.DuckDBPyConnection, rule: CheckRule
    ) -> CheckResult:
        stage_table = self._resolve_stage_table(rule.table)
        column = rule.columns and rule.columns[0]
        condition = f"{quote_ident(column)} IS NULL"
        return self._execute_condition(con, rule, stage_table, condition)

    def _handle_pattern(
        self, con: duckdb.DuckDBPyConnection, rule: CheckRule
    ) -> CheckResult:
        stage_table = self._resolve_stage_table(rule.table)
        column = rule.columns and rule.columns[0]
        pattern = rule.rule_args[0]
        condition = (
            f"{quote_ident(column)} IS NOT NULL AND NOT REGEXP_MATCHES(TRIM({quote_ident(column)}), {quote_literal(pattern)})"
        )
        return self._execute_condition(con, rule, stage_table, condition)

    def _handle_date_range(
        self, con: duckdb.DuckDBPyConnection, rule: CheckRule
    ) -> CheckResult:
        stage_table = self._resolve_stage_table(rule.table)
        column = rule.columns and rule.columns[0]
        start_literal = self._format_timestamp_literal(rule.rule_args[0])
        end_literal = self._format_timestamp_literal(rule.rule_args[1])
        clauses: list[str] = []
        if start_literal:
            clauses.append(f"{quote_ident(column)} < {start_literal}")
        if end_literal:
            clauses.append(f"{quote_ident(column)} > {end_literal}")
        condition = (
            f"{quote_ident(column)} IS NOT NULL AND ({' OR '.join(clauses)})"
        ) if clauses else "0=1"
        return self._execute_condition(con, rule, stage_table, condition)

    def _handle_non_negative_counts(
        self, con: duckdb.DuckDBPyConnection, rule: CheckRule
    ) -> CheckResult:
        matches: list[tuple[str, str]] = []
        pattern = rule.column_regex or ""
        regex = re.compile(pattern, re.IGNORECASE)
        for stage_table, columns in self._table_columns.items():
            for column in columns:
                if regex.search(column):
                    matches.append((stage_table, column))
        failure_count = 0
        total_rows = 0
        samples: list[dict[str, Any]] = []
        for stage_table, column in matches:
            total_rows += self._table_counts.get(stage_table, 0)
            query = (
                f"SELECT *, '{column}' AS failed_column FROM {quote_ident(stage_table)} "
                f"WHERE {quote_ident(column)} < 0"
            )
            batch_count = self._count_matches(con, query)
            failure_count += batch_count
            if len(samples) < SAMPLE_LIMIT:
                samples.extend(
                    self._fetch_samples(con, query, SAMPLE_LIMIT - len(samples))
                )
        failure_rate = failure_count / total_rows if total_rows else 0.0
        return CheckResult(
            rule=rule,
            table=rule.table,
            stage_table="*",
            failure_count=failure_count,
            total_rows=total_rows,
            failure_rate=failure_rate,
            penalty=self._compute_penalty(rule, failure_rate),
            status=self._determine_status(rule, failure_rate),
            issue_type=ISSUE_TYPE_MAP.get(rule.rule_type, "invalid"),
            samples=samples,
        )

    def _handle_timestamp_order(
        self, con: duckdb.DuckDBPyConnection, rule: CheckRule
    ) -> CheckResult:
        stage_table = self._resolve_stage_table(rule.table)
        columns = rule.columns or []
        clauses = [
            f"{quote_ident(nxt)} IS NOT NULL AND ({quote_ident(prev)} IS NULL OR {quote_ident(prev)} > {quote_ident(nxt)})"
            for prev, nxt in zip(columns, columns[1:])
        ]
        condition = " OR ".join(clauses) if clauses else "0=1"
        return self._execute_condition(con, rule, stage_table, condition)

    def _handle_enum(
        self, con: duckdb.DuckDBPyConnection, rule: CheckRule
    ) -> CheckResult:
        stage_table = self._resolve_stage_table(rule.table)
        column = rule.columns and rule.columns[0]
        allowed = ", ".join(quote_literal(val.lower()) for val in rule.rule_args)
        condition = (
            f"{quote_ident(column)} IS NOT NULL AND LOWER(TRIM({quote_ident(column)})) NOT IN ({allowed})"
        )
        return self._execute_condition(con, rule, stage_table, condition)

    def _handle_unique_mapping(
        self, con: duckdb.DuckDBPyConnection, rule: CheckRule
    ) -> CheckResult:
        stage_table = self._resolve_stage_table(rule.table)
        key_col, value_col = rule.rule_args
        failure_query = f"""
        WITH inconsistent AS (
            SELECT {quote_ident(key_col)}
            FROM {quote_ident(stage_table)}
            WHERE {quote_ident(key_col)} IS NOT NULL
            GROUP BY {quote_ident(key_col)}
            HAVING COUNT(DISTINCT {quote_ident(value_col)}) > 1
        )
        SELECT t.*
        FROM {quote_ident(stage_table)} t
        JOIN inconsistent i ON i.{quote_ident(key_col)} = t.{quote_ident(key_col)}
        """
        return self._execute_query_result(con, rule, stage_table, failure_query)

    def _handle_duplicate_percentage(
        self, con: duckdb.DuckDBPyConnection, rule: CheckRule
    ) -> CheckResult:
        stage_table = self._resolve_stage_table(rule.table)
        columns = rule.columns or []
        partition_cols = ", ".join(quote_ident(col) for col in columns)
        failure_query = f"""
        SELECT *
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY {partition_cols} ORDER BY {quote_ident(columns[0])}) AS dq_rank
            FROM {quote_ident(stage_table)}
        ) dq
        WHERE dq.dq_rank > 1
        """
        return self._execute_query_result(con, rule, stage_table, failure_query)

    def _handle_foreign_key(
        self, con: duckdb.DuckDBPyConnection, rule: CheckRule
    ) -> CheckResult:
        stage_table = self._resolve_stage_table(rule.table)
        column = rule.columns and rule.columns[0]
        ref_table_name, ref_column = rule.rule_args[0].split(".")
        ref_stage = self._resolve_stage_table(ref_table_name)
        failure_query = f"""
        SELECT src.*
        FROM {quote_ident(stage_table)} src
        LEFT JOIN {quote_ident(ref_stage)} ref
          ON src.{quote_ident(column)} = ref.{quote_ident(ref_column)}
        WHERE src.{quote_ident(column)} IS NOT NULL
          AND ref.{quote_ident(ref_column)} IS NULL
        """
        return self._execute_query_result(con, rule, stage_table, failure_query)
