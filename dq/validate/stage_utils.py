"""Helpers for resolving stage tables and timestamps."""

from __future__ import annotations

from scripts.profile_utils import quote_literal


class StageResolverMixin:
    def _resolve_stage_table(self, logical_name: str) -> str:
        table_map = getattr(self, "_table_map", {})
        if logical_name in table_map:
            return table_map[logical_name]
        candidate = f"staging_{logical_name}"
        for table in table_map.values():
            if table == candidate:
                return table
        raise RuntimeError(f"Missing staging table for '{logical_name}'.")

    @staticmethod
    def _format_timestamp_literal(value: str) -> str | None:
        if not value:
            return None
        value = value.strip()
        if value.lower() == "now":
            return "CURRENT_TIMESTAMP"
        return f"TIMESTAMP {quote_literal(value)}"
