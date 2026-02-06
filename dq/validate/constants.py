"""Shared validation constants for checks."""

from __future__ import annotations

ISSUE_TYPE_MAP = {
    "NULL_PERCENTAGE": "missing",
    "PATTERN": "invalid",
    "DATE_RANGE": "invalid",
    "NON_NEGATIVE_COUNTS": "invalid",
    "TIMESTAMP_ORDER": "inconsistency",
    "ENUM": "invalid",
    "UNIQUE_MAPPING": "inconsistency",
    "DUPLICATE_PERCENTAGE": "duplicate",
    "FK": "orphan",
}

SAMPLE_LIMIT = 5
