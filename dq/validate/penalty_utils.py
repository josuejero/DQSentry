"""Penalty and status helpers for rule evaluations."""

from __future__ import annotations

from dq.validate.config import CheckRule


class PenaltyMixin:
    @staticmethod
    def _compute_penalty(rule: CheckRule, failure_rate: float) -> float:
        return failure_rate * (rule.severity / 5) * rule.weight

    @staticmethod
    def _determine_status(rule: CheckRule, failure_rate: float) -> str:
        if failure_rate >= rule.threshold.fail:
            return "fail"
        if failure_rate >= rule.threshold.warning:
            return "warn"
        return "pass"
