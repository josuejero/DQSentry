"""Score calculations for validation runs."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Dict, Tuple

from dq.validate.models import CheckResult


def calculate_scores(
    results: Iterable[CheckResult], baseline: float, minimum: float
) -> Tuple[float, Dict[str, float]]:
    total_weight = sum(result.rule.weight for result in results)
    penalties: Dict[str, float] = {}
    weights: Dict[str, float] = {}
    total_penalty = 0.0
    for result in results:
        total_penalty += result.penalty
        dim = result.rule.dimension
        penalties[dim] = penalties.get(dim, 0.0) + result.penalty
        weights[dim] = weights.get(dim, 0.0) + result.rule.weight
    normalized = total_penalty / total_weight if total_weight else 0.0
    score = max(minimum, baseline - 100 * normalized)
    subscores: Dict[str, float] = {}
    for dim, weight in weights.items():
        penalty = penalties.get(dim, 0.0)
        if not weight:
            continue
        subscores[dim] = max(minimum, baseline - 100 * (penalty / weight))
    return score, subscores
