import pandas as pd

from scripts.score_helpers import build_check_summary, compute_scores_from_checks


def test_compute_scores_from_checks_penalizes_failed_weighted_checks():
    checks = pd.DataFrame(
        [
            {"dimension": "completeness", "penalty": 0.0, "weight": 1.0},
            {"dimension": "completeness", "penalty": 0.25, "weight": 1.0},
            {"dimension": "integrity", "penalty": 0.0, "weight": 2.0},
        ]
    )

    overall, subscores = compute_scores_from_checks(
        checks, baseline=100.0, minimum=0.0
    )

    assert overall == 93.75
    assert subscores["completeness"] == 87.5
    assert subscores["integrity"] == 100.0


def test_build_check_summary_orders_failures_with_highest_failure_rate_first():
    checks = pd.DataFrame(
        [
            {
                "check_id": "check_pass",
                "table_name": "users",
                "dimension": "completeness",
                "description": "desc",
                "status": "pass",
                "failure_rate": 0.0,
                "threshold_warning": 0.01,
                "threshold_fail": 0.03,
                "severity": 2,
                "weight": 1.0,
                "penalty": 0.0,
                "issue_type": "missing",
            },
            {
                "check_id": "check_fail",
                "table_name": "events",
                "dimension": "integrity",
                "description": "desc",
                "status": "fail",
                "failure_rate": 0.2,
                "threshold_warning": 0.01,
                "threshold_fail": 0.03,
                "severity": 5,
                "weight": 2.0,
                "penalty": 0.4,
                "issue_type": "fk",
            },
        ]
    )

    summary = build_check_summary(checks)

    assert summary[0]["check_id"] == "check_fail"
    assert summary[0]["severity"] == 5

