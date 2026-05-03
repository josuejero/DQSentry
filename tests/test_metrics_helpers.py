import json

import pandas as pd

from scripts.collect_project_metrics import root_cause_coverage, summarize_rules
from scripts.coverage_summary import build_coverage_summary
from scripts.issue_lifecycle import build_issue_lifecycle


def test_root_cause_coverage_requires_cause_and_fix():
    issues = pd.DataFrame(
        [
            {"probable_root_cause": "Bad import", "recommended_fix": "Reload"},
            {"probable_root_cause": "Missing map", "recommended_fix": ""},
        ]
    )

    assert root_cause_coverage(issues) == 0.5


def test_summarize_rules_counts_dimensions_and_documentation(tmp_path):
    rules_path = tmp_path / "rules.yml"
    rules_path.write_text(
        """
checks:
  completeness:
    - table: users
      rule: NULL_PERCENTAGE
      severity: 3
      weight: 1.5
      description: Email required.
  integrity:
    - table: events
      rule: FK(users.user_id)
      severity: 5
      weight: 2.0
""",
        encoding="utf-8",
    )

    summary = summarize_rules(rules_path)

    assert summary["total_rules"] == 2
    assert summary["dimensions"] == {"completeness": 1, "integrity": 1}
    assert summary["documentation_coverage_pct"] == 0.5


def test_build_coverage_summary_calculates_branch_percentage(tmp_path):
    coverage_path = tmp_path / "coverage.json"
    coverage_path.write_text(
        json.dumps(
            {
                "totals": {
                    "percent_covered": 80.0,
                    "covered_lines": 8,
                    "missing_lines": 2,
                    "num_statements": 10,
                    "covered_branches": 3,
                    "missing_branches": 1,
                    "num_branches": 4,
                }
            }
        ),
        encoding="utf-8",
    )

    summary = build_coverage_summary(coverage_path)

    assert summary["line_coverage_pct"] == 80.0
    assert summary["branch_coverage_pct"] == 75.0


def test_issue_lifecycle_marks_latest_issues_open(tmp_path):
    history_path = tmp_path / "issue_history.parquet"
    history = pd.DataFrame(
        [
            {
                "run_id": "run-1",
                "run_ts": "2026-01-01T00:00:00+00:00",
                "table_name": "users",
                "check_name": "email_unique",
                "dimension": "uniqueness",
                "issue_type": "duplicate",
                "severity": 4,
                "affected_pct": 0.2,
                "probable_root_cause": "Duplicate import",
                "recommended_fix": "Deduplicate users",
            },
            {
                "run_id": "run-2",
                "run_ts": "2026-01-02T00:00:00+00:00",
                "table_name": "users",
                "check_name": "email_unique",
                "dimension": "uniqueness",
                "issue_type": "duplicate",
                "severity": 4,
                "affected_pct": 0.1,
                "probable_root_cause": "Duplicate import",
                "recommended_fix": "Deduplicate users",
            },
            {
                "run_id": "run-1",
                "run_ts": "2026-01-01T00:00:00+00:00",
                "table_name": "events",
                "check_name": "events_user_fk",
                "dimension": "integrity",
                "issue_type": "fk",
                "severity": 5,
                "affected_pct": 0.3,
                "probable_root_cause": "Missing users",
                "recommended_fix": "Load users first",
            },
        ]
    )
    history.to_parquet(history_path, index=False)

    lifecycle = build_issue_lifecycle(history_path)
    email_issue = lifecycle[lifecycle["issue_key"] == "users|email_unique|duplicate"].iloc[0]
    fk_issue = lifecycle[lifecycle["issue_key"] == "events|events_user_fk|fk"].iloc[0]

    assert email_issue["status"] == "open"
    assert email_issue["occurrence_count"] == 2
    assert fk_issue["status"] == "not_seen_in_latest_run"
