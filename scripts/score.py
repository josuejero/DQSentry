#!/usr/bin/env python3
"""Compute scores from check results and generate latest scorecard data."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dq.validate.config import load_rules

from scripts.score_helpers import (
    CHECK_RESULTS_BASE,
    ISSUE_LOG_BASE,
    ISSUE_LOG_COLUMNS,
    LATEST_REPORT_DIR,
    append_score_history,
    build_check_summary,
    build_issue_preview,
    compute_scores_from_checks,
    parse_iso_timestamp,
    read_run_history,
)

RULES_PATH = Path(__file__).resolve().parents[1] / "dq" / "config" / "rules.yml"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compute run-level scores and persist scorecard artifacts."
    )
    parser.add_argument("--run-id", required=True, help="Run identifier to publish.")
    args = parser.parse_args()

    run_id = args.run_id
    checks_path = CHECK_RESULTS_BASE / f"run_id={run_id}" / "check_results.parquet"
    if not checks_path.exists():
        raise SystemExit(f"Missing check results at {checks_path}")
    issues_path = ISSUE_LOG_BASE / f"run_id={run_id}" / "issue_log.parquet"
    checks = pd.read_parquet(checks_path)
    issues = pd.DataFrame()
    if issues_path.exists():
        issues = pd.read_parquet(issues_path)

    _, baseline_cfg, minimum_cfg = load_rules(RULES_PATH)
    baseline = baseline_cfg
    minimum = minimum_cfg

    run_ts_value, dataset_hint = read_run_history(run_id)
    if not issues.empty:
        available_ts = issues["run_ts"].dropna()
        issue_ts = available_ts.iat[0] if not available_ts.empty else None
        if not run_ts_value and issue_ts:
            run_ts_value = issue_ts
    run_ts = parse_iso_timestamp(run_ts_value) or datetime.now(timezone.utc)
    dataset_name = checks["dataset_name"].iat[0] if not checks.empty else dataset_hint or "unknown"

    overall_score, subscores = compute_scores_from_checks(checks, baseline, minimum)
    issue_preview = build_issue_preview(issues)
    check_summary = build_check_summary(checks)
    issue_counts = (
        issues["issue_type"].value_counts().to_dict()
        if not issues.empty
        else {}
    )

    if run_ts.tzinfo is None:
        run_ts = run_ts.replace(tzinfo=timezone.utc)
    run_ts_iso = run_ts.astimezone(timezone.utc).isoformat()

    history_record = {
        "run_id": run_id,
        "run_ts": run_ts_iso,
        "dataset_name": dataset_name,
        "score": overall_score,
        "baseline": baseline,
        "minimum": minimum,
        "total_penalty": float(checks["penalty"].sum()),
        "total_weight": float(checks["weight"].sum()),
        "total_checks": int(len(checks)),
        "failed_checks": int(len(issues)),
        "subscores": json.dumps({k: float(v) for k, v in subscores.items()}, ensure_ascii=False),
    }
    append_score_history(history_record)

    payload = {
        "run_id": run_id,
        "run_ts": run_ts_iso,
        "dataset_name": dataset_name,
        "score": round(overall_score, 2),
        "baseline": baseline,
        "minimum": minimum,
        "total_checks": int(len(checks)),
        "failed_checks": int(len(issues)),
        "issue_counts": {k: int(v) for k, v in issue_counts.items()},
        "subscores": {k: round(v, 2) for k, v in subscores.items()},
        "check_summary": check_summary,
        "issue_preview": issue_preview,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    LATEST_REPORT_DIR.mkdir(parents=True, exist_ok=True)
    score_json_path = LATEST_REPORT_DIR / "score.json"
    score_json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    issues_csv_path = LATEST_REPORT_DIR / "issues.csv"
    if not issues.empty:
        issues.to_csv(issues_csv_path, index=False)
    else:
        pd.DataFrame(columns=ISSUE_LOG_COLUMNS).to_csv(issues_csv_path, index=False)

    print(f"Score data written to {score_json_path}")
    print(f"Issue log (CSV) written to {issues_csv_path}")


if __name__ == "__main__":
    main()
