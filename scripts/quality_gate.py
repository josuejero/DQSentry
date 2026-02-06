#!/usr/bin/env python3
"""Fail the build when the overall score or critical checks miss the target."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import yaml

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
SCORE_JSON_PATH = REPO_ROOT / "reports" / "latest" / "score.json"
CHECK_RESULTS_BASE = REPO_ROOT / "data" / "marts" / "dq_check_results"
CONFIG_PATH = REPO_ROOT / "dq" / "config" / "quality_gate.yml"

DEFAULT_SCORE_THRESHOLD = 90.0
DEFAULT_CRITICAL_SEVERITY = 5


def load_quality_config() -> dict[str, float]:
    if not CONFIG_PATH.exists():
        return {
            "score_threshold": DEFAULT_SCORE_THRESHOLD,
            "critical_severity": DEFAULT_CRITICAL_SEVERITY,
        }
    raw = yaml.safe_load(CONFIG_PATH.read_text()) or {}
    return {
        "score_threshold": float(raw.get("score_threshold", DEFAULT_SCORE_THRESHOLD)),
        "critical_severity": float(raw.get("critical_severity", DEFAULT_CRITICAL_SEVERITY)),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Enforce the DQSentry quality gate.")
    parser.add_argument(
        "--score-path",
        type=Path,
        default=SCORE_JSON_PATH,
        help="Score payload used to evaluate the gate.",
    )
    parser.add_argument(
        "--run-id",
        help="Optional run identifier if the score payload does not include one.",
    )
    parser.add_argument(
        "--score-threshold",
        type=float,
        help="Override the configured minimum overall score.",
    )
    parser.add_argument(
        "--critical-severity",
        type=float,
        help="Override the configured severity that defines critical checks.",
    )
    args = parser.parse_args()

    if not args.score_path.exists():
        raise SystemExit(f"Missing score payload at {args.score_path}")
    score_payload = json.loads(args.score_path.read_text())
    score_threshold = (
        args.score_threshold
        if args.score_threshold is not None
        else load_quality_config()["score_threshold"]
    )
    critical_severity = (
        args.critical_severity
        if args.critical_severity is not None
        else load_quality_config()["critical_severity"]
    )

    overall = float(score_payload.get("score", 0.0))
    run_id = args.run_id or score_payload.get("run_id")
    dataset_name = score_payload.get("dataset_name", "unknown")

    if overall < score_threshold:
        raise SystemExit(
            f"Quality gate failed: overall score {overall:.2f} is below threshold {score_threshold:.2f}."
        )

    if not run_id:
        raise SystemExit("Quality gate requires a run_id but none was provided.")

    check_path = CHECK_RESULTS_BASE / f"run_id={run_id}" / "check_results.parquet"
    if not check_path.exists():
        raise SystemExit(f"Missing check results at {check_path}")
    checks = pd.read_parquet(check_path)
    critical_fails = checks[
        (checks["status"] == "fail") & (checks["severity"] >= critical_severity)
    ]

    if not critical_fails.empty:
        checks = sorted(
            f"{row.check_id} ({row.table_name})"
            for row in critical_fails.sort_values("check_id").itertuples(index=False)
        )
        raise SystemExit(
            f"Critical checks failed for run {run_id}: {', '.join(checks)} "
            f"(severity ≥ {critical_severity:.1f})."
        )

    print(
        f"Quality gate passed for run {run_id} ({dataset_name}) "
        f"with overall score {overall:.2f} (threshold {score_threshold:.2f})."
    )


if __name__ == "__main__":
    main()
