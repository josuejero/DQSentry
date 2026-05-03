#!/usr/bin/env python3
"""Collect employer-facing DQSentry metrics from latest pipeline outputs."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
REPORTS_LATEST = REPO_ROOT / "reports" / "latest"
DATA_MARTS = REPO_ROOT / "data" / "marts"
SCORE_JSON = REPORTS_LATEST / "score.json"
ISSUES_CSV = REPORTS_LATEST / "issues.csv"
QUALITY_GATE_CONFIG = REPO_ROOT / "dq" / "config" / "quality_gate.yml"
RULES_PATH = REPO_ROOT / "dq" / "config" / "rules.yml"
METRICS_HISTORY_DIR = DATA_MARTS / "project_metrics_history"
METRICS_HISTORY_PATH = METRICS_HISTORY_DIR / "project_metrics.parquet"
ISSUE_LIFECYCLE_PATH = DATA_MARTS / "dq_issue_lifecycle" / "issue_lifecycle.parquet"


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect DQSentry project metrics.")
    parser.add_argument("--run-id", help="Run ID. Defaults to reports/latest/score.json run_id.")
    parser.add_argument("--score-path", type=Path, default=SCORE_JSON)
    parser.add_argument("--issues-path", type=Path, default=ISSUES_CSV)
    parser.add_argument(
        "--write-history",
        action="store_true",
        help="Append to project metrics history Parquet.",
    )
    args = parser.parse_args()

    payload = collect_project_metrics(
        run_id=args.run_id,
        score_path=args.score_path,
        issues_path=args.issues_path,
    )
    write_project_metric_outputs(payload)

    if args.write_history:
        append_history(payload)

    print("Wrote reports/latest/project_metrics.json")
    print("Wrote reports/latest/artifact_manifest.json")
    print("Wrote reports/latest/employer_metrics.md")
    if args.write_history:
        print(f"Appended {METRICS_HISTORY_PATH}")


def collect_project_metrics(
    run_id: str | None = None,
    score_path: Path = SCORE_JSON,
    issues_path: Path = ISSUES_CSV,
) -> dict[str, Any]:
    score = _read_json(score_path)
    resolved_run_id = run_id or score.get("run_id")
    if not resolved_run_id:
        raise SystemExit("Missing run_id. Run score.py first or pass --run-id.")

    checks = _read_checks(resolved_run_id)
    issues = _read_issues(issues_path)
    quality_cfg = _read_yaml(QUALITY_GATE_CONFIG)
    rule_summary = summarize_rules(RULES_PATH)
    regression_metrics = _read_optional_json(REPORTS_LATEST / "regression_metrics.json")
    coverage_summary = _read_optional_json(REPORTS_LATEST / "coverage_summary.json")
    security_summary = _read_optional_json(
        REPORTS_LATEST / "security" / "security_summary.json"
    )

    critical_severity = float(quality_cfg.get("critical_severity", 5))
    score_threshold = float(quality_cfg.get("score_threshold", 90.0))

    check_counts = _status_counts(checks)
    total_checks = int(score.get("total_checks", len(checks)))
    critical_failed_checks = _critical_failed_checks(checks, critical_severity)
    lifecycle_counts = _issue_lifecycle_counts()
    if lifecycle_counts["recurring_issue_count"] == 0:
        lifecycle_counts["recurring_issue_count"] = _recurring_issue_count(
            resolved_run_id
        )
    overall_score = float(score.get("score", 0.0))

    payload: dict[str, Any] = {
        "run_id": resolved_run_id,
        "run_ts": score.get("run_ts"),
        "dataset_name": score.get("dataset_name"),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "commit_sha": os.getenv("GITHUB_SHA", "local"),
        "branch": os.getenv("GITHUB_REF_NAME", _current_branch()),
        "workflow_name": os.getenv("GITHUB_WORKFLOW", "local"),
        "workflow_run_id": os.getenv("GITHUB_RUN_ID", "local"),
        "workflow_attempt": os.getenv("GITHUB_RUN_ATTEMPT", "local"),
        "overall_score": overall_score,
        "score_threshold": score_threshold,
        "critical_severity": critical_severity,
        "quality_gate_status": (
            "pass"
            if overall_score >= score_threshold and critical_failed_checks == 0
            else "fail"
        ),
        "total_checks": total_checks,
        "passed_checks": check_counts.get("pass", 0),
        "warning_checks": check_counts.get("warn", 0),
        "failed_checks": check_counts.get("fail", int(score.get("failed_checks", 0))),
        "check_pass_rate": _safe_ratio(check_counts.get("pass", 0), total_checks),
        "critical_failed_checks": critical_failed_checks,
        "subscores": score.get("subscores", {}),
        "issue_counts": score.get("issue_counts", {}),
        "total_issues": int(len(issues)),
        "max_severity": _max_numeric(issues, "severity", as_int=True),
        "max_affected_pct": _max_numeric(issues, "affected_pct"),
        "issue_root_cause_coverage_pct": root_cause_coverage(issues),
        "rule_summary": rule_summary,
        "regression": regression_metrics,
        "coverage": coverage_summary,
        "security": security_summary,
        "scorecard_published": (REPORTS_LATEST / "index.html").exists(),
    }
    payload.update(lifecycle_counts)
    payload["artifacts"] = []
    payload["artifact_count"] = 0
    return payload


def write_project_metric_outputs(payload: dict[str, Any]) -> None:
    REPORTS_LATEST.mkdir(parents=True, exist_ok=True)

    # First pass ensures generated files are visible when the manifest is built.
    (REPORTS_LATEST / "project_metrics.json").write_text(
        json.dumps(payload, indent=2), encoding="utf-8"
    )
    (REPORTS_LATEST / "artifact_manifest.json").write_text("[]\n", encoding="utf-8")
    (REPORTS_LATEST / "employer_metrics.md").write_text(
        render_employer_markdown(payload), encoding="utf-8"
    )

    artifact_manifest = build_artifact_manifest(str(payload["run_id"]))
    payload["artifacts"] = artifact_manifest
    payload["artifact_count"] = sum(1 for item in artifact_manifest if item["exists"])
    payload["scorecard_published"] = (REPORTS_LATEST / "index.html").exists()

    (REPORTS_LATEST / "project_metrics.json").write_text(
        json.dumps(payload, indent=2), encoding="utf-8"
    )
    (REPORTS_LATEST / "artifact_manifest.json").write_text(
        json.dumps(artifact_manifest, indent=2), encoding="utf-8"
    )
    (REPORTS_LATEST / "employer_metrics.md").write_text(
        render_employer_markdown(payload), encoding="utf-8"
    )


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"Missing JSON file: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _read_optional_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"error": f"Could not parse {path.relative_to(REPO_ROOT)}"}


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _read_checks(run_id: str) -> pd.DataFrame:
    path = DATA_MARTS / "dq_check_results" / f"run_id={run_id}" / "check_results.parquet"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def _read_issues(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def _status_counts(checks: pd.DataFrame) -> dict[str, int]:
    if checks.empty or "status" not in checks:
        return {"pass": 0, "warn": 0, "fail": 0}
    raw = checks["status"].fillna("unknown").value_counts().to_dict()
    return {
        "pass": int(raw.get("pass", 0)),
        "warn": int(raw.get("warn", 0)),
        "fail": int(raw.get("fail", 0)),
    }


def _critical_failed_checks(checks: pd.DataFrame, critical_severity: float) -> int:
    if checks.empty or not {"status", "severity"}.issubset(checks.columns):
        return 0
    return int(((checks["status"] == "fail") & (checks["severity"] >= critical_severity)).sum())


def _safe_ratio(numerator: int | float, denominator: int | float) -> float:
    if not denominator:
        return 0.0
    return round(float(numerator) / float(denominator), 4)


def root_cause_coverage(issues: pd.DataFrame) -> float:
    if issues.empty:
        return 1.0
    needed = ["probable_root_cause", "recommended_fix"]
    if not all(col in issues.columns for col in needed):
        return 0.0
    covered = (
        issues[needed]
        .fillna("")
        .apply(lambda row: all(str(value).strip() for value in row), axis=1)
        .sum()
    )
    return _safe_ratio(int(covered), len(issues))


def _recurring_issue_count(run_id: str) -> int:
    path = (
        DATA_MARTS
        / "dq_issue_recurrence"
        / f"run_id={run_id}"
        / "top_recurring_issues.parquet"
    )
    if not path.exists():
        return 0
    df = pd.read_parquet(path)
    if df.empty or "occurrences" not in df:
        return 0
    return int((df["occurrences"] > 1).sum())


def _issue_lifecycle_counts(path: Path = ISSUE_LIFECYCLE_PATH) -> dict[str, int]:
    counts = {
        "open_issue_count": 0,
        "new_issue_count": 0,
        "recurring_issue_count": 0,
        "resolved_or_not_seen_issue_count": 0,
    }
    if not path.exists():
        return counts
    lifecycle = pd.read_parquet(path)
    if lifecycle.empty:
        return counts
    status = lifecycle.get("status", pd.Series(dtype=object)).fillna("")
    occurrences = lifecycle.get("occurrence_count", pd.Series(dtype=int)).fillna(0)
    open_mask = status == "open"
    counts["open_issue_count"] = int(open_mask.sum())
    counts["new_issue_count"] = int(((occurrences == 1) & open_mask).sum())
    counts["recurring_issue_count"] = int(((occurrences > 1) & open_mask).sum())
    counts["resolved_or_not_seen_issue_count"] = int(
        (status == "not_seen_in_latest_run").sum()
    )
    return counts


def summarize_rules(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    checks = raw.get("checks") or {}
    rows: list[dict[str, Any]] = []
    for dimension, entries in checks.items():
        for entry in entries or []:
            rows.append(
                {
                    "dimension": dimension,
                    "table": entry.get("table", ""),
                    "rule": str(entry.get("rule", "")).split("(")[0],
                    "severity": int(entry.get("severity", 0)),
                    "weight": float(entry.get("weight", 0.0)),
                    "has_description": bool(entry.get("description")),
                }
            )
    if not rows:
        return {"total_rules": 0}
    frame = pd.DataFrame(rows)
    return {
        "total_rules": int(len(frame)),
        "dimensions": {
            str(k): int(v) for k, v in frame["dimension"].value_counts().to_dict().items()
        },
        "tables": {
            str(k): int(v) for k, v in frame["table"].value_counts().to_dict().items()
        },
        "rule_types": {
            str(k): int(v) for k, v in frame["rule"].value_counts().to_dict().items()
        },
        "severity_counts": {
            str(k): int(v)
            for k, v in frame["severity"].value_counts().sort_index().to_dict().items()
        },
        "total_weight": round(float(frame["weight"].sum()), 2),
        "documentation_coverage_pct": _safe_ratio(
            int(frame["has_description"].sum()), len(frame)
        ),
    }


def build_artifact_manifest(run_id: str) -> list[dict[str, Any]]:
    candidates = [
        ("scorecard_html", REPORTS_LATEST / "index.html"),
        ("score_json", REPORTS_LATEST / "score.json"),
        ("issues_csv", REPORTS_LATEST / "issues.csv"),
        ("project_metrics_json", REPORTS_LATEST / "project_metrics.json"),
        ("employer_metrics_md", REPORTS_LATEST / "employer_metrics.md"),
        ("artifact_manifest_json", REPORTS_LATEST / "artifact_manifest.json"),
        ("coverage_summary_json", REPORTS_LATEST / "coverage_summary.json"),
        ("regression_metrics_json", REPORTS_LATEST / "regression_metrics.json"),
        ("security_summary_json", REPORTS_LATEST / "security" / "security_summary.json"),
        ("sbom_json", REPORTS_LATEST / "security" / "bom.json"),
        ("gitleaks_json", REPORTS_LATEST / "security" / "gitleaks.json"),
        ("openssf_scorecard_json", REPORTS_LATEST / "security" / "openssf-scorecard.json"),
        ("profile_html", REPO_ROOT / "reports" / "runs" / run_id / "profile.html"),
        (
            "profile_html_partitioned",
            REPO_ROOT / "reports" / "runs" / f"run_id={run_id}" / "profile.html",
        ),
        (
            "archived_scorecard",
            REPO_ROOT / "reports" / "runs" / f"run_id={run_id}" / "index.html",
        ),
        (
            "archived_issues_csv",
            REPO_ROOT / "reports" / "runs" / f"run_id={run_id}" / "issues.csv",
        ),
        (
            "check_results_parquet",
            DATA_MARTS / "dq_check_results" / f"run_id={run_id}" / "check_results.parquet",
        ),
        (
            "issue_log_parquet",
            DATA_MARTS / "dq_issue_log" / f"run_id={run_id}" / "issue_log.parquet",
        ),
        ("issue_lifecycle_parquet", ISSUE_LIFECYCLE_PATH),
        ("project_metrics_history_parquet", METRICS_HISTORY_PATH),
    ]
    return [
        {
            "name": name,
            "path": str(path.relative_to(REPO_ROOT)),
            "exists": path.exists(),
            "size_bytes": path.stat().st_size if path.exists() and path.is_file() else 0,
        }
        for name, path in candidates
    ]


def render_employer_markdown(payload: dict[str, Any]) -> str:
    subscores = payload.get("subscores") or {}
    issue_counts = payload.get("issue_counts") or {}
    regression = payload.get("regression") or {}
    coverage = payload.get("coverage") or {}
    security = payload.get("security") or {}
    lines = [
        "# DQSentry project metrics",
        "",
        f"Generated: {payload.get('generated_at')}",
        f"Run ID: `{payload.get('run_id')}`",
        f"Dataset: `{payload.get('dataset_name')}`",
        "",
        "## Employer scan",
        "",
        f"- Overall data quality score: **{float(payload.get('overall_score', 0.0)):.2f} / 100**",
        f"- Quality gate: **{payload.get('quality_gate_status')}**",
        f"- Automated checks: **{payload.get('total_checks')}**",
        f"- Passed / warned / failed checks: **{payload.get('passed_checks')} / {payload.get('warning_checks')} / {payload.get('failed_checks')}**",
        f"- Check pass rate: **{float(payload.get('check_pass_rate', 0.0)) * 100:.1f}%**",
        f"- Critical failed checks: **{payload.get('critical_failed_checks')}**",
        f"- Issues detected: **{payload.get('total_issues')}**",
        f"- Open / new / recurring issues: **{payload.get('open_issue_count')} / {payload.get('new_issue_count')} / {payload.get('recurring_issue_count')}**",
        f"- Resolved or not seen issues: **{payload.get('resolved_or_not_seen_issue_count')}**",
        f"- Root-cause coverage: **{float(payload.get('issue_root_cause_coverage_pct', 0.0)) * 100:.1f}%**",
        f"- Published artifacts present: **{payload.get('artifact_count')}**",
        "",
        "## Dimension subscores",
        "",
    ]
    if subscores:
        for key, value in subscores.items():
            lines.append(f"- {key.replace('_', ' ').title()}: **{float(value):.2f}**")
    else:
        lines.append("- No subscores found.")
    lines.extend(["", "## Issue counts", ""])
    if issue_counts:
        for key, value in issue_counts.items():
            lines.append(f"- {key}: **{value}**")
    else:
        lines.append("- No issue counts found.")
    lines.extend(["", "## Regression and coverage", ""])
    lines.append(f"- Regression status: **{regression.get('status', 'not recorded')}**")
    if "score_delta" in regression:
        lines.append(f"- Regression score delta: **{regression.get('score_delta')}**")
    lines.append(f"- Line coverage: **{coverage.get('line_coverage_pct', 'not recorded')}**")
    lines.append(f"- Branch coverage: **{coverage.get('branch_coverage_pct', 'not recorded')}**")
    lines.extend(["", "## Security", ""])
    lines.append(f"- OpenSSF score: **{security.get('openssf_score', 'not recorded')}**")
    lines.append(f"- SBOM generated: **{security.get('sbom_generated', 'not recorded')}**")
    lines.append(
        "- Dependency vulnerabilities: "
        f"**{security.get('dependency_vulnerability_count', 'not recorded')}**"
    )
    lines.append(f"- Secret findings: **{security.get('secret_findings', 'not recorded')}**")
    lines.extend(["", "## Public artifacts", ""])
    for item in payload.get("artifacts", []):
        status = "present" if item["exists"] else "missing"
        lines.append(f"- `{item['path']}` - {status}")
    lines.append("")
    return "\n".join(lines)


def append_history(payload: dict[str, Any]) -> None:
    METRICS_HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    flattened = {
        "run_id": payload.get("run_id"),
        "run_ts": payload.get("run_ts"),
        "dataset_name": payload.get("dataset_name"),
        "generated_at": payload.get("generated_at"),
        "commit_sha": payload.get("commit_sha"),
        "branch": payload.get("branch"),
        "workflow_name": payload.get("workflow_name"),
        "workflow_run_id": str(payload.get("workflow_run_id")),
        "workflow_attempt": str(payload.get("workflow_attempt")),
        "overall_score": payload.get("overall_score"),
        "score_threshold": payload.get("score_threshold"),
        "quality_gate_status": payload.get("quality_gate_status"),
        "total_checks": payload.get("total_checks"),
        "passed_checks": payload.get("passed_checks"),
        "warning_checks": payload.get("warning_checks"),
        "failed_checks": payload.get("failed_checks"),
        "check_pass_rate": payload.get("check_pass_rate"),
        "critical_failed_checks": payload.get("critical_failed_checks"),
        "total_issues": payload.get("total_issues"),
        "max_severity": payload.get("max_severity"),
        "max_affected_pct": payload.get("max_affected_pct"),
        "issue_root_cause_coverage_pct": payload.get("issue_root_cause_coverage_pct"),
        "open_issue_count": payload.get("open_issue_count"),
        "new_issue_count": payload.get("new_issue_count"),
        "recurring_issue_count": payload.get("recurring_issue_count"),
        "resolved_or_not_seen_issue_count": payload.get(
            "resolved_or_not_seen_issue_count"
        ),
        "artifact_count": payload.get("artifact_count"),
        "scorecard_published": payload.get("scorecard_published"),
        "regression_status": (payload.get("regression") or {}).get("status"),
        "regression_score_delta": (payload.get("regression") or {}).get("score_delta"),
        "line_coverage_pct": (payload.get("coverage") or {}).get("line_coverage_pct"),
        "branch_coverage_pct": (payload.get("coverage") or {}).get(
            "branch_coverage_pct"
        ),
        "openssf_score": (payload.get("security") or {}).get("openssf_score"),
        "dependency_vulnerability_count": str(
            (payload.get("security") or {}).get("dependency_vulnerability_count")
        ),
        "sbom_generated": (payload.get("security") or {}).get("sbom_generated"),
        "secret_findings": (payload.get("security") or {}).get("secret_findings"),
    }
    frame = pd.DataFrame([flattened])
    if METRICS_HISTORY_PATH.exists():
        existing = pd.read_parquet(METRICS_HISTORY_PATH)
        frames = [
            candidate.dropna(axis=1, how="all")
            for candidate in (existing, frame)
            if not candidate.empty
        ]
        frame = pd.concat(frames, ignore_index=True) if frames else frame
        frame = frame.drop_duplicates(
            subset=["run_id", "workflow_run_id", "workflow_attempt"], keep="last"
        )
    frame.to_parquet(METRICS_HISTORY_PATH, index=False)


def _max_numeric(frame: pd.DataFrame, column: str, as_int: bool = False) -> int | float:
    if frame.empty or column not in frame.columns:
        return 0
    numeric = pd.to_numeric(frame[column], errors="coerce").dropna()
    if numeric.empty:
        return 0
    value = numeric.max()
    return int(value) if as_int else float(value)


def _current_branch() -> str:
    head = REPO_ROOT / ".git" / "HEAD"
    if not head.exists():
        return "local"
    text = head.read_text(encoding="utf-8").strip()
    if text.startswith("ref: refs/heads/"):
        return text.removeprefix("ref: refs/heads/")
    return "local"


if __name__ == "__main__":
    main()
