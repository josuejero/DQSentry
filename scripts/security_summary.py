#!/usr/bin/env python3
"""Summarize security artifacts for the DQSentry scorecard."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
SECURITY_DIR = REPO_ROOT / "reports" / "latest" / "security"


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize DQSentry security artifacts.")
    parser.add_argument("--security-dir", type=Path, default=SECURITY_DIR)
    args = parser.parse_args()

    summary = build_security_summary(args.security_dir)
    output = args.security_dir / "security_summary.json"
    args.security_dir.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"Wrote {output}")


def build_security_summary(security_dir: Path) -> dict[str, Any]:
    security_dir.mkdir(parents=True, exist_ok=True)
    openssf_path = security_dir / "openssf-scorecard.json"
    sbom_path = security_dir / "bom.json"
    gitleaks_path = security_dir / "gitleaks.json"
    dependency_review_path = security_dir / "dependency-review.json"
    dependency_vulnerability_count = _count_dependency_vulnerabilities(
        dependency_review_path
    )
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "openssf_score": _extract_openssf_score(openssf_path),
        "sbom_generated": sbom_path.exists(),
        "secret_findings": _count_json_findings(gitleaks_path),
        "dependency_vulnerability_count": dependency_vulnerability_count,
        "artifacts": {
            "openssf_scorecard_json": openssf_path.exists(),
            "sbom_json": sbom_path.exists(),
            "gitleaks_json": gitleaks_path.exists(),
            "dependency_review_json": dependency_review_path.exists(),
        },
    }


def _extract_openssf_score(path: Path) -> float | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    if payload.get("score") is not None:
        return round(float(payload["score"]), 2)
    checks = payload.get("checks")
    if not isinstance(checks, list):
        return None
    scores = [
        float(item.get("score", 0.0))
        for item in checks
        if isinstance(item, dict) and item.get("score") is not None
    ]
    return round(sum(scores) / len(scores), 2) if scores else None


def _count_json_findings(path: Path) -> int | None:
    if not path.exists():
        return None
    try:
        payload: Any = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    if isinstance(payload, list):
        return len(payload)
    if isinstance(payload, dict):
        for key in ("findings", "results", "vulnerabilities"):
            value = payload.get(key)
            if isinstance(value, list):
                return len(value)
    return None


def _count_dependency_vulnerabilities(path: Path) -> int | str:
    if not path.exists():
        return "not_recorded_locally"
    count = _count_json_findings(path)
    return count if count is not None else "not_recorded_locally"


if __name__ == "__main__":
    main()
