#!/usr/bin/env python3
"""Write a compact JSON summary from coverage.py JSON output."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = REPO_ROOT / "reports" / "latest" / "coverage.json"
DEFAULT_OUTPUT = REPO_ROOT / "reports" / "latest" / "coverage_summary.json"


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize coverage output.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()

    summary = build_coverage_summary(args.input)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"Wrote {args.output}")


def build_coverage_summary(path: Path) -> dict[str, object]:
    if not path.exists():
        raise SystemExit(f"Missing coverage JSON at {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    totals = payload.get("totals", {})
    covered_branches = int(totals.get("covered_branches", 0))
    missing_branches = int(totals.get("missing_branches", 0))
    num_branches = int(totals.get("num_branches", covered_branches + missing_branches))
    branch_pct = (covered_branches / num_branches * 100) if num_branches else 0.0
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "line_coverage_pct": round(float(totals.get("percent_covered", 0.0)), 2),
        "covered_lines": int(totals.get("covered_lines", 0)),
        "missing_lines": int(totals.get("missing_lines", 0)),
        "num_statements": int(totals.get("num_statements", 0)),
        "branch_coverage_pct": round(branch_pct, 2),
        "covered_branches": covered_branches,
        "missing_branches": missing_branches,
        "num_branches": num_branches,
    }


if __name__ == "__main__":
    main()
