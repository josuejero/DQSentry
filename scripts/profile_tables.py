#!/usr/bin/env python3
"""Entry point that profiles staged tables into Parquet and HTML artifacts."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.profile_collector import ProfileCollector, ProfileConfig
from scripts.profile_utils import load_json, resolve_stage_path

PROFILES_BASE = REPO_ROOT / "data" / "marts" / "profiles"
REPORTS_RUNS = REPO_ROOT / "reports" / "runs"
DEFAULT_TOP_N = 5


def render_html(
    run_id: str,
    dataset_name: str,
    seed: int | None,
    metadata: dict[str, Any],
    table_aggregates: dict[str, dict[str, Any]],
    type_issues: list[dict[str, Any]],
) -> str:
    title = f"Profile report: {escape(run_id)}"
    lines = [
        "<!doctype html>",
        "<html lang=\"en\">",
        "<head><meta charset=\"utf-8\"><title>Profile report</title></head>",
        "<body>",
        f"<h1>{title}</h1>",
        (
            "<p>Dataset: "
            f"{escape(dataset_name)}"
            + (f" (seed {seed})" if seed is not None else "")
            + f" · profiled at {escape(metadata['profiled_at'])}</p>"
        ),
        "<section>",
        "<h2>Table summary</h2>",
        "<table border=1 cellpadding=4 cellspacing=0>",
        "<tr><th>Table</th><th>Rows</th><th>Columns</th><th>Nulliest column</th>"
        "<th>Max null %</th><th>High cardinality column</th><th>Distinct values</th></tr>",
    ]
    for agg in table_aggregates.values():
        lines.append(
            "<tr>"
            f"<td>{escape(agg['table_name'])}</td>"
            f"<td>{agg['row_count']}</td>"
            f"<td>{agg['column_count']}</td>"
            f"<td>{escape(agg['nulliest_column'] or '')}</td>"
            f"<td>{agg['max_null_rate']:.1%}</td>"
            f"<td>{escape(agg['high_cardinality_column'] or '')}</td>"
            f"<td>{agg['max_cardinality']}</td>"
            "</tr>"
        )
    lines.extend(["</table>", "</section>", "<section>", "<h2>Type issues</h2>"])
    if not type_issues:
        lines.append("<p>No type issues detected.</p>")
    else:
        lines.append("<ul>")
        for issue in type_issues:
            lines.append(
                "<li>"
                f"{escape(issue['table_name'])}.{escape(issue['column_name'])}: "
                f"{escape(issue['issue'])}"
                "</li>"
            )
        lines.append("</ul>")
    lines.append("</section>")
    lines.append("</body></html>")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Profile staged tables and persist metadata.")
    parser.add_argument("--dataset-name", default="phase1", help="Name of the ingested dataset.")
    parser.add_argument("--seed", type=int, default=42, help="Seed used when ingesting the data.")
    parser.add_argument(
        "--stage-path",
        type=Path,
        help="Direct path to a staging directory that already exists.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=DEFAULT_TOP_N,
        help="Number of top values to capture per column.",
    )
    args = parser.parse_args()

    stage_path = resolve_stage_path(args.dataset_name, args.seed, args.stage_path)
    if not stage_path.exists():
        raise SystemExit(f"Staging directory {stage_path} does not exist.")
    duckdb_path = stage_path / "staging.duckdb"
    if not duckdb_path.exists():
        raise SystemExit(f"Missing DuckDB file at {duckdb_path}.")

    run_metadata = load_json(stage_path / "run_metadata.json")
    run_id = run_metadata.get("run_id") or f"{args.dataset_name}-{args.seed}-{int(datetime.now(timezone.utc).timestamp())}"
    profile_timestamp = datetime.now(timezone.utc).isoformat()

    profile_dir = PROFILES_BASE / f"run_id={run_id}"
    report_dir = REPORTS_RUNS / run_id
    profile_dir.mkdir(parents=True, exist_ok=True)
    report_dir.mkdir(parents=True, exist_ok=True)

    config = ProfileConfig(
        dataset_name=args.dataset_name,
        seed=args.seed,
        stage_path=stage_path,
        duckdb_path=duckdb_path,
        run_metadata=run_metadata,
        run_id=run_id,
        top_n=args.top_n,
    )
    collector = ProfileCollector(config)
    result = collector.collect()

    metadata = {
        "run_id": run_id,
        "dataset_name": args.dataset_name,
        "seed": args.seed,
        "profiled_at": profile_timestamp,
        "stage_path": str(stage_path),
        "duckdb_path": str(duckdb_path),
        "raw_metadata": run_metadata,
    }

    pd.DataFrame(result.column_records).to_parquet(
        profile_dir / "column_profiles.parquet", index=False
    )
    pd.DataFrame(result.top_value_records).to_parquet(
        profile_dir / "column_top_values.parquet", index=False
    )
    pd.DataFrame(result.type_issue_records).to_parquet(
        profile_dir / "column_type_issues.parquet", index=False
    )
    pd.DataFrame(result.table_summary_records).to_parquet(
        profile_dir / "table_summaries.parquet", index=False
    )
    pd.DataFrame([metadata]).to_parquet(profile_dir / "profile_metadata.parquet", index=False)
    (profile_dir / "profile_metadata.json").write_text(json.dumps(metadata, indent=2))

    html_report = render_html(run_id, args.dataset_name, args.seed, metadata, result.table_aggregates, result.type_issue_records)
    report_path = report_dir / "profile.html"
    report_path.write_text(html_report, encoding="utf-8")

    print(f"Profile artifacts persisted to {profile_dir}.")
    print(f"HTML report available at {report_path}.")


if __name__ == "__main__":
    main()
