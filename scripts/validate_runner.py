#!/usr/bin/env python3
"""CLI that drives the DQSentry validation runner."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from dq.validate.runner import ValidationRunner
from scripts.profile_utils import resolve_stage_path


def load_run_metadata(stage_path: Path) -> dict[str, str]:
    metadata_path = stage_path / "run_metadata.json"
    if not metadata_path.exists():
        raise SystemExit(f"Run metadata missing at {metadata_path}")
    return json.loads(metadata_path.read_text())


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Execute validation checks for a staged dataset."
    )
    parser.add_argument("--dataset-name", help="Dataset name (default comes from staging metadata).")
    parser.add_argument("--seed", type=int, default=42, help="Seed used to ingest the dataset.")
    parser.add_argument(
        "--stage-path",
        type=Path,
        help="Path to an already ingested staging directory (overrides dataset/seed).",
    )
    parser.add_argument("--run-id", help="Optional override for the run identifier.")
    parser.add_argument(
        "--duckdb-path",
        type=Path,
        help="Optional DuckDB file path (defaults to staging/staging.duckdb).",
    )
    args = parser.parse_args()

    stage_path = args.stage_path or resolve_stage_path(args.dataset_name or "phase1", args.seed, None)
    if not stage_path.exists():
        raise SystemExit(f"Staging directory not found at {stage_path}")

    metadata = load_run_metadata(stage_path)
    dataset_name = args.dataset_name or metadata.get("dataset_name")
    if not dataset_name:
        raise SystemExit("Unable to determine dataset name for validation.")
    run_id = args.run_id or metadata.get("run_id")
    if not run_id:
        raise SystemExit("Run identifier is required to execute validation.")

    duckdb_path = args.duckdb_path or stage_path / "staging.duckdb"
    if not duckdb_path.exists():
        raise SystemExit(f"DuckDB file missing at {duckdb_path}")

    runner = ValidationRunner(dataset_name, run_id, stage_path, duckdb_path)
    summary = runner.run()

    print(
        f"Validation complete · run_id={summary.run_id} · dataset={summary.dataset_name} "
        f"· score={summary.score:.2f}"
    )


if __name__ == "__main__":
    main()
