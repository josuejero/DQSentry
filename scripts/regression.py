"""Golden dataset regression suite for locking scoring logic."""

from __future__ import annotations

import argparse
import json
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.ingest_lib import ingest_dataset
from dq.validate.runner import ValidationRunner

DEFAULT_DATASET_ARCHIVE = REPO_ROOT / "dq" / "regression" / "golden_dataset.zip"
DEFAULT_EXPECTED_PATH = REPO_ROOT / "dq" / "regression" / "golden_expected.json"
DEFAULT_DATASET_NAME = "phase1"
DEFAULT_SEED = 42
DEFAULT_TOLERANCE = 0.01


def main() -> None:
    parser = argparse.ArgumentParser(description="Golden dataset regression suite.")
    parser.add_argument(
        "--dataset-archive",
        type=Path,
        default=DEFAULT_DATASET_ARCHIVE,
        help="ZIP archive containing the known-good CSV exports.",
    )
    parser.add_argument(
        "--expected-path",
        type=Path,
        default=DEFAULT_EXPECTED_PATH,
        help="JSON file that holds the expected scores and counts.",
    )
    parser.add_argument(
        "--dataset-name",
        default=DEFAULT_DATASET_NAME,
        help="Logical dataset name to pass through the pipeline.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help="Seed used when ingesting the dataset for deterministic metadata.",
    )
    parser.add_argument(
        "--tolerance",
        type=float,
        default=DEFAULT_TOLERANCE,
        help="Allowed deviation when comparing scores and subscores.",
    )
    parser.add_argument(
        "--update-expected",
        action="store_true",
        help="Refresh the expected output with the current run's results.",
    )
    parser.add_argument(
        "--metrics-output",
        type=Path,
        default=REPO_ROOT / "reports" / "latest" / "regression_metrics.json",
        help="Optional JSON path for regression metrics.",
    )
    args = parser.parse_args()

    if not args.dataset_archive.exists():
        raise SystemExit(f"Dataset archive not found at {args.dataset_archive}.")

    actual = _run_golden_pipeline(args.dataset_archive, args.dataset_name, args.seed)

    if args.update_expected:
        _write_expected(actual, args.expected_path)
        print(f"Updated expected regression data at {args.expected_path}.")
        return

    expected = _load_expected(args.expected_path)
    metrics = _build_regression_metrics(actual, expected, args.tolerance)
    _write_regression_metrics(metrics, args.metrics_output)
    try:
        _assert_matches(actual, expected, args.tolerance)
    except SystemExit as exc:
        metrics["status"] = "fail"
        metrics["failure_reason"] = str(exc)
        _write_regression_metrics(metrics, args.metrics_output)
        raise
    metrics["status"] = "pass"
    _write_regression_metrics(metrics, args.metrics_output)
    print(
        "Golden dataset regression passed for run "
        f"{actual['run_id']} (score {actual['score']:.2f})."
    )


def _run_golden_pipeline(
    archive: Path, dataset_name: str, seed: int
) -> dict[str, Any]:
    with TemporaryDirectory(prefix="dq-regression-") as tmpdir:
        temp_root = Path(tmpdir)
        raw_dir = temp_root / "raw"
        raw_dir.mkdir()
        _extract_archive(archive, raw_dir)
        metadata = _load_metadata(raw_dir)
        run_id = metadata.get("run_id")
        logical_dataset_name = metadata.get("dataset_name") or dataset_name
        stage_dir = temp_root / "stage"
        ingest_paths = ingest_dataset(
            dataset_name=logical_dataset_name,
            seed=seed,
            force=True,
            raw_path=raw_dir,
            stage_path=stage_dir,
            run_id=run_id,
        )
        stage_path = Path(ingest_paths["stage_path"])
        duckdb_path = Path(ingest_paths["db_path"])
        runner = ValidationRunner(
            logical_dataset_name,
            ingest_paths["run_id"],
            stage_path,
            duckdb_path,
        )
        summary = runner.run()
        issue_df = _load_issue_log(summary.issue_log_path)
        issue_counts = (
            issue_df["issue_type"].dropna().value_counts().to_dict()
            if not issue_df.empty
            else {}
        )
        subscores = {k: round(float(v), 2) for k, v in (summary.subscores or {}).items()}
        return {
            "run_id": summary.run_id,
            "dataset_name": summary.dataset_name,
            "score": round(summary.score, 2),
            "failed_checks": int(len(issue_df)),
            "issue_counts": {k: int(v) for k, v in issue_counts.items()},
            "subscores": subscores,
        }


def _extract_archive(archive: Path, target: Path) -> None:
    with zipfile.ZipFile(archive, "r") as handle:
        handle.extractall(target)


def _load_metadata(raw_dir: Path) -> dict[str, Any]:
    metadata_path = raw_dir / "run_metadata.json"
    if not metadata_path.exists():
        return {}
    return json.loads(metadata_path.read_text())


def _load_issue_log(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def _load_expected(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"Expected regression file missing at {path}.")
    return json.loads(path.read_text())


def _write_expected(actual: dict[str, Any], path: Path) -> None:
    payload = {
        "score": actual["score"],
        "failed_checks": actual["failed_checks"],
        "issue_counts": actual["issue_counts"],
        "subscores": actual["subscores"],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _assert_matches(actual: dict[str, Any], expected: dict[str, Any], tolerance: float) -> None:
    if abs(actual["score"] - expected.get("score", 0.0)) > tolerance:
        raise SystemExit(
            f"Score mismatch: expected {expected.get('score')} vs actual {actual['score']}"
        )
    if actual["failed_checks"] != expected.get("failed_checks", -1):
        raise SystemExit(
            f"Failed-check count mismatch: expected {expected.get('failed_checks')} vs actual {actual['failed_checks']}"
        )
    expected_counts: dict[str, int] = expected.get("issue_counts", {})
    if actual["issue_counts"] != expected_counts:
        raise SystemExit(
            "Issue count mismatch: expected {} vs actual {}".format(
                expected_counts, actual["issue_counts"]
            )
        )
    expected_subscores: dict[str, float] = expected.get("subscores", {})
    missing = set(expected_subscores) - set(actual["subscores"])
    if missing:
        raise SystemExit(f"Missing subscores: {','.join(sorted(missing))}")
    for dimension, target in expected_subscores.items():
        actual_value = actual["subscores"].get(dimension)
        if actual_value is None:
            raise SystemExit(f"Subscore missing for {dimension}.")
        if abs(actual_value - target) > tolerance:
            raise SystemExit(
                f"Subscore mismatch for {dimension}: expected {target} vs actual {actual_value}"
            )


def _build_regression_metrics(
    actual: dict[str, Any], expected: dict[str, Any], tolerance: float
) -> dict[str, Any]:
    expected_subscores = expected.get("subscores", {})
    actual_subscores = actual.get("subscores", {})
    subscore_deltas = {
        dimension: round(float(actual_subscores.get(dimension, 0.0)) - float(target), 4)
        for dimension, target in expected_subscores.items()
    }
    expected_issue_counts = expected.get("issue_counts", {})
    actual_issue_counts = actual.get("issue_counts", {})
    issue_count_deltas = {
        key: int(actual_issue_counts.get(key, 0)) - int(expected_issue_counts.get(key, 0))
        for key in sorted(set(expected_issue_counts) | set(actual_issue_counts))
    }
    return {
        "status": "pending",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "run_id": actual.get("run_id"),
        "dataset_name": actual.get("dataset_name"),
        "tolerance": tolerance,
        "expected_score": expected.get("score"),
        "actual_score": actual.get("score"),
        "score_delta": round(
            float(actual.get("score", 0.0)) - float(expected.get("score", 0.0)), 4
        ),
        "expected_failed_checks": expected.get("failed_checks"),
        "actual_failed_checks": actual.get("failed_checks"),
        "failed_check_delta": int(actual.get("failed_checks", 0))
        - int(expected.get("failed_checks", 0)),
        "issue_count_deltas": issue_count_deltas,
        "subscore_deltas": subscore_deltas,
    }


def _write_regression_metrics(metrics: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
