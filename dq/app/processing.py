"""Helpers for staging uploads and running the ingest/validation pipeline."""

from __future__ import annotations

import io
import os
import zipfile
from pathlib import Path
from typing import Iterable

import pandas as pd
from streamlit.runtime.uploaded_file_manager import UploadedFile

from scripts.ingest_lib import ingest_dataset
from scripts.ingest_tables import TABLE_SPECS
from dq.validate.models import ValidationSummary
from dq.validate.runner import ValidationRunner

REQUIRED_SOURCES = {spec["source"] for spec in TABLE_SPECS}


def _find_dataset_root(base: Path) -> Path | None:
    for dirpath, _, filenames in os.walk(base):
        if REQUIRED_SOURCES.issubset(set(filenames)):
            return Path(dirpath)
    return None


def _extract_archive(source: Path, target: Path) -> None:
    with zipfile.ZipFile(source, "r") as archive:
        archive.extractall(target)


def _stage_uploaded_files(uploads: Iterable[UploadedFile], target: Path) -> None:
    for upload in uploads:
        data = upload.read()
        if upload.name.lower().endswith(".zip"):
            with zipfile.ZipFile(io.BytesIO(data)) as archive:
                archive.extractall(target)
        else:
            (target / upload.name).write_bytes(data)


def prepare_raw_source(
    temp_root: Path, sample_archive: Path | None, uploads: list[UploadedFile] | None
) -> Path:
    raw_dir = temp_root / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    if sample_archive and sample_archive.exists():
        _extract_archive(sample_archive, raw_dir)
    if uploads:
        _stage_uploaded_files(uploads, raw_dir)
    dataset_root = _find_dataset_root(raw_dir)
    if dataset_root is None:
        raise ValueError(
            "Could not locate the required CSV exports. Provide the five DQSentry exports"
            " (districts, users, resources, events, newsletter) either zipped or uploaded together."
        )
    return dataset_root


def _build_cleaned_archive(stage_path: Path) -> bytes:
    parquet_dir = stage_path / "parquet"
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for file_path in sorted(parquet_dir.glob("*.parquet")):
            archive.write(file_path, arcname=file_path.name)
    buffer.seek(0)
    return buffer.read()


def _build_exceptions_csv(issue_df: pd.DataFrame) -> bytes:
    columns = [
        "check_name",
        "table_name",
        "dimension",
        "issue_type",
        "severity",
        "affected_rows",
        "affected_pct",
        "probable_root_cause",
        "recommended_fix",
        "sample_bad_rows_json",
    ]
    available = [col for col in columns if col in issue_df.columns]
    target = issue_df[available].copy()
    return target.to_csv(index=False).encode("utf-8")


def run_validation_pipeline(
    dataset_name: str, raw_root: Path, stage_root: Path, run_id: str
) -> tuple[pd.DataFrame, bytes, bytes, bytes, ValidationSummary]:
    stage_root.mkdir(parents=True, exist_ok=True)
    ingest_paths = ingest_dataset(
        dataset_name=dataset_name,
        seed=0,
        force=True,
        raw_path=raw_root,
        stage_path=stage_root,
        run_id=run_id,
    )
    stage_path = Path(ingest_paths["stage_path"])
    runner = ValidationRunner(
        dataset_name,
        run_id,
        stage_path,
        Path(ingest_paths["db_path"]),
    )
    summary = runner.run()
    issue_df = pd.read_parquet(summary.issue_log_path)
    cleaned = _build_cleaned_archive(stage_path)
    issues_csv = issue_df.to_csv(index=False).encode("utf-8")
    exceptions_csv = _build_exceptions_csv(issue_df)
    return issue_df, cleaned, issues_csv, exceptions_csv, summary
