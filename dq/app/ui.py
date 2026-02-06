"""Streamlit UI surface for the DQSentry CSV validator."""

from __future__ import annotations

import uuid
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Optional

import pandas as pd
import streamlit as st
from streamlit.runtime.uploaded_file_manager import UploadedFile

from dq.app.processing import prepare_raw_source, run_validation_pipeline
from dq.validate.models import ValidationSummary

APP_ROOT = Path(__file__).resolve().parent
ASSETS_DIR = APP_ROOT / "assets"

SAMPLE_DATASETS = [
    {
        "id": "phase1-demo",
        "label": "Phase 1 synthetic demo (seed 42)",
        "description": "Same CSV exports that power the automated validations.",
        "archive": ASSETS_DIR / "sample_dataset.zip",
        "dataset_name": "phase1",
    }
]


def _render_results(
    summary: ValidationSummary,
    issue_df: pd.DataFrame,
    cleaned_bytes: bytes,
    issues_csv: bytes,
    exceptions_csv: bytes,
) -> None:
    st.success(
        f"Validation complete · run_id={summary.run_id} · dataset={summary.dataset_name}"
    )
    score_display = f"{summary.score:.1f}/100"
    cols = st.columns([1, 3, 1])
    cols[0].metric("Overall score", score_display)
    subscores = summary.subscores or {}
    if subscores:
        subscore_df = (
            pd.DataFrame(subscores.items(), columns=["dimension", "score"])
            .assign(score=lambda df: df["score"].round(2))
            .sort_values("score", ascending=False)
        )
        cols[1].bar_chart(subscore_df.set_index("dimension")["score"])
    cols[2].write(f"Run ID: {summary.run_id}")

    st.subheader("Issue log")
    if issue_df.empty:
        st.info("No issues detected. All checks passed cleanly.")
    else:
        display_columns = [
            col
            for col in [
                "table_name",
                "check_name",
                "dimension",
                "issue_type",
                "severity",
                "affected_rows",
                "affected_pct",
                "recommended_fix",
            ]
            if col in issue_df.columns
        ]
        st.dataframe(
            issue_df.sort_values(
                ["severity", "affected_pct"], ascending=[False, False]
            )[display_columns],
            use_container_width=True,
        )
    with st.expander("Download artifacts", expanded=True):
        st.download_button(
            "Download issues.csv",
            issues_csv,
            file_name="issues.csv",
            mime="text/csv",
        )
        st.download_button(
            "Download exceptions.csv",
            exceptions_csv,
            file_name="exceptions.csv",
            mime="text/csv",
        )
        st.download_button(
            "Download cleaned dataset (.zip)",
            cleaned_bytes,
            file_name="cleaned_dataset.zip",
            mime="application/zip",
        )


def main() -> None:
    st.set_page_config(page_title="DQSentry CSV validator", layout="wide")
    st.title("DQSentry CSV validator")
    st.write(
        "Upload your CSV exports or use the sample dataset to see the DQSentry "
        "validation score, issue log, and download-ready clean artifacts."
    )

    available_samples = [sample for sample in SAMPLE_DATASETS if sample["archive"].exists()]
    sample_options = ["(None)"] + [sample["label"] for sample in available_samples]
    default_index = 1 if len(sample_options) > 1 else 0
    selected_label = st.selectbox("Built-in sample dataset", sample_options, index=default_index)
    selected_sample = next(
        (sample for sample in available_samples if sample["label"] == selected_label), None
    )
    if selected_sample:
        st.caption(selected_sample["description"])

    uploaded_files: Optional[list[UploadedFile]] = st.file_uploader(
        "Upload CSV files or a ZIP archive",
        type=["csv", "zip"],
        accept_multiple_files=True,
    )

    if st.button("Run validations"):
        if not uploaded_files and not selected_sample:
            st.warning("Please upload data or select a sample dataset first.")
            return
        with st.spinner("Staging uploads and running validations..."):
            try:
                with TemporaryDirectory(prefix="dqsentry-") as tmpdir:
                    temp_root = Path(tmpdir)
                    raw_root = prepare_raw_source(
                        temp_root,
                        selected_sample["archive"] if selected_sample else None,
                        uploaded_files,
                    )
                    dataset_name = selected_sample["dataset_name"] if selected_sample else "streamlit-upload"
                    run_id = f"streamlit-{uuid.uuid4().hex[:8]}"
                    stage_root = temp_root / "stage"
                    issue_df, cleaned_bytes, issues_csv, exceptions_csv, summary = (
                        run_validation_pipeline(dataset_name, raw_root, stage_root, run_id)
                    )
                _render_results(summary, issue_df, cleaned_bytes, issues_csv, exceptions_csv)
            except ValueError as exc:
                st.error(str(exc))
            except Exception as exc:
                st.exception(exc)
