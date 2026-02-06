"""Copy helpers used by the publish helpers package."""

from __future__ import annotations

import shutil

from .constants import LATEST_REPORT_DIR, RUNS_REPORT_DIR


def copy_to_run_directory(run_id: str) -> None:
    run_dir = RUNS_REPORT_DIR / f"run_id={run_id}"
    run_dir.mkdir(parents=True, exist_ok=True)
    for artifact in ("index.html", "score.json", "issues.csv"):
        src = LATEST_REPORT_DIR / artifact
        if not src.exists():
            continue
        shutil.copy(src, run_dir / artifact)
