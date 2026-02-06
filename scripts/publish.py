#!/usr/bin/env python3
"""Render the DQ Scorecard HTML site from persisted assets."""

from __future__ import annotations

import argparse

from scripts.publish_helpers import (
    LATEST_REPORT_DIR,
    copy_to_run_directory,
    load_score_payload,
    mutate_context,
    render_scorecard,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Render the latest DQ scorecard website.")
    parser.add_argument(
        "--run-id",
        help="Optional override for the run identifier used when copying to reports/runs.",
    )
    args = parser.parse_args()

    LATEST_REPORT_DIR.mkdir(parents=True, exist_ok=True)
    score_path = LATEST_REPORT_DIR / "score.json"
    payload = load_score_payload(score_path)
    context = mutate_context(payload)
    html = render_scorecard(context)
    (LATEST_REPORT_DIR / "index.html").write_text(html, encoding="utf-8")

    run_identifier = args.run_id or payload.get("run_id")
    if run_identifier:
        copy_to_run_directory(run_identifier)

    display_id = run_identifier or "latest"
    print(f"Scorecard rendered to {LATEST_REPORT_DIR / 'index.html'} for run {display_id}.")


if __name__ == "__main__":
    main()
