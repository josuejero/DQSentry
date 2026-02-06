#!/usr/bin/env python3
"""Print the run identifier stored in a staging run metadata file."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Return the run_id stored under a stage directory."
    )
    parser.add_argument(
        "--stage-path",
        required=True,
        help="Path to staging data that contains run_metadata.json",
    )
    args = parser.parse_args()

    metadata_path = Path(args.stage_path) / "run_metadata.json"
    if not metadata_path.exists():
        raise SystemExit(f"Missing run metadata at {metadata_path}")

    metadata = json.loads(metadata_path.read_text())
    run_id = metadata.get("run_id")
    if not run_id:
        raise SystemExit(f"run_id missing in {metadata_path}")

    print(run_id)


if __name__ == "__main__":
    main()
