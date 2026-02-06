"""Orchestrates CSV writing and metadata exports for the synthetic data generator."""

import csv
import json
import shutil
import textwrap
from datetime import datetime, timezone
from pathlib import Path

try:
    from tools.synthetic_builder import (
        build_districts,
        build_events,
        build_newsletter,
        build_resources,
        build_users,
        stable_id,
    )
except ModuleNotFoundError:
    from synthetic_builder import (
        build_districts,
        build_events,
        build_newsletter,
        build_resources,
        build_users,
        stable_id,
    )

REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_BASE = REPO_ROOT / "data" / "raw"

FIELD_SETS = {
    "districts.csv": ("district_id", "district_name", "state"),
    "users.csv": ("user_id", "email", "org_id", "role", "state", "district_id"),
    "resources.csv": ("resource_id", "type", "subject", "grade_band"),
    "events.csv": ("event_id", "user_id", "resource_id", "event_type", "event_ts"),
    "newsletter.csv": ("email", "subscribed_at", "opened_at", "clicked_at"),
}

DISTRICT_KEYS = ("nyc", "northside", "southvalley", "sunrise")
USER_KEYS = ("alice", "bob", "carol", "dax", "eve", "frank", "glenda")
RESOURCE_KEYS = (
    "math-fundamentals",
    "capstone-think",
    "science-explorers",
    "pathway-start",
    "missing-resource",
)


def write_csv(path: Path, columns: tuple[str, ...], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {col: (row.get(col) if row.get(col) is not None else "") for col in columns}
            )


def generate_dataset(dataset_name: str, seed: int, force: bool = False) -> dict:
    raw_path = RAW_BASE / dataset_name / str(seed)
    if raw_path.exists():
        if force:
            shutil.rmtree(raw_path)
        else:
            raise SystemExit(
                f"Run directory {raw_path} already exists. Use --force to overwrite."
            )
    raw_path.mkdir(parents=True, exist_ok=True)

    district_rows = build_districts(dataset_name, seed)
    district_ids = {
        key: stable_id("district", key, dataset_name, seed) for key in DISTRICT_KEYS
    }
    user_rows = build_users(dataset_name, seed, district_ids)
    resource_rows = build_resources(dataset_name, seed)
    user_id_map = {
        key: stable_id("user", key, dataset_name, seed) for key in USER_KEYS
    }
    resource_id_map = {
        key: stable_id("resource", key, dataset_name, seed) for key in RESOURCE_KEYS
    }
    event_rows = build_events(dataset_name, seed, user_id_map, resource_id_map)
    newsletter_rows = build_newsletter(dataset_name, seed)

    data_map = {
        "districts.csv": district_rows,
        "users.csv": user_rows,
        "resources.csv": resource_rows,
        "events.csv": event_rows,
        "newsletter.csv": newsletter_rows,
    }
    for csv_name, rows in data_map.items():
        write_csv(raw_path / csv_name, FIELD_SETS[csv_name], rows)

    metadata = {
        "dataset_name": dataset_name,
        "seed": seed,
        "run_id": stable_id("run", str(seed), dataset_name, seed),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    (raw_path / "run_metadata.json").write_text(json.dumps(metadata, indent=2))

    print(
        textwrap.dedent(
            f"""\
            Generated synthetic dataset "{dataset_name}" with seed {seed}.
            Raw exports written under {raw_path}.
            Run ID: {metadata['run_id']}.
            """
        ).strip()
    )
    return metadata
