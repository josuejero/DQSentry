#!/usr/bin/env python3
"""Entry point that orchestrates standardizing raw exports via the ingest library."""

import argparse
import textwrap

try:
    from scripts.ingest_lib import ingest_dataset
except ModuleNotFoundError:
    from ingest_lib import ingest_dataset


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ingest raw CSV exports into cleaned staging tables and Parquet."
    )
    parser.add_argument("--dataset-name", default="phase1", help="Raw dataset folder.")
    parser.add_argument("--seed", type=int, default=42, help="Matching synthetic run seed.")
    parser.add_argument(
        "--force", action="store_true", help="Remove any existing staging output."
    )
    args = parser.parse_args()

    paths = ingest_dataset(args.dataset_name, args.seed, args.force)

    print(
        textwrap.dedent(
            f"""\
            Ingested {args.dataset_name} (seed={args.seed}) into {paths['stage_path']}.
            DuckDB file: {paths['db_path']}
            Parquet exports: {paths['parquet_path']}
            """
        ).strip()
    )


if __name__ == "__main__":
    main()
