#!/usr/bin/env python3
"""Command that delegates to the split synthetic-generator modules."""

import argparse

try:
    from tools.synthetic_cli import generate_dataset
except ModuleNotFoundError:
    from synthetic_cli import generate_dataset


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate deterministic synthetic exports with deliberate issues."
    )
    parser.add_argument(
        "--dataset-name", default="phase1", help="Subdirectory under data/raw to populate."
    )
    parser.add_argument("--seed", type=int, default=42, help="Deterministic run seed.")
    parser.add_argument("--force", action="store_true", help="Overwrite an existing run directory.")
    args = parser.parse_args()
    generate_dataset(args.dataset_name, args.seed, args.force)


if __name__ == "__main__":
    main()
