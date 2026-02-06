"""IO helpers for the publish helpers package."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_score_payload(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"Missing score payload at {path}")
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)
