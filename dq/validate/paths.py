"""Path helpers for validation outputs."""

from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_MARTS_BASE = REPO_ROOT / "data" / "marts"
GE_ARTIFACTS_BASE = REPO_ROOT / "dq" / "ge"
