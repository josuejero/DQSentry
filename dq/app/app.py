"""Entrypoint that wires Streamlit to the DQSentry UI module."""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

DATA_DIR = REPO_ROOT / "data"

from dq.app.ui import main


if __name__ == "__main__":
    main()
