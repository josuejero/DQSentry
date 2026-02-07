#!/usr/bin/env python3
"""Prevent unsupported Python versions that trigger PyArrow source builds."""
import sys

MIN = (3, 9)
MAX = (3, 13)
version_info = sys.version_info
major_minor = (version_info.major, version_info.minor)
if version_info.major != 3 or not (MIN <= major_minor <= MAX):
    message = (
        f"error: Python {version_info.major}.{version_info.minor}.{version_info.micro} is not supported. "
        "PyArrow 18 ships wheels for CPython 3.9–3.13 only, so attempting to install with 3.14+ triggers a source build and fails.\n"
        "Install a 3.13 or earlier interpreter (pyenv/asdf/asdf install) and rerun setup.\n"
    )
    sys.stderr.write(message)
    sys.exit(1)
