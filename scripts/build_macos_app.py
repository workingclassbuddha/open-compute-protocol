#!/usr/bin/env python3
"""Build the unsigned macOS OCP.app beta bundle."""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ocp_desktop.macos_app import main


if __name__ == "__main__":
    raise SystemExit(main())
