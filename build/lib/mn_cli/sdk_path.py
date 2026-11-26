from __future__ import annotations

import sys
from pathlib import Path


def add_local_sdk_path(marker_file: str) -> None:
    for parent in Path(__file__).resolve().parents:
        sdk_path = parent / "mn-python-sdk"
        if (sdk_path / "mn_sdk" / marker_file).exists() and str(sdk_path) not in sys.path:
            sys.path.insert(0, str(sdk_path))
            break
