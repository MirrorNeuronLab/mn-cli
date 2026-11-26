from __future__ import annotations

from pathlib import Path
from typing import Union

from mn_sdk.bundle_io import (
    load_bundle_payloads,
    read_bundle,
    safe_payload_path as _safe_payload_path,
)


BundlePath = Union[str, Path]


__all__ = [
    "BundlePath",
    "load_bundle_payloads",
    "read_bundle",
    "_safe_payload_path",
]
