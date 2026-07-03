from __future__ import annotations

from pathlib import Path
from typing import Union

from mn_sdk import expand_manifest_json_if_source

BundlePath = Union[str, Path]


def read_bundle(path: BundlePath) -> tuple[str, dict[str, bytes]]:
    root = Path(path).expanduser()
    manifest_path = root / "manifest.json" if root.is_dir() else root
    manifest_json = manifest_path.read_text(encoding="utf-8")
    bundle_root = root if root.is_dir() else manifest_path.parent
    manifest_json = expand_manifest_json_if_source(manifest_json, root_dir=bundle_root)
    return manifest_json, load_bundle_payloads(bundle_root)


def load_bundle_payloads(bundle_root: BundlePath) -> dict[str, bytes]:
    root = Path(bundle_root).expanduser()
    payloads_dir = root / "payloads"
    payloads: dict[str, bytes] = {}
    if not payloads_dir.is_dir():
        return payloads

    for payload_path in payloads_dir.rglob("*"):
        if not payload_path.is_file():
            continue
        relative = payload_path.relative_to(payloads_dir).as_posix()
        if not _safe_payload_path(relative):
            raise ValueError(f"Unsafe payload path: {relative}")
        payloads[relative] = payload_path.read_bytes()
    return payloads


def _safe_payload_path(path: str) -> bool:
    candidate = Path(path)
    return (
        not candidate.is_absolute()
        and path not in ("", ".")
        and ".." not in candidate.parts
    )
