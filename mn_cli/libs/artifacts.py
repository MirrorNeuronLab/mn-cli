from __future__ import annotations

import hashlib
import mimetypes
import os
import socket
from pathlib import Path
from typing import Any

from mn_cli.runtime_state import read_env_file

DEFAULT_INLINE_PAYLOAD_MAX_BYTES = 1_048_576
DEFAULT_ARTIFACT_PORT = "55660"


def promote_large_payloads_to_blob_refs(
    manifest: dict[str, Any],
    payloads: dict[str, bytes],
    *,
    runtime_env: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    threshold = _inline_payload_max_bytes()
    if threshold < 0:
        return []

    env = _runtime_env_file_values()
    env.update(os.environ)
    env.update(runtime_env or {})
    root = _host_blob_store_root(env)
    promoted: list[dict[str, Any]] = []

    for rel_path, contents in list(payloads.items()):
        if len(contents) <= threshold:
            continue

        blob_ref = _store_payload_blob(root, rel_path, contents, env)
        promoted.append(blob_ref)
        del payloads[rel_path]

    if promoted:
        metadata = manifest.setdefault("metadata", {})
        artifacts = metadata.setdefault("mn_artifacts", {})
        artifacts.setdefault("blob_refs", []).extend(promoted)

    return promoted


def _store_payload_blob(
    root: Path,
    rel_path: str,
    contents: bytes,
    env: dict[str, str],
) -> dict[str, Any]:
    sha256 = hashlib.sha256(contents).hexdigest()
    target = root / sha256[:2] / sha256
    target.parent.mkdir(parents=True, exist_ok=True)

    if not target.exists():
        tmp = target.with_name(f"{target.name}.tmp-{os.getpid()}")
        tmp.write_bytes(contents)
        os.replace(tmp, target)

    media_type, _encoding = mimetypes.guess_type(rel_path)
    location = _blob_location(sha256, env)

    blob_ref: dict[str, Any] = {
        "type": "blob_ref",
        "sha256": sha256,
        "size_bytes": len(contents),
        "media_type": media_type or "application/octet-stream",
        "logical_name": Path(rel_path).name,
        "scope": "job",
        "payload_path": rel_path.replace("\\", "/"),
    }

    if location:
        blob_ref["locations"] = [location]

    return blob_ref


def _blob_location(sha256: str, env: dict[str, str]) -> dict[str, str] | None:
    base_url = str(env.get("MN_ARTIFACT_ADVERTISE_URL") or os.getenv("MN_ARTIFACT_ADVERTISE_URL") or "").strip()
    if not base_url:
        host = (
            str(env.get("MN_NETWORK_ADVERTISE_HOST") or os.getenv("MN_NETWORK_ADVERTISE_HOST") or "").strip()
            or _detect_lan_ip()
        )
        port = str(env.get("MN_ARTIFACT_PORT") or os.getenv("MN_ARTIFACT_PORT") or DEFAULT_ARTIFACT_PORT).strip()
        if not host or not port:
            return None
        base_url = f"http://{host}:{port}"

    location = {
        "url": f"{base_url.rstrip('/')}/blobs/{sha256}",
        "status": "available",
    }
    node = str(env.get("MN_NODE_NAME") or os.getenv("MN_NODE_NAME") or "").strip()
    if node:
        location["node"] = node
    return location


def _host_blob_store_root(env: dict[str, str]) -> Path:
    configured = (
        env.get("MN_HOST_BLOB_STORE_DIR")
        or os.getenv("MN_HOST_BLOB_STORE_DIR")
        or env.get("MN_BLOB_STORE_ROOT")
        or os.getenv("MN_BLOB_STORE_ROOT")
    )
    if configured:
        return Path(configured).expanduser()
    return Path.home() / ".mn" / "blobs"


def _runtime_env_file_values() -> dict[str, str]:
    home = Path(os.getenv("MN_HOME") or Path.home() / ".mn")
    env_file = home.expanduser() / "docker-compose.env"
    return {key.strip(): value.strip() for key, value in read_env_file(env_file).items()}


def _inline_payload_max_bytes() -> int:
    value = os.getenv("MN_INLINE_PAYLOAD_MAX_BYTES", str(DEFAULT_INLINE_PAYLOAD_MAX_BYTES))
    try:
        return int(value)
    except (TypeError, ValueError):
        return DEFAULT_INLINE_PAYLOAD_MAX_BYTES


def _detect_lan_ip() -> str:
    probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        probe.connect(("8.8.8.8", 80))
        return probe.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        probe.close()
