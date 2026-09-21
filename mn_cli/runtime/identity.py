"""Persistent identity for an independently federated desktop runtime."""
from __future__ import annotations

import fcntl
import json
import os
import re
import tempfile
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

NAME = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.-]*@[A-Za-z0-9][A-Za-z0-9_.-]*\Z")


def valid_name(value: object) -> bool:
    return isinstance(value, str) and value != "nonode@nohost" and bool(NAME.fullmatch(value))


def atomic_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as handle:
            json.dump(data, handle, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        Path(temporary).unlink(missing_ok=True)


@contextmanager
def identity_lock(home: Path) -> Iterator[None]:
    home.mkdir(parents=True, exist_ok=True)
    fd = os.open(home / "node-identity.lock", os.O_CREAT | os.O_RDWR, 0o600)
    os.fchmod(fd, 0o600)
    with os.fdopen(fd, "w") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def read_identity(home: Path) -> str:
    path = home / "node-identity.json"
    try:
        record = json.loads(path.read_text())
    except FileNotFoundError:
        return ""
    except (OSError, ValueError) as exc:
        raise RuntimeError("Cannot read node identity. Restore node-identity.json from backup; do not reset job data.") from exc
    if not isinstance(record, dict) or record.get("version") != 1 or not valid_name(record.get("node_name")):
        raise RuntimeError("Invalid node-identity.json. Restore the original identity before starting Core.")
    return record["node_name"]


def resolve_identity(home: Path, *, configured: str = "", explicit: str = "", observed: str = "", runtime_name: str = "") -> str:
    """Adopt consistent evidence; never reinterpret an existing name as an address."""
    with identity_lock(home):
        persisted = read_identity(home)
        evidence = []
        for source, value in (("saved identity", persisted), ("configuration", configured),
                              ("MN_NODE_NAME", explicit), ("container", observed), ("running Core", runtime_name)):
            value = str(value or "").strip()
            if not value or (value == "nonode@nohost" and source != "MN_NODE_NAME"):
                continue
            if not valid_name(value):
                raise RuntimeError(f"Invalid node name in {source}. Restore the original configuration before starting Core.")
            evidence.append(value)
        if len(set(evidence)) > 1:
            raise RuntimeError("Conflicting node identities. Restore the original MN_NODE_NAME/configuration; job ownership must not be renamed automatically.")
        name = evidence[0] if evidence else f"mirror_neuron_{uuid.uuid4().hex}@127.0.0.1"
        if not persisted:
            atomic_json(home / "node-identity.json", {"version": 1, "node_name": name})
        return name


def identity_health(expected: str, actual: str) -> dict:
    valid = valid_name(expected) and valid_name(actual) and expected == actual
    return {"expected": expected, "actual": actual, "valid": valid}


def observe_core_identity(env: dict, *, client_factory=None) -> str | None:
    from mn_sdk.client import Client
    client = (client_factory or Client)(
        target=env.get("MN_GRPC_TARGET") or f"localhost:{env.get('MN_GRPC_PORT') or 55051}",
        timeout=2, auth_token=env.get("MN_GRPC_AUTH_TOKEN"),
        admin_token=env.get("MN_GRPC_ADMIN_TOKEN"),
    )
    try:
        raw = client.get_system_summary()
        summary = json.loads(raw) if isinstance(raw, str) else raw
        if not isinstance(summary, dict):
            return ""
        local = [node for node in summary.get("nodes", []) if isinstance(node, dict)
                 and (node.get("self") is True or node.get("self?") is True)]
        actual = str(local[0].get("name") or local[0].get("node") or "") if len(local) == 1 else ""
        return actual
    except Exception:
        return None
    finally:
        client.channel.close()


def probe_core_identity(env: dict, *, client_factory=None) -> bool | None:
    actual = observe_core_identity(env, client_factory=client_factory)
    if actual is None:
        return None
    return identity_health(str(env.get("MN_NODE_NAME") or ""), actual)["valid"]
