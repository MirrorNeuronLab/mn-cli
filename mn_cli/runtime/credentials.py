"""Installer-compatible, per-installation Core credentials."""
import os
from pathlib import Path
import secrets

_LEGACY = {"mirror_neuron_password", "mirror_neuron_password_admin"}


def resolve_grpc_token(home: Path, kind: str) -> str:
    path = home / f"grpc_{kind}.token"
    try:
        stored = path.read_text().strip()
    except FileNotFoundError:
        stored = ""
    if stored and stored not in _LEGACY:
        return stored
    explicit = os.environ.get(f"MN_GRPC_{kind.upper()}_TOKEN", "").strip()
    token = explicit if explicit and explicit not in _LEGACY else secrets.token_hex(32)
    home.mkdir(parents=True, exist_ok=True)
    # Resolve concurrent first starts under one installation lock.
    from mn_cli.runtime.identity import identity_lock
    with identity_lock(home):
        try:
            current = path.read_text().strip()
        except FileNotFoundError:
            current = ""
        if current and current not in _LEGACY:
            return current
        # Credential files retain the existing plain-token wire format.
        temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        try:
            with open(temporary, "w", opener=lambda name, flags: os.open(name, flags, 0o600)) as handle:
                handle.write(token + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)
    return token
