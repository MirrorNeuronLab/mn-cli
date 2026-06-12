from __future__ import annotations

import os
import json
import sys
from dataclasses import dataclass
from pathlib import Path

for parent in Path(__file__).resolve().parents:
    sdk_path = parent / "mn-python-sdk"
    if (sdk_path / "mn_sdk" / "runtime_config.py").exists() and str(sdk_path) not in sys.path:
        sys.path.insert(0, str(sdk_path))
        break

from mn_sdk.runtime_config import RuntimeConfig


@dataclass(frozen=True)
class CliConfig:
    grpc_target: str = "localhost:55051"
    grpc_timeout_seconds: float | None = 10.0
    grpc_auth_token: str = ""
    grpc_admin_token: str = ""
    log_path: Path = Path.home() / ".mn" / "logs" / "cli.log"
    output_mode: str = "rich"

    @classmethod
    def from_env(cls) -> "CliConfig":
        runtime_config = RuntimeConfig.from_env()
        return cls(
            grpc_target=runtime_config.grpc_target,
            grpc_timeout_seconds=runtime_config.grpc_timeout_seconds,
            grpc_auth_token=runtime_config.grpc_auth_token,
            grpc_admin_token=runtime_config.grpc_admin_token,
            log_path=Path(
                os.getenv(
                    "MN_CLI_LOG_PATH",
                    str(Path.home() / ".mn" / "logs" / "cli.log"),
                )
            ).expanduser(),
            output_mode=os.getenv("MN_CLI_OUTPUT", "rich"),
        )


def _mn_home() -> Path:
    configured_home = os.getenv("MN_HOME") or os.getenv("MIRROR_NEURON_HOME")
    return Path(configured_home).expanduser() if configured_home else Path.home() / ".mn"


def _read_env_file(path: Path) -> dict[str, str]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return {}

    values: dict[str, str] = {}
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        values[key] = value
    return values


def _read_runtime_grpc_target(path: Path) -> str:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return ""

    grpc = payload.get("grpc") if isinstance(payload, dict) else None
    target = grpc.get("target") if isinstance(grpc, dict) else ""
    return str(target or "").strip()


def _timeout() -> float | None:
    value = os.getenv("MN_GRPC_TIMEOUT_SECONDS", "10")
    if value.lower() in {"", "none", "0"}:
        return None
    try:
        return float(value)
    except ValueError as exc:
        raise ValueError("MN_GRPC_TIMEOUT_SECONDS must be a number, 0, or none") from exc


def _grpc_auth_token(runtime_env: dict[str, str] | None = None) -> str:
    token = os.getenv("MN_GRPC_AUTH_TOKEN")
    if token:
        return token

    token = _read_configured_token_file("MN_GRPC_AUTH_TOKEN_FILE") or _read_token_file("grpc_auth.token")
    if token:
        return token

    return (runtime_env or {}).get("MN_GRPC_AUTH_TOKEN", "")


def _grpc_admin_token(runtime_env: dict[str, str] | None = None) -> str:
    token = os.getenv("MN_GRPC_ADMIN_TOKEN") or os.getenv("MN_MIRROR_NEURON_GRPC_ADMIN_TOKEN")
    if token:
        return token

    token = _read_configured_token_file("MN_GRPC_ADMIN_TOKEN_FILE") or _read_token_file("grpc_admin.token")
    if token:
        return token

    return (runtime_env or {}).get("MN_GRPC_ADMIN_TOKEN", "") or (runtime_env or {}).get(
        "MN_MIRROR_NEURON_GRPC_ADMIN_TOKEN", ""
    )


def _read_configured_token_file(env_name: str) -> str:
    path = os.getenv(env_name)
    if not path:
        return ""
    try:
        return Path(path).expanduser().read_text(encoding="utf-8").strip()
    except OSError:
        return ""


def _read_token_file(name: str) -> str:
    for token_file in (_mn_home() / name, Path.home() / ".mirror_neuron" / name):
        try:
            token = token_file.read_text().strip()
        except OSError:
            continue
        if token:
            return token
    return ""
