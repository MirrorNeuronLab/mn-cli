from __future__ import annotations

import os
import json
from dataclasses import dataclass
from pathlib import Path


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
        mn_home = _mn_home()
        runtime_env = _read_env_file(mn_home / "docker-compose.env")
        runtime_endpoint_target = _read_runtime_grpc_target(mn_home / "runtime-endpoints.json")
        core_host = os.getenv("MN_CORE_HOST") or runtime_env.get("MN_CORE_HOST") or "localhost"
        grpc_port = os.getenv("MN_GRPC_PORT") or runtime_env.get("MN_GRPC_PORT") or "55051"
        return cls(
            grpc_target=os.getenv(
                "MN_GRPC_TARGET",
                os.getenv(
                    "MN_CORE_GRPC_TARGET",
                    runtime_env.get("MN_GRPC_TARGET")
                    or runtime_endpoint_target
                    or runtime_env.get("MN_CORE_GRPC_TARGET")
                    or f"{core_host}:{grpc_port}",
                ),
            ),
            grpc_timeout_seconds=_timeout(),
            grpc_auth_token=_grpc_auth_token(runtime_env),
            grpc_admin_token=_grpc_admin_token(runtime_env),
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
