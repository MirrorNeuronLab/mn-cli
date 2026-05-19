from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class CliConfig:
    grpc_target: str = "localhost:50051"
    grpc_timeout_seconds: float | None = 10.0
    grpc_auth_token: str = ""
    log_path: Path = Path.home() / ".mn" / "logs" / "cli.log"
    output_mode: str = "rich"

    @classmethod
    def from_env(cls) -> "CliConfig":
        core_host = os.getenv("MN_CORE_HOST", "localhost")
        return cls(
            grpc_target=os.getenv(
                "MN_GRPC_TARGET",
                os.getenv("MN_CORE_GRPC_TARGET", f"{core_host}:50051"),
            ),
            grpc_timeout_seconds=_timeout(),
            grpc_auth_token=_grpc_auth_token(),
            log_path=Path(
                os.getenv(
                    "MN_CLI_LOG_PATH",
                    str(Path.home() / ".mn" / "logs" / "cli.log"),
                )
            ).expanduser(),
            output_mode=os.getenv("MN_CLI_OUTPUT", "rich"),
        )


def _timeout() -> float | None:
    value = os.getenv("MN_GRPC_TIMEOUT_SECONDS", "10")
    if value.lower() in {"", "none", "0"}:
        return None
    try:
        return float(value)
    except ValueError as exc:
        raise ValueError("MN_GRPC_TIMEOUT_SECONDS must be a number, 0, or none") from exc


def _grpc_auth_token() -> str:
    token = os.getenv("MN_GRPC_AUTH_TOKEN")
    if token:
        return token

    try:
        return (Path.home() / ".mirror_neuron" / "grpc_auth.token").read_text().strip()
    except OSError:
        return ""
