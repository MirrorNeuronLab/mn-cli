from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from mn_cli.sdk_path import add_local_sdk_path

add_local_sdk_path("runtime_config.py")

from mn_sdk.runtime_config import RuntimeConfig, default_logs_root


@dataclass(frozen=True)
class CliConfig:
    grpc_target: str = "localhost:55051"
    grpc_timeout_seconds: float | None = 10.0
    grpc_auth_token: str = ""
    grpc_admin_token: str = ""
    log_path: Path = default_logs_root() / "cli.log"
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
                    str(default_logs_root() / "cli.log"),
                )
            ).expanduser(),
            output_mode=os.getenv("MN_CLI_OUTPUT", "rich"),
        )
