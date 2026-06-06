from __future__ import annotations

import inspect
import sys
from pathlib import Path

for parent in Path(__file__).resolve().parents:
    sdk_path = parent / "mn-python-sdk"
    if (sdk_path / "mn_sdk" / "client.py").exists() and str(sdk_path) not in sys.path:
        sys.path.insert(0, str(sdk_path))
        break

from mn_sdk import Client
from rich.console import Console
from mn_cli.config import CliConfig
from mn_cli.logging_config import configure_logging
from mn_cli.terminal import color_disabled

config = CliConfig.from_env()
logger = configure_logging("mn-cli", config.log_path)
console = Console(no_color=color_disabled(config.output_mode), width=200)


def _client_kwargs() -> dict:
    kwargs = {
        "target": config.grpc_target,
        "timeout": config.grpc_timeout_seconds,
        "auth_token": config.grpc_auth_token,
    }
    try:
        client_params = inspect.signature(Client).parameters
    except (TypeError, ValueError):
        client_params = {}
    if "admin_token" in client_params:
        kwargs["admin_token"] = config.grpc_admin_token
    elif config.grpc_admin_token:
        logger.warning(
            "mn_sdk.Client does not accept admin_token; upgrade mirrorneuron-python-sdk "
            "for destructive admin RPC support."
        )
    return kwargs


client = Client(**_client_kwargs())
if not hasattr(client, "remove_node"):
    setattr(client, "remove_node", lambda node_name: (_ for _ in ()).throw(AttributeError("remove_node is unavailable in this SDK")))
