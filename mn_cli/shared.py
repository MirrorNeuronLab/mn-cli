from __future__ import annotations

import inspect

from mn_cli.sdk_path import add_local_sdk_path

add_local_sdk_path("client.py")

from mn_sdk import Client
from rich.console import Console
from mn_cli.config import CliConfig
from mn_cli.logging_config import configure_logging
from mn_cli.terminal import color_disabled

config = CliConfig.from_env()
logger = configure_logging("mn-cli", config.log_path)
console = Console(no_color=color_disabled(config.output_mode))


def _client_kwargs(cli_config: CliConfig | None = None) -> dict:
    cli_config = cli_config or config
    kwargs = {
        "target": cli_config.grpc_target,
        "timeout": cli_config.grpc_timeout_seconds,
        "auth_token": cli_config.grpc_auth_token,
    }
    try:
        client_params = inspect.signature(Client).parameters
    except (TypeError, ValueError):
        client_params = {}
    if "admin_token" in client_params:
        kwargs["admin_token"] = cli_config.grpc_admin_token
    elif cli_config.grpc_admin_token:
        logger.warning(
            "mn_sdk.Client does not accept admin_token; upgrade mirrorneuron-python-sdk "
            "for destructive admin RPC support."
        )
    return kwargs


def build_client(cli_config: CliConfig | None = None) -> Client:
    instance = Client(**_client_kwargs(cli_config))
    _ensure_client_compat(instance)
    return instance


def _ensure_client_compat(instance: Client) -> None:
    if not hasattr(instance, "remove_node"):
        setattr(
            instance,
            "remove_node",
            lambda node_name: (_ for _ in ()).throw(AttributeError("remove_node is unavailable in this SDK")),
        )


client = build_client(config)
