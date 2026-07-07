from __future__ import annotations

from mn_cli.sdk_path import add_local_sdk_path

add_local_sdk_path("client.py")

from mn_sdk import Client
from mn_sdk.runtime_client import runtime_client_kwargs
from rich.console import Console
from mn_cli.config import CliConfig
from mn_cli.logging_config import configure_logging
from mn_cli.terminal import color_disabled

config = CliConfig.from_env()
logger = configure_logging("mn-cli", config.log_path)
console = Console(no_color=color_disabled(config.output_mode))


def _client_kwargs(cli_config: CliConfig | None = None) -> dict:
    cli_config = cli_config or config
    return runtime_client_kwargs(cli_config, client_cls=Client, warn=logger.warning)


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
