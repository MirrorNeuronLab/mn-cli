from __future__ import annotations

from mn_cli.sdk_path import add_local_sdk_path

add_local_sdk_path("client.py")

from mn_sdk.surface_client import SurfaceRuntimeClient
from rich.console import Console
from mn_cli.config import CliConfig
from mn_cli.logging_config import configure_logging
from mn_cli.terminal import color_disabled

config = CliConfig.from_env()
logger = configure_logging("mn-cli", config.log_path)
console = Console(no_color=color_disabled(config.output_mode))
error_console = Console(stderr=True, no_color=color_disabled(config.output_mode))
setattr(console, "_mn_shared_console", True)
setattr(error_console, "_mn_shared_error_console", True)


def build_client(cli_config: CliConfig | None = None) -> SurfaceRuntimeClient:
    return SurfaceRuntimeClient()


class LazyClient:
    """Construct the SDK client on first use, never while rendering CLI help."""

    def __init__(self) -> None:
        object.__setattr__(self, "_instance", None)

    def _get(self) -> SurfaceRuntimeClient:
        instance = object.__getattribute__(self, "_instance")
        if instance is None:
            instance = build_client(config)
            object.__setattr__(self, "_instance", instance)
        return instance

    def __getattr__(self, name: str):
        return getattr(self._get(), name)

    def __setattr__(self, name: str, value) -> None:
        if name == "_instance":
            object.__setattr__(self, name, value)
            return
        setattr(self._get(), name, value)

    def __delattr__(self, name: str) -> None:
        if name == "_instance":
            object.__delattr__(self, name)
            return
        delattr(self._get(), name)


client = LazyClient()
