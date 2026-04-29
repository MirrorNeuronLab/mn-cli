from mn_sdk import Client
from rich.console import Console
from mn_cli.config import CliConfig

config = CliConfig.from_env()
console = Console(no_color=config.output_mode == "plain")
client = Client(
    target=config.grpc_target,
    timeout=config.grpc_timeout_seconds,
    auth_token=config.grpc_auth_token,
)
