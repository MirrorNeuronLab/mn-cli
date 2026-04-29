from mn_sdk import Client
from rich.console import Console
from mn_cli.config import CliConfig
from mn_cli.logging_config import configure_logging

config = CliConfig.from_env()
logger = configure_logging("mn-cli", config.log_path)
console = Console(no_color=config.output_mode == "plain")
client = Client(
    target=config.grpc_target,
    timeout=config.grpc_timeout_seconds,
    auth_token=config.grpc_auth_token,
)
