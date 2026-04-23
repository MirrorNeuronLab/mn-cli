import sys
import logging
from pathlib import Path
import grpc
from rich.console import Console

# Setup logging
log_dir = Path.home() / ".mn" / "logs"
log_dir.mkdir(parents=True, exist_ok=True)
log_file = log_dir / "cli.log"

logging.basicConfig(
    filename=str(log_file),
    level=logging.ERROR,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("mn-cli")

def handle_cli_error(e: Exception, console: Console, context: str = ""):
    """Handle exceptions gracefully, log the full trace, and print a friendly message."""
    logger.exception(f"Error during {context}")
    
    if isinstance(e, grpc.RpcError):
        code = e.code()
        details = e.details()
        
        if code == grpc.StatusCode.NOT_FOUND:
            console.print(f"[red]Error: Cannot find the job by ID. ({details})[/red]")
        elif code == grpc.StatusCode.INTERNAL and "not found" in str(details).lower():
            console.print(f"[red]Error: Cannot find the job by ID. ({details})[/red]")
        elif code == grpc.StatusCode.INTERNAL and "terminal state" in str(details).lower():
            console.print(f"[red]Error: Job is already in a terminal state and cannot be modified.[/red]")
        else:
            console.print(f"[red]Communication Error: {details} (Code: {code.name})[/red]")
            console.print("[dim]See ~/.mn/logs/cli.log for full details.[/dim]")
    else:
        console.print(f"[red]Error: {str(e)}[/red]")
        console.print("[dim]See ~/.mn/logs/cli.log for full details.[/dim]")
