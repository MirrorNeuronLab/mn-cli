import grpc
from rich.console import Console
from mn_cli.config import CliConfig
from mn_cli.logging_config import configure_logging

log_file = CliConfig.from_env().log_path
logger = configure_logging("mn-cli", log_file)

CONTEXT_MESSAGES = {
    "submit": "Error submitting job",
    "status": "Error fetching job status",
    "list_jobs": "Error listing jobs",
    "clear": "Error clearing jobs",
    "cancel": "Error cancelling job",
    "pause": "Error pausing job",
    "resume": "Error resuming job",
    "nodes": "Error fetching nodes",
    "reconcile-node": "Error reconciling node",
    "drain-node": "Error draining node",
    "undrain-node": "Error cancelling node drain",
    "maintenance-node": "Error changing node maintenance",
    "metrics": "Error fetching metrics",
    "resource list": "Error fetching resources",
    "resource set": "Error setting resource limits",
    "dead_letters": "Error listing dead letters",
    "run bundle": "Error running bundle",
    "monitor stream": "Error fetching job",
    "fetch results": "Error fetching results",
    "validate": "Validation failed",
    "leave": "Error removing node",
}

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
            console.print("[red]Error: Job is already in a terminal state and cannot be modified.[/red]")
        elif code == grpc.StatusCode.RESOURCE_EXHAUSTED:
            console.print("[yellow]Runtime is under CPU/GPU/memory pressure and is not accepting new jobs.[/yellow]")
            console.print(f"[dim]{details}[/dim]")
        else:
            console.print(f"[red]Communication Error: {details} (Code: {code.name})[/red]")
            console.print(f"[dim]See {log_file} for full details.[/dim]")
    else:
        prefix = CONTEXT_MESSAGES.get(context, "Error")
        console.print(f"[red]{prefix}: {str(e)}[/red]")
        console.print(f"[dim]See {log_file} for full details.[/dim]")
