from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any

import typer
from mn_sdk.errors import AppError, normalize_exception, sanitize_context
from rich.console import Console

from mn_cli.config import CliConfig
from mn_cli.libs.ui import print_error
from mn_cli.logging_config import configure_logging
from mn_cli.output import record_error

log_file = CliConfig.from_env().log_path
logger = configure_logging("mn-cli", log_file)

_DEBUG = False

CONTEXT_MESSAGES = {
    "nodes": "Error fetching nodes",
    "reconcile-node": "Error reconciling node",
    "drain-node": "Error draining node",
    "undrain-node": "Error cancelling node drain",
    "maintenance-node": "Error changing node maintenance",
    "resource usage": "Error fetching usage",
    "resource show": "Error fetching resources",
    "resource set": "Error setting resource limits",
    "service list": "Error listing services",
    "service show": "Error showing service",
    "run bundle": "Error running bundle",
    "monitor stream": "Error fetching job",
    "fetch results": "Error fetching results",
    "validate": "Validation failed",
    "leave": "Error removing node",
}


def set_debug(enabled: bool) -> None:
    global _DEBUG
    _DEBUG = bool(enabled)


def debug_enabled() -> bool:
    return _DEBUG or os.getenv("MN_DEBUG", "").strip().lower() in {"1", "true", "yes", "on"}


def handle_cli_error(
    error: Exception,
    console: Console,
    context: str = "",
    *,
    debug: bool | None = None,
    command_context: Mapping[str, Any] | None = None,
) -> None:
    """Log full diagnostics and print a stable user-safe CLI error."""
    app_error = normalize_exception(error, context=command_context)
    record_error(app_error, command_context=dict(command_context or {}))
    sanitized = sanitize_context(
        {
            "context": context,
            **(dict(command_context or {})),
        }
    )
    logger.exception(
        "CLI command failed error_code=%s context=%s sanitized_context=%s",
        app_error.code,
        context,
        sanitized,
    )
    print_cli_error(app_error, console, debug=debug_enabled() if debug is None else debug)
    raise typer.Exit(app_error.exit_code) from error


def print_cli_error(app_error: AppError, console: Console, *, debug: bool = False) -> None:
    if getattr(console, "_mn_shared_console", False):
        from mn_cli.shared import error_console

        console = error_console
    print_error(console, app_error.user_message, code=app_error.code)
    if app_error.hint:
        console.print(f"[bold yellow]! Hint:[/bold yellow] {app_error.hint}")
    if debug and app_error.internal_message:
        console.print(f"[dim]Diagnostic: {app_error.internal_message}[/dim]")
    if not debug:
        console.print("[dim]See the MirrorNeuron CLI logs for full details.[/dim]")
