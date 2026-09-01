from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import replace
from typing import Any

import typer
from mn_sdk.errors import AppError, normalize_exception, sanitize_context
from rich.console import Console
from rich.markup import escape

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

_CONTEXT_ALIASES = {
    "nodes": "node list",
    "reconcile-node": "node reconcile",
    "drain-node": "node drain",
    "undrain-node": "node undrain",
    "maintenance-node": "node maintenance",
    "leave": "node remove",
    "run bundle": "blueprint run",
    "monitor stream": "run watch",
    "fetch results": "run result",
    "validate": "blueprint validate",
    "job definitions": "job list",
}

_RESOURCE_CONTEXT = {
    "blueprint": (
        "blueprint",
        "blueprints",
        "mn blueprint list",
        ("blueprint_id", "blueprint"),
    ),
    "job": ("job", "jobs", "mn job list", ("job_id",)),
    "run": ("run", "runs", "mn run list", ("run_id",)),
    "model": ("model", "models", "mn model list", ("model_id", "model")),
    "node": ("node", "nodes", "mn node list", ("node_name", "node")),
    "operation": ("operation", "operations", None, ("operation_id",)),
    "runtime": ("runtime", "runtime", "mn runtime status", ()),
    "resource": (
        "runtime resource",
        "runtime resources",
        "mn resource show",
        ("resource_id",),
    ),
    "service": (
        "service",
        "services",
        "mn service list",
        ("service_id", "service", "name"),
    ),
}

_ACTION_WORDS = {
    "add": ("add", "adding"),
    "archive": ("archive", "archiving"),
    "cancel": ("cancel", "cancelling"),
    "cleanup": ("clean up", "cleaning up"),
    "compare": ("compare", "comparing"),
    "create": ("create", "creating"),
    "delete": ("delete", "deleting"),
    "doctor": ("check", "checking"),
    "drain": ("drain", "draining"),
    "ensure-context-engine": (
        "start the context engine for",
        "starting the context engine for",
    ),
    "export": ("export", "exporting"),
    "list": ("list", "listing"),
    "logs": ("load logs for", "loading logs for"),
    "maintenance": ("change maintenance mode for", "changing maintenance mode for"),
    "pause": ("pause", "pausing"),
    "probe": ("probe", "probing"),
    "reconcile": ("reconcile", "reconciling"),
    "remove": ("remove", "removing"),
    "reset-data": ("reset data for", "resetting data for"),
    "restart-sidecars": ("restart sidecars for", "restarting sidecars for"),
    "result": ("load the result for", "loading the result for"),
    "resume": ("resume", "resuming"),
    "resources": ("load resources for", "loading resources for"),
    "run": ("run", "running"),
    "set": ("update", "updating"),
    "show": ("load", "loading"),
    "start": ("start", "starting"),
    "status": ("check", "checking"),
    "stop": ("stop", "stopping"),
    "undrain": ("cancel the drain for", "cancelling the drain for"),
    "update": ("update", "updating"),
    "usage": ("load usage for", "loading usage for"),
    "validate": ("validate", "validating"),
    "watch": ("watch", "watching"),
}

_UNCERTAIN_MUTATIONS = {
    "add",
    "archive",
    "cancel",
    "cleanup",
    "create",
    "delete",
    "drain",
    "maintenance",
    "pause",
    "reconcile",
    "remove",
    "reset-data",
    "resume",
    "set",
    "start",
    "stop",
    "undrain",
    "update",
}

_GENERIC_USER_MESSAGES = {
    "MN_RUNTIME_TIMEOUT": {
        "The runtime did not respond before the request timed out.",
    },
    "MN_NODE_UNAVAILABLE": {
        "The job owner node is not reachable.",
        "The resource owner node is not reachable.",
    },
    "MN_RUNTIME_UNAVAILABLE": {
        "The MirrorNeuron runtime is not reachable.",
    },
    "MN_FAILED_PRECONDITION": {
        "The runtime is not in the required state for this operation.",
    },
    "MN_INVALID_ARGUMENT": {
        "The request was invalid.",
        "A command argument or input value is invalid.",
    },
    "MN_PERMISSION_DENIED": {
        "Permission was denied for this MirrorNeuron operation.",
    },
    "MN_ALREADY_EXISTS": {
        "A MirrorNeuron resource with that identifier already exists.",
    },
    "MN_EXECUTION_FAILED": {
        "Execution failed. Run again with --debug for more details.",
    },
}


def set_debug(enabled: bool) -> None:
    global _DEBUG
    _DEBUG = bool(enabled)


def debug_enabled() -> bool:
    return _DEBUG or os.getenv("MN_DEBUG", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def handle_cli_error(
    error: Exception,
    console: Console,
    context: str = "",
    *,
    debug: bool | None = None,
    command_context: Mapping[str, Any] | None = None,
) -> None:
    """Log full diagnostics and print a stable user-safe CLI error."""
    error_context = {
        "command": _canonical_context(context),
        **dict(command_context or {}),
    }
    app_error = normalize_exception(error, context=error_context)
    app_error = contextualize_cli_error(
        app_error, context, command_context=error_context
    )
    record_error(app_error, command_context=error_context)
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
    print_cli_error(
        app_error, console, debug=debug_enabled() if debug is None else debug
    )
    raise typer.Exit(app_error.exit_code) from error


def contextualize_cli_error(
    app_error: AppError,
    context: str,
    *,
    command_context: Mapping[str, Any] | None = None,
) -> AppError:
    """Add command and resource language without exposing transport details."""
    command = _canonical_context(context)
    parts = command.split()
    family = parts[0] if parts else ""
    action = parts[1] if len(parts) > 1 else ""
    resource = _RESOURCE_CONTEXT.get(family)
    if resource is None:
        return app_error
    if app_error.code != "MN_NOT_FOUND" and app_error.user_message not in (
        _GENERIC_USER_MESSAGES.get(app_error.code) or set()
    ):
        return app_error

    singular, plural, list_command, id_keys = resource
    identifier = _resource_identifier(command_context, id_keys, family, action)
    target = (
        f"{singular} {identifier!r}"
        if identifier
        else (plural if action == "list" else f"the {singular}")
    )
    verb, ongoing = _ACTION_WORDS.get(
        action, (action or "complete the command", action or "completing the command")
    )

    if app_error.code == "MN_NOT_FOUND":
        message = (
            f"{singular.capitalize()} {identifier!r} was not found."
            if identifier
            else f"The requested {singular} was not found."
        )
        hint = (
            f"Run '{list_command}' to find a valid {singular} ID."
            if list_command
            else "Check the identifier from the command that created the resource and try again."
        )
    elif app_error.code == "MN_RUNTIME_TIMEOUT":
        message = (
            f"The runtime did not respond while MirrorNeuron was {ongoing} {target}."
        )
        if action in _UNCERTAIN_MUTATIONS:
            hint = (
                f"The owner runtime may still be processing the request. Check the state of {target} before retrying."
                if identifier
                else "The runtime may still be processing the request. Check its state before retrying."
            )
        else:
            hint = "Check that the connected runtime and any resource owner node are reachable, then try again."
    elif app_error.code == "MN_NODE_UNAVAILABLE":
        message = f"The node that owns {target} is not reachable."
        hint = "Reconnect the owner node and try again; destructive changes are not queued while it is offline."
    elif app_error.code == "MN_RUNTIME_UNAVAILABLE":
        message = f"MirrorNeuron could not {verb} {target} because the configured runtime is not reachable."
        hint = "Run 'mn runtime status' and check the configured runtime host and port."
    elif app_error.code == "MN_FAILED_PRECONDITION":
        message = f"MirrorNeuron could not {verb} {target} in its current state."
        hint = f"Refresh the state of {target} and try again."
    elif app_error.code == "MN_INVALID_ARGUMENT":
        message = (
            f"MirrorNeuron could not {verb} {target} because the request was invalid."
        )
        hint = "Review the command arguments and try again."
    elif app_error.code == "MN_PERMISSION_DENIED":
        message = f"You do not have permission to {verb} {target}."
        hint = "Check the configured MirrorNeuron credentials and try again."
    elif app_error.code == "MN_ALREADY_EXISTS":
        message = (
            f"{singular.capitalize()} {identifier!r} already exists."
            if identifier
            else f"That {singular} already exists."
        )
        hint = (
            f"Choose a different identifier or run '{list_command}' to inspect the existing {singular}."
            if list_command
            else app_error.hint
        )
    elif app_error.code == "MN_EXECUTION_FAILED":
        message = f"MirrorNeuron could not {verb} {target}."
        hint = "Run the command again with --debug to see the underlying failure."
    else:
        return app_error

    return replace(app_error, user_message=message, hint=hint)


def print_cli_error(
    app_error: AppError, console: Console, *, debug: bool = False
) -> None:
    if getattr(console, "_mn_shared_console", False):
        from mn_cli.shared import error_console

        console = error_console
    print_error(console, escape(app_error.user_message), code=app_error.code)
    if app_error.hint:
        console.print(f"[bold yellow]! Hint:[/bold yellow] {escape(app_error.hint)}")
    if debug and app_error.internal_message:
        console.print(f"[dim]Diagnostic: {escape(app_error.internal_message)}[/dim]")
    if not debug:
        console.print("[dim]See the MirrorNeuron CLI logs for full details.[/dim]")


def _canonical_context(context: str) -> str:
    normalized = " ".join(str(context or "").strip().lower().split())
    normalized = normalized.removeprefix("mn ")
    return _CONTEXT_ALIASES.get(normalized, normalized)


def _resource_identifier(
    context: Mapping[str, Any] | None,
    keys: tuple[str, ...],
    family: str,
    action: str,
) -> str | None:
    values = dict(context or {})
    for key in keys:
        value = values.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()

    argv = values.get("argv")
    if not isinstance(argv, (list, tuple)):
        return None
    positional = [
        str(value)
        for value in argv
        if value is not None and not str(value).startswith("-")
    ]
    try:
        family_index = positional.index(family)
    except ValueError:
        return None
    candidate_index = (
        family_index + 2
        if action and family_index + 1 < len(positional)
        else family_index + 1
    )
    return positional[candidate_index] if candidate_index < len(positional) else None
