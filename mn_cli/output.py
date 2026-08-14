from __future__ import annotations

import contextlib
import functools
import inspect
import io
import json
import re
import sys
import time
from collections.abc import Callable
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import click
import typer
from mn_sdk.errors import AppError, normalize_exception
from typer.core import TyperGroup

CLI_SCHEMA = "mn.cli/v1"
STREAM_SCHEMA = "mn.cli.stream/v1"
_OUTPUT_SECRET_KEY = re.compile(
    r"(?i)(authorization|bearer|cookie|credential|password|secret|"
    r"(^|[\s_-])token($|[\s_-])|(^|[\s_-])api[\s_-]?key($|[\s_-]))"
)
_OUTPUT_BEARER = re.compile(r"(?i)(bearer\s+)[A-Za-z0-9._~+/=-]+")
_OUTPUT_ENV_SECRET = re.compile(r"(?i)\b([A-Z0-9_]*(?:TOKEN|SECRET|PASSWORD|API_KEY|COOKIE|CREDENTIAL)[A-Z0-9_]*=)([^\s]+)")
_OUTPUT_URL_SECRET = re.compile(r"(?i)([?&](?:token|access_token|api_key|key|signature|password|secret)=)[^&\s]+")

REMOVED_COMMANDS: dict[tuple[str, ...], str] = {
    ("blueprint", "install"): "mn blueprint add",
    ("blueprint", "uninstall"): "mn blueprint remove",
    ("blueprint", "monitor"): "mn run list --follow",
    ("blueprint", "tail"): "mn run logs RUN_ID --channel events",
    ("blueprint", "logs"): "mn run logs RUN_ID --channel logs",
    ("blueprint", "stream"): "mn run logs RUN_ID --channel all --follow",
    ("blueprint", "resources"): "mn run resources RUN_ID",
    ("blueprint", "compare"): "mn run compare RUN_A RUN_B",
    ("job", "inspect"): "mn job show",
    ("job", "runs"): "mn run list --job JOB_ID",
    ("run", "status"): "mn run show",
    ("run", "monitor"): "mn run watch",
    ("runtime", "health"): "mn runtime status (or mn runtime doctor for diagnostics)",
    ("runtime", "metrics"): "mn resource usage",
    ("node", "join"): "mn node add HOST --token TOKEN",
    ("node", "expose"): "mn runtime start --worker",
    ("node", "leave"): "mn node remove",
    ("operation", "status"): "mn operation show",
    ("resource", "list"): "mn resource show",
    ("resource", "ports"): "mn runtime status",
    ("service", "resolve"): "mn service show",
    ("service", "check"): "mn blueprint doctor",
    ("deployment", "status"): "mn deployment show",
    ("schedule", "create"): "mn schedule add --cron ...",
    ("schedule", "delay"): "mn schedule add --at ... (or --in ...)",
    ("schedule", "status"): "mn schedule show",
    ("schedule", "run-now"): "mn schedule run",
    ("schedule", "delete"): "mn schedule remove",
    ("model", "install"): "mn model add",
    ("model", "proxy"): "mn model add --file DEFINITION.json",
    ("model", "remote"): "mn model add --file DEFINITION.json",
    ("trigger",): "mn schedule add --event EVENT (or mn schedule list --kind event)",
}
REMOVED_OPTIONS: dict[str, str] = {
    "--verbose": "use '--debug' instead",
    "--worker-node": "use 'mn runtime start --worker' instead",
    "--join-host": (
        "run 'mn runtime start --worker' on the worker, then "
        "'mn node add HOST --token TOKEN' on the primary"
    ),
}


class RemediatingTyperGroup(TyperGroup):
    """Reject removed command paths with a deterministic replacement hint."""

    def get_command(self, ctx, cmd_name: str):
        command = super().get_command(ctx, cmd_name)
        if command is not None:
            return command
        path = tuple(str(ctx.command_path).split()[1:] + [cmd_name])
        for removed, replacement in REMOVED_COMMANDS.items():
            if path[-len(removed) :] == removed:
                raise typer.BadParameter(
                    f"'{' '.join(removed)}' was removed; use '{replacement}' instead",
                    ctx=ctx,
                )
        return None

    def main(self, args=None, prog_name=None, complete_var=None, standalone_mode=True, **extra):
        resolved_args = list(sys.argv[1:] if args is None else args)
        removed_option = next(
            (option for option in REMOVED_OPTIONS if option in resolved_args),
            None,
        )
        if removed_option is not None:
            message = (
                f"Option '{removed_option}' was removed; "
                f"{REMOVED_OPTIONS[removed_option]}."
            )
            if "--json" in resolved_args:
                payload = {
                    "schema": CLI_SCHEMA,
                    "ok": False,
                    "error": {
                        "code": "MN_USAGE_ERROR",
                        "message": message,
                        "hint": "Use the replacement and try again.",
                    },
                    "warnings": [],
                    "meta": {
                        "command": " ".join(
                            ["mn", *[part for part in resolved_args if not part.startswith("-")][:2]]
                        ),
                        "timestamp": _timestamp(),
                        "duration_ms": 0,
                    },
                }
                click.echo(json.dumps(payload, ensure_ascii=False, indent=2))
            else:
                click.echo(f"Error: {message}", err=True)
            raise SystemExit(2)
        if not standalone_mode or "--json" not in resolved_args:
            return super().main(
                args=args,
                prog_name=prog_name,
                complete_var=complete_var,
                standalone_mode=standalone_mode,
                **extra,
            )
        started = time.monotonic()
        try:
            result = super().main(
                args=args,
                prog_name=prog_name,
                complete_var=complete_var,
                standalone_mode=False,
                **extra,
            )
            if isinstance(result, int) and result != 0:
                raise SystemExit(result)
            return result
        except Exception as exc:
            if not callable(getattr(exc, "format_message", None)):
                raise
            command_parts = [part for part in resolved_args if part != "--json" and not part.startswith("-")]
            payload = {
                "schema": CLI_SCHEMA,
                "ok": False,
                "error": {
                    "code": "MN_USAGE_ERROR",
                    "message": exc.format_message(),
                    "hint": "Review the command help and try again.",
                },
                "warnings": [],
                "meta": {
                    "command": " ".join(["mn", *command_parts[:2]]),
                    "timestamp": _timestamp(),
                    "duration_ms": max(int((time.monotonic() - started) * 1000), 0),
                },
            }
            click.echo(json.dumps(payload, ensure_ascii=False, indent=2))
            raise SystemExit(int(getattr(exc, "exit_code", 2))) from None


@dataclass
class CommandSession:
    command: str
    json_output: bool
    started: float = field(default_factory=time.monotonic)
    data: Any = None
    warnings: list[dict[str, Any]] = field(default_factory=list)
    error: AppError | None = None
    stream_started: bool = False
    sequence: int = 0
    stream_output: Any = None


_SESSION: ContextVar[CommandSession | None] = ContextVar("mn_cli_output_session", default=None)


def current_session() -> CommandSession | None:
    return _SESSION.get()


def json_enabled() -> bool:
    session = current_session()
    return bool(session and session.json_output)


def record_result(data: Any) -> None:
    session = current_session()
    if session is not None:
        session.data = data


def record_warning(message: Any, *, code: str | None = None) -> None:
    session = current_session()
    if session is None:
        return
    warning: dict[str, Any] = {"message": str(message)}
    if code:
        warning["code"] = code
    session.warnings.append(warning)


def record_error(error: AppError | Exception, *, command_context: dict[str, Any] | None = None) -> AppError:
    app_error = error if isinstance(error, AppError) else normalize_exception(error, context=command_context)
    session = current_session()
    if session is not None and session.error is None:
        session.error = app_error
    return app_error


def emit_stream_record(
    record_type: str,
    *,
    data: Any = None,
    error: AppError | None = None,
) -> None:
    session = current_session()
    if session is None or not session.json_output:
        return
    session.stream_started = True
    session.sequence += 1
    payload: dict[str, Any] = {
        "schema": STREAM_SCHEMA,
        "type": record_type,
        "sequence": session.sequence,
        "timestamp": _timestamp(),
    }
    if error is not None:
        payload["error"] = _error_payload(error)
    else:
        payload["data"] = data
    typer.echo(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        file=session.stream_output,
    )


def wrap_command(callback: Callable[..., Any], command: str) -> Callable[..., Any]:
    if getattr(callback, "__mn_output_wrapped__", False):
        return callback

    signature = inspect.signature(callback)
    json_parameter = _json_parameter(signature)
    add_json_parameter = json_parameter is None

    @functools.wraps(callback)
    def wrapped(*args: Any, **kwargs: Any) -> Any:
        json_output = bool(kwargs.get(json_parameter, False)) if json_parameter else bool(kwargs.pop("json_output", False))
        session = CommandSession(command=command, json_output=json_output)
        session.stream_output = sys.stdout
        token = _SESSION.set(session)
        capture = io.StringIO()
        shared_console = None
        original_console_file = None
        try:
            if json_output:
                from mn_cli.shared import console

                shared_console = console
                original_console_file = getattr(console, "_file", None)
                console._file = capture
            stdout_context = contextlib.redirect_stdout(capture) if json_output else contextlib.nullcontext()
            with stdout_context:
                result = callback(*args, **kwargs)
            if result is not None and session.data is None:
                session.data = result
            if json_output:
                _recover_captured_data(session, capture.getvalue())
                _emit_final(session)
            return result
        except KeyboardInterrupt as exc:
            session.error = AppError(
                "MN_INTERRUPTED",
                "The command was interrupted.",
                hint="Run the command again when you are ready.",
                exit_code=130,
                http_status=499,
                cause=exc,
            )
            if json_output:
                _emit_final(session)
            else:
                click.echo("Error: The command was interrupted.", err=True)
            raise typer.Exit(130) from exc
        except typer.Exit as exc:
            if json_output:
                _recover_captured_data(session, capture.getvalue())
                if exc.exit_code:
                    exit_code = int(exc.exit_code)
                    if session.error is not None and session.error.exit_code != exit_code:
                        generic_validation = session.error.code == "MN_COMMAND_FAILED" and exit_code == 2
                        session.error = AppError(
                            "MN_VALIDATION_FAILED" if generic_validation else session.error.code,
                            session.error.user_message,
                            internal_message=session.error.internal_message,
                            hint=(
                                "Review the command input and try again."
                                if generic_validation
                                else session.error.hint
                            ),
                            details=session.error.details,
                            exit_code=exit_code,
                            http_status=422 if exit_code == 2 else session.error.http_status,
                            cause=session.error.cause,
                        )
                if exc.exit_code and session.error is None:
                    diagnostic = exit_code == 1 and session.data is not None
                    validation = exit_code == 2 and session.data is not None
                    session.error = AppError(
                        (
                            "MN_DIAGNOSTIC_FAILED"
                            if diagnostic
                            else "MN_VALIDATION_FAILED"
                            if validation
                            else "MN_COMMAND_FAILED"
                        ),
                        (
                            "Diagnostics found critical failures."
                            if diagnostic
                            else "Validation failed."
                            if validation
                            else "The command did not complete successfully."
                        ),
                        hint=(
                            "Apply the listed fixes and run the diagnostic again."
                            if diagnostic
                            else "Review the validation issues and try again."
                            if validation
                            else "Review the command input and retry."
                        ),
                        details=(
                            session.data
                            if (diagnostic or validation) and isinstance(session.data, dict)
                            else None
                        ),
                        exit_code=exit_code,
                        http_status=422 if exit_code == 2 else 500,
                    )
                _emit_final(session)
            raise
        except Exception as exc:
            if callable(getattr(exc, "format_message", None)):
                app_error = record_error(
                    AppError(
                        "MN_USAGE_ERROR",
                        exc.format_message(),
                        hint="Review the command help and try again.",
                        details={"command": command},
                        exit_code=int(getattr(exc, "exit_code", 2)),
                        http_status=422,
                        cause=exc,
                    )
                )
            else:
                app_error = record_error(exc, command_context={"command": command})
            if json_output:
                _emit_final(session)
                raise typer.Exit(app_error.exit_code) from exc
            _emit_human_error(app_error)
            raise typer.Exit(app_error.exit_code) from exc
        finally:
            if shared_console is not None:
                shared_console._file = original_console_file
            _SESSION.reset(token)

    if add_json_parameter:
        parameters = list(signature.parameters.values())
        parameters.append(
            inspect.Parameter(
                "json_output",
                kind=inspect.Parameter.KEYWORD_ONLY,
                default=typer.Option(False, "--json", help="Print a versioned machine-readable JSON result."),
                annotation=bool,
            )
        )
        wrapped.__signature__ = signature.replace(parameters=parameters)  # type: ignore[attr-defined]
    wrapped.__mn_output_wrapped__ = True  # type: ignore[attr-defined]
    return wrapped


def instrument_typer(typer_app: typer.Typer, prefix: tuple[str, ...] = ()) -> None:
    for command_info in typer_app.registered_commands:
        callback = command_info.callback
        if callback is None:
            continue
        name = command_info.name or callback.__name__.replace("_", "-")
        path = " ".join(("mn", *prefix, name))
        command_info.callback = wrap_command(callback, path)
    for group_info in typer_app.registered_groups:
        name = group_info.name or "command"
        instrument_typer(group_info.typer_instance, (*prefix, name))


def success_envelope(session: CommandSession) -> dict[str, Any]:
    return {
        "schema": CLI_SCHEMA,
        "ok": True,
        "data": {} if session.data is None else _redact_output(session.data),
        "warnings": _redact_output(session.warnings),
        "meta": _meta(session),
    }


def error_envelope(session: CommandSession, error: AppError) -> dict[str, Any]:
    return {
        "schema": CLI_SCHEMA,
        "ok": False,
        "error": _error_payload(error),
        "warnings": _redact_output(session.warnings),
        "meta": _meta(session),
    }


def _emit_final(session: CommandSession) -> None:
    if session.stream_started:
        if session.error is not None:
            emit_stream_record("error", error=session.error)
        else:
            emit_stream_record("complete", data=session.data or {})
        return
    payload = error_envelope(session, session.error) if session.error else success_envelope(session)
    typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))


def _emit_human_error(error: AppError) -> None:
    click.echo(f"Error: ({error.code}) {error.user_message}", err=True)
    if error.hint:
        click.echo(f"Hint: {error.hint}", err=True)


def _recover_captured_data(session: CommandSession, captured: str) -> None:
    if session.data is not None:
        return
    text = captured.strip()
    if not text:
        return
    try:
        session.data = json.loads(text)
    except json.JSONDecodeError:
        session.data = {"message": _plain_text(text)}


def _plain_text(value: str) -> str:
    try:
        from rich.text import Text

        return Text.from_ansi(value).plain.strip()
    except (ImportError, TypeError, ValueError):
        return value.strip()


def _json_parameter(signature: inspect.Signature) -> str | None:
    for name, parameter in signature.parameters.items():
        if name == "json_output":
            return name
        default = parameter.default
        declarations = getattr(default, "param_decls", ()) or ()
        if "--json" in declarations:
            return name
    return None


def _error_payload(error: AppError) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "code": error.code,
        "message": error.user_message,
    }
    if error.hint:
        payload["hint"] = error.hint
    details = getattr(error, "details", None)
    if details:
        payload["details"] = _redact_output(details)
    return payload


def _meta(session: CommandSession) -> dict[str, Any]:
    return {
        "command": session.command,
        "timestamp": _timestamp(),
        "duration_ms": max(int((time.monotonic() - session.started) * 1000), 0),
    }


def _timestamp() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _redact_output(value: Any, *, key: str = "") -> Any:
    if _OUTPUT_SECRET_KEY.search(key):
        text = str(value or "")
        if key.lower().endswith("_env") or text.startswith("os.environ/"):
            return value
        return value if value in (None, "") else "[redacted]"
    if isinstance(value, dict):
        return {str(item_key): _redact_output(item, key=str(item_key)) for item_key, item in value.items()}
    if isinstance(value, list):
        return [_redact_output(item) for item in value]
    if isinstance(value, tuple):
        return [_redact_output(item) for item in value]
    if isinstance(value, str):
        text = _OUTPUT_BEARER.sub(r"\1[redacted]", value)
        text = _OUTPUT_ENV_SECRET.sub(r"\1[redacted]", text)
        return _OUTPUT_URL_SECRET.sub(r"\1[redacted]", text)
    return value
