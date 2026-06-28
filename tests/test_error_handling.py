from io import StringIO

import grpc
import pytest
import typer
from rich.console import Console

from mn_cli.error_handler import handle_cli_error, set_debug


class TimeoutRpcError(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.DEADLINE_EXCEEDED

    def details(self):
        return "Deadline Exceeded --token secret-token /Users/homer/Projects/private.py"


def _console_stream():
    stream = StringIO()
    return Console(file=stream, force_terminal=False, no_color=True, width=140), stream


def test_cli_error_output_is_user_safe_and_preserves_code(mocker):
    console, stream = _console_stream()
    log = mocker.patch("mn_cli.error_handler.logger.exception")

    with pytest.raises(typer.Exit) as raised:
        handle_cli_error(
            TimeoutRpcError(),
            console,
            "node join",
            command_context={"argv": ["node", "join", "192.168.4.34", "--token", "secret-token"]},
        )

    output = stream.getvalue()
    assert raised.value.exit_code == 1
    assert "Error MN_RUNTIME_TIMEOUT" in output
    assert "The runtime did not respond" in output
    assert "Traceback" not in output
    assert "Deadline Exceeded" not in output
    assert "secret-token" not in output
    assert "/Users/homer" not in output
    log.assert_called_once()
    assert log.call_args.args[1] == "MN_RUNTIME_TIMEOUT"


def test_cli_debug_output_is_sanitized(mocker):
    console, stream = _console_stream()
    mocker.patch("mn_cli.error_handler.logger.exception")
    set_debug(True)
    try:
        with pytest.raises(typer.Exit):
            handle_cli_error(TimeoutRpcError(), console, "node join")
    finally:
        set_debug(False)

    output = stream.getvalue()
    assert "Diagnostic:" in output
    assert "Deadline Exceeded" in output
    assert "secret-token" not in output
    assert "/Users/homer" not in output


def test_cli_wrapper_catches_unhandled_command_errors(monkeypatch, capsys, mocker):
    from mn_cli import main as main_module

    def failing_app(*args, **kwargs):
        raise RuntimeError("raw failure token=secret-token /Users/homer/private.py")

    monkeypatch.setattr(main_module, "app", failing_app)
    monkeypatch.setattr("sys.argv", ["mn", "node", "join", "host", "--token", "secret-token"])
    mocker.patch("mn_cli.error_handler.logger.exception")

    with pytest.raises(SystemExit) as raised:
        main_module.cli()

    output = capsys.readouterr().out
    assert raised.value.code == 1
    assert "MN_EXECUTION_FAILED" in output
    assert "raw failure" not in output
    assert "secret-token" not in output
    assert "/Users/homer" not in output
