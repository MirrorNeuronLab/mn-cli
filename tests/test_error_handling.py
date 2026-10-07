from io import StringIO

import grpc
import pytest
import typer
from mn_sdk.errors import AppError
from rich.console import Console

from mn_cli.error_handler import contextualize_cli_error, handle_cli_error, set_debug


class TimeoutRpcError(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.DEADLINE_EXCEEDED

    def details(self):
        return "Deadline Exceeded --token secret-token /Users/homer/Projects/private.py"


class NotFoundRpcError(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.NOT_FOUND

    def details(self):
        return "job ros_amr_controller-c1eafb38a2 was not found"


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
            "node add",
            command_context={
                "argv": ["node", "join", "192.168.4.34", "--token", "secret-token"]
            },
        )

    output = stream.getvalue()
    assert raised.value.exit_code == 1
    assert "Error: (MN_RUNTIME_TIMEOUT)" in output
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
            handle_cli_error(TimeoutRpcError(), console, "node add")
    finally:
        set_debug(False)

    output = stream.getvalue()
    assert "Diagnostic:" in output
    assert "Deadline Exceeded" in output
    assert "secret-token" not in output
    assert "/Users/homer" not in output


def test_execution_failure_hint_places_global_debug_option_before_command():
    error = contextualize_cli_error(
        AppError(
            "MN_EXECUTION_FAILED",
            "Execution failed. Run again with --debug for more details.",
        ),
        "job start",
        command_context={"job_id": "job-1"},
    )

    assert error.hint == "Run again with the global option before the command: mn --debug …"


def test_job_delete_names_a_missing_job_and_points_to_job_list(mocker):
    console, stream = _console_stream()
    mocker.patch("mn_cli.error_handler.logger.exception")

    with pytest.raises(typer.Exit) as raised:
        handle_cli_error(
            NotFoundRpcError(),
            console,
            "job delete",
            command_context={"job_id": "ros_amr_controller-c1eafb38a2"},
        )

    output = stream.getvalue()
    assert raised.value.exit_code == 2
    assert "MN_NOT_FOUND" in output
    assert "Job 'ros_amr_controller-c1eafb38a2' was not found." in output
    assert "mn job list" in output
    assert "MN_EXECUTION_FAILED" not in output


def test_job_delete_timeout_warns_that_remote_mutation_outcome_is_uncertain(mocker):
    console, stream = _console_stream()
    mocker.patch("mn_cli.error_handler.logger.exception")

    with pytest.raises(typer.Exit):
        handle_cli_error(
            TimeoutRpcError(),
            console,
            "job delete",
            command_context={"job_id": "job_rac-1fef23fc"},
        )

    output = stream.getvalue()
    assert "deleting job 'job_rac-1fef23fc'" in output
    assert "may still be processing the request" in output
    assert "Check the state" in output


@pytest.mark.parametrize(
    ("command", "context", "expected"),
    [
        ("blueprint show", {"blueprint_id": "bp-1"}, "Blueprint 'bp-1' was not found."),
        ("job show", {"job_id": "job-1"}, "Job 'job-1' was not found."),
        ("run show", {"run_id": "run-1"}, "Run 'run-1' was not found."),
        ("model show", {"model": "model-1"}, "Model 'model-1' was not found."),
        ("node show", {"node_name": "node-1"}, "Node 'node-1' was not found."),
        ("operation show", {"operation_id": "op-1"}, "Operation 'op-1' was not found."),
        ("service show", {"name": "svc-1"}, "Service 'svc-1' was not found."),
    ],
)
def test_not_found_errors_are_contextualized_for_cli_resource_families(
    command, context, expected
):
    error = contextualize_cli_error(
        AppError("MN_NOT_FOUND", "The requested resource was not found."),
        command,
        command_context=context,
    )

    assert error.user_message == expected


def test_contextualization_preserves_specific_validation_message():
    error = contextualize_cli_error(
        AppError(
            "MN_INVALID_ARGUMENT",
            "manifest apiVersion must be mn.workflow/v1",
        ),
        "blueprint validate",
    )

    assert error.user_message == "manifest apiVersion must be mn.workflow/v1"


def test_cli_wrapper_catches_unhandled_command_errors(monkeypatch, capsys, mocker):
    from mn_cli import main as main_module

    def failing_app(*args, **kwargs):
        raise RuntimeError("raw failure token=secret-token /Users/homer/private.py")

    monkeypatch.setattr(main_module, "app", failing_app)
    monkeypatch.setattr(
        "sys.argv", ["mn", "node", "join", "host", "--token", "secret-token"]
    )
    mocker.patch("mn_cli.error_handler.logger.exception")

    with pytest.raises(SystemExit) as raised:
        main_module.cli()

    captured = capsys.readouterr()
    output = captured.err
    assert raised.value.code == 1
    assert "MN_EXECUTION_FAILED" in output
    assert "MirrorNeuron could not join node 'host'." in output
    assert captured.out == ""
    assert "raw failure" not in output
    assert "secret-token" not in output
    assert "/Users/homer" not in output


@pytest.mark.parametrize("width", [50, 140])
@pytest.mark.parametrize("plain", [False, True])
def test_memory_failure_explains_requirements_without_debug(mocker, monkeypatch, width, plain):
    from mn_sdk.admission_errors import placement_error
    from mn_cli.output import _error_payload
    from mn_sdk.errors import normalize_exception

    if plain:
        monkeypatch.setenv("MN_CLI_OUTPUT", "plain")
    console, stream = _console_stream()
    console.width = width
    mocker.patch("mn_cli.error_handler.logger.exception")
    error = placement_error({"node": ["host_memory_total_mb=24576 < required=49152"]}, "private")
    with pytest.raises(typer.Exit):
        handle_cli_error(error, console, "blueprint run", debug=False)
    output = " ".join(stream.getvalue().split())
    assert "MN_MEMORY_REQUIREMENT_UNMET" in output
    assert "has 24 GiB of host memory; this work requires 48 GiB" in output
    assert "reduce the workflow's memory requirement" in output
    assert "private" not in output
    payload = _error_payload(normalize_exception(error))
    assert payload["problem_code"] == 1001
    assert "Problem code: 1001 (hardware)" in output
    assert payload["category"] == "hardware"
    assert payload["retryable"] is False
    assert payload["details"]["blockers"][0]["required"] == 48


@pytest.mark.parametrize("plain", [False, True])
def test_cli_explains_core_gpu_memory_shortage_with_shared_problem_code(mocker, monkeypatch, plain):
    import grpc
    import json
    from mn_cli.output import _error_payload
    from mn_sdk.errors import normalize_exception

    class MemoryRpcError(grpc.RpcError):
        def code(self):
            return grpc.StatusCode.INTERNAL

        def details(self):
            return "placement_failed: private node token=secret\nmn_admission_v1:" + json.dumps({"blockers": [{
                "code": "MN_GPU_MEMORY_UNAVAILABLE", "node_index": 1, "node_label": "spark",
                "resource": "gpu_memory_free_mb", "available": 8.17, "required": 48, "unit": "GiB",
            }]})

    if plain:
        monkeypatch.setenv("MN_CLI_OUTPUT", "plain")
    console, stream = _console_stream()
    console.width = 120
    mocker.patch("mn_cli.error_handler.logger.exception")
    with pytest.raises(typer.Exit):
        handle_cli_error(MemoryRpcError(), console, "blueprint run", debug=False)
    output = " ".join(stream.getvalue().split())
    assert "spark has 8.17 GiB of free GPU memory" in output
    assert "this work requires 48 GiB" in output
    assert "unload unused models" in output
    assert "Problem code: 2001 (capacity)" in output
    assert "secret" not in output and "private" not in output
    payload = _error_payload(normalize_exception(MemoryRpcError()))
    assert payload["problem_code"] == 2001 and payload["retryable"] is True
    assert payload["details"]["blockers"][0]["node_label"] == "spark"
