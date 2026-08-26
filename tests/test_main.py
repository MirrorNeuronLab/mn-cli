import json
from importlib import metadata

import pytest
import typer
from mn_sdk.errors import AppError
from typer.main import get_command
from typer.testing import CliRunner

from mn_cli.banner import format_banner
from mn_cli.main import app
from mn_cli.output import emit_stream_record, instrument_typer, record_result

runner = CliRunner()


@pytest.fixture(autouse=True)
def no_local_runtime_mode(mocker):
    return mocker.patch("mn_cli.main.local_runtime_mode", return_value=None)


def test_version_prints_installed_package_version(mocker):
    mocker.patch("mn_cli.main.metadata.version", return_value="1.2.3")
    mock_update_prompt = mocker.patch("mn_cli.update_cmds.maybe_prompt_for_update")

    result = runner.invoke(app, ["--version"])

    assert result.exit_code == 0
    assert result.stdout == f"{format_banner('MirrorNeuron CLI')}\nversion 1.2.3\n"
    mock_update_prompt.assert_not_called()


def test_version_uses_fallback_when_package_metadata_is_missing(mocker):
    mocker.patch(
        "mn_cli.main.metadata.version",
        side_effect=metadata.PackageNotFoundError,
    )

    result = runner.invoke(app, ["--version"])

    assert result.exit_code == 0
    assert result.stdout == f"{format_banner('MirrorNeuron CLI')}\nversion 0.0.0\n"


def test_short_version_flag_prints_banner(mocker):
    mocker.patch("mn_cli.main.metadata.version", return_value="1.2.3")

    result = runner.invoke(app, ["-v"])

    assert result.exit_code == 0
    assert result.stdout == f"{format_banner('MirrorNeuron CLI')}\nversion 1.2.3\n"


def test_version_prints_worker_mode(mocker, no_local_runtime_mode):
    no_local_runtime_mode.return_value = "worker"
    mocker.patch("mn_cli.main.metadata.version", return_value="1.2.3")

    result = runner.invoke(app, ["--version"])

    assert result.exit_code == 0
    assert result.stdout == (
        f"{format_banner('MirrorNeuron CLI')}\n"
        "version 1.2.3\n"
        "runtime mode: worker\n"
    )


def test_short_version_prints_worker_mode(mocker, no_local_runtime_mode):
    no_local_runtime_mode.return_value = "worker"
    mocker.patch("mn_cli.main.metadata.version", return_value="1.2.3")

    result = runner.invoke(app, ["-v"])

    assert result.exit_code == 0
    assert result.stdout == (
        f"{format_banner('MirrorNeuron CLI')}\n"
        "version 1.2.3\n"
        "runtime mode: worker\n"
    )


def test_no_args_prints_banner_above_help(mocker):
    mock_update_prompt = mocker.patch("mn_cli.update_cmds.maybe_prompt_for_update")

    result = runner.invoke(app, [])

    assert result.exit_code == 0
    assert result.stdout.startswith(f"{format_banner('MirrorNeuron CLI')}\n")
    assert "Usage:" in result.stdout
    assert "Examples:" in result.stdout
    assert "mn blueprint list" in result.stdout
    assert "MN_GRPC_TARGET" in result.stdout
    mock_update_prompt.assert_not_called()


def test_no_args_help_remains_readable_on_narrow_terminal(monkeypatch):
    result = runner.invoke(app, [], env={"COLUMNS": "48"})

    assert result.exit_code == 0
    assert "Usage:" in result.stdout
    assert "Examples:" in result.stdout
    assert "mn blueprint list" in result.stdout
    assert "Usage:" in result.stdout


def test_no_args_prints_worker_mode_above_help(mocker, no_local_runtime_mode):
    no_local_runtime_mode.return_value = "worker"
    mock_update_prompt = mocker.patch("mn_cli.update_cmds.maybe_prompt_for_update")

    result = runner.invoke(app, [])

    assert result.exit_code == 0
    assert result.stdout.startswith(
        f"{format_banner('MirrorNeuron CLI')}\nRuntime mode: worker\n"
    )
    assert "Usage:" in result.stdout
    mock_update_prompt.assert_not_called()


@pytest.mark.parametrize(
    "args",
    [
        ["run", "bp-1"],
        ["monitor", "job-1"],
        ["status", "job-1"],
        ["nodes"],
        ["start"],
        ["deploy", "bundle"],
    ],
)
def test_removed_root_commands_fail(args):
    result = runner.invoke(app, args)

    assert result.exit_code != 0


def test_help_supports_short_help_flag():
    result = runner.invoke(app, ["job", "-h"])

    assert result.exit_code == 0
    assert "Create and manage durable job definitions." in result.stdout


def test_command_help_includes_argument_description_and_examples():
    result = runner.invoke(app, ["job", "create", "--help"])

    assert result.exit_code == 0
    assert "Blueprint/job bundle directory or archive." in result.stdout


def test_job_and_run_commands_have_distinct_resource_semantics():
    command = get_command(app)
    assert {"deployment", "schedule", "event"}.isdisjoint(command.commands)
    assert list(command.commands["job"].commands) == [
        "list", "create", "show", "start", "archive", "reset-data", "delete",
    ]
    assert list(command.commands["run"].commands) == [
        "list", "show", "watch", "logs", "result", "resources", "compare",
        "pause", "resume", "cancel", "delete", "human",
    ]
    assert list(command.commands["run"].commands["human"].commands) == [
        "list", "respond", "ack",
    ]
    assert "remove" in command.commands["node"].commands


def test_runtime_help_includes_sidecar_restart_command():
    result = runner.invoke(app, ["runtime", "--help"])

    assert result.exit_code == 0
    assert "status" in result.stdout
    assert "ensure-context-engine" in result.stdout
    assert "restart-sidecars" in result.stdout


def test_unknown_command_suggests_close_match():
    result = runner.invoke(app, ["job", "shwo"])

    assert result.exit_code == 2
    assert "No such command 'shwo'" in result.stderr
    assert "Did you mean 'show'?" in result.stderr


@pytest.mark.parametrize(
    "args,replacement",
    [
        (["runtime", "health"], "mn runtime status"),
        (["job", "inspect"], "mn job show"),
        (["run", "status"], "mn run show"),
        (["blueprint", "install"], "mn blueprint add"),
    ],
)
def test_removed_paths_return_replacement_without_execution(args, replacement):
    result = runner.invoke(app, [*args, "--json"])

    assert result.exit_code == 2
    payload = json.loads(result.stdout)
    assert payload["schema"] == "mn.cli/v1"
    assert payload["ok"] is False
    assert replacement in payload["error"]["message"]


def test_every_leaf_command_exposes_json():
    command = get_command(app)
    missing = []

    def visit(current, path):
        children = getattr(current, "commands", None)
        if children:
            for name, child in children.items():
                visit(child, [*path, name])
            return
        if not any("--json" in getattr(parameter, "opts", ()) for parameter in current.params):
            missing.append(" ".join(path))

    visit(command, ["mn"])
    assert missing == []


def test_verbose_root_alias_is_removed():
    result = runner.invoke(app, ["--verbose", "runtime", "status"])

    assert result.exit_code == 2
    assert "use '--debug' instead" in result.stderr


@pytest.mark.parametrize(
    "option,replacement",
    [
        ("--worker", "mn runtime start"),
        ("--worker-node", "mn runtime start"),
        ("--join-host", "mn node add HOST --token TOKEN"),
    ],
)
def test_removed_runtime_start_options_have_actionable_json_errors(option, replacement):
    result = runner.invoke(app, ["runtime", "start", option, "--json"])

    assert result.exit_code == 2
    payload = json.loads(result.stdout)
    assert payload["error"]["code"] == "MN_USAGE_ERROR"
    assert replacement in payload["error"]["message"]


def test_runtime_start_has_one_federation_capable_mode():
    result = runner.invoke(app, ["runtime", "start", "--help"])

    assert result.exit_code == 0
    assert "--host" in result.stdout
    assert "--worker" not in result.stdout


def test_streaming_json_uses_ndjson_records():
    streaming_app = typer.Typer()

    @streaming_app.callback()
    def streaming_root():
        pass

    @streaming_app.command("watch")
    def watch():
        emit_stream_record("snapshot", data={"state": "running"})
        emit_stream_record("event", data={"state": "completed"})
        record_result({"state": "completed"})

    instrument_typer(streaming_app)
    result = runner.invoke(streaming_app, ["watch", "--json"])

    assert result.exit_code == 0
    records = [json.loads(line) for line in result.stdout.splitlines()]
    assert [record["type"] for record in records] == ["snapshot", "event", "complete"]
    assert all(record["schema"] == "mn.cli.stream/v1" for record in records)


def test_json_success_preserves_paths_and_redacts_secrets():
    result_app = typer.Typer()

    @result_app.callback()
    def result_root():
        pass

    @result_app.command("show")
    def show():
        record_result(
            {
                "path": "/Users/operator/.mn/models/registry.json",
                "api_key": "sk-private",
                "New token": "cluster-private",
                "message": "MODEL_TOKEN=private-value",
            }
        )

    instrument_typer(result_app)
    result = runner.invoke(result_app, ["show", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["data"]["path"] == "/Users/operator/.mn/models/registry.json"
    assert payload["data"]["api_key"] == "[redacted]"
    assert payload["data"]["New token"] == "[redacted]"
    assert payload["data"]["message"] == "MODEL_TOKEN=[redacted]"


def test_json_error_excludes_internal_diagnostics_and_redacts_details():
    failure_app = typer.Typer()

    @failure_app.callback()
    def failure_root():
        pass

    @failure_app.command("remove")
    def remove():
        raise AppError(
            "MN_PERMISSION_DENIED",
            "Permission denied.",
            internal_message="Bearer private-internal-token",
            hint="Authenticate and retry.",
            details={"token": "private", "path": "/tmp/model.json"},
            exit_code=13,
        )

    instrument_typer(failure_app)
    result = runner.invoke(failure_app, ["remove", "--json"])

    assert result.exit_code == 13
    payload = json.loads(result.stdout)
    assert payload["error"] == {
        "code": "MN_PERMISSION_DENIED",
        "message": "Permission denied.",
        "hint": "Authenticate and retry.",
        "details": {"token": "[redacted]", "path": "/tmp/model.json"},
    }
    assert "private-internal-token" not in result.stdout


def test_json_validation_exit_preserves_message_and_normalizes_generic_code():
    validation_app = typer.Typer()

    @validation_app.callback()
    def validation_root():
        pass

    @validation_app.command("add")
    def add():
        from mn_cli.output import record_error

        record_error(AppError("MN_COMMAND_FAILED", "Choose exactly one schedule mode."))
        raise typer.Exit(2)

    instrument_typer(validation_app)
    result = runner.invoke(validation_app, ["add", "--json"])

    assert result.exit_code == 2
    payload = json.loads(result.stdout)
    assert payload["error"]["code"] == "MN_VALIDATION_FAILED"
    assert payload["error"]["message"] == "Choose exactly one schedule mode."


def test_json_interruption_uses_standard_exit_code():
    interrupted_app = typer.Typer()

    @interrupted_app.callback()
    def interrupted_root():
        pass

    @interrupted_app.command("watch")
    def watch():
        raise KeyboardInterrupt

    instrument_typer(interrupted_app)
    result = runner.invoke(interrupted_app, ["watch", "--json"])

    assert result.exit_code == 130
    payload = json.loads(result.stdout)
    assert payload["error"]["code"] == "MN_INTERRUPTED"
