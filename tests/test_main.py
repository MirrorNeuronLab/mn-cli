from importlib import metadata
import json

import pytest
from typer.testing import CliRunner

from mn_cli.banner import format_banner
from mn_cli.main import app

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
    assert max(len(line.rstrip("\n")) for line in result.stdout.splitlines() if line) <= 80


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
    assert "Submit, inspect, control, and recover workflow jobs." in result.stdout


def test_command_help_includes_argument_description_and_examples():
    result = runner.invoke(app, ["job", "submit", "--help"])

    assert result.exit_code == 0
    assert "Path to a workflow manifest JSON file." in result.stdout
    assert "Examples:" in result.stdout
    assert "mn job submit ./manifest.json" in result.stdout


def test_job_status_includes_resource_usage_when_run_data_is_available(mocker, tmp_path):
    mocker.patch(
        "mn_cli.libs.job_cmds.client.get_job",
        return_value=json.dumps({"job": {"job_id": "job-1", "run_id": "run-1", "status": "running"}}),
    )
    mocker.patch("mn_cli.libs.job_cmds.default_runs_root", return_value=tmp_path)
    mocker.patch(
        "mn_cli.libs.job_cmds.load_observability_tools",
        return_value={
            "read_run_resources": lambda run_id, runs_root=None: {
                "run_id": run_id,
                "llm": {"input_tokens": 12, "output_tokens": 4, "total_tokens": 16, "calls": 1},
                "buckets": [],
            }
        },
    )

    result = runner.invoke(app, ["job", "status", "job-1"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["resource_usage"]["llm"]["input_tokens"] == 12
    assert payload["resource_usage"]["llm"]["output_tokens"] == 4
    assert payload["resource_usage"]["llm"]["total_tokens"] == 16


def test_runtime_help_includes_sidecar_restart_command():
    result = runner.invoke(app, ["runtime", "--help"])

    assert result.exit_code == 0
    assert "status" in result.stdout
    assert "ensure-context-engine" in result.stdout
    assert "restart-sidecars" in result.stdout


def test_unknown_command_suggests_close_match():
    result = runner.invoke(app, ["job", "sumbit"])

    assert result.exit_code == 2
    assert "No such command 'sumbit'" in result.stderr
    assert "Did you mean 'submit'?" in result.stderr
