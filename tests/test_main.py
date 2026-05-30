from importlib import metadata

import pytest
from typer.testing import CliRunner

from mn_cli.banner import format_banner
from mn_cli.main import app

runner = CliRunner()


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
