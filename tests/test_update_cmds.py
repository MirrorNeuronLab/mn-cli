import json

import pytest
from typer.testing import CliRunner

from mn_cli import update_cmds
from mn_cli.main import app


runner = CliRunner()


@pytest.fixture(autouse=True)
def isolated_install_state(mocker, tmp_path):
    mocker.patch("mn_cli.update_cmds.DIR", tmp_path / ".mirror_neuron")
    mocker.patch(
        "mn_cli.update_cmds.INSTALL_METADATA_FILE",
        tmp_path / ".mirror_neuron" / "install_metadata.json",
    )
    mocker.patch(
        "mn_cli.update_cmds.CHECK_FILE",
        tmp_path / ".mirror_neuron" / ".update-check.json",
    )


def test_update_check_only_prints_available_updates(mocker):
    mocker.patch(
        "mn_cli.update_cmds.get_available_updates",
        return_value=[
            {
                "component": "mirrorneuron-cli",
                "current": "1.0.0",
                "latest": "1.1.0",
                "kind": "pypi",
            }
        ],
    )
    mock_perform = mocker.patch("mn_cli.update_cmds.perform_update")

    result = runner.invoke(app, ["update", "--check-only"])

    assert result.exit_code == 0
    assert "mirrorneuron-cli: 1.0.0 -> 1.1.0" in result.stdout
    mock_perform.assert_not_called()


def test_update_requires_ack_by_default(mocker):
    mocker.patch(
        "mn_cli.update_cmds.get_available_updates",
        return_value=[
            {
                "component": "MirrorNeuron core",
                "current": "v1.0.0",
                "latest": "v1.1.0",
                "kind": "core",
            }
        ],
    )
    mock_perform = mocker.patch("mn_cli.update_cmds.perform_update")

    result = runner.invoke(app, ["update"], input="n\n")

    assert result.exit_code == 0
    assert "Updating will stop all MirrorNeuron components" in result.stdout
    assert "Update cancelled" in result.stdout
    mock_perform.assert_not_called()


def test_update_skips_release_flow_for_local_source_install(mocker, tmp_path):
    metadata_file = tmp_path / "install_metadata.json"
    metadata_file.write_text(json.dumps({"install_type": "local_source"}))
    mocker.patch("mn_cli.update_cmds.INSTALL_METADATA_FILE", metadata_file)
    mock_get_updates = mocker.patch("mn_cli.update_cmds.get_available_updates")

    result = runner.invoke(app, ["update"])

    assert result.exit_code == 0
    assert "Local source install detected" in result.stdout
    assert "Do you want to update now?" not in result.stdout
    mock_get_updates.assert_not_called()


def test_update_yes_stops_updates_and_restarts(mocker):
    updates = [
        {
            "component": "mirrorneuron-cli",
            "current": "1.0.0",
            "latest": "1.1.0",
            "kind": "pypi",
        },
        {
            "component": "mirrorneuron-web-ui",
            "current": "1.0.0",
            "latest": "1.1.0",
            "kind": "npm",
        },
        {
            "component": "MirrorNeuron core",
            "current": "v1.0.0",
            "latest": "v1.1.0",
            "kind": "core",
        },
    ]
    mock_stop = mocker.patch("mn_cli.libs.sys_cmds.stop")
    mock_python = mocker.patch("mn_cli.update_cmds._update_python_packages")
    mock_web = mocker.patch("mn_cli.update_cmds._update_web_ui")
    mock_core = mocker.patch("mn_cli.update_cmds._update_core")
    mock_record = mocker.patch("mn_cli.update_cmds._record_check")
    mock_start = mocker.patch("mn_cli.update_cmds._start_server")

    update_cmds.perform_update(updates)

    mock_stop.assert_called_once()
    mock_python.assert_called_once()
    mock_web.assert_called_once()
    mock_core.assert_called_once()
    mock_record.assert_called_once()
    mock_start.assert_called_once()


def test_available_updates_compares_release_channels(mocker, tmp_path):
    metadata_file = tmp_path / "install_metadata.json"
    metadata_file.write_text(json.dumps({"core_release_tag": "v1.0.0"}))
    mocker.patch("mn_cli.update_cmds.INSTALL_METADATA_FILE", metadata_file)
    mocker.patch(
        "mn_cli.update_cmds._installed_python_version",
        side_effect=lambda name: "1.0.0" if name == "mirrorneuron-cli" else "1.1.0",
    )
    mocker.patch("mn_cli.update_cmds._pypi_latest_version", return_value="1.1.0")
    mocker.patch("mn_cli.update_cmds._web_ui_installed", return_value=True)
    mocker.patch("mn_cli.update_cmds._installed_npm_version", return_value="1.0.0")
    mocker.patch("mn_cli.update_cmds._npm_latest_version", return_value="1.1.0")
    mocker.patch("mn_cli.update_cmds._github_latest_release", return_value={"tag_name": "v1.1.0"})

    updates = update_cmds.get_available_updates()

    assert {item["component"] for item in updates} == {
        "mirrorneuron-cli",
        "mirrorneuron-web-ui",
        "MirrorNeuron core",
    }


def test_python_package_updates_exclude_blueprint_support_skill(mocker):
    mock_run = mocker.patch("mn_cli.update_cmds.subprocess.run")

    update_cmds._update_python_packages()

    command = mock_run.call_args.args[0]
    assert "mirrorneuron-blueprint-support-skill[webui]" not in command
    assert "mirrorneuron-blueprint-support-skill" not in update_cmds.PYPI_PACKAGES
