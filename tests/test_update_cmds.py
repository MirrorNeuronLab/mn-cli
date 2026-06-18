import json
import io
import tarfile

import pytest
from typer.testing import CliRunner

from mn_cli import update_cmds
from mn_cli.main import app


runner = CliRunner()


@pytest.fixture(autouse=True)
def isolated_install_state(mocker, tmp_path):
    mocker.patch("mn_cli.update_cmds.DIR", tmp_path / ".mn")
    mocker.patch(
        "mn_cli.update_cmds.INSTALL_METADATA_FILE",
        tmp_path / ".mn" / "install_metadata.json",
    )
    mocker.patch(
        "mn_cli.update_cmds.CHECK_FILE",
        tmp_path / ".mn" / ".update-check.json",
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

    result = runner.invoke(app, ["runtime", "update", "--check-only"])

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

    result = runner.invoke(app, ["runtime", "update"], input="n\n")

    assert result.exit_code == 0
    assert "Updating will stop all MirrorNeuron components" in result.stdout
    assert "Update cancelled" in result.stdout
    mock_perform.assert_not_called()


def test_update_skips_release_flow_for_local_source_install(mocker, tmp_path):
    metadata_file = tmp_path / "install_metadata.json"
    metadata_file.write_text(json.dumps({"install_type": "local_source"}))
    mocker.patch("mn_cli.update_cmds.INSTALL_METADATA_FILE", metadata_file)
    mock_get_updates = mocker.patch("mn_cli.update_cmds.get_available_updates")

    result = runner.invoke(app, ["runtime", "update"])

    assert result.exit_code == 0
    assert "Local source install detected" in result.stdout
    assert "Do you want to update now?" not in result.stdout
    mock_get_updates.assert_not_called()


def test_local_source_install_ignores_invalid_metadata_but_detects_source_tree():
    update_cmds.INSTALL_METADATA_FILE.parent.mkdir(parents=True)
    update_cmds.INSTALL_METADATA_FILE.write_text("{", encoding="utf-8")

    assert update_cmds._local_source_install() is False

    (update_cmds.DIR / "core-source").mkdir()
    assert update_cmds._local_source_install() is True


def test_check_due_treats_invalid_or_non_numeric_check_file_as_due():
    update_cmds.CHECK_FILE.parent.mkdir(parents=True)
    update_cmds.CHECK_FILE.write_text("{", encoding="utf-8")
    assert update_cmds._check_due() is True

    update_cmds.CHECK_FILE.write_text(json.dumps({"checked_at": "not-a-time"}), encoding="utf-8")
    assert update_cmds._check_due() is True


def test_update_yes_stops_updates_and_restarts(mocker, capsys):
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

    output = capsys.readouterr().out
    assert "MirrorNeuron update successful." in output
    assert "Status: installed" in output
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
    assert command[-len(update_cmds.PYPI_PACKAGES):] == update_cmds.PYPI_PACKAGES
    assert "mirrorneuron-blueprint-support-skill[webui]" not in command
    assert "mirrorneuron-blueprint-support-skill" not in update_cmds.PYPI_PACKAGES


def test_web_ui_update_uses_configured_package_name(mocker, tmp_path):
    (tmp_path / "package.json").write_text("{}", encoding="utf-8")
    mocker.patch("mn_cli.update_cmds.WEB_UI_DIRS", [tmp_path])
    mock_run = mocker.patch("mn_cli.update_cmds.subprocess.run")

    update_cmds._update_web_ui()

    assert mock_run.call_args.args[0] == [
        "npm",
        "--prefix",
        str(tmp_path),
        "install",
        f"{update_cmds.NPM_PACKAGE}@latest",
    ]


def test_safe_extract_tar_extracts_valid_release_member(tmp_path):
    tar_path = tmp_path / "core.tar.gz"
    contents = b"ok\n"
    with tarfile.open(tar_path, "w:gz") as archive:
        info = tarfile.TarInfo("mirror_neuron/bin/mirror_neuron")
        info.size = len(contents)
        archive.addfile(info, io.BytesIO(contents))

    target = tmp_path / "install"
    with tarfile.open(tar_path) as archive:
        update_cmds._safe_extract_tar(archive, target)

    assert (target / "mirror_neuron" / "bin" / "mirror_neuron").read_bytes() == contents


def test_prepare_core_docker_context_copies_release_and_writes_dockerfile(tmp_path):
    install_dir = tmp_path / "install"
    core_bin = install_dir / "mirror_neuron" / "bin"
    core_bin.mkdir(parents=True)
    (core_bin / "mirror_neuron").write_text("run\n", encoding="utf-8")
    context_dir = tmp_path / "context"
    context_dir.mkdir()

    update_cmds._prepare_core_docker_context(context_dir, install_dir)

    copied_binary = context_dir / "mirror_neuron" / "bin" / "mirror_neuron"
    assert copied_binary.read_text(encoding="utf-8") == "run\n"
    dockerfile = (context_dir / "Dockerfile").read_text(encoding="utf-8")
    assert "COPY mirror_neuron /opt/mirror_neuron" in dockerfile
    assert 'CMD ["bin/mirror_neuron", "foreground"]' in dockerfile


def test_clear_core_install_dir_preserves_runtime_state(tmp_path):
    install_dir = tmp_path / "install"
    (install_dir / "mirror_neuron").mkdir(parents=True)
    (install_dir / "mirror_neuron" / "old").write_text("old\n", encoding="utf-8")
    (install_dir / "stale.txt").write_text("stale\n", encoding="utf-8")
    for preserved_name in update_cmds.CORE_INSTALL_PRESERVE_NAMES:
        preserved = install_dir / preserved_name
        if "." in preserved_name[1:]:
            preserved.write_text("keep\n", encoding="utf-8")
        else:
            preserved.mkdir()

    update_cmds._clear_core_install_dir(install_dir)

    assert not (install_dir / "mirror_neuron").exists()
    assert not (install_dir / "stale.txt").exists()
    for preserved_name in update_cmds.CORE_INSTALL_PRESERVE_NAMES:
        assert (install_dir / preserved_name).exists()


@pytest.mark.parametrize(
    "member_name",
    ["/tmp/escape", "mirror_neuron/../../escape"],
)
def test_safe_extract_tar_rejects_unsafe_member_paths(tmp_path, member_name):
    tar_path = tmp_path / "core.tar.gz"
    with tarfile.open(tar_path, "w:gz") as archive:
        info = tarfile.TarInfo(member_name)
        info.size = 1
        archive.addfile(info, io.BytesIO(b"x"))

    with tarfile.open(tar_path) as archive:
        with pytest.raises(RuntimeError, match="unsafe path"):
            update_cmds._safe_extract_tar(archive, tmp_path / "install")


def test_safe_extract_tar_rejects_unsafe_symlink(tmp_path):
    tar_path = tmp_path / "core.tar.gz"
    with tarfile.open(tar_path, "w:gz") as archive:
        info = tarfile.TarInfo("mirror_neuron/bin/python")
        info.type = tarfile.SYMTYPE
        info.linkname = "../../escape"
        archive.addfile(info)

    with tarfile.open(tar_path) as archive:
        with pytest.raises(RuntimeError, match="unsafe symlink"):
            update_cmds._safe_extract_tar(archive, tmp_path / "install")
