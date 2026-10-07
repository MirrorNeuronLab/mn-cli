"""Regression coverage for local SDK/component installs with optional extras."""

import subprocess
from pathlib import Path

import pytest

from mn_cli.libs.run_cmds.handlers import doctor


@pytest.fixture(autouse=True)
def isolated_runtime(tmp_path, monkeypatch):
    monkeypatch.setenv("MN_HOME", str(tmp_path / "mn-home"))
    monkeypatch.setenv("MN_WORKSPACE_ROOT", str(tmp_path / "workspace"))
    for name in (
        "MN_SHARED_STORAGE_ROOT", "MN_HOST_SHARED_STORAGE_ROOT",
        "MN_RUNTIME_SHARED_STORAGE_ROOT", "MN_CONTAINER_SHARED_STORAGE_ROOT",
        "MN_BLUEPRINT_PYTHON_ENVS_DIR", "MN_CONTAINER_BLUEPRINT_PYTHON_ENVS_DIR",
    ):
        monkeypatch.delenv(name, raising=False)


def source_project(root):
    source = root / "mn-python-sdk"
    source.mkdir(parents=True)
    source.joinpath("pyproject.toml").write_text(
        "[tool.setuptools_scm]\nfallback_version = '0.0.0'\n",
        encoding="utf-8",
    )
    source.joinpath("example.py").write_text("VALUE = 1\n", encoding="utf-8")
    return source


@pytest.mark.parametrize("container", ["", "mirror-neuron-core"])
def test_preparation_stages_source_with_extras_and_declared_version(tmp_path, mocker, container):
    source = source_project(tmp_path / "workspace")
    package = str(source) + "[context,grpc]"
    manifest = {"metadata": {"mn_local_skill_dependencies": {"sources": [
        {"source": str(source), "version": "1.3.58.dev45", "extras": "[context,grpc]"},
    ]}}}
    versions = doctor._doctor_hostlocal_local_source_versions(manifest, [package])
    assert versions == {package: "1.3.58.dev45"}
    mocker.patch.object(doctor, "_doctor_running_core_container", return_value=container)
    calls = []

    def run(command, **_kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(
            command, 0, stdout="Python 3.11.2\n" if "--version" in command else "", stderr="",
        )

    mocker.patch.object(doctor.subprocess, "run", side_effect=run)
    env_dir = doctor._doctor_prepare_python_env_from_content(
        blueprint_id="source-extras", node_id="collector", packages=[package],
        requirements_content="", timeout=1, local_source_versions=versions,
    )
    staged = env_dir.parent.parent / "blueprint-python-sources" / env_dir.name / source.name
    staged = staged.with_name("0-" + staged.name)
    runtime_source = (
        Path("/root/.mn/cache/blueprint-python-sources") / env_dir.name / staged.name
        if container else staged
    )
    assert str(runtime_source) + "[context,grpc]" in calls[-1]
    assert package not in calls[-1]
    assert staged.joinpath("example.py").read_text() == "VALUE = 1\n"
    assert 'fallback_version = "1.3.58.dev45"' in staged.joinpath("pyproject.toml").read_text()
    assert "0.0.0" in source.joinpath("pyproject.toml").read_text()


def test_rebasing_retains_extras_and_version_identity(tmp_path, mocker):
    source = source_project(tmp_path / "workspace")
    missing = "/missing/checkout/mn-python-sdk[context]"
    mocker.patch.object(doctor, "_doctor_source_checkout_roots", return_value=[source.parent])
    packages, versions = doctor._doctor_rebase_missing_local_sources(
        [missing, "requests==2.32.0"], local_source_versions={missing: "1.3.58.dev45"},
    )
    assert packages == [str(source) + "[context]", "requests==2.32.0"]
    assert versions[packages[0]] == "1.3.58.dev45"


def test_extras_do_not_expand_trusted_source_roots(tmp_path):
    outside = source_project(tmp_path / "untrusted")
    assert doctor._doctor_workspace_local_source(str(outside) + "[context]") is None


def test_invalid_extras_remain_unresolved(tmp_path):
    source = source_project(tmp_path / "workspace")
    assert doctor._doctor_workspace_local_source(str(source) + "[../../secret]") is None


def test_staged_wheel_preserves_extras(tmp_path):
    source = tmp_path / "workspace" / "example-1.0-py3-none-any.whl"
    source.parent.mkdir()
    source.write_bytes(b"wheel fixture")
    assert doctor._doctor_workspace_local_source(str(source) + "[context]") == source


def test_pip_failure_retains_error_before_verbose_install_progress(mocker):
    mocker.patch.object(doctor, "_doctor_running_core_container", return_value="")

    def run(command, **_kwargs):
        if "install" in command:
            return subprocess.CompletedProcess(
                command, 1, stdout="Collecting dependency\n" * 100,
                stderr="ERROR: dependency resolution failed\n",
            )
        return subprocess.CompletedProcess(command, 0, stdout="Python 3.11.2\n", stderr="")

    mocker.patch.object(doctor.subprocess, "run", side_effect=run)
    with pytest.raises(RuntimeError, match="ERROR: dependency resolution failed"):
        doctor._doctor_prepare_python_env_from_content(
            blueprint_id="pip-failure", node_id="collector", packages=["example==1.0"],
            requirements_content="", timeout=1,
        )


def test_mac_local_only_workflow_prepares_native_python(tmp_path, mocker):
    env = tmp_path / "prepared-env"
    config = {"runner_module": "MirrorNeuron.Runner.HostLocal",
              "python_environment": {"packages": ["example==1.0"]}}
    manifest = {"runtime": {"placement": {"must_run_local": True}},
                "requirements": {"os": "darwin"}, "agents": {"nodes": [{"node_id": "collector", "config": config}]}}
    prepare = mocker.patch.object(doctor, "_doctor_prepare_python_env", return_value=env)
    mocker.patch.object(doctor, "prepare_hostlocal_python_bridge", return_value=tmp_path / "bridge")
    mocker.patch.object(doctor, "_doctor_running_core_container", return_value="")
    mocker.patch.object(doctor, "_doctor_runtime_python_env_path", return_value=tmp_path / "bridge")
    report = doctor._doctor_prepare_hostlocal_python_envs(tmp_path, manifest, timeout=1, check_only=False)
    assert report["status"] == "passing"
    assert prepare.call_args.kwargs["native_host"] is True
    assert config["mn_native_host_python"]["python"] == str(env / "bin/python")
    assert config["mn_native_host_python"]["environment"]["path"] == str(env)
    assert config["python_environment"]["path"] == str(tmp_path / "bridge")
