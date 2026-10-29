"""Cross-machine source paths must not depend on the submitter's OS or username."""

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


def project(path):
    path.mkdir(parents=True)
    path.joinpath("pyproject.toml").write_text(
        "[tool.setuptools_scm]\nfallback_version = '0.0.0'\n", encoding="utf-8",
    )
    path.joinpath("client.py").write_text("VALUE = 1\n", encoding="utf-8")
    return path


@pytest.mark.parametrize("origin", [
    "/Users/submitter/Work", "/home/submitter/build",
    "D:/Work trees/checkout", r"D:\Work trees\checkout",
    r"\\build-server\source share\checkout",
])
@pytest.mark.parametrize("relative", [
    "Membrane/mn-context-engine-python-sdk", "mn-skills/video_tools",
    "new-component/python/client",
])
def test_rebases_verified_checkout_projects_from_any_system(tmp_path, mocker, origin, relative):
    workspace = tmp_path / "workspace"
    source = project(workspace / relative)
    missing = origin + "/" + relative + "[grpc,context]"
    mocker.patch.object(doctor, "_doctor_source_checkout_roots", return_value=[workspace])

    packages, versions = doctor._doctor_rebase_missing_local_sources(
        [missing, "requests==2.32.0"], local_source_versions={missing: "2.1.7"},
    )

    assert packages == [str(source) + "[grpc,context]", "requests==2.32.0"]
    assert versions[packages[0]] == "2.1.7"


@pytest.mark.parametrize("origin", ["/Users/submitter/Work", r"C:\Work\checkout"])
def test_remote_context_source_reaches_staged_pip_transaction(tmp_path, mocker, origin):
    workspace = tmp_path / "workspace"
    source = project(workspace / "Membrane" / "mn-context-engine-python-sdk")
    missing = origin + "/Membrane/mn-context-engine-python-sdk[grpc]"
    mocker.patch.object(doctor, "_doctor_source_checkout_roots", return_value=[workspace])
    mocker.patch.object(doctor, "_doctor_running_core_container", return_value="mirror-neuron-core")
    calls = []

    def run(command, **_kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(
            command, 0, stdout="Python 3.12.2\n" if "--version" in command else "", stderr="",
        )

    mocker.patch.object(doctor.subprocess, "run", side_effect=run)
    env_dir = doctor._doctor_prepare_python_env_from_content(
        blueprint_id="context-service", node_id="mcp", packages=[missing],
        requirements_content="", timeout=1, local_source_versions={missing: "2.1.7"},
    )

    staged = env_dir.parent.parent / "blueprint-python-sources" / env_dir.name / ("0-" + source.name)
    runtime_source = Path("/root/.mn/cache/blueprint-python-sources") / env_dir.name / staged.name
    assert str(runtime_source) + "[grpc]" in calls[-1]
    assert missing not in calls[-1]
    assert str(source) + "[grpc]" not in calls[-1]
    assert 'fallback_version = "2.1.7"' in staged.joinpath("pyproject.toml").read_text()
    assert "0.0.0" in source.joinpath("pyproject.toml").read_text()


@pytest.mark.parametrize("requirement", [
    "relative/mn-python-sdk[context]", "requests==2.32.0", "--index-url",
    "https://example.test/mn-python-sdk", "/submitter/missing-project[grpc]",
    "/submitter/mn-python-sdk[../../secret]", "/submitter/../mn-python-sdk[context]",
    r"C:\submitter\..\mn-python-sdk[context]",
])
def test_unresolved_or_unsafe_requirements_are_left_for_validation(tmp_path, mocker, requirement):
    workspace = tmp_path / "workspace"
    project(workspace / "mn-python-sdk")
    mocker.patch.object(doctor, "_doctor_source_checkout_roots", return_value=[workspace])
    assert doctor._doctor_rebase_missing_local_source(requirement) == requirement


def test_rebasing_does_not_follow_a_checkout_symlink_outside_its_root(tmp_path, mocker):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    outside = project(tmp_path / "outside")
    workspace.joinpath("context-sdk").symlink_to(outside, target_is_directory=True)
    mocker.patch.object(doctor, "_doctor_source_checkout_roots", return_value=[workspace])
    missing = "/submitter/context-sdk[grpc]"
    assert doctor._doctor_rebase_missing_local_source(missing) == missing


def test_ambiguous_checkout_sources_are_not_selected(tmp_path, mocker):
    roots = [tmp_path / "first", tmp_path / "second"]
    for root in roots:
        project(root / "Membrane" / "context-sdk")
    mocker.patch.object(doctor, "_doctor_source_checkout_roots", return_value=roots)
    missing = "/submitter/Membrane/context-sdk[grpc]"
    assert doctor._doctor_rebase_missing_local_source(missing) == missing


def test_rebasing_preserves_existing_local_sources(tmp_path, mocker):
    source = project(tmp_path / "own-checkout" / "context-sdk")
    mocker.patch.object(doctor, "_doctor_source_checkout_roots", return_value=[])
    requirement = str(source) + "[grpc]"
    assert doctor._doctor_rebase_missing_local_source(requirement) == requirement


def test_rebases_a_local_wheel_without_repository_name_assumptions(tmp_path, mocker):
    workspace = tmp_path / "workspace"
    wheel = workspace / "new-component" / "client-2.1.7-py3-none-any.whl"
    wheel.parent.mkdir(parents=True)
    wheel.write_bytes(b"wheel fixture")
    mocker.patch.object(doctor, "_doctor_source_checkout_roots", return_value=[workspace])
    missing = "D:/checkout/new-component/" + wheel.name + "[grpc]"
    assert doctor._doctor_rebase_missing_local_source(missing) == str(wheel) + "[grpc]"
