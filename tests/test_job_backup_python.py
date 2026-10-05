import json
import subprocess
import sys
from pathlib import Path

import pytest

from mn_cli.runtime import job_backup_python
from mn_cli.libs.run_cmds.handlers import doctor
from mn_sdk.job_backup.archive import BackupRestoreError


@pytest.mark.parametrize("capture_versions", [False, True])
def test_wheels_are_built_with_core_python_and_mounted_paths(
    tmp_path, monkeypatch, capture_versions
):
    monkeypatch.setenv("MN_HOME", str(tmp_path / "mn"))
    root = tmp_path / "mn/cache/job-backup-build/build-example"
    (root / "bundle").mkdir(parents=True)
    nodes = (
        [
            {
                "config": {
                    "runner_module": "MirrorNeuron.Runner.HostLocal",
                    "python_environment": {"path": "/root/.mn/cache/source-env"},
                }
            }
        ]
        if capture_versions
        else []
    )
    (root / "bundle/manifest.json").write_text(json.dumps({"agents": {"nodes": nodes}}))
    monkeypatch.setattr(
        doctor, "_doctor_running_core_container", lambda *a, **k: "core"
    )
    monkeypatch.setattr(
        doctor,
        "_doctor_path_is_in_core_mount",
        lambda path: path.is_relative_to(tmp_path / "mn"),
    )
    monkeypatch.setattr(
        doctor,
        "_doctor_runtime_python_env_path",
        lambda path, **k: Path("/root/.mn") / path.relative_to(tmp_path / "mn"),
    )
    calls = []
    profile = {"os": "linux", "architecture": "aarch64", "python_abi": "cpython-311"}

    def run(command, **kwargs):
        calls.append(command)
        response = (
            [["Shared_Dependency", "2.4.1"], ["local.skill", "1.3.2.dev4"]]
            if command[:4]
            == ["docker", "exec", "core", "/root/.mn/cache/source-env/bin/python"]
            else profile
        )
        return subprocess.CompletedProcess(command, 0, stdout=json.dumps(response))

    monkeypatch.setattr(job_backup_python.subprocess, "run", run)

    def build(manifest, bundle, wheels, *, command_runner, constraints):
        if capture_versions:
            assert (
                constraints.read_text()
                == "local-skill==1.3.2.dev4\nshared-dependency==2.4.1"
            )
        else:
            assert constraints is None
        command_runner(
            [
                sys.executable,
                "-m",
                "pip",
                "wheel",
                "--wheel-dir",
                str(wheels),
                *(["--constraint", str(constraints)] if constraints else []),
                str(bundle / "payloads/package"),
            ]
        )

    monkeypatch.setattr(job_backup_python, "_build_airgap_wheelhouse", build)
    result = job_backup_python.prepare_job_backup_python(
        {"action": "export_hostlocal_wheels", "build_root": str(root)}
    )
    assert result["compatibility"] == profile
    assert calls[0][:4] == ["docker", "exec", "core", "python3"]
    assert calls[-1] == [
        "docker",
        "exec",
        "core",
        "python3",
        "-m",
        "pip",
        "wheel",
        "--wheel-dir",
        "/root/.mn/cache/job-backup-build/build-example/wheels",
        *(
            [
                "--constraint",
                "/root/.mn/cache/job-backup-build/build-example/installed-versions.txt",
            ]
            if capture_versions
            else []
        ),
        "/root/.mn/cache/job-backup-build/build-example/bundle/payloads/package",
    ]


@pytest.mark.parametrize(
    "installed",
    [
        [[["dependency", "1.0"]], [["Dependency", "2.0"]]],
        [[[None, "1.0"]]],
        [[["dependency", "1.0\nother==2"]]],
    ],
)
def test_source_environment_pins_reject_conflicts_and_invalid_metadata(
    tmp_path, monkeypatch, installed
):
    monkeypatch.setenv("MN_HOME", str(tmp_path / "mn"))
    root = tmp_path / "mn/cache/job-backup-build/build-example"
    (root / "bundle").mkdir(parents=True)
    nodes = [
        {
            "config": {
                "runner_module": "MirrorNeuron.Runner.HostLocal",
                "python_environment": {"path": str(tmp_path / f"env-{index}")},
            }
        }
        for index in range(len(installed))
    ]
    (root / "bundle/manifest.json").write_text(json.dumps({"agents": {"nodes": nodes}}))
    monkeypatch.setattr(doctor, "_doctor_running_core_container", lambda *a, **k: "")
    responses = iter([{}, *installed])
    monkeypatch.setattr(
        job_backup_python.subprocess,
        "run",
        lambda command, **k: subprocess.CompletedProcess(
            command, 0, stdout=json.dumps(next(responses))
        ),
    )
    monkeypatch.setattr(
        job_backup_python,
        "_build_airgap_wheelhouse",
        lambda *a, **k: pytest.fail(
            "Invalid source environments must fail before building wheels"
        ),
    )
    with pytest.raises(BackupRestoreError, match="conflicting|metadata is invalid"):
        job_backup_python.prepare_job_backup_python(
            {"action": "export_hostlocal_wheels", "build_root": str(root)}
        )


def test_wheel_export_rejects_paths_outside_owned_build_cache(tmp_path, monkeypatch):
    monkeypatch.setenv("MN_HOME", str(tmp_path / "mn"))
    monkeypatch.setattr(doctor, "_doctor_running_core_container", lambda *a, **k: "")
    monkeypatch.setattr(
        job_backup_python.subprocess,
        "run",
        lambda command, **k: subprocess.CompletedProcess(command, 0, stdout="{}"),
    )
    with pytest.raises(BackupRestoreError, match="local job backup build"):
        job_backup_python.prepare_job_backup_python(
            {"action": "export_hostlocal_wheels", "build_root": str(tmp_path)}
        )
