import json
import subprocess
import sys
from pathlib import Path

import pytest

from mn_cli.runtime import job_backup_python
from mn_cli.libs.run_cmds.handlers import doctor
from mn_sdk.job_backup.archive import BackupRestoreError


def test_wheels_are_built_with_core_python_and_mounted_paths(tmp_path, monkeypatch):
    monkeypatch.setenv("MN_HOME", str(tmp_path / "mn"))
    root = tmp_path / "mn/cache/job-backup-build/build-example"
    (root / "bundle").mkdir(parents=True)
    (root / "bundle/manifest.json").write_text('{"agents":{"nodes":[]}}')
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
        return subprocess.CompletedProcess(command, 0, stdout=json.dumps(profile))

    monkeypatch.setattr(job_backup_python.subprocess, "run", run)

    def build(manifest, bundle, wheels, *, command_runner):
        command_runner(
            [
                sys.executable,
                "-m",
                "pip",
                "wheel",
                "--wheel-dir",
                str(wheels),
                str(bundle / "payloads/package"),
            ]
        )

    monkeypatch.setattr(job_backup_python, "_build_airgap_wheelhouse", build)
    result = job_backup_python.prepare_job_backup_python(
        {"action": "export_hostlocal_wheels", "build_root": str(root)}
    )
    assert result["compatibility"] == profile
    assert calls[0][:4] == ["docker", "exec", "core", "python3"]
    assert calls[1] == [
        "docker",
        "exec",
        "core",
        "python3",
        "-m",
        "pip",
        "wheel",
        "--wheel-dir",
        "/root/.mn/cache/job-backup-build/build-example/wheels",
        "/root/.mn/cache/job-backup-build/build-example/bundle/payloads/package",
    ]


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
