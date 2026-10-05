"""Native Python platform and wheel capture for air-gapped job transfer."""

from __future__ import annotations

import json
import subprocess
import sys
import os
import re
from pathlib import Path

from mn_sdk.job_backup.airgap import _build_airgap_wheelhouse
from mn_sdk.job_backup.archive import BackupRestoreError
from mn_sdk.runtime_config import resolve_mn_home

PROFILE_SCRIPT = (
    "import json,platform,sys; print(json.dumps({"
    "'os':platform.system().lower(),'architecture':platform.machine().lower(),"
    "'python_implementation':platform.python_implementation().lower(),"
    "'python_abi':sys.implementation.cache_tag,'python_version':platform.python_version()}))"
)


def prepare_job_backup_python(attrs):
    from mn_cli.libs.run_cmds.handlers.doctor import (
        _doctor_running_core_container,
        _doctor_runtime_python_env_path,
        _doctor_path_is_in_core_mount,
    )

    container = _doctor_running_core_container(3, allow_remote_target=True)
    python = ["docker", "exec", container, "python3"] if container else [sys.executable]
    result = subprocess.run(
        [*python, "-c", PROFILE_SCRIPT],
        capture_output=True,
        text=True,
        check=True,
        timeout=10,
    )
    profile = json.loads(result.stdout)
    if attrs["action"] == "inspect_hostlocal_python":
        return {"status": "ready", "compatibility": profile}
    root = Path(str(attrs.get("build_root") or "")).resolve()
    cache = (resolve_mn_home() / "cache" / "job-backup-build").resolve()
    if root.parent != cache or not root.is_dir():
        raise BackupRestoreError(
            "Wheel capture requires a local job backup build directory"
        )
    if container and not _doctor_path_is_in_core_mount(root):
        raise BackupRestoreError("Wheel capture directory is not mounted in Core")
    manifest = json.loads((root / "bundle" / "manifest.json").read_text())
    wheels = root / "wheels"
    wheels.mkdir(exist_ok=True)
    environment = dict(os.environ)
    for record in (
        manifest.get("metadata", {})
        .get("mn_local_skill_dependencies", {})
        .get("sources", [])
    ):
        if record.get("package") and record.get("version"):
            package = re.sub(r"[-.]+", "_", record["package"]).upper()
            environment["SETUPTOOLS_SCM_PRETEND_VERSION_FOR_" + package] = record[
                "version"
            ]

    def run(command, **kwargs):
        arguments = command[1:]
        if container:
            arguments = [
                str(
                    _doctor_runtime_python_env_path(Path(arg), core_container=container)
                )
                if Path(arg).is_absolute() and _doctor_path_is_in_core_mount(Path(arg))
                else arg
                for arg in arguments
            ]
        # Build identities must survive removal of development Git metadata.
        prefix = python
        if container:
            prefix = ["docker", "exec"]
            for name, value in environment.items():
                if name.startswith("SETUPTOOLS_SCM_PRETEND_VERSION_FOR_"):
                    prefix.extend(["-e", name + "=" + value])
            prefix.extend([container, "python3"])
        return subprocess.run([*prefix, *arguments], env=environment, **kwargs)

    _build_airgap_wheelhouse(manifest, root / "bundle", wheels, command_runner=run)
    return {"status": "ready", "compatibility": profile}
