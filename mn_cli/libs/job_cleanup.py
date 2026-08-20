from __future__ import annotations

import hashlib
import json
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

from mn_sdk import cleanup_docker_worker_services
from mn_sdk.runtime_config import default_runs_root

from mn_cli.libs.blueprint_resources import (
    cleanup_blueprint_host_hooks,
    cleanup_web_ui_process,
    default_generated_bundles_dir,
)

_RESOURCE_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


class JobResourceCleanupError(RuntimeError):
    """Raised when a cleared job still has adapter-owned local resources."""


def cleanup_cancelled_job_resources(
    job_id: str, *, runtime_client: Any, log: Any
) -> dict[str, list[str]]:
    summary: dict[str, list[str]] = {
        "process_removed": [],
        "process_skipped": [],
        "errors": [],
    }
    original_job_id = job_id
    job_id = _validated_resource_id(job_id)
    if job_id is None:
        log.warning(
            "Refusing local cleanup for invalid job ID: %r", original_job_id
        )
        summary["errors"].append(f"invalid job ID: {original_job_id!r}")
        return summary

    run_id = blueprint_run_id_for_job(job_id, runtime_client=runtime_client)
    if run_id:
        run_dir = default_runs_root() / run_id
        if run_dir.is_dir() and not run_dir.is_symlink():
            cleanup_blueprint_host_hooks(
                run_dir, dry_run=False, summary=summary, reason="job_cancelled"
            )
            cleanup_web_ui_process(
                run_dir, dry_run=False, summary=summary, reason="job_cancelled"
            )

    cleanup_local_openshell_sandboxes(job_id, summary)
    for error in summary["errors"]:
        log.warning("Failed to cleanup local resources for job %s: %s", job_id, error)
    return summary


def cleanup_job_resources(
    job_id: str, *, runtime_client: Any, log: Any
) -> None:
    original_job_id = job_id
    job_id = _validated_resource_id(job_id)
    if job_id is None:
        if original_job_id:
            log.warning(
                "Refusing local cleanup for invalid job ID: %r", original_job_id
            )
        raise JobResourceCleanupError(f"invalid job ID: {original_job_id!r}")

    run_id = blueprint_run_id_for_job(job_id, runtime_client=runtime_client)
    cancelled_summary = (
        cleanup_cancelled_job_resources(
            job_id, runtime_client=runtime_client, log=log
        )
        or {}
    )
    errors = list(cancelled_summary.get("errors") or [])
    try:
        docker_result = cleanup_docker_worker_services(job_id=job_id)
        if isinstance(docker_result, dict):
            errors.extend(str(error) for error in docker_result.get("errors") or [])
    except Exception as error:
        errors.append(f"DockerWorker cleanup failed: {error}")
        log.warning(
            "Failed to cleanup DockerWorker resources for cleared job %s",
            job_id,
            exc_info=True,
        )

    paths = [Path(f"/tmp/mn_{job_id}")]
    if run_id:
        paths.extend(
            [
                default_runs_root() / run_id,
                default_generated_bundles_dir() / run_id,
            ]
        )

    for path in paths:
        try:
            if path.is_symlink():
                path.unlink()
            else:
                shutil.rmtree(path)
        except FileNotFoundError:
            pass
        except OSError as error:
            errors.append(f"failed to remove {path}: {error}")
            log.warning("Failed to remove cleared job path %s", path, exc_info=True)

    if errors:
        raise JobResourceCleanupError(
            f"local cleanup incomplete for {job_id}: {'; '.join(errors)}"
        )


def blueprint_run_id_for_job(job_id: str, *, runtime_client: Any) -> str | None:
    job_id = _validated_resource_id(job_id)
    if job_id is None:
        return None

    run_id = blueprint_run_id_from_run_store(job_id)
    if run_id:
        return run_id

    snapshot_path = Path(f"/tmp/mn_{job_id}") / "job_snapshot.json"
    if snapshot_path.is_file():
        try:
            snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
            run_id = snapshot.get("run_id")
            if validated_run_id := _validated_resource_id(run_id):
                return validated_run_id
        except (OSError, json.JSONDecodeError):
            pass

    try:
        job = json.loads(runtime_client.get_job(job_id))
    except Exception:  # noqa: BLE001 - transport implementations expose different errors
        return None

    metadata = ((job.get("job") or {}).get("manifest") or {}).get("metadata") or {}
    mn_cli_metadata = metadata.get("mn_cli") if isinstance(metadata, dict) else {}
    run_id = (
        mn_cli_metadata.get("blueprint_run_id")
        if isinstance(mn_cli_metadata, dict)
        else None
    )
    return _validated_resource_id(run_id)


def blueprint_run_id_from_run_store(job_id: str) -> str | None:
    job_id = _validated_resource_id(job_id)
    if job_id is None:
        return None

    runs_root = default_runs_root()
    if not runs_root.is_dir():
        return None

    for job_file in runs_root.glob("*/job.json"):
        try:
            payload = json.loads(job_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue

        if payload.get("job_id") == job_id:
            run_id = payload.get("run_id") or job_file.parent.name
            return _validated_resource_id(run_id)

    return None


def cleanup_local_openshell_sandboxes(
    job_id: str, summary: dict[str, list[str]]
) -> None:
    cleanup_prepared_openshell_sandbox(job_id, summary)

    try:
        result = subprocess.run(
            [
                "docker",
                "ps",
                "-a",
                "--filter",
                f"name=openshell-mirror-neuron-job-{job_id}",
                "--format",
                "{{.Names}}",
            ],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        summary["errors"].append(
            f"Failed to list OpenShell sandboxes for {job_id}: {error}"
        )
        return

    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        summary["errors"].append(
            f"Failed to list OpenShell sandboxes for {job_id}: {detail}"
        )
        return

    for name in [line.strip() for line in result.stdout.splitlines() if line.strip()]:
        try:
            remove = subprocess.run(
                ["docker", "rm", "-f", name],
                capture_output=True,
                text=True,
                timeout=20,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            summary["errors"].append(
                f"Failed to remove OpenShell sandbox {name}: {error}"
            )
            continue

        if remove.returncode == 0:
            summary["process_removed"].append(name)
        else:
            detail = remove.stderr.strip() or remove.stdout.strip()
            summary["errors"].append(
                f"Failed to remove OpenShell sandbox {name}: {detail}"
            )


def cleanup_prepared_openshell_sandbox(
    job_id: str, summary: dict[str, list[str]]
) -> None:
    sandbox_name = openshell_sandbox_name(job_id)
    executable = Path.home() / ".local" / "bin" / "openshell"
    command = str(executable) if executable.is_file() else "openshell"

    try:
        existing = subprocess.run(
            [command, "sandbox", "get", sandbox_name],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return

    if existing.returncode != 0:
        return

    try:
        removed = subprocess.run(
            [command, "sandbox", "delete", sandbox_name],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        summary["errors"].append(
            f"Failed to delete prepared OpenShell sandbox {sandbox_name}: {error}"
        )
        return

    if removed.returncode == 0:
        summary["process_removed"].append(sandbox_name)
    else:
        detail = removed.stderr.strip() or removed.stdout.strip()
        summary["errors"].append(
            f"Failed to delete prepared OpenShell sandbox {sandbox_name}: {detail}"
        )


def openshell_sandbox_name(job_id: str) -> str:
    raw_job_id = _validated_resource_id(job_id)
    if raw_job_id is None:
        raise ValueError(f"invalid job ID: {job_id!r}")

    base = re.sub(
        r"[^a-z0-9-]",
        "-",
        f"mirror-neuron-job-{raw_job_id}".lower(),
    ).strip("-")
    digest = hashlib.sha256(raw_job_id.encode("utf-8")).hexdigest()[:10]
    suffix = f"-{digest}"
    return f"{base[: max(63 - len(suffix), 1)].rstrip('-')}{suffix}"


def _validated_resource_id(value: Any) -> str | None:
    if not isinstance(value, str):
        return None

    return value if _RESOURCE_ID_PATTERN.fullmatch(value) else None
