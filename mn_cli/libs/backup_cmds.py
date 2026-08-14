from __future__ import annotations

import copy
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import zipfile
from pathlib import Path, PurePosixPath
from typing import Any

import typer

from mn_sdk.runtime_config import default_runs_root
from mn_sdk.skill_dependencies import GAR_PIP_INDEX_URL, PYPI_PIP_INDEX_URL

from mn_cli.error_handler import handle_cli_error
from mn_cli.libs.airgap import (
    AIRGAP_MARKER_SCHEMA_VERSION,
    AirGapError,
    compatibility_profile,
    hydrate_payload_models,
    validate_compatibility,
)
from mn_cli.libs.artifacts import blob_store_path, install_blob_file
from mn_cli.libs.blueprint_observability import make_blueprint_run_id
from mn_cli.libs.ui import print_error, print_success_confirmation, print_warning
from mn_cli.shared import client, console, logger

SCHEMA_VERSION = "mn.backup.v2"
CHECKSUMS_ENTRY = "checksums.json"
REQUIRED_ENTRIES = {
    "mn-backup.json",
    "runtime/job.json",
    "runtime/agents.json",
    "runtime/events.jsonl",
    "bundle/manifest.json",
    CHECKSUMS_ENTRY,
}
PAUSED_STATUS = "paused"


class BackupRestoreError(Exception):
    pass


def backup(
    identifier: str = typer.Argument(..., help="Job ID, run ID, or blueprint ID."),
    output: Path = typer.Option(
        ...,
        "--output",
        "-o",
        help="Folder where the backup zip should be written.",
        file_okay=False,
        dir_okay=True,
    ),
    air_gapped: bool = typer.Option(
        False,
        "--air-gapped",
        help="Include every local package, image, model, and blob required to restore without internet.",
    ),
) -> None:
    """Export a paused blueprint job into a restorable zip archive."""
    try:
        target = _resolve_backup_target(identifier)
        job = target["job"]
        job_id = target["job_id"]

        if job.get("status") != PAUSED_STATUS:
            raise BackupRestoreError(
                f"Job {job_id} must be paused before backup; current status is {job.get('status', 'unknown')}."
            )

        if not hasattr(client, "export_job_backup"):
            raise BackupRestoreError(
                "This mn SDK does not support backup yet. Update mirrorneuron-python-sdk and try again."
            )

        print_warning(
            console,
            "Backups are complete runtime clones and may contain secrets from manifests, config, environment, runtime state, or payloads. Nothing is redacted.",
        )

        backup_json, bundle_files = client.export_job_backup(job_id)
        backup_payload = json.loads(backup_json)
        _merge_cli_source_metadata(backup_payload, target)

        with tempfile.TemporaryDirectory(prefix="mn-airgap-export-") as temp_dir:
            extra_files: dict[str, Path] = {}
            if air_gapped:
                extra_files = _prepare_airgap_export(
                    backup_payload,
                    bundle_files,
                    Path(temp_dir),
                )
            archive_path = _write_backup_archive(
                backup_payload,
                bundle_files,
                output,
                target,
                extra_files=extra_files,
                air_gapped=air_gapped,
            )
        print_success_confirmation(
            console,
            "Job backup",
            details=[("Backup", archive_path), ("Source job", job_id)],
            next_steps=f"mn job restore <blueprint-id> --input {archive_path}",
        )
    except BackupRestoreError as exc:
        print_error(console, exc)
        raise typer.Exit(1)
    except Exception as exc:
        handle_cli_error(exc, console, "backup")
        raise typer.Exit(1)


def restore(
    blueprint_id: str = typer.Argument(..., help="Blueprint ID for the cloned run."),
    input: Path = typer.Option(
        ...,
        "--input",
        "-i",
        help="Backup zip file created by mn job backup.",
        exists=True,
        file_okay=True,
        dir_okay=False,
    ),
) -> None:
    """Restore a backup zip as a new paused blueprint job."""
    try:
        if not hasattr(client, "restore_job_backup"):
            raise BackupRestoreError(
                "This mn SDK does not support restore yet. Update mirrorneuron-python-sdk and try again."
            )

        backup_payload, bundle_files, run_store_files, knowledge_files = _read_backup_archive(input)
        run_id = make_blueprint_run_id(blueprint_id)
        restore_json = json.dumps(backup_payload, separators=(",", ":"), sort_keys=True)
        result_json = client.restore_job_backup(
            restore_json,
            bundle_files,
            blueprint_id=blueprint_id,
            run_id=run_id,
        )
        result = json.loads(result_json)
        new_job_id = result.get("job_id")
        new_run_id = result.get("run_id") or run_id
        _restore_local_run_store(
            new_run_id,
            new_job_id,
            blueprint_id,
            result,
            run_store_files,
            knowledge_files,
        )

        print_success_confirmation(
            console,
            "Job restore",
            status="paused",
            details=[
                ("Job ID", new_job_id),
                ("Run ID", new_run_id),
                ("Original job", result.get("source_job_id", "unknown")),
            ],
            next_steps=f"mn run resume {new_job_id}",
        )
    except BackupRestoreError as exc:
        console.print(f"[red]{exc}[/red]")
        raise typer.Exit(1)
    except Exception as exc:
        handle_cli_error(exc, console, "restore")
        raise typer.Exit(1)


def _resolve_backup_target(identifier: str) -> dict[str, Any]:
    exact_job = _load_runtime_job(identifier)
    if exact_job:
        job = exact_job["job"]
        job_id = job.get("job_id") or identifier
        run_match = _find_run_record_by_job_id(job_id)
        return {
            "job_id": job_id,
            "job": job,
            "run_id": _run_id_from_job(job) or (run_match or {}).get("run_id"),
            "run_dir": (run_match or {}).get("run_dir"),
            "blueprint_id": _blueprint_id_from_job(job) or (run_match or {}).get("blueprint_id"),
        }

    exact_run = _load_run_record(identifier)
    if exact_run and exact_run.get("job_id"):
        job = _runtime_job_or_record(exact_run["job_id"], exact_run)
        return {
            "job_id": exact_run["job_id"],
            "job": job,
            "run_id": exact_run.get("run_id") or identifier,
            "run_dir": exact_run.get("run_dir"),
            "blueprint_id": exact_run.get("blueprint_id") or _blueprint_id_from_job(job),
        }

    candidates = []
    for record in _scan_run_records():
        if record.get("blueprint_id") != identifier or not record.get("job_id"):
            continue
        job = _runtime_job_or_record(record["job_id"], record)
        if job.get("status") == PAUSED_STATUS:
            candidates.append(
                {
                    "job_id": record["job_id"],
                    "job": job,
                    "run_id": record.get("run_id"),
                    "run_dir": record.get("run_dir"),
                    "blueprint_id": identifier,
                }
            )

    if len(candidates) == 1:
        return candidates[0]

    if len(candidates) > 1:
        candidate_text = ", ".join(
            f"{candidate['job_id']} (run {candidate.get('run_id') or 'unknown'})"
            for candidate in candidates
        )
        raise BackupRestoreError(
            f"Blueprint {identifier} has multiple paused runs. Use an exact job_id or run_id. Candidates: {candidate_text}"
        )

    raise BackupRestoreError(
        f"Could not resolve {identifier} as a job_id, run_id, or unique paused blueprint run."
    )


def _load_runtime_job(job_id: str) -> dict[str, Any] | None:
    try:
        payload = json.loads(client.get_job(job_id))
    except Exception:
        logger.exception("Failed to load job %s while resolving backup target", job_id)
        return None

    job = _job_from_get_job_payload(payload)
    if not job:
        return None
    job.setdefault("job_id", job_id)
    return {"job": job, "payload": payload}


def _runtime_job_or_record(job_id: str, record: dict[str, Any]) -> dict[str, Any]:
    runtime = _load_runtime_job(job_id)
    if runtime:
        return runtime["job"]
    return {
        "job_id": job_id,
        "status": record.get("status") or record.get("job_status"),
        "manifest": record.get("manifest") or {},
    }


def _job_from_get_job_payload(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict) or payload == {}:
        return {}
    job = payload.get("job")
    if isinstance(job, dict) and job:
        return job
    if payload.get("job_id") or payload.get("status"):
        return payload
    return {}


def _runs_root() -> Path:
    return default_runs_root()


def _load_run_record(run_id: str) -> dict[str, Any] | None:
    job_file = _runs_root() / run_id / "job.json"
    if not job_file.is_file():
        return None
    return _record_from_job_file(job_file)


def _find_run_record_by_job_id(job_id: str) -> dict[str, Any] | None:
    for record in _scan_run_records():
        if record.get("job_id") == job_id:
            return record
    return None


def _scan_run_records() -> list[dict[str, Any]]:
    root = _runs_root()
    if not root.is_dir():
        return []
    return [
        record
        for job_file in root.glob("*/job.json")
        if (record := _record_from_job_file(job_file)) is not None
    ]


def _record_from_job_file(job_file: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(job_file.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None

    job = payload.get("job") if isinstance(payload.get("job"), dict) else {}
    run = payload.get("run") if isinstance(payload.get("run"), dict) else {}
    manifest = payload.get("manifest") if isinstance(payload.get("manifest"), dict) else {}
    mn_cli = (((job.get("manifest") or manifest).get("metadata") or {}).get("mn_cli") or {})
    record = {
        "run_id": payload.get("run_id") or run.get("run_id") or job_file.parent.name,
        "job_id": payload.get("job_id") or job.get("job_id"),
        "blueprint_id": (
            payload.get("blueprint_id")
            or run.get("blueprint_id")
            or mn_cli.get("blueprint_id")
        ),
        "status": payload.get("status") or payload.get("job_status") or job.get("status"),
        "manifest": job.get("manifest") or manifest,
        "run_dir": job_file.parent,
        "raw": payload,
    }
    return record


def _run_id_from_job(job: dict[str, Any]) -> str | None:
    value = (((job.get("manifest") or {}).get("metadata") or {}).get("mn_cli") or {}).get(
        "blueprint_run_id"
    )
    return value if isinstance(value, str) and value else None


def _blueprint_id_from_job(job: dict[str, Any]) -> str | None:
    value = (((job.get("manifest") or {}).get("metadata") or {}).get("mn_cli") or {}).get(
        "blueprint_id"
    )
    return value if isinstance(value, str) and value else None


def _merge_cli_source_metadata(backup_payload: dict[str, Any], target: dict[str, Any]) -> None:
    source = backup_payload.setdefault("source", {})
    source.setdefault("job_id", target.get("job_id"))
    if target.get("run_id"):
        source["run_id"] = target["run_id"]
    if target.get("blueprint_id"):
        source["blueprint_id"] = target["blueprint_id"]


def _write_backup_archive(
    backup_payload: dict[str, Any],
    bundle_files: dict[str, bytes],
    output_folder: Path,
    target: dict[str, Any],
    *,
    extra_files: dict[str, Path] | None = None,
    air_gapped: bool = False,
) -> Path:
    if backup_payload.get("schema_version") != SCHEMA_VERSION:
        raise BackupRestoreError(
            f"Runtime returned unsupported backup schema {backup_payload.get('schema_version')!r}."
        )
    if "manifest.json" not in bundle_files:
        raise BackupRestoreError("Runtime backup did not include bundle/manifest.json.")

    output_folder.mkdir(parents=True, exist_ok=True)
    archive_path = output_folder / _backup_filename(
        backup_payload, target, air_gapped=air_gapped
    )
    archive_metadata = _archive_metadata(backup_payload)
    entries: dict[str, bytes] = {
        "mn-backup.json": _json_bytes(archive_metadata),
        "runtime/job.json": _json_bytes(backup_payload["runtime"]["job"]),
        "runtime/agents.json": _json_bytes(backup_payload["runtime"].get("agents", [])),
        "runtime/events.jsonl": _events_jsonl_bytes(
            backup_payload["runtime"].get("events", [])
        ),
    }

    for relative_path, contents in bundle_files.items():
        safe_path = _safe_archive_relative_path(relative_path)
        entries[f"bundle/{safe_path}"] = _bytes(contents)

    run_dir = target.get("run_dir")
    if isinstance(run_dir, Path) and run_dir.is_dir():
        _add_directory_entries(entries, run_dir, "run_store")
        knowledge_dir = run_dir / "knowledge"
        if knowledge_dir.is_dir():
            _add_directory_entries(entries, knowledge_dir, "knowledge")

    file_entries = dict(extra_files or {})
    checksums = {
        "algorithm": "sha256",
        "entries": {
            name: hashlib.sha256(contents).hexdigest()
            for name, contents in sorted(entries.items())
        },
    }
    for name, path in sorted(file_entries.items()):
        checksums["entries"][name] = _file_sha256(path)
    entries[CHECKSUMS_ENTRY] = _json_bytes(checksums)

    partial = archive_path.with_suffix(f"{archive_path.suffix}.partial")
    try:
        with zipfile.ZipFile(partial, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            if not any(name.startswith("bundle/payloads/") for name in entries):
                zf.writestr("bundle/payloads/", b"")
            for name, contents in sorted(entries.items()):
                zf.writestr(name, contents)
            for name, path in sorted(file_entries.items()):
                zf.write(path, arcname=name)
        partial.replace(archive_path)
    finally:
        partial.unlink(missing_ok=True)

    return archive_path


def _read_backup_archive(
    archive_path: Path,
) -> tuple[dict[str, Any], dict[str, bytes], dict[str, bytes], dict[str, bytes]]:
    with zipfile.ZipFile(archive_path, "r") as zf:
        names = zf.namelist()
        _validate_archive_names(names)
        missing = REQUIRED_ENTRIES - set(names)
        if missing:
            raise BackupRestoreError(
                f"Backup zip is missing required entries: {', '.join(sorted(missing))}"
            )
        if not any(name.startswith("bundle/payloads/") for name in names):
            raise BackupRestoreError("Backup zip is missing bundle/payloads/.")
        _verify_checksums(zf)

        metadata = json.loads(zf.read("mn-backup.json"))
        if metadata.get("schema_version") != SCHEMA_VERSION:
            raise BackupRestoreError(
                f"Unsupported backup schema {metadata.get('schema_version')!r}."
            )

        wheelhouse = _hydrate_airgap_archive(zf, metadata, archive_path)
        runtime = {
            "job": json.loads(zf.read("runtime/job.json")),
            "agents": json.loads(zf.read("runtime/agents.json")),
            "events": _parse_events_jsonl(zf.read("runtime/events.jsonl")),
        }
        backup_payload = copy.deepcopy(metadata)
        backup_payload["runtime"] = runtime

        bundle_files = _read_prefixed_files(zf, "bundle/")
        run_store_files = _read_prefixed_files(zf, "run_store/")
        knowledge_files = _read_prefixed_files(zf, "knowledge/")
        if "manifest.json" not in bundle_files:
            raise BackupRestoreError("Backup zip is missing bundle/manifest.json.")
        if metadata.get("air_gap", {}).get("enabled") is True:
            _prepare_restored_airgap_bundle(
                bundle_files,
                wheelhouse=wheelhouse,
            )
        return backup_payload, bundle_files, run_store_files, knowledge_files


def _archive_metadata(backup_payload: dict[str, Any]) -> dict[str, Any]:
    metadata = {
        key: value
        for key, value in backup_payload.items()
        if key not in {"runtime"}
    }
    metadata.setdefault("version", 1)
    metadata.setdefault("schema_version", SCHEMA_VERSION)
    metadata.setdefault("created_at", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    return metadata


def _backup_filename(
    backup_payload: dict[str, Any],
    target: dict[str, Any],
    *,
    air_gapped: bool = False,
) -> str:
    source = backup_payload.get("source") or {}
    label = source.get("blueprint_id") or target.get("run_id") or target.get("job_id") or "mn"
    timestamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    suffix = "mn-airgap-backup.zip" if air_gapped else "mnbackup.zip"
    return f"{_slug(label)}-{_slug(target['job_id'])}-{timestamp}.{suffix}"


def _slug(value: Any) -> str:
    slug = re.sub(r"[^A-Za-z0-9_.-]+", "-", str(value)).strip("-._")
    return slug or "mn"


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _events_jsonl_bytes(events: list[Any]) -> bytes:
    return b"".join(
        (json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
        for event in events
    )


def _parse_events_jsonl(contents: bytes) -> list[Any]:
    events = []
    for line in contents.decode("utf-8").splitlines():
        if line.strip():
            events.append(json.loads(line))
    return events


def _bytes(value: Any) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, str):
        return value.encode("utf-8")
    raise BackupRestoreError(f"Expected bytes for bundle file, got {type(value).__name__}.")


def _add_directory_entries(entries: dict[str, bytes], source_dir: Path, prefix: str) -> None:
    for path in sorted(source_dir.rglob("*")):
        if not path.is_file():
            continue
        relative_path = path.relative_to(source_dir).as_posix()
        entries[f"{prefix}/{_safe_archive_relative_path(relative_path)}"] = path.read_bytes()


def _validate_archive_names(names: list[str]) -> None:
    for name in names:
        _safe_archive_relative_path(name, allow_directory=True)


def _safe_archive_relative_path(path: str, *, allow_directory: bool = False) -> str:
    if not isinstance(path, str) or path == "":
        raise BackupRestoreError("Archive contains an empty path.")
    if "\\" in path:
        raise BackupRestoreError(f"Archive path must use forward slashes: {path!r}")
    if path.startswith("/"):
        raise BackupRestoreError(f"Archive path must be relative: {path!r}")
    posix_path = PurePosixPath(path)
    if posix_path.is_absolute() or ".." in posix_path.parts:
        raise BackupRestoreError(f"Archive path escapes the backup root: {path!r}")
    if not allow_directory and path.endswith("/"):
        raise BackupRestoreError(f"Archive file path must not be a directory: {path!r}")
    return posix_path.as_posix()


def _verify_checksums(zf: zipfile.ZipFile) -> None:
    try:
        payload = json.loads(zf.read(CHECKSUMS_ENTRY))
    except (KeyError, json.JSONDecodeError) as exc:
        raise BackupRestoreError("Backup zip has a malformed checksums.json.") from exc

    entries = payload.get("entries") if isinstance(payload, dict) else None
    if not isinstance(entries, dict):
        raise BackupRestoreError("Backup zip checksums.json must contain an entries map.")

    member_names = {name for name in zf.namelist() if not name.endswith("/")}
    expected_names = member_names - {CHECKSUMS_ENTRY}
    missing_checksums = expected_names - set(entries)
    if missing_checksums:
        raise BackupRestoreError(
            f"Backup zip is missing checksums for: {', '.join(sorted(missing_checksums))}"
        )
    unknown_checksums = set(entries) - expected_names
    if unknown_checksums:
        raise BackupRestoreError(
            f"Backup zip checksums reference missing files: {', '.join(sorted(unknown_checksums))}"
        )

    for name, expected in sorted(entries.items()):
        actual = _zip_member_sha256(zf, name)
        if actual != expected:
            raise BackupRestoreError(f"Checksum mismatch for {name}.")


def _read_prefixed_files(zf: zipfile.ZipFile, prefix: str) -> dict[str, bytes]:
    files: dict[str, bytes] = {}
    for name in zf.namelist():
        if name.endswith("/") or not name.startswith(prefix):
            continue
        relative_path = name[len(prefix) :]
        if relative_path == "":
            continue
        files[_safe_archive_relative_path(relative_path)] = zf.read(name)
    return files


def _prepare_airgap_export(
    backup_payload: dict[str, Any],
    bundle_files: dict[str, bytes],
    temp_root: Path,
) -> dict[str, Path]:
    try:
        manifest = json.loads(bundle_files["manifest.json"])
    except (KeyError, json.JSONDecodeError) as exc:
        raise BackupRestoreError(
            "Runtime backup manifest is required for air-gapped export."
        ) from exc
    if not isinstance(manifest, dict):
        raise BackupRestoreError("Runtime backup manifest must be a JSON object.")
    _audit_airgap_network_dependencies(manifest)
    _audit_airgap_model_dependencies(manifest)

    assets: list[dict[str, Any]] = []
    files: dict[str, Path] = {}
    refs = _manifest_blob_refs(manifest)
    model_paths = _payload_model_paths(manifest)
    for ref in refs:
        sha256 = str(ref.get("sha256") or "").strip().lower()
        payload_path = str(ref.get("payload_path") or "").strip()
        try:
            source = blob_store_path(sha256)
        except ValueError as exc:
            raise BackupRestoreError(
                f"Invalid blob reference for {payload_path or '<unknown>'}."
            ) from exc
        if not source.is_file():
            raise BackupRestoreError(
                f"Required air-gap blob {sha256} for {payload_path} is not available locally."
            )
        actual = _file_sha256(source)
        if actual != sha256:
            raise BackupRestoreError(
                f"Required air-gap blob {sha256} failed checksum validation."
            )
        archive_path = f"airgap/blobs/{sha256}"
        files[archive_path] = source
        assets.append(
            {
                "kind": "blob",
                "purpose": "model" if _is_model_payload(payload_path, model_paths) else "payload",
                "archive_path": archive_path,
                "payload_path": payload_path,
                "sha256": sha256,
                "size_bytes": source.stat().st_size,
            }
        )

    bundle_root = temp_root / "bundle"
    _write_bundle_map(bundle_root, bundle_files)
    wheelhouse = temp_root / "wheelhouse"
    wheelhouse.mkdir(parents=True, exist_ok=True)
    _build_airgap_wheelhouse(manifest, bundle_root, wheelhouse)
    for wheel in sorted(wheelhouse.glob("*.whl")):
        sha256 = _file_sha256(wheel)
        archive_path = f"airgap/python/wheelhouse/{wheel.name}"
        files[archive_path] = wheel
        assets.append(
            {
                "kind": "python_wheel",
                "archive_path": archive_path,
                "sha256": sha256,
                "size_bytes": wheel.stat().st_size,
            }
        )

    image_root = temp_root / "images"
    image_root.mkdir(parents=True, exist_ok=True)
    for image in _required_docker_images(manifest):
        inspect = subprocess.run(
            ["docker", "image", "inspect", image],
            capture_output=True,
            text=True,
            check=False,
        )
        if inspect.returncode != 0:
            raise BackupRestoreError(
                f"Required Docker image {image} is not available locally."
            )
        image_path = image_root / f"{_slug(image)}.tar"
        saved = subprocess.run(
            ["docker", "image", "save", "--output", str(image_path), image],
            capture_output=True,
            text=True,
            check=False,
        )
        if saved.returncode != 0:
            detail = (saved.stderr or saved.stdout or "docker image save failed").strip()
            raise BackupRestoreError(
                f"Could not export Docker image {image}: {detail}"
            )
        archive_path = f"airgap/images/{image_path.name}"
        files[archive_path] = image_path
        assets.append(
            {
                "kind": "docker_image",
                "image": image,
                "archive_path": archive_path,
                "sha256": _file_sha256(image_path),
                "size_bytes": image_path.stat().st_size,
            }
        )

    backup_payload["air_gap"] = {
        "enabled": True,
        "network": "forbidden",
        "compatibility": compatibility_profile(),
        "assets": assets,
    }
    bundle_files[".mn-airgap.json"] = _json_bytes(
        {
            "schema_version": AIRGAP_MARKER_SCHEMA_VERSION,
            "capsule_manifest": "../mn-backup.json",
        }
    )
    return files


def _build_airgap_wheelhouse(
    manifest: dict[str, Any],
    bundle_root: Path,
    wheelhouse: Path,
) -> None:
    dependencies = []
    for field in ("skill_dependencies", "agent_dependencies"):
        value = manifest.get(field)
        if isinstance(value, list):
            dependencies.extend(item for item in value if isinstance(item, dict))
    for dependency in dependencies:
        name = str(dependency.get("name") or "").strip()
        version = str(dependency.get("version") or "").strip().removeprefix("v")
        source = str(dependency.get("source") or "").strip()
        if not name or not version:
            raise BackupRestoreError("Air-gap Python dependencies require name and version.")
        if source == "payload":
            relative = _safe_archive_relative_path(
                str(dependency.get("path") or "")
            )
            package = bundle_root / "payloads" / Path(*PurePosixPath(relative).parts)
            if str(dependency.get("format") or "source") == "wheel":
                if not package.is_file():
                    raise BackupRestoreError(
                        f"Payload wheel is missing: payloads/{relative}"
                    )
                result = subprocess.run(
                    [
                        sys.executable,
                        "-m",
                        "pip",
                        "download",
                        "--dest",
                        str(wheelhouse),
                        "--index-url",
                        GAR_PIP_INDEX_URL,
                        "--extra-index-url",
                        PYPI_PIP_INDEX_URL,
                        str(package),
                    ],
                    capture_output=True,
                    text=True,
                    check=False,
                )
            else:
                if not (package / "pyproject.toml").is_file():
                    raise BackupRestoreError(
                        f"Payload source package is missing pyproject.toml: payloads/{relative}"
                    )
                result = subprocess.run(
                    [
                        sys.executable,
                        "-m",
                        "pip",
                        "wheel",
                        "--no-build-isolation",
                        "--wheel-dir",
                        str(wheelhouse),
                        "--index-url",
                        GAR_PIP_INDEX_URL,
                        "--extra-index-url",
                        PYPI_PIP_INDEX_URL,
                        str(package),
                    ],
                    capture_output=True,
                    text=True,
                    check=False,
                )
        elif source == "gar":
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "pip",
                    "download",
                    "--only-binary=:all:",
                    "--dest",
                    str(wheelhouse),
                    "--index-url",
                    GAR_PIP_INDEX_URL,
                    "--extra-index-url",
                    PYPI_PIP_INDEX_URL,
                    f"{name}=={version}",
                ],
                capture_output=True,
                text=True,
                check=False,
            )
        else:
            raise BackupRestoreError(
                f"Unsupported Python dependency source {source!r} for {name}."
            )
        if result.returncode != 0:
            detail = (result.stderr or result.stdout or "wheel materialization failed").strip()
            raise BackupRestoreError(
                f"Could not materialize {name}=={version} for air-gapped use: {detail}"
            )
    _build_hostlocal_requirement_wheels(
        manifest,
        bundle_root=bundle_root,
        wheelhouse=wheelhouse,
        declared_names={
            str(item.get("name") or "")
            for item in dependencies
            if item.get("name")
        },
    )


def _build_hostlocal_requirement_wheels(
    manifest: dict[str, Any],
    *,
    bundle_root: Path,
    wheelhouse: Path,
    declared_names: set[str],
) -> None:
    from mn_sdk.skill_dependencies import normalize_package_name
    from mn_sdk.submission_preparation import manifest_nodes

    normalized_declared = {
        normalize_package_name(name) for name in declared_names if name
    }
    requirements_files: set[Path] = set()
    packages: list[str] = []
    for node in manifest_nodes(manifest):
        config = node.get("config") if isinstance(node.get("config"), dict) else {}
        if config.get("runner_module") != "MirrorNeuron.Runner.HostLocal":
            continue
        python_environment = (
            config.get("python_environment")
            if isinstance(config.get("python_environment"), dict)
            else {}
        )
        requirements = str(python_environment.get("requirements") or "").strip()
        if requirements:
            relative = _safe_archive_relative_path(requirements)
            path = bundle_root / "payloads" / Path(*PurePosixPath(relative).parts)
            if not path.is_file():
                raise BackupRestoreError(
                    f"HostLocal requirements file is missing: payloads/{relative}"
                )
            requirements_files.add(path)
        for item in python_environment.get("packages") or []:
            package = str(item or "").strip()
            if not package or package.startswith("-"):
                continue
            candidate = Path(package).expanduser()
            if candidate.is_absolute() or package.startswith(("file:", "git+")):
                continue
            name = re.split(r"[\s<>=!~\[;@]", package, maxsplit=1)[0]
            if normalize_package_name(name) in normalized_declared:
                continue
            if package not in packages:
                packages.append(package)

    for requirements in sorted(requirements_files):
        _run_airgap_pip_wheel(
            [
                "-r",
                str(requirements),
            ],
            wheelhouse=wheelhouse,
            label=f"payloads/{requirements.relative_to(bundle_root / 'payloads')}",
        )
    if packages:
        _run_airgap_pip_wheel(
            packages,
            wheelhouse=wheelhouse,
            label="HostLocal python_environment.packages",
        )


def _run_airgap_pip_wheel(
    requirements: list[str],
    *,
    wheelhouse: Path,
    label: str,
) -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "wheel",
            "--wheel-dir",
            str(wheelhouse),
            "--index-url",
            GAR_PIP_INDEX_URL,
            "--extra-index-url",
            PYPI_PIP_INDEX_URL,
            *requirements,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        detail = (
            result.stderr or result.stdout or "wheel materialization failed"
        ).strip()
        raise BackupRestoreError(
            f"Could not materialize {label} for air-gapped use: {detail}"
        )


def _hydrate_airgap_archive(
    zf: zipfile.ZipFile,
    metadata: dict[str, Any],
    archive_path: Path,
) -> Path | None:
    air_gap = metadata.get("air_gap")
    if not isinstance(air_gap, dict) or air_gap.get("enabled") is not True:
        return None
    try:
        validate_compatibility(air_gap.get("compatibility") or {})
    except AirGapError as exc:
        raise BackupRestoreError(str(exc)) from exc
    capsule_id = hashlib.sha256(
        f"{archive_path.resolve()}:{archive_path.stat().st_size}".encode("utf-8")
    ).hexdigest()
    airgap_root = default_runs_root().parent / "airgap" / capsule_id
    wheelhouse = airgap_root / "python" / "wheelhouse"
    wheelhouse.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="mn-airgap-hydrate-") as temp_dir:
        temp_root = Path(temp_dir)
        for asset in air_gap.get("assets") or []:
            if not isinstance(asset, dict):
                raise BackupRestoreError("Air-gap asset inventory entries must be objects.")
            member = _safe_archive_relative_path(str(asset.get("archive_path") or ""))
            if member not in zf.namelist():
                raise BackupRestoreError(f"Air-gap asset is missing from archive: {member}")
            expected = str(asset.get("sha256") or "").strip().lower()
            if _zip_member_sha256(zf, member) != expected:
                raise BackupRestoreError(f"Checksum mismatch for {member}.")
            kind = str(asset.get("kind") or "")
            if kind == "python_wheel":
                target = wheelhouse / Path(member).name
                _extract_member(zf, member, target)
            elif kind == "blob":
                temporary = temp_root / expected
                _extract_member(zf, member, temporary)
                try:
                    install_blob_file(temporary, expected)
                except ValueError as exc:
                    raise BackupRestoreError(str(exc)) from exc
            elif kind == "docker_image":
                temporary = temp_root / Path(member).name
                _extract_member(zf, member, temporary)
                loaded = subprocess.run(
                    ["docker", "image", "load", "--input", str(temporary)],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                if loaded.returncode != 0:
                    detail = (
                        loaded.stderr or loaded.stdout or "docker image load failed"
                    ).strip()
                    raise BackupRestoreError(
                        f"Could not load air-gap Docker image: {detail}"
                    )
            else:
                raise BackupRestoreError(f"Unsupported air-gap asset kind {kind!r}.")
    return wheelhouse


def _prepare_restored_airgap_bundle(
    bundle_files: dict[str, bytes],
    *,
    wheelhouse: Path | None,
) -> None:
    with tempfile.TemporaryDirectory(prefix="mn-airgap-bundle-") as temp_dir:
        bundle_root = Path(temp_dir) / "bundle"
        _write_bundle_map(bundle_root, bundle_files)
        manifest = json.loads(bundle_files["manifest.json"])
        if not isinstance(manifest, dict):
            raise BackupRestoreError("Air-gap bundle manifest must be a JSON object.")
        metadata = manifest.setdefault("metadata", {})
        metadata["mn_airgap"] = {
            "wheelhouse": str(wheelhouse) if wheelhouse is not None else "",
            "network": "forbidden",
        }
        if wheelhouse is not None:
            _patch_hostlocal_wheelhouse(manifest, wheelhouse)
        try:
            hydrate_payload_models(bundle_root, manifest)
        except AirGapError as exc:
            raise BackupRestoreError(str(exc)) from exc
        if wheelhouse is not None:
            from mn_cli.libs.run_cmds.handlers.doctor import (
                _doctor_prepare_hostlocal_python_envs,
            )

            report = _doctor_prepare_hostlocal_python_envs(
                bundle_root,
                manifest,
                timeout=float(
                    os.getenv("MN_BLUEPRINT_PYTHON_ENV_TIMEOUT_SECONDS", "30")
                ),
                check_only=False,
            )
            if report.get("status") == "critical":
                raise BackupRestoreError(
                    "Could not prepare offline HostLocal Python environment: "
                    + str(report.get("detail") or report.get("failures") or "unknown failure")
                )
        bundle_files["manifest.json"] = _json_bytes(manifest)


def _patch_hostlocal_wheelhouse(manifest: dict[str, Any], wheelhouse: Path) -> None:
    from mn_sdk.skill_dependencies import (
        normalize_package_name,
        requirement_package_name,
    )

    dependencies = []
    for field in ("skill_dependencies", "agent_dependencies"):
        value = manifest.get(field)
        if isinstance(value, list):
            dependencies.extend(item for item in value if isinstance(item, dict))
    requirements = [
        f"{item['name']}=={str(item['version']).removeprefix('v')}"
        for item in dependencies
        if item.get("name") and item.get("version")
    ]
    dependency_names = {
        normalize_package_name(str(item.get("name") or ""))
        for item in dependencies
        if item.get("name")
    }
    from mn_sdk.submission_preparation import manifest_nodes

    for node in manifest_nodes(manifest):
        config = node.get("config") if isinstance(node.get("config"), dict) else {}
        if config.get("runner_module") != "MirrorNeuron.Runner.HostLocal":
            continue
        python_environment = (
            config.get("python_environment")
            if isinstance(config.get("python_environment"), dict)
            else {}
        )
        existing = [
            str(item).strip()
            for item in python_environment.get("packages") or []
            if str(item).strip()
        ]
        preserved: list[str] = []
        skip_option_value = False
        for item in existing:
            if skip_option_value:
                skip_option_value = False
                continue
            if item in {"--find-links", "--index-url", "--extra-index-url"}:
                skip_option_value = True
                continue
            if item.startswith("-"):
                continue
            candidate = Path(item).expanduser()
            if candidate.is_absolute() or item.startswith(("file:", "git+")):
                continue
            package_name = requirement_package_name(item)
            if package_name and package_name in dependency_names:
                continue
            if item not in preserved:
                preserved.append(item)
        python_environment["packages"] = [
            "--no-index",
            "--find-links",
            str(wheelhouse),
            *requirements,
            *preserved,
        ]
        config["python_environment"] = python_environment


def _required_docker_images(manifest: dict[str, Any]) -> list[str]:
    from mn_sdk.submission_preparation import manifest_nodes

    images: list[str] = []
    for node in manifest_nodes(manifest):
        config = node.get("config") if isinstance(node.get("config"), dict) else {}
        if config.get("runner_module") != "MirrorNeuron.Runner.DockerWorker":
            continue
        image = str(config.get("image") or "").strip()
        if image and image not in images:
            images.append(image)
    return images


def _audit_airgap_network_dependencies(manifest: dict[str, Any]) -> None:
    allowed = {
        "docker",
        "docker-model-runner",
        "docker_model_runner",
        "redis",
        "mirrorneuron",
    }
    services = manifest.get("required_services")
    if not isinstance(services, list):
        return
    blockers = []
    for service in services:
        if not isinstance(service, dict):
            continue
        name = str(service.get("name") or service.get("service") or "").strip()
        url = str(service.get("url") or service.get("endpoint") or "").strip()
        if name.lower() not in allowed and (
            url.startswith("http://") or url.startswith("https://")
        ) and not any(
            token in url for token in ("127.0.0.1", "localhost", "host.docker.internal")
        ):
            blockers.append(name or url)
    if blockers:
        raise BackupRestoreError(
            "Air-gapped export cannot materialize declared network services: "
            + ", ".join(sorted(blockers))
        )


def _audit_airgap_model_dependencies(manifest: dict[str, Any]) -> None:
    runtime = manifest.get("runtime") if isinstance(manifest.get("runtime"), dict) else {}
    models = runtime.get("models") if isinstance(runtime.get("models"), dict) else {}
    missing_payloads: list[str] = []
    for model_id, declaration in models.items():
        if not isinstance(declaration, dict):
            continue
        source = (
            declaration.get("source")
            if isinstance(declaration.get("source"), dict)
            else {}
        )
        if source.get("type") == "payload":
            continue
        model_name = str(
            declaration.get("runtime_model")
            or declaration.get("model")
            or model_id
        ).strip()
        missing_payloads.append(f"{model_id} ({model_name})")
    if missing_payloads:
        raise BackupRestoreError(
            "Air-gapped export requires every runtime model to have a physical "
            "payload source under payloads/models. Missing payload sources: "
            + ", ".join(sorted(missing_payloads))
        )


def _manifest_blob_refs(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    metadata = manifest.get("metadata") if isinstance(manifest.get("metadata"), dict) else {}
    artifacts = metadata.get("mn_artifacts") if isinstance(metadata.get("mn_artifacts"), dict) else {}
    refs = artifacts.get("blob_refs") if isinstance(artifacts.get("blob_refs"), list) else []
    return [item for item in refs if isinstance(item, dict)]


def _payload_model_paths(manifest: dict[str, Any]) -> set[str]:
    runtime = manifest.get("runtime") if isinstance(manifest.get("runtime"), dict) else {}
    models = runtime.get("models") if isinstance(runtime.get("models"), dict) else {}
    paths: set[str] = set()
    for model in models.values():
        source = model.get("source") if isinstance(model, dict) and isinstance(model.get("source"), dict) else {}
        if source.get("type") == "payload" and source.get("path"):
            paths.add(str(source["path"]).strip("/"))
    return paths


def _is_model_payload(payload_path: str, model_paths: set[str]) -> bool:
    return any(
        payload_path == model_path or payload_path.startswith(model_path.rstrip("/") + "/")
        for model_path in model_paths
    )


def _write_bundle_map(root: Path, bundle_files: dict[str, bytes]) -> None:
    (root / "payloads").mkdir(parents=True, exist_ok=True)
    for relative, contents in bundle_files.items():
        safe = _safe_archive_relative_path(relative)
        target = root / Path(*PurePosixPath(safe).parts)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(contents)


def _extract_member(zf: zipfile.ZipFile, member: str, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    with zf.open(member) as source, target.open("wb") as destination:
        while chunk := source.read(1024 * 1024):
            destination.write(chunk)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _zip_member_sha256(zf: zipfile.ZipFile, name: str) -> str:
    digest = hashlib.sha256()
    with zf.open(name) as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _restore_local_run_store(
    run_id: str,
    job_id: str | None,
    blueprint_id: str,
    result: dict[str, Any],
    run_store_files: dict[str, bytes],
    knowledge_files: dict[str, bytes],
) -> None:
    run_dir = _runs_root() / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    for relative_path, contents in run_store_files.items():
        if relative_path == "job.json":
            continue
        _write_safe_run_file(run_dir, relative_path, contents)

    for relative_path, contents in knowledge_files.items():
        _write_safe_run_file(run_dir / "knowledge", relative_path, contents)

    provenance = result.get("restore_provenance") or {}
    job_payload = {
        "run_id": run_id,
        "job_id": job_id,
        "blueprint_id": blueprint_id,
        "restored_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "source_job_id": result.get("source_job_id"),
        "source_run_id": result.get("source_run_id"),
    }
    (run_dir / "job.json").write_text(
        json.dumps(job_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (run_dir / "restore_provenance.json").write_text(
        json.dumps(provenance, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _write_safe_run_file(root: Path, relative_path: str, contents: bytes) -> None:
    safe_path = _safe_archive_relative_path(relative_path)
    target = root / Path(*PurePosixPath(safe_path).parts)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(contents)
