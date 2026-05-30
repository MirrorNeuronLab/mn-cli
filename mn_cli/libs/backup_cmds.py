from __future__ import annotations

import copy
import hashlib
import json
import os
import re
import time
import zipfile
from pathlib import Path, PurePosixPath
from typing import Any

import typer

from mn_cli.error_handler import handle_cli_error
from mn_cli.libs.blueprint_observability import make_blueprint_run_id
from mn_cli.shared import client, console, logger

SCHEMA_VERSION = "mn.backup.v1"
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

        console.print(
            "[yellow]Warning: backups are complete runtime clones and may contain secrets from manifests, config, environment, runtime state, or payloads. Nothing is redacted.[/yellow]"
        )

        backup_json, bundle_files = client.export_job_backup(job_id)
        backup_payload = json.loads(backup_json)
        _merge_cli_source_metadata(backup_payload, target)

        archive_path = _write_backup_archive(backup_payload, bundle_files, output, target)
        console.print(f"[green]Backup written:[/green] {archive_path}")
        console.print(f"Source job: [bold]{job_id}[/bold]")
    except BackupRestoreError as exc:
        console.print(f"[red]{exc}[/red]")
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

        console.print(f"[green]Restored backup as paused job:[/green] {new_job_id}")
        console.print(f"New run: [bold]{new_run_id}[/bold]")
        console.print(
            f"Original job: [bold]{result.get('source_job_id', 'unknown')}[/bold]"
        )
        console.print(f"Resume when ready: [bold]mn job resume {new_job_id}[/bold]")
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
    return Path(os.getenv("MN_RUNS_ROOT") or "~/.mn/runs").expanduser()


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
) -> Path:
    if backup_payload.get("schema_version") != SCHEMA_VERSION:
        raise BackupRestoreError(
            f"Runtime returned unsupported backup schema {backup_payload.get('schema_version')!r}."
        )
    if "manifest.json" not in bundle_files:
        raise BackupRestoreError("Runtime backup did not include bundle/manifest.json.")

    output_folder.mkdir(parents=True, exist_ok=True)
    archive_path = output_folder / _backup_filename(backup_payload, target)
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

    checksums = {
        "algorithm": "sha256",
        "entries": {
            name: hashlib.sha256(contents).hexdigest()
            for name, contents in sorted(entries.items())
        },
    }
    entries[CHECKSUMS_ENTRY] = _json_bytes(checksums)

    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        if not any(name.startswith("bundle/payloads/") for name in entries):
            zf.writestr("bundle/payloads/", b"")
        for name, contents in sorted(entries.items()):
            zf.writestr(name, contents)

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
        return backup_payload, bundle_files, run_store_files, knowledge_files


def _archive_metadata(backup_payload: dict[str, Any]) -> dict[str, Any]:
    metadata = {
        key: value
        for key, value in backup_payload.items()
        if key not in {"runtime"}
    }
    metadata.setdefault("schema_version", SCHEMA_VERSION)
    metadata.setdefault("created_at", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
    return metadata


def _backup_filename(backup_payload: dict[str, Any], target: dict[str, Any]) -> str:
    source = backup_payload.get("source") or {}
    label = source.get("blueprint_id") or target.get("run_id") or target.get("job_id") or "mn"
    timestamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    return f"{_slug(label)}-{_slug(target['job_id'])}-{timestamp}.mnbackup.zip"


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
        actual = hashlib.sha256(zf.read(name)).hexdigest()
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
