from __future__ import annotations

import json
import os
import re
import shutil
import signal
import subprocess
import time
from pathlib import Path
from typing import Any


RESOURCE_METADATA_FILE = ".mn-blueprint-resource.json"
DEFAULT_TEMP_DIR = "/tmp/mirror_neuron"
DOCKER_LABEL_KEYS = ("mirrorneuron.blueprint_id", "com.mirrorneuron.blueprint_id")
DOCKER_NAME_PREFIXES = ("mn-blueprint-", "mirror-neuron-blueprint-")
RUN_METADATA_FILES = ("run.json", "job.json", "ui.json", "web_ui.json", "web_ui_process.json", "config.json")
GENERATED_BUNDLE_METADATA_FILES = ("manifest.json", "config/default.json", "config.json", "scenario.json")
BUNDLE_CACHE_METADATA_FILES = ("manifest.json", "config/default.json", "config.json", "scenario.json")


def default_python_envs_dir() -> Path:
    configured = os.getenv("MN_BLUEPRINT_PYTHON_ENVS_DIR")
    if configured:
        return Path(configured).expanduser()
    temp_dir = Path(os.getenv("MN_TEMP_DIR", DEFAULT_TEMP_DIR)).expanduser()
    return temp_dir / "blueprint_python_envs"


def default_runs_root() -> Path:
    return Path(os.getenv("MN_RUNS_ROOT") or "~/.mn/runs").expanduser()


def default_generated_bundles_dir() -> Path:
    return Path(os.getenv("MN_GENERATED_BLUEPRINT_BUNDLES_DIR") or "~/.mn/generated_blueprint_bundles").expanduser()


def default_bundle_cache_dir() -> Path:
    configured = os.getenv("MN_BUNDLE_CACHE_DIR")
    if configured:
        return Path(configured).expanduser()
    temp_dir = Path(os.getenv("MN_TEMP_DIR", DEFAULT_TEMP_DIR)).expanduser()
    return temp_dir / "bundle_cache"


def cleanup_blueprint_resources(
    *,
    blueprint_ids: set[str] | None = None,
    active_blueprint_ids: set[str] | None = None,
    python_envs_dir: Path | None = None,
    runs_root: Path | None = None,
    generated_bundles_dir: Path | None = None,
    bundle_cache_dir: Path | None = None,
    include_dead: bool = True,
    include_docker: bool = True,
    include_files: bool = True,
    dry_run: bool = False,
) -> dict[str, Any]:
    normalized_blueprint_ids = _normalize_ids(blueprint_ids)
    normalized_active_ids = _normalize_ids(active_blueprint_ids)
    summary: dict[str, Any] = {
        "python_removed": [],
        "python_skipped": [],
        "run_removed": [],
        "run_skipped": [],
        "generated_removed": [],
        "generated_skipped": [],
        "bundle_removed": [],
        "bundle_skipped": [],
        "docker_removed": [],
        "docker_skipped": [],
        "process_removed": [],
        "process_skipped": [],
        "errors": [],
        "dry_run": dry_run,
    }

    cleanup_python_envs(
        python_envs_dir or default_python_envs_dir(),
        blueprint_ids=normalized_blueprint_ids,
        active_blueprint_ids=normalized_active_ids,
        include_dead=include_dead,
        dry_run=dry_run,
        summary=summary,
    )

    removed_run_ids: set[str] = set()
    if include_files:
        removed_run_ids = cleanup_run_records(
            runs_root or default_runs_root(),
            blueprint_ids=normalized_blueprint_ids,
            active_blueprint_ids=normalized_active_ids,
            include_dead=include_dead,
            dry_run=dry_run,
            summary=summary,
        )
        cleanup_generated_bundles(
            generated_bundles_dir or default_generated_bundles_dir(),
            runs_root=runs_root or default_runs_root(),
            removed_run_ids=removed_run_ids,
            blueprint_ids=normalized_blueprint_ids,
            active_blueprint_ids=normalized_active_ids,
            include_dead=include_dead,
            dry_run=dry_run,
            summary=summary,
        )
        cleanup_bundle_cache(
            bundle_cache_dir or default_bundle_cache_dir(),
            blueprint_ids=normalized_blueprint_ids,
            active_blueprint_ids=normalized_active_ids,
            include_dead=include_dead,
            dry_run=dry_run,
            summary=summary,
        )

    if include_docker:
        cleanup_docker_resources(
            blueprint_ids=normalized_blueprint_ids,
            active_blueprint_ids=normalized_active_ids,
            dry_run=dry_run,
            summary=summary,
        )

    return summary


def cleanup_python_envs(
    root: Path,
    *,
    blueprint_ids: set[str],
    active_blueprint_ids: set[str],
    include_dead: bool,
    dry_run: bool,
    summary: dict[str, Any],
) -> None:
    if not root.exists():
        return
    if not root.is_dir():
        summary["errors"].append(f"python env cache is not a directory: {root}")
        return

    for child in sorted(root.iterdir()):
        try:
            action = classify_python_env_resource(
                child,
                blueprint_ids=blueprint_ids,
                active_blueprint_ids=active_blueprint_ids,
                include_dead=include_dead,
            )
            if action["remove"]:
                remove_path(child, dry_run=dry_run)
                summary["python_removed"].append(
                    {
                        "path": str(child),
                        "reason": action["reason"],
                        "blueprint_id": action.get("blueprint_id"),
                    }
                )
            elif action["reason"]:
                summary["python_skipped"].append(
                    {
                        "path": str(child),
                        "reason": action["reason"],
                        "blueprint_id": action.get("blueprint_id"),
                    }
                )
        except Exception as exc:
            summary["errors"].append(f"failed to inspect python resource {child}: {exc}")


def classify_python_env_resource(
    path: Path,
    *,
    blueprint_ids: set[str],
    active_blueprint_ids: set[str],
    include_dead: bool,
) -> dict[str, Any]:
    if path.name.endswith(".lock"):
        if include_dead and path_age_seconds(path) >= stale_resource_seconds():
            return {"remove": True, "reason": "stale_lock"}
        return {"remove": False, "reason": "active_or_recent_lock"}

    if not path.is_dir():
        return {"remove": False, "reason": ""}

    metadata_path = path / RESOURCE_METADATA_FILE
    metadata = read_resource_metadata(metadata_path)
    ready = (path / ".ready").exists() and (path / "bin" / "python").exists()

    if metadata is None:
        if include_dead and not ready and path_age_seconds(path) >= stale_resource_seconds():
            return {"remove": True, "reason": "incomplete_untracked_python_env"}
        return {"remove": False, "reason": "untracked_python_env"}

    if metadata.get("corrupt") and include_dead and path_age_seconds(path) >= stale_resource_seconds():
        return {"remove": True, "reason": "corrupt_python_env_metadata"}

    blueprint_id = normalize_id(metadata.get("blueprint_id"))

    if blueprint_ids and blueprint_id in blueprint_ids:
        return {"remove": True, "reason": "blueprint_removed", "blueprint_id": blueprint_id}

    if active_blueprint_ids and blueprint_id and blueprint_id not in active_blueprint_ids:
        return {"remove": True, "reason": "dead_blueprint_resource", "blueprint_id": blueprint_id}

    if include_dead and not ready and path_age_seconds(path) >= stale_resource_seconds():
        return {"remove": True, "reason": "incomplete_python_env", "blueprint_id": blueprint_id}

    return {"remove": False, "reason": "active_python_env", "blueprint_id": blueprint_id}


def read_resource_metadata(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"blueprint_id": None, "corrupt": True}
    return data if isinstance(data, dict) else {"blueprint_id": None, "corrupt": True}


def cleanup_run_records(
    root: Path,
    *,
    blueprint_ids: set[str],
    active_blueprint_ids: set[str],
    include_dead: bool,
    dry_run: bool,
    summary: dict[str, Any],
) -> set[str]:
    removed_run_ids: set[str] = set()
    if not root.exists():
        return removed_run_ids
    if not root.is_dir():
        summary["errors"].append(f"blueprint run store is not a directory: {root}")
        return removed_run_ids

    for child in sorted(root.iterdir()):
        try:
            action = classify_run_record(
                child,
                blueprint_ids=blueprint_ids,
                active_blueprint_ids=active_blueprint_ids,
                include_dead=include_dead,
            )
            if action["remove"]:
                cleanup_web_ui_process(child, dry_run=dry_run, summary=summary)
                remove_path(child, dry_run=dry_run)
                removed_run_ids.add(child.name)
                summary["run_removed"].append(
                    {
                        "path": str(child),
                        "reason": action["reason"],
                        "blueprint_id": action.get("blueprint_id"),
                        "run_id": child.name,
                    }
                )
            elif action["reason"]:
                summary["run_skipped"].append(
                    {
                        "path": str(child),
                        "reason": action["reason"],
                        "blueprint_id": action.get("blueprint_id"),
                        "run_id": child.name,
                    }
                )
        except Exception as exc:
            summary["errors"].append(f"failed to inspect blueprint run resource {child}: {exc}")
    return removed_run_ids


def cleanup_web_ui_process(run_dir: Path, *, dry_run: bool, summary: dict[str, Any]) -> None:
    process_info = read_json_object(run_dir / "web_ui_process.json")
    if not process_info:
        return
    try:
        pid = int(process_info.get("pid"))
    except (TypeError, ValueError):
        summary["process_skipped"].append({"path": str(run_dir), "reason": "invalid_web_ui_pid"})
        return

    if not process_is_running(pid):
        summary["process_skipped"].append({"path": str(run_dir), "pid": pid, "reason": "web_ui_process_not_running"})
        return

    if dry_run:
        summary["process_removed"].append({"path": str(run_dir), "pid": pid, "reason": "dry_run"})
        return

    try:
        os.killpg(pid, signal.SIGTERM)
    except ProcessLookupError:
        summary["process_skipped"].append({"path": str(run_dir), "pid": pid, "reason": "web_ui_process_not_running"})
        return
    except OSError:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            summary["process_skipped"].append({"path": str(run_dir), "pid": pid, "reason": "web_ui_process_not_running"})
            return
        except OSError as exc:
            summary["errors"].append(f"failed to stop web UI process {pid} for {run_dir}: {exc}")
            return

    summary["process_removed"].append({"path": str(run_dir), "pid": pid, "reason": "run_record_removed"})


def process_is_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def classify_run_record(
    path: Path,
    *,
    blueprint_ids: set[str],
    active_blueprint_ids: set[str],
    include_dead: bool,
) -> dict[str, Any]:
    if not path.is_dir():
        return {"remove": False, "reason": ""}

    blueprint_id = blueprint_id_from_files(path, RUN_METADATA_FILES)

    if blueprint_ids and (blueprint_id in blueprint_ids or run_name_matches_blueprint(path.name, blueprint_ids)):
        return {"remove": True, "reason": "blueprint_removed_run_record", "blueprint_id": blueprint_id}

    if active_blueprint_ids and blueprint_id and blueprint_id not in active_blueprint_ids:
        return {"remove": True, "reason": "dead_blueprint_run_record", "blueprint_id": blueprint_id}

    if include_dead and not blueprint_id and not any((path / name).exists() for name in RUN_METADATA_FILES):
        if path_age_seconds(path) >= stale_resource_seconds():
            return {"remove": True, "reason": "incomplete_untracked_run_record"}

    return {"remove": False, "reason": "active_run_record" if blueprint_id else "unowned_run_record", "blueprint_id": blueprint_id}


def cleanup_generated_bundles(
    root: Path,
    *,
    runs_root: Path,
    removed_run_ids: set[str],
    blueprint_ids: set[str],
    active_blueprint_ids: set[str],
    include_dead: bool,
    dry_run: bool,
    summary: dict[str, Any],
) -> None:
    if not root.exists():
        return
    if not root.is_dir():
        summary["errors"].append(f"generated blueprint bundle cache is not a directory: {root}")
        return

    for child in sorted(root.iterdir()):
        try:
            action = classify_generated_bundle(
                child,
                runs_root=runs_root,
                removed_run_ids=removed_run_ids,
                blueprint_ids=blueprint_ids,
                active_blueprint_ids=active_blueprint_ids,
                include_dead=include_dead,
            )
            if action["remove"]:
                remove_path(child, dry_run=dry_run)
                summary["generated_removed"].append(
                    {
                        "path": str(child),
                        "reason": action["reason"],
                        "blueprint_id": action.get("blueprint_id"),
                        "run_id": child.name,
                    }
                )
            elif action["reason"]:
                summary["generated_skipped"].append(
                    {
                        "path": str(child),
                        "reason": action["reason"],
                        "blueprint_id": action.get("blueprint_id"),
                        "run_id": child.name,
                    }
                )
        except Exception as exc:
            summary["errors"].append(f"failed to inspect generated blueprint bundle {child}: {exc}")


def classify_generated_bundle(
    path: Path,
    *,
    runs_root: Path,
    removed_run_ids: set[str],
    blueprint_ids: set[str],
    active_blueprint_ids: set[str],
    include_dead: bool,
) -> dict[str, Any]:
    if not path.is_dir():
        return {"remove": False, "reason": ""}

    blueprint_id = blueprint_id_from_files(path, GENERATED_BUNDLE_METADATA_FILES)

    if path.name in removed_run_ids:
        return {"remove": True, "reason": "removed_run_generated_bundle", "blueprint_id": blueprint_id}

    if blueprint_ids and (blueprint_id in blueprint_ids or run_name_matches_blueprint(path.name, blueprint_ids)):
        return {"remove": True, "reason": "blueprint_removed_generated_bundle", "blueprint_id": blueprint_id}

    if active_blueprint_ids and blueprint_id and blueprint_id not in active_blueprint_ids:
        return {"remove": True, "reason": "dead_blueprint_generated_bundle", "blueprint_id": blueprint_id}

    if include_dead and not (runs_root / path.name).exists() and path_age_seconds(path) >= stale_resource_seconds():
        return {"remove": True, "reason": "stale_generated_bundle_without_run", "blueprint_id": blueprint_id}

    return {
        "remove": False,
        "reason": "active_generated_bundle" if blueprint_id else "unowned_generated_bundle",
        "blueprint_id": blueprint_id,
    }


def cleanup_bundle_cache(
    root: Path,
    *,
    blueprint_ids: set[str],
    active_blueprint_ids: set[str],
    include_dead: bool,
    dry_run: bool,
    summary: dict[str, Any],
) -> None:
    if not root.exists():
        return
    if not root.is_dir():
        summary["errors"].append(f"bundle cache is not a directory: {root}")
        return

    for child in sorted(root.iterdir()):
        try:
            action = classify_bundle_cache_entry(
                child,
                blueprint_ids=blueprint_ids,
                active_blueprint_ids=active_blueprint_ids,
                include_dead=include_dead,
            )
            if action["remove"]:
                remove_path(child, dry_run=dry_run)
                summary["bundle_removed"].append(
                    {
                        "path": str(child),
                        "reason": action["reason"],
                        "blueprint_id": action.get("blueprint_id"),
                    }
                )
            elif action["reason"]:
                summary["bundle_skipped"].append(
                    {
                        "path": str(child),
                        "reason": action["reason"],
                        "blueprint_id": action.get("blueprint_id"),
                    }
                )
        except Exception as exc:
            summary["errors"].append(f"failed to inspect bundle cache resource {child}: {exc}")


def classify_bundle_cache_entry(
    path: Path,
    *,
    blueprint_ids: set[str],
    active_blueprint_ids: set[str],
    include_dead: bool,
) -> dict[str, Any]:
    if not path.is_dir():
        return {"remove": False, "reason": ""}

    blueprint_id = blueprint_id_from_files(path, BUNDLE_CACHE_METADATA_FILES)

    if blueprint_ids and blueprint_id in blueprint_ids:
        return {"remove": True, "reason": "blueprint_removed_bundle_cache", "blueprint_id": blueprint_id}

    if active_blueprint_ids and blueprint_id and blueprint_id not in active_blueprint_ids:
        return {"remove": True, "reason": "dead_blueprint_bundle_cache", "blueprint_id": blueprint_id}

    if include_dead and not blueprint_id and not (path / "manifest.json").exists():
        if path_age_seconds(path) >= stale_resource_seconds():
            return {"remove": True, "reason": "incomplete_untracked_bundle_cache"}

    return {"remove": False, "reason": "active_bundle_cache" if blueprint_id else "unowned_bundle_cache", "blueprint_id": blueprint_id}


def blueprint_id_from_files(root: Path, file_names: tuple[str, ...]) -> str | None:
    for file_name in file_names:
        data = read_json_object(root / file_name)
        if not data:
            continue
        blueprint_id = extract_blueprint_id(data)
        if blueprint_id:
            return blueprint_id
    return None


def read_json_object(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def extract_blueprint_id(data: dict[str, Any]) -> str | None:
    direct = normalize_id(data.get("blueprint_id"))
    if direct:
        return direct

    for key in ("metadata", "identity", "run"):
        nested = data.get(key)
        if isinstance(nested, dict):
            nested_id = normalize_id(nested.get("blueprint_id"))
            if nested_id:
                return nested_id

    config = data.get("config")
    if isinstance(config, dict):
        config_id = extract_blueprint_id(config)
        if config_id:
            return config_id

    return None


def run_name_matches_blueprint(run_id: str, blueprint_ids: set[str]) -> bool:
    sanitized_run_id = sanitize_blueprint_id(run_id)
    for blueprint_id in blueprint_ids:
        if run_id == blueprint_id or run_id.startswith(f"{blueprint_id}-"):
            return True
        sanitized = sanitize_blueprint_id(blueprint_id)
        if sanitized_run_id == sanitized or sanitized_run_id.startswith(f"{sanitized}-"):
            return True
    return False


def cleanup_docker_resources(
    *,
    blueprint_ids: set[str],
    active_blueprint_ids: set[str],
    dry_run: bool,
    summary: dict[str, Any],
) -> None:
    if shutil.which("docker") is None:
        summary["docker_skipped"].append({"reason": "docker_not_available"})
        return

    ids_to_clean = set(blueprint_ids)
    if active_blueprint_ids:
        ids_to_clean.update(dead_docker_blueprint_ids(active_blueprint_ids, summary))
    if not ids_to_clean:
        return

    for blueprint_id in sorted(ids_to_clean):
        cleanup_docker_for_blueprint(blueprint_id, dry_run=dry_run, summary=summary)


def dead_docker_blueprint_ids(active_blueprint_ids: set[str], summary: dict[str, Any]) -> set[str]:
    discovered: set[str] = set()
    for label_key in DOCKER_LABEL_KEYS:
        for resource_type in ("container", "image"):
            command = (
                ["docker", "ps", "-a", "--filter", f"label={label_key}", "--format", "{{.Label \"" + label_key + "\"}}"]
                if resource_type == "container"
                else ["docker", "images", "--filter", f"label={label_key}", "--format", "{{.Label \"" + label_key + "\"}}"]
            )
            result = run_docker(command, summary)
            if result is None:
                continue
            for line in result.stdout.splitlines():
                blueprint_id = normalize_id(line)
                if blueprint_id and blueprint_id not in active_blueprint_ids:
                    discovered.add(blueprint_id)
    return discovered


def cleanup_docker_for_blueprint(blueprint_id: str, *, dry_run: bool, summary: dict[str, Any]) -> None:
    container_ids = docker_resource_ids("container", blueprint_id, summary)
    image_ids = docker_resource_ids("image", blueprint_id, summary)

    for container_id in sorted(container_ids):
        if dry_run:
            summary["docker_removed"].append({"type": "container", "id": container_id, "blueprint_id": blueprint_id})
            continue
        result = run_docker(["docker", "rm", "-f", container_id], summary)
        if result is not None and result.returncode == 0:
            summary["docker_removed"].append({"type": "container", "id": container_id, "blueprint_id": blueprint_id})

    for image_id in sorted(image_ids):
        if dry_run:
            summary["docker_removed"].append({"type": "image", "id": image_id, "blueprint_id": blueprint_id})
            continue
        result = run_docker(["docker", "rmi", "-f", image_id], summary)
        if result is not None and result.returncode == 0:
            summary["docker_removed"].append({"type": "image", "id": image_id, "blueprint_id": blueprint_id})


def docker_resource_ids(resource_type: str, blueprint_id: str, summary: dict[str, Any]) -> set[str]:
    ids: set[str] = set()
    for label_key in DOCKER_LABEL_KEYS:
        command = (
            ["docker", "ps", "-a", "--filter", f"label={label_key}={blueprint_id}", "--format", "{{.ID}}"]
            if resource_type == "container"
            else ["docker", "images", "--filter", f"label={label_key}={blueprint_id}", "--format", "{{.ID}}"]
        )
        result = run_docker(command, summary)
        if result is not None:
            ids.update(line.strip() for line in result.stdout.splitlines() if line.strip())

    ids.update(docker_named_resource_ids(resource_type, blueprint_id, summary))
    return ids


def docker_named_resource_ids(resource_type: str, blueprint_id: str, summary: dict[str, Any]) -> set[str]:
    sanitized = sanitize_blueprint_id(blueprint_id)
    allowed_names = {f"{prefix}{sanitized}" for prefix in DOCKER_NAME_PREFIXES}
    if resource_type == "container":
        result = run_docker(["docker", "ps", "-a", "--format", "{{.ID}} {{.Names}}"], summary)
    else:
        result = run_docker(["docker", "images", "--format", "{{.ID}} {{.Repository}}"], summary)
    if result is None:
        return set()
    matches: set[str] = set()
    for line in result.stdout.splitlines():
        parts = line.split(maxsplit=1)
        if len(parts) == 2 and parts[1] in allowed_names:
            matches.add(parts[0])
    return matches


def run_docker(command: list[str], summary: dict[str, Any]) -> subprocess.CompletedProcess[str] | None:
    try:
        result = subprocess.run(command, capture_output=True, text=True)
    except OSError as exc:
        summary["docker_skipped"].append({"reason": f"docker_command_failed: {exc}"})
        return None
    if result.returncode != 0:
        summary["docker_skipped"].append(
            {
                "reason": "docker_command_failed",
                "command": " ".join(command),
                "stderr": result.stderr.strip(),
            }
        )
        return None
    return result


def remove_path(path: Path, *, dry_run: bool) -> None:
    if dry_run:
        return
    if path.is_dir():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()


def path_age_seconds(path: Path) -> float:
    try:
        return max(time.time() - path.stat().st_mtime, 0)
    except OSError:
        return 0


def stale_resource_seconds() -> int:
    try:
        return max(int(os.getenv("MN_BLUEPRINT_RESOURCE_STALE_SECONDS", "3600")), 0)
    except ValueError:
        return 3600


def sanitize_blueprint_id(blueprint_id: str) -> str:
    return re.sub(r"[^a-z0-9_.-]+", "-", blueprint_id.lower()).strip("-._")


def normalize_id(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def _normalize_ids(values: set[str] | None) -> set[str]:
    if not values:
        return set()
    return {normalized for value in values if (normalized := normalize_id(value))}
