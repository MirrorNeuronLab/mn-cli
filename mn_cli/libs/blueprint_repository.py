from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
from typing import Any, Optional

import typer

from mn_sdk.blueprint_source import DEFAULT_BLUEPRINT_REPO, resolve_blueprint_source_config
from mn_sdk.runtime_config import resolve_mn_home

from mn_cli.shared import console, logger


DEFAULT_BLUEPRINT_STORAGE_NAME = "blueprints"
CUSTOM_BLUEPRINT_STORAGE_ROOT_NAME = "blueprint_repos"
BLUEPRINT_REPO_CONTEXT_KEY = "blueprint_repo"


class BlueprintIndexError(Exception):
    """Raised when a blueprint index is missing or malformed."""


def context_blueprint_repo(ctx: typer.Context) -> Optional[str]:
    if isinstance(ctx.obj, dict):
        value = ctx.obj.get(BLUEPRINT_REPO_CONTEXT_KEY)
        return str(value) if value else None
    return None


def default_blueprint_storage_dir() -> Path:
    return resolve_mn_home() / DEFAULT_BLUEPRINT_STORAGE_NAME


def custom_blueprint_storage_dir(repo: str) -> Path:
    normalized = repo.strip().rstrip("/")
    name = normalized.removesuffix(".git").split("/")[-1] or "blueprints"
    name = re.sub(r"[^A-Za-z0-9_.-]+", "-", name).strip("-._") or "blueprints"
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:12]
    return resolve_mn_home() / CUSTOM_BLUEPRINT_STORAGE_ROOT_NAME / f"{name}-{digest}"


def blueprint_storage_dir_for_source(source: str, *, use_default_cache: bool = False) -> Path:
    source_path = Path(source).expanduser()
    if source_path.exists():
        return source_path
    if use_default_cache:
        return default_blueprint_storage_dir()
    return custom_blueprint_storage_dir(source)


def blueprint_cache_dir_for_repo(repo: str) -> Path:
    return custom_blueprint_storage_dir(repo)


def resolved_blueprint_source(
    *,
    source: Optional[str],
    blueprint_repo: Optional[str],
) -> tuple[str, bool]:
    if source:
        return source, False
    if blueprint_repo:
        return blueprint_repo, blueprint_repo == DEFAULT_BLUEPRINT_REPO
    config = resolve_blueprint_source_config()
    return config.active_location, config.source == "github" and config.repo == DEFAULT_BLUEPRINT_REPO


def load_blueprint_index(index_path: Path, *, require_paths: bool = False) -> list[dict[str, Any]]:
    if not index_path.exists():
        raise BlueprintIndexError(f"index.json not found in blueprint storage at {index_path.parent}")
    try:
        data = json.loads(index_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise BlueprintIndexError(f"Error parsing index.json: {exc}") from exc

    if not isinstance(data, list):
        raise BlueprintIndexError("index.json is not well formatted: expected a JSON list of blueprint entries")

    blueprints: list[dict[str, Any]] = []
    for position, entry in enumerate(data):
        if not isinstance(entry, dict):
            raise BlueprintIndexError(
                f"index.json is not well formatted: entry {position} must be a JSON object"
            )
        if require_paths and not isinstance(entry.get("path"), str):
            raise BlueprintIndexError(
                f"index.json is not well formatted: entry {position} must include a string path"
            )
        blueprints.append(entry)
    return blueprints


def ensure_blueprint_source(
    *,
    source: Optional[str],
    blueprint_repo: Optional[str],
    update: bool,
    offline: bool,
    revision: Optional[str],
) -> str:
    repo_source, uses_default_repo = resolved_blueprint_source(source=source, blueprint_repo=blueprint_repo)
    storage_dir = blueprint_storage_dir_for_source(
        repo_source,
        use_default_cache=uses_default_repo,
    )

    if not storage_dir.exists():
        if offline:
            console.print(f"[red]Blueprint storage not found at {storage_dir}; offline mode cannot clone {repo_source!r}.[/red]")
            raise typer.Exit(1)
        if uses_default_repo:
            console.print(f"Initializing blueprint storage at {storage_dir}...")
        else:
            console.print(f"Initializing blueprint storage for {repo_source} at {storage_dir}...")
        clone_blueprint_repo(repo_source, storage_dir)
    elif update:
        git_pull(storage_dir)
    elif not source or storage_dir != Path(source).expanduser():
        console.print(f"Using cached blueprint storage at {storage_dir}. Run 'mn blueprint update' or pass --update to refresh.")

    if revision:
        if offline:
            git_checkout(storage_dir, revision)
        else:
            git_fetch(storage_dir)
            git_checkout(storage_dir, revision)
    return str(storage_dir)


def clone_blueprint_repo(source: str, storage_dir: Path) -> None:
    storage_dir.parent.mkdir(parents=True, exist_ok=True)
    res = subprocess.run(["git", "clone", source, str(storage_dir)], capture_output=True, text=True)
    if res.returncode != 0:
        logger.error("Failed to clone blueprint repository: %s", res.stderr)
        console.print(f"[red]Failed to clone blueprint repository: {res.stderr}[/red]")
        raise typer.Exit(1)


def git_pull(storage_dir: Path) -> None:
    console.print(f"Updating blueprint storage at {storage_dir}...")
    res = subprocess.run(["git", "-C", str(storage_dir), "pull", "--ff-only"], capture_output=True, text=True)
    if res.returncode != 0:
        logger.warning("Failed to update blueprint repository: %s", res.stderr)
        console.print(f"[yellow]Warning: Failed to update blueprint repository: {res.stderr}[/yellow]")


def git_fetch(storage_dir: Path) -> None:
    subprocess.run(["git", "-C", str(storage_dir), "fetch", "--all", "--tags"], capture_output=True, text=True)


def git_checkout(storage_dir: Path, revision: str) -> None:
    res = subprocess.run(["git", "-C", str(storage_dir), "checkout", revision], capture_output=True, text=True)
    if res.returncode != 0:
        console.print(f"[red]Failed to checkout blueprint revision {revision}: {res.stderr}[/red]")
        raise typer.Exit(1)


def git_revision(storage_dir: Path) -> Optional[str]:
    res = subprocess.run(["git", "-C", str(storage_dir), "rev-parse", "HEAD"], capture_output=True, text=True)
    if res.returncode != 0:
        return None
    stdout = getattr(res, "stdout", "") or ""
    return str(stdout).strip() or None
