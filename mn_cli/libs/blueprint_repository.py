from __future__ import annotations

from pathlib import Path
import subprocess
from typing import Any, Optional

import typer

from mn_sdk.blueprint_source import (
    DEFAULT_BLUEPRINT_REPO,
    BlueprintCatalogError,
    blueprint_storage_dir_for_source as sdk_blueprint_storage_dir_for_source,
    custom_blueprint_storage_dir as sdk_custom_blueprint_storage_dir,
    default_blueprint_storage_dir as sdk_default_blueprint_storage_dir,
    load_blueprint_index as sdk_load_blueprint_index,
    resolve_blueprint_source_config,
)

from mn_cli.shared import console, logger


BLUEPRINT_REPO_CONTEXT_KEY = "blueprint_repo"


class BlueprintIndexError(Exception):
    """Raised when a blueprint index is missing or malformed."""


def context_blueprint_repo(ctx: typer.Context) -> Optional[str]:
    if isinstance(ctx.obj, dict):
        value = ctx.obj.get(BLUEPRINT_REPO_CONTEXT_KEY)
        return str(value) if value else None
    return None


def default_blueprint_storage_dir() -> Path:
    return sdk_default_blueprint_storage_dir()


def custom_blueprint_storage_dir(repo: str) -> Path:
    return sdk_custom_blueprint_storage_dir(repo)


def blueprint_storage_dir_for_source(source: str, *, use_default_cache: bool = False) -> Path:
    return sdk_blueprint_storage_dir_for_source(source, use_default_cache=use_default_cache)


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
    try:
        return sdk_load_blueprint_index(index_path, require_paths=require_paths)
    except BlueprintCatalogError as exc:
        raise BlueprintIndexError(exc.detail) from exc


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
