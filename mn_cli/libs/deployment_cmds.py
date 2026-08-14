import json
from typing import Optional

import typer

from mn_cli.error_handler import handle_cli_error
from mn_cli.libs.bundles import read_bundle
from mn_cli.libs.ui import print_collection, print_detail, print_error, print_success_confirmation, require_confirmation
from mn_cli.output import record_result
from mn_cli.shared import client, console
from mn_sdk import deployment_policy

def deploy(
    bundle: str,
    key: str = typer.Option("", "--key", help="Stable deployment key."),
    strategy: str = typer.Option("rolling", "--strategy", help="rolling, canary, or blue-green."),
    canary: int = typer.Option(0, "--canary", help="Number of canary agents."),
    max_parallel: int = typer.Option(1, "--max-parallel", help="Agents to update at once."),
    auto_promote: bool = typer.Option(False, "--auto-promote", help="Promote a healthy canary automatically."),
    auto_revert: bool = typer.Option(False, "--auto-revert", help="Revert automatically when deployment fails."),
    wait: bool = typer.Option(False, "--wait", help="Wait for the launched job to become active."),
):
    """Deploy a bundle under a stable deployment key."""
    try:
        manifest_json, payloads = read_bundle(bundle)
        result_json = client.deploy_job(
            manifest_json,
            payloads,
            deployment_key=key,
            update_policy=update_policy(strategy, canary, max_parallel, auto_promote, auto_revert),
            wait=wait,
        )
        _print_deployment_confirmation(
            "Deployment deploy",
            result_json,
            details=[("Bundle", bundle), ("Key", key), ("Strategy", strategy)],
            next_steps="mn deployment list",
        )
    except Exception as exc:
        handle_cli_error(exc, console, "deploy")


def list_deployments():
    """List deployments."""
    try:
        payload = json.loads(client.list_deployments())
        items = payload.get("deployments") or payload.get("data") or [] if isinstance(payload, dict) else []
        print_collection(
            console,
            "Deployments",
            items,
            columns=(("ID", "deployment_id"), ("Kind", "strategy"), ("State", "status"), ("Node / Owner", "deployment_key"), ("Updated", "updated_at")),
        )
    except Exception as exc:
        handle_cli_error(exc, console, "deployment list")


def status(id_or_key: str):
    """Show deployment status."""
    try:
        print_detail(console, "Deployment", json.loads(client.get_deployment(id_or_key)))
    except Exception as exc:
        handle_cli_error(exc, console, "deployment show")


def promote(id_or_key: str):
    """Promote a canary deployment."""
    try:
        _print_deployment_confirmation(
            "Deployment promote",
            client.promote_deployment(id_or_key),
            details={"Deployment": id_or_key},
            next_steps=f"mn deployment show {id_or_key}",
        )
    except Exception as exc:
        handle_cli_error(exc, console, "deployment promote")


def rollback(
    id_or_key: str,
    version: Optional[str] = typer.Option(None, "--version", help="Version to roll back to."),
    tag: str = typer.Option("", "--tag", help="Version tag to roll back to."),
    reason: str = typer.Option("", "--reason", help="Reason recorded on rollback."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Confirm rollback without prompting."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview rollback without changing state."),
):
    """Roll back to a previous stable version."""
    yes = yes is True
    dry_run = dry_run is True
    if dry_run:
        print_success_confirmation(
            console,
            "Deployment rollback dry run",
            status="planned",
            details={"Deployment": id_or_key, "Version": version or tag},
        )
        return
    require_confirmation(
        console,
        action="Deployment rollback",
        prompt=f"Roll back deployment {id_or_key!r}?",
        yes=yes,
    )
    try:
        _print_deployment_confirmation(
            "Deployment rollback",
            client.rollback_deployment(
                id_or_key,
                version=version or "",
                tag=tag,
                reason=reason,
            ),
            details=[("Deployment", id_or_key), ("Version", version or tag)],
            next_steps=f"mn deployment show {id_or_key}",
        )
    except Exception as exc:
        handle_cli_error(exc, console, "deployment rollback")


def pause(id_or_key: str, reason: str = typer.Option("", "--reason")):
    """Pause deployment bookkeeping."""
    try:
        _print_deployment_confirmation(
            "Deployment pause",
            client.pause_deployment(id_or_key, reason=reason),
            details={"Deployment": id_or_key},
            next_steps=f"mn deployment show {id_or_key}",
        )
    except Exception as exc:
        handle_cli_error(exc, console, "deployment pause")


def resume(id_or_key: str, reason: str = typer.Option("", "--reason")):
    """Resume deployment bookkeeping."""
    try:
        _print_deployment_confirmation(
            "Deployment resume",
            client.resume_deployment(id_or_key, reason=reason),
            details={"Deployment": id_or_key},
            next_steps=f"mn deployment show {id_or_key}",
        )
    except Exception as exc:
        handle_cli_error(exc, console, "deployment resume")


def fail(id_or_key: str, reason: str = typer.Option("", "--reason")):
    """Mark a deployment failed."""
    try:
        _print_deployment_confirmation(
            "Deployment fail",
            client.fail_deployment(id_or_key, reason=reason),
            details={"Deployment": id_or_key},
            next_steps=f"mn deployment show {id_or_key}",
        )
    except Exception as exc:
        handle_cli_error(exc, console, "deployment fail")


def _print_deployment_confirmation(
    action: str,
    result_json: str,
    *,
    details=None,
    next_steps: str | None = None,
) -> None:
    payload = json.loads(result_json)
    detail_items: list[tuple[str, object]] = []
    if details:
        detail_items.extend(details.items() if isinstance(details, dict) else details)
    detail_items.extend(
        [
            ("Deployment ID", payload.get("deployment_id") or payload.get("id")),
            ("Key", payload.get("deployment_key") or payload.get("key")),
            ("Job ID", payload.get("job_id")),
            ("Version", payload.get("version")),
        ]
    )
    print_success_confirmation(
        console,
        action,
        status=payload.get("status"),
        details=detail_items,
        next_steps=next_steps,
    )
    record_result(payload)


def update_policy(
    strategy: str,
    canary: int,
    max_parallel: int,
    auto_promote: bool,
    auto_revert: bool,
) -> dict:
    return deployment_policy(strategy, canary, max_parallel, auto_promote, auto_revert)
