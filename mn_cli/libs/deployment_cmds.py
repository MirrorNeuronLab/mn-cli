import json
from pathlib import Path
from typing import Optional

import typer

from mn_cli.error_handler import handle_cli_error
from mn_cli.shared import client, console

deployment_app = typer.Typer(help="Deployment commands")


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
        console.print_json(data=json.loads(result_json))
    except Exception as exc:
        handle_cli_error(exc, console, "deploy")


@deployment_app.command(name="list")
def list_deployments():
    """List deployments."""
    try:
        console.print_json(data=json.loads(client.list_deployments()))
    except Exception as exc:
        handle_cli_error(exc, console, "deployment list")


@deployment_app.command(name="status")
def status(id_or_key: str):
    """Show deployment status."""
    try:
        console.print_json(data=json.loads(client.get_deployment(id_or_key)))
    except Exception as exc:
        handle_cli_error(exc, console, "deployment status")


@deployment_app.command(name="promote")
def promote(id_or_key: str):
    """Promote a canary deployment."""
    try:
        console.print_json(data=json.loads(client.promote_deployment(id_or_key)))
    except Exception as exc:
        handle_cli_error(exc, console, "deployment promote")


@deployment_app.command(name="rollback")
def rollback(
    id_or_key: str,
    version: Optional[str] = typer.Option(None, "--version", help="Version to roll back to."),
    tag: str = typer.Option("", "--tag", help="Version tag to roll back to."),
    reason: str = typer.Option("", "--reason", help="Reason recorded on rollback."),
):
    """Roll back to a previous stable version."""
    try:
        console.print_json(
            data=json.loads(
                client.rollback_deployment(
                    id_or_key,
                    version=version or "",
                    tag=tag,
                    reason=reason,
                )
            )
        )
    except Exception as exc:
        handle_cli_error(exc, console, "deployment rollback")


@deployment_app.command(name="pause")
def pause(id_or_key: str, reason: str = typer.Option("", "--reason")):
    """Pause deployment bookkeeping."""
    try:
        console.print_json(data=json.loads(client.pause_deployment(id_or_key, reason=reason)))
    except Exception as exc:
        handle_cli_error(exc, console, "deployment pause")


@deployment_app.command(name="resume")
def resume(id_or_key: str, reason: str = typer.Option("", "--reason")):
    """Resume deployment bookkeeping."""
    try:
        console.print_json(data=json.loads(client.resume_deployment(id_or_key, reason=reason)))
    except Exception as exc:
        handle_cli_error(exc, console, "deployment resume")


@deployment_app.command(name="fail")
def fail(id_or_key: str, reason: str = typer.Option("", "--reason")):
    """Mark a deployment failed."""
    try:
        console.print_json(data=json.loads(client.fail_deployment(id_or_key, reason=reason)))
    except Exception as exc:
        handle_cli_error(exc, console, "deployment fail")


def read_bundle(path: str) -> tuple[str, dict[str, bytes]]:
    root = Path(path).expanduser()
    manifest_path = root / "manifest.json" if root.is_dir() else root

    manifest_json = manifest_path.read_text(encoding="utf-8")
    payloads: dict[str, bytes] = {}

    payloads_dir = root / "payloads" if root.is_dir() else manifest_path.parent / "payloads"
    if payloads_dir.is_dir():
        for payload_path in payloads_dir.rglob("*"):
            if payload_path.is_file():
                payloads[str(payload_path.relative_to(payloads_dir))] = payload_path.read_bytes()

    return manifest_json, payloads


def update_policy(
    strategy: str,
    canary: int,
    max_parallel: int,
    auto_promote: bool,
    auto_revert: bool,
) -> dict:
    return {
        "strategy": strategy,
        "canary": canary,
        "max_parallel": max_parallel,
        "auto_promote": auto_promote,
        "auto_revert": auto_revert,
    }
