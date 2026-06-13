from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated, Any, Optional

import typer

from mn_cli.error_handler import handle_cli_error
from mn_cli.libs.ui import print_confirmed
from mn_cli.libs.run_cmds import (
    _emit_validation_report,
    _normalize_validation_output,
)
from mn_cli.libs.run_manifest import blueprint_runtime_environment, load_blueprint_config
from mn_cli.shared import client, console
from mn_sdk import run_service_validation


service_app = typer.Typer(help="Inspect and check MirrorNeuron service discovery")


@service_app.command(name="list")
def list_services(
    name: Annotated[Optional[str], typer.Option("--name", help="Filter by service name.")] = None,
    node: Annotated[Optional[str], typer.Option("--node", help="Filter by node name.")] = None,
    job_id: Annotated[Optional[str], typer.Option("--job-id", help="Filter by job ID.")] = None,
    status: Annotated[Optional[str], typer.Option("--status", help="Filter by health status.")] = None,
    all_statuses: Annotated[bool, typer.Option("--all", help="Show warning and critical services too.")] = False,
):
    """List registered services"""
    try:
        response = client.list_services(
            name=name,
            node=node,
            job_id=job_id,
            status=status,
            passing_only=not all_statuses,
        )
        console.print_json(data=json.loads(response))
    except Exception as exc:
        handle_cli_error(exc, console, "service list")


@service_app.command(name="resolve")
def resolve_service(
    name: str,
    tag: Annotated[Optional[list[str]], typer.Option("--tag", help="Require a service tag.")] = None,
    node: Annotated[Optional[str], typer.Option("--node", help="Filter by node name.")] = None,
    all_statuses: Annotated[bool, typer.Option("--all", help="Return warning and critical services too.")] = False,
):
    """Resolve healthy instances for one service"""
    try:
        response = client.resolve_service(
            name,
            tags=tag or [],
            node=node,
            passing_only=not all_statuses,
        )
        console.print_json(data=json.loads(response))
    except Exception as exc:
        handle_cli_error(exc, console, "service resolve")


@service_app.command(name="check")
def check_services(
    bundle_path: str,
    output: Annotated[
        str,
        typer.Option("--output", "-o", help="Output format: table or json."),
    ] = "table",
):
    """Run required service checks for a local bundle"""
    try:
        output_format = _normalize_validation_output(output)
        bundle_dir = Path(bundle_path)
        manifest_path = bundle_dir / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        config = load_blueprint_config(bundle_dir)
        env = blueprint_runtime_environment(bundle_dir, config=config)

        def resolver(name: str, requirement: dict[str, Any]) -> list[dict[str, Any]]:
            response = client.resolve_service(
                name,
                tags=requirement.get("tags") or [],
                passing_only=True,
            )
            decoded = json.loads(response)
            services = decoded.get("services") if isinstance(decoded, dict) else []
            return services if isinstance(services, list) else []

        report = run_service_validation(
            bundle_dir,
            manifest,
            config=config,
            env=env,
            resolver=resolver,
        )

        if output_format == "json":
            console.print_json(data=report)
            if not report.get("ok"):
                raise typer.Exit(1)
            return

        if report.get("ok"):
            print_confirmed(
                console,
                "Service check",
                status="healthy",
                details={"Bundle": bundle_path},
            )
            return

        _emit_validation_report(report, output_format, title="Service validation failed")
        raise typer.Exit(1)
    except typer.Exit:
        raise
    except Exception as exc:
        handle_cli_error(exc, console, "service check")
        raise typer.Exit(1)
