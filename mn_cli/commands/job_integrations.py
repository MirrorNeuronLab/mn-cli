"""Manage and connect the same job that the desktop supervises."""

import time
from pathlib import Path
from uuid import uuid4

import typer
from mn_surface.resources import resource_id

from mn_cli.client_commands import dispatch, display
from mn_cli.client_management import read_json_option
from mn_cli.client_runs import watch_operation
from mn_cli.output import json_enabled


def command(parts, values):
    return display(parts, dispatch(parts, values))


def connect(
    provider: str,
    name: str | None = typer.Option(None),
    job_id: str = typer.Option(...),
    channel: str | None = typer.Option(None),
    session: str | None = typer.Option(None),
):
    """Connect a public channel, reusing an approved workspace on this device."""
    return command(("job", "connect"), locals())


def list_skills(job_id: str = typer.Option(...)):
    return command(("job", "skills", "list"), locals())


def add_skill(declaration: Path, job_id: str = typer.Option(...)):
    """Prepare a dependency declared in a JSON file for subsequent runs."""
    return command(("job", "skills", "add"), locals())


def enable(name: str, job_id: str = typer.Option(...)):
    return command(("job", "skills", "enable"), locals())


def disable(name: str, job_id: str = typer.Option(...)):
    return command(("job", "skills", "disable"), locals())


def integrations(job_id: str = typer.Option(...)):
    return command(("job", "integrations", "list"), locals())


def add_provider(provider: str, job_id: str = typer.Option(...)):
    """Make a provider visible to this job before authorizing delivery."""
    return command(("job", "integrations", "add"), locals())


def pause(provider: str, job_id: str = typer.Option(...)):
    return command(("job", "integrations", "pause"), locals())


def disconnect(provider: str, job_id: str = typer.Option(...)):
    return command(("job", "integrations", "disconnect"), locals())


def register(job):
    skills = typer.Typer(help="Manage prepared job skills.")
    providers = typer.Typer(help="Manage this job's connections.")
    for name, fn in [
        ("list", list_skills),
        ("add", add_skill),
        ("enable", enable),
        ("disable", disable),
    ]:
        skills.command(name)(fn)
    for name, fn in [
        ("list", integrations),
        ("add", add_provider),
        ("pause", pause),
        ("disconnect", disconnect),
    ]:
        providers.command(name)(fn)
    job.add_typer(skills, name="skills")
    job.add_typer(providers, name="integrations")
    job.command("connect")(connect)


def execute(api, parts, values):
    base = "/jobs/" + resource_id(values["job_id"])
    if parts[1] == "skills":
        path = base + "/skills"
        if parts[2] == "list":
            return api.request("GET", path)
        if parts[2] == "add":
            receipt = api.request(
                "POST",
                path,
                json=read_json_option(values["declaration"]),
                headers={"Idempotency-Key": str(uuid4())},
            )
            return (
                watch_operation(api, receipt)
                if receipt.get("operation_id")
                else receipt
            )
        return api.request(
            "PUT",
            path + "/" + resource_id(values["name"]),
            json={"enabled": parts[2] == "enable"},
        )
    if parts[-1] == "list":
        return api.request("GET", base + "/integrations")
    path = base + "/integrations/" + resource_id(values["provider"])
    if parts[1:] == ("integrations", "add"):
        return api.request("PUT", path)
    if parts[1] != "connect":
        return api.request("POST", path + "/" + parts[-1], json={})
    body = {"name": values["name"]} if values.get("name") else {}
    if values.get("session"):
        body["session"] = values["session"]
    connect_path = path + "/connect"
    setup = api.request("POST", connect_path, json=body)
    if setup.get("operation_id"):
        watch_operation(api, setup)
        setup = api.request("POST", connect_path, json=body)
    if setup.get("delivery") == "enabled":
        return setup
    body["session"] = setup["session"]
    if json_enabled() and not values.get("channel"):
        return setup
    try:
        if setup.get("url"):
            typer.echo("Authorize this workspace: " + setup["url"], err=True)
        deadline = time.monotonic() + 600
        while setup.get("authorization") == "pending":
            if time.monotonic() >= deadline:
                raise typer.BadParameter("Authorization expired. Run connect again.")
            time.sleep(2)
            setup = api.request("POST", connect_path, json=body)
        choices = {item["id"]: item["name"] for item in setup.get("channels", [])}
        if not choices:
            raise typer.BadParameter(
                "Invite the app to a public channel and run connect again."
            )
        channel = values.get("channel")
        if not channel:
            for identity, name in choices.items():
                typer.echo(f"#{name}  {identity}", err=True)
            channel = typer.prompt("Public channel ID")
        if channel not in choices:
            raise typer.BadParameter("Select a listed public channel.")
        return api.request("POST", connect_path, json={**body, "channel": channel})
    except (KeyboardInterrupt, typer.Abort):
        api.request("POST", path + "/cancel", json=body)
        raise
