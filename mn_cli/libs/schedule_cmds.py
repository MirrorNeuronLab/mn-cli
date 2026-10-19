"""Inspect, defer, or cancel durable starts through their owner Core."""
import json
import typer
from mn_cli.shared import client, console
from mn_cli.error_handler import handle_cli_error
from mn_cli.libs.ui import print_collection, print_detail, print_success_confirmation
from mn_cli.output import record_result


def list_schedules(node: str = typer.Option("", "--node", help="Selected PC's owner node.")):
    try:
        result = json.loads(client.list_schedules(owner_node=node if isinstance(node, str) else ""))
        print_collection(console, "Schedules and queued starts", result.get("items", []),
            columns=(("ID", "schedule_id"), ("Name", "name"), ("State", "status"), ("PC", "owner_node"), ("Run", "target_run_id")))
    except Exception as error:
        handle_cli_error(error, console, "schedule list")


def show(schedule_id: str, node: str = typer.Option("", "--node")):
    try:
        print_detail(console, "Schedule", json.loads(client.get_schedule(schedule_id, owner_node=node if isinstance(node, str) else "")))
    except Exception as error:
        handle_cli_error(error, console, "schedule show")


def set_time(schedule_id: str, at: str = typer.Option(..., "--at", help="Future ISO 8601 date and time with timezone."), node: str = typer.Option("", "--node")):
    try:
        owner = node if isinstance(node, str) else ""
        current = json.loads(client.get_schedule(schedule_id, owner_node=owner))
        result = json.loads(client.update_schedule(schedule_id, owner_node=owner,
            expected_revision=current.get("revision", 0), schedule={"run_at": at}))
        print_success_confirmation(console, "Start time saved", details={"Schedule": schedule_id})
        record_result(result)
    except Exception as error:
        handle_cli_error(error, console, "schedule set-time")


def cancel(schedule_id: str, node: str = typer.Option("", "--node")):
    try:
        owner = node if isinstance(node, str) else ""
        current = json.loads(client.get_schedule(schedule_id, owner_node=owner))
        result = json.loads(client.delete_schedule(schedule_id, owner_node=owner,
            expected_revision=current.get("revision", 0)))
        print_success_confirmation(console, "Scheduled start cancelled", details={"Schedule": schedule_id})
        record_result(result)
    except Exception as error:
        handle_cli_error(error, console, "schedule cancel")
