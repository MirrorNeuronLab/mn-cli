"""CLI adapters for remote jobs, nodes, schedules and service reads."""

import json
from pathlib import Path
from uuid import uuid4

from mn_sdk.errors import AppError
from mn_sdk.remote_api import resource_id
from mn_cli.client_commands import confirm, unsupported
from mn_cli.client_runs import watch_operation


def execute(api, command, values):
    family, action = command[:2]
    if family == "job":
        return job(api, action, values)
    if family == "node":
        return node(api, action, values)
    if family == "operation":
        if action == "watch":
            return watch_operation(api, values["operation_id"])
        return api.request("GET", "/operations/" + resource_id(values["operation_id"]))
    if family == "runtime":
        return {
            "mode": "client",
            "runtime_installed": False,
            "remote": api.request("GET", "/runtime/diagnostics"),
        }
    if family == "schedule":
        params = {"owner_node": values.get("node", "")}
        if action == "list":
            return api.items("/schedules", **params)
        path = "/schedules/" + resource_id(values["schedule_id"])
        if action == "show":
            return api.request("GET", path, params=params)
        if action == "cancel":
            return api.conditional("DELETE", path, params=params)
        return api.conditional(
            "PATCH", path, params=params, json={"schedule": {"run_at": values["at"]}}
        )
    if family == "resource":
        if action == "set":
            unsupported("Configure runtime resource limits on the remote node.")
        return api.request(
            "GET", "/metrics" if action == "usage" else "/runtime/resources"
        )
    if family == "service":
        params = {
            k: v
            for k, v in values.items()
            if k in {"name", "node", "job_id", "status", "all_statuses", "tag"}
            and v is not None
        }
        params["passing_only"] = not params.pop("all_statuses", False)
        if action == "list":
            return api.items("/services", **params)
        return api.request(
            "GET",
            "/services/" + resource_id(values["name"]) + "/resolution",
            params={k: v for k, v in params.items() if k != "name"},
        )
    if family == "model":
        if action == "list":
            return api.items(
                "/models", installed_only=not values.get("available", False)
            )
        if action == "show":
            return api.request("GET", "/models/" + resource_id(values["model"]))
        unsupported(
            "Run model setup and lifecycle commands on the remote runtime node."
        )
    unsupported()


def read_json_option(path):
    if not path:
        return {}
    try:
        value = json.loads(Path(path).expanduser().read_text())
        if not isinstance(value, dict):
            raise ValueError
        return value
    except (OSError, ValueError):
        raise AppError(
            "MN_VALIDATION_FAILED",
            "Supply a readable JSON object for this configuration option.",
            exit_code=2,
            http_status=422,
        ) from None


def job(api, action, values):
    if action == "list":
        return api.items(
            "/jobs", include_archived=values.get("include_archived", False)
        )
    key = {"Idempotency-Key": str(uuid4())}
    if action == "create":
        from mn_cli.client_blueprints import catalog_id

        catalog_id(values["bundle"])
        body = {
            "blueprint_id": values["bundle"],
            "resolved_configuration": read_json_option(values.get("config")),
        }
        body.update({k: values[k] for k in ("job_id",) if values.get(k)})
        if values.get("node"):
            body["owner_node"] = values["node"]
        return api.request("POST", "/job-preparations", json=body, headers=key)
    if action == "restore":
        unsupported(
            "Restore backup archives on the remote node; client mode uses remote catalog sources."
        )
    path = "/jobs/" + resource_id(values["job_id"])
    if action == "show":
        return api.request("GET", path)
    if action == "analysis":
        return api.request("GET", path + "/analysis")
    if action == "start":
        if values.get("force"):
            confirm(
                "Permanently replace the remote service run and its history", values
            )
        body = {
            "run_id": values.get("run_id"),
            "inputs": read_json_option(values.get("inputs")),
            "admission_policy": "enqueue_if_busy"
            if values.get("queue")
            else "start_now",
            "replace_existing_run": values.get("force", False),
        }
        return api.request("POST", path + "/runs", json=body, headers=key)
    if action == "archive":
        return api.conditional("PATCH", path, json={"status": "archived"})
    if action in {"delete", "reset-data"}:
        if values.get("dry_run"):
            return {"dry_run": True, "job": api.request("GET", path)}
        confirm("Permanently " + action + " the remote job", values)
        if action == "delete":
            return api.conditional("DELETE", path)
        return api.request("POST", path + "/data-resets", headers=key)
    if action == "relocate":
        if values.get("abort"):
            unsupported("Abort relocation on the remote node.")
        return api.request(
            "POST",
            path + "/relocations",
            json={"target_node": values["node"], "transfer_id": uuid4().hex},
        )
    if action == "backup":
        return api.download(path + "/backups", values["output"], method="POST")
    unsupported()


def node(api, action, values):
    if action in {"list", "show"}:
        nodes = api.items("/nodes")
        if action == "list":
            return nodes
        for item in nodes:
            if values["node_name"] in [
                item.get(k) for k in ("node_id", "node_name", "node", "name", "id")
            ]:
                return item
        raise AppError(
            "MN_REMOTE_NODE_NOT_FOUND",
            "The remote node was not found.",
            http_status=404,
        )
    if action == "refresh-token":
        unsupported(
            "Client mode uses a remote API token; it has no federation join token."
        )
    path = "/nodes/" + resource_id(values["node_name"])
    if action == "drain":
        result = api.request(
            "PUT",
            path + "/drain",
            json={
                k: values[k]
                for k in ("reason", "deadline", "dry_run", "wait", "ignore_system_jobs")
            },
        )
        return watch_operation(api, result) if values.get("wait") else result
    if action == "undrain":
        if values.get("mark_eligible") or values.get("reason"):
            unsupported(
                "Set node eligibility and annotated undrain on the remote node."
            )
        return api.request("DELETE", path + "/drain")
    if action == "maintenance":
        return api.request(
            "PATCH",
            path,
            json={"maintenance": values["enable"], "reason": values["reason"]},
        )
    if action == "reconcile":
        return api.request(
            "POST",
            path + "/reconciliations",
            json={k: values[k] for k in ("reason", "dry_run")},
        )
    unsupported()
