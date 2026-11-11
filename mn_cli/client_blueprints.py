"""CLI parsing for remote catalog operations; never resolve local sources."""

from uuid import uuid4

import typer
from mn_sdk.remote_api import resource_id
from mn_cli.client_commands import confirm, unsupported


def catalog_id(value):
    if (
        not value
        or str(value).startswith((".", "/", "~"))
        or any(char in str(value) for char in ("/", "\\", "://"))
    ):
        unsupported(
            "Supply a blueprint ID from the remote catalog; local blueprint paths are unavailable."
        )
    return resource_id(value)


def execute(api, command, values):
    action = command[1]
    ctx = values.get("ctx")
    if any(
        values.get(key) for key in ("source", "blueprint_repo", "revision", "offline")
    ) or (ctx is not None and (ctx.obj or {}).get("blueprint_repo")):
        unsupported(
            "Client mode uses the remote catalog; local source-selection options are unavailable."
        )
    key = {"Idempotency-Key": str(uuid4())}
    if action == "list":
        return api.items("/blueprints")
    if action == "update":
        if not values.get("blueprint_id") and not values.get("all_"):
            raise typer.BadParameter("Supply a blueprint ID or --all.")
        return api.request("POST", "/blueprint-catalog-refreshes", headers=key)
    if action == "cleanup":
        if values.get("blueprint_id") or not all(
            values.get(k, True)
            for k in ("include_files", "include_docker", "include_dead")
        ):
            unsupported(
                "The remote cleanup endpoint supports complete cleanup and --dry-run."
            )
        if not values.get("dry_run"):
            confirm("Clean unused resources on the remote node", values)
        return api.request(
            "POST",
            "/blueprint-cleanups",
            json={"dry_run": values.get("dry_run", False)},
            headers=key,
        )
    if action == "export":
        path = "/runs/" + resource_id(values["run_id"]) + "/export"
        if values.get("output"):
            # Export is a remote read; writing a requested result is safe locally.
            path += "?format=" + resource_id(values["output_format"])
            return api.download(path, values["output"])
        with api.stream(path, params={"format": values["output_format"]}) as response:
            return {"export": response.read().decode("utf-8")}
    blueprint = catalog_id(
        values.get("target") or values.get("blueprint_id") or values.get("bundle_path")
    )
    path = "/blueprints/" + blueprint
    if action == "show":
        return api.request("GET", path)
    if action == "add":
        return api.request(
            "POST",
            path + "/additions",
            json={"force": values.get("force", False)},
            headers=key,
        )
    if action == "remove":
        if values.get("all_"):
            unsupported("Remove remote blueprints individually by catalog ID.")
        if not values.get("dry_run"):
            confirm("Remove this blueprint from the remote node", values)
        body = {
            k: values.get(k, False)
            for k in ("keep_resources", "keep_models", "remove_models", "dry_run")
        }
        return api.request("POST", path + "/removals", json=body, headers=key)
    if action in {"validate", "doctor"}:
        return api.request("POST", path + "/validations", json={})
    if action != "run":
        unsupported()
    return run(api, path, values, key)


def run(api, path, values, headers):
    from mn_cli.libs.blueprint_cmds import _parse_config_set_overrides
    from mn_cli.libs.run_cmds.common import _run_schedule_attrs

    if values.get("queue") and (values.get("schedule") or values.get("auto_schedule")):
        raise typer.BadParameter(
            "Pass --queue on its own, without --auto-schedule or --schedule."
        )
    if values.get("replace_existing_run"):
        if not values.get("job_id"):
            raise typer.BadParameter("--replace-existing-run requires --job-id.")
        confirm("Permanently replace the remote service run and its history", values)
    config = _parse_config_set_overrides(values.get("set_values"))
    for flag in ("benchmark", "debug", "fake_skills"):
        if values.get(flag):
            config.setdefault("execution", {})[flag] = True
    for flag, field in (("web_ui_host", "host"), ("web_ui_port", "port")):
        if values.get(flag) is not None:
            config.setdefault("web_ui", {}).setdefault("service", {})[field] = values[
                flag
            ]
    if values.get("update"):
        operation = api.request(
            "POST",
            "/blueprint-catalog-refreshes",
            headers={"Idempotency-Key": str(uuid4())},
        )
        from mn_cli.client_runs import watch_operation

        watch_operation(api, operation)
    body = {
        k: values[k]
        for k in (
            "run_id",
            "job_id",
            "owner_node",
            "force",
            "fake_llm",
            "fake_skills",
            "replace_existing_run",
        )
        if values.get(k) is not None
    }
    body["config_overrides"] = config
    body["admission_policy"] = "enqueue_if_busy" if values.get("queue") else "start_now"
    schedule = _run_schedule_attrs(
        auto_schedule=values.get("auto_schedule", False),
        schedule=values.get("schedule"),
    )
    if schedule is not None:
        body["schedule"] = schedule
    result = api.request("POST", path + "/runs", json=body, headers=headers)
    result["idempotency_key"] = headers["Idempotency-Key"]
    if values.get("web_ui") and result.get("job_id"):
        result["web_ui"] = api.request(
            "GET", "/jobs/" + resource_id(result["job_id"]) + "/ui"
        )
    if not values.get("detached") and schedule is None and result.get("run_id"):
        from mn_cli.client_runs import watch

        watch(api, result["run_id"], seconds=values.get("follow_seconds"))
    return result
