"""CLI parsing for remote catalog operations; never resolve local sources."""

from uuid import uuid4
from pathlib import Path

import typer
from mn_sdk.runtime_resources import resource_id
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
    local_source = any(
        values.get(key) for key in ("source", "blueprint_repo", "revision", "offline")
    ) or bool(ctx is not None and (ctx.obj or {}).get("blueprint_repo"))
    target = str(
        values.get("target")
        or values.get("blueprint_id")
        or values.get("bundle_path")
        or ""
    )
    local_source = local_source or bool(
        target
        and (
            target.startswith((".", "/", "~"))
            or "/" in target
            or "\\" in target
            or Path(target).exists()
        )
    )
    key = {"Idempotency-Key": str(uuid4())}
    local_extended = (
        action == "remove"
        and values.get("all_")
        or action == "cleanup"
        and (
            values.get("blueprint_id")
            or any(
                not values.get(k, True)
                for k in ("include_files", "include_docker", "include_dead")
            )
        )
    )
    if local_source or local_extended:
        from mn_sdk.runtime_connection import load_connection
        from mn_sdk.runtime_control import RuntimeControlClient

        connection = load_connection()
        if not connection.runtime_home:
            unsupported(
                "Local sources require an attached local runtime. Use mn node add local --runtime-home <PATH>, or choose a remote catalog ID."
            )
        options = {
            name: str(value) if isinstance(value, Path) else value
            for name, value in values.items()
            if name != "ctx"
        }
        if ctx is not None and (ctx.obj or {}).get("blueprint_repo"):
            options["blueprint_repo"] = ctx.obj["blueprint_repo"]
        for name in ("target", "bundle_path", "source", "blueprint_repo"):
            value = options.get(name)
            if value and "://" not in str(value) and not str(value).startswith("git@"):
                path = Path(value).expanduser()
                if (
                    path.is_absolute()
                    or str(value).startswith((".", "~"))
                    or path.exists()
                ):
                    options[name] = str(path.resolve())
        if action in {"remove", "cleanup"} and not values.get("dry_run"):
            confirm(
                "Remove blueprints"
                if action == "remove"
                else "Clean unused runtime resources",
                values,
            )
        local = {
            "runtime_home": connection.runtime_home,
            "action": action,
            "options": options,
            "request_id": key["Idempotency-Key"],
        }
        if action == "run":
            return run(api, None, values, key, local=local)
        with RuntimeControlClient(connection, timeout=3600) as client:
            return client.command("blueprint.local", local)
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


def run(api, path, values, headers, *, local=None):
    from mn_cli.option_values import _parse_config_set_overrides
    from mn_cli.option_values import _run_schedule_attrs

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
    if values.get("update") and local is None:
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
    if local is not None:
        from mn_sdk.runtime_control import RuntimeControlClient

        with RuntimeControlClient(timeout=3600) as client:
            result = client.command("blueprint.local", {**local, "request": body})
    else:
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
