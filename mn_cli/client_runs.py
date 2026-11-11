"""Remote observation, run control, and explicit result downloads."""

import json
import time
from pathlib import Path
from uuid import uuid4

from mn_sdk.client_install import installation_home
from mn_sdk.errors import AppError
from mn_sdk.runtime_resources import resource_id
from mn_cli.client_commands import confirm, unsupported

TERMINAL = {
    "complete",
    "completed",
    "failed",
    "cancelled",
    "succeeded",
    "deleted",
    "stopped",
    "dry_run",
}


def _emit(value, kind="snapshot"):
    from mn_cli.output import emit_stream_record, json_enabled
    from mn_cli.libs.ui import print_detail
    from mn_cli.shared import console

    if json_enabled():
        emit_stream_record(kind, data=value)
    else:
        print_detail(console, "Remote " + kind, value)


def watch(api, run_id, *, seconds=None):
    deadline = time.monotonic() + seconds if seconds is not None else None
    last = None
    try:
        while True:
            current = api.request("GET", "/runs/" + resource_id(run_id))
            if current != last:
                _emit(current)
                last = current
            if str(current.get("status", "")).lower() in TERMINAL:
                return current
            if deadline is not None and time.monotonic() >= deadline:
                return {"run_id": run_id, "detached": True}
            progress = api.request(
                "GET", "/runs/" + resource_id(run_id) + "/workflow-progress"
            )
            _emit(progress, "progress")
            time.sleep(min(1, max(0, deadline - time.monotonic())) if deadline else 1)
    except KeyboardInterrupt:
        return {"run_id": run_id, "detached": True}


def watch_operation(api, operation, *, stream_progress=True):
    from mn_cli.output import json_enabled

    report_progress = stream_progress or not json_enabled()
    if isinstance(operation, str):
        operation = api.request("GET", "/operations/" + resource_id(operation))
    while str(operation.get("status", "")).lower() not in TERMINAL:
        if report_progress:
            _emit(operation, "progress")
        time.sleep(1)
        operation = api.request(
            "GET", "/operations/" + resource_id(operation["operation_id"])
        )
    if operation.get("status") in {"failed", "cancelled"}:
        raise AppError(
            "MN_REMOTE_OPERATION_FAILED",
            "The remote operation did not complete successfully.",
            hint="Inspect the remote operation for validation and preparation errors.",
        )
    return operation


def execute(api, command, values):
    action = command[1]
    if action == "list":
        path = (
            "/jobs/" + resource_id(values["job"]) + "/runs"
            if values.get("job")
            else "/runs"
        )
        try:
            while True:
                items = api.items(path)
                if values.get("blueprint"):
                    items = [
                        r for r in items if r.get("blueprint_id") == values["blueprint"]
                    ]
                items = items[: values.get("limit", 20)]
                if not values.get("follow"):
                    return items
                _emit({"items": items})
                time.sleep(values.get("interval", 2))
        except KeyboardInterrupt:
            return {"detached": True}
    if action == "compare":
        return {
            key: api.request("GET", "/runs/" + resource_id(values[key]))
            for key in ("run_a", "run_b")
        }
    run_id = values["run_id"]
    path = "/runs/" + resource_id(run_id)
    if action in {"show", "info"}:
        return api.request("GET", path)
    if action == "watch":
        return watch(api, run_id)
    if action in {"pause", "resume", "cancel"}:
        return api.request(
            "PATCH",
            path,
            json={
                "desired_state": {
                    "pause": "paused",
                    "resume": "running",
                    "cancel": "cancelled",
                }[action],
                "queued_only": values.get("queued_only", False),
            },
        )
    if action == "delete":
        if values.get("dry_run"):
            return {"dry_run": True, "run": api.request("GET", path)}
        confirm("Permanently delete the remote run", values)
        return api.request("DELETE", path)
    if action == "retry":
        return retry(api, path, values)
    if action == "resources":
        try:
            while True:
                result = api.request(
                    "GET",
                    path + "/resources",
                    params={k: values[k] for k in ("window", "bucket")},
                )
                if not values.get("live"):
                    return result
                _emit(result)
                time.sleep(values.get("interval", 5))
        except KeyboardInterrupt:
            return {"run_id": run_id, "detached": True}
    if action == "logs":
        return logs(api, path, values)
    if action == "result":
        return results(api, path, run_id, values.get("output"))
    if action == "human":
        action = command[2]
        path += "/human-requests"
        if action == "list":
            return api.items(path, status="pending" if values.get("pending") else None)
        request_id = values.get("request_id") or values.get("notice_id")
        path += "/" + resource_id(request_id)
        if action == "respond":
            return api.request(
                "POST",
                path + "/responses",
                json={
                    "response": {
                        k: values[k] for k in ("decision", "notes", "reviewer")
                    }
                },
            )
        return api.request(
            "POST",
            path + "/acknowledgements",
            json={"note": values.get("reviewer", "cli")},
        )
    unsupported()


def retry(api, path, values):
    from mn_sdk.run_retry import parse_retry_settings

    overrides = parse_retry_settings(values.get("settings", []))
    attempt, revision = (
        values.get("expected_attempt"),
        values.get("checkpoint_revision"),
    )
    if (attempt is None) != (revision is None) or (
        attempt is not None
        and (not values.get("idempotency_key") or values.get("dry_run"))
    ):
        unsupported(
            "Retry resubmission requires --expected-attempt, --checkpoint-revision and --idempotency-key together."
        )
    plan = (
        {"eligible": True, "expected_attempt": attempt, "checkpoint_revision": revision}
        if attempt is not None
        else api.request(
            "POST", path + "/retry-plans", json={"configuration_overrides": overrides}
        )
    )
    if values.get("dry_run"):
        return plan
    if plan.get("eligible") is not True:
        raise AppError(
            "MN_REMOTE_RETRY_UNAVAILABLE",
            "The remote run is not eligible for retry.",
            details=plan,
            http_status=409,
        )
    key = values.get("idempotency_key") or str(uuid4())
    body = {k: plan[k] for k in ("expected_attempt", "checkpoint_revision")}
    body.update(
        configuration_overrides=overrides,
        admission_policy="enqueue_if_busy" if values.get("queue") else "start_now",
    )
    _emit({"idempotency_key": key, **body}, "retry_selection")
    result = api.request(
        "POST", path + "/retries", json=body, headers={"Idempotency-Key": key}
    )
    return {**result, "idempotency_key": key}


def logs(api, path, values):
    channel = values.get("channel", "logs")
    if channel not in {"logs", "events", "all"}:
        unsupported("--channel must be logs, events, or all.")
    seen = set()
    try:
        while True:
            records = []
            for selected in ["logs", "events"] if channel == "all" else [channel]:
                params = {"level": values.get("level")} if selected == "logs" else {}
                records.extend(
                    api.items(path + "/" + selected, **params)[
                        -values.get("lines", 50) :
                    ]
                )
            if not values.get("follow"):
                return {"items": records}
            for record in records:
                identity = json.dumps(record, sort_keys=True)
                if identity not in seen:
                    _emit(record, "event")
                    seen.add(identity)
            time.sleep(values.get("interval", 1))
    except KeyboardInterrupt:
        return {"detached": True}


def results(api, path, run_id, destination):
    destination = (
        Path(destination)
        if destination
        else installation_home() / "outputs" / resource_id(run_id)
    )
    destination.mkdir(parents=True, exist_ok=True)
    api.download(path + "/artifacts/final", destination / "final_artifact.json")
    files = ["final_artifact.json"]
    for index, item in enumerate(api.items(path + "/outputs")):
        output_index = item.get("index", index)
        if not isinstance(output_index, int) or output_index < 0:
            raise AppError(
                "MN_REMOTE_API_PROTOCOL",
                "The remote output index is invalid.",
                http_status=502,
            )
        # Remote filenames cannot redirect writes outside the chosen directory.
        name = f"output-{output_index}"
        api.download(path + f"/outputs/{output_index}", destination / name)
        files.append(name)
    return {"run_id": run_id, "output": str(destination), "files": files}
