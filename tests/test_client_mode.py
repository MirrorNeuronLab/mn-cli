"""Every installation role uses the same native SDK boundary."""

import json
import subprocess
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

from mn_cli.main import app
from mn_sdk.blueprints.authoring import write_blueprint_definition
from mn_sdk.errors import AppError
from mn_sdk.proto import runtime_control_pb2 as wire
from mn_sdk.runtime_connection import (
    REQUIRED_CAPABILITIES,
    RuntimeConnection,
    load_connection,
    save_connection,
)
from mn_sdk.runtime_control import RuntimeControlClient

runner = CliRunner()


@pytest.fixture
def remote(tmp_path, monkeypatch):
    monkeypatch.setenv("MN_HOME", str(tmp_path))
    (tmp_path / "install_metadata.json").write_text('{"install_role":"client"}')
    calls = []
    run_reads = []

    def capabilities(self):
        if self.connection.token != "remote-secret":
            raise AppError(
                "MN_RUNTIME_UNAUTHENTICATED",
                "Runtime credential rejected.",
                http_status=401,
            )
        return {
            "runtime_control_version": 1,
            "runtime_id": "runtime-one",
            "node_name": "node@remote",
            "capabilities": sorted(REQUIRED_CAPABILITIES),
        }

    def init(self, connection=None, **kwargs):
        self.connection = connection or load_connection()
        self.channel = SimpleNamespace(close=lambda: None)

    def call(self, method, action, arguments=None, **kwargs):
        calls.append((method, action, arguments or {}, kwargs, self.connection))
        capabilities(self)
        data = {"status": "completed"}
        if action == "runtime.capabilities":
            data = capabilities(self)
        elif action == "list_nodes":
            data = {"items": [{"node_name": "node@remote"}]}
        elif action == "get_node":
            data = {"node_name": "node@remote"}
        elif action == "create_blueprint_run":
            data = {
                "job_id": arguments["request"].get("job_id") or "job-1",
                "run_id": "run-1",
                "status": "completed",
            }
            if arguments["request"].get("schedule"):
                data = {
                    "job_id": "job-1",
                    "schedule_id": "schedule-1",
                    "status": "scheduled",
                }
        elif action in {"get_run", "get_run_workflow_progress"}:
            run_reads.append(action)
            data = {
                "run_id": "run-1",
                "status": "completed" if len(run_reads) > 1 else "running",
                "progress": 100,
            }
        elif action == "list_run_logs":
            data = {"items": [{"message": "remote log"}]}
        elif action == "get_run_final_artifact":
            data = {"answer": 42}
        elif action == "list_run_outputs":
            data = {"items": [{"index": 0, "name": "../../escape"}]}
        elif action == "get_job":
            data = {"job_id": "job-1", "status": "ready"}
        elif action == "blueprint.local":
            data = {"operation_id": "prepare-1", "status": "running"}
        elif action == "get_operation":
            data = {
                "operation_id": "prepare-1",
                "status": "completed",
                "result": {"job_id": "job-1", "status": "active"},
            }
        elif action == "get_schedule":
            data = {"schedule_id": "schedule-1", "status": "scheduled"}
        elif action == "node.remove":
            data = {"status": "removed", "node_name": arguments["node_name"]}
        return wire.RuntimeReply(
            version=1,
            status=200,
            value_json=json.dumps(data),
            metadata={"etag": '"rev-1"'},
        )

    @contextmanager
    def stream(self, method, action, arguments):
        calls.append((method, action, arguments, {}, self.connection))
        data = b'{"answer":42}' if "artifact" in action else b"remote output"
        yield iter(
            [
                wire.RuntimeFrame(
                    version=1,
                    kind="headers",
                    reply=wire.RuntimeReply(version=1, status=200),
                ),
                wire.RuntimeFrame(version=1, kind="data", data=data),
            ]
        )

    monkeypatch.setattr(RuntimeControlClient, "__init__", init)
    monkeypatch.setattr(RuntimeControlClient, "call", call)
    monkeypatch.setattr(RuntimeControlClient, "stream", stream)
    monkeypatch.setattr("mn_cli.client_runs.time.sleep", lambda _: None)

    def forbidden(*args, **kwargs):
        raise AssertionError("Surface invoked a local host process")

    monkeypatch.setattr(subprocess, "run", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    return calls, tmp_path


def invoke(*args):
    result = runner.invoke(app, list(args))
    assert result.exit_code == 0, (result.output, result.exception)
    return result.output


def connect():
    invoke("node", "add", "remote", "--token", "remote-secret", "--json")


def test_local_job_create_waits_and_returns_one_json_envelope(remote):
    calls, home = remote
    bundle = home / "blueprint"
    bundle.mkdir()
    save_connection(
        RuntimeConnection(
            "remote:55051", "remote-secret", "node@remote", "runtime-one", str(home)
        )
    )

    result = json.loads(
        invoke("job", "create", str(bundle), "--job-id", "job-1", "--json")
    )

    assert result["schema"] == "mn.cli/v1"
    assert result["ok"] is True
    assert result["data"] == {"job_id": "job-1", "status": "active"}
    assert [call[1] for call in calls] == ["blueprint.local", "get_operation"]


def test_catalog_launch_monitor_cancel_results_and_remove(remote):
    calls, home = remote
    connect()
    assert "node@remote" in invoke("node", "list", "--json")
    assert "node@remote" in invoke("node", "show", "node@remote", "--json")
    invoke(
        "blueprint",
        "run",
        "remote-bp",
        "--job-id",
        "existing-job",
        "--run-id",
        "run-1",
        "--node",
        "worker@remote",
        "--queue",
        "--set",
        "input.path=/remote/data.csv",
        "--json",
    )
    submission = next(call for call in calls if call[1] == "create_blueprint_run")
    body = submission[2]["request"]
    assert body["job_id"] == "existing-job" and body["run_id"] == "run-1"
    assert (
        body["owner_node"] == "worker@remote"
        and body["admission_policy"] == "enqueue_if_busy"
    )
    assert body["config_overrides"] == {"input": {"path": "/remote/data.csv"}}
    assert submission[3]["request_id"]
    assert any(call[1] == "get_run_workflow_progress" for call in calls)
    assert "remote log" in invoke("run", "logs", "run-1", "--json")
    invoke("run", "cancel", "run-1", "--json")
    cancellation = next(call for call in calls if call[1] == "update_run")
    assert cancellation[2]["request"]["desired_state"] == "cancelled"
    output = home / "download"
    invoke("run", "result", "run-1", "--output", str(output), "--json")
    assert (output / "output-0").read_bytes() == b"remote output"
    assert json.loads((output / "final_artifact.json").read_text()) == {"answer": 42}
    assert not (home / "escape").exists()
    invoke("node", "remove", "node@remote", "--yes", "--json")
    assert any(call[1] == "node.remove" for call in calls)
    assert not (home / "client.json").exists()
    assert not (home / "grpc_auth.token").exists() and not (home / "runs").exists()


@pytest.mark.parametrize(
    "args",
    [
        ("blueprint", "run", "./local"),
        ("blueprint", "run", "/absolute/path"),
        ("blueprint", "run", "folder/blueprint"),
        ("blueprint", "run", "remote-bp", "--offline"),
        ("blueprint", "run", "remote-bp", "--revision", "main"),
        ("blueprint", "--blueprint-repo", "./repo", "run", "remote-bp"),
        ("runtime", "start"),
        ("runtime", "stop"),
        ("runtime", "upgrade", "--yes"),
        ("node", "refresh-token"),
    ],
)
def test_client_rejects_local_actions_without_remote_or_host_side_effects(remote, args):
    calls, _ = remote
    save_connection(
        RuntimeConnection("remote:55051", "remote-secret", "node@remote", "runtime-one")
    )
    result = runner.invoke(app, [*args, "--json"])
    assert result.exit_code != 0
    assert any(
        code in result.output
        for code in ("MN_CLIENT_UNSUPPORTED", "MN_RUNTIME_NOT_INSTALLED")
    )
    assert not calls


def test_schedule_and_conditional_job_management(remote):
    calls, _ = remote
    connect()
    invoke("blueprint", "run", "remote-bp", "--auto-schedule", "--json")
    assert calls[-1][2]["request"]["schedule"]["kind"] == "resource_wait"
    invoke("job", "archive", "job-1", "--json")
    assert calls[-1][2]["if_match"] == '"rev-1"'
    assert calls[-1][2]["request"] == {"status": "archived"}
    invoke("schedule", "cancel", "schedule-1", "--node", "worker", "--json")
    assert calls[-1][2]["owner_node"] == "worker"
    assert calls[-1][2]["if_match"] == '"rev-1"'


def test_disconnected_status_and_help_do_not_bootstrap(remote):
    calls, _ = remote
    assert "Client mode" in invoke("--version")
    assert "client" in invoke("runtime", "status", "--json")
    invoke("blueprint", "run", "--help")
    result = runner.invoke(app, ["blueprint", "run", "remote-bp", "--json"])
    assert result.exit_code != 0 and "MN_CLIENT_NOT_CONNECTED" in result.output
    assert not calls


def test_catalog_listing_is_a_surface_command(remote, monkeypatch):
    calls, home = remote
    catalog = home / "catalog"
    catalog.mkdir()
    write_blueprint_definition(
        catalog / "example",
        {
            "apiVersion": "mn.workflow/v1",
            "kind": "Workflow",
            "id": "example",
            "name": "Surface example",
            "workflow": {"steps": [{"id": "finish"}]},
            "agents": {},
            "runtime": {},
            "contract": {},
        },
    )
    (catalog / "index.json").write_text('["example"]')
    monkeypatch.setenv("MN_BLUEPRINT_SOURCE", "local")
    monkeypatch.setenv("MN_BLUEPRINT_LOCAL", str(catalog))
    monkeypatch.setenv("MN_CLI_OUTPUT", "plain")
    assert "Surface example" in invoke("blueprint", "list")
    data = json.loads(
        invoke("blueprint", "--blueprint-repo", str(catalog), "list", "--json")
    )
    assert data["schema"] == "mn.cli/v1" and data["ok"]
    assert [item["id"] for item in data["data"]["items"]] == ["example"]
    assert not calls and not (home / "client.json").exists()


def test_full_role_uses_identical_transport_and_never_local_callback(remote):
    calls, home = remote
    (home / "install_metadata.json").write_text('{"install_role":"full"}')
    connect()
    invoke("blueprint", "run", "remote-bp", "--detached", "--json")
    assert any(call[1] == "create_blueprint_run" for call in calls)


def test_bad_credential_preserves_existing_profile(remote):
    calls, home = remote
    connect()
    before = (home / "client.json").read_bytes()
    result = runner.invoke(
        app, ["node", "add", "replacement", "--token", "invalid", "--json"]
    )
    assert result.exit_code != 0 and "MN_RUNTIME_UNAUTHENTICATED" in result.output
    assert (home / "client.json").read_bytes() == before
    assert not any(call[1] == "node.add" for call in calls)


def test_legacy_url_requires_explicit_native_reconnection(remote):
    calls, home = remote
    old = '{"api_url":"https://old/api/v1","token":"old-token"}'
    (home / "client.json").write_text(old)
    result = runner.invoke(app, ["node", "list", "--json"])
    assert (
        result.exit_code != 0
        and "MN_RUNTIME_CONNECTION_UPGRADE_REQUIRED" in result.output
    )
    assert (home / "client.json").read_text() == old and not calls
    connect()
    assert load_connection().target == "remote:55051"


@pytest.mark.parametrize("role", ["client", "full"])
@pytest.mark.parametrize("action", ["start", "stop", "unload", "probe", "doctor"])
def test_model_actions_use_runtime_native_control_in_every_role(remote, role, action):
    calls, home = remote
    (home / "install_metadata.json").write_text(json.dumps({"install_role": role}))
    connect()
    invoke("model", action, "small", "--json")
    assert calls[-1][1] == "model." + action
    assert calls[-1][2]["model"] == "small"


def test_attached_local_runtime_resolves_local_source_without_surface_preparation(
    remote,
):
    calls, home = remote
    save_connection(
        RuntimeConnection(
            "localhost:55051",
            "remote-secret",
            "node@remote",
            "runtime-one",
            str(home / "runtime"),
        )
    )
    invoke(
        "blueprint",
        "run",
        "./blueprint",
        "--offline",
        "--revision",
        "v1",
        "--detached",
        "--json",
    )
    method, action, args, kw, connection = calls[-1]
    assert method == "Execute" and action == "blueprint.local"
    assert args["runtime_home"] == str(home / "runtime")
    assert args["options"]["target"] == str(Path("./blueprint").resolve())
    assert args["options"]["offline"] is True and args["options"]["revision"] == "v1"
