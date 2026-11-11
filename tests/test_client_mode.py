import json
import subprocess

import httpx
import pytest
from typer.testing import CliRunner

from mn_cli.main import app
from mn_sdk.client_install import RemoteConnection, save_connection
from mn_sdk.remote_api import RemoteApiClient

runner = CliRunner()


@pytest.fixture
def remote(tmp_path, monkeypatch):
    monkeypatch.setenv("MN_HOME", str(tmp_path))
    (tmp_path / "install_metadata.json").write_text('{"install_role":"client"}')
    calls = []
    run_reads = []

    def handler(request):
        calls.append(request)
        path = request.url.path.removeprefix("/api/v1")
        assert request.headers["authorization"] == "Bearer remote-secret"
        if path == "/health":
            payload = {"api_contract": "mirrorneuron.rest.v1"}
        elif path == "/system/summary":
            payload = {"identity": {"actual": "node@remote", "valid": True}}
        elif path == "/nodes":
            payload = {"items": [{"node_name": "node@remote"}]}
        elif path == "/blueprints":
            payload = {"items": [{"blueprint_id": "remote-bp"}]}
        elif path.endswith("/validations"):
            payload = {"valid": True}
        elif path == "/blueprints/remote-bp/runs":
            body = json.loads(request.content)
            payload = {
                "job_id": body.get("job_id") or "job-1",
                "run_id": "run-1",
                "status": "running",
            }
            if body.get("schedule"):
                payload = {
                    "job_id": "job-1",
                    "schedule_id": "schedule-1",
                    "status": "scheduled",
                }
        elif path == "/runs/run-1/workflow-progress":
            payload = {"status": "running", "progress": 0.5}
        elif path == "/runs/run-1":
            run_reads.append(request)
            payload = {
                "run_id": "run-1",
                "status": "completed" if len(run_reads) > 1 else "running",
            }
        elif path == "/runs/run-1/logs":
            payload = {"items": [{"message": "remote log"}]}
        elif path == "/runs/run-1/artifacts/final":
            payload = {"answer": 42}
        elif path == "/runs/run-1/outputs":
            payload = {"items": [{"index": 0, "name": "../../escape"}]}
        elif path == "/runs/run-1/outputs/0":
            return httpx.Response(200, content=b"remote output")
        elif path == "/jobs/job-1":
            payload = {"job_id": "job-1", "status": "ready"}
        elif path == "/schedules/schedule-1":
            payload = {"schedule_id": "schedule-1", "status": "scheduled"}
        else:
            raise AssertionError(
                f"Unexpected remote request: {request.method} {request.url}"
            )
        return httpx.Response(200, json=payload, headers={"ETag": '"rev-1"'})

    transport = httpx.MockTransport(handler)
    monkeypatch.setattr(
        RemoteApiClient,
        "_client",
        lambda self: httpx.Client(
            transport=transport,
            base_url=self.connection.api_url + "/",
            headers={"Authorization": "Bearer " + self.connection.token},
        ),
    )

    def forbidden(*args, **kwargs):
        raise AssertionError("Client reached local preparation or runtime bootstrap")

    from mn_cli.libs import blueprint_cmds

    for name in (
        "run_catalog_blueprint",
        "run_local_blueprint_folder",
        "_install_blueprint_model_dependencies",
        "_prepare_blueprint_bundle_for_run",
    ):
        monkeypatch.setattr(blueprint_cmds, name, forbidden)
    monkeypatch.setattr("grpc.insecure_channel", forbidden)
    monkeypatch.setattr(subprocess, "run", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    monkeypatch.setattr("mn_cli.client_runs.time.sleep", lambda _: None)
    return calls, tmp_path


def invoke(*args):
    result = runner.invoke(app, list(args))
    assert result.exit_code == 0, (result.output, result.exception)
    return result.output


def connect():
    invoke("node", "add", "https://remote", "--token", "remote-secret", "--json")


def test_cli_connection_catalog_launch_monitor_cancel_results_and_disconnect(remote):
    calls, home = remote
    connect()
    assert "remote-bp" in invoke("blueprint", "list", "--json")
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
    submission = next(
        r for r in calls if r.url.path.endswith("/blueprints/remote-bp/runs")
    )
    body = json.loads(submission.content)
    assert body["job_id"] == "existing-job"
    assert body["run_id"] == "run-1"
    assert body["owner_node"] == "worker@remote"
    assert body["admission_policy"] == "enqueue_if_busy"
    assert body["config_overrides"] == {"input": {"path": "/remote/data.csv"}}
    assert submission.headers["idempotency-key"]
    assert any(r.url.path.endswith("/workflow-progress") for r in calls)
    assert "remote log" in invoke("run", "logs", "run-1", "--json")
    invoke("run", "cancel", "run-1", "--json")
    patch = next(r for r in calls if r.method == "PATCH")
    assert json.loads(patch.content)["desired_state"] == "cancelled"
    output = home / "download"
    invoke("run", "result", "run-1", "--output", str(output), "--json")
    assert (output / "output-0").read_bytes() == b"remote output"
    assert json.loads((output / "final_artifact.json").read_text()) == {"answer": 42}
    assert not (home / "escape").exists()
    invoke("node", "remove", "node@remote", "--yes", "--json")
    assert not (home / "client.json").exists()
    assert not any(
        r.method in {"POST", "DELETE"} and r.url.path.startswith("/api/v1/nodes")
        for r in calls
    )
    assert not (home / "grpc_auth.token").exists()
    assert not (home / "runs").exists()


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
        ("runtime", "upgrade"),
        ("node", "refresh-token"),
    ],
)
def test_client_rejects_local_paths_and_runtime_actions_without_http(remote, args):
    calls, _ = remote
    save_connection(
        RemoteConnection("https://remote/api/v1", "remote-secret", "node@remote")
    )
    result = runner.invoke(app, [*args, "--json"])
    assert result.exit_code != 0
    assert "MN_CLIENT_UNSUPPORTED" in result.output
    assert calls == []


def test_schedule_and_conditional_job_management(remote):
    calls, _ = remote
    connect()
    invoke("blueprint", "run", "remote-bp", "--auto-schedule", "--json")
    body = json.loads(calls[-1].content)
    assert body["schedule"]["kind"] == "resource_wait"
    invoke("job", "archive", "job-1", "--json")
    assert calls[-1].headers["if-match"] == '"rev-1"'
    assert json.loads(calls[-1].content) == {"status": "archived"}
    invoke("schedule", "cancel", "schedule-1", "--node", "worker", "--json")
    assert calls[-1].method == "DELETE"
    assert calls[-1].url.params["owner_node"] == "worker"
    assert calls[-1].headers["if-match"] == '"rev-1"'


def test_disconnected_status_help_and_failures_never_bootstrap(remote):
    calls, _ = remote
    assert "Client mode" in invoke("--version")
    assert "client" in invoke("runtime", "status", "--json")
    invoke("blueprint", "run", "--help")
    result = runner.invoke(app, ["blueprint", "run", "remote-bp", "--json"])
    assert result.exit_code != 0
    assert "MN_CLIENT_NOT_CONNECTED" in result.output
    assert not calls


def test_wrapping_preserves_normal_runtime_callback(tmp_path, monkeypatch):
    from mn_cli.client_commands import _wrap

    monkeypatch.setenv("MN_HOME", str(tmp_path))
    calls = []
    callback = _wrap(lambda target: calls.append(target), ("blueprint", "run"))
    callback("local-bp")
    assert calls == ["local-bp"]


def test_auth_failure_and_failed_reconnect_preserve_cli_connection(remote, monkeypatch):
    _calls, home = remote
    connect()
    previous = (home / "client.json").read_bytes()
    requests = []

    def rejected(request):
        requests.append(request)
        return httpx.Response(401, json={"detail": "Unauthorized"})

    monkeypatch.setattr(
        RemoteApiClient,
        "_client",
        lambda self: httpx.Client(
            transport=httpx.MockTransport(rejected),
            base_url=self.connection.api_url + "/",
        ),
    )
    for args in (
        ["blueprint", "run", "remote-bp", "--detached"],
        ["node", "add", "https://replacement", "--token", "invalid"],
    ):
        result = runner.invoke(app, [*args, "--json"])
        assert result.exit_code != 0
        assert "MN_REMOTE_API_ERROR" in result.output
        assert "API token" in result.output
        assert (home / "client.json").read_bytes() == previous
    assert len(requests) == 2
