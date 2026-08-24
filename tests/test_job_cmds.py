import json
import subprocess
from io import StringIO
from types import SimpleNamespace

import grpc
import pytest
import typer
from rich.console import Console

from mn_cli.libs import job_cleanup, job_cmds, job_definition_cmds, operation_cmds


class StubRpcError(grpc.RpcError):
    def __init__(self, code, details):
        self._code = code
        self._details = details

    def code(self):
        return self._code

    def details(self):
        return self._details


def _capture_console(monkeypatch):
    output = StringIO()
    test_console = Console(file=output, force_terminal=False, width=160)
    monkeypatch.setattr(job_cmds, "console", test_console)
    monkeypatch.setattr(job_definition_cmds, "console", test_console)
    monkeypatch.setattr(operation_cmds, "console", test_console)
    return output


def _operation_client(final_operation, events=(), **methods):
    client_methods = {
        "list_jobs": lambda **_kwargs: json.dumps({"data": []}),
        **methods,
    }
    return SimpleNamespace(
        start_operation=lambda _kind, _options: json.dumps(
            {"operation_id": "op-test", "target_count": final_operation.get("counters", {}).get("total", 0)}
        ),
        stream_operation_events=lambda _operation_id, **_kwargs: iter(events),
        get_operation=lambda _operation_id: json.dumps(final_operation),
        **client_methods,
    )







def test_job_resource_cleanup_deletes_prepared_openshell_sandbox(monkeypatch):
    calls = []

    def run(command, **_kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(job_cleanup.subprocess, "run", run)
    monkeypatch.setattr(
        job_cleanup, "cleanup_docker_worker_services", lambda **_kwargs: None
    )
    monkeypatch.setattr(
        job_cleanup,
        "blueprint_run_id_for_job",
        lambda _job_id, **_kwargs: None,
    )

    job_cleanup.cleanup_job_resources(
        "run-1",
        runtime_client=SimpleNamespace(),
        log=SimpleNamespace(warning=lambda *_args, **_kwargs: None),
    )

    sandbox_name = job_cleanup.openshell_sandbox_name("run-1")
    assert calls[0][-3:] == ["sandbox", "get", sandbox_name]
    assert calls[1][-3:] == ["sandbox", "delete", sandbox_name]
    assert calls[2][0:3] == ["docker", "ps", "-a"]


def test_job_resource_cleanup_removes_run_and_generated_bundle_files(
    monkeypatch, tmp_path
):
    runs_root = tmp_path / "runs"
    generated_root = tmp_path / "generated"
    run_dir = runs_root / "blueprint-run-1"
    generated_dir = generated_root / "blueprint-run-1"
    run_dir.mkdir(parents=True)
    generated_dir.mkdir(parents=True)

    monkeypatch.setattr(job_cleanup, "default_runs_root", lambda: runs_root)
    monkeypatch.setattr(
        job_cleanup, "default_generated_bundles_dir", lambda: generated_root
    )
    monkeypatch.setattr(
        job_cleanup,
        "blueprint_run_id_for_job",
        lambda _job_id, **_kwargs: "blueprint-run-1",
    )
    monkeypatch.setattr(
        job_cleanup,
        "cleanup_cancelled_job_resources",
        lambda _job_id, **_kwargs: None,
    )
    monkeypatch.setattr(
        job_cleanup, "cleanup_docker_worker_services", lambda **_kwargs: None
    )

    job_cleanup.cleanup_job_resources(
        "run-1",
        runtime_client=SimpleNamespace(),
        log=SimpleNamespace(warning=lambda *_args, **_kwargs: None),
    )

    assert not run_dir.exists()
    assert not generated_dir.exists()


def test_job_resource_cleanup_rejects_unsafe_job_id(monkeypatch):
    warnings = []
    monkeypatch.setattr(
        job_cleanup.shutil,
        "rmtree",
        lambda *_args, **_kwargs: pytest.fail("unsafe path must not be removed"),
    )
    monkeypatch.setattr(
        job_cleanup.subprocess,
        "run",
        lambda *_args, **_kwargs: pytest.fail("unsafe resource must not be queried"),
    )
    monkeypatch.setattr(
        job_cleanup,
        "cleanup_docker_worker_services",
        lambda **_kwargs: pytest.fail("unsafe resource must not be queried"),
    )

    with pytest.raises(job_cleanup.JobResourceCleanupError, match="invalid job ID"):
        job_cleanup.cleanup_job_resources(
            "../../escape",
            runtime_client=SimpleNamespace(),
            log=SimpleNamespace(
                warning=lambda message, *args, **_kwargs: warnings.append(
                    message % args
                )
            ),
        )

    assert warnings == ["Refusing local cleanup for invalid job ID: '../../escape'"]


def test_job_resource_cleanup_fails_when_local_resources_remain(monkeypatch):
    docker_cleanup = []
    removed_paths = []
    monkeypatch.setattr(
        job_cleanup,
        "blueprint_run_id_for_job",
        lambda _job_id, **_kwargs: None,
    )
    monkeypatch.setattr(
        job_cleanup,
        "cleanup_cancelled_job_resources",
        lambda _job_id, **_kwargs: {"errors": ["OpenShell sandbox is busy"]},
    )
    monkeypatch.setattr(
        job_cleanup,
        "cleanup_docker_worker_services",
        lambda **kwargs: docker_cleanup.append(kwargs) or {"errors": []},
    )
    monkeypatch.setattr(
        job_cleanup.shutil,
        "rmtree",
        lambda path: removed_paths.append(path),
    )

    with pytest.raises(
        job_cleanup.JobResourceCleanupError, match="OpenShell sandbox is busy"
    ):
        job_cleanup.cleanup_job_resources(
            "run-1",
            runtime_client=SimpleNamespace(),
            log=SimpleNamespace(warning=lambda *_args, **_kwargs: None),
        )

    assert docker_cleanup == [{"job_id": "run-1"}]
    assert removed_paths == [job_cleanup.Path("/tmp/mn_run-1")]


def test_openshell_cleanup_name_matches_long_prepared_sandbox_name():
    job_id = f"{'a' * 33}-{'b' * 80}"

    sandbox_name = job_cleanup.openshell_sandbox_name(job_id)

    assert sandbox_name == (
        "mirror-neuron-job-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        "-820154137a"
    )
    assert len(sandbox_name) <= 63


def test_job_list_shows_type_and_owning_node(monkeypatch):
    output = _capture_console(monkeypatch)
    payload = {
        "items": [
            {
                "job_id": "job-service",
                "type": "service",
                "status": "active",
                "owner_node": "mirror_neuron@spark",
                "updated_at": "2026-08-24T17:23:25.642Z",
            },
            {
                "job_id": "job-batch",
                "type": "batch",
                "status": "archived",
                "owner_node": "mirror_neuron@homer",
                "updated_at": "2026-08-24T16:23:25.642Z",
            },
        ]
    }
    monkeypatch.setattr(
        job_definition_cmds,
        "client",
        SimpleNamespace(list_jobs=lambda **_kwargs: json.dumps(payload)),
    )

    job_definition_cmds.definitions(include_archived=True)

    rendered = output.getvalue()
    assert "Type" in rendered
    assert "Node" in rendered
    assert "Owner" not in rendered
    assert "service" in rendered
    assert "batch" in rendered
    assert "mirror_neuron@spark" in rendered
    assert "mirror_neuron@homer" in rendered










def test_node_list_strips_restart_history_and_reasons(monkeypatch):
    output = _capture_console(monkeypatch)
    summary = {
        "nodes": [
            {
                "name": "mirror_neuron@local",
                "status": "healthy",
                "restart_history": [
                    {"at": "2026-07-03T00:00:00Z", "reason": "model emitted invalid JSON"}
                ],
                "restartReason": "runtime config changed",
                "restart_exhausted_reason": "attempts exhausted",
                "drain": {"reason": "operator maintenance"},
            }
        ],
        "jobs": [
            {
                "job_id": "job-1",
                "agents": [
                    {
                        "agent_id": "research_planner",
                        "restartHistory": [{"reason": "actor failed"}],
                    }
                ],
            }
        ],
    }
    monkeypatch.setattr(
        job_cmds,
        "client",
        SimpleNamespace(get_system_summary=lambda: json.dumps(summary)),
    )

    job_cmds.nodes()

    rendered = output.getvalue()
    assert "restart_history" not in rendered
    assert "restartHistory" not in rendered
    assert "restartReason" not in rendered
    assert "restart_exhausted_reason" not in rendered
    assert "model emitted invalid JSON" not in rendered
    assert "actor failed" not in rendered
    assert "attempts exhausted" not in rendered
    assert "operator maintenance" not in rendered


def test_node_list_uses_complete_node_specific_columns(monkeypatch):
    output = _capture_console(monkeypatch)
    summary = {
        "nodes": [
            {
                "name": "mirror_neuron@10.0.4.23",
                "display_name": "spark",
                "hostname": "spark",
                "grpc_host": "10.0.4.23",
                "grpc_port": 55051,
                "status": "healthy",
                "connection_mode": "federated",
                "job_owner_eligible": True,
                "scheduling_eligible": True,
            },
            {
                "name": "mirror_neuron@10.0.4.26",
                "status": "maintenance",
                "self?": True,
                "job_owner_eligible": False,
            },
        ]
    }
    monkeypatch.setattr(
        job_cmds,
        "client",
        SimpleNamespace(get_system_summary=lambda: json.dumps(summary)),
    )

    job_cmds.nodes()

    rendered = output.getvalue()
    for label in ("Node", "Hostname", "Status", "Role"):
        assert label in rendered
    assert "Kind" not in rendered
    assert "Node / Owner" not in rendered
    assert "Updated" not in rendered
    assert "spark" in rendered
    assert "federated" in rendered
    assert "job owner" in rendered
    assert "local" in rendered
    assert "jobs unavailable" in rendered
    assert "10.0.4.26" in rendered
