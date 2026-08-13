from io import StringIO
import json
import subprocess
from types import SimpleNamespace

import grpc
import pytest
from rich.console import Console
import typer

import mn_cli.libs.job_cmds as job_cmds
import mn_cli.libs.job_cleanup as job_cleanup
import mn_cli.libs.operation_cmds as operation_cmds


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


def test_clear_runs_without_local_admin_token_preflight(monkeypatch):
    output = _capture_console(monkeypatch)
    client = _operation_client(
        {"operation_id": "op-test", "status": "completed", "counters": {"total": 1, "finished": 1}},
        admin_token="",
    )
    monkeypatch.setattr(job_cmds, "client", client)
    monkeypatch.setattr(job_cmds, "config", SimpleNamespace(grpc_admin_token=""))

    job_cmds.clear(yes=True)

    rendered = output.getvalue()
    assert "Job clear successful" in rendered
    assert "Operation ID:" in rendered


@pytest.mark.parametrize("plain_output", [False, True])
def test_clear_with_no_jobs_ignores_operation_completion_event(
    monkeypatch, plain_output
):
    output = _capture_console(monkeypatch)
    if plain_output:
        monkeypatch.setenv("MN_CLI_OUTPUT", "plain")
    else:
        monkeypatch.delenv("MN_CLI_OUTPUT", raising=False)

    cleaned_up = []
    monkeypatch.setattr(job_cmds, "_cleanup_cleared_job_resources", cleaned_up.append)
    monkeypatch.setattr(
        job_cmds,
        "client",
        _operation_client(
            {
                "operation_id": "op-empty",
                "status": "completed",
                "counters": {
                    "total": 0,
                    "finished": 0,
                    "succeeded": 0,
                    "failed": 0,
                    "deferred": 0,
                },
            },
            [
                json.dumps(
                    {
                        "type": "operation_completed",
                        "status": "completed",
                        "counters": {
                            "total": 0,
                            "finished": 0,
                            "succeeded": 0,
                            "failed": 0,
                            "deferred": 0,
                        },
                    }
                )
            ],
        ),
    )

    job_cmds.clear(yes=True)

    assert cleaned_up == []
    assert "Job clear successful" in output.getvalue()


def test_clear_cleans_local_resources_for_each_cleared_job(monkeypatch):
    _capture_console(monkeypatch)
    cleaned_up = []
    monkeypatch.setattr(job_cmds, "_cleanup_cleared_job_resources", cleaned_up.append)
    monkeypatch.setattr(
        job_cmds,
        "client",
        _operation_client(
            {
                "operation_id": "op-test",
                "status": "completed",
                "counters": {
                    "total": 1,
                    "finished": 1,
                    "succeeded": 1,
                    "failed": 0,
                    "deferred": 0,
                },
            },
            [
                json.dumps(
                    {
                        "type": "item_completed",
                        "item_id": "run-1",
                        "status": "cleared",
                    }
                )
            ],
        ),
    )

    job_cmds.clear(yes=True)

    assert cleaned_up == ["run-1"]


def test_clear_attempts_every_local_cleanup_before_reporting_failure(monkeypatch):
    _capture_console(monkeypatch)
    cleaned_up = []
    handled_errors = []

    def cleanup(job_id):
        cleaned_up.append(job_id)
        if job_id == "run-1":
            raise job_cleanup.JobResourceCleanupError("sandbox is busy")

    monkeypatch.setattr(job_cmds, "_cleanup_cleared_job_resources", cleanup)
    monkeypatch.setattr(
        job_cmds,
        "handle_cli_error",
        lambda error, _console, action: handled_errors.append((str(error), action)),
    )
    monkeypatch.setattr(
        job_cmds,
        "client",
        _operation_client(
            {
                "operation_id": "op-test",
                "status": "completed",
                "counters": {
                    "total": 2,
                    "finished": 2,
                    "succeeded": 2,
                    "failed": 0,
                    "deferred": 0,
                },
            },
            [
                json.dumps(
                    {
                        "type": "item_completed",
                        "item_id": run_id,
                        "status": "cleared",
                    }
                )
                for run_id in ("run-1", "run-2")
            ],
        ),
    )

    job_cmds.clear(yes=True)

    assert cleaned_up == ["run-1", "run-2"]
    assert handled_errors == [("sandbox is busy", "clear")]


def test_clear_does_not_delete_records_when_precleanup_fails(monkeypatch):
    cleaned_up = []
    handled_errors = []

    def cleanup(job_id):
        cleaned_up.append(job_id)
        raise job_cleanup.JobResourceCleanupError("checkpoint is busy")

    monkeypatch.setattr(job_cmds, "_cleanup_cleared_job_resources", cleanup)
    monkeypatch.setattr(
        job_cmds,
        "handle_cli_error",
        lambda error, _console, action: handled_errors.append((str(error), action)),
    )
    monkeypatch.setattr(
        job_cmds,
        "client",
        SimpleNamespace(
            list_jobs=lambda **_kwargs: json.dumps(
                {
                    "data": [
                        {"job_id": "run-1", "status": "completed"},
                        {"job_id": "run-active", "status": "running"},
                    ]
                }
            ),
            start_operation=lambda *_args, **_kwargs: pytest.fail(
                "Core deletion must not start after local cleanup fails"
            ),
        ),
    )

    job_cmds.clear(yes=True)

    assert cleaned_up == ["run-1"]
    assert handled_errors == [("checkpoint is busy", "clear")]


def test_clear_resource_cleanup_deletes_prepared_openshell_sandbox(monkeypatch):
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

    job_cleanup.cleanup_cleared_job_resources(
        "run-1",
        runtime_client=SimpleNamespace(),
        log=SimpleNamespace(warning=lambda *_args, **_kwargs: None),
    )

    sandbox_name = job_cleanup.openshell_sandbox_name("run-1")
    assert calls[0][-3:] == ["sandbox", "get", sandbox_name]
    assert calls[1][-3:] == ["sandbox", "delete", sandbox_name]
    assert calls[2][0:3] == ["docker", "ps", "-a"]


def test_clear_resource_cleanup_removes_run_and_generated_bundle_files(
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

    job_cleanup.cleanup_cleared_job_resources(
        "run-1",
        runtime_client=SimpleNamespace(),
        log=SimpleNamespace(warning=lambda *_args, **_kwargs: None),
    )

    assert not run_dir.exists()
    assert not generated_dir.exists()


def test_clear_resource_cleanup_rejects_unsafe_job_id(monkeypatch):
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
        job_cleanup.cleanup_cleared_job_resources(
            "../../escape",
            runtime_client=SimpleNamespace(),
            log=SimpleNamespace(
                warning=lambda message, *args, **_kwargs: warnings.append(
                    message % args
                )
            ),
        )

    assert warnings == ["Refusing local cleanup for invalid job ID: '../../escape'"]


def test_clear_resource_cleanup_fails_when_local_resources_remain(monkeypatch):
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
        job_cleanup.cleanup_cleared_job_resources(
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


def test_clear_reports_admin_token_mismatch(monkeypatch):
    output = _capture_console(monkeypatch)

    def start_operation(_kind, _options):
        raise StubRpcError(
            grpc.StatusCode.PERMISSION_DENIED,
            "StartOperation requires MN_GRPC_ADMIN_TOKEN",
        )

    monkeypatch.setattr(
        job_cmds,
        "client",
        SimpleNamespace(
            admin_token="local-admin-token",
            list_jobs=lambda **_kwargs: json.dumps({"data": []}),
            start_operation=start_operation,
        ),
    )
    monkeypatch.setattr(job_cmds, "config", SimpleNamespace(grpc_admin_token="local-admin-token"))

    job_cmds.clear(yes=True)

    rendered = output.getvalue()
    assert "ClearJobs admin authorization failed" in rendered
    assert "fixed gRPC admin token" in rendered
    assert "mn runtime start to reconcile and recreate stale-token runtime containers" in rendered
    assert "Retry after: mn runtime start; mn job clear" in rendered


def test_clear_reports_missing_local_admin_token(monkeypatch):
    output = _capture_console(monkeypatch)

    def start_operation(_kind, _options):
        raise StubRpcError(
            grpc.StatusCode.PERMISSION_DENIED,
            "StartOperation requires MN_GRPC_ADMIN_TOKEN",
        )

    monkeypatch.setattr(
        job_cmds,
        "client",
        SimpleNamespace(
            admin_token="",
            list_jobs=lambda **_kwargs: json.dumps({"data": []}),
            start_operation=start_operation,
        ),
    )
    monkeypatch.setattr(job_cmds, "config", SimpleNamespace(grpc_admin_token=""))

    job_cmds.clear(yes=True)

    rendered = output.getvalue()
    assert "ClearJobs admin authorization failed" in rendered
    assert "did not load a gRPC admin token from runtime state" in rendered
    assert "fixed gRPC admin token" not in rendered
    assert "Retry after: mn runtime start; mn job clear" in rendered


def test_cancel_all_cancels_every_active_job_without_prompt(monkeypatch):
    output = _capture_console(monkeypatch)
    list_calls = []
    cleaned_up = []
    jobs = [
        {"job_id": "job-pending", "status": "pending"},
        {"job_id": "job-validated", "status": "validated"},
        {"job_id": "job-scheduled", "status": "scheduled"},
        {"job_id": "job-running", "status": "running"},
        {"job_id": "job-paused", "status": "paused"},
        {"job_id": "job-completed", "status": "completed"},
    ]

    def list_jobs(*, limit, include_terminal):
        list_calls.append((limit, include_terminal))
        return json.dumps({"data": jobs})

    monkeypatch.setattr(
        job_cmds,
        "client",
        _operation_client(
            {
                "operation_id": "op-test",
                "status": "completed",
                "counters": {"total": 5, "finished": 5, "succeeded": 5, "failed": 0, "deferred": 0},
            },
            [
                json.dumps({"type": "item_completed", "item_id": job["job_id"], "status": "cancelled"})
                for job in jobs[:-1]
            ],
            list_jobs=list_jobs,
        ),
    )
    monkeypatch.setattr(job_cmds, "_cleanup_cancelled_job_web_ui", cleaned_up.append)
    monkeypatch.setattr(
        job_cmds.typer,
        "confirm",
        lambda *_args, **_kwargs: pytest.fail("confirmation should be skipped"),
    )

    job_cmds.cancel_all(yes=True)

    active_job_ids = [job["job_id"] for job in jobs[:-1]]
    assert list_calls == [(2_147_483_647, False)]
    assert cleaned_up == active_job_ids
    rendered = output.getvalue()
    assert "Job cancel-all successful" in rendered
    assert "Completed" in rendered
    assert "5" in rendered


def test_cancel_all_reports_when_no_active_jobs(monkeypatch):
    output = _capture_console(monkeypatch)
    monkeypatch.setattr(
        job_cmds,
        "client",
        SimpleNamespace(
            list_jobs=lambda **_kwargs: json.dumps(
                {"data": [{"job_id": "job-completed", "status": "completed"}]}
            ),
            cancel_job=lambda _job_id: pytest.fail("no job should be cancelled"),
        ),
    )
    monkeypatch.setattr(
        job_cmds.typer,
        "confirm",
        lambda *_args, **_kwargs: pytest.fail("confirmation should not be shown"),
    )

    job_cmds.cancel_all(yes=False)

    assert "no active jobs" in output.getvalue()


def test_cancel_all_aborts_when_confirmation_is_declined(monkeypatch):
    output = _capture_console(monkeypatch)
    monkeypatch.setattr(
        job_cmds,
        "client",
        SimpleNamespace(
            list_jobs=lambda **_kwargs: json.dumps(
                {"data": [{"job_id": "job-running", "status": "running"}]}
            ),
            cancel_job=lambda _job_id: pytest.fail("no job should be cancelled"),
        ),
    )
    monkeypatch.setattr(job_cmds.typer, "confirm", lambda *_args, **_kwargs: False)

    job_cmds.cancel_all(yes=False)

    rendered = output.getvalue()
    assert "Job cancel-all confirmed" in rendered
    assert "aborted" in rendered


def test_cancel_all_reports_every_failure(monkeypatch):
    output = _capture_console(monkeypatch)
    cleaned_up = []
    jobs = [
        {"job_id": "job-1", "status": "running"},
        {"job_id": "job-2", "status": "paused"},
        {"job_id": "job-3", "status": "pending"},
    ]

    monkeypatch.setattr(
        job_cmds,
        "client",
        _operation_client(
            {
                "operation_id": "op-test",
                "status": "completed_with_failures",
                "counters": {"total": 3, "finished": 3, "succeeded": 2, "failed": 1, "deferred": 0},
            },
            [
                json.dumps({"type": "item_completed", "item_id": "job-1", "status": "cancelled"}),
                json.dumps({"type": "item_completed", "item_id": "job-2", "status": "failed", "error": "remote node unavailable"}),
                json.dumps({"type": "item_completed", "item_id": "job-3", "status": "cancelled"}),
            ],
            list_jobs=lambda **_kwargs: json.dumps({"data": jobs}),
        ),
    )
    monkeypatch.setattr(job_cmds, "_cleanup_cancelled_job_web_ui", cleaned_up.append)

    with pytest.raises(typer.Exit) as exc_info:
        job_cmds.cancel_all(yes=True)

    assert exc_info.value.exit_code == 1
    assert cleaned_up == ["job-1", "job-3"]
    rendered = output.getvalue()
    assert "Job cancel-all completed with failures" in rendered
    assert "Operation ID: op-test" in rendered


def test_cancel_all_accepts_a_deferred_cluster_cancellation(monkeypatch):
    output = _capture_console(monkeypatch)
    monkeypatch.setattr(
        job_cmds,
        "client",
        _operation_client(
            {
                "operation_id": "op-test",
                "status": "completed",
                "counters": {"total": 1, "finished": 1, "succeeded": 0, "failed": 0, "deferred": 1},
            },
            [
                json.dumps(
                    {"type": "item_deferred", "item_id": "job-1", "status": "cancellation_pending"}
                )
            ],
            list_jobs=lambda **_kwargs: json.dumps({"data": [{"job_id": "job-1", "status": "running"}]}),
        ),
    )

    job_cmds.cancel_all(yes=True)

    assert "queued cleanup continues" in output.getvalue()


def test_cancel_all_plain_output_reports_deferred_progress_in_arrival_order(monkeypatch):
    output = _capture_console(monkeypatch)
    monkeypatch.setenv("MN_CLI_OUTPUT", "plain")
    monkeypatch.setattr(
        job_cmds,
        "client",
        _operation_client(
            {
                "operation_id": "op-test",
                "status": "completed",
                "counters": {"total": 2, "finished": 2, "succeeded": 1, "failed": 0, "deferred": 1},
            },
            [
                json.dumps({"type": "item_started", "item_id": "job-b", "status": "running"}),
                json.dumps({"type": "item_completed", "item_id": "job-b", "status": "cancelled"}),
                json.dumps({"type": "item_deferred", "item_id": "job-a", "status": "cancellation_pending"}),
            ],
            list_jobs=lambda **_kwargs: json.dumps(
                {"data": [{"job_id": "job-a", "status": "running"}, {"job_id": "job-b", "status": "running"}]}
            ),
        ),
    )

    job_cmds.cancel_all(yes=True)

    rendered = output.getvalue()
    assert "→ job-b: started" in rendered
    assert "✓ job-b: cancelled" in rendered
    assert "→ job-a: cancellation accepted; cleanup queued on owner node" in rendered
    assert rendered.index("job-b: cancelled") < rendered.index("job-a: cancellation accepted")


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
    assert "operator maintenance" in rendered
