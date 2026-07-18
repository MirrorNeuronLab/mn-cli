from io import StringIO
import json
from types import SimpleNamespace

import grpc
import pytest
from rich.console import Console
import typer

import mn_cli.libs.job_cmds as job_cmds
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
    return SimpleNamespace(
        start_operation=lambda _kind, _options: json.dumps(
            {"operation_id": "op-test", "target_count": final_operation.get("counters", {}).get("total", 0)}
        ),
        stream_operation_events=lambda _operation_id, **_kwargs: iter(events),
        get_operation=lambda _operation_id: json.dumps(final_operation),
        **methods,
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


def test_clear_reports_admin_token_mismatch(monkeypatch):
    output = _capture_console(monkeypatch)

    def start_operation(_kind, _options):
        raise StubRpcError(
            grpc.StatusCode.PERMISSION_DENIED,
            "StartOperation requires MN_GRPC_ADMIN_TOKEN",
        )

    monkeypatch.setattr(job_cmds, "client", SimpleNamespace(admin_token="local-admin-token", start_operation=start_operation))
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

    monkeypatch.setattr(job_cmds, "client", SimpleNamespace(admin_token="", start_operation=start_operation))
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
