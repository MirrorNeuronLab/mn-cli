from io import StringIO
import json
from types import SimpleNamespace

import grpc
from rich.console import Console

import mn_cli.libs.job_cmds as job_cmds


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
    monkeypatch.setattr(job_cmds, "console", Console(file=output, force_terminal=False, width=160))
    return output


def test_clear_runs_without_local_admin_token_preflight(monkeypatch):
    output = _capture_console(monkeypatch)
    client = SimpleNamespace(admin_token="", clear_jobs=lambda: 1)
    monkeypatch.setattr(job_cmds, "client", client)
    monkeypatch.setattr(job_cmds, "config", SimpleNamespace(grpc_admin_token=""))

    job_cmds.clear()

    rendered = output.getvalue()
    assert "Job clear successful" in rendered
    assert "Jobs cleared: 1 non-running" in rendered


def test_clear_reports_admin_token_mismatch(monkeypatch):
    output = _capture_console(monkeypatch)

    def clear_jobs():
        raise StubRpcError(
            grpc.StatusCode.PERMISSION_DENIED,
            "ClearJobs requires MN_GRPC_ADMIN_TOKEN",
        )

    monkeypatch.setattr(job_cmds, "client", SimpleNamespace(admin_token="local-admin-token", clear_jobs=clear_jobs))
    monkeypatch.setattr(job_cmds, "config", SimpleNamespace(grpc_admin_token="local-admin-token"))

    job_cmds.clear()

    rendered = output.getvalue()
    assert "ClearJobs admin authorization failed" in rendered
    assert "fixed gRPC admin token" in rendered


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
