from io import StringIO
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


def test_clear_preflights_missing_admin_token(monkeypatch):
    output = _capture_console(monkeypatch)
    client = SimpleNamespace(admin_token="", clear_jobs=lambda: 1)
    monkeypatch.setattr(job_cmds, "client", client)
    monkeypatch.setattr(job_cmds, "config", SimpleNamespace(grpc_admin_token=""))

    job_cmds.clear()

    rendered = output.getvalue()
    assert "No local gRPC admin token was found" in rendered
    assert "shared grpc_admin.token file" in rendered


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
    assert "different cluster tokens" in rendered
