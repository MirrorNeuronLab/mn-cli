import json
from io import StringIO
from types import SimpleNamespace
import pytest
import typer
from typer.testing import CliRunner
from rich.console import Console
from mn_cli.libs import job_analysis_cmd as command

@pytest.mark.parametrize("plain", [False, True])
@pytest.mark.parametrize("width", [45, 120])
def test_command_readable_and_json(monkeypatch, plain, width):
    monkeypatch.setenv("MN_CLI_OUTPUT", "plain" if plain else "rich")
    monkeypatch.setattr(command, "client", SimpleNamespace(
        get_job=lambda id, **kwargs: json.dumps({"job_id": id, "run_count": 0}),
        list_runs=lambda *args, **kwargs: json.dumps({"items": []})))
    stream = StringIO()
    monkeypatch.setattr(command, "console", Console(file=stream, width=width, force_terminal=False))
    monkeypatch.setattr(command, "record_result", lambda _: None)
    app = typer.Typer()
    app.command()(command.analysis)
    result = CliRunner().invoke(app, ["job-a", "--json"])
    assert result.exit_code == 0, result.exception
    assert json.loads(stream.getvalue())["job_id"] == "job-a"
    stream.truncate(0); stream.seek(0)
    result = CliRunner().invoke(app, ["job-a"])
    assert result.exit_code == 0, result.exception
    assert "Successful" in stream.getvalue()
    assert "tokens used" in stream.getvalue().lower()


def test_registered_command_uses_standard_json_envelope(monkeypatch):
    from mn_cli.main import app
    calls = []
    def get_job(job_id, **kwargs):
        calls.append(kwargs)
        return json.dumps({"job_id": job_id, "run_count": 0})
    monkeypatch.setattr(command, "client", SimpleNamespace(get_job=get_job, list_runs=lambda *args, **kwargs: json.dumps({"items": []})))
    result = CliRunner().invoke(app, ["job", "analysis", "job-a", "--json"])
    assert result.exit_code == 0, result.output
    envelope = json.loads(result.output)
    assert envelope["data"]["job_id"] == "job-a"
    assert envelope["data"]["runs"]["total"] == 0
    assert 0 < calls[0]["timeout"] <= 20
