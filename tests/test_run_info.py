import json
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

from mn_cli.libs import job_definition_cmds
from mn_cli.main import app


@pytest.fixture
def run_info(monkeypatch):
    record = {"run_id": "run-1", "status": "paused", "placement": {
        "owner_node": "spark@pc", "node_label": "Spark", "reservation": {
            "state": "reserved", "mode": "shared", "cpu_cores": 2.5,
            "memory_mb": 8192, "gpu_count": 1, "gpu_memory_mb": 4096, "disk_mb": 1024,
        },
    }}
    calls = []
    monkeypatch.setattr(job_definition_cmds, "client", SimpleNamespace(
        get_run=lambda run_id: calls.append(run_id) or json.dumps(record)))
    return record, calls


@pytest.mark.parametrize("command", ["show", "info"])
@pytest.mark.parametrize("mode", ["auto", "plain"])
@pytest.mark.parametrize("width", [48, 120])
def test_run_info_shows_assignment_and_each_reservation_at_narrow_and_wide_widths(run_info, command, mode, width):
    _, calls = run_info
    result = CliRunner().invoke(app, ["run", command, "run-1"], env={"COLUMNS": str(width), "MN_CLI_OUTPUT": mode})
    assert result.exit_code == 0, result.stdout
    for value in ["Spark (spark@pc)", "2.5 cores", "8 GB", "4 GB", "1 GB"]:
        assert value in result.stdout
    assert result.stdout.index("Assigned Node") < result.stdout.index("Run Id")
    assert calls == ["run-1"]


@pytest.mark.parametrize("command", ["show", "info"])
def test_run_info_json_preserves_the_exact_runtime_record(run_info, command):
    record, _ = run_info
    result = CliRunner().invoke(app, ["run", command, "run-1", "--json"])
    assert result.exit_code == 0
    assert json.loads(result.stdout)["data"] == record


@pytest.mark.parametrize("state,expected", [("queued", "waiting for resources"), ("unreserved", "No resources reserved"), ("unavailable", "Unavailable")])
def test_run_info_distinguishes_reservation_states(run_info, state, expected):
    record, _ = run_info
    record["placement"]["reservation"] = {"state": state}
    result = CliRunner().invoke(app, ["run", "info", "run-1"])
    assert result.exit_code == 0
    assert expected in result.stdout
    assert "Reserved Cpu" not in result.stdout
