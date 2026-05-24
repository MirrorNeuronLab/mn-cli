import json

from typer.testing import CliRunner

from mn_cli.main import app


runner = CliRunner()


def _bundle(tmp_path):
    bundle = tmp_path / "bundle"
    payloads = bundle / "payloads"
    payloads.mkdir(parents=True)
    (bundle / "manifest.json").write_text('{"graph_id": "scheduled"}', encoding="utf-8")
    (payloads / "input.json").write_bytes(b"{}")
    return bundle


def test_schedule_create_passes_cron_policy_and_payloads(mocker, tmp_path):
    mock_create = mocker.patch(
        "mn_cli.libs.schedule_cmds.client.create_schedule",
        return_value=json.dumps({"schedule_id": "sched-1", "kind": "periodic"}),
    )

    result = runner.invoke(
        app,
        ["schedule", "create", str(_bundle(tmp_path)), "--cron", "0 2 * * *", "--window", "30m"],
    )

    assert result.exit_code == 0
    assert "sched-1" in result.stdout
    args, kwargs = mock_create.call_args
    assert args[0] == '{"graph_id": "scheduled"}'
    assert args[1] == {"input.json": b"{}"}
    assert kwargs["schedule"]["crons"] == ["0 2 * * *"]
    assert kwargs["schedule"]["window"]["duration_ms"] == 1_800_000


def test_trigger_create_builds_event_schedule(mocker, tmp_path):
    mock_create = mocker.patch(
        "mn_cli.libs.schedule_cmds.client.create_schedule",
        return_value=json.dumps({"schedule_id": "sched-event"}),
    )

    result = runner.invoke(
        app,
        [
            "trigger",
            "create",
            str(_bundle(tmp_path)),
            "--event",
            "file_uploaded",
            "--filter-json",
            '{"path": {"prefix": "datasets/"}}',
        ],
    )

    assert result.exit_code == 0
    schedule = mock_create.call_args.kwargs["schedule"]
    assert schedule["kind"] == "event"
    assert schedule["trigger"]["event_type"] == "file_uploaded"
    assert schedule["trigger"]["filters"]["path"]["prefix"] == "datasets/"


def test_event_emit_passes_payload(mocker):
    mock_emit = mocker.patch(
        "mn_cli.libs.schedule_cmds.client.emit_trigger_event",
        return_value=json.dumps({"dispatched": 1}),
    )

    result = runner.invoke(
        app,
        ["event", "emit", "demo", "--payload-json", '{"topic": "alpha"}'],
    )

    assert result.exit_code == 0
    mock_emit.assert_called_once_with("demo", payload={"topic": "alpha"}, source="cli")
