import json

from typer.testing import CliRunner

from mn_cli.main import app


runner = CliRunner()


def _bundle(tmp_path):
    bundle = tmp_path / "bundle"
    payloads = bundle / "payloads"
    payloads.mkdir(parents=True)
    (bundle / "manifest.json").write_text(
        '{"apiVersion":"mn.workflow/v2","kind":"Workflow","graph_id":"scheduled"}',
        encoding="utf-8",
    )
    (payloads / "input.json").write_bytes(b"{}")
    return bundle


def test_schedule_add_passes_cron_policy_and_payloads(mocker, tmp_path):
    mock_create = mocker.patch(
        "mn_cli.libs.schedule_cmds.client.create_schedule",
        return_value=json.dumps({"schedule_id": "sched-1", "kind": "periodic"}),
    )

    result = runner.invoke(
        app,
        ["schedule", "add", str(_bundle(tmp_path)), "--cron", "0 2 * * *", "--window", "30m"],
    )

    assert result.exit_code == 0
    assert "Schedule add successful." in result.stdout
    assert "sched-1" in result.stdout
    args, kwargs = mock_create.call_args
    assert json.loads(args[0])["graph_id"] == "scheduled"
    assert args[1] == {"input.json": b"{}"}
    assert kwargs["schedule"]["crons"] == ["0 2 * * *"]
    assert kwargs["schedule"]["window"]["duration_ms"] == 1_800_000


def test_schedule_add_requires_exactly_one_mode_without_side_effects(mocker, tmp_path):
    mock_create = mocker.patch(
        "mn_cli.libs.schedule_cmds.client.create_schedule",
        return_value=json.dumps({"schedule_id": "sched-1", "kind": "periodic"}),
    )
    bundle = _bundle(tmp_path)

    with_cron = runner.invoke(app, ["schedule", "add", str(bundle), "--cron", "0 2 * * *"])
    without_mode = runner.invoke(app, ["schedule", "add", str(bundle), "--json"])

    assert with_cron.exit_code == 0
    assert without_mode.exit_code == 2
    assert mock_create.call_args_list[0].kwargs["schedule"]["crons"] == ["0 2 * * *"]
    assert mock_create.call_count == 1
    assert json.loads(without_mode.stdout)["error"]["code"] == "MN_USAGE_ERROR"


def test_schedule_add_builds_event_schedule(mocker, tmp_path):
    mock_create = mocker.patch(
        "mn_cli.libs.schedule_cmds.client.create_schedule",
        return_value=json.dumps({"schedule_id": "sched-event"}),
    )

    result = runner.invoke(
        app,
        [
            "schedule",
            "add",
            str(_bundle(tmp_path)),
            "--event",
            "file_uploaded",
            "--filter",
            '{"path": {"prefix": "datasets/"}}',
        ],
    )

    assert result.exit_code == 0
    assert "Schedule add successful." in result.stdout
    assert "sched-event" in result.stdout
    schedule = mock_create.call_args.kwargs["schedule"]
    assert schedule["kind"] == "event"
    assert schedule["trigger"]["event_type"] == "file_uploaded"
    assert schedule["trigger"]["filters"]["path"]["prefix"] == "datasets/"


def test_schedule_add_rejects_multiple_modes_atomically(mocker, tmp_path):
    create = mocker.patch("mn_cli.libs.schedule_cmds.client.create_schedule")

    result = runner.invoke(
        app,
        [
            "schedule",
            "add",
            str(_bundle(tmp_path)),
            "--cron",
            "0 2 * * *",
            "--event",
            "file_uploaded",
            "--json",
        ],
    )

    assert result.exit_code == 2
    create.assert_not_called()


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
    assert "Event emit successful." in result.stdout
    assert "demo" in result.stdout
    mock_emit.assert_called_once_with("demo", payload={"topic": "alpha"}, source="cli")
