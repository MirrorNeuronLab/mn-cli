import json

from typer.testing import CliRunner

from mn_cli.main import app

runner = CliRunner()


def _documents(output: str) -> list[dict]:
    return [json.loads(line) for line in output.splitlines() if line.strip()]


def test_run_result_resolves_runtime_job_and_uses_run_scoped_output(mocker, monkeypatch, tmp_path):
    monkeypatch.setenv("MN_HOME", str(tmp_path / "mn-home"))
    mocker.patch(
        "mn_cli.libs.run_public.client.get_run",
        return_value=json.dumps({"run_id": "run-1", "runtime_job_id": "runtime-9"}),
    )
    fetch = mocker.patch("mn_cli.libs.run_public.run_cmds.fetch_and_save_results")

    result = runner.invoke(app, ["run", "result", "run-1", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    destination = tmp_path / "mn-home" / "outputs" / "run-1"
    assert payload["data"] == {
        "run_id": "run-1",
        "runtime_job_id": "runtime-9",
        "output": str(destination),
    }
    fetch.assert_called_once_with(
        "runtime-9",
        data={"run_id": "run-1", "runtime_job_id": "runtime-9"},
        output_dir=destination,
    )


def test_run_watch_json_is_ndjson_and_resolves_runtime_job(mocker):
    mocker.patch(
        "mn_cli.libs.run_public.client.get_run",
        return_value=json.dumps({"run_id": "run-1", "runtime_job_id": "runtime-9"}),
    )
    get_job = mocker.patch(
        "mn_cli.libs.run_public.client.get_job",
        return_value=json.dumps({"job_id": "runtime-9", "status": "running"}),
    )
    mocker.patch(
        "mn_cli.libs.run_public.client.stream_events",
        return_value=[
            json.dumps({"type": "heartbeat"}),
            json.dumps({"type": "job_completed", "job_id": "runtime-9"}),
        ],
    )

    result = runner.invoke(app, ["run", "watch", "run-1", "--json"])

    assert result.exit_code == 0
    records = _documents(result.stdout)
    assert [record["type"] for record in records] == ["snapshot", "event", "complete"]
    assert all(record["schema"] == "mn.cli.stream/v1" for record in records)
    get_job.assert_called_once_with("runtime-9")


def test_run_logs_json_one_shot_uses_standard_envelope(mocker):
    mocker.patch("mn_cli.libs.run_public.blueprint_cmds._load_run_or_exit")
    mocker.patch(
        "mn_cli.libs.run_public.blueprint_cmds._load_observability_tools",
        return_value={
            "read_run_logs": lambda *_args, **_kwargs: [
                {"ts": "2026-08-14T00:00:00Z", "message": "ready"}
            ]
        },
    )

    result = runner.invoke(app, ["run", "logs", "run-1", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["schema"] == "mn.cli/v1"
    assert payload["data"]["channel"] == "logs"
    assert payload["data"]["count"] == 1


def test_run_list_for_job_accepts_v2_items_response(mocker):
    mocker.patch(
        "mn_cli.libs.run_public.client.list_runs",
        return_value=json.dumps(
            {
                "items": [
                    {
                        "run_id": "run-1",
                        "job_id": "stable-job",
                        "status": "running",
                        "updated_at": "2026-08-17T17:00:00Z",
                    }
                ]
            }
        ),
    )

    result = runner.invoke(app, ["run", "list", "--job", "stable-job", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["data"] == {
        "items": [
            {
                "run_id": "run-1",
                "job_id": "stable-job",
                "status": "running",
                "updated_at": "2026-08-17T17:00:00Z",
            }
        ],
        "count": 1,
    }
