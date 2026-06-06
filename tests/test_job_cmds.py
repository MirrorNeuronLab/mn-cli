from typer.testing import CliRunner
import json

from mn_cli.main import app

runner = CliRunner()


def test_submit_success(mocker, tmp_path):
    mock_submit = mocker.patch(
        "mn_cli.libs.job_cmds.client.submit_job", return_value="job-123"
    )

    manifest_file = tmp_path / "manifest.json"
    manifest_file.write_text('{"graph_id": "test"}')

    result = runner.invoke(app, ["job", "submit", str(manifest_file)])

    assert result.exit_code == 0
    assert "Job submit successful." in result.stdout
    assert "Job ID: job-123" in result.stdout
    mock_submit.assert_called_once_with('{"graph_id": "test"}', {})


def test_submit_error(mocker, tmp_path):
    mocker.patch(
        "mn_cli.libs.job_cmds.client.submit_job",
        side_effect=Exception("Failed API call"),
    )

    manifest_file = tmp_path / "manifest.json"
    manifest_file.write_text('{"graph_id": "test"}')

    result = runner.invoke(app, ["job", "submit", str(manifest_file)])

    assert result.exit_code == 0
    assert "Error submitting job: Failed API call" in result.stdout


def test_status_success(mocker):
    mock_get = mocker.patch(
        "mn_cli.libs.job_cmds.client.get_job",
        return_value=json.dumps(
            {
                "status": "running",
                "restart_policy": {"attempts": 3},
                "reschedule_policy": {"unlimited": True},
                "policy_state": {"agents": {"worker": {"next_action": "restart"}}},
            }
        ),
    )

    result = runner.invoke(app, ["job", "status", "job-123"])

    assert result.exit_code == 0
    assert "running" in result.stdout
    assert "restart_policy" in result.stdout
    assert "policy_state" in result.stdout
    assert "next_action" in result.stdout
    mock_get.assert_called_once_with("job-123")


def test_status_error(mocker):
    mocker.patch(
        "mn_cli.libs.job_cmds.client.get_job", side_effect=Exception("Job not found")
    )
    result = runner.invoke(app, ["job", "status", "job-123"])
    assert result.exit_code == 0
    assert "Error fetching job status: Job not found" in result.stdout


def test_list_jobs_success(mocker):
    mock_list = mocker.patch(
        "mn_cli.libs.job_cmds.client.list_jobs",
        return_value=json.dumps(
            {
                "data": [
                    {
                        "job_id": "job-1",
                        "graph_id": "g-1",
                        "status": "completed",
                        "submitted_at": "2023-10-01",
                        "recovery_status": "normal",
                    }
                ]
            }
        ),
    )
    result = runner.invoke(app, ["job", "list"])
    assert result.exit_code == 0
    assert "job-1" in result.stdout
    assert "normal" in result.stdout
    mock_list.assert_called_once()


def test_list_jobs_running_only_success(mocker):
    mock_list = mocker.patch(
        "mn_cli.libs.job_cmds.client.list_jobs",
        return_value=json.dumps(
            {
                "data": [
                    {
                        "job_id": "job-1",
                        "graph_id": "g-1",
                        "status": "completed",
                        "submitted_at": "2023-10-01",
                    },
                    {
                        "job_id": "job-2",
                        "graph_id": "g-2",
                        "status": "running",
                        "submitted_at": "2023-10-01",
                    },
                ]
            }
        ),
    )
    result = runner.invoke(app, ["job", "list", "--running-only"])
    assert result.exit_code == 0
    assert "job-2" in result.stdout
    assert "job-1" not in result.stdout
    mock_list.assert_called_once()


def test_list_jobs_error(mocker):
    mocker.patch(
        "mn_cli.libs.job_cmds.client.list_jobs", side_effect=Exception("Network error")
    )
    result = runner.invoke(app, ["job", "list"])
    assert result.exit_code == 0
    assert "Error listing jobs: Network error" in result.stdout


def test_clear_success(mocker):
    mock_clear = mocker.patch("mn_cli.libs.job_cmds.client.clear_jobs", return_value=5)
    result = runner.invoke(app, ["job", "clear"])
    assert result.exit_code == 0
    assert "Job clear successful." in result.stdout
    assert "Jobs cleared: 5 non-running" in result.stdout
    mock_clear.assert_called_once()


def test_clear_error(mocker):
    mocker.patch(
        "mn_cli.libs.job_cmds.client.clear_jobs", side_effect=Exception("DB Error")
    )
    result = runner.invoke(app, ["job", "clear"])
    assert result.exit_code == 0
    assert "Error clearing jobs: DB Error" in result.stdout


def test_cancel_success(mocker):
    mock_cancel = mocker.patch(
        "mn_cli.libs.job_cmds.client.cancel_job", return_value="cancelled"
    )
    mock_cleanup = mocker.patch("mn_cli.libs.job_cmds._cleanup_cancelled_job_web_ui")
    result = runner.invoke(app, ["job", "cancel", "job-123"])
    assert result.exit_code == 0
    assert "Job cancel successful." in result.stdout
    assert "Status: cancelled" in result.stdout
    mock_cancel.assert_called_once_with("job-123")
    mock_cleanup.assert_called_once_with("job-123")


def test_cancel_error(mocker):
    mocker.patch(
        "mn_cli.libs.job_cmds.client.cancel_job", side_effect=Exception("Fail")
    )
    mock_cleanup = mocker.patch("mn_cli.libs.job_cmds._cleanup_cancelled_job_web_ui")
    result = runner.invoke(app, ["job", "cancel", "job-123"])
    assert result.exit_code == 0
    assert "Error cancelling job: Fail" in result.stdout
    mock_cleanup.assert_called_once_with("job-123")


def test_cancel_cleans_blueprint_web_ui_process(mocker, tmp_path, monkeypatch):
    monkeypatch.setenv("MN_RUNS_ROOT", str(tmp_path))
    run_dir = tmp_path / "run-123"
    run_dir.mkdir()
    mocker.patch(
        "mn_cli.libs.job_cmds._blueprint_run_id_for_job", return_value="run-123"
    )
    mock_cleanup = mocker.patch("mn_cli.libs.job_cmds.cleanup_web_ui_process")

    mocker.patch("mn_cli.libs.job_cmds.client.cancel_job", return_value="cancelled")
    result = runner.invoke(app, ["job", "cancel", "job-123"])

    assert result.exit_code == 0
    mock_cleanup.assert_called_once()
    _, kwargs = mock_cleanup.call_args
    assert kwargs["dry_run"] is False
    assert kwargs["reason"] == "job_cancelled"


def test_pause_success(mocker):
    mock_pause = mocker.patch(
        "mn_cli.libs.job_cmds.client.pause_job", return_value="paused"
    )
    result = runner.invoke(app, ["job", "pause", "job-123"])
    assert result.exit_code == 0
    assert "Job pause successful." in result.stdout
    assert "Status: paused" in result.stdout
    mock_pause.assert_called_once_with("job-123")


def test_pause_error(mocker):
    mocker.patch("mn_cli.libs.job_cmds.client.pause_job", side_effect=Exception("Fail"))
    result = runner.invoke(app, ["job", "pause", "job-123"])
    assert result.exit_code == 0
    assert "Error pausing job: Fail" in result.stdout


def test_resume_success(mocker):
    mock_resume = mocker.patch(
        "mn_cli.libs.job_cmds.client.resume_job", return_value="running"
    )
    result = runner.invoke(app, ["job", "resume", "job-123"])
    assert result.exit_code == 0
    assert "Job resume successful." in result.stdout
    assert "Status: running" in result.stdout
    mock_resume.assert_called_once_with("job-123")


def test_resume_error(mocker):
    mocker.patch(
        "mn_cli.libs.job_cmds.client.resume_job", side_effect=Exception("Fail")
    )
    result = runner.invoke(app, ["job", "resume", "job-123"])
    assert result.exit_code == 0
    assert "Error resuming job: Fail" in result.stdout


def test_unfinished_jobs_shows_recovery_review_state(mocker):
    mock_list = mocker.patch(
        "mn_cli.libs.job_cmds.client.list_jobs",
        return_value=json.dumps(
            {
                "data": [
                    {
                        "job_id": "job-review",
                        "graph_id": "g-review",
                        "status": "paused",
                        "updated_at": "2026-05-05T00:00:00Z",
                        "recovery_status": "paused_for_review",
                        "recovery_requires_review": True,
                    }
                ]
            }
        ),
    )

    result = runner.invoke(app, ["job", "unfinished"])

    assert result.exit_code == 0
    assert "job-review" in result.stdout
    assert "paused_for_review" in result.stdout
    assert "yes" in result.stdout
    assert "mn job resume <job_id>" in result.stdout
    mock_list.assert_called_once_with(include_terminal=False)


def test_unfinished_jobs_empty(mocker):
    mocker.patch(
        "mn_cli.libs.job_cmds.client.list_jobs", return_value=json.dumps({"data": []})
    )

    result = runner.invoke(app, ["job", "unfinished"])

    assert result.exit_code == 0
    assert "Unfinished job check confirmed." in result.stdout
    assert "Status: none found" in result.stdout


def test_unfinished_jobs_accepts_nested_recovery_summary(mocker):
    mocker.patch(
        "mn_cli.libs.job_cmds.client.list_jobs",
        return_value=json.dumps(
            {
                "data": [
                    {
                        "job_id": "job-nested",
                        "graph_id": "g-nested",
                        "status": "paused",
                        "submitted_at": "2026-05-05T00:00:00Z",
                        "recovery": {
                            "status": "paused_for_review",
                            "requires_review": True,
                        },
                    }
                ]
            }
        ),
    )

    result = runner.invoke(app, ["job", "unfinished"])

    assert result.exit_code == 0
    assert "job-nested" in result.stdout
    assert "paused_for_review" in result.stdout
    assert "review=yes" in result.stdout


def test_nodes_success(mocker):
    mock_nodes = mocker.patch(
        "mn_cli.libs.job_cmds.client.get_system_summary",
        return_value='{"nodes": ["node1"]}',
    )
    result = runner.invoke(app, ["node", "list"])
    assert result.exit_code == 0
    assert "node1" in result.stdout
    mock_nodes.assert_called_once()


def test_nodes_error(mocker):
    mocker.patch(
        "mn_cli.libs.job_cmds.client.get_system_summary", side_effect=Exception("Fail")
    )
    result = runner.invoke(app, ["node", "list"])
    assert result.exit_code == 0
    assert "Error fetching nodes: Fail" in result.stdout


def test_reconcile_node_success(mocker):
    mock_reconcile = mocker.patch(
        "mn_cli.libs.job_cmds.client.reconcile_node",
        return_value=json.dumps({"checked": 1, "recovered": 1}),
    )
    result = runner.invoke(
        app, ["node", "reconcile", "node@lab", "--reason", "test", "--dry-run"]
    )
    assert result.exit_code == 0
    assert "Node reconcile successful." in result.stdout
    assert "Node: node@lab" in result.stdout
    assert "Dry run: True" in result.stdout
    mock_reconcile.assert_called_once_with("node@lab", reason="test", dry_run=True)


def test_reconcile_node_error(mocker):
    mocker.patch(
        "mn_cli.libs.job_cmds.client.reconcile_node", side_effect=Exception("Fail")
    )
    result = runner.invoke(app, ["node", "reconcile", "node@lab"])
    assert result.exit_code == 0
    assert "Error reconciling node: Fail" in result.stdout


def test_drain_node_success(mocker):
    mock_drain = mocker.patch(
        "mn_cli.libs.job_cmds.client.drain_node",
        return_value=json.dumps({"node": "node@lab", "status": "dry_run"}),
    )
    result = runner.invoke(
        app,
        [
            "node",
            "drain",
            "node@lab",
            "--reason",
            "update",
            "--deadline",
            "10s",
            "--dry-run",
        ],
    )
    assert result.exit_code == 0
    assert "Node drain successful." in result.stdout
    assert "Status: dry_run" in result.stdout
    mock_drain.assert_called_once_with(
        "node@lab",
        reason="update",
        deadline_ms=10_000,
        dry_run=True,
        ignore_system_jobs=True,
        wait=False,
    )


def test_drain_node_can_include_system_jobs(mocker):
    mock_drain = mocker.patch(
        "mn_cli.libs.job_cmds.client.drain_node",
        return_value=json.dumps({"node": "node@lab", "status": "complete"}),
    )
    result = runner.invoke(
        app,
        ["node", "drain", "node@lab", "--deadline", "2m", "--include-system-jobs"],
    )
    assert result.exit_code == 0
    assert "Status: complete" in result.stdout
    mock_drain.assert_called_once_with(
        "node@lab",
        reason="",
        deadline_ms=120_000,
        dry_run=False,
        ignore_system_jobs=False,
        wait=False,
    )


def test_drain_node_invalid_deadline_reports_error_without_backend_call(mocker):
    mock_drain = mocker.patch("mn_cli.libs.job_cmds.client.drain_node")

    invalid = runner.invoke(app, ["node", "drain", "node@lab", "--deadline", "not-a-duration"])
    negative = runner.invoke(app, ["node", "drain", "node@lab", "--deadline", "-1s"])

    assert invalid.exit_code == 0
    assert "Error draining node: deadline must be a duration like 30m, 10s, 1h, or 5000ms" in invalid.stdout
    assert negative.exit_code == 0
    assert "Error draining node: deadline must be non-negative" in negative.stdout
    mock_drain.assert_not_called()


def test_drain_node_wait_polls_status(mocker):
    mock_drain = mocker.patch(
        "mn_cli.libs.job_cmds.client.drain_node",
        return_value=json.dumps({"node": "node@lab", "status": "draining"}),
    )
    mock_status = mocker.patch(
        "mn_cli.libs.job_cmds.client.get_node_drain_status",
        return_value=json.dumps(
            {
                "node": "node@lab",
                "status": "maintenance",
                "scheduling_eligible": False,
                "drain": {"status": "complete"},
            }
        ),
    )
    mocker.patch("time.sleep")

    result = runner.invoke(app, ["node", "drain", "node@lab", "--wait"])
    assert result.exit_code == 0
    assert "Status: complete" in result.stdout
    mock_drain.assert_called_once_with(
        "node@lab",
        reason="",
        deadline_ms=1_800_000,
        dry_run=False,
        ignore_system_jobs=True,
        wait=True,
    )
    mock_status.assert_called_once_with("node@lab")


def test_drain_node_wait_stops_on_paused_for_review(mocker):
    mock_drain = mocker.patch(
        "mn_cli.libs.job_cmds.client.drain_node",
        return_value=json.dumps(
            {
                "node": "node@lab",
                "status": "paused_for_review",
                "reason": "manual recovery required",
            }
        ),
    )
    mock_status = mocker.patch("mn_cli.libs.job_cmds.client.get_node_drain_status")
    mock_sleep = mocker.patch("time.sleep")

    result = runner.invoke(app, ["node", "drain", "node@lab", "--wait"])

    assert result.exit_code == 0
    assert "Status: paused_for_review" in result.stdout
    mock_drain.assert_called_once_with(
        "node@lab",
        reason="",
        deadline_ms=1_800_000,
        dry_run=False,
        ignore_system_jobs=True,
        wait=True,
    )
    mock_status.assert_not_called()
    mock_sleep.assert_not_called()


def test_undrain_node_success(mocker):
    mock_undrain = mocker.patch(
        "mn_cli.libs.job_cmds.client.cancel_node_drain",
        return_value=json.dumps({"node": "node@lab", "scheduling_eligible": True}),
    )
    result = runner.invoke(app, ["node", "undrain", "node@lab", "--mark-eligible"])
    assert result.exit_code == 0
    assert "Node undrain successful." in result.stdout
    assert "Node: node@lab" in result.stdout
    mock_undrain.assert_called_once_with("node@lab", reason="", mark_eligible=True)


def test_undrain_node_defaults_to_not_mark_eligible(mocker):
    mock_undrain = mocker.patch(
        "mn_cli.libs.job_cmds.client.cancel_node_drain",
        return_value=json.dumps({"node": "node@lab", "status": "maintenance"}),
    )
    result = runner.invoke(app, ["node", "undrain", "node@lab"])
    assert result.exit_code == 0
    assert "Status: maintenance" in result.stdout
    mock_undrain.assert_called_once_with("node@lab", reason="", mark_eligible=False)


def test_maintenance_node_success(mocker):
    mock_maintenance = mocker.patch(
        "mn_cli.libs.job_cmds.client.set_node_maintenance",
        return_value=json.dumps({"node": "node@lab", "status": "maintenance"}),
    )
    result = runner.invoke(
        app, ["node", "maintenance", "node@lab", "--enable", "--reason", "patch"]
    )
    assert result.exit_code == 0
    assert "Status: maintenance" in result.stdout
    mock_maintenance.assert_called_once_with("node@lab", True, reason="patch")


def test_maintenance_node_can_disable(mocker):
    mock_maintenance = mocker.patch(
        "mn_cli.libs.job_cmds.client.set_node_maintenance",
        return_value=json.dumps({"node": "node@lab", "status": "healthy"}),
    )
    result = runner.invoke(
        app, ["node", "maintenance", "node@lab", "--disable", "--reason", "done"]
    )
    assert result.exit_code == 0
    assert "Status: healthy" in result.stdout
    mock_maintenance.assert_called_once_with("node@lab", False, reason="done")


def test_metrics_success(mocker):
    mocker.patch(
        "mn_cli.libs.job_cmds.client.get_system_summary",
        return_value=json.dumps({"nodes": ["n1"], "jobs": [{"status": "running"}]}),
    )
    result = runner.invoke(app, ["runtime", "metrics"])
    assert result.exit_code == 0
    assert '"running": 1' in result.stdout


def test_resource_list_success(mocker):
    mock_resource = mocker.patch(
        "mn_cli.libs.resource_cmds.client.get_resource",
        return_value=json.dumps(
            {
                "mode": "cluster",
                "node_count": 2,
                "nodes": [
                    {
                        "name": "mn1",
                        "cpu_cores": 8,
                        "cpu_model": "AMD Ryzen AI Max+ 395",
                        "gpu_count": 2,
                        "gpu_model": "NVIDIA RTX 4090",
                        "gpu_models": ["NVIDIA RTX 4090", "NVIDIA RTX 6000 Ada"],
                        "gpu_memory_total_mb": 48_000,
                        "gpu_memory_free_mb": 32_000,
                        "memory_gb": 16.0,
                    },
                    {"name": "mn2", "cpu_cores": 4, "gpu_count": 0, "memory_gb": 8.0},
                ],
                "limits": {"cpu": 100},
            }
        ),
        create=True,
    )
    result = runner.invoke(app, ["resource", "list"])
    assert result.exit_code == 0
    assert '"combined"' in result.stdout
    assert '"cpu_cores": 12' in result.stdout
    assert '"gpu_count": 2' in result.stdout
    assert '"gpu_memory_total_mb": 48000.0' in result.stdout
    assert '"gpu_memory_total_gb": 46.88' in result.stdout
    assert '"memory_gb": 24.0' in result.stdout
    assert '"memory_total_gb": 24.0' in result.stdout
    assert '"memory_available_gb": 0.0' in result.stdout
    assert '"name": "mn1"' in result.stdout
    assert '"cpu_model": "AMD Ryzen AI Max+ 395"' in result.stdout
    assert '"gpu_models": [' in result.stdout
    assert '"NVIDIA RTX 4090"' in result.stdout
    assert '"native_ports"' in result.stdout
    mock_resource.assert_called_once()


def test_resource_set_success(mocker):
    mock_set = mocker.patch(
        "mn_cli.libs.resource_cmds.client.set_resource",
        return_value=json.dumps(
            {
                "limits": {"cpu": 50, "gpu": 75},
                "totals": {
                    "cpu_cores": 8,
                    "gpu_count": 1,
                    "gpu_memory_total_mb": 24_576,
                    "gpu_memory_free_mb": 20_480,
                    "memory_gb": 32.0,
                },
            }
        ),
        create=True,
    )
    result = runner.invoke(app, ["resource", "set", "--cpu", "50", "--gpu", "75"])
    assert result.exit_code == 0
    assert "Resource set successful." in result.stdout
    assert "CPU: 50" in result.stdout
    assert "GPU: 75" in result.stdout
    assert '"gpu_memory_total_gb": 24.0' in result.stdout
    assert '"memory_total_gb": 32.0' in result.stdout
    mock_set.assert_called_once_with({"cpu": 50, "gpu": 75})


def test_dead_letters_success(mocker):
    mocker.patch(
        "mn_cli.libs.job_cmds.client.stream_events",
        return_value=[
            json.dumps({"type": "agent_started"}),
            json.dumps(
                {"type": "dead_letter", "agent_id": "a1", "reason": "queue full"}
            ),
        ],
    )
    result = runner.invoke(app, ["job", "dead-letters", "job-1"])
    assert result.exit_code == 0
    assert "queue full" in result.stdout
