from unittest.mock import Mock
from typer.testing import CliRunner
from mn_cli.main import app
from mn_cli.libs import job_backup_cmds
from mn_sdk.job_backup import BackupRestoreError


def test_backup_and_restore_have_json_results_and_need_no_blueprint(monkeypatch, tmp_path):
    backup = Mock(return_value={"job_id": "job-1", "path": str(tmp_path / "backup.zip"), "air_gapped": True})
    restore = Mock(return_value={"job_id": "job-new", "run_id": "run-new", "started": True})
    monkeypatch.setattr(job_backup_cmds, "backup_job", backup)
    monkeypatch.setattr(job_backup_cmds, "restore_job", restore)
    runner = CliRunner()
    result = runner.invoke(app, ["job", "backup", "job-1", "--output", str(tmp_path / "backup.zip"), "--json"])
    assert result.exit_code == 0, result.output
    assert '"air_gapped": true' in result.output
    assert backup.call_args.kwargs == {"air_gapped": True}
    result = runner.invoke(app, ["job", "restore", "--input", str(tmp_path / "backup.zip"), "--start", "--json"])
    assert result.exit_code == 0, result.output
    assert '"job_id": "job-new"' in result.output
    assert restore.call_args.kwargs == {"job_id": None, "start": True}


def test_backup_reports_dependency_failure_and_help_names_the_zip(monkeypatch):
    monkeypatch.setattr(job_backup_cmds, "backup_job", Mock(side_effect=BackupRestoreError("Required model asset is missing")))
    runner = CliRunner()
    result = runner.invoke(app, ["job", "backup", "job-1", "--output", "backup.zip", "--json"])
    assert result.exit_code != 0
    assert '"ok": false' in result.output
    assert "model asset is missing" in result.output
    for command in ("backup", "restore"):
        result = runner.invoke(app, ["job", command, "--help"])
        assert result.exit_code == 0
        assert "ZIP" in result.output
