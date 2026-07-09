import hashlib
import json
import zipfile

from typer.testing import CliRunner

from mn_cli.main import app

runner = CliRunner()


def test_backup_fails_when_job_is_not_paused(mocker, tmp_path):
    mocker.patch(
        "mn_cli.libs.backup_cmds.client.get_job",
        return_value=json.dumps({"job": {"job_id": "job-1", "status": "running"}}),
    )
    mock_export = mocker.patch("mn_cli.libs.backup_cmds.client.export_job_backup")

    result = runner.invoke(app, ["job", "backup", "job-1", "--output", str(tmp_path)])

    assert result.exit_code == 1
    assert "must be paused before backup" in result.stdout
    mock_export.assert_not_called()


def test_backup_writes_zip_members_and_secret_warning(mocker, tmp_path):
    job = {
        "job_id": "job-1",
        "status": "paused",
        "manifest": {
            "metadata": {
                "mn_cli": {
                    "blueprint_id": "bp",
                    "blueprint_run_id": "bp-run-1",
                }
            }
        },
    }
    backup_payload = {
        "schema_version": "mn.backup.v1",
        "created_at": "2026-05-27T00:00:00Z",
        "source": {"job_id": "job-1"},
        "target_policy": {"restore_mode": "clone"},
        "sections": {},
        "runtime": {
            "job": job,
            "agents": [{"agent_id": "worker", "parent_job_id": "job-1"}],
            "events": [{"type": "job_paused"}],
        },
    }
    mocker.patch(
        "mn_cli.libs.backup_cmds.client.get_job",
        return_value=json.dumps({"job": job}),
    )
    mocker.patch(
        "mn_cli.libs.backup_cmds.client.export_job_backup",
        return_value=(
            json.dumps(backup_payload),
            {"manifest.json": b'{"graph_id":"g"}', "payloads/code.py": b"print(1)"},
        ),
    )

    result = runner.invoke(app, ["job", "backup", "job-1", "--output", str(tmp_path)])

    assert result.exit_code == 0
    assert "may contain secrets" in result.stdout
    assert "Job backup successful." in result.stdout
    assert "job-1" in result.stdout
    archives = list(tmp_path.glob("*.mnbackup.zip"))
    assert len(archives) == 1
    with zipfile.ZipFile(archives[0]) as zf:
        names = set(zf.namelist())
        assert "mn-backup.json" in names
        assert "runtime/job.json" in names
        assert "runtime/agents.json" in names
        assert "runtime/events.jsonl" in names
        assert "bundle/manifest.json" in names
        assert "bundle/payloads/code.py" in names
        assert "checksums.json" in names
        metadata = json.loads(zf.read("mn-backup.json"))
        assert "runtime" not in metadata
        assert metadata["source"]["run_id"] == "bp-run-1"


def test_restore_rejects_path_traversal_zip(mocker, tmp_path):
    archive = tmp_path / "bad.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("../evil", b"nope")

    mock_restore = mocker.patch("mn_cli.libs.backup_cmds.client.restore_job_backup")

    result = runner.invoke(app, ["job", "restore", "bp", "--input", str(archive)])

    assert result.exit_code == 1
    assert "escapes the backup root" in result.stdout
    mock_restore.assert_not_called()


def test_restore_rejects_missing_required_archive_entries(mocker, tmp_path):
    archive = tmp_path / "incomplete.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("mn-backup.json", b'{"schema_version":"mn.backup.v1"}')

    mock_restore = mocker.patch("mn_cli.libs.backup_cmds.client.restore_job_backup")

    result = runner.invoke(app, ["job", "restore", "bp", "--input", str(archive)])

    assert result.exit_code == 1
    assert "Backup zip is missing required entries" in result.stdout
    assert "runtime/job.json" in result.stdout
    assert "bundle/manifest.json" in result.stdout
    mock_restore.assert_not_called()


def test_restore_rejects_checksum_mismatch(mocker, tmp_path):
    archive = tmp_path / "tampered.zip"
    entries = {
        "mn-backup.json": b'{"schema_version":"mn.backup.v1"}',
        "runtime/job.json": b'{"job_id":"old-job","status":"paused"}',
        "runtime/agents.json": b"[]",
        "runtime/events.jsonl": b'{"type":"job_paused"}\n',
        "bundle/manifest.json": b'{"graph_id":"g"}',
        "bundle/payloads/.mn-empty": b"",
    }
    checksums = {
        "algorithm": "sha256",
        "entries": {
            name: hashlib.sha256(contents).hexdigest()
            for name, contents in entries.items()
        },
    }
    tampered_entries = {**entries, "runtime/job.json": b'{"job_id":"tampered"}'}
    with zipfile.ZipFile(archive, "w") as zf:
        for name, contents in tampered_entries.items():
            zf.writestr(name, contents)
        zf.writestr("checksums.json", json.dumps(checksums).encode("utf-8"))

    mock_restore = mocker.patch("mn_cli.libs.backup_cmds.client.restore_job_backup")

    result = runner.invoke(app, ["job", "restore", "bp", "--input", str(archive)])

    assert result.exit_code == 1
    assert "Checksum mismatch for runtime/job.json" in result.stdout
    mock_restore.assert_not_called()


def test_restore_writes_new_run_mapping_and_prints_provenance(mocker, tmp_path, monkeypatch):
    monkeypatch.setenv("MN_RUNS_ROOT", str(tmp_path / "runs"))
    archive = tmp_path / "backup.zip"
    entries = {
        "mn-backup.json": json.dumps(
            {
                "schema_version": "mn.backup.v1",
                "created_at": "2026-05-27T00:00:00Z",
                "source": {"job_id": "old-job", "run_id": "old-run"},
                "target_policy": {"restore_mode": "clone"},
                "sections": {},
            },
            sort_keys=True,
        ).encode("utf-8"),
        "runtime/job.json": b'{"job_id":"old-job","status":"paused"}',
        "runtime/agents.json": b"[]",
        "runtime/events.jsonl": b'{"type":"job_paused"}\n',
        "bundle/manifest.json": b'{"graph_id":"g"}',
        "bundle/payloads/.mn-empty": b"",
    }
    checksums = {
        "algorithm": "sha256",
        "entries": {
            name: hashlib.sha256(contents).hexdigest()
            for name, contents in entries.items()
        },
    }
    with zipfile.ZipFile(archive, "w") as zf:
        for name, contents in entries.items():
            zf.writestr(name, contents)
        zf.writestr("checksums.json", json.dumps(checksums).encode("utf-8"))

    mocker.patch("mn_cli.libs.backup_cmds.make_blueprint_run_id", return_value="bp-run-new")
    mock_restore = mocker.patch(
        "mn_cli.libs.backup_cmds.client.restore_job_backup",
        return_value=json.dumps(
            {
                "job_id": "new-job",
                "run_id": "bp-run-new",
                "source_job_id": "old-job",
                "source_run_id": "old-run",
                "restore_provenance": {
                    "source": {"job_id": "old-job"},
                    "target": {"job_id": "new-job"},
                },
            }
        ),
    )

    result = runner.invoke(app, ["job", "restore", "bp", "--input", str(archive)])

    assert result.exit_code == 0
    assert "Job restore successful." in result.stdout
    assert "paused" in result.stdout
    assert "new-job" in result.stdout
    assert "old-job" in result.stdout
    _, restore_kwargs = mock_restore.call_args
    assert restore_kwargs["blueprint_id"] == "bp"
    assert restore_kwargs["run_id"] == "bp-run-new"
    job_mapping = json.loads((tmp_path / "runs" / "bp-run-new" / "job.json").read_text())
    assert job_mapping["job_id"] == "new-job"
    assert job_mapping["source_job_id"] == "old-job"
