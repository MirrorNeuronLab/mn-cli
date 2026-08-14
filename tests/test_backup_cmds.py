import hashlib
import json
import subprocess
import zipfile

import typer
from typer.testing import CliRunner

from mn_cli.libs import backup_cmds

runner = CliRunner()
handler_app = typer.Typer()
handler_job_app = typer.Typer()
handler_app.add_typer(handler_job_app, name="job")
handler_job_app.command("backup")(backup_cmds.backup)
handler_job_app.command("restore")(backup_cmds.restore)


def test_backup_fails_when_job_is_not_paused(mocker, tmp_path):
    mocker.patch(
        "mn_cli.libs.backup_cmds.client.get_job",
        return_value=json.dumps({"job": {"job_id": "job-1", "status": "running"}}),
    )
    mock_export = mocker.patch("mn_cli.libs.backup_cmds.client.export_job_backup")

    result = runner.invoke(handler_app, ["job", "backup", "job-1", "--output", str(tmp_path)])

    assert result.exit_code == 1
    assert "must be paused before backup" in result.stderr
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
        "schema_version": "mn.backup.v2",
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

    result = runner.invoke(handler_app, ["job", "backup", "job-1", "--output", str(tmp_path)])

    assert result.exit_code == 0
    assert "may contain secrets" in result.stderr
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

    result = runner.invoke(handler_app, ["job", "restore", "bp", "--input", str(archive)])

    assert result.exit_code == 1
    assert "escapes the backup root" in result.stderr
    mock_restore.assert_not_called()


def test_restore_rejects_missing_required_archive_entries(mocker, tmp_path):
    archive = tmp_path / "incomplete.zip"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("mn-backup.json", b'{"schema_version":"mn.backup.v2"}')

    mock_restore = mocker.patch("mn_cli.libs.backup_cmds.client.restore_job_backup")

    result = runner.invoke(handler_app, ["job", "restore", "bp", "--input", str(archive)])

    assert result.exit_code == 1
    assert "Backup zip is missing required entries" in result.stderr
    assert "runtime/job.json" in result.stderr
    assert "bundle/manifest.json" in result.stderr
    mock_restore.assert_not_called()


def test_restore_rejects_checksum_mismatch(mocker, tmp_path):
    archive = tmp_path / "tampered.zip"
    entries = {
        "mn-backup.json": b'{"schema_version":"mn.backup.v2"}',
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

    result = runner.invoke(handler_app, ["job", "restore", "bp", "--input", str(archive)])

    assert result.exit_code == 1
    assert "Checksum mismatch for runtime/job.json" in result.stderr
    mock_restore.assert_not_called()


def test_restore_writes_new_run_mapping_and_prints_provenance(mocker, tmp_path, monkeypatch):
    monkeypatch.setenv("MN_RUNS_ROOT", str(tmp_path / "runs"))
    archive = tmp_path / "backup.zip"
    entries = {
        "mn-backup.json": json.dumps(
            {
                "schema_version": "mn.backup.v2",
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

    result = runner.invoke(handler_app, ["job", "restore", "bp", "--input", str(archive)])

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


def test_air_gapped_backup_streams_referenced_model_blob(mocker, tmp_path, monkeypatch):
    blob_root = tmp_path / "blobs"
    monkeypatch.setenv("MN_HOST_BLOB_STORE_DIR", str(blob_root))
    model_bytes = b"local-gguf-model"
    sha256 = hashlib.sha256(model_bytes).hexdigest()
    blob = blob_root / sha256[:2] / sha256
    blob.parent.mkdir(parents=True)
    blob.write_bytes(model_bytes)
    manifest = {
        "apiVersion": "mn.workflow/v2",
        "runtime": {
            "models": {
                "primary": {
                    "provider": "docker_model_runner",
                    "runtime_model": "demo/airgap:latest",
                    "source": {
                        "type": "payload",
                        "path": "models/demo.gguf",
                        "format": "gguf",
                    },
                }
            }
        },
        "metadata": {
            "mn_artifacts": {
                "blob_refs": [
                    {
                        "type": "blob_ref",
                        "payload_path": "models/demo.gguf",
                        "sha256": sha256,
                        "size_bytes": len(model_bytes),
                    }
                ]
            }
        },
    }
    job = {"job_id": "job-1", "status": "paused", "manifest": manifest}
    backup_payload = {
        "schema_version": "mn.backup.v2",
        "source": {"job_id": "job-1", "blueprint_id": "demo-airgap"},
        "runtime": {"job": job, "events": []},
    }
    mocker.patch(
        "mn_cli.libs.backup_cmds.client.get_job",
        return_value=json.dumps({"job": job}),
    )
    mocker.patch(
        "mn_cli.libs.backup_cmds.client.export_job_backup",
        return_value=(
            json.dumps(backup_payload),
            {"manifest.json": json.dumps(manifest).encode("utf-8")},
        ),
    )

    result = runner.invoke(
        handler_app,
        ["job", "backup", "job-1", "--output", str(tmp_path), "--air-gapped"],
    )

    assert result.exit_code == 0, result.stdout
    archives = list(tmp_path.glob("*.mn-airgap-backup.zip"))
    assert len(archives) == 1
    with zipfile.ZipFile(archives[0]) as zf:
        metadata = json.loads(zf.read("mn-backup.json"))
        assert metadata["schema_version"] == "mn.backup.v2"
        assert metadata["air_gap"]["network"] == "forbidden"
        assert zf.read(f"airgap/blobs/{sha256}") == model_bytes
        marker = json.loads(zf.read("bundle/.mn-airgap.json"))
        assert marker["schema_version"] == "mn.airgap.bundle.v1"


def test_airgap_wheelhouse_includes_payload_transitive_dependencies_and_hostlocal_packages(
    mocker,
    tmp_path,
):
    bundle = tmp_path / "bundle"
    package = bundle / "payloads" / "skills" / "demo"
    package.mkdir(parents=True)
    package.joinpath("pyproject.toml").write_text(
        "[project]\n"
        "name='demo-skill'\n"
        "version='1.0.0'\n"
        "dependencies=['charset-normalizer==3.4.0']\n",
        encoding="utf-8",
    )
    requirements = bundle / "payloads" / "worker" / "requirements.txt"
    requirements.parent.mkdir(parents=True)
    requirements.write_text("certifi==2026.1.1\n", encoding="utf-8")
    manifest = {
        "skill_dependencies": [
            {
                "type": "pip",
                "source": "payload",
                "name": "demo-skill",
                "version": "1.0.0",
                "path": "skills/demo",
                "format": "source",
            }
        ],
        "agents": {
            "nodes": [
                {
                    "node_id": "worker",
                    "config": {
                        "runner_module": "MirrorNeuron.Runner.HostLocal",
                        "python_environment": {
                            "requirements": "worker/requirements.txt",
                            "packages": [
                                "/original/payload/source",
                                "demo-skill==1.0.0",
                                "requests==2.32.0",
                            ],
                        },
                    },
                }
            ]
        },
    }
    run = mocker.patch(
        "mn_cli.libs.backup_cmds.subprocess.run",
        return_value=subprocess.CompletedProcess([], 0, stdout="", stderr=""),
    )

    backup_cmds._build_airgap_wheelhouse(
        manifest,
        bundle,
        tmp_path / "wheelhouse",
    )

    commands = [call.args[0] for call in run.call_args_list]
    source_command = next(command for command in commands if str(package) in command)
    assert "--no-deps" not in source_command
    assert "--index-url" in source_command
    assert any(
        "-r" in command and str(requirements) in command for command in commands
    )
    assert any("requests==2.32.0" in command for command in commands)
    assert not any("/original/payload/source" in command for command in commands)


def test_airgap_export_rejects_runtime_model_without_payload_source(tmp_path):
    manifest = {
        "runtime": {
            "models": {
                "primary": {
                    "provider": "docker_model_runner",
                    "runtime_model": "ai/catalog-model:latest",
                }
            }
        }
    }

    try:
        backup_cmds._prepare_airgap_export(
            {},
            {"manifest.json": json.dumps(manifest).encode("utf-8")},
            tmp_path,
        )
    except backup_cmds.BackupRestoreError as exc:
        assert "payloads/models" in str(exc)
        assert "primary (ai/catalog-model:latest)" in str(exc)
    else:
        raise AssertionError("Air-gapped export accepted a catalog-only model.")


def test_patch_hostlocal_wheelhouse_preserves_non_payload_requirements(tmp_path):
    wheelhouse = tmp_path / "wheelhouse"
    manifest = {
        "skill_dependencies": [
            {
                "name": "demo-skill",
                "version": "1.0.0",
                "source": "payload",
            }
        ],
        "agents": {
            "nodes": [
                {
                    "node_id": "worker",
                    "config": {
                        "runner_module": "MirrorNeuron.Runner.HostLocal",
                        "python_environment": {
                            "packages": [
                                "--index-url",
                                "https://packages.example.invalid/simple",
                                "/old/payload/source",
                                "demo-skill==1.0.0",
                                "requests==2.32.0",
                            ]
                        },
                    },
                }
            ]
        },
    }

    backup_cmds._patch_hostlocal_wheelhouse(manifest, wheelhouse)

    assert manifest["agents"]["nodes"][0]["config"]["python_environment"][
        "packages"
    ] == [
        "--no-index",
        "--find-links",
        str(wheelhouse),
        "demo-skill==1.0.0",
        "requests==2.32.0",
    ]
