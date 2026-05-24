import json
import subprocess

from mn_cli.libs.blueprint_resources import cleanup_blueprint_host_hooks, cleanup_blueprint_resources


def _completed(command, stdout="", returncode=0, stderr=""):
    return subprocess.CompletedProcess(command, returncode, stdout=stdout, stderr=stderr)


def _write_run_record(path, blueprint_id):
    path.mkdir(parents=True, exist_ok=True)
    (path / "run.json").write_text(json.dumps({"run_id": path.name, "blueprint_id": blueprint_id}))


def _write_generated_bundle(path, blueprint_id):
    path.mkdir(parents=True, exist_ok=True)
    (path / "manifest.json").write_text(json.dumps({"metadata": {"blueprint_id": blueprint_id}}))


def _write_bundle_cache(path, blueprint_id):
    _write_generated_bundle(path, blueprint_id)


def test_cleanup_reclaims_blueprint_files_from_mn_home(mocker, tmp_path, monkeypatch):
    mocker.patch("mn_cli.libs.blueprint_resources.shutil.which", return_value=None)
    monkeypatch.setenv("MN_BLUEPRINT_RESOURCE_STALE_SECONDS", "0")
    runs_root = tmp_path / "runs"
    generated_root = tmp_path / "generated_blueprint_bundles"
    bundle_cache_root = tmp_path / "bundle_cache"

    active_run = runs_root / "bp-active-run"
    removed_run = runs_root / "bp-removed-run"
    incomplete_run = runs_root / "incomplete-run"
    active_generated = generated_root / "bp-active-run"
    removed_generated = generated_root / "bp-removed-run"
    orphan_generated = generated_root / "orphan-generated"
    active_bundle_cache = bundle_cache_root / "active-fingerprint"
    removed_bundle_cache = bundle_cache_root / "removed-fingerprint"
    incomplete_bundle_cache = bundle_cache_root / "incomplete-fingerprint"
    _write_run_record(active_run, "bp-active")
    _write_run_record(removed_run, "bp-removed")
    incomplete_run.mkdir(parents=True)
    _write_generated_bundle(active_generated, "bp-active")
    _write_generated_bundle(removed_generated, "bp-removed")
    orphan_generated.mkdir(parents=True)
    _write_bundle_cache(active_bundle_cache, "bp-active")
    _write_bundle_cache(removed_bundle_cache, "bp-removed")
    incomplete_bundle_cache.mkdir(parents=True)

    summary = cleanup_blueprint_resources(
        active_blueprint_ids={"bp-active"},
        python_envs_dir=tmp_path / "missing-envs",
        runs_root=runs_root,
        generated_bundles_dir=generated_root,
        bundle_cache_dir=bundle_cache_root,
        include_docker=False,
    )

    assert active_run.exists()
    assert active_generated.exists()
    assert active_bundle_cache.exists()
    assert not removed_run.exists()
    assert not incomplete_run.exists()
    assert not removed_generated.exists()
    assert not orphan_generated.exists()
    assert not removed_bundle_cache.exists()
    assert not incomplete_bundle_cache.exists()
    assert {item["reason"] for item in summary["run_removed"]} == {
        "dead_blueprint_run_record",
        "incomplete_untracked_run_record",
    }
    assert {item["reason"] for item in summary["generated_removed"]} == {
        "removed_run_generated_bundle",
        "stale_generated_bundle_without_run",
    }
    assert {item["reason"] for item in summary["bundle_removed"]} == {
        "dead_blueprint_bundle_cache",
        "incomplete_untracked_bundle_cache",
    }


def test_cleanup_removes_legacy_run_records_by_blueprint_prefix(mocker, tmp_path):
    mocker.patch("mn_cli.libs.blueprint_resources.shutil.which", return_value=None)
    runs_root = tmp_path / "runs"
    generated_root = tmp_path / "generated_blueprint_bundles"
    legacy_run = runs_root / "bp-old-20260513T000000Z-abc123"
    legacy_generated = generated_root / legacy_run.name
    legacy_run.mkdir(parents=True)
    legacy_generated.mkdir(parents=True)

    summary = cleanup_blueprint_resources(
        blueprint_ids={"bp-old"},
        python_envs_dir=tmp_path / "missing-envs",
        runs_root=runs_root,
        generated_bundles_dir=generated_root,
        include_docker=False,
    )

    assert not legacy_run.exists()
    assert not legacy_generated.exists()
    assert summary["run_removed"][0]["reason"] == "blueprint_removed_run_record"
    assert summary["generated_removed"][0]["reason"] == "removed_run_generated_bundle"


def test_cleanup_stops_blueprint_web_ui_process_with_run_record(mocker, tmp_path):
    mocker.patch("mn_cli.libs.blueprint_resources.shutil.which", return_value=None)
    mock_killpg = mocker.patch("mn_cli.libs.blueprint_resources.os.killpg")
    mocker.patch("mn_cli.libs.blueprint_resources.process_is_running", side_effect=[True, False, False])
    runs_root = tmp_path / "runs"
    run_dir = runs_root / "bp-removed-run"
    _write_run_record(run_dir, "bp-removed")
    (run_dir / "web_ui_process.json").write_text(json.dumps({"pid": 4242, "blueprint_id": "bp-removed"}))

    summary = cleanup_blueprint_resources(
        blueprint_ids={"bp-removed"},
        python_envs_dir=tmp_path / "missing-envs",
        runs_root=runs_root,
        include_docker=False,
    )

    assert not run_dir.exists()
    mock_killpg.assert_called_once_with(4242, 15)
    assert summary["process_removed"][0]["pid"] == 4242


def test_cleanup_stops_blueprint_pre_launch_process_with_run_record(mocker, tmp_path):
    mocker.patch("mn_cli.libs.blueprint_resources.shutil.which", return_value=None)
    mock_killpg = mocker.patch("mn_cli.libs.blueprint_resources.os.killpg")
    mocker.patch("mn_cli.libs.blueprint_resources.process_is_running", side_effect=[True, False, False])
    runs_root = tmp_path / "runs"
    run_dir = runs_root / "bp-removed-run"
    _write_run_record(run_dir, "bp-removed")
    (run_dir / "pre_launch_process.json").write_text(json.dumps({"pid": 5252, "blueprint_id": "bp-removed"}))

    summary = cleanup_blueprint_resources(
        blueprint_ids={"bp-removed"},
        python_envs_dir=tmp_path / "missing-envs",
        runs_root=runs_root,
        include_docker=False,
    )

    assert not run_dir.exists()
    mock_killpg.assert_called_once_with(5252, 15)
    assert summary["process_removed"][0]["pid"] == 5252


def test_cleanup_runs_blueprint_post_launch_hook_with_ready_env(mocker, tmp_path):
    run_dir = tmp_path / "runs" / "video-run"
    run_dir.mkdir(parents=True)
    script = tmp_path / "bundle" / "scripts" / "post-launch.sh"
    script.parent.mkdir(parents=True)
    script.write_text("#!/usr/bin/env bash\n")
    (run_dir / "pre_launch.ready").write_text(json.dumps({
        "status": "ready",
        "env": {
            "RTSP_PORT": "8567",
            "WEBRTC_PORT": "8891",
        },
    }))
    (run_dir / "post_launch_hook.json").write_text(json.dumps({
        "command": ["bash", str(script)],
        "script": str(script),
        "cwd": str(script.parent.parent),
        "log": str(run_dir / "post_launch.log"),
        "run_id": "video-run",
        "bundle_dir": str(script.parent.parent),
        "state_file": str(run_dir / "post_launch_state.json"),
        "pre_launch_ready_file": str(run_dir / "pre_launch.ready"),
        "pre_launch_process_file": str(run_dir / "pre_launch_process.json"),
    }))
    mock_run = mocker.patch(
        "mn_cli.libs.blueprint_resources.subprocess.run",
        return_value=subprocess.CompletedProcess(["bash", str(script)], 0),
    )

    summary = {"process_removed": [], "process_skipped": [], "errors": []}
    cleanup_blueprint_host_hooks(run_dir, dry_run=False, summary=summary, reason="job_cancelled")

    mock_run.assert_called_once()
    assert mock_run.call_args.args[0] == ["bash", str(script)]
    assert mock_run.call_args.kwargs["env"]["MN_POST_LAUNCH_REASON"] == "job_cancelled"
    assert mock_run.call_args.kwargs["env"]["RTSP_PORT"] == "8567"
    assert mock_run.call_args.kwargs["env"]["WEBRTC_PORT"] == "8891"
    assert summary["process_removed"][0]["script"] == str(script)


def test_cleanup_removes_dead_labeled_and_named_docker_resources(mocker, tmp_path):
    mocker.patch("mn_cli.libs.blueprint_resources.shutil.which", return_value="/usr/local/bin/docker")
    calls = []

    def fake_run(command, **kwargs):
        calls.append(command)
        command_text = " ".join(command)
        if command[:2] == ["docker", "ps"] and "--filter" in command and "label=mirrorneuron.blueprint_id=" not in command_text and "label=mirrorneuron.blueprint_id" in command_text:
            return _completed(command, stdout="bp-old\nbp-active\n")
        if command[:2] == ["docker", "ps"] and "--filter" in command:
            return _completed(command, stdout="container-by-label\n" if "label=mirrorneuron.blueprint_id=bp-old" in command_text else "")
        if command[:2] == ["docker", "images"] and "--filter" in command and command[-1] == "{{.ID}}":
            return _completed(command, stdout="image-by-label\n")
        if command[:2] == ["docker", "images"] and "--filter" in command:
            return _completed(command)
        if command[:3] == ["docker", "ps", "-a"] and "--format" in command:
            return _completed(
                command,
                stdout="container-by-name mn-blueprint-bp-old\ncontainer-active mn-blueprint-bp-active\n",
            )
        if command[:2] == ["docker", "images"] and "--format" in command:
            return _completed(
                command,
                stdout="image-by-name mirror-neuron-blueprint-bp-old\nimage-active mirror-neuron-blueprint-bp-active\n",
            )
        if command[:3] == ["docker", "rm", "-f"]:
            return _completed(command)
        if command[:3] == ["docker", "rmi", "-f"]:
            return _completed(command)
        raise AssertionError(f"unexpected docker command: {command}")

    mocker.patch("mn_cli.libs.blueprint_resources.subprocess.run", side_effect=fake_run)

    summary = cleanup_blueprint_resources(
        active_blueprint_ids={"bp-active"},
        python_envs_dir=tmp_path / "missing-envs",
        include_docker=True,
    )

    removed_ids = {item["id"] for item in summary["docker_removed"]}
    assert {"container-by-label", "container-by-name", "image-by-label", "image-by-name"} <= removed_ids
    assert "container-active" not in removed_ids
    assert "image-active" not in removed_ids
    assert ["docker", "rm", "-f", "container-active"] not in calls
    assert ["docker", "rmi", "-f", "image-active"] not in calls


def test_cleanup_reports_docker_failures_without_crashing(mocker, tmp_path):
    mocker.patch("mn_cli.libs.blueprint_resources.shutil.which", return_value="/usr/local/bin/docker")

    def fake_run(command, **kwargs):
        return _completed(command, returncode=1, stderr="docker unavailable")

    mocker.patch("mn_cli.libs.blueprint_resources.subprocess.run", side_effect=fake_run)

    summary = cleanup_blueprint_resources(
        blueprint_ids={"bp-old"},
        python_envs_dir=tmp_path / "missing-envs",
        include_docker=True,
    )

    assert summary["docker_removed"] == []
    assert summary["docker_skipped"]
    assert all(item["reason"] == "docker_command_failed" for item in summary["docker_skipped"])
