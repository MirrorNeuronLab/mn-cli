import pytest
import json
from pathlib import Path
from io import StringIO
from rich.console import Console
import typer
from typer.testing import CliRunner
from mn_cli.main import app
from mn_cli.libs import blueprint_cmds
from mn_sdk import load_model_ownership

runner = CliRunner()


@pytest.fixture(autouse=True)
def isolated_blueprint_source_env(monkeypatch, tmp_path):
    monkeypatch.delenv("MN_BLUEPRINT_SOURCE", raising=False)
    monkeypatch.delenv("MN_BLUEPRINT_REPO", raising=False)
    monkeypatch.delenv("MN_BLUEPRINT_LOCAL", raising=False)
    mn_home = tmp_path / ".mn"
    mn_home.mkdir()
    monkeypatch.setenv("MN_HOME", str(mn_home))


def _default_blueprint_storage(tmp_path: Path) -> Path:
    return tmp_path / ".mn" / "blueprints"


def _custom_blueprint_cache_root(tmp_path: Path) -> Path:
    return tmp_path / ".mn" / "blueprint_repos"


def _use_local_blueprint_source(monkeypatch, catalog_dir: Path) -> None:
    monkeypatch.setenv("MN_BLUEPRINT_SOURCE", "local")
    monkeypatch.setenv("MN_BLUEPRINT_LOCAL", str(catalog_dir))
    monkeypatch.delenv("MN_BLUEPRINT_REPO", raising=False)


def _write_python_resource(path: Path, blueprint_id: str) -> None:
    (path / "bin").mkdir(parents=True, exist_ok=True)
    (path / "bin" / "python").write_text("")
    (path / ".ready").write_text("")
    (path / ".mn-blueprint-resource.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "resource_type": "python_venv",
                "blueprint_id": blueprint_id,
            }
        )
    )


def _write_generated_bundle(path: Path, blueprint_id: str) -> None:
    path.mkdir(parents=True, exist_ok=True)
    (path / "manifest.json").write_text(
        json.dumps({"metadata": {"blueprint_id": blueprint_id}, "nodes": []})
    )


def _write_bundle_cache(path: Path, blueprint_id: str) -> None:
    _write_generated_bundle(path, blueprint_id)


def _capture_console(*, width: int = 120) -> tuple[Console, StringIO]:
    stream = StringIO()
    return Console(file=stream, force_terminal=False, no_color=True, width=width), stream


def _runtime_model_manifest(
    model: str,
    *,
    provider: str = "docker_model_runner",
    backend: str = "llama.cpp",
    model_config: dict | None = None,
) -> dict:
    entry = {
        "model": model,
        "provider": provider,
        "backend": backend,
    }
    if model_config:
        entry.update(model_config)
    return {
        "runtime": {
            "models": {
                "primary": entry
            }
        }
    }


def _single_model_catalog(
    model_id: str,
    docker_model: str,
    *,
    provider: str = "docker_model_runner",
    backend: str = "llama.cpp",
) -> dict:
    return {
        model_id: {
            "id": model_id,
            "model": docker_model,
            "provider": provider,
            "backend": backend,
        }
    }


def test_blueprint_model_dependency_records_already_installed_manual_owner(
    mocker,
    tmp_path,
    monkeypatch,
    capsys,
):
    monkeypatch.setenv("MN_MODEL_OWNERSHIP_PATH", str(tmp_path / "ownership.json"))
    model_id = "custom-runtime:default"
    docker_model = "custom/runtime:latest"
    mocker.patch(
        "mn_cli.libs.blueprint_cmds._load_model_catalog",
        return_value=_single_model_catalog(model_id, docker_model),
    )
    mocker.patch("mn_cli.libs.blueprint_cmds._model_installed", return_value=True)
    install_model = mocker.patch("mn_cli.libs.blueprint_cmds._install_model_entry")

    summary = blueprint_cmds._install_blueprint_model_dependencies(
        blueprint_id="bp-owned",
        blueprint_revision="rev-1",
        bundle_root=tmp_path / "bundle",
        manifest=_runtime_model_manifest(model_id),
        config={},
        install_source="test-source",
        force=False,
    )

    install_model.assert_not_called()
    assert summary["ok"] is True
    assert summary["models"][0]["status"] == "already_installed"
    record = load_model_ownership()["models"][docker_model]
    assert record["manual"] is True
    assert record["owners"]["bp-owned"]["blueprint_revision"] == "rev-1"
    assert "this may take a few minutes" not in capsys.readouterr().out


def test_blueprint_model_dependency_installs_missing_model_with_feedback(
    mocker,
    tmp_path,
    monkeypatch,
    capsys,
):
    monkeypatch.setenv("MN_MODEL_OWNERSHIP_PATH", str(tmp_path / "ownership.json"))
    model_id = "custom-runtime:default"
    docker_model = "custom/runtime:latest"
    mocker.patch(
        "mn_cli.libs.blueprint_cmds._load_model_catalog",
        return_value=_single_model_catalog(model_id, docker_model),
    )
    mocker.patch("mn_cli.libs.blueprint_cmds._model_installed", return_value=False)
    install_model = mocker.patch(
        "mn_cli.libs.blueprint_cmds._install_model_entry",
        return_value={
            "entry": {"id": model_id, "model": docker_model},
            "docker_model": docker_model,
            "compatibility": {"backend": "llama.cpp"},
        },
    )

    summary = blueprint_cmds._install_blueprint_model_dependencies(
        blueprint_id="bp-owned",
        blueprint_revision="rev-1",
        bundle_root=tmp_path / "bundle",
        manifest=_runtime_model_manifest(model_id),
        config={},
        install_source="test-source",
        force=False,
    )

    install_model.assert_called_once()
    assert summary["ok"] is True
    assert summary["models"][0]["status"] == "installed"
    output = capsys.readouterr().out
    assert "Runtime model custom-runtime:default (custom/runtime:latest) is not installed." in output
    assert "Installing runtime model custom-runtime:default (custom/runtime:latest)" in output
    assert "Docker Model Runner" in output
    assert "this may take a few minutes the first time" in output


def test_blueprint_model_dependency_install_failure_does_not_record_owner(
    mocker,
    tmp_path,
    monkeypatch,
    capsys,
):
    monkeypatch.setenv("MN_MODEL_OWNERSHIP_PATH", str(tmp_path / "ownership.json"))
    model_id = "custom-runtime:default"
    docker_model = "custom/runtime:latest"
    mocker.patch(
        "mn_cli.libs.blueprint_cmds._load_model_catalog",
        return_value=_single_model_catalog(model_id, docker_model),
    )
    mocker.patch("mn_cli.libs.blueprint_cmds._model_installed", return_value=False)
    mocker.patch(
        "mn_cli.libs.blueprint_cmds._install_model_entry",
        side_effect=RuntimeError("pull failed"),
    )

    with pytest.raises(typer.Exit) as raised:
        blueprint_cmds._install_blueprint_model_dependencies(
            blueprint_id="bp-owned",
            blueprint_revision="rev-1",
            bundle_root=tmp_path / "bundle",
            manifest=_runtime_model_manifest(model_id),
            config={},
            install_source="test-source",
            force=False,
        )

    assert raised.value.exit_code == 1
    assert load_model_ownership()["models"] == {}
    output = capsys.readouterr().out
    assert "this may take a few minutes the first time" in output
    assert "Installing runtime model custom-runtime:default (custom/runtime:latest)" in output
    assert "Docker Model Runner" in output


def test_blueprint_model_dependency_service_model_records_owner_without_docker_install(
    mocker,
    tmp_path,
    monkeypatch,
    capsys,
):
    monkeypatch.setenv("MN_MODEL_OWNERSHIP_PATH", str(tmp_path / "ownership.json"))
    model_id = "voice-asr:default"
    docker_model = "service/voice-asr:latest"
    mocker.patch(
        "mn_cli.libs.blueprint_cmds._load_model_catalog",
        return_value=_single_model_catalog(
            model_id,
            docker_model,
            provider="nvidia_service",
            backend="vllm",
        ),
    )
    model_installed = mocker.patch("mn_cli.libs.blueprint_cmds._model_installed")
    install_model = mocker.patch("mn_cli.libs.blueprint_cmds._install_model_entry")

    summary = blueprint_cmds._install_blueprint_model_dependencies(
        blueprint_id="bp-service",
        blueprint_revision="rev-2",
        bundle_root=tmp_path / "bundle",
        manifest=_runtime_model_manifest(model_id, provider="nvidia_service", backend="vllm"),
        config={},
        install_source="test-source",
        force=False,
    )

    model_installed.assert_not_called()
    install_model.assert_not_called()
    assert summary["ok"] is True
    assert summary["models"][0]["status"] == "service_required"
    record = load_model_ownership()["models"][docker_model]
    assert record["provider"] == "nvidia_service"
    assert record["backend"] == "vllm"
    assert record["manual"] is False
    assert record["owners"]["bp-service"]["blueprint_revision"] == "rev-2"
    assert "this may take a few minutes" not in capsys.readouterr().out


def test_blueprint_model_dependency_cluster_provided_skips_local_install(
    mocker,
    tmp_path,
    monkeypatch,
    capsys,
):
    monkeypatch.setenv("MN_MODEL_OWNERSHIP_PATH", str(tmp_path / "ownership.json"))
    model_id = "video-vlm:default"
    docker_model = "hf.co/acme/video-vlm"
    mocker.patch(
        "mn_cli.libs.blueprint_cmds._load_model_catalog",
        return_value=_single_model_catalog(model_id, docker_model),
    )
    model_installed = mocker.patch("mn_cli.libs.blueprint_cmds._model_installed")
    install_model = mocker.patch("mn_cli.libs.blueprint_cmds._install_model_entry")

    summary = blueprint_cmds._install_blueprint_model_dependencies(
        blueprint_id="bp-cluster-model",
        blueprint_revision="rev-3",
        bundle_root=tmp_path / "bundle",
        manifest=_runtime_model_manifest(
            model_id,
            model_config={"install_mode": "cluster_provided"},
        ),
        config={},
        install_source="test-source",
        force=False,
    )

    model_installed.assert_not_called()
    install_model.assert_not_called()
    assert summary["ok"] is True
    assert summary["models"][0]["status"] == "cluster_provided"
    assert load_model_ownership()["models"] == {}
    output = capsys.readouterr().out
    assert "this may take a few minutes" not in output
    assert "Installing runtime model" not in output


def test_blueprint_list_not_initialized(monkeypatch, tmp_path):
    _use_local_blueprint_source(monkeypatch, tmp_path)
    result = runner.invoke(app, ["blueprint", "list"])
    assert result.exit_code == 1
    assert "MN_BLUEPRINT_LOCAL must point to a blueprint catalog with index.json" in result.stdout

def test_blueprint_list_success(monkeypatch, tmp_path):
    index_file = tmp_path / "index.json"
    index_file.write_text(json.dumps([{"id": "bp-1", "name": "Blueprint 1"}]))
    _use_local_blueprint_source(monkeypatch, tmp_path)
    result = runner.invoke(app, ["blueprint", "list"])
    assert result.exit_code == 0
    assert "bp-1" in result.stdout
    assert "Blueprint 1" in result.stdout

def test_blueprint_list_error(monkeypatch, tmp_path):
    index_file = tmp_path / "index.json"
    index_file.write_text("invalid json")
    _use_local_blueprint_source(monkeypatch, tmp_path)
    result = runner.invoke(app, ["blueprint", "list"])
    assert result.exit_code == 1
    assert "Error reading blueprints index" in result.stdout


def test_print_run_table_wraps_on_narrow_console(monkeypatch):
    console, stream = _capture_console(width=52)
    monkeypatch.setattr(blueprint_cmds, "console", console)

    blueprint_cmds._print_run_table(
        [
            {
                "run_id": "run-with-a-very-long-identifier-that-might-wrap",
                "job_id": "job-with-an-astonishingly-long-identifier",
                "status": "completed",
                "ended_at": "2026-06-01T12:34:56Z",
                "blueprint_id": "blueprint-with-an-extraordinarily-long-name-that-wraps",
                "web_ui": "https://example.example/tools/blueprints/that/can/be/very/long",
            }
        ]
    )

    output_lines = [line for line in stream.getvalue().splitlines() if line.strip()]
    assert any("Run ID" in line for line in output_lines)
    assert all(len(line) <= 60 for line in output_lines)


def test_print_log_records_wraps_on_narrow_console(monkeypatch):
    console, stream = _capture_console(width=52)
    monkeypatch.setattr(blueprint_cmds, "console", console)

    blueprint_cmds._print_log_records(
        [
            {
                "ts": "2026-06-01T12:34:56.123456Z",
                "level": "WARNING",
                "component": "worker",
                "message": "A very long log message that should wrap cleanly across lines when output width is narrow and remain readable",
            }
        ]
    )

    output = stream.getvalue()
    output_lines = [line for line in output.splitlines() if line.strip()]
    assert any("Timestamp" in line for line in output_lines)
    assert "very long log message" in output
    assert all(len(line) <= 60 for line in output_lines)


def test_blueprint_observability_commands_read_shared_run_store(tmp_path):
    run_dir = tmp_path / "observe-run"
    run_dir.mkdir()
    (run_dir / "run.json").write_text(json.dumps({
        "run_id": "observe-run",
        "blueprint_id": "general_human_in_the_loop_workflow",
        "status": "running",
    }))
    (run_dir / "logs.jsonl").write_text(json.dumps({
        "ts": "2026-05-22T12:00:01Z",
        "run_id": "observe-run",
        "blueprint_id": "general_human_in_the_loop_workflow",
        "level": "WARN",
        "component": "worker",
        "message": "needs attention",
    }) + "\n")
    (run_dir / "human.jsonl").write_text(json.dumps({
        "ts": "2026-05-22T12:00:02Z",
        "run_id": "observe-run",
        "blueprint_id": "general_human_in_the_loop_workflow",
        "channel": "human",
        "type": "human_input_requested",
        "payload": {"request_id": "hitl-1", "prompt": "Approve?"},
    }) + "\n")
    (run_dir / "resources.jsonl").write_text(json.dumps({
        "ts": "2026-05-22T12:00:03Z",
        "run_id": "observe-run",
        "blueprint_id": "general_human_in_the_loop_workflow",
        "component": "worker",
        "cpu_pct": 12.5,
        "memory_rss_mb": 256,
        "gpu": [],
        "llm": {"input_tokens": 10, "output_tokens": 5, "total_tokens": 15, "calls": 1, "estimated": False},
    }) + "\n")

    logs = runner.invoke(app, ["blueprint", "logs", "observe-run", "--runs-root", str(tmp_path)])
    human = runner.invoke(app, ["blueprint", "human", "observe-run", "--pending", "--runs-root", str(tmp_path)])
    response = runner.invoke(
        app,
        [
            "blueprint",
            "human",
            "respond",
            "observe-run",
            "hitl-1",
            "--decision",
            "approve",
            "--runs-root",
            str(tmp_path),
        ],
    )
    resources = runner.invoke(app, ["blueprint", "resources", "observe-run", "--window", "24000h", "--runs-root", str(tmp_path)])

    assert logs.exit_code == 0
    assert "needs" in logs.stdout
    assert "attention" in logs.stdout
    assert human.exit_code == 0
    assert "hitl-1" in human.stdout
    assert response.exit_code == 0
    assert "Human response successful." in response.stdout
    assert "Approved: True" in response.stdout
    assert resources.exit_code == 0
    assert "15" in resources.stdout


def test_blueprint_list_blueprint_repo_reads_custom_index(mocker, tmp_path):
    repo_url = "https://github.com/MirrorNeuronLab/customer-blueprints"
    custom_cache_root = _custom_blueprint_cache_root(tmp_path)

    def fake_expanduser(path):
        if path == "~/.mn/blueprint_repos":
            return str(custom_cache_root)
        return str(tmp_path / "default-blueprints")

    mocker.patch('mn_cli.libs.blueprint_cmds.os.path.expanduser', side_effect=fake_expanduser)

    completed = mocker.Mock(returncode=0, stderr="", stdout="")

    def fake_run(args, **kwargs):
        if args[:2] == ["git", "clone"]:
            storage_dir = Path(args[-1])
            storage_dir.mkdir(parents=True, exist_ok=True)
            (storage_dir / "index.json").write_text(
                json.dumps([{"id": "private-bp", "name": "Private Blueprint"}])
            )
        return completed

    mocker.patch('mn_cli.libs.blueprint_cmds.subprocess.run', side_effect=fake_run)

    result = runner.invoke(app, ["blueprint", "--blueprint-repo", repo_url, "list"])

    assert result.exit_code == 0
    assert "private-bp" in result.stdout
    assert "Private Blueprint" in result.stdout


def test_blueprint_list_uses_standard_env_local_source(tmp_path, monkeypatch):
    repo = tmp_path / "blueprints"
    repo.mkdir()
    (repo / "index.json").write_text(json.dumps([{"id": "env-bp", "name": "Env Blueprint"}]))
    monkeypatch.setenv("MN_BLUEPRINT_SOURCE", "local")
    monkeypatch.setenv("MN_BLUEPRINT_LOCAL", str(repo))
    monkeypatch.delenv("MN_BLUEPRINT_REPO", raising=False)

    result = runner.invoke(app, ["blueprint", "list"])

    assert result.exit_code == 0
    assert "env-bp" in result.stdout
    assert "Env Blueprint" in result.stdout


def test_blueprint_run_init_success(mocker, tmp_path):
    storage_dir = _default_blueprint_storage(tmp_path)
    mocker.patch('mn_cli.libs.blueprint_cmds.os.path.expanduser', return_value=str(storage_dir))
    
    # Mock subprocess clone
    mock_run = mocker.patch('mn_cli.libs.blueprint_cmds.subprocess.run')
    mock_run.return_value.returncode = 0
    
    # Needs to write index.json and manifest.json so we don't exit early
    def create_files(*args, **kwargs):
        index_file = storage_dir / "index.json"
        storage_dir.mkdir(parents=True, exist_ok=True)
        index_file.write_text(json.dumps([{"id": "bp-1", "path": "bp-1-dir"}]))
        bp_dir = storage_dir / "bp-1-dir"
        bp_dir.mkdir(parents=True, exist_ok=True)
        (bp_dir / "manifest.json").write_text("{}")
        return mock_run.return_value
    
    mock_run.side_effect = create_files
    mock_run_bundle = mocker.patch('mn_cli.libs.blueprint_cmds._run_bundle')
    mocker.patch('mn_cli.libs.blueprint_cmds._git_revision', return_value="abc123")
    
    result = runner.invoke(app, ["blueprint", "run", "bp-1"])
    assert result.exit_code == 0
    assert "Initializing blueprint storage" in result.stdout
    mock_run_bundle.assert_called_once()
    assert mock_run_bundle.call_args.args[0] == str(storage_dir / "bp-1-dir")
    assert mock_run_bundle.call_args.kwargs["env_overrides"]["MN_RUN_ID"].startswith("bp-1-")
    assert mock_run_bundle.call_args.kwargs["submission_metadata"]["blueprint_revision"] == "abc123"


def test_blueprint_run_detached_catalog_name_passes_through(mocker, tmp_path):
    storage_dir = _default_blueprint_storage(tmp_path)
    storage_dir.mkdir()
    mocker.patch('mn_cli.libs.blueprint_cmds.os.path.expanduser', return_value=str(storage_dir))
    mock_run = mocker.patch('mn_cli.libs.blueprint_cmds.subprocess.run')
    mock_run.return_value.returncode = 0
    mocker.patch('mn_cli.libs.blueprint_cmds._git_revision', return_value="abc123")
    (storage_dir / "index.json").write_text(json.dumps([{"id": "bp-1", "path": "bp-1-dir"}]))
    bp_dir = storage_dir / "bp-1-dir"
    bp_dir.mkdir()
    (bp_dir / "manifest.json").write_text("{}")
    mock_run_bundle = mocker.patch('mn_cli.libs.blueprint_cmds._run_bundle')

    result = runner.invoke(app, ["blueprint", "run", "bp-1", "--detached"])

    assert result.exit_code == 0
    mock_run_bundle.assert_called_once()
    assert mock_run_bundle.call_args.kwargs["detached"] is True


def test_blueprint_run_update_success(mocker, tmp_path):
    storage_dir = _default_blueprint_storage(tmp_path)
    storage_dir.mkdir()
    
    mocker.patch('mn_cli.libs.blueprint_cmds.os.path.expanduser', return_value=str(storage_dir))
    
    # Mock subprocess; default run should not pull mutable remote state.
    mock_run = mocker.patch('mn_cli.libs.blueprint_cmds.subprocess.run')
    mock_run.return_value.returncode = 0
    
    index_file = storage_dir / "index.json"
    index_file.write_text(json.dumps([{"id": "bp-1", "path": "bp-1-dir"}]))
    bp_dir = storage_dir / "bp-1-dir"
    bp_dir.mkdir()
    (bp_dir / "manifest.json").write_text("{}")
    
    mock_run_bundle = mocker.patch('mn_cli.libs.blueprint_cmds._run_bundle')
    mocker.patch('mn_cli.libs.blueprint_cmds._git_revision', return_value="abc123")
    
    result = runner.invoke(app, ["blueprint", "run", "bp-1"])
    assert result.exit_code == 0
    assert "Using cached blueprint storage" in result.stdout
    assert not any("pull" in call.args[0] for call in mock_run.call_args_list if call.args)
    mock_run_bundle.assert_called_once()
    assert mock_run_bundle.call_args.args[0] == str(storage_dir / "bp-1-dir")


def test_blueprint_run_update_flag_pulls_cache(mocker, tmp_path):
    storage_dir = _default_blueprint_storage(tmp_path)
    storage_dir.mkdir()
    mocker.patch('mn_cli.libs.blueprint_cmds.os.path.expanduser', return_value=str(storage_dir))
    mock_run = mocker.patch('mn_cli.libs.blueprint_cmds.subprocess.run')
    mock_run.return_value.returncode = 0
    mocker.patch('mn_cli.libs.blueprint_cmds._git_revision', return_value="abc123")
    index_file = storage_dir / "index.json"
    index_file.write_text(json.dumps([{"id": "bp-1", "path": "bp-1-dir"}]))
    bp_dir = storage_dir / "bp-1-dir"
    bp_dir.mkdir()
    (bp_dir / "manifest.json").write_text("{}")
    mock_run_bundle = mocker.patch('mn_cli.libs.blueprint_cmds._run_bundle')

    result = runner.invoke(app, ["blueprint", "run", "bp-1", "--update"])

    assert result.exit_code == 0
    assert "Updating blueprint storage" in result.stdout
    assert any("pull" in call.args[0] for call in mock_run.call_args_list if call.args)
    mock_run_bundle.assert_called_once()


def test_blueprint_update_cleans_resources_for_removed_blueprints(mocker, tmp_path, monkeypatch):
    storage_dir = _default_blueprint_storage(tmp_path)
    storage_dir.mkdir()
    env_root = tmp_path / "python_envs"
    runs_root = tmp_path / "runs"
    generated_root = tmp_path / "generated_blueprint_bundles"
    bundle_cache_root = tmp_path / "bundle_cache"
    removed_env = env_root / "removed-env"
    active_env = env_root / "active-env"
    removed_run = _write_run(runs_root, "bp-removed-run", blueprint_id="bp-removed")
    active_run = _write_run(runs_root, "bp-active-run", blueprint_id="bp-active")
    removed_generated = generated_root / "bp-removed-run"
    active_generated = generated_root / "bp-active-run"
    removed_bundle_cache = bundle_cache_root / "removed-fingerprint"
    active_bundle_cache = bundle_cache_root / "active-fingerprint"
    _write_generated_bundle(removed_generated, "bp-removed")
    _write_generated_bundle(active_generated, "bp-active")
    _write_bundle_cache(removed_bundle_cache, "bp-removed")
    _write_bundle_cache(active_bundle_cache, "bp-active")
    _write_python_resource(removed_env, "bp-removed")
    _write_python_resource(active_env, "bp-active")
    (storage_dir / "index.json").write_text(
        json.dumps(
            [
                {"id": "bp-removed", "path": "bp-removed"},
                {"id": "bp-active", "path": "bp-active"},
            ]
        )
    )
    monkeypatch.setenv("MN_BLUEPRINT_PYTHON_ENVS_DIR", str(env_root))
    monkeypatch.setenv("MN_RUNS_ROOT", str(runs_root))
    monkeypatch.setenv("MN_GENERATED_BLUEPRINT_BUNDLES_DIR", str(generated_root))
    monkeypatch.setenv("MN_BUNDLE_CACHE_DIR", str(bundle_cache_root))
    mocker.patch("mn_cli.libs.blueprint_resources.shutil.which", return_value=None)

    def fake_pull(path):
        assert Path(path) == storage_dir
        (storage_dir / "index.json").write_text(json.dumps([{"id": "bp-active", "path": "bp-active"}]))

    mocker.patch("mn_cli.libs.blueprint_cmds._git_pull", side_effect=fake_pull)

    result = runner.invoke(app, ["blueprint", "update", "--source", str(storage_dir)])

    assert result.exit_code == 0
    assert not removed_env.exists()
    assert not removed_run.exists()
    assert not removed_generated.exists()
    assert not removed_bundle_cache.exists()
    assert active_env.exists()
    assert active_run.exists()
    assert active_generated.exists()
    assert active_bundle_cache.exists()
    assert "Blueprint cleanup successful." in result.stdout
    assert "Python env resources: 1" in result.stdout
    assert "Run records: 1" in result.stdout
    assert "Generated bundles: 1" in result.stdout
    assert "Bundle cache resources: 1" in result.stdout


def test_blueprint_cleanup_removes_dead_and_stale_resources(mocker, tmp_path, monkeypatch):
    storage_dir = _default_blueprint_storage(tmp_path)
    storage_dir.mkdir()
    (storage_dir / "index.json").write_text(json.dumps([{"id": "bp-active", "path": "bp-active"}]))
    env_root = tmp_path / "python_envs"
    runs_root = tmp_path / "runs"
    generated_root = tmp_path / "generated_blueprint_bundles"
    bundle_cache_root = tmp_path / "bundle_cache"
    active_env = env_root / "active-env"
    removed_env = env_root / "removed-env"
    incomplete_env = env_root / "incomplete-env"
    corrupt_env = env_root / "corrupt-env"
    active_run = _write_run(runs_root, "bp-active-run", blueprint_id="bp-active")
    removed_run = _write_run(runs_root, "bp-removed-run", blueprint_id="bp-removed")
    incomplete_run = runs_root / "incomplete-run"
    incomplete_run.mkdir(parents=True)
    active_generated = generated_root / "bp-active-run"
    removed_generated = generated_root / "bp-removed-run"
    orphan_generated = generated_root / "orphan-generated"
    active_bundle_cache = bundle_cache_root / "active-fingerprint"
    removed_bundle_cache = bundle_cache_root / "removed-fingerprint"
    incomplete_bundle_cache = bundle_cache_root / "incomplete-fingerprint"
    _write_generated_bundle(active_generated, "bp-active")
    _write_generated_bundle(removed_generated, "bp-removed")
    orphan_generated.mkdir(parents=True)
    _write_bundle_cache(active_bundle_cache, "bp-active")
    _write_bundle_cache(removed_bundle_cache, "bp-removed")
    incomplete_bundle_cache.mkdir(parents=True)
    _write_python_resource(active_env, "bp-active")
    _write_python_resource(removed_env, "bp-removed")
    incomplete_env.mkdir(parents=True)
    corrupt_env.mkdir(parents=True)
    (corrupt_env / ".mn-blueprint-resource.json").write_text("{not-json")
    monkeypatch.setenv("MN_BLUEPRINT_PYTHON_ENVS_DIR", str(env_root))
    monkeypatch.setenv("MN_RUNS_ROOT", str(runs_root))
    monkeypatch.setenv("MN_GENERATED_BLUEPRINT_BUNDLES_DIR", str(generated_root))
    monkeypatch.setenv("MN_BUNDLE_CACHE_DIR", str(bundle_cache_root))
    monkeypatch.setenv("MN_BLUEPRINT_RESOURCE_STALE_SECONDS", "0")
    mocker.patch("mn_cli.libs.blueprint_resources.shutil.which", return_value=None)

    result = runner.invoke(app, ["blueprint", "cleanup", "--source", str(storage_dir)])

    assert result.exit_code == 0
    assert active_env.exists()
    assert active_run.exists()
    assert active_generated.exists()
    assert active_bundle_cache.exists()
    assert not removed_env.exists()
    assert not removed_run.exists()
    assert not removed_generated.exists()
    assert not removed_bundle_cache.exists()
    assert not incomplete_env.exists()
    assert not incomplete_run.exists()
    assert not corrupt_env.exists()
    assert not orphan_generated.exists()
    assert not incomplete_bundle_cache.exists()
    assert "Blueprint cleanup successful." in result.stdout
    assert "Python env resources: 3" in result.stdout


def test_blueprint_uninstall_removes_storage_and_owned_resources(mocker, tmp_path, monkeypatch):
    storage_dir = _default_blueprint_storage(tmp_path)
    storage_dir.mkdir()
    (storage_dir / "index.json").write_text(json.dumps([{"id": "bp-old", "path": "bp-old"}]))
    env_root = tmp_path / "python_envs"
    runs_root = tmp_path / "runs"
    generated_root = tmp_path / "generated_blueprint_bundles"
    bundle_cache_root = tmp_path / "bundle_cache"
    old_env = env_root / "old-env"
    old_run = _write_run(runs_root, "bp-old-run", blueprint_id="bp-old")
    old_generated = generated_root / "bp-old-run"
    old_bundle_cache = bundle_cache_root / "old-fingerprint"
    _write_generated_bundle(old_generated, "bp-old")
    _write_bundle_cache(old_bundle_cache, "bp-old")
    _write_python_resource(old_env, "bp-old")
    monkeypatch.setenv("MN_BLUEPRINT_PYTHON_ENVS_DIR", str(env_root))
    monkeypatch.setenv("MN_RUNS_ROOT", str(runs_root))
    monkeypatch.setenv("MN_GENERATED_BLUEPRINT_BUNDLES_DIR", str(generated_root))
    monkeypatch.setenv("MN_BUNDLE_CACHE_DIR", str(bundle_cache_root))
    mocker.patch("mn_cli.libs.blueprint_resources.shutil.which", return_value=None)

    result = runner.invoke(app, ["blueprint", "uninstall", "--source", str(storage_dir)])

    assert result.exit_code == 0
    assert not storage_dir.exists()
    assert not old_env.exists()
    assert not old_run.exists()
    assert not old_generated.exists()
    assert not old_bundle_cache.exists()


def test_blueprint_run_blueprint_repo_uses_repo_specific_cache(mocker, tmp_path):
    repo_url = "https://github.com/MirrorNeuronLab/customer-blueprints"
    custom_cache_root = _custom_blueprint_cache_root(tmp_path)
    default_storage = _default_blueprint_storage(tmp_path)

    def fake_expanduser(path):
        if path == "~/.mn/blueprint_repos":
            return str(custom_cache_root)
        return str(default_storage)

    mocker.patch('mn_cli.libs.blueprint_cmds.os.path.expanduser', side_effect=fake_expanduser)

    completed = mocker.Mock(returncode=0, stderr="", stdout="")

    def fake_run(args, **kwargs):
        if args[:2] == ["git", "clone"]:
            storage_dir = Path(args[-1])
            storage_dir.mkdir(parents=True, exist_ok=True)
            (storage_dir / "index.json").write_text(json.dumps([{"id": "bp-1", "path": "bp-1-dir"}]))
            bp_dir = storage_dir / "bp-1-dir"
            bp_dir.mkdir()
            (bp_dir / "manifest.json").write_text("{}")
        return completed

    mock_run = mocker.patch('mn_cli.libs.blueprint_cmds.subprocess.run', side_effect=fake_run)
    mocker.patch('mn_cli.libs.blueprint_cmds._git_revision', return_value="abc123")
    mock_run_bundle = mocker.patch('mn_cli.libs.blueprint_cmds._run_bundle')

    result = runner.invoke(app, ["blueprint", "run", "--blueprint-repo", repo_url, "bp-1"])

    assert result.exit_code == 0
    clone_args = mock_run.call_args_list[0].args[0]
    assert clone_args[:3] == ["git", "clone", repo_url]
    storage_dir = Path(clone_args[-1])
    assert storage_dir.parent == custom_cache_root
    assert storage_dir != default_storage
    assert "Initializing blueprint storage for" in result.stdout
    mock_run_bundle.assert_called_once()
    assert mock_run_bundle.call_args.args[0] == str(storage_dir / "bp-1-dir")


def test_blueprint_run_uses_standard_env_local_source(mocker, tmp_path, monkeypatch):
    repo = tmp_path / "blueprints"
    repo.mkdir()
    (repo / "index.json").write_text(json.dumps([{"id": "bp-1", "path": "bp-1-dir"}]))
    bp_dir = repo / "bp-1-dir"
    bp_dir.mkdir()
    (bp_dir / "manifest.json").write_text("{}")
    monkeypatch.setenv("MN_BLUEPRINT_SOURCE", "local")
    monkeypatch.setenv("MN_BLUEPRINT_LOCAL", str(repo))
    monkeypatch.delenv("MN_BLUEPRINT_REPO", raising=False)
    mock_run_bundle = mocker.patch('mn_cli.libs.blueprint_cmds._run_bundle')

    result = runner.invoke(app, ["blueprint", "run", "bp-1"])

    assert result.exit_code == 0
    mock_run_bundle.assert_called_once()
    assert mock_run_bundle.call_args.args[0] == str(bp_dir)


def test_blueprint_run_fake_llm_flag_overrides_local_bundle(mocker, tmp_path):
    bp_dir = tmp_path / "bundle"
    bp_dir.mkdir()
    (bp_dir / "manifest.json").write_text(json.dumps({"runtime": {"models": {"primary": {"model": "default"}}}}))
    config_dir = bp_dir / "config"
    config_dir.mkdir()
    (config_dir / "default.json").write_text(
        json.dumps(
            {
                "llm": {
                    "enabled": True,
                    "mode": "live",
                    "model": "default",
                    "default_config": "primary",
                    "configs": {
                        "primary": {
                            "provider": "docker_model_runner",
                            "model": "default",
                            "api_base": "http://host.docker.internal:12434/engines/v1",
                        }
                    },
                }
            }
        )
    )
    mock_run_bundle = mocker.patch("mn_cli.libs.blueprint_cmds._run_bundle")

    result = runner.invoke(app, ["blueprint", "run", "--folder", str(bp_dir), "--fake-llm"])

    assert result.exit_code == 0
    mock_run_bundle.assert_called_once()
    kwargs = mock_run_bundle.call_args.kwargs
    assert kwargs["env_overrides"]["MN_BLUEPRINT_LLM_MODE"] == "fake"
    assert kwargs["env_overrides"]["MN_LLM_PROVIDER"] == "fake"
    assert kwargs["env_overrides"]["MN_LLM_MODEL"] == "fake-deterministic-blueprint-agent"
    assert kwargs["submission_metadata"]["fake_llm"] is True
    overrides = kwargs["config_overrides"]
    assert overrides["llm"]["mode"] == "fake"
    assert overrides["llm"]["require_live"] is False
    assert overrides["llm"]["runtime_model"] is None
    assert overrides["llm"]["configs"]["primary"]["provider"] == "fake"
    assert overrides["llm"]["configs"]["primary"]["model"] == "fake-deterministic-blueprint-agent"
    assert overrides["llm"]["configs"]["primary"]["runtime_model"] is None


def test_blueprint_run_testing_flags_override_local_bundle(mocker, tmp_path):
    bp_dir = tmp_path / "bundle"
    bp_dir.mkdir()
    (bp_dir / "manifest.json").write_text(json.dumps({}))
    config_dir = bp_dir / "config"
    config_dir.mkdir()
    (config_dir / "default.json").write_text(json.dumps({"execution": {"existing": True}}))
    mock_run_bundle = mocker.patch("mn_cli.libs.blueprint_cmds._run_bundle")

    result = runner.invoke(app, ["blueprint", "run", "--folder", str(bp_dir), "--fake-skills", "--benchmark", "--debug"])

    assert result.exit_code == 0
    mock_run_bundle.assert_called_once()
    kwargs = mock_run_bundle.call_args.kwargs
    assert kwargs["env_overrides"]["MN_BLUEPRINT_FAKE_SKILLS"] == "1"
    assert kwargs["env_overrides"]["MN_FAKE_SKILLS"] == "1"
    assert kwargs["env_overrides"]["MN_BLUEPRINT_BENCHMARK"] == "1"
    assert kwargs["env_overrides"]["MN_BLUEPRINT_DEBUG"] == "1"
    assert kwargs["env_overrides"]["MN_DEBUG"] == "1"
    assert kwargs["submission_metadata"]["fake_skills"] is True
    assert kwargs["submission_metadata"]["benchmark"] is True
    assert kwargs["submission_metadata"]["debug"] is True
    assert kwargs["config_overrides"]["execution"]["fake_skills"] is True
    assert kwargs["config_overrides"]["execution"]["benchmark"] is True
    assert kwargs["config_overrides"]["execution"]["debug"] is True


def test_blueprint_run_passes_follow_seconds_to_bundle(mocker, tmp_path):
    bp_dir = tmp_path / "bundle"
    bp_dir.mkdir()
    (bp_dir / "manifest.json").write_text(json.dumps({}))
    mock_run_bundle = mocker.patch("mn_cli.libs.blueprint_cmds._run_bundle")

    result = runner.invoke(app, ["blueprint", "run", "--folder", str(bp_dir), "--follow-seconds", "2.5"])

    assert result.exit_code == 0
    assert mock_run_bundle.call_args.kwargs["follow_seconds"] == 2.5


def test_blueprint_doctor_local_folder_passes_flags(mocker, tmp_path):
    bp_dir = tmp_path / "bundle"
    bp_dir.mkdir()
    (bp_dir / "manifest.json").write_text(json.dumps({"metadata": {"blueprint_id": "local-bp"}}))
    mocker.patch("mn_cli.libs.blueprint_cmds._make_blueprint_run_id", return_value="doctor-run")
    mock_doctor = mocker.patch(
        "mn_cli.libs.blueprint_cmds._doctor_bundle",
        return_value={"summary": {"status": "passing"}},
    )

    result = runner.invoke(
        app,
        [
            "blueprint",
            "doctor",
            "--folder",
            str(bp_dir),
            "--json",
            "--timeout",
            "4.5",
            "--check-only",
            "--no-llm-call",
            "--cleanup",
            "--force",
            "--debug",
        ],
    )

    assert result.exit_code == 0
    mock_doctor.assert_called_once()
    assert mock_doctor.call_args.args[0] == str(bp_dir)
    kwargs = mock_doctor.call_args.kwargs
    assert kwargs["json_output"] is True
    assert kwargs["timeout"] == 4.5
    assert kwargs["check_only"] is True
    assert kwargs["no_llm_call"] is True
    assert kwargs["cleanup"] is True
    assert kwargs["force"] is True
    assert kwargs["debug"] is True
    assert kwargs["env_overrides"]["MN_RUN_ID"] == "doctor-run"
    assert kwargs["env_overrides"]["MN_BLUEPRINT_ID"] == "local-bp"
    assert kwargs["submission_metadata"]["doctor"] is True


def test_blueprint_doctor_catalog_uses_run_resolution(mocker, tmp_path):
    storage_dir = _default_blueprint_storage(tmp_path)
    storage_dir.mkdir()
    mocker.patch("mn_cli.libs.blueprint_cmds.os.path.expanduser", return_value=str(storage_dir))
    (storage_dir / "index.json").write_text(json.dumps([{"id": "bp-1", "path": "bp-1-dir"}]))
    bp_dir = storage_dir / "bp-1-dir"
    bp_dir.mkdir()
    (bp_dir / "manifest.json").write_text(json.dumps({"metadata": {"blueprint_id": "catalog-bp"}}))
    mocker.patch("mn_cli.libs.blueprint_cmds._git_revision", return_value="abc123")
    mocker.patch("mn_cli.libs.blueprint_cmds._make_blueprint_run_id", return_value="doctor-run")
    mock_doctor = mocker.patch(
        "mn_cli.libs.blueprint_cmds._doctor_bundle",
        return_value={"summary": {"status": "passing"}},
    )

    result = runner.invoke(app, ["blueprint", "doctor", "bp-1", "--offline", "--no-llm-call"])

    assert result.exit_code == 0
    mock_doctor.assert_called_once()
    assert mock_doctor.call_args.args[0] == str(bp_dir)
    kwargs = mock_doctor.call_args.kwargs
    assert kwargs["env_overrides"]["MN_BLUEPRINT_ID"] == "catalog-bp"
    assert kwargs["env_overrides"]["MN_BLUEPRINT_REVISION"] == "abc123"
    assert kwargs["submission_metadata"]["blueprint_source"] == str(storage_dir)
    assert kwargs["no_llm_call"] is True


def test_blueprint_doctor_blueprint_repo_flag_uses_custom_cache(mocker, tmp_path):
    repo_url = "https://example.test/blueprints.git"
    custom_cache_root = _custom_blueprint_cache_root(tmp_path)
    default_storage = _default_blueprint_storage(tmp_path)

    def fake_expanduser(path):
        if path == "~/.mn/blueprint_repos":
            return str(custom_cache_root)
        return str(default_storage)

    mocker.patch("mn_cli.libs.blueprint_cmds.os.path.expanduser", side_effect=fake_expanduser)
    completed = mocker.Mock(returncode=0, stderr="", stdout="")

    def fake_run(args, **kwargs):
        if args[:2] == ["git", "clone"]:
            storage_dir = Path(args[-1])
            storage_dir.mkdir(parents=True, exist_ok=True)
            (storage_dir / "index.json").write_text(json.dumps([{"id": "bp-1", "path": "bp-1-dir"}]))
            bp_dir = storage_dir / "bp-1-dir"
            bp_dir.mkdir()
            (bp_dir / "manifest.json").write_text("{}")
        return completed

    mock_run = mocker.patch("mn_cli.libs.blueprint_cmds.subprocess.run", side_effect=fake_run)
    mocker.patch("mn_cli.libs.blueprint_cmds._git_revision", return_value="abc123")
    mock_doctor = mocker.patch("mn_cli.libs.blueprint_cmds._doctor_bundle")

    result = runner.invoke(app, ["blueprint", "doctor", "--blueprint-repo", repo_url, "bp-1"])

    assert result.exit_code == 0
    clone_args = mock_run.call_args_list[0].args[0]
    assert clone_args[:3] == ["git", "clone", repo_url]
    storage_dir = Path(clone_args[-1])
    assert storage_dir.parent == custom_cache_root
    mock_doctor.assert_called_once()
    assert mock_doctor.call_args.args[0] == str(storage_dir / "bp-1-dir")


def test_blueprint_doctor_rejects_invalid_target_combinations(mocker, tmp_path):
    bp_dir = tmp_path / "bundle"
    bp_dir.mkdir()
    mock_doctor = mocker.patch("mn_cli.libs.blueprint_cmds._doctor_bundle")

    result = runner.invoke(app, ["blueprint", "doctor", "bp-1", "--folder", str(bp_dir)])

    assert result.exit_code == 1
    assert "pass either a blueprint ID or --folder" in result.stdout
    mock_doctor.assert_not_called()


def test_blueprint_doctor_rejects_local_path_without_folder(mocker, tmp_path):
    bp_dir = tmp_path / "bundle"
    bp_dir.mkdir()
    mock_doctor = mocker.patch("mn_cli.libs.blueprint_cmds._doctor_bundle")

    result = runner.invoke(app, ["blueprint", "doctor", str(bp_dir)])

    assert result.exit_code == 1
    assert "local folders must be passed with --folder" in result.stdout
    mock_doctor.assert_not_called()


def test_blueprint_run_help_lists_testing_flags():
    result = runner.invoke(app, ["blueprint", "run", "--help"])

    assert result.exit_code == 0
    assert "--fake-skills" in result.output
    assert "--benchmark" in result.output
    assert "--debug" in result.output


def test_fake_llm_manifest_override_skips_live_runtime_model_requirement():
    manifest = {"runtime": {"models": {"primary": {"model": "default"}}}, "llm": {"require_live": True, "model": "default"}}

    override = blueprint_cmds._fake_llm_manifest_for_model_dependencies(manifest)

    assert override["llm"]["require_live"] is False
    assert override["runtime"]["models"]["primary"]["provider"] == "fake"
    assert override["runtime"]["models"]["primary"]["model"] == "fake-deterministic-blueprint-agent"


def test_blueprint_run_blueprint_repo_missing_index_errors(mocker, tmp_path):
    repo_url = "https://github.com/MirrorNeuronLab/customer-blueprints"
    custom_cache_root = _custom_blueprint_cache_root(tmp_path)

    def fake_expanduser(path):
        if path == "~/.mn/blueprint_repos":
            return str(custom_cache_root)
        return str(tmp_path / "default-blueprints")

    mocker.patch('mn_cli.libs.blueprint_cmds.os.path.expanduser', side_effect=fake_expanduser)

    completed = mocker.Mock(returncode=0, stderr="", stdout="")

    def fake_run(args, **kwargs):
        if args[:2] == ["git", "clone"]:
            Path(args[-1]).mkdir(parents=True, exist_ok=True)
        return completed

    mocker.patch('mn_cli.libs.blueprint_cmds.subprocess.run', side_effect=fake_run)
    mock_run_bundle = mocker.patch('mn_cli.libs.blueprint_cmds._run_bundle')

    result = runner.invoke(app, ["blueprint", "run", "--blueprint-repo", repo_url, "bp-1"])

    assert result.exit_code == 1
    assert "index.json not found" in result.stdout
    mock_run_bundle.assert_not_called()


def test_blueprint_run_blueprint_repo_malformed_index_errors(mocker, tmp_path):
    repo_url = "https://github.com/MirrorNeuronLab/customer-blueprints"
    custom_cache_root = _custom_blueprint_cache_root(tmp_path)

    def fake_expanduser(path):
        if path == "~/.mn/blueprint_repos":
            return str(custom_cache_root)
        return str(tmp_path / "default-blueprints")

    mocker.patch('mn_cli.libs.blueprint_cmds.os.path.expanduser', side_effect=fake_expanduser)

    completed = mocker.Mock(returncode=0, stderr="", stdout="")

    def fake_run(args, **kwargs):
        if args[:2] == ["git", "clone"]:
            storage_dir = Path(args[-1])
            storage_dir.mkdir(parents=True, exist_ok=True)
            (storage_dir / "index.json").write_text(json.dumps({"blueprints": {}}))
        return completed

    mocker.patch('mn_cli.libs.blueprint_cmds.subprocess.run', side_effect=fake_run)
    mock_run_bundle = mocker.patch('mn_cli.libs.blueprint_cmds._run_bundle')

    result = runner.invoke(app, ["blueprint", "run", "--blueprint-repo", repo_url, "bp-1"])

    assert result.exit_code == 1
    assert "index.json is not well formatted" in result.stdout
    mock_run_bundle.assert_not_called()


def test_blueprint_run_generates_python_source_bundle(mocker, tmp_path, monkeypatch):
    storage_dir = _default_blueprint_storage(tmp_path)
    generated_root = tmp_path / "generated"
    storage_dir.mkdir()
    monkeypatch.setenv("MN_GENERATED_BLUEPRINT_BUNDLES_DIR", str(generated_root))

    def fake_expanduser(path):
        return str(storage_dir)

    mocker.patch('mn_cli.libs.blueprint_cmds.os.path.expanduser', side_effect=fake_expanduser)
    mocker.patch('mn_cli.libs.blueprint_cmds._git_revision', return_value="abc123")

    index_file = storage_dir / "index.json"
    index_file.write_text(json.dumps([{"id": "bp-1", "path": "bp-1-dir"}]))
    bp_dir = storage_dir / "bp-1-dir"
    bp_dir.mkdir()
    (bp_dir / "manifest.json").write_text(
        json.dumps({"metadata": {"blueprint_id": "bp-1", "python_source_mode": True}})
    )

    def fake_generate_bundle(blueprint_dir, output_dir):
        output_dir.mkdir(parents=True)
        (output_dir / "manifest.json").write_text(
            json.dumps({"metadata": {"blueprint_id": "bp-1", "python_source_mode": True}})
        )
        (output_dir / "payloads").mkdir()
        return output_dir

    mock_generate_bundle = mocker.patch(
        'mn_cli.libs.blueprint_cmds._generate_python_source_bundle',
        side_effect=fake_generate_bundle,
    )
    mock_run_bundle = mocker.patch('mn_cli.libs.blueprint_cmds._run_bundle')

    result = runner.invoke(app, ["blueprint", "run", "bp-1", "--run-id", "run-123"])

    assert result.exit_code == 0
    assert "Generating Python workflow bundle" in result.stdout
    mock_generate_bundle.assert_called_once()
    assert mock_generate_bundle.call_args.args[0] == bp_dir
    mock_run_bundle.assert_called_once()
    assert mock_run_bundle.call_args.args[0] == str(generated_root / "run-123")


def test_blueprint_run_local_bundle_folder_is_rejected(mocker, tmp_path):
    bundle_dir = tmp_path / "local-bundle"
    bundle_dir.mkdir()
    (bundle_dir / "manifest.json").write_text(
        json.dumps(
            {
                "graph_id": "local_bundle_v1",
                "metadata": {"blueprint_id": "local_bundle"},
                "nodes": [],
            }
        )
    )
    mock_run_bundle = mocker.patch('mn_cli.libs.blueprint_cmds._run_bundle')

    result = runner.invoke(app, ["blueprint", "run", str(bundle_dir), "--run-id", "run-local"])

    assert result.exit_code == 1
    assert "local folders must be passed with --folder" in result.stdout
    assert f"mnblueprintrun--folder{bundle_dir}" in "".join(result.stdout.split())
    mock_run_bundle.assert_not_called()


def test_blueprint_run_local_python_source_folder_is_rejected(mocker, tmp_path):
    source_dir = tmp_path / "source-blueprint"
    source_dir.mkdir()
    (source_dir / "manifest.json").write_text(
        json.dumps(
            {
                "graph_id": "source_blueprint_v1",
                "metadata": {
                    "blueprint_id": "source_blueprint",
                    "python_workflow": {"module": "workflow", "class": "Workflow"},
                },
            }
        )
    )
    mock_generate_bundle = mocker.patch(
        'mn_cli.libs.blueprint_cmds._generate_python_source_bundle'
    )
    mock_run_bundle = mocker.patch('mn_cli.libs.blueprint_cmds._run_bundle')

    result = runner.invoke(app, ["blueprint", "run", str(source_dir), "--run-id", "run-source"])

    assert result.exit_code == 1
    assert "local folders must be passed with --folder" in result.stdout
    mock_generate_bundle.assert_not_called()
    mock_run_bundle.assert_not_called()


def test_blueprint_run_local_folder_missing_manifest_is_rejected(mocker, tmp_path):
    source_dir = tmp_path / "missing-manifest"
    source_dir.mkdir()
    mocker.patch('mn_cli.libs.blueprint_cmds._run_bundle')

    result = runner.invoke(app, ["blueprint", "run", str(source_dir)])

    assert result.exit_code == 1
    assert "local folders must be passed with --folder" in result.stdout


def test_blueprint_run_local_python_source_generation_failure_is_not_reached(mocker, tmp_path):
    source_dir = tmp_path / "source-blueprint"
    source_dir.mkdir()
    (source_dir / "manifest.json").write_text(
        json.dumps(
            {
                "metadata": {
                    "blueprint_id": "source_blueprint",
                    "python_source_mode": True,
                }
            }
        )
    )
    mocker.patch('mn_cli.libs.blueprint_cmds.os.path.expanduser', return_value=str(tmp_path / "generated"))
    mocker.patch('mn_cli.libs.blueprint_cmds._git_revision', return_value=None)
    mocker.patch(
        'mn_cli.libs.blueprint_cmds._generate_python_source_bundle',
        side_effect=RuntimeError("compiler exploded"),
    )
    mock_run_bundle = mocker.patch('mn_cli.libs.blueprint_cmds._run_bundle')

    result = runner.invoke(app, ["blueprint", "run", str(source_dir), "--run-id", "run-source"])

    assert result.exit_code == 1
    assert "local folders must be passed with --folder" in result.stdout
    mock_run_bundle.assert_not_called()


def test_run_folder_generates_local_python_source_bundle(mocker, tmp_path, monkeypatch):
    source_dir = tmp_path / "source-blueprint"
    generated_root = tmp_path / "generated"
    monkeypatch.setenv("MN_GENERATED_BLUEPRINT_BUNDLES_DIR", str(generated_root))
    source_dir.mkdir()
    (source_dir / "manifest.json").write_text(
        json.dumps(
            {
                "graph_id": "source_blueprint_v1",
                "metadata": {
                    "blueprint_id": "source_blueprint",
                    "python_source_mode": True,
                },
            }
        )
    )

    def fake_generate_bundle(blueprint_dir, output_dir):
        output_dir.mkdir(parents=True)
        (output_dir / "manifest.json").write_text(
            json.dumps({"metadata": {"blueprint_id": "source_blueprint"}})
        )
        (output_dir / "payloads").mkdir()
        return output_dir

    mock_generate_bundle = mocker.patch(
        "mn_cli.libs.blueprint_cmds._generate_python_source_bundle",
        side_effect=fake_generate_bundle,
    )
    mock_run_bundle = mocker.patch("mn_cli.libs.blueprint_cmds._run_bundle")

    result = runner.invoke(app, ["blueprint", "run", "--folder", str(source_dir), "--run-id", "run-source"])

    assert result.exit_code == 0
    assert "Generating Python workflow bundle" in result.stdout
    mock_generate_bundle.assert_called_once()
    assert mock_generate_bundle.call_args.args[0] == source_dir
    mock_run_bundle.assert_called_once()
    assert mock_run_bundle.call_args.args[0] == str(generated_root / "run-source")


def test_root_run_command_is_removed():
    result = runner.invoke(app, ["run", "bp-1"])

    assert result.exit_code != 0


def test_blueprint_run_init_fail(mocker, tmp_path):
    storage_dir = _default_blueprint_storage(tmp_path)
    mocker.patch('mn_cli.libs.blueprint_cmds.os.path.expanduser', return_value=str(storage_dir))
    
    # Mock subprocess clone
    mock_run = mocker.patch('mn_cli.libs.blueprint_cmds.subprocess.run')
    mock_run.return_value.returncode = 1
    mock_run.return_value.stderr = "git clone failed"
    
    result = runner.invoke(app, ["blueprint", "run", "bp-1"])
    assert result.exit_code == 1
    assert "Failed to clone blueprint repository" in result.stdout

def test_blueprint_run_no_index(mocker, tmp_path):
    storage_dir = _default_blueprint_storage(tmp_path)
    storage_dir.mkdir()
    mocker.patch('mn_cli.libs.blueprint_cmds.os.path.expanduser', return_value=str(storage_dir))
    
    mock_run = mocker.patch('mn_cli.libs.blueprint_cmds.subprocess.run')
    mock_run.return_value.returncode = 0
    
    result = runner.invoke(app, ["blueprint", "run", "bp-1"])
    assert result.exit_code == 1
    assert "index.json not found" in result.stdout

def test_blueprint_run_invalid_index(mocker, tmp_path):
    storage_dir = _default_blueprint_storage(tmp_path)
    storage_dir.mkdir()
    mocker.patch('mn_cli.libs.blueprint_cmds.os.path.expanduser', return_value=str(storage_dir))
    
    mock_run = mocker.patch('mn_cli.libs.blueprint_cmds.subprocess.run')
    mock_run.return_value.returncode = 0
    
    index_file = storage_dir / "index.json"
    index_file.write_text("{badjson}")
    
    result = runner.invoke(app, ["blueprint", "run", "bp-1"])
    assert result.exit_code == 1
    assert "Error parsing index.json" in result.stdout

def test_blueprint_run_not_found(mocker, tmp_path):
    storage_dir = _default_blueprint_storage(tmp_path)
    storage_dir.mkdir()
    mocker.patch('mn_cli.libs.blueprint_cmds.os.path.expanduser', return_value=str(storage_dir))
    
    mock_run = mocker.patch('mn_cli.libs.blueprint_cmds.subprocess.run')
    mock_run.return_value.returncode = 0
    
    index_file = storage_dir / "index.json"
    index_file.write_text(json.dumps([{"id": "bp-2", "path": "bp-2"}]))
    
    result = runner.invoke(app, ["blueprint", "run", "bp-1"])
    assert result.exit_code == 1
    assert "not found in index" in result.stdout

def test_blueprint_run_no_manifest(mocker, tmp_path):
    storage_dir = _default_blueprint_storage(tmp_path)
    storage_dir.mkdir()
    mocker.patch('mn_cli.libs.blueprint_cmds.os.path.expanduser', return_value=str(storage_dir))
    
    mock_run = mocker.patch('mn_cli.libs.blueprint_cmds.subprocess.run')
    mock_run.return_value.returncode = 0
    
    index_file = storage_dir / "index.json"
    index_file.write_text(json.dumps([{"id": "bp-1", "path": "bp-1-dir"}]))
    
    bp_dir = storage_dir / "bp-1-dir"
    bp_dir.mkdir()
    # explicitly NO manifest.json
    
    result = runner.invoke(app, ["blueprint", "run", "bp-1"])
    assert result.exit_code == 1
    assert "missing manifest.json" in result.stdout
def test_blueprint_run_update_fail(mocker, tmp_path):
    storage_dir = _default_blueprint_storage(tmp_path)
    storage_dir.mkdir()
    
    mocker.patch('mn_cli.libs.blueprint_cmds.os.path.expanduser', return_value=str(storage_dir))
    
    # Mock subprocess pull to fail
    mock_run = mocker.patch('mn_cli.libs.blueprint_cmds.subprocess.run')
    mock_run.return_value.returncode = 1
    mock_run.return_value.stderr = "git pull error"
    
    index_file = storage_dir / "index.json"
    index_file.write_text(json.dumps([{"id": "bp-1", "path": "bp-1-dir"}]))
    bp_dir = storage_dir / "bp-1-dir"
    bp_dir.mkdir()
    (bp_dir / "manifest.json").write_text("{}")
    
    mock_run_bundle = mocker.patch('mn_cli.libs.blueprint_cmds._run_bundle')
    mocker.patch('mn_cli.libs.blueprint_cmds._git_revision', return_value="abc123")
    
    result = runner.invoke(app, ["blueprint", "run", "bp-1", "--update"])
    assert result.exit_code == 0
    assert "Warning: Failed to update blueprint repository: git pull error" in result.stdout
    mock_run_bundle.assert_called_once()


def _write_run(runs_root, run_id, blueprint_id="general_closed_loop_agent_runtime", status="completed", action="hold_policy"):
    run_dir = runs_root / run_id
    run_dir.mkdir(parents=True)
    (run_dir / "run.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "blueprint_id": blueprint_id,
                "status": status,
                "started_at": "2026-05-04T00:00:00+00:00",
                "ended_at": "2026-05-04T00:01:00+00:00",
                "run_dir": str(run_dir),
            }
        )
    )
    (run_dir / "config.json").write_text(json.dumps({"simulation": {"steps": 3}}))
    (run_dir / "inputs.json").write_text(json.dumps({"input_source": "mock"}))
    (run_dir / "events.jsonl").write_text(
        "\n".join(
            [
                json.dumps({"timestamp": "2026-05-04T00:00:01+00:00", "type": "run_started"}),
                json.dumps({"timestamp": "2026-05-04T00:00:30+00:00", "type": "agent_decision", "action": action}),
                json.dumps({"timestamp": "2026-05-04T00:01:00+00:00", "type": "run_completed"}),
            ]
        )
        + "\n"
    )
    (run_dir / "result.json").write_text(json.dumps({"score": 0.82, "final_artifact": {"recommended_action": action}}))
    (run_dir / "final_artifact.json").write_text(
        json.dumps({"recommended_action": action, "risk_level": "medium", "score": 0.82})
    )
    (run_dir / "web_ui.json").write_text(
        json.dumps({"adapter": "static_html", "kind": "output", "status": "available", "url": f"file://{run_dir}/web/index.html"})
    )
    return run_dir


def test_blueprint_monitor_reads_shared_run_store(tmp_path):
    runs_root = tmp_path / "runs"
    _write_run(runs_root, "run-1", action="rebalance")

    result = runner.invoke(app, ["blueprint", "monitor", "--runs-root", str(runs_root)])

    assert result.exit_code == 0
    assert "run-1" in result.stdout
    assert "general_closed_loop_agent_runtime" in result.stdout
    assert "completed" in result.stdout
    assert "file://" in result.stdout


def test_blueprint_tail_prints_event_stream(tmp_path):
    runs_root = tmp_path / "runs"
    _write_run(runs_root, "run-1", action="escalate_review")

    result = runner.invoke(app, ["blueprint", "tail", "run-1", "--runs-root", str(runs_root), "--lines", "2"])

    assert result.exit_code == 0
    assert "agent_decision" in result.stdout
    assert "escalate_review" in result.stdout
    assert "run_completed" in result.stdout


def test_blueprint_compare_shows_artifact_differences(tmp_path):
    runs_root = tmp_path / "runs"
    _write_run(runs_root, "run-a", action="hold_policy")
    _write_run(runs_root, "run-b", action="rebalance")

    result = runner.invoke(app, ["blueprint", "compare", "run-a", "run-b", "--runs-root", str(runs_root)])

    assert result.exit_code == 0
    assert "run-a" in result.stdout
    assert "run-b" in result.stdout
    assert "hold_policy" in result.stdout
    assert "rebalance" in result.stdout


def test_blueprint_export_markdown_contains_standard_artifacts(tmp_path):
    runs_root = tmp_path / "runs"
    _write_run(runs_root, "run-1", action="approve_plan")

    result = runner.invoke(
        app,
        ["blueprint", "export", "run-1", "--runs-root", str(runs_root), "--format", "markdown"],
    )

    assert result.exit_code == 0
    assert "# Blueprint Run run-1" in result.stdout
    assert "## Final Artifact" in result.stdout
    assert "approve_plan" in result.stdout
    assert "## Event Tail" in result.stdout
    assert "## Web UI" in result.stdout


def test_blueprint_export_html_writes_static_report(tmp_path):
    runs_root = tmp_path / "runs"
    run_dir = _write_run(runs_root, "run-1", action="approve_plan")

    result = runner.invoke(
        app,
        ["blueprint", "export", "run-1", "--runs-root", str(runs_root), "--format", "html"],
    )

    assert result.exit_code == 0
    assert result.stdout.strip().startswith("file://")
    assert (run_dir / "web" / "index.html").exists()


def test_blueprint_export_rejects_unknown_format(tmp_path):
    runs_root = tmp_path / "runs"
    _write_run(runs_root, "run-1")

    result = runner.invoke(app, ["blueprint", "export", "run-1", "--runs-root", str(runs_root), "--format", "yaml"])

    assert result.exit_code == 1
    assert "Unsupported export format" in result.stdout


def test_blueprint_tail_missing_run_reports_error(tmp_path):
    runs_root = tmp_path / "runs"
    runs_root.mkdir()

    result = runner.invoke(app, ["blueprint", "tail", "missing-run", "--runs-root", str(runs_root)])

    assert result.exit_code == 1
    assert "missing-run" in result.stdout
    assert "not found" in result.stdout
