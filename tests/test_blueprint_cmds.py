import pytest
import json
import subprocess
from typer.testing import CliRunner
from mn_cli.main import app
import os
from pathlib import Path

runner = CliRunner()

def test_blueprint_list_not_initialized(mocker, tmp_path):
    mocker.patch('mn_cli.libs.blueprint_cmds.os.path.expanduser', return_value=str(tmp_path / "index.json"))
    result = runner.invoke(app, ["blueprint", "list"])
    assert result.exit_code == 0
    assert "Blueprint storage not initialized" in result.stdout

def test_blueprint_list_success(mocker, tmp_path):
    index_file = tmp_path / "index.json"
    index_file.write_text(json.dumps([{"id": "bp-1", "name": "Blueprint 1"}]))
    
    mocker.patch('mn_cli.libs.blueprint_cmds.os.path.expanduser', return_value=str(index_file))
    result = runner.invoke(app, ["blueprint", "list"])
    assert result.exit_code == 0
    assert "bp-1" in result.stdout
    assert "Blueprint 1" in result.stdout

def test_blueprint_list_error(mocker, tmp_path):
    index_file = tmp_path / "index.json"
    index_file.write_text("invalid json")
    
    mocker.patch('mn_cli.libs.blueprint_cmds.os.path.expanduser', return_value=str(index_file))
    result = runner.invoke(app, ["blueprint", "list"])
    assert result.exit_code == 0
    assert "Error reading blueprints index" in result.stdout

def test_blueprint_run_init_success(mocker, tmp_path):
    storage_dir = tmp_path / "blueprints"
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
    
    result = runner.invoke(app, ["blueprint", "run", "bp-1"])
    assert result.exit_code == 0
    assert "Initializing blueprint storage" in result.stdout
    mock_run_bundle.assert_called_once_with(str(storage_dir / "bp-1-dir"))

def test_blueprint_run_update_success(mocker, tmp_path):
    storage_dir = tmp_path / "blueprints"
    storage_dir.mkdir()
    
    mocker.patch('mn_cli.libs.blueprint_cmds.os.path.expanduser', return_value=str(storage_dir))
    
    # Mock subprocess pull
    mock_run = mocker.patch('mn_cli.libs.blueprint_cmds.subprocess.run')
    mock_run.return_value.returncode = 0
    
    index_file = storage_dir / "index.json"
    index_file.write_text(json.dumps([{"id": "bp-1", "path": "bp-1-dir"}]))
    bp_dir = storage_dir / "bp-1-dir"
    bp_dir.mkdir()
    (bp_dir / "manifest.json").write_text("{}")
    
    mock_run_bundle = mocker.patch('mn_cli.libs.blueprint_cmds._run_bundle')
    
    result = runner.invoke(app, ["blueprint", "run", "bp-1"])
    assert result.exit_code == 0
    assert "Updating blueprint storage" in result.stdout
    mock_run_bundle.assert_called_once_with(str(storage_dir / "bp-1-dir"))

def test_blueprint_run_init_fail(mocker, tmp_path):
    storage_dir = tmp_path / "blueprints"
    mocker.patch('mn_cli.libs.blueprint_cmds.os.path.expanduser', return_value=str(storage_dir))
    
    # Mock subprocess clone
    mock_run = mocker.patch('mn_cli.libs.blueprint_cmds.subprocess.run')
    mock_run.return_value.returncode = 1
    mock_run.return_value.stderr = "git clone failed"
    
    result = runner.invoke(app, ["blueprint", "run", "bp-1"])
    assert result.exit_code == 1
    assert "Failed to clone blueprint repository" in result.stdout

def test_blueprint_run_no_index(mocker, tmp_path):
    storage_dir = tmp_path / "blueprints"
    storage_dir.mkdir()
    mocker.patch('mn_cli.libs.blueprint_cmds.os.path.expanduser', return_value=str(storage_dir))
    
    mock_run = mocker.patch('mn_cli.libs.blueprint_cmds.subprocess.run')
    mock_run.return_value.returncode = 0
    
    result = runner.invoke(app, ["blueprint", "run", "bp-1"])
    assert result.exit_code == 1
    assert "index.json not found" in result.stdout

def test_blueprint_run_invalid_index(mocker, tmp_path):
    storage_dir = tmp_path / "blueprints"
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
    storage_dir = tmp_path / "blueprints"
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
    storage_dir = tmp_path / "blueprints"
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
    storage_dir = tmp_path / "blueprints"
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
    
    result = runner.invoke(app, ["blueprint", "run", "bp-1"])
    assert result.exit_code == 0
    assert "Warning: Failed to update blueprint repository: git pull error" in result.stdout
    mock_run_bundle.assert_called_once_with(str(storage_dir / "bp-1-dir"))