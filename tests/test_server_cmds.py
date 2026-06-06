import json
import pytest
import subprocess
import sys
from io import StringIO
from pathlib import Path
from unittest.mock import call
from rich.console import Console
import mn_cli.server_cmds as server_cmds
from mn_cli.server_cmds import (
    check_status,
    kill_tree,
    _resolve_grpc_admin_token,
    _resolve_grpc_auth_token,
    _resolve_mn_cookie,
    _resolve_network_token,
    _refresh_network_token,
    _derive_network_secret,
    _erl_aflags,
    _start_server,
    _start_network_seed,
    _start_worker_node,
    _stop_local_runtime_for_worker,
    _join_network,
    _avoid_local_compose_port_conflicts,
    _detach_local_docker_node_if_matches,
    _ensure_docker_network,
    _resolve_node_alias,
    find_web_ui_dir,
    _start_api_if_installed,
    _start_web_ui_if_installed,
    _compose_runtime_env,
    _print_service_endpoints,
    _runtime_endpoint_snapshot,
    _runtime_blueprint_env_updates,
    runtime_compose_cmd,
)
import typer

ORIGINAL_WEB_UI_DIRS = server_cmds.WEB_UI_DIRS

@pytest.fixture(autouse=True)
def isolated_mn_cookie_home(mocker, tmp_path, monkeypatch):
    monkeypatch.delenv("MN_COOKIE", raising=False)
    monkeypatch.delenv("MN_GRPC_AUTH_TOKEN", raising=False)
    monkeypatch.delenv("MN_GRPC_ADMIN_TOKEN", raising=False)
    monkeypatch.delenv("MN_MIRROR_NEURON_GRPC_ADMIN_TOKEN", raising=False)
    monkeypatch.delenv("MN_NODE_GPU", raising=False)
    monkeypatch.delenv("MN_NODE_GPU_COUNT", raising=False)
    monkeypatch.delenv("MN_NODE_DISPLAY_NAME", raising=False)
    monkeypatch.delenv("MN_NODE_ALIAS", raising=False)
    monkeypatch.delenv("MN_DOCKER_NETWORK_MODE", raising=False)
    monkeypatch.delenv("MN_DOCKER_NETWORK_NAME", raising=False)
    monkeypatch.delenv("MN_NETWORK_JOIN_TOKEN", raising=False)
    state_dir = tmp_path / ".mirror_neuron"
    log_dir = state_dir / ".logs"
    pid_dir = state_dir / ".pids"
    mocker.patch('mn_cli.server_cmds.DIR', state_dir)
    mocker.patch('mn_cli.server_cmds.PID_DIR', pid_dir)
    mocker.patch('mn_cli.server_cmds.LOG_DIR', log_dir)
    mocker.patch('mn_cli.server_cmds.BEAM_PID_FILE', pid_dir / "beam.pid")
    mocker.patch('mn_cli.server_cmds.API_PID_FILE', pid_dir / "api.pid")
    mocker.patch('mn_cli.server_cmds.API_WATCHDOG_PID_FILE', pid_dir / "api-watchdog.pid")
    mocker.patch('mn_cli.server_cmds.WEB_UI_PID_FILE', pid_dir / "web-ui.pid")
    mocker.patch('mn_cli.server_cmds.WEB_UI_WATCHDOG_PID_FILE', pid_dir / "web-ui-watchdog.pid")
    mocker.patch('mn_cli.server_cmds.BEAM_LOG', log_dir / "beam.log")
    mocker.patch('mn_cli.server_cmds.API_LOG', log_dir / "api.log")
    mocker.patch('mn_cli.server_cmds.API_WATCHDOG_LOG', log_dir / "api-watchdog.log")
    mocker.patch('mn_cli.server_cmds.WEB_UI_LOG', log_dir / "web-ui.log")
    mocker.patch('mn_cli.server_cmds.WEB_UI_WATCHDOG_LOG', log_dir / "web-ui-watchdog.log")
    mocker.patch('mn_cli.server_cmds.VENV_DIR', tmp_path / "mn_venv")
    mocker.patch('mn_cli.server_cmds.RUNTIME_COMPOSE_FILE', state_dir / "docker-compose.yml")
    mocker.patch('mn_cli.server_cmds.RUNTIME_COMPOSE_ENV', state_dir / "docker-compose.env")
    mocker.patch('mn_cli.server_cmds.RUNTIME_ENDPOINTS_FILE', state_dir / "runtime-endpoints.json")
    mocker.patch('mn_cli.server_cmds.NETWORK_TOKEN_FILE', state_dir / "network.token")
    mocker.patch('mn_cli.server_cmds.NETWORK_REDIS_ENV_FILE', state_dir / "network-redis.env")
    mocker.patch('mn_cli.server_cmds._published_container_port', return_value=None)
    mocker.patch(
        'mn_cli.server_cmds.WEB_UI_DIRS',
        (state_dir / "webui", state_dir / "web-ui-source"),
    )

def test_check_status_running(mocker, tmp_path):
    pid_file = tmp_path / "test.pid"
    pid_file.write_text("1234")
    mocker.patch('mn_cli.server_cmds.os.kill')
    assert check_status(pid_file) == 0

def test_check_status_stale(mocker, tmp_path):
    pid_file = tmp_path / "test.pid"
    pid_file.write_text("1234")
    mocker.patch('mn_cli.server_cmds.os.kill', side_effect=OSError)
    assert check_status(pid_file) == 1

def test_check_status_invalid(tmp_path):
    pid_file = tmp_path / "test.pid"
    pid_file.write_text("abc")
    assert check_status(pid_file) == 1

def test_check_status_missing(tmp_path):
    pid_file = tmp_path / "test.pid"
    assert check_status(pid_file) == 2

def test_detect_lan_ip_prefers_route_selected_non_loopback(mocker):
    class ProbeSocket:
        def connect(self, _target):
            pass

        def getsockname(self):
            return ("192.168.4.35", 50123)

        def close(self):
            pass

    mocker.patch("mn_cli.server_cmds.socket.socket", return_value=ProbeSocket())
    mocker.patch("mn_cli.server_cmds.socket.gethostname", return_value="loopback-host")
    mocker.patch("mn_cli.server_cmds.socket.gethostbyname", return_value="127.0.0.1")

    assert server_cmds._detect_lan_ip() == "192.168.4.35"

def test_detect_host_gpu_count_uses_macos_system_profiler(mocker):
    class UnameMock:
        sysname = "Darwin"

    mocker.patch("mn_cli.server_cmds.os.uname", return_value=UnameMock())

    def mock_run(cmd, **kwargs):
        assert cmd == ["system_profiler", "SPDisplaysDataType"]
        m = mocker.Mock()
        m.returncode = 0
        m.stdout = "Graphics/Displays:\n  Chipset Model: Apple M5\n"
        return m

    mocker.patch("mn_cli.server_cmds.subprocess.run", side_effect=mock_run)

    assert server_cmds._detect_host_gpu_count() == 1

def test_detect_host_gpu_count_uses_linux_nvidia_smi(mocker):
    class UnameMock:
        sysname = "Linux"

    mocker.patch("mn_cli.server_cmds.os.uname", return_value=UnameMock())

    def mock_run(cmd, **kwargs):
        assert cmd == ["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"]
        m = mocker.Mock()
        m.returncode = 0
        m.stdout = "NVIDIA GB10\n"
        return m

    mocker.patch("mn_cli.server_cmds.subprocess.run", side_effect=mock_run)

    assert server_cmds._detect_host_gpu_count() == 1

def test_docker_network_command_args_omit_default_bridge_network():
    assert server_cmds._docker_network_command_args("bridge", "mirror-neuron-runtime") == ""
    assert server_cmds._docker_network_command_args("disabled", "mirror-neuron-runtime") == ""
    assert (
        server_cmds._docker_network_command_args("overlay", "mn-overlay")
        == " --network overlay --docker-network mn-overlay"
    )
    assert (
        server_cmds._docker_network_command_args("bridge", "custom-bridge")
        == " --network bridge --docker-network custom-bridge"
    )

def test_kill_tree(mocker):
    # Mock os.kill to succeed for existence check
    mock_kill = mocker.patch('mn_cli.server_cmds.os.kill')
    
    # Mock pgrep to return children
    mocker.patch('mn_cli.server_cmds.subprocess.check_output', side_effect=[
        b" 1235 \n 1236 \n", # First call for parent 1234
        subprocess.CalledProcessError(1, "pgrep"), # Second call for child 1235
        subprocess.CalledProcessError(1, "pgrep")  # Third call for child 1236
    ])
    
    kill_tree(1234)
    
    # Should call kill for existence, then pgrep, then recurse, then kill with SIGTERM
    assert mock_kill.call_count == 3 * 2 # (exists + term) for 1234, 1235, 1236
    
def test_kill_tree_not_exist(mocker):
    mocker.patch('mn_cli.server_cmds.os.kill', side_effect=OSError)
    mock_check_output = mocker.patch('mn_cli.server_cmds.subprocess.check_output')
    kill_tree(1234)
    mock_check_output.assert_not_called()

def test_kill_tree_term_fails(mocker):
    # Succeed first check, fail SIGTERM
    mock_kill = mocker.patch('mn_cli.server_cmds.os.kill', side_effect=[None, OSError])
    mocker.patch('mn_cli.server_cmds.subprocess.check_output', side_effect=subprocess.CalledProcessError(1, "pgrep"))
    kill_tree(1234)
    assert mock_kill.call_count == 2

def test_resolve_mn_cookie_generates_persistent_non_default_cookie(tmp_path, mocker):
    cookie_dir = tmp_path / "state"
    mocker.patch('mn_cli.server_cmds.DIR', cookie_dir)

    cookie = _resolve_mn_cookie()

    assert cookie
    assert cookie != "mirrorneuron"
    assert (cookie_dir / "erlang.cookie").read_text().strip() == cookie
    assert (cookie_dir / "erlang.cookie").stat().st_mode & 0o777 == 0o600
    assert _resolve_mn_cookie() == cookie

def test_resolve_mn_cookie_prefers_non_default_env(monkeypatch):
    monkeypatch.setenv("MN_COOKIE", "operator-provided-cookie")

    assert _resolve_mn_cookie() == "operator-provided-cookie"

def test_resolve_grpc_auth_token_generates_persistent_token(tmp_path, mocker):
    token_dir = tmp_path / "state"
    mocker.patch('mn_cli.server_cmds.DIR', token_dir)

    token = _resolve_grpc_auth_token()

    assert token
    assert (token_dir / "grpc_auth.token").read_text().strip() == token
    assert (token_dir / "grpc_auth.token").stat().st_mode & 0o777 == 0o600
    assert _resolve_grpc_auth_token() == token

def test_resolve_grpc_auth_token_prefers_env(monkeypatch):
    monkeypatch.setenv("MN_GRPC_AUTH_TOKEN", "auth-token")

    assert _resolve_grpc_auth_token() == "auth-token"

def test_runtime_blueprint_env_updates_prefers_host_home_dir(tmp_path):
    host_home = tmp_path / "mn-home"

    updates = _runtime_blueprint_env_updates({"MN_HOST_HOME_DIR": str(host_home)})

    assert updates["MN_HOST_ARTIFACTS_DIR"] == str(host_home / "runs")
    assert updates["MN_RUNS_ROOT"] == str(host_home / "runs")

def test_runtime_blueprint_env_updates_accepts_legacy_host_mn_dir(tmp_path):
    legacy_home = tmp_path / "legacy-mn-home"

    updates = _runtime_blueprint_env_updates({"MN_HOST_MN_DIR": str(legacy_home)})

    assert updates["MN_HOST_ARTIFACTS_DIR"] == str(legacy_home / "runs")
    assert updates["MN_RUNS_ROOT"] == str(legacy_home / "runs")

def test_resolve_grpc_admin_token_generates_persistent_token(tmp_path, mocker):
    token_dir = tmp_path / "state"
    mocker.patch('mn_cli.server_cmds.DIR', token_dir)

    token = _resolve_grpc_admin_token()

    assert token
    assert (token_dir / "grpc_admin.token").read_text().strip() == token
    assert (token_dir / "grpc_admin.token").stat().st_mode & 0o777 == 0o600
    assert _resolve_grpc_admin_token() == token

def test_resolve_grpc_admin_token_prefers_env(monkeypatch):
    monkeypatch.setenv("MN_GRPC_ADMIN_TOKEN", "admin-token")

    assert _resolve_grpc_admin_token() == "admin-token"

def test_resolve_grpc_admin_token_accepts_legacy_env(monkeypatch):
    monkeypatch.setenv("MN_MIRROR_NEURON_GRPC_ADMIN_TOKEN", "legacy-admin-token")

    assert _resolve_grpc_admin_token() == "legacy-admin-token"

def test_start_server_persists_env_grpc_tokens_for_later_cli_process(mocker, monkeypatch):
    monkeypatch.setenv("MN_GRPC_AUTH_TOKEN", "runtime-auth-token")
    monkeypatch.setenv("MN_GRPC_ADMIN_TOKEN", "runtime-admin-token")

    commands = []

    def mock_run(cmd, **kwargs):
        commands.append(cmd)
        m = mocker.Mock()
        m.returncode = 0
        m.stdout = "false\n"
        return m

    mocker.patch('mn_cli.server_cmds.subprocess.run', side_effect=mock_run)
    mocker.patch('mn_cli.server_cmds.time.sleep')
    mocker.patch('mn_cli.server_cmds._detect_host_gpu_count', return_value=0)
    mocker.patch('mn_cli.server_cmds._detect_lan_ip', return_value="127.0.0.1")

    _start_server()

    assert ["docker", "run", "-d", "--name", "mirror-neuron-core"] == next(
        cmd[:5] for cmd in commands if cmd[:3] == ["docker", "run", "-d"]
    )
    assert (server_cmds.DIR / "grpc_auth.token").read_text().strip() == "runtime-auth-token"
    assert (server_cmds.DIR / "grpc_admin.token").read_text().strip() == "runtime-admin-token"
    assert (server_cmds.DIR / "grpc_auth.token").stat().st_mode & 0o777 == 0o600
    assert (server_cmds.DIR / "grpc_admin.token").stat().st_mode & 0o777 == 0o600

def test_start_server_refreshes_token_files_from_compose_runtime_env(mocker, tmp_path):
    compose_file = tmp_path / "docker-compose.yml"
    compose_env = tmp_path / "docker-compose.env"
    compose_file.write_text("services: {}\n")
    compose_env.write_text(
        "COMPOSE_PROJECT_NAME=mirror-neuron\n"
        "MN_DOCKER_NETWORK_MODE=disabled\n"
        "MN_GRPC_AUTH_TOKEN=compose-auth-token\n"
        "MN_MIRROR_NEURON_GRPC_ADMIN_TOKEN=compose-admin-token\n"
    )
    server_cmds.DIR.mkdir(parents=True, exist_ok=True)
    (server_cmds.DIR / "grpc_auth.token").write_text("stale-auth-token\n")
    (server_cmds.DIR / "grpc_admin.token").write_text("stale-admin-token\n")

    mocker.patch('mn_cli.server_cmds.RUNTIME_COMPOSE_FILE', compose_file)
    mocker.patch('mn_cli.server_cmds.RUNTIME_COMPOSE_ENV', compose_env)
    mocker.patch('mn_cli.server_cmds.subprocess.run', return_value=mocker.Mock(returncode=0, stdout="false\n"))
    mocker.patch('mn_cli.server_cmds.time.sleep')
    mocker.patch('mn_cli.server_cmds._detect_host_gpu_count', return_value=0)
    mocker.patch('mn_cli.server_cmds._detect_lan_ip', return_value="192.168.4.99")
    mocker.patch('mn_cli.server_cmds._find_available_published_port', return_value=56379)

    _start_server()

    assert (server_cmds.DIR / "grpc_auth.token").read_text().strip() == "compose-auth-token"
    assert (server_cmds.DIR / "grpc_admin.token").read_text().strip() == "compose-admin-token"
    assert "MN_GRPC_AUTH_TOKEN=compose-auth-token" in compose_env.read_text()
    assert "MN_GRPC_ADMIN_TOKEN=compose-admin-token" in compose_env.read_text()

def test_runtime_grpc_tokens_from_running_container_reads_normal_and_admin_tokens(mocker):
    values = {
        ("mirror-neuron-core", "MN_GRPC_AUTH_TOKEN"): "container-auth-token",
        ("mirror-neuron-core", "MN_GRPC_ADMIN_TOKEN"): "container-admin-token",
    }
    mocker.patch(
        'mn_cli.server_cmds._docker_container_env_value',
        side_effect=lambda container, key: values.get((container, key)),
    )

    assert server_cmds._runtime_grpc_tokens_from_running_container() == {
        "MN_GRPC_AUTH_TOKEN": "container-auth-token",
        "MN_GRPC_ADMIN_TOKEN": "container-admin-token",
    }

def test_resolve_network_token_generates_and_reuses_persistent_token(tmp_path, mocker):
    token_dir = tmp_path / "state"
    mocker.patch('mn_cli.server_cmds.DIR', token_dir)
    mocker.patch('mn_cli.server_cmds.NETWORK_TOKEN_FILE', token_dir / "network.token")
    mocker.patch('mn_cli.server_cmds.secrets.token_urlsafe', return_value="fixed-token")

    token = _resolve_network_token()

    assert token == "fixed-token"
    assert (token_dir / "network.token").read_text().strip() == "fixed-token"
    assert (token_dir / "network.token").stat().st_mode & 0o777 == 0o600
    assert _resolve_network_token() == "fixed-token"

def test_resolve_network_token_does_not_override_persisted_token_from_env(monkeypatch):
    server_cmds.NETWORK_TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    server_cmds.NETWORK_TOKEN_FILE.write_text("persisted-token\n")
    monkeypatch.setenv("MN_NETWORK_JOIN_TOKEN", "env-token")

    assert _resolve_network_token() == "persisted-token"
    assert server_cmds.NETWORK_TOKEN_FILE.read_text().strip() == "persisted-token"

def test_resolve_network_token_seeds_missing_file_from_compose_env(mocker):
    server_cmds.RUNTIME_COMPOSE_ENV.parent.mkdir(parents=True, exist_ok=True)
    server_cmds.RUNTIME_COMPOSE_ENV.write_text("MN_NETWORK_JOIN_TOKEN=compose-token\n")
    mocker.patch('mn_cli.server_cmds.secrets.token_urlsafe', return_value="new-token")

    assert _resolve_network_token() == "compose-token"
    assert server_cmds.NETWORK_TOKEN_FILE.read_text().strip() == "compose-token"

def test_refresh_network_token_rotates_only_when_requested(mocker):
    server_cmds.NETWORK_TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    server_cmds.NETWORK_TOKEN_FILE.write_text("old-token\n")
    server_cmds.RUNTIME_COMPOSE_ENV.parent.mkdir(parents=True, exist_ok=True)
    server_cmds.RUNTIME_COMPOSE_ENV.write_text("MN_NETWORK_JOIN_TOKEN=old-token\n")
    mocker.patch('mn_cli.server_cmds.secrets.token_urlsafe', return_value="new-token")

    assert _refresh_network_token() == "new-token"
    assert server_cmds.NETWORK_TOKEN_FILE.read_text().strip() == "new-token"
    assert "MN_NETWORK_JOIN_TOKEN=new-token" in server_cmds.RUNTIME_COMPOSE_ENV.read_text()

def test_resolve_node_alias_generates_and_reuses_stable_alias(mocker):
    mocker.patch("mn_cli.server_cmds.secrets.token_hex", return_value="a1b2c3d4")

    alias = _resolve_node_alias()

    assert alias == "mn-a1b2c3d4"
    assert (server_cmds.DIR / "node.alias").read_text().strip() == "mn-a1b2c3d4"
    assert _resolve_node_alias() == "mn-a1b2c3d4"

def test_resolve_node_alias_prefers_env_and_persists(monkeypatch):
    monkeypatch.setenv("MN_NODE_ALIAS", "MN-Laptop-01")

    assert _resolve_node_alias() == "mn-laptop-01"
    assert (server_cmds.DIR / "node.alias").read_text().strip() == "mn-laptop-01"

def test_ensure_docker_network_creates_missing_bridge(mocker):
    calls = []

    def mock_run(cmd, **kwargs):
        calls.append(cmd)
        m = mocker.Mock()
        m.returncode = 1 if cmd[:3] == ["docker", "network", "inspect"] else 0
        m.stdout = ""
        return m

    mocker.patch("mn_cli.server_cmds.subprocess.run", side_effect=mock_run)

    _ensure_docker_network("bridge", "mirror-neuron-runtime")

    assert ["docker", "network", "create", "--driver", "bridge", "mirror-neuron-runtime"] in calls

def test_ensure_docker_network_validates_overlay_attachable(mocker):
    output = json.dumps([{"Driver": "overlay", "Attachable": True}])
    mocker.patch(
        "mn_cli.server_cmds.subprocess.run",
        return_value=mocker.Mock(returncode=0, stdout=output),
    )

    _ensure_docker_network("overlay", "mn-overlay")

def test_ensure_docker_network_rejects_missing_overlay(mocker):
    mocker.patch(
        "mn_cli.server_cmds.subprocess.run",
        return_value=mocker.Mock(returncode=1, stdout=""),
    )

    with pytest.raises(typer.Exit) as exc:
        _ensure_docker_network("overlay", "mn-overlay")

    assert exc.value.exit_code == 1

def test_ensure_docker_network_rejects_non_attachable_overlay(mocker):
    output = json.dumps([{"Driver": "overlay", "Attachable": False}])
    mocker.patch(
        "mn_cli.server_cmds.subprocess.run",
        return_value=mocker.Mock(returncode=0, stdout=output),
    )

    with pytest.raises(typer.Exit) as exc:
        _ensure_docker_network("overlay", "mn-overlay")

    assert exc.value.exit_code == 1

def test_start_network_seed_starts_only_core_and_redis(mocker, tmp_path, monkeypatch):
    monkeypatch.delenv("MN_REDIS_URL", raising=False)
    monkeypatch.setenv("MN_NODE_ALIAS", "mn-seed")
    token_file = tmp_path / "network.token"
    mocker.patch('mn_cli.server_cmds.DIR', tmp_path)
    mocker.patch('mn_cli.server_cmds.NETWORK_TOKEN_FILE', token_file)
    mocker.patch('mn_cli.server_cmds.secrets.token_urlsafe', return_value="seed-token")
    mocker.patch('mn_cli.server_cmds._docker_container_running', return_value=False)
    port_available = mocker.patch('mn_cli.server_cmds._port_available_or_owned', return_value=True)
    start_api = mocker.patch('mn_cli.server_cmds._start_api_if_installed')
    start_web_ui = mocker.patch('mn_cli.server_cmds._start_web_ui_if_installed')

    commands = []

    def mock_run(cmd, **kwargs):
        commands.append(cmd)
        m = mocker.Mock()
        m.returncode = 0
        m.stdout = (
            json.dumps([{"Driver": "overlay", "Attachable": True}])
            if cmd[:3] == ["docker", "network", "inspect"]
            else "false\n"
        )
        return m

    mocker.patch('mn_cli.server_cmds.subprocess.run', side_effect=mock_run)

    assert _start_network_seed(
        host="192.168.4.10",
        grpc_port=50055,
        dist_port=4500,
        docker_network_mode="overlay",
    ) == "seed-token"

    assert token_file.read_text().strip() == "seed-token"
    assert any(cmd[:4] == ["docker", "run", "-d", "--name"] and cmd[4] == "mirror-neuron-network-redis" for cmd in commands)
    core_run = next(cmd for cmd in commands if len(cmd) > 4 and cmd[:4] == ["docker", "run", "-d", "--name"] and cmd[4] == "mirror-neuron-network-core")
    assert "mirror-neuron-core:latest" in core_run
    assert "redis:7" not in core_run
    assert core_run.count("-p") == 1
    assert "0.0.0.0:50055:50055" in core_run
    assert "192.168.4.10:4369:4369" not in core_run
    assert "192.168.4.10:4500:4500" not in core_run
    assert f"MN_COOKIE={_derive_network_secret('seed-token', 'cookie')}" in core_run
    assert "MN_NETWORK_ONLY=true" in core_run
    assert "MN_REDIS_FORWARD_PRIMARY=true" in core_run
    assert "MN_NODE_ALIAS=mn-seed" in core_run
    assert "MN_DOCKER_NETWORK_MODE=overlay" in core_run
    assert "MN_DOCKER_NETWORK_NAME=mirror-neuron-runtime" in core_run
    assert "MN_NODE_NAME=mirror_neuron@mn-seed" in core_run
    assert "MN_CLUSTER_NODES=mirror_neuron@mn-seed" in core_run
    assert "--network" in core_run
    assert "mirror-neuron-runtime" in core_run
    assert "--network-alias" in core_run
    assert "mn-seed" in core_run
    assert (
        f"MN_REDIS_URL=redis://:{_derive_network_secret('seed-token', 'redis')}"
        "@mn-seed-redis:6379/0"
    ) in core_run
    assert "MN_NETWORK_REDIS_HOST=mn-seed-redis" in core_run
    redis_run = next(cmd for cmd in commands if len(cmd) > 4 and cmd[:4] == ["docker", "run", "-d", "--name"] and cmd[4] == "mirror-neuron-network-redis")
    assert "--network-alias" in redis_run
    assert "mn-seed-redis" in redis_run
    assert "-p" not in redis_run
    start_api.assert_not_called()
    start_web_ui.assert_not_called()
    port_available.assert_not_called()

def test_start_network_seed_default_disabled_ignores_stale_named_network(mocker, tmp_path, monkeypatch):
    monkeypatch.delenv("MN_REDIS_URL", raising=False)
    monkeypatch.setenv("MN_NODE_ALIAS", "mn-worker")
    token_file = tmp_path / "network.token"
    mocker.patch('mn_cli.server_cmds.DIR', tmp_path)
    mocker.patch('mn_cli.server_cmds.NETWORK_TOKEN_FILE', token_file)
    mocker.patch('mn_cli.server_cmds.NETWORK_REDIS_ENV_FILE', tmp_path / "network-redis.env")
    mocker.patch('mn_cli.server_cmds.secrets.token_urlsafe', return_value="worker-token")
    mocker.patch('mn_cli.server_cmds._docker_container_running', return_value=False)
    mocker.patch('mn_cli.server_cmds._port_available_or_owned', return_value=True)

    commands = []

    def mock_run(cmd, **kwargs):
        commands.append(cmd)
        m = mocker.Mock()
        m.returncode = 0
        m.stdout = (
            json.dumps([{"Driver": "overlay", "Attachable": False}])
            if cmd[:3] == ["docker", "network", "inspect"]
            else "false\n"
        )
        return m

    mocker.patch('mn_cli.server_cmds.subprocess.run', side_effect=mock_run)

    assert _start_network_seed(
        host="192.168.4.173",
        grpc_port=50055,
        dist_port=4500,
    ) == "worker-token"

    assert all(cmd[:3] != ["docker", "network", "inspect"] for cmd in commands)
    assert all(cmd[:3] != ["docker", "network", "create"] for cmd in commands)
    redis_run = next(cmd for cmd in commands if len(cmd) > 4 and cmd[:4] == ["docker", "run", "-d", "--name"] and cmd[4] == "mirror-neuron-network-redis")
    core_run = next(cmd for cmd in commands if len(cmd) > 4 and cmd[:4] == ["docker", "run", "-d", "--name"] and cmd[4] == "mirror-neuron-network-core")
    assert "--network" not in redis_run
    assert "--network" not in core_run
    assert f"0.0.0.0:{server_cmds.REDIS_DYNAMIC_PORT_START}:6379" in redis_run
    assert f"0.0.0.0:{server_cmds.DEFAULT_EPMD_PORT}:{server_cmds.DEFAULT_EPMD_PORT}" in core_run
    assert f"ERL_EPMD_PORT={server_cmds.DEFAULT_EPMD_PORT}" in core_run
    assert "0.0.0.0:4369:4369" not in core_run
    assert (
        f"MN_REDIS_URL=redis://:{_derive_network_secret('worker-token', 'redis')}"
        f"@192.168.4.173:{server_cmds.REDIS_DYNAMIC_PORT_START}/0"
    ) in core_run
    assert "MN_NETWORK_REDIS_HOST=192.168.4.173" in core_run
    assert f"MN_NETWORK_REDIS_PORT={server_cmds.REDIS_DYNAMIC_PORT_START}" in core_run
    assert "MN_DOCKER_NETWORK_MODE=disabled" in core_run
    assert "MN_NODE_NAME=mirror_neuron@192.168.4.173" in core_run

def test_start_network_seed_already_exposed_prints_existing_token(mocker):
    output = StringIO()
    mocker.patch('mn_cli.server_cmds.console', Console(file=output, force_terminal=False, width=120))
    mocker.patch(
        'mn_cli.server_cmds._docker_container_running',
        side_effect=lambda name: name == server_cmds.NETWORK_CORE_CONTAINER,
    )
    mocker.patch(
        'mn_cli.server_cmds._docker_container_env_value',
        side_effect=lambda _name, key: {
            "MN_NETWORK_JOIN_TOKEN": "seed-token",
            "MN_NETWORK_ADVERTISE_HOST": "192.168.4.10",
        }.get(key),
    )
    mock_start_redis = mocker.patch('mn_cli.server_cmds._start_network_redis')
    mock_start_core = mocker.patch('mn_cli.server_cmds._start_network_core')

    token = _start_network_seed(grpc_port=50055, force_new_token=True)

    rendered = output.getvalue()
    assert token == "seed-token"
    assert "already ready to join" in rendered
    assert "Token: seed-token" in rendered
    assert "mn node join 192.168.4.10 --token seed-token" in rendered
    mock_start_redis.assert_not_called()
    mock_start_core.assert_not_called()

def test_start_network_seed_running_local_runtime_prints_existing_token(mocker):
    output = StringIO()
    mocker.patch('mn_cli.server_cmds.console', Console(file=output, force_terminal=False, width=120))
    server_cmds.API_PID_FILE.parent.mkdir(parents=True, exist_ok=True)
    server_cmds.API_PID_FILE.write_text("1234")
    server_cmds.NETWORK_TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    server_cmds.NETWORK_TOKEN_FILE.write_text("runtime-token\n")
    mocker.patch('mn_cli.server_cmds.os.kill')
    mocker.patch('mn_cli.server_cmds._docker_container_env_value', return_value=None)
    mock_start_redis = mocker.patch('mn_cli.server_cmds._start_network_redis')
    mock_start_core = mocker.patch('mn_cli.server_cmds._start_network_core')

    token = _start_network_seed(host="192.168.4.20")

    rendered = output.getvalue()
    assert token == "runtime-token"
    assert "already ready to join" in rendered
    assert "Token: runtime-token" in rendered
    assert "mn node join 192.168.4.20 --token runtime-token" in rendered
    mock_start_redis.assert_not_called()
    mock_start_core.assert_not_called()

def test_start_worker_node_clears_state_and_starts_worker(mocker, tmp_path):
    network_redis_dir = server_cmds.DIR / "network-redis"
    network_redis_dir.mkdir(parents=True)
    (network_redis_dir / "appendonly.aof").write_text("old-state")
    server_cmds.NETWORK_REDIS_ENV_FILE.parent.mkdir(parents=True, exist_ok=True)
    server_cmds.NETWORK_REDIS_ENV_FILE.write_text("MN_REDIS_PORT=56379\n")

    stop_runtime = mocker.patch('mn_cli.server_cmds._stop_local_runtime_for_worker')
    refresh_token = mocker.patch('mn_cli.server_cmds._refresh_network_token', return_value="rotated-token")
    start_seed = mocker.patch('mn_cli.server_cmds._start_network_seed', return_value="worker-token")
    start_api = mocker.patch('mn_cli.server_cmds._start_api_if_installed')
    start_web_ui = mocker.patch('mn_cli.server_cmds._start_web_ui_if_installed')
    mocker.patch('mn_cli.server_cmds.subprocess.run')

    assert _start_worker_node(host="192.168.4.20", grpc_port=50055) == "worker-token"

    stop_runtime.assert_called_once_with()
    refresh_token.assert_called_once_with()
    assert not network_redis_dir.exists()
    assert not server_cmds.NETWORK_REDIS_ENV_FILE.exists()
    start_seed.assert_called_once_with(
        host="192.168.4.20",
        grpc_port=50055,
        dist_port=54370,
        redis_port=None,
        force_new_token=False,
        docker_network_mode=None,
        docker_network_name=None,
        worker_node=True,
    )
    start_api.assert_not_called()
    start_web_ui.assert_not_called()

def test_stop_local_runtime_for_worker_stops_compose_and_sidecars(mocker):
    server_cmds.RUNTIME_COMPOSE_FILE.parent.mkdir(parents=True, exist_ok=True)
    server_cmds.RUNTIME_COMPOSE_FILE.write_text("services: {}\n")
    server_cmds.RUNTIME_COMPOSE_ENV.write_text("COMPOSE_PROJECT_NAME=mirror-neuron\n")
    server_cmds.API_WATCHDOG_PID_FILE.parent.mkdir(parents=True, exist_ok=True)
    server_cmds.API_WATCHDOG_PID_FILE.write_text("1234")
    server_cmds.WEB_UI_PID_FILE.write_text("5678")

    stop_network = mocker.patch('mn_cli.server_cmds._stop_network_runtime')
    run = mocker.patch('mn_cli.server_cmds.subprocess.run')
    mocker.patch('mn_cli.server_cmds.os.kill')
    kill = mocker.patch('mn_cli.server_cmds.kill_tree')

    _stop_local_runtime_for_worker()

    stop_network.assert_called_once_with()
    run.assert_any_call(runtime_compose_cmd("down"), stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
    assert call(1234) in kill.call_args_list
    assert call(5678) in kill.call_args_list
    assert not server_cmds.API_WATCHDOG_PID_FILE.exists()
    assert not server_cmds.WEB_UI_PID_FILE.exists()

def test_add_node_uses_handshake_and_local_core(mocker, tmp_path):
    import mn_sdk
    import mn_cli.shared

    mocker.patch('mn_cli.server_cmds.DIR', tmp_path)
    mocker.patch('mn_cli.server_cmds._docker_container_running', return_value=False)
    mocker.patch('mn_cli.server_cmds._detect_host_gpu_count', return_value=1)
    redis_password = _derive_network_secret("join-token", "redis")

    class StubClient:
        def __init__(self, target, auth_token, timeout):
            assert target == "192.168.4.10:50055"
            assert auth_token == ""
            assert timeout == 10

        def network_handshake(self, token, node_name="", node_info=None):
            assert token == "join-token"
            assert node_name
            assert node_info["node_name"] == node_name
            assert "display_name" in node_info
            return {
                "node_name": "mirror_neuron@192.168.4.10",
                "redis_host": "192.168.4.10",
                "redis_port": 6380,
                "redis_url": f"redis://:{redis_password}@192.168.4.10:6380/0",
            }

    mocker.patch.object(mn_sdk, "Client", StubClient)
    mock_add_node = mocker.patch.object(mn_cli.shared.client, "add_node", return_value="connected")
    mock_run = mocker.patch('mn_cli.server_cmds.subprocess.run')

    _join_network(
        "192.168.4.10",
        "join-token",
        grpc_port=50055,
    )

    mock_run.assert_not_called()
    mock_add_node.assert_called_once_with("mirror_neuron@192.168.4.10", token="join-token")

def test_add_node_overlay_uses_local_alias_in_handshake(mocker, tmp_path, monkeypatch):
    import mn_sdk
    import mn_cli.shared

    monkeypatch.setenv("MN_NODE_ALIAS", "mn-main")
    redis_password = _derive_network_secret("join-token", "redis")
    mocker.patch(
        "mn_cli.server_cmds.subprocess.run",
        return_value=mocker.Mock(
            returncode=0,
            stdout=json.dumps([{"Driver": "overlay", "Attachable": True}]),
        ),
    )

    class StubClient:
        def __init__(self, target, auth_token, timeout):
            assert target == "192.168.4.10:50055"

        def network_handshake(self, token, node_name="", node_info=None):
            assert token == "join-token"
            assert node_name == "mirror_neuron@mn-main"
            assert node_info["node_name"] == "mirror_neuron@mn-main"
            return {
                "node_name": "mirror_neuron@mn-seed",
                "redis_host": "mn-seed-redis",
                "redis_port": 6379,
                "redis_url": f"redis://:{redis_password}@mn-seed-redis:6379/0",
            }

    mocker.patch.object(mn_sdk, "Client", StubClient)
    mock_add_node = mocker.patch.object(mn_cli.shared.client, "add_node", return_value="connected")

    _join_network(
        "192.168.4.10",
        "join-token",
        grpc_port=50055,
        docker_network_mode="overlay",
        docker_network_name="mn-overlay",
    )

    mock_add_node.assert_called_once_with("mirror_neuron@mn-seed", token="join-token")

def test_add_node_bridge_uses_docker_alias_in_handshake(mocker, monkeypatch):
    import mn_sdk
    import mn_cli.shared

    monkeypatch.setenv("MN_NODE_ALIAS", "mn-main")
    redis_password = _derive_network_secret("join-token", "redis")
    mock_run = mocker.patch(
        "mn_cli.server_cmds.subprocess.run",
        return_value=mocker.Mock(
            returncode=0,
            stdout=json.dumps([{"Driver": "bridge", "Attachable": False}]),
        ),
    )

    class StubClient:
        def __init__(self, target, auth_token, timeout):
            assert target == "192.168.4.10:50055"

        def network_handshake(self, token, node_name="", node_info=None):
            assert token == "join-token"
            assert node_name == "mirror_neuron@mn-main"
            assert node_info["node_name"] == "mirror_neuron@mn-main"
            return {
                "node_name": "mirror_neuron@mn-seed",
                "redis_host": "mn-seed-redis",
                "redis_port": 6379,
                "redis_url": f"redis://:{redis_password}@mn-seed-redis:6379/0",
            }

    mocker.patch.object(mn_sdk, "Client", StubClient)
    mock_add_node = mocker.patch.object(mn_cli.shared.client, "add_node", return_value="connected")

    _join_network(
        "192.168.4.10",
        "join-token",
        grpc_port=50055,
        docker_network_mode="bridge",
        docker_network_name="mirror-neuron-runtime",
    )

    mock_run.assert_any_call(
        ["docker", "network", "inspect", "mirror-neuron-runtime"],
        capture_output=True,
        text=True,
    )
    mock_add_node.assert_called_once_with("mirror_neuron@mn-seed", token="join-token")

def test_add_node_rejects_missing_remote_redis_details(mocker, tmp_path):
    import mn_sdk

    mocker.patch('mn_cli.server_cmds.DIR', tmp_path)
    mocker.patch('mn_cli.server_cmds._detect_host_gpu_count', return_value=1)

    class StubClient:
        def __init__(self, target, auth_token, timeout):
            assert target == "192.168.4.10:50055"

        def network_handshake(self, token, node_name="", node_info=None):
            return {"node_name": "mirror_neuron@192.168.4.10"}

    mocker.patch.object(mn_sdk, "Client", StubClient)

    with pytest.raises(typer.Exit) as exc:
        _join_network("192.168.4.10", "join-token", grpc_port=50055)

    assert exc.value.exit_code == 1

def test_add_node_rejects_redis_url_without_token_password(mocker, tmp_path):
    import mn_sdk

    mocker.patch('mn_cli.server_cmds.DIR', tmp_path)
    mocker.patch('mn_cli.server_cmds._detect_host_gpu_count', return_value=1)

    class StubClient:
        def __init__(self, target, auth_token, timeout):
            assert target == "192.168.4.10:50055"

        def network_handshake(self, token, node_name="", node_info=None):
            return {
                "node_name": "mirror_neuron@192.168.4.10",
                "redis_host": "192.168.4.10",
                "redis_port": 6380,
                "redis_url": "redis://192.168.4.10:6380/0",
            }

    mocker.patch.object(mn_sdk, "Client", StubClient)

    with pytest.raises(typer.Exit) as exc:
        _join_network("192.168.4.10", "join-token", grpc_port=50055)

    assert exc.value.exit_code == 1

def test_join_network_configures_worker_redis_replica(mocker, tmp_path):
    import mn_sdk
    import mn_cli.shared

    compose_file = server_cmds.RUNTIME_COMPOSE_FILE
    compose_env = server_cmds.RUNTIME_COMPOSE_ENV
    primary_password = _derive_network_secret("primary-token", "redis")
    worker_password = _derive_network_secret("join-token", "redis")
    compose_file.parent.mkdir(parents=True, exist_ok=True)
    compose_file.write_text("services: {}\n")
    compose_env.write_text(
        "COMPOSE_PROJECT_NAME=mirror-neuron\n"
        "MN_NETWORK_JOIN_TOKEN=primary-token\n"
        "MN_NETWORK_REDIS_HOST=192.168.4.99\n"
        "MN_NETWORK_REDIS_PORT=56379\n"
        f"MN_REDIS_PASSWORD={primary_password}\n"
        f"MN_REDIS_URL=redis://:{primary_password}@redis:6379/0\n"
    )

    class StubClient:
        def __init__(self, target, auth_token, timeout):
            assert target == "192.168.4.20:50055"

        def network_handshake(self, token, node_name="", node_info=None):
            assert token == "join-token"
            return {
                "node_name": "mirror_neuron@192.168.4.20",
                "redis_host": "192.168.4.20",
                "redis_port": 56380,
                "redis_url": f"redis://:{worker_password}@192.168.4.20:56380/0",
            }

    redis_calls = []

    def redis_command(host, port, password, *args):
        redis_calls.append((host, port, password, args))
        return "OK"

    mocker.patch.object(mn_sdk, "Client", StubClient)
    mocker.patch.object(mn_cli.shared.client, "add_node", return_value="connected")
    mocker.patch('mn_cli.server_cmds._redis_command', side_effect=redis_command)
    mocker.patch('mn_cli.server_cmds._detect_host_gpu_count', return_value=0)

    _join_network("192.168.4.20", "join-token", grpc_port=50055)

    assert (
        "192.168.4.20",
        56380,
        worker_password,
        ("CONFIG", "SET", "masterauth", primary_password),
    ) in redis_calls
    assert (
        "192.168.4.20",
        56380,
        worker_password,
        ("REPLICAOF", "192.168.4.99", "56379"),
    ) in redis_calls
    assert (
        "192.168.4.99",
        56379,
        primary_password,
        ("WAIT", "1", "1000"),
    ) in redis_calls

def test_start_server_already_running(mocker, tmp_path):
    mocker.patch('mn_cli.server_cmds.API_PID_FILE', tmp_path / "api.pid")
    (tmp_path / "api.pid").write_text("1234")
    mocker.patch('mn_cli.server_cmds.os.kill') # check_status returns 0

    mock_container_tokens = mocker.patch(
        'mn_cli.server_cmds._runtime_grpc_tokens_from_running_container',
        return_value={
            "MN_GRPC_AUTH_TOKEN": "running-auth-token",
            "MN_GRPC_ADMIN_TOKEN": "running-admin-token",
        },
    )
    mock_start_web = mocker.patch('mn_cli.server_cmds._start_web_ui_if_installed', return_value=True)
    mock_write_endpoints = mocker.patch('mn_cli.server_cmds._write_runtime_endpoints_file', return_value={"api": {}})
    mock_print_endpoints = mocker.patch('mn_cli.server_cmds._print_service_endpoints')

    _start_server()

    mock_container_tokens.assert_called_once()
    assert (server_cmds.DIR / "grpc_auth.token").read_text().strip() == "running-auth-token"
    assert (server_cmds.DIR / "grpc_admin.token").read_text().strip() == "running-admin-token"
    mock_start_web.assert_called_once()
    mock_write_endpoints.assert_called_once()
    mock_print_endpoints.assert_called_once_with(None, True)

def test_start_server_existing_api_starts_missing_compose_core(mocker, tmp_path):
    mocker.patch('mn_cli.server_cmds.API_PID_FILE', tmp_path / "api.pid")
    (tmp_path / "api.pid").write_text("1234")
    mocker.patch('mn_cli.server_cmds.os.kill') # check_status returns 0

    compose_file = server_cmds.RUNTIME_COMPOSE_FILE
    compose_env = server_cmds.RUNTIME_COMPOSE_ENV
    compose_file.parent.mkdir(parents=True, exist_ok=True)
    compose_file.write_text("services: {}\n", encoding="utf-8")
    compose_env.write_text("MN_GRPC_PORT=55051\n", encoding="utf-8")
    mocker.patch('mn_cli.server_cmds._docker_container_running', return_value=False)
    mocker.patch('mn_cli.server_cmds._start_api_if_installed')
    mocker.patch('mn_cli.server_cmds._start_web_ui_if_installed', return_value=False)
    mocker.patch('mn_cli.server_cmds._write_runtime_endpoints_file', return_value={"api": {}})
    mocker.patch('mn_cli.server_cmds._print_service_endpoints')

    calls = []

    def mock_run(cmd, **kwargs):
        calls.append((cmd, kwargs))
        result = mocker.Mock()
        result.stdout = ""
        return result

    mocker.patch('mn_cli.server_cmds.subprocess.run', side_effect=mock_run)

    _start_server(host="192.168.4.20")

    compose_up = next(item for item in calls if item[0] == runtime_compose_cmd("up", "-d"))
    assert compose_up[1]["env"]["ERL_AFLAGS"] == _erl_aflags("54370")

def test_join_still_errors_when_local_api_already_running(mocker, tmp_path):
    mocker.patch('mn_cli.server_cmds.API_PID_FILE', tmp_path / "api.pid")
    (tmp_path / "api.pid").write_text("1234")
    mocker.patch('mn_cli.server_cmds.os.kill') # check_status returns 0

    with pytest.raises(typer.Exit) as exc:
        _start_server(ip="192.168.4.10", token="join-token")
    assert exc.value.exit_code == 1

def test_start_server_docker_running(mocker, tmp_path):
    mocker.patch('mn_cli.server_cmds.API_PID_FILE', tmp_path / "api.pid") # Missing, check_status -> 2
    
    mock_run = mocker.patch('mn_cli.server_cmds.subprocess.run')
    mock_run.return_value.stdout = "true\n"
    
    with pytest.raises(typer.Exit) as exc:
        _start_server()
    assert exc.value.exit_code == 1

def test_start_server_no_docker(mocker, tmp_path):
    mocker.patch('mn_cli.server_cmds.API_PID_FILE', tmp_path / "api.pid")
    mocker.patch('mn_cli.server_cmds.subprocess.run', side_effect=FileNotFoundError)
    
    with pytest.raises(typer.Exit) as exc:
        _start_server()
    assert exc.value.exit_code == 1

def test_start_server_docker_start_fails(mocker, tmp_path):
    mocker.patch('mn_cli.server_cmds.API_PID_FILE', tmp_path / "api.pid")
    
    def mock_run(cmd, **kwargs):
        if cmd[1] == "inspect":
            m = mocker.Mock()
            m.stdout = "false\n"
            return m
        elif cmd[1] == "run":
            raise subprocess.CalledProcessError(1, "docker run")
        return mocker.Mock()
        
    mocker.patch('mn_cli.server_cmds.subprocess.run', side_effect=mock_run)
    mocker.patch('mn_cli.server_cmds.PID_DIR', tmp_path / ".pids")
    mocker.patch('mn_cli.server_cmds.LOG_DIR', tmp_path / ".logs")
    
    with pytest.raises(typer.Exit) as exc:
        _start_server()
    assert exc.value.exit_code == 1

def test_runtime_compose_cmd_uses_installed_runtime_files(mocker, tmp_path):
    compose_file = tmp_path / "docker-compose.yml"
    compose_env = tmp_path / "docker-compose.env"
    mocker.patch('mn_cli.server_cmds.RUNTIME_COMPOSE_FILE', compose_file)
    mocker.patch('mn_cli.server_cmds.RUNTIME_COMPOSE_ENV', compose_env)

    assert runtime_compose_cmd("up", "-d") == [
        "docker",
        "compose",
        "--env-file",
        str(compose_env),
        "-f",
        str(compose_file),
        "up",
        "-d",
    ]

def test_compose_internal_redis_settings_persists_docker_alias(mocker, tmp_path):
    compose_env = tmp_path / "docker-compose.env"
    compose_env.write_text("COMPOSE_PROJECT_NAME=mirror-neuron\n")
    mocker.patch('mn_cli.server_cmds.RUNTIME_COMPOSE_ENV', compose_env)
    port_available = mocker.patch('mn_cli.server_cmds._port_available_or_owned')

    env = server_cmds._ensure_compose_internal_redis_settings(
        {},
        token="join-token",
        network_redis_host="mn-node-redis",
        network_redis_port=6379,
    )

    redis_password = _derive_network_secret("join-token", "redis")
    assert env["MN_REDIS_PASSWORD"] == redis_password
    assert env["MN_REDIS_URL"] == f"redis://:{redis_password}@redis:6379/0"
    assert env["MN_NETWORK_REDIS_HOST"] == "mn-node-redis"
    assert env["MN_NETWORK_REDIS_PORT"] == "6379"
    compose_env_text = compose_env.read_text()
    assert "MN_REDIS_BIND_HOST=" not in compose_env_text
    assert "MN_REDIS_PORT=" not in compose_env_text
    assert f"MN_REDIS_PASSWORD={redis_password}" in compose_env_text
    assert "MN_NETWORK_REDIS_HOST=mn-node-redis" in compose_env_text
    assert "MN_NETWORK_REDIS_PORT=6379" in compose_env_text
    port_available.assert_not_called()

def test_compose_internal_redis_settings_defaults_to_service_name(mocker, tmp_path):
    compose_env = tmp_path / "docker-compose.env"
    compose_env.write_text("COMPOSE_PROJECT_NAME=mirror-neuron\n")
    mocker.patch('mn_cli.server_cmds.RUNTIME_COMPOSE_ENV', compose_env)

    env = server_cmds._ensure_compose_internal_redis_settings(
        {},
        token="join-token",
    )

    assert env["MN_NETWORK_REDIS_HOST"] == "redis"
    assert env["MN_NETWORK_REDIS_PORT"] == "6379"

def test_compose_cluster_port_settings_treats_persisted_redis_port_as_preference(mocker, tmp_path):
    compose_env = tmp_path / "docker-compose.env"
    compose_env.write_text("COMPOSE_PROJECT_NAME=mirror-neuron\nMN_REDIS_PORT=56379\n")
    mocker.patch('mn_cli.server_cmds.RUNTIME_COMPOSE_ENV', compose_env)
    find_port = mocker.patch('mn_cli.server_cmds._find_available_published_port', return_value=56380)

    env = server_cmds._ensure_compose_cluster_port_settings(
        {"MN_REDIS_PORT": "56379"},
        token="join-token",
        advertised_host="192.168.4.20",
    )

    assert env["MN_REDIS_PORT"] == "56380"
    find_port.assert_called_once_with("0.0.0.0", 56379, server_cmds.COMPOSE_REDIS_CONTAINER, 6379)
    assert "MN_REDIS_PORT=56380" in compose_env.read_text()

def test_compose_native_settings_persists_runtime_blueprint_env(mocker, tmp_path):
    compose_env = tmp_path / "docker-compose.env"
    compose_env.write_text("COMPOSE_PROJECT_NAME=mirror-neuron\n")
    mocker.patch('mn_cli.server_cmds.RUNTIME_COMPOSE_ENV', compose_env)

    env = server_cmds._ensure_compose_native_port_settings(
        {
            "MN_BLUEPRINT_REPO": "/opt/mn/blueprints",
            "MN_DEV_LOCAL_BLUEPRINT_REPO": "/work/mn/otterdesk-blueprints",
            "MN_RUNS_ROOT": "/opt/mn/runs",
        }
    )

    assert env["MN_BLUEPRINT_REPO"] == "/opt/mn/blueprints"
    assert env["MN_DEV_LOCAL_BLUEPRINT_REPO"] == "/work/mn/otterdesk-blueprints"
    assert env["MN_RUNS_ROOT"] == "/opt/mn/runs"
    assert env["MN_HOST_ARTIFACTS_DIR"] == "/opt/mn/runs"
    assert env["MN_CONTAINER_RUNS_ROOT"] == "/root/.mn/runs"
    assert env["MN_BLUEPRINT_WEB_UI_BIND_HOST"] == "0.0.0.0"
    assert env["MN_BLUEPRINT_WEB_UI_PUBLIC_HOST"] == "localhost"
    assert env["MN_BLUEPRINT_WEB_UI_PORT_START"] == "61000"
    assert env["MN_BLUEPRINT_WEB_UI_PORT_END"] == "61049"
    assert env["MN_BLUEPRINT_WEB_UI_PORT_ALLOCATION_MODE"] == "prepublished"
    compose_env_text = compose_env.read_text()
    assert "MN_BLUEPRINT_REPO=/opt/mn/blueprints" in compose_env_text
    assert "MN_DEV_LOCAL_BLUEPRINT_REPO=/work/mn/otterdesk-blueprints" in compose_env_text
    assert "MN_HOST_ARTIFACTS_DIR=/opt/mn/runs" in compose_env_text
    assert "MN_RUNS_ROOT=/opt/mn/runs" in compose_env_text
    assert "MN_CONTAINER_RUNS_ROOT=/root/.mn/runs" in compose_env_text
    assert "MN_BLUEPRINT_WEB_UI_BIND_HOST=0.0.0.0" in compose_env_text
    assert "MN_BLUEPRINT_WEB_UI_PUBLIC_HOST=localhost" in compose_env_text
    assert "MN_BLUEPRINT_WEB_UI_PORT_START=61000" in compose_env_text
    assert "MN_BLUEPRINT_WEB_UI_PORT_END=61049" in compose_env_text
    assert "MN_BLUEPRINT_WEB_UI_PORT_ALLOCATION_MODE=prepublished" in compose_env_text

def test_compose_native_settings_defaults_runtime_blueprint_repo(mocker, tmp_path):
    compose_env = tmp_path / "docker-compose.env"
    compose_env.write_text("COMPOSE_PROJECT_NAME=mirror-neuron\nMN_BLUEPRINT_REPO=\n")
    mocker.patch('mn_cli.server_cmds.RUNTIME_COMPOSE_ENV', compose_env)

    env = server_cmds._ensure_compose_native_port_settings({})

    assert env["MN_DEFAULT_BLUEPRINT_REPO"] == "https://github.com/MirrorNeuronLab/mn-blueprints.git"
    assert env["MN_BLUEPRINT_REPO"] == "https://github.com/MirrorNeuronLab/mn-blueprints.git"
    assert env["MN_HOST_ARTIFACTS_DIR"].endswith(".mirror_neuron/runs")
    assert env["MN_RUNS_ROOT"].endswith(".mirror_neuron/runs")
    assert env["MN_CONTAINER_RUNS_ROOT"] == "/root/.mn/runs"
    assert env["MN_BLUEPRINT_WEB_UI_PORT_START"] == "61000"
    assert env["MN_BLUEPRINT_WEB_UI_PORT_END"] == "61049"
    assert env["MN_BLUEPRINT_WEB_UI_PORT_ALLOCATION_MODE"] == "prepublished"
    compose_env_text = compose_env.read_text()
    assert "MN_DEFAULT_BLUEPRINT_REPO=https://github.com/MirrorNeuronLab/mn-blueprints.git" in compose_env_text
    assert "MN_BLUEPRINT_REPO=https://github.com/MirrorNeuronLab/mn-blueprints.git" in compose_env_text
    assert "MN_HOST_ARTIFACTS_DIR=" in compose_env_text
    assert "MN_RUNS_ROOT=" in compose_env_text
    assert "MN_CONTAINER_RUNS_ROOT=/root/.mn/runs" in compose_env_text
    assert "MN_BLUEPRINT_WEB_UI_PORT_START=61000" in compose_env_text
    assert "MN_BLUEPRINT_WEB_UI_PORT_END=61049" in compose_env_text
    assert "MN_BLUEPRINT_WEB_UI_PORT_ALLOCATION_MODE=prepublished" in compose_env_text

def test_compose_cluster_bind_settings_exposes_lan_advertised_host(mocker, tmp_path, monkeypatch):
    for name in ("MN_GRPC_BIND_HOST", "MN_EPMD_BIND_HOST", "MN_DIST_BIND_HOST", "ERL_EPMD_ADDRESS"):
        monkeypatch.delenv(name, raising=False)
    compose_env = tmp_path / "docker-compose.env"
    compose_env.write_text(
        "COMPOSE_PROJECT_NAME=mirror-neuron\n"
        "MN_GRPC_BIND_HOST=127.0.0.1\n"
        "MN_EPMD_BIND_HOST=127.0.0.1\n"
        "MN_DIST_BIND_HOST=127.0.0.1\n"
        "ERL_EPMD_ADDRESS=127.0.0.1\n"
    )
    mocker.patch('mn_cli.server_cmds.RUNTIME_COMPOSE_ENV', compose_env)

    env = server_cmds._ensure_compose_cluster_bind_settings(
        {
            "MN_GRPC_BIND_HOST": "127.0.0.1",
            "MN_EPMD_BIND_HOST": "127.0.0.1",
            "MN_DIST_BIND_HOST": "127.0.0.1",
            "ERL_EPMD_ADDRESS": "127.0.0.1",
        },
        "192.168.4.173",
    )

    assert env["MN_GRPC_BIND_HOST"] == "0.0.0.0"
    assert env["MN_EPMD_BIND_HOST"] == "127.0.0.1"
    assert env["MN_DIST_BIND_HOST"] == "127.0.0.1"
    assert env["ERL_EPMD_ADDRESS"] == "127.0.0.1"
    compose_env_text = compose_env.read_text()
    assert "MN_GRPC_BIND_HOST=0.0.0.0" in compose_env_text
    assert "MN_EPMD_BIND_HOST=0.0.0.0" not in compose_env_text
    assert "MN_DIST_BIND_HOST=0.0.0.0" not in compose_env_text
    assert "ERL_EPMD_ADDRESS=0.0.0.0" not in compose_env_text

def test_compose_cluster_bind_settings_preserves_explicit_env(mocker, tmp_path, monkeypatch):
    monkeypatch.setenv("MN_GRPC_BIND_HOST", "127.0.0.1")
    for name in ("MN_EPMD_BIND_HOST", "MN_DIST_BIND_HOST", "ERL_EPMD_ADDRESS"):
        monkeypatch.delenv(name, raising=False)
    compose_env = tmp_path / "docker-compose.env"
    compose_env.write_text(
        "COMPOSE_PROJECT_NAME=mirror-neuron\n"
        "MN_GRPC_BIND_HOST=127.0.0.1\n"
        "MN_EPMD_BIND_HOST=127.0.0.1\n"
        "MN_DIST_BIND_HOST=127.0.0.1\n"
    )
    mocker.patch('mn_cli.server_cmds.RUNTIME_COMPOSE_ENV', compose_env)

    env = server_cmds._ensure_compose_cluster_bind_settings(
        {
            "MN_GRPC_BIND_HOST": "127.0.0.1",
            "MN_EPMD_BIND_HOST": "127.0.0.1",
            "MN_DIST_BIND_HOST": "127.0.0.1",
        },
        "192.168.4.173",
    )

    assert env["MN_GRPC_BIND_HOST"] == "127.0.0.1"
    assert env["MN_EPMD_BIND_HOST"] == "127.0.0.1"
    assert env["MN_DIST_BIND_HOST"] == "127.0.0.1"

def test_start_server_uses_compose_runtime_when_available(mocker, tmp_path):
    compose_file = tmp_path / "docker-compose.yml"
    compose_env = tmp_path / "docker-compose.env"
    compose_file.write_text("services: {}\n")
    compose_env.write_text(
        "COMPOSE_PROJECT_NAME=mirror-neuron\n"
        "MN_DOCKER_NETWORK_MODE=bridge\n"
        "MN_DOCKER_NETWORK_NAME=mirror-neuron-runtime\n"
        "MN_NODE_NAME=\n"
        "MN_CLUSTER_NODES=nonode@nohost\n"
    )

    mocker.patch('mn_cli.server_cmds.RUNTIME_COMPOSE_FILE', compose_file)
    mocker.patch('mn_cli.server_cmds.RUNTIME_COMPOSE_ENV', compose_env)
    mocker.patch('mn_cli.server_cmds.API_PID_FILE', tmp_path / "api.pid")
    mocker.patch('mn_cli.server_cmds.API_WATCHDOG_PID_FILE', tmp_path / "api-watchdog.pid")
    mocker.patch('mn_cli.server_cmds.WEB_UI_DIRS', ())
    mocker.patch('mn_cli.server_cmds.time.sleep')
    mocker.patch('mn_cli.server_cmds.PID_DIR', tmp_path / ".pids")
    mocker.patch('mn_cli.server_cmds.LOG_DIR', tmp_path / ".logs")
    mocker.patch('mn_cli.server_cmds.BEAM_LOG', tmp_path / "beam.log")
    mocker.patch('mn_cli.server_cmds.API_LOG', tmp_path / "api.log")
    mocker.patch('mn_cli.server_cmds.API_WATCHDOG_LOG', tmp_path / "api-watchdog.log")
    mocker.patch('mn_cli.server_cmds._wait_for_api', return_value=True)
    mocker.patch(
        'mn_cli.server_cmds._handshake_with_main_node',
        return_value={
            "node_name": "mirror_neuron@127.0.0.1",
            "redis_host": "127.0.0.1",
            "redis_port": 6379,
            "redis_url": "redis://127.0.0.1:6379/0",
        },
    )
    mocker.patch('mn_cli.server_cmds.VENV_DIR', tmp_path)
    mocker.patch('mn_cli.server_cmds._detect_lan_ip', return_value="192.168.4.99")
    mocker.patch("mn_cli.server_cmds.secrets.token_hex", return_value="abc12345")

    calls = []

    def mock_run(cmd, **kwargs):
        calls.append((cmd, kwargs))
        m = mocker.Mock()
        if cmd[:3] == ["docker", "network", "inspect"]:
            m.returncode = 1
            m.stdout = ""
        else:
            m.returncode = 0
            m.stdout = "false\n"
        return m

    mocker.patch('mn_cli.server_cmds.subprocess.run', side_effect=mock_run)
    mocker.patch('mn_cli.server_cmds._find_available_published_port', return_value=56379)

    _start_server()

    commands = [call[0] for call in calls]
    assert runtime_compose_cmd("up", "-d") in commands
    assert all(cmd[:3] != ["docker", "network", "inspect"] for cmd in commands)
    assert ["docker", "network", "create", "--driver", "bridge", "mirror-neuron-runtime"] not in commands
    assert all(cmd[:3] != ["docker", "run", "-d"] for cmd in commands)
    compose_call = next(item for item in calls if item[0] == runtime_compose_cmd("up", "-d"))
    env = compose_call[1]["env"]
    assert env["MN_DOCKER_NETWORK_MODE"] == "disabled"
    assert env["MN_NODE_NAME"] == "mirror_neuron@192.168.4.99"
    assert env["MN_CLUSTER_NODES"] == "mirror_neuron@192.168.4.99"
    assert env["MN_NETWORK_REDIS_HOST"] == "192.168.4.99"
    assert env["MN_NETWORK_REDIS_PORT"] == "56379"
    assert env["MN_REDIS_BIND_HOST"] == "0.0.0.0"
    assert env["MN_REDIS_PORT"] == "56379"
    assert env["MN_COOKIE"] == _derive_network_secret(env["MN_NETWORK_JOIN_TOKEN"], "cookie")
    compose_env_text = compose_env.read_text()
    assert "MN_NETWORK_ADVERTISE_HOST=192.168.4.99" in compose_env_text
    assert "MN_DOCKER_NETWORK_MODE=disabled" in compose_env_text
    assert "MN_NODE_NAME=mirror_neuron@192.168.4.99" in compose_env_text
    assert "MN_CLUSTER_NODES=mirror_neuron@192.168.4.99" in compose_env_text
    assert "MN_NETWORK_REDIS_HOST=192.168.4.99" in compose_env_text
    assert "MN_NETWORK_REDIS_PORT=56379" in compose_env_text
    assert "MN_GRPC_AUTH_TOKEN=" in compose_env_text
    assert "MN_GRPC_ADMIN_TOKEN=" in compose_env_text
    assert "MN_COOKIE=" in compose_env_text
    assert "MN_HOST_ARTIFACTS_DIR=" in compose_env_text
    assert "MN_RUNS_ROOT=" in compose_env_text
    assert "MN_CONTAINER_RUNS_ROOT=/root/.mn/runs" in compose_env_text
    assert "mirror-neuron-core:" in (server_cmds.DIR / server_cmds.RUNTIME_CLUSTER_OVERRIDE_FILE).read_text()

def test_start_server_passes_cluster_env_to_compose_runtime(mocker, tmp_path):
    compose_file = tmp_path / "docker-compose.yml"
    compose_env = tmp_path / "docker-compose.env"
    compose_file.write_text("services: {}\n")
    compose_env.write_text("COMPOSE_PROJECT_NAME=mirror-neuron\n")

    mocker.patch('mn_cli.server_cmds.RUNTIME_COMPOSE_FILE', compose_file)
    mocker.patch('mn_cli.server_cmds.RUNTIME_COMPOSE_ENV', compose_env)
    mocker.patch('mn_cli.server_cmds.API_PID_FILE', tmp_path / "api.pid")
    mocker.patch('mn_cli.server_cmds.WEB_UI_DIRS', ())
    mocker.patch('mn_cli.server_cmds.time.sleep')
    mocker.patch('mn_cli.server_cmds.PID_DIR', tmp_path / ".pids")
    mocker.patch('mn_cli.server_cmds.LOG_DIR', tmp_path / ".logs")
    mocker.patch('mn_cli.server_cmds.BEAM_LOG', tmp_path / "beam.log")
    mocker.patch('mn_cli.server_cmds.API_LOG', tmp_path / "api.log")
    mocker.patch('mn_cli.server_cmds.VENV_DIR', tmp_path)
    mocker.patch('mn_cli.server_cmds._detect_lan_ip', return_value="192.168.4.99")
    redis_password = _derive_network_secret("join-token", "redis")
    mocker.patch(
        'mn_cli.server_cmds._handshake_with_main_node',
        return_value={
            "node_name": "mirror_neuron@192.168.4.173",
            "redis_host": "192.168.4.173",
            "redis_port": 6380,
            "redis_url": f"redis://:{redis_password}@192.168.4.173:6380/0",
        },
    )

    calls = []

    def mock_run(cmd, **kwargs):
        calls.append((cmd, kwargs))
        m = mocker.Mock()
        m.stdout = "false\n"
        return m

    mocker.patch('mn_cli.server_cmds.subprocess.run', side_effect=mock_run)
    mocker.patch('mn_cli.server_cmds._find_available_published_port', return_value=56379)

    _start_server(ip="192.168.4.173", token="join-token")

    compose_call = next(item for item in calls if item[0] == runtime_compose_cmd("up", "-d"))
    env = compose_call[1]["env"]
    assert env["MN_NODE_NAME"] == "mirror_neuron@192.168.4.99"
    assert env["MN_CLUSTER_NODES"] == "mirror_neuron@192.168.4.173"
    assert env["MN_REDIS_URL"] == f"redis://:{redis_password}@192.168.4.173:6380/0"
    assert env["MN_CONTEXT_REDIS_URL"] == f"redis://:{redis_password}@192.168.4.173:6380/1"
    assert env["MN_NETWORK_JOIN_TOKEN"] == "join-token"
    assert env["MN_COOKIE"] == _derive_network_secret("join-token", "cookie")
    assert env["MN_DIST_PORT"] == "54370"
    assert env["ERL_AFLAGS"] == _erl_aflags("54370")

def test_start_server_preserves_persisted_join_profile_on_restart(mocker, tmp_path):
    compose_file = tmp_path / "docker-compose.yml"
    compose_env = tmp_path / "docker-compose.env"
    redis_password = _derive_network_secret("join-token", "redis")
    compose_file.write_text("services: {}\n")
    compose_env.write_text(
        "COMPOSE_PROJECT_NAME=mirror-neuron\n"
        "MN_NETWORK_JOIN_TOKEN=join-token\n"
        "MN_NODE_NAME=mirror_neuron@192.168.4.173\n"
        "MN_CLUSTER_NODES=mirror_neuron@192.168.4.35\n"
        "MN_NETWORK_REDIS_HOST=192.168.4.35\n"
        "MN_NETWORK_REDIS_PORT=56381\n"
        f"MN_REDIS_URL=redis://:{redis_password}@192.168.4.35:56381/0\n"
        f"MN_CONTEXT_REDIS_URL=redis://:{redis_password}@192.168.4.35:56381/1\n"
    )

    mocker.patch('mn_cli.server_cmds.RUNTIME_COMPOSE_FILE', compose_file)
    mocker.patch('mn_cli.server_cmds.RUNTIME_COMPOSE_ENV', compose_env)
    mocker.patch('mn_cli.server_cmds.API_PID_FILE', tmp_path / "api.pid")
    mocker.patch('mn_cli.server_cmds.WEB_UI_DIRS', ())
    mocker.patch('mn_cli.server_cmds.time.sleep')
    mocker.patch('mn_cli.server_cmds.PID_DIR', tmp_path / ".pids")
    mocker.patch('mn_cli.server_cmds.LOG_DIR', tmp_path / ".logs")
    mocker.patch('mn_cli.server_cmds.BEAM_LOG', tmp_path / "beam.log")
    mocker.patch('mn_cli.server_cmds.API_LOG', tmp_path / "api.log")
    mocker.patch('mn_cli.server_cmds.VENV_DIR', tmp_path)
    mocker.patch('mn_cli.server_cmds._detect_lan_ip', return_value="192.168.4.173")

    calls = []

    def mock_run(cmd, **kwargs):
        calls.append((cmd, kwargs))
        m = mocker.Mock()
        m.stdout = "false\n"
        return m

    mocker.patch('mn_cli.server_cmds.subprocess.run', side_effect=mock_run)
    ensure_redis = mocker.patch('mn_cli.server_cmds._ensure_compose_internal_redis_settings')

    _start_server()

    compose_call = next(item for item in calls if item[0] == runtime_compose_cmd("up", "-d"))
    env = compose_call[1]["env"]
    assert env["MN_NETWORK_JOIN_TOKEN"] == "join-token"
    assert env["MN_NODE_NAME"] == "mirror_neuron@192.168.4.173"
    assert env["MN_CLUSTER_NODES"] == "mirror_neuron@192.168.4.35"
    assert env["MN_NETWORK_REDIS_HOST"] == "192.168.4.35"
    assert env["MN_NETWORK_REDIS_PORT"] == "56381"
    assert env["MN_REDIS_URL"] == f"redis://:{redis_password}@192.168.4.35:56381/0"
    ensure_redis.assert_not_called()

def test_start_server_refreshes_generated_node_name_for_joined_runtime_ip_change(mocker, tmp_path):
    compose_file = tmp_path / "docker-compose.yml"
    compose_env = tmp_path / "docker-compose.env"
    redis_password = _derive_network_secret("join-token", "redis")
    compose_file.write_text("services: {}\n")
    compose_env.write_text(
        "COMPOSE_PROJECT_NAME=mirror-neuron\n"
        "MN_NETWORK_JOIN_TOKEN=join-token\n"
        "MN_NETWORK_ADVERTISE_HOST=192.168.4.173\n"
        "MN_NODE_NAME=mirror_neuron@192.168.4.173\n"
        "MN_CLUSTER_NODES=mirror_neuron@192.168.4.35\n"
        "MN_NETWORK_REDIS_HOST=192.168.4.35\n"
        "MN_NETWORK_REDIS_PORT=56381\n"
        f"MN_REDIS_URL=redis://:{redis_password}@192.168.4.35:56381/0\n"
        f"MN_CONTEXT_REDIS_URL=redis://:{redis_password}@192.168.4.35:56381/1\n"
    )

    mocker.patch('mn_cli.server_cmds.RUNTIME_COMPOSE_FILE', compose_file)
    mocker.patch('mn_cli.server_cmds.RUNTIME_COMPOSE_ENV', compose_env)
    mocker.patch('mn_cli.server_cmds.API_PID_FILE', tmp_path / "api.pid")
    mocker.patch('mn_cli.server_cmds.WEB_UI_DIRS', ())
    mocker.patch('mn_cli.server_cmds.time.sleep')
    mocker.patch('mn_cli.server_cmds.PID_DIR', tmp_path / ".pids")
    mocker.patch('mn_cli.server_cmds.LOG_DIR', tmp_path / ".logs")
    mocker.patch('mn_cli.server_cmds.BEAM_LOG', tmp_path / "beam.log")
    mocker.patch('mn_cli.server_cmds.API_LOG', tmp_path / "api.log")
    mocker.patch('mn_cli.server_cmds.VENV_DIR', tmp_path)
    mocker.patch('mn_cli.server_cmds._detect_lan_ip', return_value="192.168.4.44")
    handshake = mocker.patch('mn_cli.server_cmds._handshake_with_main_node')

    calls = []

    def mock_run(cmd, **kwargs):
        calls.append((cmd, kwargs))
        m = mocker.Mock()
        m.stdout = "false\n"
        return m

    mocker.patch('mn_cli.server_cmds.subprocess.run', side_effect=mock_run)
    ensure_redis = mocker.patch('mn_cli.server_cmds._ensure_compose_internal_redis_settings')

    _start_server()

    compose_call = next(item for item in calls if item[0] == runtime_compose_cmd("up", "-d"))
    env = compose_call[1]["env"]
    assert env["MN_NETWORK_ADVERTISE_HOST"] == "192.168.4.44"
    assert env["MN_NODE_NAME"] == "mirror_neuron@192.168.4.44"
    assert env["MN_CLUSTER_NODES"] == "mirror_neuron@192.168.4.35"
    assert env["MN_NETWORK_REDIS_HOST"] == "192.168.4.35"
    assert env["MN_NETWORK_REDIS_PORT"] == "56381"
    assert env["MN_REDIS_URL"] == f"redis://:{redis_password}@192.168.4.35:56381/0"
    assert env["MN_CONTEXT_REDIS_URL"] == f"redis://:{redis_password}@192.168.4.35:56381/1"

    compose_env_text = compose_env.read_text()
    assert "MN_NETWORK_ADVERTISE_HOST=192.168.4.44" in compose_env_text
    assert "MN_NODE_NAME=mirror_neuron@192.168.4.44" in compose_env_text
    assert "MN_CLUSTER_NODES=mirror_neuron@192.168.4.35" in compose_env_text
    assert "MN_NETWORK_REDIS_HOST=192.168.4.35" in compose_env_text
    assert "MN_NETWORK_REDIS_PORT=56381" in compose_env_text
    handshake.assert_not_called()
    ensure_redis.assert_not_called()

def test_start_server_refreshes_generated_node_name_for_joined_runtime_explicit_host_change(mocker, tmp_path):
    compose_file = tmp_path / "docker-compose.yml"
    compose_env = tmp_path / "docker-compose.env"
    redis_password = _derive_network_secret("join-token", "redis")
    compose_file.write_text("services: {}\n")
    compose_env.write_text(
        "COMPOSE_PROJECT_NAME=mirror-neuron\n"
        "MN_NETWORK_JOIN_TOKEN=join-token\n"
        "MN_NETWORK_ADVERTISE_HOST=192.168.4.173\n"
        "MN_NODE_NAME=mirror_neuron@192.168.4.173\n"
        "MN_CLUSTER_NODES=mirror_neuron@192.168.4.35\n"
        "MN_NETWORK_REDIS_HOST=192.168.4.35\n"
        "MN_NETWORK_REDIS_PORT=56381\n"
        f"MN_REDIS_URL=redis://:{redis_password}@192.168.4.35:56381/0\n"
        f"MN_CONTEXT_REDIS_URL=redis://:{redis_password}@192.168.4.35:56381/1\n"
    )

    mocker.patch('mn_cli.server_cmds.RUNTIME_COMPOSE_FILE', compose_file)
    mocker.patch('mn_cli.server_cmds.RUNTIME_COMPOSE_ENV', compose_env)
    mocker.patch('mn_cli.server_cmds.API_PID_FILE', tmp_path / "api.pid")
    mocker.patch('mn_cli.server_cmds.WEB_UI_DIRS', ())
    mocker.patch('mn_cli.server_cmds.time.sleep')
    mocker.patch('mn_cli.server_cmds.PID_DIR', tmp_path / ".pids")
    mocker.patch('mn_cli.server_cmds.LOG_DIR', tmp_path / ".logs")
    mocker.patch('mn_cli.server_cmds.BEAM_LOG', tmp_path / "beam.log")
    mocker.patch('mn_cli.server_cmds.API_LOG', tmp_path / "api.log")
    mocker.patch('mn_cli.server_cmds.VENV_DIR', tmp_path)
    mocker.patch('mn_cli.server_cmds._detect_lan_ip', return_value="192.168.4.44")
    handshake = mocker.patch('mn_cli.server_cmds._handshake_with_main_node')

    calls = []

    def mock_run(cmd, **kwargs):
        calls.append((cmd, kwargs))
        m = mocker.Mock()
        m.stdout = "false\n"
        return m

    mocker.patch('mn_cli.server_cmds.subprocess.run', side_effect=mock_run)
    ensure_redis = mocker.patch('mn_cli.server_cmds._ensure_compose_internal_redis_settings')

    _start_server(host="gb10-worker.local")

    compose_call = next(item for item in calls if item[0] == runtime_compose_cmd("up", "-d"))
    env = compose_call[1]["env"]
    assert env["MN_NETWORK_ADVERTISE_HOST"] == "gb10-worker.local"
    assert env["MN_NODE_NAME"] == "mirror_neuron@gb10-worker.local"
    assert env["MN_CLUSTER_NODES"] == "mirror_neuron@192.168.4.35"
    assert env["MN_NETWORK_REDIS_HOST"] == "192.168.4.35"
    assert env["MN_NETWORK_REDIS_PORT"] == "56381"
    assert env["MN_REDIS_URL"] == f"redis://:{redis_password}@192.168.4.35:56381/0"
    assert env["MN_CONTEXT_REDIS_URL"] == f"redis://:{redis_password}@192.168.4.35:56381/1"

    compose_env_text = compose_env.read_text()
    assert "MN_NETWORK_ADVERTISE_HOST=gb10-worker.local" in compose_env_text
    assert "MN_NODE_NAME=mirror_neuron@gb10-worker.local" in compose_env_text
    assert "MN_CLUSTER_NODES=mirror_neuron@192.168.4.35" in compose_env_text
    assert "MN_NETWORK_REDIS_HOST=192.168.4.35" in compose_env_text
    assert "MN_NETWORK_REDIS_PORT=56381" in compose_env_text
    handshake.assert_not_called()
    ensure_redis.assert_not_called()

def test_compose_runtime_env_respects_explicit_cluster_names(monkeypatch):
    env = {
        "MN_NODE_NAME": "mn2@192.168.4.173",
        "MN_CLUSTER_NODES": "mn1@192.168.4.10,mn2@192.168.4.173",
        "MN_REDIS_URL": "redis://192.168.4.10:6379/0",
        "MN_DIST_PORT": "4500",
    }

    resolved = _compose_runtime_env(env, ip="192.168.4.10")

    assert resolved["MN_NODE_NAME"] == "mn2@192.168.4.173"
    assert resolved["MN_CLUSTER_NODES"] == "mn1@192.168.4.10,mn2@192.168.4.173"
    assert resolved["MN_REDIS_URL"] == "redis://192.168.4.10:6379/0"
    assert resolved["ERL_AFLAGS"] == _erl_aflags("4500")

def test_compose_runtime_env_upgrades_stale_erl_aflags(monkeypatch):
    env = {
        "MN_NODE_NAME": "mn2@192.168.4.173",
        "MN_CLUSTER_NODES": "mn1@192.168.4.10",
        "MN_DIST_PORT": "4500",
        "ERL_AFLAGS": "-kernel inet_dist_listen_min 4500 inet_dist_listen_max 4500",
    }

    resolved = _compose_runtime_env(env, ip="192.168.4.10")

    assert resolved["ERL_AFLAGS"] == _erl_aflags("4500")

def test_compose_runtime_env_replaces_blank_or_stale_cluster_names(mocker):
    mocker.patch("mn_cli.server_cmds._detect_lan_ip", return_value="192.168.4.99")
    env = {
        "MN_NODE_NAME": "",
        "MN_NODE_ROLE": "",
        "MN_CLUSTER_NODES": "nonode@nohost",
        "MN_DIST_PORT": "4500",
    }

    resolved = _compose_runtime_env(env, ip="192.168.4.173")

    assert resolved["MN_NODE_NAME"] == "mirror_neuron@192.168.4.99"
    assert resolved["MN_NODE_ROLE"] == "runtime"
    assert resolved["MN_CLUSTER_NODES"] == "mirror_neuron@192.168.4.173"
    assert resolved["ERL_AFLAGS"] == _erl_aflags("4500")

def test_compose_runtime_env_replaces_stale_generated_names_after_ip_change(mocker):
    mocker.patch("mn_cli.server_cmds._detect_lan_ip", return_value="192.168.4.35")
    env = {
        "MN_NETWORK_ADVERTISE_HOST": "192.168.4.35",
        "MN_NODE_NAME": "mirror_neuron@192.168.4.20",
        "MN_NODE_ROLE": "",
        "MN_CLUSTER_NODES": "mirror_neuron@192.168.4.20",
        "MN_DIST_PORT": "4500",
    }

    resolved = _compose_runtime_env(env, ip="192.168.4.173")

    assert resolved["MN_NODE_NAME"] == "mirror_neuron@192.168.4.35"
    assert resolved["MN_NODE_ROLE"] == "runtime"
    assert resolved["MN_CLUSTER_NODES"] == "mirror_neuron@192.168.4.173"
    assert resolved["ERL_AFLAGS"] == _erl_aflags("4500")

def test_compose_runtime_env_preserves_docker_alias_identity(mocker):
    mocker.patch("mn_cli.server_cmds._detect_lan_ip", return_value="192.168.4.35")
    env = {
        "MN_DOCKER_NETWORK_MODE": "overlay",
        "MN_NODE_ALIAS": "mn-local",
        "MN_NETWORK_ADVERTISE_HOST": "192.168.4.35",
        "MN_NODE_NAME": "mirror_neuron@192.168.4.20",
        "MN_NODE_ROLE": "",
        "MN_CLUSTER_NODES": "mirror_neuron@mn-seed",
        "MN_DIST_PORT": "4500",
    }

    resolved = _compose_runtime_env(env, ip="192.168.4.173")

    assert resolved["MN_NODE_NAME"] == "mirror_neuron@mn-local"
    assert resolved["MN_NODE_ROLE"] == "runtime"
    assert resolved["MN_CLUSTER_NODES"] == "mirror_neuron@mn-seed"
    assert resolved["ERL_AFLAGS"] == _erl_aflags("4500")

def test_compose_port_conflict_resolution_leaves_internal_ports_unchanged(mocker):
    host_probe = mocker.patch("mn_cli.server_cmds._host_port_available")

    env = {
        "MN_EPMD_BIND_HOST": "0.0.0.0",
        "MN_EPMD_PORT": "54369",
        "MN_DIST_BIND_HOST": "0.0.0.0",
        "MN_DIST_PORT": "54370",
        "ERL_AFLAGS": _erl_aflags("54370"),
    }
    resolved = _avoid_local_compose_port_conflicts(env)

    assert resolved == env
    host_probe.assert_not_called()

def test_runtime_endpoint_snapshot_uses_local_hosts_for_wildcard_binds():
    snapshot = _runtime_endpoint_snapshot(
        {
            "MN_API_HOST": "0.0.0.0",
            "MN_API_PORT": "54111",
            "MN_GRPC_BIND_HOST": "0.0.0.0",
            "MN_GRPC_PORT": "55111",
            "MN_WEB_UI_HOST": "::",
            "MN_WEB_UI_PORT": "55174",
        },
        web_ui_available=True,
    )

    assert snapshot["api"]["base_url"] == "http://127.0.0.1:54111/api/v1"
    assert snapshot["api"]["host"] == "127.0.0.1"
    assert snapshot["grpc"]["target"] == "127.0.0.1:55111"
    assert snapshot["web_ui"]["url"] == "http://127.0.0.1:55174"

def test_runtime_endpoint_snapshot_uses_advertised_grpc_host_for_cluster_binds():
    snapshot = _runtime_endpoint_snapshot(
        {
            "MN_NETWORK_ADVERTISE_HOST": "192.168.4.173",
            "MN_GRPC_BIND_HOST": "0.0.0.0",
            "MN_GRPC_PORT": "55051",
            "MN_CORE_GRPC_TARGET": "localhost:55051",
        },
        web_ui_available=False,
    )

    assert snapshot["grpc"]["host"] == "192.168.4.173"
    assert snapshot["grpc"]["target"] == "192.168.4.173:55051"

def test_start_server_success(mocker, tmp_path, monkeypatch):
    mocker.patch('mn_cli.server_cmds.API_PID_FILE', tmp_path / "api.pid")
    mocker.patch('mn_cli.server_cmds.API_WATCHDOG_PID_FILE', tmp_path / "api-watchdog.pid")
    mocker.patch('mn_cli.server_cmds.WEB_UI_DIRS', ())
    redis_password = _derive_network_secret("join-token", "redis")
    monkeypatch.setenv("MN_API_PORT", "54111")
    monkeypatch.setenv("MN_BLUEPRINT_REPO", "/opt/mn/blueprints")
    monkeypatch.setenv("MN_DEV_LOCAL_BLUEPRINT_REPO", "/work/mn/otterdesk-blueprints")
    monkeypatch.setenv("MN_RUNS_ROOT", "/opt/mn/runs")
    
    def mock_run(cmd, **kwargs):
        m = mocker.Mock()
        m.stdout = "false\n"
        return m
        
    mocker.patch('mn_cli.server_cmds.subprocess.run', side_effect=mock_run)
    mocker.patch('mn_cli.server_cmds.time.sleep')
    mocker.patch('mn_cli.server_cmds.PID_DIR', tmp_path / ".pids")
    mocker.patch('mn_cli.server_cmds.LOG_DIR', tmp_path / ".logs")
    mocker.patch('mn_cli.server_cmds.BEAM_LOG', tmp_path / "beam.log")
    mocker.patch('mn_cli.server_cmds.API_LOG', tmp_path / "api.log")
    mocker.patch('mn_cli.server_cmds.API_WATCHDOG_LOG', tmp_path / "api-watchdog.log")
    mocker.patch('mn_cli.server_cmds._wait_for_api', return_value=True)
    mocker.patch(
        'mn_cli.server_cmds._handshake_with_main_node',
        return_value={
            "node_name": "mirror_neuron@127.0.0.1",
            "redis_host": "127.0.0.1",
            "redis_port": 6379,
            "redis_url": f"redis://:{redis_password}@127.0.0.1:6379/0",
        },
    )
    
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    api_bin = bin_dir / "mn-api"
    api_bin.touch()
    mocker.patch('mn_cli.server_cmds.VENV_DIR', tmp_path)
    
    mock_popen = mocker.patch('mn_cli.server_cmds.subprocess.Popen')
    mock_popen.return_value.pid = 9999
    
    class UnameMock:
        sysname = "Linux"
    mocker.patch('mn_cli.server_cmds.os.uname', return_value=UnameMock())
    
    _start_server(ip="127.0.0.1", token="join-token")
    
    assert (tmp_path / "api-watchdog.pid").exists()
    assert (tmp_path / "api-watchdog.pid").read_text() == "9999"
    api_env = mock_popen.call_args.kwargs["env"]
    assert api_env["MN_BLUEPRINT_REPO"] == "/opt/mn/blueprints"
    assert api_env["MN_DEV_LOCAL_BLUEPRINT_REPO"] == "/work/mn/otterdesk-blueprints"
    assert api_env["MN_RUNS_ROOT"] == "/opt/mn/runs"
    assert api_env["MN_HOST_ARTIFACTS_DIR"] == "/opt/mn/runs"
    assert api_env["MN_CONTAINER_RUNS_ROOT"] == "/root/.mn/runs"
    assert api_env["MN_BLUEPRINT_WEB_UI_BIND_HOST"] == "0.0.0.0"
    assert api_env["MN_BLUEPRINT_WEB_UI_PUBLIC_HOST"] == "localhost"
    assert api_env["MN_BLUEPRINT_WEB_UI_PORT_START"] == "61000"
    assert api_env["MN_BLUEPRINT_WEB_UI_PORT_END"] == "61049"
    assert api_env["MN_BLUEPRINT_WEB_UI_PORT_ALLOCATION_MODE"] == "prepublished"
    runtime_endpoints = json.loads(server_cmds.RUNTIME_ENDPOINTS_FILE.read_text())
    assert runtime_endpoints["api"]["base_url"] == "http://localhost:54111/api/v1"
    assert runtime_endpoints["api"]["port"] == "54111"
    assert "MN_GRPC_AUTH_TOKEN" not in json.dumps(runtime_endpoints)

def test_start_server_darwin(mocker, tmp_path):
    mocker.patch('mn_cli.server_cmds.API_PID_FILE', tmp_path / "api.pid")
    mocker.patch('mn_cli.server_cmds.WEB_UI_DIRS', ())
    
    def mock_run(cmd, **kwargs):
        m = mocker.Mock()
        m.stdout = "false\n"
        return m
        
    mocker.patch('mn_cli.server_cmds.subprocess.run', side_effect=mock_run)
    mocker.patch('mn_cli.server_cmds.time.sleep')
    mocker.patch('mn_cli.server_cmds.PID_DIR', tmp_path / ".pids")
    mocker.patch('mn_cli.server_cmds.LOG_DIR', tmp_path / ".logs")
    mocker.patch('mn_cli.server_cmds.BEAM_LOG', tmp_path / "beam.log")
    mocker.patch('mn_cli.server_cmds.API_LOG', tmp_path / "api.log")
    mocker.patch('mn_cli.server_cmds.VENV_DIR', tmp_path) # no mn-api to skip api
    
    class UnameMock:
        sysname = "Darwin"
    mocker.patch('mn_cli.server_cmds.os.uname', return_value=UnameMock())
    
    _start_server()


def test_start_server_passes_slack_env_to_docker(mocker, tmp_path, monkeypatch):
    mocker.patch('mn_cli.server_cmds.API_PID_FILE', tmp_path / "api.pid")
    mocker.patch('mn_cli.server_cmds.WEB_UI_DIRS', ())
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-test")
    monkeypatch.setenv("SLACK_DEFAULT_CHANNEL", "#claw")

    commands = []

    def mock_run(cmd, **kwargs):
        commands.append(cmd)
        m = mocker.Mock()
        m.stdout = "false\n"
        return m

    mocker.patch('mn_cli.server_cmds.subprocess.run', side_effect=mock_run)
    mocker.patch('mn_cli.server_cmds.time.sleep')
    mocker.patch('mn_cli.server_cmds.PID_DIR', tmp_path / ".pids")
    mocker.patch('mn_cli.server_cmds.LOG_DIR', tmp_path / ".logs")
    mocker.patch('mn_cli.server_cmds.BEAM_LOG', tmp_path / "beam.log")
    mocker.patch('mn_cli.server_cmds.API_LOG', tmp_path / "api.log")
    mocker.patch('mn_cli.server_cmds.VENV_DIR', tmp_path)

    class UnameMock:
        sysname = "Darwin"

    mocker.patch('mn_cli.server_cmds.os.uname', return_value=UnameMock())

    _start_server()

    docker_run = next(cmd for cmd in commands if cmd[:3] == ["docker", "run", "-d"])
    cookie_env = next(value for flag, value in zip(docker_run, docker_run[1:]) if flag == "-e" and value.startswith("MN_COOKIE="))
    auth_env = next(value for flag, value in zip(docker_run, docker_run[1:]) if flag == "-e" and value.startswith("MN_GRPC_AUTH_TOKEN="))
    admin_env = next(value for flag, value in zip(docker_run, docker_run[1:]) if flag == "-e" and value.startswith("MN_GRPC_ADMIN_TOKEN="))
    runs_root_env = next(value for flag, value in zip(docker_run, docker_run[1:]) if flag == "-e" and value.startswith("MN_RUNS_ROOT="))
    assert cookie_env != "MN_COOKIE=mirrorneuron"
    assert auth_env != "MN_GRPC_AUTH_TOKEN="
    assert admin_env != "MN_GRPC_ADMIN_TOKEN="
    assert runs_root_env == "MN_RUNS_ROOT=/root/.mn/runs"
    assert ["-v", f"{server_cmds.DIR}:/root/.mn"] == docker_run[
        docker_run.index(f"{server_cmds.DIR}:/root/.mn") - 1 : docker_run.index(f"{server_cmds.DIR}:/root/.mn") + 1
    ]
    assert ["-v", f"{server_cmds.DIR / 'runs'}:/root/.mn/runs"] == docker_run[
        docker_run.index(f"{server_cmds.DIR / 'runs'}:/root/.mn/runs") - 1 : docker_run.index(f"{server_cmds.DIR / 'runs'}:/root/.mn/runs") + 1
    ]
    assert ["-e", "SLACK_BOT_TOKEN"] == docker_run[
        docker_run.index("SLACK_BOT_TOKEN") - 1 : docker_run.index("SLACK_BOT_TOKEN") + 1
    ]
    assert ["-e", "SLACK_DEFAULT_CHANNEL"] == docker_run[
        docker_run.index("SLACK_DEFAULT_CHANNEL") - 1 : docker_run.index("SLACK_DEFAULT_CHANNEL") + 1
    ]

def test_detach_local_docker_node_stops_compose_core_for_local_alias(mocker, tmp_path):
    compose_file = tmp_path / "docker-compose.yml"
    compose_env = tmp_path / "docker-compose.env"
    compose_file.write_text("services: {}\n")
    compose_env.write_text("MN_NODE_ALIAS=mn-local\n")
    mocker.patch("mn_cli.server_cmds.RUNTIME_COMPOSE_FILE", compose_file)
    mocker.patch("mn_cli.server_cmds.RUNTIME_COMPOSE_ENV", compose_env)
    mock_run = mocker.patch("mn_cli.server_cmds.subprocess.run")

    assert _detach_local_docker_node_if_matches("mirror_neuron@mn-local") is True

    mock_run.assert_called_once_with(
        runtime_compose_cmd("stop", "mirror-neuron-core"),
        stderr=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
    )

def test_detach_local_docker_node_ignores_remote_alias(mocker, tmp_path):
    (server_cmds.DIR / "node.alias").parent.mkdir(parents=True, exist_ok=True)
    (server_cmds.DIR / "node.alias").write_text("mn-local\n")
    mock_run = mocker.patch("mn_cli.server_cmds.subprocess.run")

    assert _detach_local_docker_node_if_matches("mirror_neuron@mn-remote") is False
    mock_run.assert_not_called()

def test_default_web_ui_dirs_use_nested_install_path():
    assert ORIGINAL_WEB_UI_DIRS[0].name == "webui"
    assert ORIGINAL_WEB_UI_DIRS[1] == ORIGINAL_WEB_UI_DIRS[0].parent / "web-ui-source"
    assert Path.home() / ".mn" / "webui" in ORIGINAL_WEB_UI_DIRS
    assert Path.home() / ".mn" / "web-ui-source" in ORIGINAL_WEB_UI_DIRS

def test_web_ui_dirs_include_default_install_when_runtime_home_is_custom(mocker, tmp_path):
    custom_home = tmp_path / "custom-home"
    default_home = tmp_path / "default-home"

    mocker.patch("mn_cli.server_cmds.DIR", custom_home)
    mocker.patch("mn_cli.server_cmds.DEFAULT_DIR", default_home)
    mocker.patch("mn_cli.server_cmds._source_checkout_web_ui_dir", return_value=None)

    assert server_cmds._web_ui_dirs() == (
        custom_home / "webui",
        custom_home / "web-ui-source",
        default_home / "webui",
        default_home / "web-ui-source",
    )

def test_find_web_ui_dir_uses_default_install_when_runtime_home_has_no_webui(tmp_path, mocker):
    runtime_web_ui = tmp_path / "runtime-home" / "webui"
    default_web_ui = tmp_path / "default-home" / "webui"
    (default_web_ui / "dist").mkdir(parents=True)
    (default_web_ui / "dist" / "index.html").write_text("<div id=\"root\"></div>")

    mocker.patch('mn_cli.server_cmds.WEB_UI_DIRS', (runtime_web_ui, default_web_ui))

    assert find_web_ui_dir() == default_web_ui

def test_find_web_ui_dir_installed(tmp_path, mocker):
    missing = tmp_path / "missing"
    installed = tmp_path / "web-ui"
    (installed / "dist").mkdir(parents=True)
    (installed / "dist" / "index.html").write_text("<div id=\"root\"></div>")
    mocker.patch('mn_cli.server_cmds.WEB_UI_DIRS', (missing, installed))

    assert find_web_ui_dir() == installed

def test_find_web_ui_dir_accepts_packaged_static_root(tmp_path, mocker):
    installed = tmp_path / "web-ui"
    installed.mkdir()
    (installed / "index.html").write_text("<div id=\"root\"></div>")
    mocker.patch('mn_cli.server_cmds.WEB_UI_DIRS', (installed,))

    assert find_web_ui_dir() == installed

def test_web_ui_http_url_uses_connectable_loopback_for_wildcard_hosts():
    assert server_cmds._web_ui_http_url("0.0.0.0", "55173") == "http://127.0.0.1:55173/"
    assert server_cmds._web_ui_http_url("::", "55173") == "http://127.0.0.1:55173/"
    assert server_cmds._web_ui_http_url("::1", "55173") == "http://[::1]:55173/"

def test_start_api_if_installed(mocker, tmp_path):
    api_bin = tmp_path / "mn_venv" / "bin" / "mn-api"
    api_bin.parent.mkdir(parents=True)
    api_bin.write_text("#!/bin/sh\n")

    mocker.patch('mn_cli.server_cmds.VENV_DIR', tmp_path / "mn_venv")
    mocker.patch('mn_cli.server_cmds.API_PID_FILE', tmp_path / "api.pid")
    mocker.patch('mn_cli.server_cmds.API_WATCHDOG_PID_FILE', tmp_path / "api-watchdog.pid")
    mocker.patch('mn_cli.server_cmds.API_LOG', tmp_path / "api.log")
    mocker.patch('mn_cli.server_cmds.API_WATCHDOG_LOG', tmp_path / "api-watchdog.log")
    mock_wait = mocker.patch('mn_cli.server_cmds._wait_for_api', side_effect=[False, True])

    mock_popen = mocker.patch('mn_cli.server_cmds.subprocess.Popen')
    mock_popen.return_value.pid = 54001

    assert _start_api_if_installed({"MN_API_HOST": "localhost", "MN_API_PORT": "54001"}) is True

    assert (tmp_path / "api-watchdog.pid").read_text() == "54001"
    mock_popen.assert_called_once()
    command = mock_popen.call_args.args[0]
    assert command[0] == sys.executable
    assert command[1] == "-c"
    watchdog_config = json.loads(command[3])
    assert watchdog_config["command"] == [str(api_bin)]
    assert watchdog_config["pid_file"] == str(tmp_path / "api.pid")
    assert mock_popen.call_args.kwargs["stdin"] == subprocess.DEVNULL
    assert mock_wait.call_args_list == [
        call("localhost", "54001", timeout_seconds=10.0),
    ]

def test_start_web_ui_if_installed(mocker, tmp_path):
    web_ui_dir = tmp_path / "web-ui"
    (web_ui_dir / "dist").mkdir(parents=True)
    (web_ui_dir / "dist" / "index.html").write_text("<div id=\"root\"></div>")

    mocker.patch('mn_cli.server_cmds.WEB_UI_DIRS', (web_ui_dir,))
    mocker.patch('mn_cli.server_cmds.WEB_UI_PID_FILE', tmp_path / "web-ui.pid")
    mocker.patch('mn_cli.server_cmds.WEB_UI_WATCHDOG_PID_FILE', tmp_path / "web-ui-watchdog.pid")
    mocker.patch('mn_cli.server_cmds.WEB_UI_LOG', tmp_path / "web-ui.log")
    mocker.patch('mn_cli.server_cmds.WEB_UI_WATCHDOG_LOG', tmp_path / "web-ui-watchdog.log")
    mock_wait = mocker.patch('mn_cli.server_cmds._wait_for_web_ui', side_effect=[False, True])

    mock_popen = mocker.patch('mn_cli.server_cmds.subprocess.Popen')
    mock_popen.return_value.pid = 5173

    _start_web_ui_if_installed()

    assert (tmp_path / "web-ui-watchdog.pid").read_text() == "5173"
    mock_popen.assert_called_once()
    command = mock_popen.call_args.args[0]
    assert command[0] == sys.executable
    assert command[1] == "-c"
    watchdog_config = json.loads(command[3])
    assert watchdog_config["command"] == [sys.executable, "-m", "mn_api.web_ui_server"]
    assert watchdog_config["cwd"] == str(web_ui_dir)
    assert mock_popen.call_args.kwargs["stdin"] == subprocess.DEVNULL
    assert mock_popen.call_args.kwargs["env"]["MN_WEB_UI_DIST_DIR"] == str(web_ui_dir / "dist")
    assert mock_wait.call_args_list == [
        call("localhost", "55173", timeout_seconds=1.0),
        call("localhost", "55173", timeout_seconds=10.0),
    ]

def test_start_web_ui_reports_available_when_watchdog_starts_before_health(mocker, tmp_path):
    web_ui_dir = tmp_path / "web-ui"
    (web_ui_dir / "dist").mkdir(parents=True)
    (web_ui_dir / "dist" / "index.html").write_text("<div id=\"root\"></div>")

    mocker.patch('mn_cli.server_cmds.WEB_UI_DIRS', (web_ui_dir,))
    mocker.patch('mn_cli.server_cmds.WEB_UI_PID_FILE', tmp_path / "web-ui.pid")
    mocker.patch('mn_cli.server_cmds.WEB_UI_WATCHDOG_PID_FILE', tmp_path / "web-ui-watchdog.pid")
    mocker.patch('mn_cli.server_cmds.WEB_UI_LOG', tmp_path / "web-ui.log")
    mocker.patch('mn_cli.server_cmds.WEB_UI_WATCHDOG_LOG', tmp_path / "web-ui-watchdog.log")
    mocker.patch('mn_cli.server_cmds._wait_for_web_ui', return_value=False)

    mock_popen = mocker.patch('mn_cli.server_cmds.subprocess.Popen')
    mock_popen.return_value.pid = 5173

    assert _start_web_ui_if_installed() is True
    assert (tmp_path / "web-ui-watchdog.pid").read_text() == "5173"

def test_start_web_ui_advertises_existing_healthy_instance_without_pid_files(mocker, tmp_path):
    web_ui_dir = tmp_path / "web-ui"
    (web_ui_dir / "dist").mkdir(parents=True)
    (web_ui_dir / "dist" / "index.html").write_text("<div id=\"root\"></div>")

    mocker.patch('mn_cli.server_cmds.WEB_UI_DIRS', (web_ui_dir,))
    mocker.patch('mn_cli.server_cmds.WEB_UI_PID_FILE', tmp_path / "web-ui.pid")
    mocker.patch('mn_cli.server_cmds.WEB_UI_WATCHDOG_PID_FILE', tmp_path / "web-ui-watchdog.pid")
    mocker.patch('mn_cli.server_cmds._wait_for_web_ui', return_value=True)

    mock_popen = mocker.patch('mn_cli.server_cmds.subprocess.Popen')

    assert _start_web_ui_if_installed() is True
    mock_popen.assert_not_called()

def test_start_web_ui_restarts_unresponsive_watchdog(mocker, tmp_path):
    web_ui_dir = tmp_path / "web-ui"
    (web_ui_dir / "dist").mkdir(parents=True)
    (web_ui_dir / "dist" / "index.html").write_text("<div id=\"root\"></div>")
    watchdog_pid_file = tmp_path / "web-ui-watchdog.pid"
    child_pid_file = tmp_path / "web-ui.pid"
    watchdog_pid_file.write_text("9001")
    child_pid_file.write_text("9002")

    mocker.patch('mn_cli.server_cmds.WEB_UI_DIRS', (web_ui_dir,))
    mocker.patch('mn_cli.server_cmds.WEB_UI_PID_FILE', child_pid_file)
    mocker.patch('mn_cli.server_cmds.WEB_UI_WATCHDOG_PID_FILE', watchdog_pid_file)
    mocker.patch('mn_cli.server_cmds.WEB_UI_LOG', tmp_path / "web-ui.log")
    mocker.patch('mn_cli.server_cmds.WEB_UI_WATCHDOG_LOG', tmp_path / "web-ui-watchdog.log")
    mocker.patch('mn_cli.server_cmds.os.kill')
    mock_kill_tree = mocker.patch('mn_cli.server_cmds.kill_tree')
    mock_wait = mocker.patch('mn_cli.server_cmds._wait_for_web_ui', side_effect=[False, True])
    mock_popen = mocker.patch('mn_cli.server_cmds.subprocess.Popen')
    mock_popen.return_value.pid = 5173

    assert _start_web_ui_if_installed() is True

    mock_kill_tree.assert_called_once_with(9001)
    assert watchdog_pid_file.read_text() == "5173"
    assert mock_wait.call_args_list == [
        call("localhost", "55173", timeout_seconds=5.0),
        call("localhost", "55173", timeout_seconds=10.0),
    ]

def test_start_web_ui_cleans_stale_watchdog_pid(mocker, tmp_path):
    web_ui_dir = tmp_path / "web-ui"
    (web_ui_dir / "dist").mkdir(parents=True)
    (web_ui_dir / "dist" / "index.html").write_text("<div id=\"root\"></div>")
    watchdog_pid_file = tmp_path / "web-ui-watchdog.pid"
    watchdog_pid_file.write_text("9001")

    mocker.patch('mn_cli.server_cmds.WEB_UI_DIRS', (web_ui_dir,))
    mocker.patch('mn_cli.server_cmds.WEB_UI_PID_FILE', tmp_path / "web-ui.pid")
    mocker.patch('mn_cli.server_cmds.WEB_UI_WATCHDOG_PID_FILE', watchdog_pid_file)
    mocker.patch('mn_cli.server_cmds.WEB_UI_LOG', tmp_path / "web-ui.log")
    mocker.patch('mn_cli.server_cmds.WEB_UI_WATCHDOG_LOG', tmp_path / "web-ui-watchdog.log")
    mocker.patch('mn_cli.server_cmds.os.kill', side_effect=OSError("stale pid"))
    mocker.patch('mn_cli.server_cmds._wait_for_web_ui', return_value=True)
    mock_popen = mocker.patch('mn_cli.server_cmds.subprocess.Popen')
    mock_popen.return_value.pid = 5173

    assert _start_web_ui_if_installed() is True

    assert watchdog_pid_file.read_text() == "5173"

def test_start_web_ui_missing_noop(mocker, tmp_path):
    mocker.patch('mn_cli.server_cmds.WEB_UI_DIRS', (tmp_path / "web-ui",))
    mock_popen = mocker.patch('mn_cli.server_cmds.subprocess.Popen')

    _start_web_ui_if_installed()

    mock_popen.assert_not_called()

def test_start_web_ui_missing_build_skips(mocker, tmp_path):
    web_ui_dir = tmp_path / "web-ui"
    web_ui_dir.mkdir()

    mocker.patch('mn_cli.server_cmds.WEB_UI_DIRS', (web_ui_dir,))
    mocker.patch('mn_cli.server_cmds.WEB_UI_PID_FILE', tmp_path / "web-ui.pid")
    mocker.patch('mn_cli.server_cmds.WEB_UI_LOG', tmp_path / "web-ui.log")
    mock_popen = mocker.patch('mn_cli.server_cmds.subprocess.Popen')

    assert _start_web_ui_if_installed() is False
    assert not (tmp_path / "web-ui.pid").exists()
    mock_popen.assert_not_called()

def test_print_service_endpoints(mocker, monkeypatch):
    output = StringIO()
    mocker.patch('mn_cli.server_cmds.console', Console(file=output, force_terminal=False, width=120))
    monkeypatch.setenv("MN_GRPC_TARGET", "core.local:55555")
    monkeypatch.setenv("MN_API_HOST", "127.0.0.1")
    monkeypatch.setenv("MN_API_PORT", "4401")
    monkeypatch.setenv("MN_REDIS_URL", "redis://redis.local:6380/0")
    monkeypatch.setenv("MN_DIST_PORT", "54370")

    _print_service_endpoints(ip=None, web_ui_available=True)

    rendered = output.getvalue()
    assert "Service endpoints" in rendered
    assert "Core gRPC" in rendered
    assert "core.local" in rendered
    assert "55555" in rendered
    assert "REST API" in rendered
    assert "4401" in rendered
    assert "Redis" in rendered
    assert "redis.local" in rendered
    assert "6380" in rendered
    assert "Erlang EPMD" in rendered
    assert "Erlang dist" in rendered
    assert "54370" in rendered
    assert "Web UI" in rendered
    assert "55173" in rendered

def test_print_service_endpoints_defaults_to_localhost(mocker, monkeypatch):
    output = StringIO()
    mocker.patch('mn_cli.server_cmds.console', Console(file=output, force_terminal=False, width=120))
    for name in (
        "MN_GRPC_TARGET",
        "MN_CORE_GRPC_TARGET",
        "MN_CORE_HOST",
        "MN_API_HOST",
        "MN_REDIS_HOST",
        "MN_REDIS_URL",
        "MN_EPMD_HOST",
        "MN_DIST_HOST",
        "MN_WEB_UI_HOST",
        "MN_NETWORK_ADVERTISE_HOST",
    ):
        monkeypatch.delenv(name, raising=False)

    _print_service_endpoints(ip=None, web_ui_available=True)

    rendered = output.getvalue()
    assert "Core gRPC" in rendered
    assert "REST API" in rendered
    assert "Redis" in rendered
    assert "Web UI" in rendered
    assert "localhost" in rendered
    assert "0.0.0.0" not in rendered
    assert "127.0.0.1" not in rendered

def test_print_service_endpoints_shows_compose_native_ports(mocker, monkeypatch, tmp_path):
    output = StringIO()
    mocker.patch('mn_cli.server_cmds.console', Console(file=output, force_terminal=False, width=120))
    monkeypatch.delenv("OPENSHELL_GATEWAY_ENDPOINT", raising=False)
    monkeypatch.setenv("OPENSHELL_CONFIG_DIR", str(tmp_path / "missing-openshell"))
    compose_file = tmp_path / "docker-compose.yml"
    compose_env = tmp_path / "docker-compose.env"
    compose_file.write_text("services: {}\n")
    compose_env.write_text(
        "COMPOSE_PROJECT_NAME=mirror-neuron\n"
        "MN_NETWORK_REDIS_HOST=192.168.4.10\n"
        "MN_NETWORK_REDIS_PORT=56379\n"
    )
    mocker.patch('mn_cli.server_cmds.RUNTIME_COMPOSE_FILE', compose_file)
    mocker.patch('mn_cli.server_cmds.RUNTIME_COMPOSE_ENV', compose_env)

    _print_service_endpoints(ip=None, web_ui_available=False)

    rendered = output.getvalue()
    assert "Core gRPC" in rendered
    assert "REST API" in rendered
    assert "redis://192.168.4.10:56379/0" not in rendered
    assert "56379" not in rendered
    assert "auth required" not in rendered
    assert "Context engine" not in rendered
    assert "Redis and Erlang cluster traffic use Docker internal networking." not in rendered
    assert "OpenShell" not in rendered
    assert "http://127.0.0.1:58080" not in rendered
    assert "Erlang EPMD" not in rendered
    assert "Erlang dist" not in rendered
    assert "54370" not in rendered

def test_print_service_endpoints_shows_advertised_cluster_host(mocker, monkeypatch, tmp_path):
    output = StringIO()
    mocker.patch('mn_cli.server_cmds.console', Console(file=output, force_terminal=False, width=120))
    monkeypatch.delenv("OPENSHELL_GATEWAY_ENDPOINT", raising=False)
    monkeypatch.setenv("OPENSHELL_CONFIG_DIR", str(tmp_path / "missing-openshell"))
    compose_file = tmp_path / "docker-compose.yml"
    compose_env = tmp_path / "docker-compose.env"
    compose_file.write_text("services: {}\n")
    compose_env.write_text(
        "COMPOSE_PROJECT_NAME=mirror-neuron\n"
        "MN_NETWORK_ADVERTISE_HOST=192.168.4.173\n"
        "MN_GRPC_BIND_HOST=0.0.0.0\n"
        "MN_EPMD_BIND_HOST=0.0.0.0\n"
        "MN_DIST_BIND_HOST=0.0.0.0\n"
        "MN_NETWORK_REDIS_HOST=192.168.4.173\n"
        "MN_NETWORK_REDIS_PORT=56379\n"
    )
    mocker.patch('mn_cli.server_cmds.RUNTIME_COMPOSE_FILE', compose_file)
    mocker.patch('mn_cli.server_cmds.RUNTIME_COMPOSE_ENV', compose_env)

    _print_service_endpoints(ip=None, web_ui_available=False)

    rendered = output.getvalue()
    assert "192.168.4.173" in rendered
    assert "192.168.4.173:55051" in rendered
    assert "192.168.4.173:54369" not in rendered
    assert "192.168.4.173:54370" not in rendered
    assert "redis://192.168.4.173:56379/0" not in rendered
    assert "Redis and Erlang cluster traffic use Docker internal networking." not in rendered
    assert "0.0.0.0" not in rendered
