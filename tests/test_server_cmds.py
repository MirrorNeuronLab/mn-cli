import json
import pytest
import subprocess
from io import StringIO
from pathlib import Path
from rich.console import Console
import mn_cli.server_cmds as server_cmds
from mn_cli.server_cmds import (
    check_status,
    kill_tree,
    _resolve_grpc_admin_token,
    _resolve_grpc_auth_token,
    _resolve_mn_cookie,
    _resolve_network_token,
    _derive_network_secret,
    _start_server,
    _start_network_seed,
    _join_network,
    _avoid_local_compose_port_conflicts,
    find_web_ui_dir,
    _start_web_ui_if_installed,
    _compose_runtime_env,
    _print_service_endpoints,
    _runtime_endpoint_snapshot,
    runtime_compose_cmd,
)
import typer

ORIGINAL_WEB_UI_DIRS = server_cmds.WEB_UI_DIRS

@pytest.fixture(autouse=True)
def isolated_mn_cookie_home(mocker, tmp_path, monkeypatch):
    monkeypatch.delenv("MN_COOKIE", raising=False)
    monkeypatch.delenv("MN_GRPC_AUTH_TOKEN", raising=False)
    monkeypatch.delenv("MN_MIRROR_NEURON_GRPC_ADMIN_TOKEN", raising=False)
    state_dir = tmp_path / ".mirror_neuron"
    log_dir = state_dir / ".logs"
    pid_dir = state_dir / ".pids"
    mocker.patch('mn_cli.server_cmds.DIR', state_dir)
    mocker.patch('mn_cli.server_cmds.PID_DIR', pid_dir)
    mocker.patch('mn_cli.server_cmds.LOG_DIR', log_dir)
    mocker.patch('mn_cli.server_cmds.BEAM_PID_FILE', pid_dir / "beam.pid")
    mocker.patch('mn_cli.server_cmds.API_PID_FILE', pid_dir / "api.pid")
    mocker.patch('mn_cli.server_cmds.WEB_UI_PID_FILE', pid_dir / "web-ui.pid")
    mocker.patch('mn_cli.server_cmds.BEAM_LOG', log_dir / "beam.log")
    mocker.patch('mn_cli.server_cmds.API_LOG', log_dir / "api.log")
    mocker.patch('mn_cli.server_cmds.WEB_UI_LOG', log_dir / "web-ui.log")
    mocker.patch('mn_cli.server_cmds.VENV_DIR', tmp_path / "mn_venv")
    mocker.patch('mn_cli.server_cmds.RUNTIME_COMPOSE_FILE', state_dir / "docker-compose.yml")
    mocker.patch('mn_cli.server_cmds.RUNTIME_COMPOSE_ENV', state_dir / "docker-compose.env")
    mocker.patch('mn_cli.server_cmds.RUNTIME_ENDPOINTS_FILE', state_dir / "runtime-endpoints.json")
    mocker.patch('mn_cli.server_cmds.NETWORK_TOKEN_FILE', state_dir / "network.token")
    mocker.patch('mn_cli.server_cmds.NETWORK_REDIS_ENV_FILE', state_dir / "network-redis.env")
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

def test_resolve_grpc_admin_token_generates_persistent_token(tmp_path, mocker):
    token_dir = tmp_path / "state"
    mocker.patch('mn_cli.server_cmds.DIR', token_dir)

    token = _resolve_grpc_admin_token()

    assert token
    assert (token_dir / "grpc_admin.token").read_text().strip() == token
    assert (token_dir / "grpc_admin.token").stat().st_mode & 0o777 == 0o600
    assert _resolve_grpc_admin_token() == token

def test_resolve_grpc_admin_token_prefers_env(monkeypatch):
    monkeypatch.setenv("MN_MIRROR_NEURON_GRPC_ADMIN_TOKEN", "admin-token")

    assert _resolve_grpc_admin_token() == "admin-token"

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

def test_start_network_seed_starts_only_core_and_redis(mocker, tmp_path, monkeypatch):
    monkeypatch.delenv("MN_REDIS_URL", raising=False)
    token_file = tmp_path / "network.token"
    mocker.patch('mn_cli.server_cmds.DIR', tmp_path)
    mocker.patch('mn_cli.server_cmds.NETWORK_TOKEN_FILE', token_file)
    mocker.patch('mn_cli.server_cmds.secrets.token_urlsafe', return_value="seed-token")
    mocker.patch('mn_cli.server_cmds._docker_container_running', return_value=False)
    mocker.patch('mn_cli.server_cmds._port_available_or_owned', return_value=True)

    commands = []

    def mock_run(cmd, **kwargs):
        commands.append(cmd)
        m = mocker.Mock()
        m.returncode = 1 if cmd[:3] == ["docker", "network", "inspect"] else 0
        m.stdout = "false\n"
        return m

    mocker.patch('mn_cli.server_cmds.subprocess.run', side_effect=mock_run)

    assert _start_network_seed(
        host="192.168.4.10",
        grpc_port=50055,
        dist_port=4500,
        redis_port=6380,
    ) == "seed-token"

    assert token_file.read_text().strip() == "seed-token"
    assert any(cmd[:4] == ["docker", "run", "-d", "--name"] and cmd[4] == "mirror-neuron-network-redis" for cmd in commands)
    core_run = next(cmd for cmd in commands if len(cmd) > 4 and cmd[:4] == ["docker", "run", "-d", "--name"] and cmd[4] == "mirror-neuron-network-core")
    assert "mirror-neuron-core:latest" in core_run
    assert "redis:7" not in core_run
    assert "-p" in core_run
    assert f"MN_COOKIE={_derive_network_secret('seed-token', 'cookie')}" in core_run
    assert "MN_NETWORK_ONLY=true" in core_run
    assert "MN_NODE_NAME=mirror_neuron@192.168.4.10" in core_run
    assert "MN_CLUSTER_NODES=mirror_neuron@192.168.4.10" in core_run
    assert (
        f"MN_REDIS_URL=redis://:{_derive_network_secret('seed-token', 'redis')}"
        "@mirror-neuron-network-redis:6379/0"
    ) in core_run

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
    assert "mn add-node 192.168.4.10 --token seed-token" in rendered
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
    assert "mn add-node 192.168.4.20 --token runtime-token" in rendered
    mock_start_redis.assert_not_called()
    mock_start_core.assert_not_called()

def test_add_node_uses_handshake_and_local_core(mocker, tmp_path):
    import mn_sdk
    import mn_cli.shared

    mocker.patch('mn_cli.server_cmds.DIR', tmp_path)
    mocker.patch('mn_cli.server_cmds._docker_container_running', return_value=False)
    redis_password = _derive_network_secret("join-token", "redis")

    class StubClient:
        def __init__(self, target, auth_token, timeout):
            assert target == "192.168.4.10:50055"
            assert auth_token == ""
            assert timeout == 10

        def network_handshake(self, token):
            assert token == "join-token"
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

def test_add_node_rejects_missing_remote_redis_details(mocker, tmp_path):
    import mn_sdk

    mocker.patch('mn_cli.server_cmds.DIR', tmp_path)

    class StubClient:
        def __init__(self, target, auth_token, timeout):
            assert target == "192.168.4.10:50055"

        def network_handshake(self, token):
            return {"node_name": "mirror_neuron@192.168.4.10"}

    mocker.patch.object(mn_sdk, "Client", StubClient)

    with pytest.raises(typer.Exit) as exc:
        _join_network("192.168.4.10", "join-token", grpc_port=50055)

    assert exc.value.exit_code == 1

def test_add_node_rejects_redis_url_without_token_password(mocker, tmp_path):
    import mn_sdk

    mocker.patch('mn_cli.server_cmds.DIR', tmp_path)

    class StubClient:
        def __init__(self, target, auth_token, timeout):
            assert target == "192.168.4.10:50055"

        def network_handshake(self, token):
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

def test_start_server_already_running(mocker, tmp_path):
    mocker.patch('mn_cli.server_cmds.API_PID_FILE', tmp_path / "api.pid")
    (tmp_path / "api.pid").write_text("1234")
    mocker.patch('mn_cli.server_cmds.os.kill') # check_status returns 0
    
    with pytest.raises(typer.Exit) as exc:
        _start_server()
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

def test_compose_redis_publish_settings_persists_dynamic_port(mocker, tmp_path):
    compose_env = tmp_path / "docker-compose.env"
    compose_env.write_text("COMPOSE_PROJECT_NAME=mirror-neuron\n")
    mocker.patch('mn_cli.server_cmds.RUNTIME_COMPOSE_ENV', compose_env)

    def available(host, port, owner_container, target_port):
        return port == 56380

    mocker.patch('mn_cli.server_cmds._port_available_or_owned', side_effect=available)

    env, port = server_cmds._ensure_compose_redis_publish_settings(
        {"MN_REDIS_BIND_HOST": "0.0.0.0"},
        token="join-token",
        advertised_host="192.168.4.10",
    )

    redis_password = _derive_network_secret("join-token", "redis")
    assert port == 56380
    assert env["MN_REDIS_PORT"] == "56380"
    assert env["MN_REDIS_PASSWORD"] == redis_password
    assert env["MN_REDIS_URL"] == f"redis://:{redis_password}@redis:6379/0"
    compose_env_text = compose_env.read_text()
    assert "MN_REDIS_BIND_HOST=0.0.0.0" in compose_env_text
    assert "MN_REDIS_PORT=56380" in compose_env_text
    assert f"MN_REDIS_PASSWORD={redis_password}" in compose_env_text
    assert "MN_NETWORK_REDIS_HOST=192.168.4.10" in compose_env_text
    assert "MN_NETWORK_REDIS_PORT=56380" in compose_env_text

def test_compose_redis_explicit_port_must_be_available(mocker, tmp_path, monkeypatch):
    compose_env = tmp_path / "docker-compose.env"
    compose_env.write_text("COMPOSE_PROJECT_NAME=mirror-neuron\n")
    mocker.patch('mn_cli.server_cmds.RUNTIME_COMPOSE_ENV', compose_env)
    mocker.patch('mn_cli.server_cmds._port_available_or_owned', return_value=False)
    monkeypatch.setenv("MN_REDIS_PORT", "56379")

    with pytest.raises(typer.Exit) as exc:
        server_cmds._ensure_compose_redis_publish_settings(
            {},
            token="join-token",
            advertised_host="192.168.4.10",
        )

    assert exc.value.exit_code == 1

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
    assert env["MN_EPMD_BIND_HOST"] == "0.0.0.0"
    assert env["MN_DIST_BIND_HOST"] == "0.0.0.0"
    assert env["ERL_EPMD_ADDRESS"] == "0.0.0.0"
    compose_env_text = compose_env.read_text()
    assert "MN_GRPC_BIND_HOST=0.0.0.0" in compose_env_text
    assert "MN_EPMD_BIND_HOST=0.0.0.0" in compose_env_text
    assert "MN_DIST_BIND_HOST=0.0.0.0" in compose_env_text
    assert "ERL_EPMD_ADDRESS=0.0.0.0" in compose_env_text

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
    assert env["MN_EPMD_BIND_HOST"] == "0.0.0.0"
    assert env["MN_DIST_BIND_HOST"] == "0.0.0.0"

def test_start_server_uses_compose_runtime_when_available(mocker, tmp_path):
    compose_file = tmp_path / "docker-compose.yml"
    compose_env = tmp_path / "docker-compose.env"
    compose_file.write_text("services: {}\n")
    compose_env.write_text(
        "COMPOSE_PROJECT_NAME=mirror-neuron\n"
        "MN_NODE_NAME=\n"
        "MN_CLUSTER_NODES=nonode@nohost\n"
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

    calls = []

    def mock_run(cmd, **kwargs):
        calls.append((cmd, kwargs))
        m = mocker.Mock()
        m.stdout = "false\n"
        return m

    mocker.patch('mn_cli.server_cmds.subprocess.run', side_effect=mock_run)
    mocker.patch('mn_cli.server_cmds._find_available_published_port', return_value=56379)

    _start_server()

    commands = [call[0] for call in calls]
    assert runtime_compose_cmd("up", "-d") in commands
    assert all(cmd[:2] != ["docker", "inspect"] for cmd in commands)
    assert all(cmd[:3] != ["docker", "run", "-d"] for cmd in commands)
    compose_call = next(item for item in calls if item[0] == runtime_compose_cmd("up", "-d"))
    env = compose_call[1]["env"]
    assert env["MN_NODE_NAME"] == "mirror_neuron@192.168.4.99"
    assert env["MN_CLUSTER_NODES"] == "mirror_neuron@192.168.4.99"
    assert env["MN_COOKIE"] == _derive_network_secret(env["MN_NETWORK_JOIN_TOKEN"], "cookie")
    compose_env_text = compose_env.read_text()
    assert "MN_GRPC_AUTH_TOKEN=" in compose_env_text
    assert "MN_MIRROR_NEURON_GRPC_ADMIN_TOKEN=" in compose_env_text
    assert "MN_COOKIE=" in compose_env_text
    assert "MN_HOST_ARTIFACTS_DIR=" in compose_env_text
    assert "MN_RUNS_ROOT=" in compose_env_text
    assert "MN_CONTAINER_RUNS_ROOT=/root/.mn/runs" in compose_env_text

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
    assert env["ERL_AFLAGS"] == "-kernel inet_dist_listen_min 54370 inet_dist_listen_max 54370"

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
    assert resolved["ERL_AFLAGS"] == "-kernel inet_dist_listen_min 4500 inet_dist_listen_max 4500"

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
    assert resolved["ERL_AFLAGS"] == "-kernel inet_dist_listen_min 4500 inet_dist_listen_max 4500"

def test_compose_port_conflict_resolution_reserves_selected_ports(mocker):
    mocker.patch("mn_cli.server_cmds.runtime_compose_available", return_value=False)
    mocker.patch("mn_cli.server_cmds._host_port_available", side_effect=lambda _host, port: port not in {54469, 54470})

    resolved = _avoid_local_compose_port_conflicts({
        "MN_EPMD_BIND_HOST": "127.0.0.1",
        "MN_EPMD_PORT": "54469",
        "MN_DIST_BIND_HOST": "127.0.0.1",
        "MN_DIST_PORT": "54470",
        "ERL_AFLAGS": "-kernel inet_dist_listen_min 54470 inet_dist_listen_max 54470",
    })

    assert resolved["MN_EPMD_PORT"] == "54471"
    assert resolved["MN_DIST_PORT"] == "54472"
    assert resolved["ERL_AFLAGS"] == "-kernel inet_dist_listen_min 54472 inet_dist_listen_max 54472"

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

def test_start_server_success(mocker, tmp_path, monkeypatch):
    mocker.patch('mn_cli.server_cmds.API_PID_FILE', tmp_path / "api.pid")
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
    
    assert (tmp_path / "api.pid").exists()
    assert (tmp_path / "api.pid").read_text() == "9999"
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
    admin_env = next(value for flag, value in zip(docker_run, docker_run[1:]) if flag == "-e" and value.startswith("MN_MIRROR_NEURON_GRPC_ADMIN_TOKEN="))
    runs_root_env = next(value for flag, value in zip(docker_run, docker_run[1:]) if flag == "-e" and value.startswith("MN_RUNS_ROOT="))
    assert cookie_env != "MN_COOKIE=mirrorneuron"
    assert auth_env != "MN_GRPC_AUTH_TOKEN="
    assert admin_env != "MN_MIRROR_NEURON_GRPC_ADMIN_TOKEN="
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

def test_default_web_ui_dirs_use_nested_install_path():
    assert ORIGINAL_WEB_UI_DIRS == (
        Path.home() / ".mn" / "webui",
        Path.home() / ".mn" / "web-ui-source",
    )

def test_find_web_ui_dir_installed(tmp_path, mocker):
    missing = tmp_path / "missing"
    installed = tmp_path / "web-ui"
    installed.mkdir()
    (installed / "package.json").write_text("{}")
    (installed / "node_modules").mkdir()
    mocker.patch('mn_cli.server_cmds.WEB_UI_DIRS', (missing, installed))

    assert find_web_ui_dir() == installed

def test_start_web_ui_if_installed(mocker, tmp_path):
    web_ui_dir = tmp_path / "web-ui"
    web_ui_dir.mkdir()
    (web_ui_dir / "package.json").write_text("{}")
    (web_ui_dir / "node_modules").mkdir()

    mocker.patch('mn_cli.server_cmds.WEB_UI_DIRS', (web_ui_dir,))
    mocker.patch('mn_cli.server_cmds.WEB_UI_PID_FILE', tmp_path / "web-ui.pid")
    mocker.patch('mn_cli.server_cmds.WEB_UI_LOG', tmp_path / "web-ui.log")

    mock_popen = mocker.patch('mn_cli.server_cmds.subprocess.Popen')
    mock_popen.return_value.pid = 5173

    _start_web_ui_if_installed()

    assert (tmp_path / "web-ui.pid").read_text() == "5173"
    mock_popen.assert_called_once()
    assert mock_popen.call_args.args[0][:3] == ["npm", "run", "dev"]
    assert mock_popen.call_args.args[0][-3:] == ["localhost", "--port", "55173"]
    assert mock_popen.call_args.kwargs["cwd"] == web_ui_dir

def test_start_web_ui_missing_noop(mocker, tmp_path):
    mocker.patch('mn_cli.server_cmds.WEB_UI_DIRS', (tmp_path / "web-ui",))
    mock_popen = mocker.patch('mn_cli.server_cmds.subprocess.Popen')

    _start_web_ui_if_installed()

    mock_popen.assert_not_called()

def test_start_web_ui_missing_npm_skips(mocker, tmp_path):
    web_ui_dir = tmp_path / "web-ui"
    web_ui_dir.mkdir()
    (web_ui_dir / "package.json").write_text("{}")
    (web_ui_dir / "node_modules").mkdir()

    mocker.patch('mn_cli.server_cmds.WEB_UI_DIRS', (web_ui_dir,))
    mocker.patch('mn_cli.server_cmds.WEB_UI_PID_FILE', tmp_path / "web-ui.pid")
    mocker.patch('mn_cli.server_cmds.WEB_UI_LOG', tmp_path / "web-ui.log")
    mocker.patch('mn_cli.server_cmds.subprocess.Popen', side_effect=FileNotFoundError)

    assert _start_web_ui_if_installed() is False
    assert not (tmp_path / "web-ui.pid").exists()

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
    assert "Redis" in rendered
    assert "Redis host" not in rendered
    assert "192.168.4.10" in rendered
    assert "56379" in rendered
    assert "auth required" in rendered
    assert "Context engine" not in rendered
    assert "internal" not in rendered
    assert "OpenShell" in rendered
    assert "http://127.0.0.1:58080" in rendered
    assert "Erlang EPMD" in rendered
    assert "Erlang dist" in rendered
    assert "54370" in rendered

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
    assert "192.168.4.173:54369" in rendered
    assert "192.168.4.173:54370" in rendered
    assert "redis://192.168.4.173:56379/0" in rendered
    assert "0.0.0.0" not in rendered
