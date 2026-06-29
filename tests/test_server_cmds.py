import json
import pytest
import shutil
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
    _ensure_local_cluster_runtime_for_join,
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
    monkeypatch.delenv("MN_GRPC_AUTH_TOKEN_FILE", raising=False)
    monkeypatch.delenv("MN_GRPC_ADMIN_TOKEN_FILE", raising=False)
    monkeypatch.delenv("MN_API_TOKEN", raising=False)
    monkeypatch.delenv("MN_ENV", raising=False)
    monkeypatch.delenv("MN_ARTIFACT_AUTH_TOKEN", raising=False)
    monkeypatch.delenv("MN_NODE_GPU", raising=False)
    monkeypatch.delenv("MN_NODE_GPU_COUNT", raising=False)
    monkeypatch.delenv("MN_NODE_DISPLAY_NAME", raising=False)
    monkeypatch.delenv("MN_NODE_CPU_MODEL", raising=False)
    monkeypatch.delenv("MN_NODE_GPU_VENDOR", raising=False)
    monkeypatch.delenv("MN_NODE_GPU_DRIVER", raising=False)
    monkeypatch.delenv("MN_NODE_GPU_TYPE", raising=False)
    monkeypatch.delenv("MN_NODE_GPU_NAME", raising=False)
    monkeypatch.delenv("MN_NODE_GPU_API_VERSION", raising=False)
    monkeypatch.delenv("MN_NODE_GPU_DRIVER_VERSION", raising=False)
    monkeypatch.delenv("MN_NODE_ALIAS", raising=False)
    monkeypatch.delenv("MN_BLUEPRINT_SOURCE", raising=False)
    monkeypatch.delenv("MN_BLUEPRINT_REPO", raising=False)
    monkeypatch.delenv("MN_BLUEPRINT_LOCAL", raising=False)
    monkeypatch.delenv("MN_DOCKER_NETWORK_MODE", raising=False)
    monkeypatch.delenv("MN_DOCKER_NETWORK_NAME", raising=False)
    monkeypatch.delenv("MN_REDIS_IMAGE", raising=False)
    monkeypatch.setenv("MN_NFS_ENABLED", "0")
    monkeypatch.delenv("MN_NFS_REQUIRED", raising=False)
    monkeypatch.delenv("MN_NFS_EXPORT_PATH", raising=False)
    monkeypatch.delenv("DOCKER_HOST_SOCKET", raising=False)
    monkeypatch.delenv("MN_NETWORK_JOIN_TOKEN", raising=False)
    state_dir = tmp_path / ".mn"
    log_dir = state_dir / ".logs"
    pid_dir = state_dir / ".pids"
    mocker.patch('mn_cli.server_cmds.DIR', state_dir)
    mocker.patch('mn_cli.server_cmds.PID_DIR', pid_dir)
    mocker.patch('mn_cli.server_cmds.LOG_DIR', log_dir)
    mocker.patch('mn_cli.server_cmds.BEAM_PID_FILE', pid_dir / "beam.pid")
    mocker.patch('mn_cli.server_cmds.API_PID_FILE', pid_dir / "api.pid")
    mocker.patch('mn_cli.server_cmds.API_WATCHDOG_PID_FILE', pid_dir / "api-watchdog.pid")
    mocker.patch('mn_cli.server_cmds.API_TOKEN_FILE', state_dir / "api.token")
    mocker.patch('mn_cli.server_cmds.REDIS_PASSWORD_FILE', state_dir / "redis.password")
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
    mocker.patch('mn_cli.server_cmds._docker_host_socket', return_value=None)
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

def test_detect_lan_ip_uses_first_interface_lan_ip(mocker):
    mocker.patch("mn_cli.server_cmds._interface_lan_ips", return_value=["192.168.4.35", "10.0.0.12"])

    assert server_cmds._detect_lan_ip() == "192.168.4.35"

def test_detect_lan_ip_falls_back_to_route_selected_non_loopback(mocker):
    class ProbeSocket:
        def connect(self, _target):
            pass

        def getsockname(self):
            return ("192.168.4.35", 50123)

        def close(self):
            pass

    mocker.patch("mn_cli.server_cmds._interface_lan_ips", return_value=[])
    mocker.patch("mn_cli.server_cmds.socket.socket", return_value=ProbeSocket())
    mocker.patch("mn_cli.server_cmds.socket.gethostname", return_value="loopback-host")
    mocker.patch("mn_cli.server_cmds.socket.gethostbyname", return_value="127.0.0.1")

    assert server_cmds._detect_lan_ip() == "192.168.4.35"

def test_detected_lan_ips_filters_loopback_and_deduplicates(mocker):
    class ProbeSocket:
        def connect(self, _target):
            pass

        def getsockname(self):
            return ("192.168.4.35", 50123)

        def close(self):
            pass

    mocker.patch("mn_cli.server_cmds._interface_lan_ips", return_value=["127.0.0.1", "192.168.4.35"])
    mocker.patch("mn_cli.server_cmds.socket.socket", return_value=ProbeSocket())
    mocker.patch("mn_cli.server_cmds.socket.gethostname", return_value="host")
    mocker.patch("mn_cli.server_cmds.socket.gethostbyname", return_value="10.0.0.12")

    assert server_cmds._detected_lan_ips() == ["192.168.4.35", "10.0.0.12"]

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

def test_ensure_node_advertisement_adds_cpu_and_gpu_profile(mocker):
    mocker.patch("mn_cli.server_cmds._node_display_name", return_value="lab-box")
    mocker.patch("mn_cli.server_cmds._detect_host_cpu_model", return_value="AMD Ryzen AI Max+ 395")
    mocker.patch("mn_cli.server_cmds._detect_host_gpu_count", return_value=1)
    mocker.patch(
        "mn_cli.server_cmds._detect_host_gpu_profile",
        return_value={
            "MN_NODE_GPU_VENDOR": "nvidia",
            "MN_NODE_GPU_DRIVER": "cuda",
            "MN_NODE_GPU_TYPE": "nvidia/gpu",
            "MN_NODE_GPU_NAME": "NVIDIA GB10",
            "MN_NODE_GPU_API_VERSION": "12.6",
        },
    )

    env = server_cmds._ensure_node_advertisement_settings({})

    assert env["MN_NODE_DISPLAY_NAME"] == "lab-box"
    assert env["MN_NODE_CPU_MODEL"] == "AMD Ryzen AI Max+ 395"
    assert env["MN_NODE_GPU_COUNT"] == "1"
    assert env["MN_NODE_GPU_VENDOR"] == "nvidia"
    assert env["MN_NODE_GPU_NAME"] == "NVIDIA GB10"
    assert env["MN_NODE_GPU_API_VERSION"] == "12.6"

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

    assert token == "mirror_neuron_password"
    assert not (token_dir / "grpc_auth.token").exists()
    assert _resolve_grpc_auth_token() == token

def test_resolve_grpc_auth_token_prefers_env(monkeypatch):
    monkeypatch.setenv("MN_GRPC_AUTH_TOKEN", "auth-token")

    assert _resolve_grpc_auth_token() == "mirror_neuron_password"

def test_runtime_blueprint_env_updates_prefers_host_home_dir(tmp_path):
    host_home = tmp_path / "mn-home"

    updates = _runtime_blueprint_env_updates({"MN_HOST_HOME_DIR": str(host_home)})

    assert updates["MN_HOST_ARTIFACTS_DIR"] == str(host_home / "runs")
    assert updates["MN_RUNS_ROOT"] == str(host_home / "runs")

def test_runtime_blueprint_env_updates_prefers_host_shared_storage_root(tmp_path):
    host_shared = tmp_path / "host-shared"

    updates = _runtime_blueprint_env_updates(
        {
            "MN_HOST_SHARED_STORAGE_ROOT": str(host_shared),
            "MN_SHARED_STORAGE_ROOT": "/root/.mn/shared",
            "MN_RUNTIME_SHARED_STORAGE_ROOT": "/root/.mn/shared",
        }
    )

    assert updates["MN_HOST_SHARED_STORAGE_ROOT"] == str(host_shared)
    assert updates["MN_SHARED_STORAGE_ROOT"] == str(host_shared)
    assert updates["MN_RUNTIME_SHARED_STORAGE_ROOT"] == "/root/.mn/shared"
    assert updates["MN_BUNDLE_CACHE_DIR"] == "/root/.mn/shared/bundle_cache"

def test_shared_storage_env_from_handshake_adopts_existing_primary_mount(tmp_path):
    shared_root = tmp_path / "mn-shared"
    shared_root.mkdir()

    updates = server_cmds._shared_storage_env_from_handshake(
        {
            "node_info": {
                "host_shared_storage_root": str(shared_root),
                "runtime_shared_storage_root": "/root/.mn/shared",
            }
        }
    )

    assert updates["MN_HOST_SHARED_STORAGE_ROOT"] == str(shared_root)
    assert updates["MN_SHARED_STORAGE_ROOT"] == str(shared_root)
    assert updates["MN_RUNTIME_SHARED_STORAGE_ROOT"] == "/root/.mn/shared"
    assert updates["MN_BUNDLE_CACHE_DIR"] == "/root/.mn/shared/bundle_cache"

def test_shared_storage_env_from_handshake_ignores_missing_primary_mount(tmp_path):
    missing_root = tmp_path / "missing-shared"

    updates = server_cmds._shared_storage_env_from_handshake(
        {
            "node_info": {
                "host_shared_storage_root": str(missing_root),
                "runtime_shared_storage_root": "/root/.mn/shared",
            }
        }
    )

    assert updates == {}


def test_network_core_env_preserves_configured_shared_storage(monkeypatch, tmp_path):
    host_shared = tmp_path / "nfs-shared"
    monkeypatch.setenv("MN_HOST_SHARED_STORAGE_ROOT", str(host_shared))
    monkeypatch.setenv("MN_RUNTIME_SHARED_STORAGE_ROOT", "/runtime/shared")

    env = server_cmds._network_core_env(
        token="join-token",
        host="192.168.1.10",
        docker_network_mode="disabled",
        docker_network_name="mirror-neuron-runtime",
        node_alias="",
        node_name="mirror_neuron@192.168.1.10",
        cluster_nodes="mirror_neuron@192.168.1.10",
        grpc_port=55051,
        epmd_port=54369,
        dist_port=54370,
        redis_url="redis://:secret@192.168.1.10:56379/0",
        redis_public_host="192.168.1.10",
        redis_public_port=56379,
    )

    assert env["MN_HOST_SHARED_STORAGE_ROOT"] == str(host_shared)
    assert env["MN_SHARED_STORAGE_ROOT"] == "/runtime/shared"
    assert env["MN_RUNTIME_SHARED_STORAGE_ROOT"] == "/runtime/shared"
    assert env["MN_CONTAINER_SHARED_STORAGE_ROOT"] == "/runtime/shared"
    assert env["MN_BUNDLE_CACHE_DIR"] == "/runtime/shared/bundle_cache"


def test_network_core_bind_args_mounts_configured_shared_storage(tmp_path):
    host_shared = tmp_path / "nfs-shared"

    bind_args = server_cmds._network_core_bind_args(
        {
            "MN_HOST_SHARED_STORAGE_ROOT": str(host_shared),
            "MN_RUNTIME_SHARED_STORAGE_ROOT": "/runtime/shared",
        }
    )

    runtime_mount = f"{host_shared}:/runtime/shared:rw"
    mirror_mount = f"{host_shared}:/opt/mirror_neuron/.mn/shared:rw"
    assert ["-v", f"{host_shared}:/runtime/shared:rw"] == bind_args[
        bind_args.index(runtime_mount) - 1 : bind_args.index(runtime_mount) + 1
    ]
    assert ["-v", f"{host_shared}:/opt/mirror_neuron/.mn/shared:rw"] == bind_args[
        bind_args.index(mirror_mount) - 1 : bind_args.index(mirror_mount) + 1
    ]


def test_ensure_nfs_export_for_cluster_runs_platform_export(mocker, tmp_path, monkeypatch):
    host_shared = tmp_path / "shared"
    monkeypatch.setenv("MN_NFS_ENABLED", "auto")
    mocker.patch("mn_cli.server_cmds.os.uname", return_value=mocker.Mock(sysname="Darwin"))
    run = mocker.patch("mn_cli.server_cmds.subprocess.run", return_value=mocker.Mock(returncode=0))

    server_cmds._ensure_nfs_export_for_cluster(
        {
            "MN_HOST_SHARED_STORAGE_ROOT": str(host_shared),
            "MN_RUNTIME_SHARED_STORAGE_ROOT": "/root/.mn/shared",
        },
        advertised_host="192.168.6.28",
    )

    assert host_shared.is_dir()
    run.assert_called_once()
    assert run.call_args.args[0][:4] == ["sudo", "-n", "sh", "-c"]
    assert str(host_shared) in run.call_args.args[0][4]


def test_ensure_nfs_mount_from_handshake_mounts_missing_primary_path(mocker, tmp_path, monkeypatch):
    host_shared = tmp_path / "primary-shared"
    monkeypatch.setenv("MN_NFS_ENABLED", "auto")
    mocker.patch("mn_cli.server_cmds.os.uname", return_value=mocker.Mock(sysname="Linux"))
    mocker.patch("mn_cli.server_cmds._path_is_mountpoint", return_value=False)
    run = mocker.patch("mn_cli.server_cmds.subprocess.run", return_value=mocker.Mock(returncode=0))

    server_cmds._ensure_nfs_mount_from_handshake(
        "192.168.6.28",
        {
            "node_info": {
                "host_shared_storage_root": str(host_shared),
                "runtime_shared_storage_root": "/root/.mn/shared",
            }
        },
        {},
    )

    assert run.call_args_list[0].args[0] == ["sudo", "-n", "mkdir", "-p", str(host_shared)]
    assert run.call_args_list[1].args[0] == [
        "sudo",
        "-n",
        "mount",
        "-t",
        "nfs",
        "-o",
        "vers=3,tcp,nolock",
        f"192.168.6.28:{host_shared}",
        str(host_shared),
    ]


def test_deploy_compose_passes_host_shared_storage_to_core():
    compose_path = Path(__file__).resolve().parents[2] / "mn-deploy" / "docker-compose.yml"
    if not compose_path.exists():
        pytest.skip("mn-deploy checkout is not available")

    compose_text = compose_path.read_text(encoding="utf-8")

    assert "MN_HOST_SHARED_STORAGE_ROOT:" in compose_text
    assert "${MN_HOST_SHARED_STORAGE_ROOT:-${MN_SHARED_STORAGE_ROOT:-" in compose_text
    assert "membrane-context-engine:" in compose_text
    assert "MN_CONTEXT_MODEL_ENDPOINT" in compose_text
    assert "MN_CONTEXT_MODEL_NAME" in compose_text

def test_runtime_blueprint_env_updates_ignores_legacy_host_mn_dir(tmp_path):
    legacy_home = tmp_path / "legacy-mn-home"

    updates = _runtime_blueprint_env_updates({"MN_HOST_MN_DIR": str(legacy_home)})

    assert updates["MN_HOST_ARTIFACTS_DIR"] == str(server_cmds.DIR / "runs")
    assert updates["MN_RUNS_ROOT"] == str(server_cmds.DIR / "runs")

def test_resolve_grpc_admin_token_generates_persistent_token(tmp_path, mocker):
    token_dir = tmp_path / "state"
    mocker.patch('mn_cli.server_cmds.DIR', token_dir)

    token = _resolve_grpc_admin_token()

    assert token == "mirror_neuron_password_admin"
    assert not (token_dir / "grpc_admin.token").exists()
    assert _resolve_grpc_admin_token() == token

def test_resolve_grpc_admin_token_prefers_env(monkeypatch):
    monkeypatch.setenv("MN_GRPC_ADMIN_TOKEN", "admin-token")

    assert _resolve_grpc_admin_token() == "mirror_neuron_password_admin"

def test_resolve_grpc_admin_token_ignores_legacy_env(monkeypatch):
    monkeypatch.setenv("MN_MIRROR_NEURON_GRPC_ADMIN_TOKEN", "legacy-admin-token")

    assert _resolve_grpc_admin_token() == "mirror_neuron_password_admin"

def test_resolve_redis_password_uses_fixed_dev_password():
    assert server_cmds._resolve_redis_password({"MN_ENV": "dev"}) == "mirror_neuron_redis_dev"
    assert not server_cmds.REDIS_PASSWORD_FILE.exists()

def test_resolve_redis_password_derives_from_admin_token_in_prod():
    expected = server_cmds._derive_redis_password("prod-admin-token")

    password = server_cmds._resolve_redis_password(
        {"MN_ENV": "prod", "MN_GRPC_ADMIN_TOKEN": "prod-admin-token"}
    )

    assert password == expected
    assert server_cmds.REDIS_PASSWORD_FILE.read_text().strip() == expected
    assert server_cmds.REDIS_PASSWORD_FILE.stat().st_mode & 0o777 == 0o600

def test_runtime_api_token_is_generated_for_prod_and_persisted(mocker):
    mocker.patch("mn_cli.server_cmds.secrets.token_hex", return_value="generated-api-token")

    env = server_cmds._ensure_runtime_api_token({"MN_ENV": "prod"})

    assert env["MN_API_TOKEN"] == "generated-api-token"
    assert server_cmds.API_TOKEN_FILE.read_text().strip() == "generated-api-token"
    assert server_cmds.API_TOKEN_FILE.stat().st_mode & 0o777 == 0o600
    assert server_cmds._ensure_runtime_api_token({"MN_ENV": "prod"})["MN_API_TOKEN"] == "generated-api-token"

def test_runtime_api_token_prefers_env_and_persists_compose(mocker, monkeypatch):
    monkeypatch.setenv("MN_API_TOKEN", "explicit-api-token")
    compose_file = server_cmds.RUNTIME_COMPOSE_FILE
    compose_env = server_cmds.RUNTIME_COMPOSE_ENV
    compose_file.parent.mkdir(parents=True, exist_ok=True)
    compose_file.write_text("services: {}\n", encoding="utf-8")
    compose_env.write_text("COMPOSE_PROJECT_NAME=mirror-neuron\n", encoding="utf-8")

    env = server_cmds._ensure_runtime_api_token({"MN_ENV": "prod"}, persist_compose=True)

    assert env["MN_API_TOKEN"] == "explicit-api-token"
    assert server_cmds.API_TOKEN_FILE.read_text().strip() == "explicit-api-token"
    assert "MN_API_TOKEN=explicit-api-token" in compose_env.read_text()

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
    docker_run = next(cmd for cmd in commands if cmd[:3] == ["docker", "run", "-d"])
    assert "MN_GRPC_AUTH_TOKEN=mirror_neuron_password" in docker_run
    assert "MN_GRPC_ADMIN_TOKEN=mirror_neuron_password_admin" in docker_run
    assert not any(value.startswith("MN_GRPC_AUTH_TOKEN_FILE=") for value in docker_run)
    assert not any(value.startswith("MN_GRPC_ADMIN_TOKEN_FILE=") for value in docker_run)
    assert not any(value.startswith("MN_ARTIFACT_AUTH_TOKEN=") for value in docker_run)
    assert not (server_cmds.DIR / "grpc_auth.token").exists()
    assert not (server_cmds.DIR / "grpc_admin.token").exists()

def test_start_server_refreshes_token_files_from_compose_runtime_env(mocker, tmp_path):
    compose_file = tmp_path / "docker-compose.yml"
    compose_env = tmp_path / "docker-compose.env"
    compose_file.write_text("services: {}\n")
    compose_env.write_text(
        "COMPOSE_PROJECT_NAME=mirror-neuron\n"
        "MN_DOCKER_NETWORK_MODE=disabled\n"
        "MN_GRPC_AUTH_TOKEN=compose-auth-token\n"
        "MN_GRPC_ADMIN_TOKEN=compose-admin-token\n"
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

    assert (server_cmds.DIR / "grpc_auth.token").read_text().strip() == "stale-auth-token"
    assert (server_cmds.DIR / "grpc_admin.token").read_text().strip() == "stale-admin-token"
    assert "MN_GRPC_AUTH_TOKEN=mirror_neuron_password" in compose_env.read_text()
    assert "MN_GRPC_ADMIN_TOKEN=mirror_neuron_password_admin" in compose_env.read_text()
    assert "MN_GRPC_AUTH_TOKEN_FILE=" not in compose_env.read_text()
    assert "MN_GRPC_ADMIN_TOKEN_FILE=" not in compose_env.read_text()

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
        "MN_GRPC_AUTH_TOKEN": "mirror_neuron_password",
        "MN_GRPC_ADMIN_TOKEN": "mirror_neuron_password_admin",
    }

def test_runtime_grpc_tokens_from_running_container_refreshes_compose_env(mocker):
    compose_env = server_cmds.RUNTIME_COMPOSE_ENV
    compose_env.parent.mkdir(parents=True, exist_ok=True)
    compose_env.write_text(
        "COMPOSE_PROJECT_NAME=mirror-neuron\n"
        "MN_GRPC_AUTH_TOKEN=stale-auth-token\n"
        "MN_GRPC_ADMIN_TOKEN=stale-admin-token\n",
        encoding="utf-8",
    )
    tokens = {
        "MN_GRPC_AUTH_TOKEN": "running-auth-token",
        "MN_GRPC_ADMIN_TOKEN": "running-admin-token",
    }

    server_cmds._ensure_runtime_grpc_tokens(tokens, persist_compose=True)

    assert not (server_cmds.DIR / "grpc_auth.token").exists()
    assert not (server_cmds.DIR / "grpc_admin.token").exists()
    compose_text = compose_env.read_text(encoding="utf-8")
    assert "MN_GRPC_AUTH_TOKEN=mirror_neuron_password" in compose_text
    assert "MN_GRPC_ADMIN_TOKEN=mirror_neuron_password_admin" in compose_text

def test_runtime_base_env_scrubs_deprecated_artifact_auth_token(monkeypatch):
    compose_env = server_cmds.RUNTIME_COMPOSE_ENV
    compose_file = server_cmds.RUNTIME_COMPOSE_FILE
    compose_env.parent.mkdir(parents=True, exist_ok=True)
    compose_env.write_text(
        "COMPOSE_PROJECT_NAME=mirror-neuron\n"
        "MN_ARTIFACT_AUTH_TOKEN=stale-artifact-token\n"
        "MN_NETWORK_JOIN_TOKEN=join-token\n",
        encoding="utf-8",
    )
    compose_file.write_text(
        "services:\n"
        "  mirror-neuron-core:\n"
        "    environment:\n"
        "      MN_ARTIFACT_AUTH_TOKEN: ${MN_ARTIFACT_AUTH_TOKEN:-}\n"
        "      MN_NETWORK_JOIN_TOKEN: ${MN_NETWORK_JOIN_TOKEN:-}\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("MN_ARTIFACT_AUTH_TOKEN", "ambient-artifact-token")

    env = server_cmds._runtime_base_env(True)

    assert env["MN_NETWORK_JOIN_TOKEN"] == "join-token"
    assert "MN_ARTIFACT_AUTH_TOKEN" not in env
    assert "MN_ARTIFACT_AUTH_TOKEN" not in compose_env.read_text(encoding="utf-8")
    assert "MN_ARTIFACT_AUTH_TOKEN" not in compose_file.read_text(encoding="utf-8")

def test_runtime_base_env_advertises_installed_runtime_models(mocker):
    compose_env = server_cmds.RUNTIME_COMPOSE_ENV
    compose_env.parent.mkdir(parents=True, exist_ok=True)
    compose_env.write_text(
        "COMPOSE_PROJECT_NAME=mirror-neuron\n"
        "MN_NODE_MODELS=\n"
        "MN_NODE_RUNTIME_MODELS=\n",
        encoding="utf-8",
    )
    mocker.patch(
        "mn_cli.server_cmds._installed_catalog_runtime_models",
        return_value=["gemma4:e2b"],
    )

    env = server_cmds._runtime_base_env(True)

    assert env["MN_NODE_RUNTIME_MODELS"] == "gemma4:e2b"

def test_runtime_base_env_keeps_explicit_runtime_models(mocker):
    mocker.patch(
        "mn_cli.server_cmds._installed_catalog_runtime_models",
        return_value=["gemma4:e2b"],
    )

    env = server_cmds._ensure_installed_runtime_model_env(
        {"MN_NODE_RUNTIME_MODELS": "custom:model"}
    )

    assert env["MN_NODE_RUNTIME_MODELS"] == "custom:model"

def test_record_runtime_model_install_writes_compose_model_override():
    server_cmds.RUNTIME_COMPOSE_ENV.parent.mkdir(parents=True, exist_ok=True)
    server_cmds.RUNTIME_COMPOSE_ENV.write_text(
        "COMPOSE_PROJECT_NAME=mirror-neuron\n"
        "MN_NODE_RUNTIME_MODELS=\n",
        encoding="utf-8",
    )

    path = server_cmds.record_runtime_model_install(
        {
            "id": "gemma4:e2b",
            "model": "ai/gemma4:E2B",
            "aliases": ["default", "gemma4"],
        }
    )

    env = server_cmds._read_env_file(server_cmds.RUNTIME_COMPOSE_ENV)
    assert env["MN_NODE_RUNTIME_MODELS"] == "gemma4:e2b"
    assert env["MN_LLM_MODEL_RUNNER_MODEL"] == "ai/gemma4:E2B"
    assert path == server_cmds._runtime_compose_models_override_file()
    override = path.read_text(encoding="utf-8")
    assert "mirror-neuron-core:" in override
    assert "endpoint_var: MN_DOCKER_MODEL_RUNNER_API_BASE" in override
    assert "model_var: MN_DOCKER_MODEL_RUNNER_MODEL" in override
    assert 'model: "${MN_LLM_MODEL_RUNNER_MODEL:-ai/gemma4:E2B}"' in override

def test_ensure_context_engine_runtime_persists_profile_and_starts_compose(mocker, tmp_path):
    membrane_dir = tmp_path / "Membrane"
    membrane_dir.mkdir()
    (membrane_dir / "Dockerfile").write_text("FROM scratch\n", encoding="utf-8")
    server_cmds.RUNTIME_COMPOSE_ENV.parent.mkdir(parents=True, exist_ok=True)
    server_cmds.RUNTIME_COMPOSE_ENV.write_text(
        "COMPOSE_PROJECT_NAME=mirror-neuron\n"
        "COMPOSE_PROFILES=openshell\n",
        encoding="utf-8",
    )
    server_cmds.RUNTIME_COMPOSE_FILE.write_text("services: {}\n", encoding="utf-8")
    mocker.patch("mn_cli.server_cmds._ensure_context_engine_source", return_value=membrane_dir)
    mocker.patch("mn_cli.server_cmds._ensure_docker_model_runner")
    inspect_model = mocker.patch("mn_cli.server_cmds._docker_model_inspect_ok", return_value=False)
    mocker.patch("mn_cli.server_cmds._remove_non_mirror_neuron_container")
    mocker.patch("mn_cli.server_cmds._docker_container_running", return_value=False)
    run = mocker.patch(
        "mn_cli.server_cmds.subprocess.run",
        return_value=subprocess.CompletedProcess([], 0, "", ""),
    )

    result = server_cmds.ensure_context_engine_runtime()

    env = server_cmds._read_env_file(server_cmds.RUNTIME_COMPOSE_ENV)
    assert env["COMPOSE_PROFILES"] == "openshell,context"
    assert env["MEMBRANE_DIR"] == str(membrane_dir)
    assert env["MN_CONTEXT_MODEL_RUNNER_MODEL"] == server_cmds.DEFAULT_CONTEXT_MODEL_RUNNER_MODEL
    assert result["status"] == "started"
    assert result["model_status"] == "installed"
    assert inspect_model.call_args_list[0].args[0] == server_cmds.DEFAULT_CONTEXT_MODEL_RUNNER_MODEL
    assert run.call_args_list[0].args[0] == [
        "docker",
        "model",
        "pull",
        server_cmds.DEFAULT_CONTEXT_MODEL_RUNNER_MODEL,
    ]
    assert run.call_args_list[1].args[0] == [
        "docker",
        "model",
        "run",
        "--detach",
        server_cmds.DEFAULT_CONTEXT_MODEL_RUNNER_MODEL,
    ]
    assert run.call_args_list[2].args[0] == runtime_compose_cmd("build", "membrane-context-engine")
    assert run.call_args_list[3].args[0] == runtime_compose_cmd("up", "-d", "membrane-context-engine")

def test_ensure_context_engine_runtime_uses_release_image_without_source_clone(mocker, monkeypatch):
    monkeypatch.setenv("PATH", "/usr/local/bin:/usr/bin:/bin")
    server_cmds.RUNTIME_COMPOSE_ENV.parent.mkdir(parents=True, exist_ok=True)
    server_cmds.RUNTIME_COMPOSE_ENV.write_text(
        "COMPOSE_PROJECT_NAME=mirror-neuron\n"
        "COMPOSE_PROFILES=openshell\n"
        "MN_RUNTIME_MODULE_VERSION=1.2.7\n"
        "MEMBRANE_DIR=/private/membrane\n",
        encoding="utf-8",
    )
    server_cmds.RUNTIME_COMPOSE_FILE.write_text("services: {}\n", encoding="utf-8")
    ensure_source = mocker.patch("mn_cli.server_cmds._ensure_context_engine_source")
    mocker.patch("mn_cli.server_cmds._ensure_docker_model_runner")
    inspect_model = mocker.patch("mn_cli.server_cmds._docker_model_inspect_ok", return_value=True)
    mocker.patch("mn_cli.server_cmds._remove_non_mirror_neuron_container")
    mocker.patch("mn_cli.server_cmds._docker_container_running", return_value=False)
    run = mocker.patch("mn_cli.server_cmds.subprocess.run")

    result = server_cmds.ensure_context_engine_runtime()

    env = server_cmds._read_env_file(server_cmds.RUNTIME_COMPOSE_ENV)
    expected_image = (
        "us-central1-docker.pkg.dev/mirrorneuron-public-packages/"
        "mirrorneuron-runtime/membrane-context-engine:v1.2.7"
    )
    assert env["COMPOSE_PROFILES"] == "openshell,context"
    assert env["ENGINE_IMAGE"] == expected_image
    assert env["MN_MEMBRANE_ENGINE_IMAGE"] == expected_image
    assert "MEMBRANE_DIR" not in env
    assert result["status"] == "started"
    assert result["model_status"] == "already_installed"
    assert result["engine_image"] == expected_image
    ensure_source.assert_not_called()
    inspect_model.assert_called_once_with(server_cmds.DEFAULT_CONTEXT_MODEL_RUNNER_MODEL)
    assert run.call_args_list[0].args[0] == runtime_compose_cmd("pull", "membrane-context-engine")
    assert run.call_args_list[1].args[0] == runtime_compose_cmd("up", "-d", "--no-build", "membrane-context-engine")
    assert run.call_args_list[0].kwargs["env"]["DOCKER_CONFIG"] != str(Path.home() / ".docker")
    assert run.call_args_list[0].kwargs["env"]["PATH"] == "/usr/local/bin:/usr/bin:/bin"

def test_public_gar_docker_env_strips_gcloud_helpers(monkeypatch, tmp_path):
    docker_config = tmp_path / "docker-config"
    docker_config.mkdir()
    docker_plugins = docker_config / "cli-plugins"
    docker_plugins.mkdir()
    (docker_plugins / "docker-compose").write_text("#!/bin/sh\n", encoding="utf-8")
    docker_context = docker_config / "contexts" / "meta" / "desktop-linux"
    docker_context.mkdir(parents=True)
    (docker_context / "meta.json").write_text('{"Name":"desktop-linux"}\n', encoding="utf-8")
    (docker_config / "config.json").write_text(
        json.dumps(
            {
                "currentContext": "desktop-linux",
                "auths": {
                    "https://us-central1-docker.pkg.dev": {"auth": "private"},
                    "ghcr.io": {"auth": "keep"},
                },
                "credHelpers": {
                    "us-central1-docker.pkg.dev": "gcloud",
                    "ghcr.io": "ghcr",
                },
                "credsStore": "desktop",
                "proxies": {"default": {"httpProxy": "http://proxy.test"}},
            }
        ),
        encoding="utf-8",
    )
    env = {"DOCKER_CONFIG": str(docker_config)}

    next_env, temp_config_dir = server_cmds._anonymous_public_gar_docker_env(
        env,
        "us-central1-docker.pkg.dev/mirrorneuron-public-packages/mirrorneuron-runtime/membrane-context-engine:v1.2.8",
    )

    try:
        assert temp_config_dir is not None
        assert next_env["DOCKER_CONFIG"] == str(temp_config_dir)
        assert next_env["DOCKER_CONFIG"] != env["DOCKER_CONFIG"]
        sanitized = json.loads((temp_config_dir / "config.json").read_text(encoding="utf-8"))
        assert "credsStore" not in sanitized
        assert sanitized["credHelpers"] == {"ghcr.io": "ghcr"}
        assert sanitized["auths"] == {"ghcr.io": {"auth": "keep"}}
        assert sanitized["proxies"] == {"default": {"httpProxy": "http://proxy.test"}}
        assert sanitized["currentContext"] == "desktop-linux"
        assert (temp_config_dir / "cli-plugins" / "docker-compose").exists()
        assert (temp_config_dir / "contexts" / "meta" / "desktop-linux" / "meta.json").exists()
    finally:
        shutil.rmtree(temp_config_dir, ignore_errors=True)

def test_ensure_context_engine_runtime_skips_compose_when_already_running(mocker, tmp_path):
    membrane_dir = tmp_path / "Membrane"
    membrane_dir.mkdir()
    (membrane_dir / "Dockerfile").write_text("FROM scratch\n", encoding="utf-8")
    server_cmds.RUNTIME_COMPOSE_ENV.parent.mkdir(parents=True, exist_ok=True)
    server_cmds.RUNTIME_COMPOSE_ENV.write_text(
        "COMPOSE_PROJECT_NAME=mirror-neuron\n"
        "COMPOSE_PROFILES=context\n"
        "MN_CONTEXT_MODEL_RUNNER_MODEL=hf.co/acme/context\n",
        encoding="utf-8",
    )
    server_cmds.RUNTIME_COMPOSE_FILE.write_text("services: {}\n", encoding="utf-8")
    mocker.patch("mn_cli.server_cmds._ensure_context_engine_source", return_value=membrane_dir)
    mocker.patch("mn_cli.server_cmds._ensure_docker_model_runner")
    inspect_model = mocker.patch("mn_cli.server_cmds._docker_model_inspect_ok", return_value=True)
    mocker.patch("mn_cli.server_cmds._remove_non_mirror_neuron_container")
    mocker.patch("mn_cli.server_cmds._docker_container_running", return_value=True)
    run = mocker.patch("mn_cli.server_cmds.subprocess.run")

    result = server_cmds.ensure_context_engine_runtime()

    assert result["status"] == "already_running"
    assert result["model"] == "hf.co/acme/context"
    assert result["model_status"] == "already_installed"
    inspect_model.assert_called_once_with("hf.co/acme/context")
    run.assert_not_called()

def test_ensure_context_engine_runtime_installs_missing_model_without_compose_restart(mocker, tmp_path):
    membrane_dir = tmp_path / "Membrane"
    membrane_dir.mkdir()
    (membrane_dir / "Dockerfile").write_text("FROM scratch\n", encoding="utf-8")
    server_cmds.RUNTIME_COMPOSE_ENV.parent.mkdir(parents=True, exist_ok=True)
    server_cmds.RUNTIME_COMPOSE_ENV.write_text(
        "COMPOSE_PROJECT_NAME=mirror-neuron\n"
        "COMPOSE_PROFILES=context\n"
        "MN_CONTEXT_MODEL_RUNNER_MODEL=hf.co/acme/context\n",
        encoding="utf-8",
    )
    server_cmds.RUNTIME_COMPOSE_FILE.write_text("services: {}\n", encoding="utf-8")
    mocker.patch("mn_cli.server_cmds._ensure_context_engine_source", return_value=membrane_dir)
    mocker.patch("mn_cli.server_cmds._ensure_docker_model_runner")
    mocker.patch("mn_cli.server_cmds._docker_model_inspect_ok", return_value=False)
    mocker.patch("mn_cli.server_cmds._remove_non_mirror_neuron_container")
    mocker.patch("mn_cli.server_cmds._docker_container_running", return_value=True)
    run = mocker.patch(
        "mn_cli.server_cmds.subprocess.run",
        return_value=subprocess.CompletedProcess([], 0, "", ""),
    )

    result = server_cmds.ensure_context_engine_runtime()

    assert result["status"] == "already_running"
    assert result["model_status"] == "installed"
    assert run.call_args_list[0].args[0] == ["docker", "model", "pull", "hf.co/acme/context"]
    assert run.call_args_list[1].args[0] == ["docker", "model", "run", "--detach", "hf.co/acme/context"]
    assert len(run.call_args_list) == 2

def test_runtime_compose_cmd_includes_models_override():
    server_cmds.RUNTIME_COMPOSE_ENV.parent.mkdir(parents=True, exist_ok=True)
    server_cmds.RUNTIME_COMPOSE_ENV.write_text("COMPOSE_PROJECT_NAME=mirror-neuron\n", encoding="utf-8")
    server_cmds.RUNTIME_COMPOSE_FILE.write_text("services: {}\n", encoding="utf-8")
    models_override = server_cmds._runtime_compose_models_override_file()
    models_override.write_text("services: {}\n", encoding="utf-8")

    command = runtime_compose_cmd("up", "-d")

    assert "-f" in command
    assert str(models_override) in command
    assert command[-2:] == ["up", "-d"]

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
    (server_cmds.DIR / "grpc_auth.token").write_text("stable-auth-token\n")
    (server_cmds.DIR / "grpc_admin.token").write_text("stable-admin-token\n")
    server_cmds.RUNTIME_COMPOSE_ENV.parent.mkdir(parents=True, exist_ok=True)
    server_cmds.RUNTIME_COMPOSE_ENV.write_text(
        "MN_NETWORK_JOIN_TOKEN=old-token\n"
        "MN_GRPC_AUTH_TOKEN=stable-auth-token\n"
        "MN_GRPC_ADMIN_TOKEN=stable-admin-token\n"
    )
    mocker.patch('mn_cli.server_cmds.secrets.token_urlsafe', return_value="new-token")

    assert _refresh_network_token() == "new-token"
    assert server_cmds.NETWORK_TOKEN_FILE.read_text().strip() == "new-token"
    assert (server_cmds.DIR / "grpc_auth.token").read_text().strip() == "stable-auth-token"
    assert (server_cmds.DIR / "grpc_admin.token").read_text().strip() == "stable-admin-token"
    compose_text = server_cmds.RUNTIME_COMPOSE_ENV.read_text()
    assert "MN_NETWORK_JOIN_TOKEN=new-token" in compose_text
    assert "MN_GRPC_AUTH_TOKEN=stable-auth-token" in compose_text
    assert "MN_GRPC_ADMIN_TOKEN=stable-admin-token" in compose_text

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
    image_index = core_run.index("mirror-neuron-core:latest")
    assert core_run[image_index + 1 : image_index + 3] == ["sh", "-c"]
    assert "epmd_bin=" in core_run[image_index + 3]
    assert "RELEASE_DISTRIBUTION=name" in core_run[image_index + 3]
    assert "redis:7" not in core_run
    assert core_run.count("-p") == 1
    assert "0.0.0.0:50055:50055" in core_run
    assert "192.168.4.10:4369:4369" not in core_run
    assert "192.168.4.10:4500:4500" not in core_run
    assert f"MN_COOKIE={_derive_network_secret('seed-token', 'cookie')}" in core_run
    assert f"MN_GRPC_AUTH_TOKEN={_derive_network_secret('seed-token', 'grpc-auth')}" not in core_run
    assert f"MN_GRPC_ADMIN_TOKEN={_derive_network_secret('seed-token', 'grpc-admin')}" not in core_run
    assert any(value.startswith("MN_GRPC_AUTH_TOKEN=") and value != "MN_GRPC_AUTH_TOKEN=" for value in core_run)
    assert any(value.startswith("MN_GRPC_ADMIN_TOKEN=") and value != "MN_GRPC_ADMIN_TOKEN=" for value in core_run)
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
    assert f"MN_REDIS_URL=redis://:{server_cmds.DEV_REDIS_PASSWORD}@mn-seed-redis:6379/0" in core_run
    assert "MN_NETWORK_REDIS_HOST=mn-seed-redis" in core_run
    redis_run = next(cmd for cmd in commands if len(cmd) > 4 and cmd[:4] == ["docker", "run", "-d", "--name"] and cmd[4] == "mirror-neuron-network-redis")
    assert "--network-alias" in redis_run
    assert "mn-seed-redis" in redis_run
    assert "-p" not in redis_run
    redis_primary = next(
        cmd for cmd in commands if cmd[:3] == ["docker", "exec", "mirror-neuron-network-redis"]
    )
    assert "REPLICAOF NO ONE" in redis_primary[-1]
    assert "mn:network-redis:write-probe" in redis_primary[-1]
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
    mocker.patch('mn_cli.server_cmds._docker_host_socket', return_value=None)

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
        f"MN_REDIS_URL=redis://:{server_cmds.DEV_REDIS_PASSWORD}"
        f"@192.168.4.173:{server_cmds.REDIS_DYNAMIC_PORT_START}/0"
    ) in core_run
    assert "MN_NETWORK_REDIS_HOST=192.168.4.173" in core_run
    assert f"MN_NETWORK_REDIS_PORT={server_cmds.REDIS_DYNAMIC_PORT_START}" in core_run
    assert "MN_DOCKER_NETWORK_MODE=disabled" in core_run
    assert "MN_NODE_NAME=mirror_neuron@192.168.4.173" in core_run

def test_start_network_seed_uses_configured_redis_image(mocker, tmp_path, monkeypatch):
    monkeypatch.delenv("MN_REDIS_URL", raising=False)
    monkeypatch.setenv("MN_REDIS_IMAGE", "redis:8.8")
    token_file = tmp_path / "network.token"
    mocker.patch('mn_cli.server_cmds.DIR', tmp_path)
    mocker.patch('mn_cli.server_cmds.NETWORK_TOKEN_FILE', token_file)
    mocker.patch('mn_cli.server_cmds.NETWORK_REDIS_ENV_FILE', tmp_path / "network-redis.env")
    mocker.patch('mn_cli.server_cmds.secrets.token_urlsafe', return_value="worker-token")
    mocker.patch('mn_cli.server_cmds._docker_container_running', return_value=False)
    mocker.patch('mn_cli.server_cmds._port_available_or_owned', return_value=True)
    mocker.patch('mn_cli.server_cmds._docker_host_socket', return_value=None)

    commands = []

    def mock_run(cmd, **kwargs):
        commands.append(cmd)
        m = mocker.Mock()
        m.returncode = 0
        m.stdout = "false\n"
        return m

    mocker.patch('mn_cli.server_cmds.subprocess.run', side_effect=mock_run)

    _start_network_seed(host="192.168.4.173", grpc_port=50055, dist_port=4500)

    redis_run = next(cmd for cmd in commands if len(cmd) > 4 and cmd[:4] == ["docker", "run", "-d", "--name"] and cmd[4] == "mirror-neuron-network-redis")
    assert "redis:8.8" in redis_run

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
    assert "MirrorNeuron node ready confirmed." in rendered
    assert "Status: already running" in rendered
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
    assert "MirrorNeuron node ready confirmed." in rendered
    assert "Status: already running" in rendered
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
    refresh_token.assert_not_called()
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

def test_sidecar_pid_files_include_legacy_checkout_paths():
    legacy_pid_dir = server_cmds._legacy_checkout_pid_dir()

    assert (legacy_pid_dir / "api-watchdog.pid", "REST API watchdog") in server_cmds.api_pid_files()
    assert (legacy_pid_dir / "api.pid", "REST API") in server_cmds.api_pid_files()
    assert (legacy_pid_dir / "web-ui-watchdog.pid", "Web UI watchdog") in server_cmds.web_ui_pid_files()
    assert (legacy_pid_dir / "web-ui.pid", "Web UI") in server_cmds.web_ui_pid_files()

def test_persist_compose_cluster_node_appends_remote_once():
    server_cmds.RUNTIME_COMPOSE_ENV.parent.mkdir(parents=True, exist_ok=True)
    server_cmds.RUNTIME_COMPOSE_ENV.write_text("MN_CLUSTER_NODES=mirror_neuron@local\n")

    server_cmds._persist_compose_cluster_node("mirror_neuron@worker")
    server_cmds._persist_compose_cluster_node("mirror_neuron@worker")

    env = server_cmds._read_env_file(server_cmds.RUNTIME_COMPOSE_ENV)
    assert env["MN_CLUSTER_NODES"] == "mirror_neuron@local,mirror_neuron@worker"


def test_add_node_uses_handshake_and_local_core(mocker, tmp_path, capsys):
    import mn_sdk
    import mn_cli.shared

    mocker.patch('mn_cli.server_cmds.DIR', tmp_path)
    mocker.patch('mn_cli.server_cmds._docker_container_running', return_value=False)
    mocker.patch('mn_cli.server_cmds._detect_host_gpu_count', return_value=1)
    mocker.patch('mn_cli.server_cmds._detect_lan_ip', return_value="192.168.4.99")
    redis_password = "remote-redis-password"

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
    output = capsys.readouterr().out
    assert "Node join successful." in output
    assert "Status: connected" in output
    assert "Node: mirror_neuron@192.168.4.10" in output
    assert "Remote Redis: 192.168.4.10:6380" in output
    assert "Remote Redis URL:" in output
    assert "redis://:" in output
    assert "Next: mn node list" in output
    assert "Next: mn resource list" in output

def test_add_node_overlay_uses_local_alias_in_handshake(mocker, tmp_path, monkeypatch):
    import mn_sdk
    import mn_cli.shared

    monkeypatch.setenv("MN_NODE_ALIAS", "mn-main")
    redis_password = "remote-redis-password"
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
    redis_password = "remote-redis-password"
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

def test_join_network_configures_worker_redis_replica(mocker, tmp_path, capsys):
    import mn_sdk
    import mn_cli.shared

    compose_file = server_cmds.RUNTIME_COMPOSE_FILE
    compose_env = server_cmds.RUNTIME_COMPOSE_ENV
    primary_password = "primary-redis-password"
    worker_password = "worker-redis-password"
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
    mocker.patch('mn_cli.server_cmds._ensure_local_cluster_runtime_for_join')

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
    assert not any(call[3][:3] == ("CONFIG", "SET", "requirepass") for call in redis_calls)
    assert (
        "192.168.4.99",
        56379,
        primary_password,
        ("WAIT", "1", "1000"),
    ) in redis_calls
    output = capsys.readouterr().out
    assert "Node join successful." in output
    assert "Status: connected" in output
    assert "Replication: 192.168.4.20:56380 -> 192.168.4.99:56379" in output

def test_join_promotes_local_compose_runtime_to_cluster_mode(mocker, tmp_path):
    compose_file = server_cmds.RUNTIME_COMPOSE_FILE
    compose_env = server_cmds.RUNTIME_COMPOSE_ENV
    compose_file.parent.mkdir(parents=True, exist_ok=True)
    compose_file.write_text("services: {}\n", encoding="utf-8")
    compose_env.write_text(
        "COMPOSE_PROJECT_NAME=mirror-neuron\n"
        "MN_NETWORK_JOIN_TOKEN=primary-token\n"
        "MN_NODE_NAME=\n"
        "MN_CLUSTER_NODES=\n"
        "MN_GRPC_BIND_HOST=127.0.0.1\n",
        encoding="utf-8",
    )
    mocker.patch('mn_cli.server_cmds._port_available_or_owned', return_value=True)
    mocker.patch('mn_cli.server_cmds._published_container_port', return_value=56379)
    mocker.patch('mn_cli.server_cmds._detect_host_cpu_model', return_value="")
    mocker.patch('mn_cli.server_cmds._detect_host_gpu_count', return_value=0)
    mocker.patch('mn_cli.server_cmds._wait_for_local_cluster_grpc')
    mock_run = mocker.patch('mn_cli.server_cmds.subprocess.run')

    _ensure_local_cluster_runtime_for_join(
        local_host="192.168.4.20",
        node_name="mirror_neuron@192.168.4.20",
        docker_network_mode="disabled",
        docker_network_name="mirror-neuron-runtime",
    )

    env_text = compose_env.read_text(encoding="utf-8")
    assert "MN_NODE_NAME=mirror_neuron@192.168.4.20" in env_text
    assert "MN_MODEL_SERVICE_NODE_NAME=mirror_neuron@192.168.4.20" in env_text
    assert "MN_CLUSTER_NODES=mirror_neuron@192.168.4.20" in env_text
    assert "MN_NETWORK_ADVERTISE_HOST=192.168.4.20" in env_text
    assert "MN_GRPC_BIND_HOST=0.0.0.0" in env_text
    assert mock_run.call_args_list[0].args[0] == runtime_compose_cmd(
        "up", "-d", "--force-recreate", "redis", "mirror-neuron-core"
    )
    assert mock_run.call_args_list[1].args[0][:3] == ["docker", "exec", "mirror-neuron-redis"]
    assert "REPLICAOF NO ONE" in mock_run.call_args_list[1].args[0][-1]
    assert "mn:compose-redis:write-probe" in mock_run.call_args_list[1].args[0][-1]

def test_wait_for_local_cluster_grpc_uses_injected_core_client(mocker):
    shared_summary = mocker.patch(
        "mn_cli.shared.client.get_system_summary",
        side_effect=AssertionError("shared client should not be called"),
    )
    core_client = _FlakyCoreClient(failures=1)
    sleeps = []
    times = iter([0.0, 0.0, 0.1])

    server_cmds._wait_for_local_cluster_grpc(
        timeout_seconds=1.0,
        core_client=core_client,
        sleep_fn=sleeps.append,
        time_fn=lambda: next(times),
    )

    assert core_client.calls == 2
    assert sleeps == [0.25]
    shared_summary.assert_not_called()

def test_wait_for_local_cluster_grpc_raises_after_injected_client_timeout(capsys):
    core_client = _FlakyCoreClient(failures=10)
    times = iter([0.0, 0.0, 1.1])

    with pytest.raises(typer.Exit) as exc_info:
        server_cmds._wait_for_local_cluster_grpc(
            timeout_seconds=1.0,
            core_client=core_client,
            sleep_fn=lambda _seconds: None,
            time_fn=lambda: next(times),
        )

    assert exc_info.value.exit_code == 1
    assert core_client.calls == 1
    assert "Local MirrorNeuron core did not become ready" in capsys.readouterr().out

class _FlakyCoreClient:
    def __init__(self, *, failures: int):
        self.calls = 0
        self.failures = failures

    def get_system_summary(self):
        self.calls += 1
        if self.failures > 0:
            self.failures -= 1
            raise RuntimeError("core not ready")
        return '{"nodes":[]}'

def test_start_server_already_running(mocker, tmp_path):
    mocker.patch('mn_cli.server_cmds.API_PID_FILE', tmp_path / "api.pid")
    (tmp_path / "api.pid").write_text("1234")
    mocker.patch('mn_cli.server_cmds.os.kill') # check_status returns 0
    mocker.patch('mn_cli.server_cmds._read_runtime_api_health', return_value=None)

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
    assert not (server_cmds.DIR / "grpc_auth.token").exists()
    assert not (server_cmds.DIR / "grpc_admin.token").exists()
    mock_start_web.assert_called_once()
    mock_write_endpoints.assert_called_once()
    mock_print_endpoints.assert_called_once_with(None, True)

def test_start_server_restarts_existing_api_when_runtime_blueprint_env_changes(mocker, tmp_path, monkeypatch):
    mocker.patch('mn_cli.server_cmds.API_PID_FILE', tmp_path / "api.pid")
    (tmp_path / "api.pid").write_text("1234")
    mocker.patch('mn_cli.server_cmds.os.kill') # check_status returns 0

    compose_file = server_cmds.RUNTIME_COMPOSE_FILE
    compose_env = server_cmds.RUNTIME_COMPOSE_ENV
    compose_file.parent.mkdir(parents=True, exist_ok=True)
    compose_file.write_text("services: {}\n", encoding="utf-8")
    compose_env.write_text(
        "COMPOSE_PROJECT_NAME=mirror-neuron\n"
        "MN_ENV=dev\n"
        "MN_BLUEPRINT_SOURCE=github\n"
        "MN_BLUEPRINT_REPO=https://github.com/MirrorNeuronLab/mn-blueprints.git\n"
        "MN_RUNS_ROOT=/tmp/mn-runs\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("MN_ENV", "prod")
    monkeypatch.setenv("MN_BLUEPRINT_SOURCE", "github")
    monkeypatch.setenv(
        "MN_BLUEPRINT_REPO",
        "[MirrorNeuronLab/otterdesk-blueprints](https://github.com/MirrorNeuronLab/otterdesk-blueprints)",
    )
    monkeypatch.setenv("MN_RUNS_ROOT", "/tmp/otterdesk-runs")
    mocker.patch('mn_cli.server_cmds._runtime_grpc_tokens_from_running_container', return_value={})
    mocker.patch('mn_cli.server_cmds._docker_container_running', return_value=True)
    mocker.patch(
        'mn_cli.server_cmds._read_runtime_api_health',
        return_value={
            "status": "ok",
            "env": "dev",
            "blueprint_source": "github",
            "blueprint_repo": "https://github.com/MirrorNeuronLab/mn-blueprints.git",
            "active_blueprint_location": "https://github.com/MirrorNeuronLab/mn-blueprints.git",
            "runs_root": "/tmp/mn-runs",
        },
    )
    mock_run = mocker.patch('mn_cli.server_cmds.subprocess.run')
    start_api = mocker.patch('mn_cli.server_cmds._start_api_if_installed')
    mocker.patch('mn_cli.server_cmds._start_web_ui_if_installed', return_value=False)
    mocker.patch('mn_cli.server_cmds._write_runtime_endpoints_file', return_value={"api": {}})
    mocker.patch('mn_cli.server_cmds._print_service_endpoints')

    _start_server()

    start_api.assert_called_once()
    api_env = start_api.call_args.args[0]
    assert start_api.call_args.kwargs["restart_running"] is True
    assert start_api.call_args.kwargs["restart_reason"] == "runtime config changed"
    assert api_env["MN_ENV"] == "prod"
    assert api_env["MN_BLUEPRINT_REPO"] == "https://github.com/MirrorNeuronLab/otterdesk-blueprints"
    assert api_env["MN_RUNS_ROOT"] == "/tmp/otterdesk-runs"
    mock_run.assert_any_call(runtime_compose_cmd("up", "-d"), check=True, stdout=subprocess.DEVNULL, env=api_env)
    compose_text = compose_env.read_text()
    assert "MN_ENV=prod" in compose_text
    assert "MN_BLUEPRINT_REPO=https://github.com/MirrorNeuronLab/otterdesk-blueprints" in compose_text
    assert "MN_RUNS_ROOT=/tmp/otterdesk-runs" in compose_text

def test_start_server_join_compose_imports_primary_grpc_tokens(mocker, tmp_path):
    compose_file = server_cmds.RUNTIME_COMPOSE_FILE
    compose_env = server_cmds.RUNTIME_COMPOSE_ENV
    compose_file.parent.mkdir(parents=True, exist_ok=True)
    compose_file.write_text("services: {}\n", encoding="utf-8")
    compose_env.write_text("COMPOSE_PROJECT_NAME=mirror-neuron\n", encoding="utf-8")
    redis_password = "remote-redis-password"

    mocker.patch(
        'mn_cli.server_cmds._handshake_with_main_node',
        return_value={
            "node_name": "mirror_neuron@192.168.4.20",
            "redis_host": "192.168.4.20",
            "redis_port": 56379,
            "redis_url": f"redis://:{redis_password}@192.168.4.20:56379/0",
            "grpc_auth_token": "primary-auth-token",
            "grpc_admin_token": "primary-admin-token",
        },
    )
    mocker.patch('mn_cli.server_cmds._detect_lan_ip', return_value="192.168.4.99")
    mocker.patch('mn_cli.server_cmds._detect_host_gpu_count', return_value=0)
    mocker.patch('mn_cli.server_cmds._start_api_if_installed')
    mocker.patch('mn_cli.server_cmds._start_web_ui_if_installed', return_value=False)
    mocker.patch('mn_cli.server_cmds._print_service_endpoints')
    mocker.patch('mn_cli.server_cmds._write_runtime_endpoints_file', return_value={"api": {}})
    mock_run = mocker.patch('mn_cli.server_cmds.subprocess.run', return_value=mocker.Mock(returncode=0, stdout="false\n"))

    _start_server(ip="192.168.4.20", token="join-token")

    compose_up = next(call for call in mock_run.call_args_list if call.args[0] == runtime_compose_cmd("up", "-d"))
    assert compose_up.kwargs["env"]["MN_GRPC_AUTH_TOKEN"] == "mirror_neuron_password"
    assert compose_up.kwargs["env"]["MN_GRPC_ADMIN_TOKEN"] == "mirror_neuron_password_admin"
    assert not (server_cmds.DIR / "grpc_auth.token").exists()
    assert not (server_cmds.DIR / "grpc_admin.token").exists()
    compose_text = compose_env.read_text(encoding="utf-8")
    assert "MN_GRPC_AUTH_TOKEN=mirror_neuron_password" in compose_text
    assert "MN_GRPC_ADMIN_TOKEN=mirror_neuron_password_admin" in compose_text

def test_start_server_join_docker_imports_primary_grpc_tokens(mocker, tmp_path):
    redis_password = "remote-redis-password"
    commands = []

    def mock_run(cmd, **kwargs):
        commands.append(cmd)
        if cmd[:3] == ["docker", "inspect", "-f"]:
            return mocker.Mock(returncode=0, stdout="false\n")
        return mocker.Mock(returncode=0, stdout="")

    mocker.patch(
        'mn_cli.server_cmds._handshake_with_main_node',
        return_value={
            "node_name": "mirror_neuron@192.168.4.20",
            "redis_host": "192.168.4.20",
            "redis_port": 56379,
            "redis_url": f"redis://:{redis_password}@192.168.4.20:56379/0",
            "grpc_auth_token": "primary-auth-token",
            "grpc_admin_token": "primary-admin-token",
        },
    )
    mocker.patch('mn_cli.server_cmds._detect_lan_ip', return_value="192.168.4.99")
    mocker.patch('mn_cli.server_cmds._detect_host_gpu_count', return_value=0)
    mocker.patch('mn_cli.server_cmds._start_api_if_installed')
    mocker.patch('mn_cli.server_cmds._start_web_ui_if_installed', return_value=False)
    mocker.patch('mn_cli.server_cmds._print_service_endpoints')
    mocker.patch('mn_cli.server_cmds._write_runtime_endpoints_file', return_value={"api": {}})
    mocker.patch('mn_cli.server_cmds.subprocess.run', side_effect=mock_run)

    _start_server(ip="192.168.4.20", token="join-token")

    docker_run = next(cmd for cmd in commands if cmd[:3] == ["docker", "run", "-d"])
    assert "MN_GRPC_AUTH_TOKEN=mirror_neuron_password" in docker_run
    assert "MN_GRPC_ADMIN_TOKEN=mirror_neuron_password_admin" in docker_run
    assert not (server_cmds.DIR / "grpc_auth.token").exists()
    assert not (server_cmds.DIR / "grpc_admin.token").exists()

def test_start_network_seed_uses_primary_persisted_grpc_tokens(mocker, monkeypatch):
    monkeypatch.setenv("MN_GRPC_AUTH_TOKEN", "primary-auth-token")
    monkeypatch.setenv("MN_GRPC_ADMIN_TOKEN", "primary-admin-token")
    mocker.patch('mn_cli.server_cmds._docker_container_running', return_value=False)
    mocker.patch('mn_cli.server_cmds._detect_lan_ip', return_value="192.168.4.20")
    mocker.patch('mn_cli.server_cmds._ensure_network_docker_network')
    mocker.patch('mn_cli.server_cmds._find_available_published_port', return_value=56379)
    mocker.patch('mn_cli.server_cmds._start_network_redis')
    start_core = mocker.patch('mn_cli.server_cmds._start_network_core')
    mocker.patch('mn_cli.server_cmds._print_network_seed_ready')

    token = _start_network_seed(host="192.168.4.20", grpc_port=50055)

    assert token
    env = start_core.call_args.args[0]
    assert env["MN_GRPC_AUTH_TOKEN"] == "mirror_neuron_password"
    assert env["MN_GRPC_ADMIN_TOKEN"] == "mirror_neuron_password_admin"
    assert not (server_cmds.DIR / "grpc_auth.token").exists()
    assert not (server_cmds.DIR / "grpc_admin.token").exists()

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
    mocker.patch('mn_cli.server_cmds._detect_lan_ip', return_value="192.168.4.99")
    
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

    redis_password = server_cmds.DEV_REDIS_PASSWORD
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
            "MN_ENV": "prod",
            "MN_BLUEPRINT_SOURCE": "local",
            "MN_BLUEPRINT_REPO": "https://github.com/MirrorNeuronLab/mn-blueprints.git",
            "MN_BLUEPRINT_LOCAL": "/work/mn/otterdesk-blueprints",
            "MN_RUNS_ROOT": "/opt/mn/runs",
        }
    )

    assert env["MN_ENV"] == "prod"
    assert env["MN_BLUEPRINT_SOURCE"] == "local"
    assert env["MN_BLUEPRINT_REPO"] == "https://github.com/MirrorNeuronLab/mn-blueprints.git"
    assert env["MN_BLUEPRINT_LOCAL"] == "/work/mn/otterdesk-blueprints"
    assert env["MN_RUNS_ROOT"] == "/opt/mn/runs"
    assert env["MN_HOST_ARTIFACTS_DIR"] == "/opt/mn/runs"
    assert env["MN_CONTAINER_RUNS_ROOT"] == "/root/.mn/runs"
    assert env["MN_BLUEPRINT_WEB_UI_BIND_HOST"] == "0.0.0.0"
    assert env["MN_BLUEPRINT_WEB_UI_PUBLIC_HOST"] == "localhost"
    assert env["MN_BLUEPRINT_WEB_UI_PORT_START"] == "61000"
    assert env["MN_BLUEPRINT_WEB_UI_PORT_END"] == "61049"
    assert env["MN_BLUEPRINT_WEB_UI_PORT_ALLOCATION_MODE"] == "prepublished"
    compose_env_text = compose_env.read_text()
    assert "MN_ENV=prod" in compose_env_text
    assert "MN_BLUEPRINT_SOURCE=local" in compose_env_text
    assert "MN_BLUEPRINT_REPO=https://github.com/MirrorNeuronLab/mn-blueprints.git" in compose_env_text
    assert "MN_BLUEPRINT_LOCAL=/work/mn/otterdesk-blueprints" in compose_env_text
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

    assert env["MN_BLUEPRINT_SOURCE"] == "github"
    assert env["MN_BLUEPRINT_REPO"] == "https://github.com/MirrorNeuronLab/mn-blueprints.git"
    assert env["MN_BLUEPRINT_LOCAL"] == ""
    assert env["MN_HOST_ARTIFACTS_DIR"].endswith(".mn/runs")
    assert env["MN_RUNS_ROOT"].endswith(".mn/runs")
    assert env["MN_CONTAINER_RUNS_ROOT"] == "/root/.mn/runs"
    assert env["MN_BLUEPRINT_WEB_UI_PORT_START"] == "61000"
    assert env["MN_BLUEPRINT_WEB_UI_PORT_END"] == "61049"
    assert env["MN_BLUEPRINT_WEB_UI_PORT_ALLOCATION_MODE"] == "prepublished"
    compose_env_text = compose_env.read_text()
    assert "MN_ENV=dev" in compose_env_text
    assert "MN_BLUEPRINT_SOURCE=github" in compose_env_text
    assert "MN_BLUEPRINT_REPO=https://github.com/MirrorNeuronLab/mn-blueprints.git" in compose_env_text
    assert "MN_BLUEPRINT_LOCAL=" in compose_env_text
    assert "MN_HOST_ARTIFACTS_DIR=" in compose_env_text
    assert "MN_RUNS_ROOT=" in compose_env_text
    assert "MN_CONTAINER_RUNS_ROOT=/root/.mn/runs" in compose_env_text
    assert "MN_BLUEPRINT_WEB_UI_PORT_START=61000" in compose_env_text
    assert "MN_BLUEPRINT_WEB_UI_PORT_END=61049" in compose_env_text
    assert "MN_BLUEPRINT_WEB_UI_PORT_ALLOCATION_MODE=prepublished" in compose_env_text

def test_compose_native_settings_normalizes_markdown_blueprint_repo(mocker, tmp_path):
    compose_env = tmp_path / "docker-compose.env"
    compose_env.write_text("COMPOSE_PROJECT_NAME=mirror-neuron\n")
    mocker.patch('mn_cli.server_cmds.RUNTIME_COMPOSE_ENV', compose_env)

    env = server_cmds._ensure_compose_native_port_settings(
        {
            "MN_BLUEPRINT_SOURCE": "github",
            "MN_BLUEPRINT_REPO": "[MirrorNeuronLab/otterdesk-blueprints](https://github.com/MirrorNeuronLab/otterdesk-blueprints)",
        }
    )

    assert env["MN_BLUEPRINT_REPO"] == "https://github.com/MirrorNeuronLab/otterdesk-blueprints"
    assert "MN_BLUEPRINT_REPO=https://github.com/MirrorNeuronLab/otterdesk-blueprints" in compose_env.read_text()

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
    assert env["MN_MODEL_SERVICE_NODE_NAME"] == "mirror_neuron@192.168.4.99"
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
    assert "MN_MODEL_SERVICE_NODE_NAME=mirror_neuron@192.168.4.99" in compose_env_text
    assert "MN_CLUSTER_NODES=mirror_neuron@192.168.4.99" in compose_env_text
    assert "MN_NETWORK_REDIS_HOST=192.168.4.99" in compose_env_text
    assert "MN_NETWORK_REDIS_PORT=56379" in compose_env_text
    assert "MN_GRPC_AUTH_TOKEN=mirror_neuron_password" in compose_env_text
    assert "MN_GRPC_ADMIN_TOKEN=mirror_neuron_password_admin" in compose_env_text
    assert "MN_GRPC_AUTH_TOKEN_FILE=" not in compose_env_text
    assert "MN_GRPC_ADMIN_TOKEN_FILE=" not in compose_env_text
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
    redis_password = "remote-redis-password"
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
    assert "MN_ARTIFACT_AUTH_TOKEN" not in env
    assert env["MN_COOKIE"] == _derive_network_secret("join-token", "cookie")
    assert env["MN_DIST_PORT"] == "54370"
    assert env["ERL_AFLAGS"] == _erl_aflags("54370")

def test_start_server_preserves_persisted_join_profile_on_restart(mocker, tmp_path):
    compose_file = tmp_path / "docker-compose.yml"
    compose_env = tmp_path / "docker-compose.env"
    redis_password = "persisted-redis-password"
    compose_file.write_text("services: {}\n")
    compose_env.write_text(
        "COMPOSE_PROJECT_NAME=mirror-neuron\n"
        "MN_NETWORK_JOIN_TOKEN=join-token\n"
        "MN_NODE_NAME=mirror_neuron@192.168.4.173\n"
        "MN_CLUSTER_NODES=mirror_neuron@192.168.4.35\n"
        "MN_NETWORK_REDIS_HOST=192.168.4.35\n"
        "MN_NETWORK_REDIS_PORT=56381\n"
        "MN_GRPC_AUTH_TOKEN=stable-auth-token\n"
        "MN_GRPC_ADMIN_TOKEN=stable-admin-token\n"
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
    assert env["MN_GRPC_AUTH_TOKEN"] == "mirror_neuron_password"
    assert env["MN_GRPC_ADMIN_TOKEN"] == "mirror_neuron_password_admin"
    assert env["MN_NODE_NAME"] == "mirror_neuron@192.168.4.173"
    assert env["MN_CLUSTER_NODES"] == "mirror_neuron@192.168.4.35"
    assert env["MN_NETWORK_REDIS_HOST"] == "192.168.4.35"
    assert env["MN_NETWORK_REDIS_PORT"] == "56381"
    assert env["MN_REDIS_URL"] == f"redis://:{redis_password}@192.168.4.35:56381/0"
    assert not (server_cmds.DIR / "grpc_auth.token").exists()
    assert not (server_cmds.DIR / "grpc_admin.token").exists()
    ensure_redis.assert_not_called()

def test_start_server_refreshes_generated_node_name_for_joined_runtime_ip_change(mocker, tmp_path):
    compose_file = tmp_path / "docker-compose.yml"
    compose_env = tmp_path / "docker-compose.env"
    redis_password = "persisted-redis-password"
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
    redis_password = "persisted-redis-password"
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
        },
        web_ui_available=False,
    )

    assert snapshot["grpc"]["host"] == "192.168.4.173"
    assert snapshot["grpc"]["target"] == "192.168.4.173:55051"

def test_start_server_success(mocker, tmp_path, monkeypatch):
    mocker.patch('mn_cli.server_cmds.API_PID_FILE', tmp_path / "api.pid")
    mocker.patch('mn_cli.server_cmds.API_WATCHDOG_PID_FILE', tmp_path / "api-watchdog.pid")
    mocker.patch('mn_cli.server_cmds.WEB_UI_DIRS', ())
    redis_password = "remote-redis-password"
    monkeypatch.setenv("MN_API_PORT", "54111")
    monkeypatch.setenv("MN_BLUEPRINT_SOURCE", "local")
    monkeypatch.setenv("MN_BLUEPRINT_REPO", "https://github.com/MirrorNeuronLab/mn-blueprints.git")
    monkeypatch.setenv("MN_BLUEPRINT_LOCAL", "/work/mn/otterdesk-blueprints")
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
    assert api_env["MN_BLUEPRINT_SOURCE"] == "local"
    assert api_env["MN_BLUEPRINT_REPO"] == "https://github.com/MirrorNeuronLab/mn-blueprints.git"
    assert api_env["MN_BLUEPRINT_LOCAL"] == "/work/mn/otterdesk-blueprints"
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
    host_shared = tmp_path / "host-shared"
    monkeypatch.setenv("MN_HOST_SHARED_STORAGE_ROOT", str(host_shared))
    monkeypatch.setenv("MN_SHARED_STORAGE_ROOT", "/root/.mn/shared")
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
    image_index = docker_run.index("mirror-neuron-core:latest")
    assert docker_run[image_index + 1 : image_index + 3] == ["sh", "-c"]
    assert "epmd_bin=" in docker_run[image_index + 3]
    assert "RELEASE_DISTRIBUTION=name" in docker_run[image_index + 3]
    cookie_env = next(value for flag, value in zip(docker_run, docker_run[1:]) if flag == "-e" and value.startswith("MN_COOKIE="))
    auth_env = next(value for flag, value in zip(docker_run, docker_run[1:]) if flag == "-e" and value.startswith("MN_GRPC_AUTH_TOKEN="))
    admin_env = next(value for flag, value in zip(docker_run, docker_run[1:]) if flag == "-e" and value.startswith("MN_GRPC_ADMIN_TOKEN="))
    runs_root_env = next(value for flag, value in zip(docker_run, docker_run[1:]) if flag == "-e" and value.startswith("MN_RUNS_ROOT="))
    bundle_cache_env = next(value for flag, value in zip(docker_run, docker_run[1:]) if flag == "-e" and value.startswith("MN_BUNDLE_CACHE_DIR="))
    assert cookie_env != "MN_COOKIE=mirrorneuron"
    assert auth_env == "MN_GRPC_AUTH_TOKEN=mirror_neuron_password"
    assert admin_env == "MN_GRPC_ADMIN_TOKEN=mirror_neuron_password_admin"
    assert not any(
        value.startswith("MN_GRPC_AUTH_TOKEN_FILE=")
        for flag, value in zip(docker_run, docker_run[1:])
        if flag == "-e"
    )
    assert not any(
        value.startswith("MN_GRPC_ADMIN_TOKEN_FILE=")
        for flag, value in zip(docker_run, docker_run[1:])
        if flag == "-e"
    )
    assert runs_root_env == "MN_RUNS_ROOT=/root/.mn/runs"
    assert bundle_cache_env == "MN_BUNDLE_CACHE_DIR=/root/.mn/shared/bundle_cache"
    assert ["-e", f"MN_HOST_SHARED_STORAGE_ROOT={host_shared}"] == docker_run[
        docker_run.index(f"MN_HOST_SHARED_STORAGE_ROOT={host_shared}") - 1 : docker_run.index(f"MN_HOST_SHARED_STORAGE_ROOT={host_shared}") + 1
    ]
    assert ["-v", f"{server_cmds.DIR}:/root/.mn"] == docker_run[
        docker_run.index(f"{server_cmds.DIR}:/root/.mn") - 1 : docker_run.index(f"{server_cmds.DIR}:/root/.mn") + 1
    ]
    assert ["-v", f"{server_cmds.DIR / 'runs'}:/root/.mn/runs"] == docker_run[
        docker_run.index(f"{server_cmds.DIR / 'runs'}:/root/.mn/runs") - 1 : docker_run.index(f"{server_cmds.DIR / 'runs'}:/root/.mn/runs") + 1
    ]
    assert ["-v", f"{host_shared}:/root/.mn/shared"] == docker_run[
        docker_run.index(f"{host_shared}:/root/.mn/shared") - 1 : docker_run.index(f"{host_shared}:/root/.mn/shared") + 1
    ]
    assert ["-e", "SLACK_BOT_TOKEN"] == docker_run[
        docker_run.index("SLACK_BOT_TOKEN") - 1 : docker_run.index("SLACK_BOT_TOKEN") + 1
    ]
    assert ["-e", "SLACK_DEFAULT_CHANNEL"] == docker_run[
        docker_run.index("SLACK_DEFAULT_CHANNEL") - 1 : docker_run.index("SLACK_DEFAULT_CHANNEL") + 1
    ]

def test_start_server_mounts_docker_worker_socket_and_linux_cli(mocker, tmp_path, monkeypatch):
    mocker.patch('mn_cli.server_cmds.API_PID_FILE', tmp_path / "api.pid")
    mocker.patch('mn_cli.server_cmds.WEB_UI_DIRS', ())
    docker_socket = tmp_path / "docker.sock"
    docker_socket.touch()
    host_docker = tmp_path / "docker"
    host_docker.write_text("#!/bin/sh\n", encoding="utf-8")
    mocker.patch('mn_cli.server_cmds._docker_host_socket', return_value=docker_socket)
    mocker.patch('mn_cli.server_cmds.shutil.which', return_value=str(host_docker))
    monkeypatch.setattr(sys, "platform", "linux")

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
        sysname = "Linux"

    mocker.patch('mn_cli.server_cmds.os.uname', return_value=UnameMock())

    _start_server()

    docker_run = next(cmd for cmd in commands if cmd[:3] == ["docker", "run", "-d"])
    assert ["-v", f"{docker_socket}:/var/run/docker.sock:rw"] == docker_run[
        docker_run.index(f"{docker_socket}:/var/run/docker.sock:rw") - 1 : docker_run.index(f"{docker_socket}:/var/run/docker.sock:rw") + 1
    ]
    assert ["-v", f"{host_docker}:/usr/local/bin/docker:ro"] == docker_run[
        docker_run.index(f"{host_docker}:/usr/local/bin/docker:ro") - 1 : docker_run.index(f"{host_docker}:/usr/local/bin/docker:ro") + 1
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

def test_web_ui_dirs_use_runtime_home_only_when_runtime_home_is_custom(mocker, tmp_path):
    custom_home = tmp_path / "custom-home"

    mocker.patch("mn_cli.server_cmds.DIR", custom_home)
    mocker.patch("mn_cli.server_cmds._source_checkout_web_ui_dir", return_value=None)

    assert server_cmds._web_ui_dirs() == (
        custom_home / "webui",
        custom_home / "web-ui-source",
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
    source_api = tmp_path / "mn-api"
    (source_api / "mn_api").mkdir(parents=True)

    mocker.patch('mn_cli.server_cmds.VENV_DIR', tmp_path / "mn_venv")
    mocker.patch('mn_cli.server_cmds._source_checkout_api_dir', return_value=source_api)
    mocker.patch('mn_cli.server_cmds.API_PID_FILE', tmp_path / "api.pid")
    mocker.patch('mn_cli.server_cmds.API_WATCHDOG_PID_FILE', tmp_path / "api-watchdog.pid")
    mocker.patch('mn_cli.server_cmds.API_LOG', tmp_path / "api.log")
    mocker.patch('mn_cli.server_cmds.API_WATCHDOG_LOG', tmp_path / "api-watchdog.log")
    mock_wait = mocker.patch('mn_cli.server_cmds._wait_for_api', side_effect=[False, True])
    stop_matching = mocker.patch('mn_cli.server_cmds.stop_matching_sidecar_processes', return_value=False)

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
    pythonpath = mock_popen.call_args.kwargs["env"]["PYTHONPATH"].split(server_cmds.os.pathsep)
    assert pythonpath[0] == str(source_api)
    assert mock_wait.call_args_list == [
        call("localhost", "54001", timeout_seconds=1.0),
        call("localhost", "54001", timeout_seconds=10.0),
    ]
    stop_matching.assert_called_once_with("mn-api", "REST API")

def test_start_api_restarts_untracked_healthy_instance_under_watchdog(mocker, tmp_path):
    api_bin = tmp_path / "mn_venv" / "bin" / "mn-api"
    api_bin.parent.mkdir(parents=True)
    api_bin.write_text("#!/bin/sh\n")

    mocker.patch('mn_cli.server_cmds.VENV_DIR', tmp_path / "mn_venv")
    mocker.patch('mn_cli.server_cmds.API_PID_FILE', tmp_path / "api.pid")
    mocker.patch('mn_cli.server_cmds.API_WATCHDOG_PID_FILE', tmp_path / "api-watchdog.pid")
    mocker.patch('mn_cli.server_cmds.API_LOG', tmp_path / "api.log")
    mocker.patch('mn_cli.server_cmds.API_WATCHDOG_LOG', tmp_path / "api-watchdog.log")
    mock_wait = mocker.patch('mn_cli.server_cmds._wait_for_api', side_effect=[True, True])
    stop_matching = mocker.patch('mn_cli.server_cmds.stop_matching_sidecar_processes', return_value=True)
    mock_popen = mocker.patch('mn_cli.server_cmds.subprocess.Popen')
    mock_popen.return_value.pid = 54001

    assert _start_api_if_installed({"MN_API_HOST": "localhost", "MN_API_PORT": "54001"}) is True

    stop_matching.assert_called_once_with("mn-api", "REST API")
    mock_popen.assert_called_once()
    assert mock_wait.call_args_list == [
        call("localhost", "54001", timeout_seconds=1.0),
        call("localhost", "54001", timeout_seconds=10.0),
    ]

def test_start_api_restarts_running_watchdog_when_requested(mocker, tmp_path):
    api_bin = tmp_path / "mn_venv" / "bin" / "mn-api"
    api_bin.parent.mkdir(parents=True)
    api_bin.write_text("#!/bin/sh\n")
    api_pid = tmp_path / "api.pid"
    watchdog_pid = tmp_path / "api-watchdog.pid"
    api_pid.write_text("1234")
    watchdog_pid.write_text("5678")

    mocker.patch('mn_cli.server_cmds.VENV_DIR', tmp_path / "mn_venv")
    mocker.patch('mn_cli.server_cmds.API_PID_FILE', api_pid)
    mocker.patch('mn_cli.server_cmds.API_WATCHDOG_PID_FILE', watchdog_pid)
    mocker.patch('mn_cli.server_cmds.API_LOG', tmp_path / "api.log")
    mocker.patch('mn_cli.server_cmds.API_WATCHDOG_LOG', tmp_path / "api-watchdog.log")
    mocker.patch('mn_cli.server_cmds.os.kill') # check_status returns running
    mocker.patch('mn_cli.server_cmds.time.sleep')
    kill = mocker.patch('mn_cli.server_cmds.kill_tree')
    mock_wait = mocker.patch('mn_cli.server_cmds._wait_for_api', side_effect=[True, True, True])
    stop_matching = mocker.patch('mn_cli.server_cmds.stop_matching_sidecar_processes', return_value=False)
    mock_popen = mocker.patch('mn_cli.server_cmds.subprocess.Popen')
    mock_popen.return_value.pid = 54001

    assert _start_api_if_installed(
        {"MN_API_HOST": "localhost", "MN_API_PORT": "54001"},
        restart_running=True,
        restart_reason="runtime config changed",
    ) is True

    assert kill.call_args_list == [call(5678)]
    stop_matching.assert_called_once_with("mn-api", "REST API")
    mock_popen.assert_called_once()
    assert mock_wait.call_args_list == [
        call("localhost", "54001", timeout_seconds=5.0),
        call("localhost", "54001", timeout_seconds=1.0),
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
    stop_matching = mocker.patch('mn_cli.server_cmds.stop_matching_sidecar_processes', return_value=False)

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
    stop_matching.assert_called_once_with("mn-web-ui-server", "Web UI")

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
    mocker.patch('mn_cli.server_cmds.stop_matching_sidecar_processes', return_value=False)

    mock_popen = mocker.patch('mn_cli.server_cmds.subprocess.Popen')
    mock_popen.return_value.pid = 5173

    assert _start_web_ui_if_installed() is True
    assert (tmp_path / "web-ui-watchdog.pid").read_text() == "5173"

def test_start_web_ui_restarts_untracked_healthy_instance_under_watchdog(mocker, tmp_path):
    web_ui_dir = tmp_path / "web-ui"
    (web_ui_dir / "dist").mkdir(parents=True)
    (web_ui_dir / "dist" / "index.html").write_text("<div id=\"root\"></div>")

    mocker.patch('mn_cli.server_cmds.WEB_UI_DIRS', (web_ui_dir,))
    mocker.patch('mn_cli.server_cmds.WEB_UI_PID_FILE', tmp_path / "web-ui.pid")
    mocker.patch('mn_cli.server_cmds.WEB_UI_WATCHDOG_PID_FILE', tmp_path / "web-ui-watchdog.pid")
    mocker.patch('mn_cli.server_cmds.WEB_UI_LOG', tmp_path / "web-ui.log")
    mocker.patch('mn_cli.server_cmds.WEB_UI_WATCHDOG_LOG', tmp_path / "web-ui-watchdog.log")
    mock_wait = mocker.patch('mn_cli.server_cmds._wait_for_web_ui', side_effect=[True, True])
    stop_matching = mocker.patch('mn_cli.server_cmds.stop_matching_sidecar_processes', return_value=True)

    mock_popen = mocker.patch('mn_cli.server_cmds.subprocess.Popen')
    mock_popen.return_value.pid = 5173

    assert _start_web_ui_if_installed() is True
    stop_matching.assert_called_once_with("mn-web-ui-server", "Web UI")
    mock_popen.assert_called_once()
    assert mock_wait.call_args_list == [
        call("localhost", "55173", timeout_seconds=1.0),
        call("localhost", "55173", timeout_seconds=10.0),
    ]

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
    mocker.patch('mn_cli.server_cmds.stop_matching_sidecar_processes', return_value=False)
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
    mocker.patch('mn_cli.server_cmds.stop_matching_sidecar_processes', return_value=False)
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
