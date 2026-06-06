import os
import json
import hashlib
import ipaddress
import signal
import secrets
import shutil
import socket
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse
import typer
from rich.console import Console
from rich.table import Table
from mn_cli.config import CliConfig
from mn_cli.libs.ui import print_confirmed, print_success_confirmation
from mn_cli.logging_config import configure_logging

console = Console()
logger = configure_logging("mn-cli", CliConfig.from_env().log_path)
GRPC_ADMIN_TOKEN_ENV = "MN_GRPC_ADMIN_TOKEN"
LEGACY_GRPC_ADMIN_TOKEN_ENV = "MN_MIRROR_NEURON_GRPC_ADMIN_TOKEN"

def _erl_aflags(dist_port: str | int) -> str:
    return (
        f"-connect_all false -kernel prevent_overlapping_partitions false "
        f"inet_dist_listen_min {dist_port} inet_dist_listen_max {dist_port}"
    )

def _erl_aflags_needs_update(value: Optional[str], dist_port: str | int) -> bool:
    text = str(value or "").strip()
    if not text:
        return True
    return (
        "-connect_all false" not in text
        or "prevent_overlapping_partitions false" not in text
        or f"inet_dist_listen_min {dist_port}" not in text
        or f"inet_dist_listen_max {dist_port}" not in text
    )

def _mn_home() -> Path:
    configured_home = os.getenv("MN_HOME") or os.getenv("MIRROR_NEURON_HOME")
    return Path(configured_home).expanduser() if configured_home else Path.home() / ".mn"


DIR = _mn_home()
DEFAULT_DIR = Path.home() / ".mn"
PID_DIR = DIR / "pids"
LOG_DIR = DIR / "logs"
BEAM_PID_FILE = PID_DIR / "beam.pid"
API_PID_FILE = PID_DIR / "api.pid"
API_WATCHDOG_PID_FILE = PID_DIR / "api-watchdog.pid"
WEB_UI_PID_FILE = PID_DIR / "web-ui.pid"
WEB_UI_WATCHDOG_PID_FILE = PID_DIR / "web-ui-watchdog.pid"
BEAM_LOG = LOG_DIR / "beam.log"
API_LOG = LOG_DIR / "api.log"
API_WATCHDOG_LOG = LOG_DIR / "api-watchdog.log"
WEB_UI_LOG = LOG_DIR / "web-ui.log"
WEB_UI_WATCHDOG_LOG = LOG_DIR / "web-ui-watchdog.log"
VENV_DIR = Path.home() / ".local" / "share" / "mn_venv"
RUNTIME_COMPOSE_FILE = DIR / "docker-compose.yml"
RUNTIME_COMPOSE_ENV = DIR / "docker-compose.env"
RUNTIME_ENDPOINTS_FILE = DIR / "runtime-endpoints.json"
def _unique_paths(paths: list[Path]) -> tuple[Path, ...]:
    unique: list[Path] = []
    seen: set[str] = set()
    for path in paths:
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        unique.append(path)
    return tuple(unique)


def _source_checkout_web_ui_dir() -> Optional[Path]:
    checkout_dir = Path(__file__).resolve().parents[2]
    web_ui_dir = checkout_dir / "mn-web-ui"
    return web_ui_dir if web_ui_dir.exists() else None


def _web_ui_dirs() -> tuple[Path, ...]:
    paths = [
        DIR / "webui",
        DIR / "web-ui-source",
        DEFAULT_DIR / "webui",
        DEFAULT_DIR / "web-ui-source",
    ]
    source_web_ui_dir = _source_checkout_web_ui_dir()
    if source_web_ui_dir is not None:
        paths.append(source_web_ui_dir)
    return _unique_paths(paths)


WEB_UI_DIRS = _web_ui_dirs()
DEFAULT_HOST = "localhost"
DEFAULT_GRPC_PORT = "55051"
DEFAULT_API_PORT = "54001"
DEFAULT_EPMD_PORT = "54369"
DEFAULT_DIST_PORT = "54370"
DEFAULT_WEB_UI_PORT = "55173"
DEFAULT_WEB_UI_RESTART_DELAY_SECONDS = "2"
DEFAULT_OPENSHELL_GATEWAY_PORT = "58080"
DEFAULT_BLUEPRINT_REPO = "https://github.com/MirrorNeuronLab/mn-blueprints.git"
DEFAULT_BLUEPRINT_WEB_UI_BIND_HOST = "0.0.0.0"
DEFAULT_BLUEPRINT_WEB_UI_PUBLIC_HOST = "localhost"
DEFAULT_BLUEPRINT_WEB_UI_PORT_START = "61000"
DEFAULT_BLUEPRINT_WEB_UI_PORT_END = "61049"
DEFAULT_BLUEPRINT_WEB_UI_PORT_ALLOCATION_MODE = "prepublished"
DEFAULT_CONTAINER_RUNS_ROOT = "/root/.mn/runs"
LEGACY_GRPC_PORT = "50051"
LEGACY_API_PORT = "4001"
LEGACY_EPMD_PORT = "4369"
LEGACY_DIST_PORT = "4370"
LEGACY_WEB_UI_PORT = "5173"
LEGACY_OPENSHELL_GATEWAY_PORT = "8080"
WEB_UI_PORT = DEFAULT_WEB_UI_PORT
REDIS_CONTAINER_PORT = 6379
REDIS_DYNAMIC_PORT_START = 56379
REDIS_DYNAMIC_PORT_END = 56478
NETWORK_TOKEN_FILE = DIR / "network.token"
NETWORK_REDIS_ENV_FILE = DIR / "network-redis.env"
DEFAULT_DOCKER_NETWORK_NAME = "mirror-neuron-runtime"
NETWORK_DOCKER_NETWORK = DEFAULT_DOCKER_NETWORK_NAME
LOCAL_CORE_CONTAINER = "mirror-neuron-core"
COMPOSE_REDIS_CONTAINER = "mirror-neuron-redis"
NETWORK_CORE_CONTAINER = "mirror-neuron-network-core"
NETWORK_REDIS_CONTAINER = "mirror-neuron-network-redis"
RUNTIME_CLUSTER_OVERRIDE_FILE = "docker-compose.cluster.yml"


def _openshell_config_dir() -> Path:
    return Path(os.getenv("OPENSHELL_CONFIG_DIR", str(Path.home() / ".config" / "openshell"))).expanduser()


def _openshell_gateway_endpoint(env: Optional[dict[str, str]] = None) -> str:
    values = env or os.environ
    configured_endpoint = values.get("OPENSHELL_GATEWAY_ENDPOINT")
    if configured_endpoint:
        return configured_endpoint

    gateway_name = values.get("OPENSHELL_GATEWAY", "").strip()
    if not gateway_name:
        try:
            gateway_name = (_openshell_config_dir() / "active_gateway").read_text(encoding="utf-8").strip()
        except OSError:
            gateway_name = ""
    if gateway_name:
        try:
            metadata = json.loads(
                (_openshell_config_dir() / "gateways" / gateway_name / "metadata.json").read_text(
                    encoding="utf-8"
                )
            )
        except (OSError, json.JSONDecodeError):
            metadata = {}
        endpoint = metadata.get("gateway_endpoint") if isinstance(metadata, dict) else None
        if isinstance(endpoint, str) and endpoint.strip():
            return endpoint.strip()

    return f"http://127.0.0.1:{values.get('OPENSHELL_GATEWAY_PORT', DEFAULT_OPENSHELL_GATEWAY_PORT)}"

def _env_host(name: str, default: str = DEFAULT_HOST) -> str:
    return os.getenv(name, default).strip() or default

def _core_host() -> str:
    return _env_host("MN_CORE_HOST")

def _api_host() -> str:
    return _env_host("MN_API_HOST")

def _redis_host() -> str:
    return _env_host("MN_REDIS_HOST")

def _epmd_host() -> str:
    return _env_host("MN_EPMD_HOST")

def _dist_host() -> str:
    return _env_host("MN_DIST_HOST")

def _web_ui_host() -> str:
    return _env_host("MN_WEB_UI_HOST")

def _docker_publish_host(host: str) -> str:
    return "127.0.0.1" if host == "localhost" else host

def _network_publish_host(host: str) -> str:
    normalized = (host or "").strip().lower()
    if normalized in {"", "localhost", "127.0.0.1", "::1"}:
        return "127.0.0.1"
    return "0.0.0.0"

def _loopback_host(host: str) -> bool:
    normalized = (host or "").strip().lower()
    return normalized in {"", "localhost", "127.0.0.1", "::1"} or normalized.startswith("127.")

def _valid_lan_ip(value: object) -> bool:
    try:
        ip = ipaddress.ip_address(str(value or "").strip())
    except ValueError:
        return False
    return (
        ip.version == 4
        and not ip.is_loopback
        and not ip.is_unspecified
        and not ip.is_multicast
    )

def _append_lan_ip(addresses: list[str], value: object) -> None:
    text = str(value or "").strip()
    if _valid_lan_ip(text) and text not in addresses:
        addresses.append(text)

def _interface_lan_ips() -> list[str]:
    addresses: list[str] = []
    commands = (
        ["ip", "-o", "-4", "addr", "show", "scope", "global"],
        ["ifconfig"],
    )
    for command in commands:
        try:
            result = subprocess.run(command, capture_output=True, text=True, timeout=3)
        except (FileNotFoundError, subprocess.SubprocessError):
            continue
        if result.returncode != 0:
            continue
        for line in result.stdout.splitlines():
            parts = line.strip().split()
            for index, part in enumerate(parts):
                candidate = ""
                if part == "inet" and index + 1 < len(parts):
                    candidate = parts[index + 1]
                elif "." in part and "/" in part:
                    candidate = part
                if candidate:
                    _append_lan_ip(addresses, candidate.split("/", 1)[0])
    return addresses

def _detected_lan_ips() -> list[str]:
    addresses: list[str] = []
    for address in _interface_lan_ips():
        _append_lan_ip(addresses, address)

    probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        probe.connect(("10.255.255.255", 1))
        _append_lan_ip(addresses, probe.getsockname()[0])
    except Exception:
        pass
    finally:
        probe.close()

    try:
        _append_lan_ip(addresses, socket.gethostbyname(socket.gethostname()))
    except socket.gaierror:
        pass

    return addresses

def _detect_lan_ip() -> str:
    detected = _detected_lan_ips()
    return detected[0] if detected else "127.0.0.1"

def _parse_gpu_count(value: object) -> Optional[int]:
    try:
        count = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    return count if count >= 0 else None

def _host_gpu_count_from_command(command: list[str]) -> int:
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=5)
    except (FileNotFoundError, subprocess.SubprocessError):
        return 0
    if result.returncode != 0:
        return 0
    return len([line for line in result.stdout.splitlines() if line.strip()])

def _detect_host_gpu_count() -> int:
    explicit_count = _parse_gpu_count(os.getenv("MN_NODE_GPU_COUNT"))
    if explicit_count is not None:
        return explicit_count

    explicit_gpu = os.getenv("MN_NODE_GPU", "").strip().lower()
    if explicit_gpu in {"0", "false", "no", "off"}:
        return 0
    if explicit_gpu in {"1", "true", "yes", "on"}:
        return 1

    system_name = os.uname().sysname
    if system_name == "Darwin":
        try:
            result = subprocess.run(
                ["system_profiler", "SPDisplaysDataType"],
                capture_output=True,
                text=True,
                timeout=10,
            )
        except (FileNotFoundError, subprocess.SubprocessError):
            return 0
        if result.returncode == 0:
            return sum(1 for line in result.stdout.splitlines() if "Chipset Model:" in line)
        return 0
    if system_name == "Linux":
        count = _host_gpu_count_from_command(
            ["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"]
        )
        if count:
            return count

    return 0

def _node_display_name() -> str:
    explicit = os.getenv("MN_NODE_DISPLAY_NAME", "").strip()
    if explicit:
        return explicit
    try:
        hostname = socket.gethostname().strip()
        if hostname:
            return hostname.split(".", 1)[0]
    except OSError:
        pass
    return "local"

def _ensure_node_advertisement_settings(env: dict[str, str]) -> dict[str, str]:
    adjusted = dict(env)
    if not os.getenv("MN_NODE_DISPLAY_NAME", "").strip():
        adjusted["MN_NODE_DISPLAY_NAME"] = _node_display_name()

    detected_gpu_count = _detect_host_gpu_count()
    existing_gpu_count = _parse_gpu_count(adjusted.get("MN_NODE_GPU_COUNT"))
    if os.getenv("MN_NODE_GPU_COUNT", "").strip():
        adjusted["MN_NODE_GPU_COUNT"] = str(detected_gpu_count)
    elif detected_gpu_count > 0 or existing_gpu_count is None:
        adjusted["MN_NODE_GPU_COUNT"] = str(detected_gpu_count)

    return adjusted

def _compose_runtime_env(env: dict[str, str], ip: Optional[str]) -> dict[str, str]:
    compose_env = dict(env)
    if not str(compose_env.get("MN_NODE_ROLE") or "").strip():
        compose_env["MN_NODE_ROLE"] = "runtime"

    if ip:
        if not str(compose_env.get("MN_DIST_PORT") or "").strip():
            compose_env["MN_DIST_PORT"] = DEFAULT_DIST_PORT
        local_host = str(compose_env.get("MN_NETWORK_ADVERTISE_HOST") or "").strip() or _detect_lan_ip()
        docker_mode_value = str(compose_env.get("MN_DOCKER_NETWORK_MODE") or "").strip().lower()
        node_alias = str(compose_env.get("MN_NODE_ALIAS") or "").strip()
        docker_identity = _docker_network_uses_internal_identity(docker_mode_value) and node_alias
        local_node_name = _docker_node_name(node_alias) if docker_identity else _network_node_name(local_host)
        existing_cluster_nodes = str(compose_env.get("MN_CLUSTER_NODES") or "").strip()
        seed_node_name = (
            existing_cluster_nodes
            if docker_identity and not _cluster_nodes_unset(existing_cluster_nodes)
            else _network_node_name(ip)
        )
        if _generated_node_setting_should_update("MN_NODE_NAME", compose_env.get("MN_NODE_NAME"), local_node_name):
            compose_env["MN_NODE_NAME"] = local_node_name
        if (
            (
                not docker_identity
                and _generated_cluster_setting_should_update(compose_env.get("MN_CLUSTER_NODES"), seed_node_name)
            )
            or (docker_identity and _cluster_nodes_unset(compose_env.get("MN_CLUSTER_NODES")))
            or compose_env.get("MN_CLUSTER_NODES") == ip
        ):
            compose_env["MN_CLUSTER_NODES"] = seed_node_name
        compose_env["MN_REDIS_URL"] = compose_env.get("MN_REDIS_URL") or f"redis://{ip}:6379/0"

        if compose_env.get("MN_NODE_NAME"):
            dist_port = compose_env.get("MN_DIST_PORT", DEFAULT_DIST_PORT)
            if _erl_aflags_needs_update(compose_env.get("ERL_AFLAGS"), dist_port):
                compose_env["ERL_AFLAGS"] = _erl_aflags(dist_port)

    return compose_env

def _host_port_available(host: str, port: int) -> bool:
    bind_host = "127.0.0.1" if host in {"", "localhost"} else host
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.bind((bind_host, port))
        return True
    except OSError:
        return False
    finally:
        probe.close()

def _container_publishes_port(container_name: str, target_port: int, published_port: int) -> bool:
    try:
        result = subprocess.run(
            ["docker", "port", container_name, f"{target_port}/tcp"],
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        return False
    if result.returncode != 0:
        return False

    for line in result.stdout.splitlines():
        _, _, port_text = line.rpartition(":")
        try:
            if int(port_text) == published_port:
                return True
        except ValueError:
            continue
    return False

def _docker_container_running(container_name: str) -> bool:
    try:
        result = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Running}}", container_name],
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        return False
    return result.stdout.strip().lower() == "true"

def _published_container_port(container_name: str, target_port: int) -> Optional[int]:
    if not _docker_container_running(container_name):
        return None

    try:
        result = subprocess.run(
            ["docker", "port", container_name, f"{target_port}/tcp"],
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        return None
    if result.returncode != 0:
        return None

    for line in result.stdout.splitlines():
        _, _, port_text = line.rpartition(":")
        try:
            port = int(port_text)
        except ValueError:
            continue
        if 1 <= port <= 65535:
            return port
    return None

def _port_available_or_owned(host: str, port: int, owner_container: str, target_port: int) -> bool:
    return _host_port_available(host, port) or _container_publishes_port(owner_container, target_port, port)

def _find_available_published_port(host: str, preferred: int, owner_container: str, target_port: int) -> int:
    if _port_available_or_owned(host, preferred, owner_container, target_port):
        return preferred

    for candidate in range(REDIS_DYNAMIC_PORT_START, REDIS_DYNAMIC_PORT_END + 1):
        if _port_available_or_owned(host, candidate, owner_container, target_port):
            return candidate
    return 0

def _parse_configured_port(value: object) -> Optional[int]:
    if value is None or str(value).strip() == "":
        return None
    try:
        port = int(str(value).strip())
    except ValueError:
        return None
    if 1 <= port <= 65535:
        return port
    return None

def _resolve_published_redis_port(
    *,
    bind_host: str,
    configured_port: object,
    explicit: bool,
    owner_container: str,
) -> int:
    requested_port = _parse_configured_port(configured_port)
    if explicit:
        if requested_port is None:
            console.print("[red]Error: MN_REDIS_PORT must be a TCP port between 1 and 65535.[/red]")
            raise typer.Exit(1)
        if not _port_available_or_owned(bind_host, requested_port, owner_container, REDIS_CONTAINER_PORT):
            console.print(f"[red]Error: Redis port {requested_port} is already in use.[/red]")
            raise typer.Exit(1)
        return requested_port

    current_port = _published_container_port(owner_container, REDIS_CONTAINER_PORT)
    if current_port and REDIS_DYNAMIC_PORT_START <= current_port <= REDIS_DYNAMIC_PORT_END:
        return current_port

    preferred = requested_port or REDIS_DYNAMIC_PORT_START
    if not (REDIS_DYNAMIC_PORT_START <= preferred <= REDIS_DYNAMIC_PORT_END):
        preferred = REDIS_DYNAMIC_PORT_START

    selected = _find_available_published_port(
        bind_host,
        preferred,
        owner_container,
        REDIS_CONTAINER_PORT,
    )
    if selected == 0:
        console.print(
            f"[red]Error: No Redis port is available in {REDIS_DYNAMIC_PORT_START}-{REDIS_DYNAMIC_PORT_END}.[/red]"
        )
        raise typer.Exit(1)
    return selected

def _redis_url_with_database(redis_url: str, database: str) -> str:
    parsed = urlparse(redis_url)
    scheme = parsed.scheme or "redis"
    netloc = parsed.netloc
    if not netloc and parsed.hostname:
        netloc = parsed.hostname
    return f"{scheme}://{netloc}/{database}"

def _avoid_local_compose_port_conflicts(env: dict[str, str]) -> dict[str, str]:
    return dict(env)

def _resolve_mn_cookie() -> str:
    env_cookie = os.getenv("MN_COOKIE", "").strip()
    if env_cookie and env_cookie != "mirrorneuron":
        return env_cookie

    cookie_file = DIR / "erlang.cookie"
    try:
        existing_cookie = cookie_file.read_text().strip()
        if existing_cookie and existing_cookie != "mirrorneuron":
            return existing_cookie
    except FileNotFoundError:
        pass

    DIR.mkdir(parents=True, exist_ok=True)
    generated_cookie = secrets.token_hex(32)
    fd = os.open(cookie_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(f"{generated_cookie}\n")
    try:
        cookie_file.chmod(0o600)
    except OSError:
        logger.debug("Failed to chmod Erlang cookie file %s", cookie_file, exc_info=True)
    return generated_cookie

def _resolve_grpc_auth_token() -> str:
    env_token = os.getenv("MN_GRPC_AUTH_TOKEN", "").strip()
    if env_token:
        return env_token

    token_file = DIR / "grpc_auth.token"
    try:
        existing_token = token_file.read_text().strip()
        if existing_token:
            return existing_token
    except FileNotFoundError:
        pass

    DIR.mkdir(parents=True, exist_ok=True)
    generated_token = secrets.token_hex(32)
    _write_grpc_token_file(token_file, generated_token, "gRPC auth")
    return generated_token

def _resolve_grpc_admin_token() -> str:
    env_token = (
        os.getenv(GRPC_ADMIN_TOKEN_ENV, "").strip()
        or os.getenv(LEGACY_GRPC_ADMIN_TOKEN_ENV, "").strip()
    )
    if env_token:
        return env_token

    token_file = DIR / "grpc_admin.token"
    try:
        existing_token = token_file.read_text().strip()
        if existing_token:
            return existing_token
    except FileNotFoundError:
        pass

    DIR.mkdir(parents=True, exist_ok=True)
    generated_token = secrets.token_hex(32)
    _write_grpc_token_file(token_file, generated_token, "gRPC admin")
    return generated_token

def _write_grpc_token_file(token_file: Path, token: str, label: str) -> None:
    token = str(token or "").strip()
    if not token:
        return
    token_file.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(token_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(f"{token}\n")
    try:
        token_file.chmod(0o600)
    except OSError:
        logger.debug("Failed to chmod %s token file %s", label, token_file, exc_info=True)

def _ensure_runtime_grpc_tokens(env: dict[str, str], *, persist_compose: bool = False) -> dict[str, str]:
    resolved = dict(env)
    if not str(resolved.get("MN_GRPC_AUTH_TOKEN") or "").strip():
        resolved["MN_GRPC_AUTH_TOKEN"] = _resolve_grpc_auth_token()
    if not str(resolved.get(GRPC_ADMIN_TOKEN_ENV) or "").strip():
        resolved[GRPC_ADMIN_TOKEN_ENV] = str(resolved.get(LEGACY_GRPC_ADMIN_TOKEN_ENV) or "").strip()
    if not str(resolved.get(GRPC_ADMIN_TOKEN_ENV) or "").strip():
        resolved[GRPC_ADMIN_TOKEN_ENV] = _resolve_grpc_admin_token()

    _write_grpc_token_file(DIR / "grpc_auth.token", resolved["MN_GRPC_AUTH_TOKEN"], "gRPC auth")
    _write_grpc_token_file(
        DIR / "grpc_admin.token",
        resolved[GRPC_ADMIN_TOKEN_ENV],
        "gRPC admin",
    )
    if persist_compose:
        _write_env_file_values(
            RUNTIME_COMPOSE_ENV,
            {
                "MN_GRPC_AUTH_TOKEN": resolved["MN_GRPC_AUTH_TOKEN"],
                GRPC_ADMIN_TOKEN_ENV: resolved[GRPC_ADMIN_TOKEN_ENV],
            },
        )
    return resolved

def _runtime_grpc_tokens_from_running_container() -> dict[str, str]:
    tokens: dict[str, str] = {}
    for container_name in (LOCAL_CORE_CONTAINER, NETWORK_CORE_CONTAINER):
        auth_token = _docker_container_env_value(container_name, "MN_GRPC_AUTH_TOKEN")
        admin_token = _docker_container_env_value(container_name, GRPC_ADMIN_TOKEN_ENV)
        if not admin_token:
            admin_token = _docker_container_env_value(container_name, LEGACY_GRPC_ADMIN_TOKEN_ENV)
        if auth_token:
            tokens["MN_GRPC_AUTH_TOKEN"] = auth_token
        if admin_token:
            tokens[GRPC_ADMIN_TOKEN_ENV] = admin_token
        if tokens:
            break
    return tokens

def _resolve_network_token(force_new: bool = False) -> str:
    if force_new:
        return _refresh_network_token()

    try:
        existing_token = NETWORK_TOKEN_FILE.read_text().strip()
        if existing_token:
            return existing_token
    except FileNotFoundError:
        pass

    compose_token = _read_env_file(RUNTIME_COMPOSE_ENV).get("MN_NETWORK_JOIN_TOKEN", "").strip()
    if compose_token:
        _write_network_token(compose_token)
        return compose_token

    env_token = os.getenv("MN_NETWORK_JOIN_TOKEN", "").strip()
    if env_token:
        _write_network_token(env_token)
        return env_token

    DIR.mkdir(parents=True, exist_ok=True)
    generated_token = secrets.token_urlsafe(32)
    _write_network_token(generated_token)
    return generated_token

def _refresh_network_token() -> str:
    token = secrets.token_urlsafe(32)
    _write_network_token(token)
    if RUNTIME_COMPOSE_ENV.exists():
        _write_env_file_values(RUNTIME_COMPOSE_ENV, {"MN_NETWORK_JOIN_TOKEN": token})
    return token

def _write_network_token(token: str) -> None:
    DIR.mkdir(parents=True, exist_ok=True)
    fd = os.open(NETWORK_TOKEN_FILE, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(f"{token}\n")
    try:
        NETWORK_TOKEN_FILE.chmod(0o600)
    except OSError:
        logger.debug("Failed to chmod network token file %s", NETWORK_TOKEN_FILE, exc_info=True)

def _derive_network_secret(token: str, label: str) -> str:
    material = f"mirror-neuron:{label}:{token}".encode("utf-8")
    return hashlib.sha256(material).hexdigest()

def _network_node_name(host: str) -> str:
    return f"mirror_neuron@{host}"

def _node_alias_file() -> Path:
    return DIR / "node.alias"

def _valid_node_alias(value: str) -> bool:
    if not value or len(value) > 63:
        return False
    if not value[0].isalnum() or not value[-1].isalnum():
        return False
    return all(ch.isalnum() or ch == "-" for ch in value)

def _normalize_node_alias(value: object) -> str:
    return str(value or "").strip().lower()

def _write_node_alias(alias: str) -> None:
    DIR.mkdir(parents=True, exist_ok=True)
    alias_file = _node_alias_file()
    fd = os.open(alias_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        f.write(f"{alias}\n")
    try:
        alias_file.chmod(0o600)
    except OSError:
        logger.debug("Failed to chmod node alias file %s", alias_file, exc_info=True)

def _configured_node_alias(env: Optional[dict[str, str]] = None) -> str:
    values = env or {}
    for value in (
        os.getenv("MN_NODE_ALIAS", "").strip(),
        str(values.get("MN_NODE_ALIAS") or "").strip(),
        _read_env_file(RUNTIME_COMPOSE_ENV).get("MN_NODE_ALIAS", "").strip(),
    ):
        alias = _normalize_node_alias(value)
        if alias:
            return alias

    try:
        return _normalize_node_alias(_node_alias_file().read_text(encoding="utf-8"))
    except OSError:
        return ""

def _resolve_node_alias(env: Optional[dict[str, str]] = None) -> str:
    alias = _configured_node_alias(env)
    if alias:
        if not _valid_node_alias(alias):
            console.print(
                "[red]Error: MN_NODE_ALIAS must be 1-63 lowercase letters, numbers, or hyphens, "
                "and must start and end with a letter or number.[/red]"
            )
            raise typer.Exit(1)
        _write_node_alias(alias)
        return alias

    generated = f"mn-{secrets.token_hex(4)}"
    _write_node_alias(generated)
    return generated

def _docker_node_name(alias: str) -> str:
    return _network_node_name(alias)

def _docker_redis_alias(alias: str) -> str:
    return f"{alias}-redis"

def _docker_network_name(name: Optional[str] = None) -> str:
    return (
        name
        or os.getenv("MN_DOCKER_NETWORK_NAME", "").strip()
        or _read_env_file(RUNTIME_COMPOSE_ENV).get("MN_DOCKER_NETWORK_NAME", "").strip()
        or DEFAULT_DOCKER_NETWORK_NAME
    )

def _docker_network_mode(mode: Optional[str] = None, *, default: str = "bridge") -> str:
    raw = (mode or os.getenv("MN_DOCKER_NETWORK_MODE", "").strip() or default).strip().lower()
    if raw in {"", "bridge"}:
        return "bridge"
    if raw == "overlay":
        return "overlay"
    if raw in {"disabled", "disable", "none", "off", "host", "ip"}:
        return "disabled"
    console.print("[red]Error: Docker network mode must be bridge, overlay, or disabled.[/red]")
    raise typer.Exit(1)

def _docker_network_uses_internal_identity(mode: str) -> bool:
    return mode in {"bridge", "overlay"}

def _docker_network_command_args(mode: Optional[str], name: Optional[str]) -> str:
    if not mode or not _docker_network_uses_internal_identity(mode):
        return ""
    network_name = name or DEFAULT_DOCKER_NETWORK_NAME
    if mode == "bridge" and network_name == DEFAULT_DOCKER_NETWORK_NAME:
        return ""
    return f" --network {mode} --docker-network {network_name}"

def _inspect_docker_network(name: str) -> Optional[dict[str, object]]:
    try:
        result = subprocess.run(
            ["docker", "network", "inspect", name],
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        console.print("[red]Error: Docker is not installed or not in PATH.[/red]")
        raise typer.Exit(1)
    if result.returncode != 0:
        return None
    try:
        inspected = json.loads(result.stdout)
    except json.JSONDecodeError:
        console.print(f"[red]Error: Could not parse Docker network inspect output for {name}.[/red]")
        raise typer.Exit(1)
    if isinstance(inspected, list) and inspected and isinstance(inspected[0], dict):
        return inspected[0]
    console.print(f"[red]Error: Docker network inspect returned unexpected data for {name}.[/red]")
    raise typer.Exit(1)

def _ensure_docker_network(mode: str, name: str) -> None:
    if mode == "disabled":
        return

    inspected = _inspect_docker_network(name)
    if mode == "bridge":
        if inspected is None:
            subprocess.run(
                ["docker", "network", "create", "--driver", "bridge", name],
                check=True,
                stdout=subprocess.DEVNULL,
            )
            return
        if str(inspected.get("Driver") or "") != "bridge":
            console.print(f"[red]Error: Docker network {name} exists but is not a bridge network.[/red]")
            raise typer.Exit(1)
        return

    if inspected is None:
        console.print(f"[red]Error: Docker overlay network {name} does not exist.[/red]")
        console.print("Create it first with:")
        console.print(f"  docker network create --driver overlay --attachable {name}")
        raise typer.Exit(1)
    if str(inspected.get("Driver") or "") != "overlay":
        console.print(f"[red]Error: Docker network {name} exists but is not an overlay network.[/red]")
        raise typer.Exit(1)
    if inspected.get("Attachable") is not True:
        console.print(f"[red]Error: Docker overlay network {name} is not attachable.[/red]")
        console.print("Create an attachable overlay network with:")
        console.print(f"  docker network create --driver overlay --attachable {name}")
        raise typer.Exit(1)

def _docker_network_env(mode: str, name: str, alias: str) -> dict[str, str]:
    driver = "overlay" if mode == "overlay" else "bridge"
    return {
        "MN_DOCKER_NETWORK_MODE": mode,
        "MN_DOCKER_NETWORK_NAME": name,
        "MN_DOCKER_NETWORK_DRIVER": driver,
        "MN_DOCKER_NETWORK_ATTACHABLE": "true" if mode == "overlay" else "false",
        "MN_DOCKER_NETWORK_EXTERNAL": "true",
        "MN_NODE_ALIAS": alias,
    }

def _docker_network_run_args(mode: str, name: str, alias: str) -> list[str]:
    if mode == "disabled":
        return []
    return ["--network", name, "--network-alias", alias]

def _network_node_host(node_name: object) -> str:
    normalized = str(node_name or "").strip()
    if "@" not in normalized:
        return ""
    return normalized.rsplit("@", 1)[1].strip()

def _node_name_unset(value: object) -> bool:
    return str(value or "").strip() in {"", "nonode@nohost"}

def _cluster_nodes_unset(value: object) -> bool:
    normalized = str(value or "").strip()
    return normalized in {"", "nonode@nohost"}

def _generated_network_node_name(value: object) -> bool:
    normalized = str(value or "").strip()
    return normalized.startswith("mirror_neuron@") and "," not in normalized

def _generated_node_setting_should_update(key: str, value: object, desired: str) -> bool:
    if os.getenv(key, "").strip():
        return False
    normalized = str(value or "").strip()
    return _node_name_unset(normalized) or (
        _generated_network_node_name(normalized) and normalized != desired
    )

def _generated_cluster_setting_should_update(value: object, desired: str) -> bool:
    if os.getenv("MN_CLUSTER_NODES", "").strip():
        return False
    normalized = str(value or "").strip()
    return _cluster_nodes_unset(normalized) or (
        _generated_network_node_name(normalized) and normalized != desired
    )

def _persisted_join_profile(env: dict[str, str]) -> bool:
    cluster_nodes = str(env.get("MN_CLUSTER_NODES") or "").strip()
    node_name = str(env.get("MN_NODE_NAME") or "").strip()
    return (
        _generated_network_node_name(cluster_nodes)
        and _generated_network_node_name(node_name)
        and cluster_nodes != node_name
    )

def _advertised_network_host(host: Optional[str] = None) -> str:
    configured = host or os.getenv("MN_NETWORK_ADVERTISE_HOST", "").strip()
    return (configured or _detect_lan_ip()).strip() or "127.0.0.1"

def _parse_port(value: object, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default

def _compose_supports_redis_password() -> bool:
    try:
        return "MN_REDIS_PASSWORD" in RUNTIME_COMPOSE_FILE.read_text(encoding="utf-8")
    except OSError:
        return False

def _redis_url_with_public_endpoint(redis_url: str, host: str, port: int) -> str:
    parsed = urlparse(redis_url)
    scheme = parsed.scheme or "redis"
    path = parsed.path or "/0"
    userinfo = ""
    if "@" in parsed.netloc:
        userinfo = f"{parsed.netloc.rsplit('@', 1)[0]}@"
    return f"{scheme}://{userinfo}{host}:{port}{path}"

def _handshake_node_info(local_host: str, node_name: Optional[str] = None) -> dict[str, object]:
    hostname = ""
    try:
        hostname = socket.gethostname().strip()
    except OSError:
        pass

    return {
        "node_name": node_name or _network_node_name(local_host),
        "display_name": _node_display_name(),
        "hostname": hostname,
        "gpu_count": _detect_host_gpu_count(),
    }

def _handshake_with_main_node(
    seed_host: str,
    token: str,
    grpc_port: int,
    *,
    local_host: Optional[str] = None,
    local_node_name: Optional[str] = None,
) -> dict:
    from mn_sdk import Client

    target = f"{seed_host}:{grpc_port}"
    advertised_node_name = local_node_name or (_network_node_name(local_host) if local_host else "")
    local_node_info = (
        _handshake_node_info(local_host, node_name=advertised_node_name)
        if local_host
        else None
    )
    try:
        handshake = Client(target=target, auth_token="", timeout=10).network_handshake(
            token,
            node_name=advertised_node_name,
            node_info=local_node_info,
        )
    except Exception as exc:
        console.print(f"[red]Error: Could not join MirrorNeuron node at {target}.[/red]")
        console.print("Check the host, gRPC port, and token printed by 'mn runtime start' on the main box.")
        console.print(f"[dim]{exc}[/dim]")
        raise typer.Exit(1) from exc

    if _node_name_unset(handshake.get("node_name")):
        handshake["node_name"] = _network_node_name(seed_host)
    return handshake

def _docker_container_running(name: str) -> bool:
    try:
        result = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Running}}", name],
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        console.print("[red]Error: Docker is not installed or not in PATH.[/red]")
        raise typer.Exit(1)
    return result.returncode == 0 and result.stdout.strip() == "true"

def _docker_container_env_value(name: str, key: str) -> Optional[str]:
    try:
        result = subprocess.run(
            ["docker", "inspect", "-f", "{{range .Config.Env}}{{println .}}{{end}}", name],
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        return None
    if result.returncode != 0:
        return None

    prefix = f"{key}="
    for line in result.stdout.splitlines():
        if line.startswith(prefix):
            return line[len(prefix) :].strip() or None
    return None

def _ensure_network_docker_network(mode: str = "bridge", name: Optional[str] = None) -> None:
    _ensure_docker_network(mode, name or NETWORK_DOCKER_NETWORK)

def _network_redis_url(token: str, redis_host: str, redis_port: int) -> str:
    password = _derive_network_secret(token, "redis")
    return f"redis://:{password}@{redis_host}:{redis_port}/0"

def _network_core_env(
    *,
    token: str,
    host: str,
    docker_network_mode: str,
    docker_network_name: str,
    node_alias: str,
    node_name: str,
    cluster_nodes: str,
    grpc_port: int,
    epmd_port: int,
    dist_port: int,
    redis_url: str,
    redis_public_host: str,
    redis_public_port: int,
) -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "MN_NETWORK_ONLY": "true",
            "MN_REDIS_FORWARD_PRIMARY": "true",
            "MN_NETWORK_JOIN_TOKEN": token,
            "MN_NETWORK_ADVERTISE_HOST": host,
            "MN_NETWORK_REDIS_HOST": redis_public_host,
            "MN_NETWORK_REDIS_PORT": str(redis_public_port),
            "MN_NODE_ALIAS": node_alias,
            "MN_DOCKER_NETWORK_MODE": docker_network_mode,
            "MN_DOCKER_NETWORK_NAME": docker_network_name,
            "MN_CORE_HOST": "0.0.0.0",
            "MN_GRPC_PORT": str(grpc_port),
            "MN_EPMD_PORT": str(epmd_port),
            "MN_NODE_NAME": node_name,
            "MN_NODE_ROLE": "runtime",
            "MN_CLUSTER_NODES": cluster_nodes,
            "MN_REDIS_URL": redis_url,
            "MN_DIST_PORT": str(dist_port),
            "MN_COOKIE": _derive_network_secret(token, "cookie"),
            "MN_GRPC_AUTH_TOKEN": _derive_network_secret(token, "grpc-auth"),
            GRPC_ADMIN_TOKEN_ENV: _derive_network_secret(token, "grpc-admin"),
            "ERL_EPMD_ADDRESS": "0.0.0.0",
            "ERL_EPMD_PORT": str(epmd_port),
            "ERL_AFLAGS": _erl_aflags(dist_port),
        }
    )
    env = _ensure_node_advertisement_settings(env)
    return env

def _docker_env_args(env: dict[str, str]) -> list[str]:
    args: list[str] = []
    for key in sorted(env):
        if key.startswith("MN_") or key in {"ERL_AFLAGS", "ERL_EPMD_ADDRESS", "ERL_EPMD_PORT"}:
            args.extend(["-e", f"{key}={env[key]}"])
    return args

def _start_network_redis(
    host: str,
    redis_port: Optional[int],
    token: str,
    *,
    docker_network_mode: str,
    docker_network_name: str,
    redis_alias: str,
    publish_host_port: bool = False,
) -> None:
    subprocess.run(["docker", "rm", "-f", NETWORK_REDIS_CONTAINER], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
    publish_host = _network_publish_host(host)
    password = _derive_network_secret(token, "redis")
    data_dir = DIR / "network-redis"
    data_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        "docker",
        "run",
        "-d",
        "--name",
        NETWORK_REDIS_CONTAINER,
        *_docker_network_run_args(docker_network_mode, docker_network_name, redis_alias),
        "-v",
        f"{data_dir}:/data",
        "redis:7",
        "redis-server",
        "--appendonly",
        "yes",
        "--requirepass",
        password,
        "--masterauth",
        password,
    ]
    if publish_host_port:
        volume_index = cmd.index("-v")
        cmd[volume_index:volume_index] = ["-p", f"{publish_host}:{redis_port or REDIS_CONTAINER_PORT}:6379"]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL)

def _start_network_core(
    env: dict[str, str],
    host: str,
    grpc_port: int,
    dist_port: int,
    *,
    docker_network_mode: str,
    docker_network_name: str,
    node_alias: str,
    publish_cluster_ports: bool = False,
) -> None:
    subprocess.run(["docker", "rm", "-f", NETWORK_CORE_CONTAINER], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
    publish_host = _network_publish_host(host)
    env_args = _docker_env_args(env)
    port_args = ["-p", f"{publish_host}:{grpc_port}:{grpc_port}"]
    if publish_cluster_ports:
        epmd_port = _parse_configured_port(env.get("MN_EPMD_PORT")) or int(DEFAULT_EPMD_PORT)
        port_args.extend(
            [
                "-p",
                f"{publish_host}:{epmd_port}:{epmd_port}",
                "-p",
                f"{publish_host}:{dist_port}:{dist_port}",
            ]
        )
    cmd = [
        "docker",
        "run",
        "-d",
        "--name",
        NETWORK_CORE_CONTAINER,
        *_docker_network_run_args(docker_network_mode, docker_network_name, node_alias),
        *port_args,
        *env_args,
        "mirror-neuron-core:latest",
    ]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL)

def _running_network_token(container_names: tuple[str, ...] = ()) -> Optional[str]:
    for container_name in container_names:
        token = _docker_container_env_value(container_name, "MN_NETWORK_JOIN_TOKEN")
        if token:
            return token

    try:
        token = NETWORK_TOKEN_FILE.read_text(encoding="utf-8").strip()
        if token:
            return token
    except OSError:
        pass

    token = _read_env_file(RUNTIME_COMPOSE_ENV).get("MN_NETWORK_JOIN_TOKEN", "").strip()
    if token:
        return token

    env_token = os.getenv("MN_NETWORK_JOIN_TOKEN", "").strip()
    return env_token or None

def _running_network_host(host: Optional[str], container_names: tuple[str, ...] = ()) -> str:
    configured_host = (host or "").strip()
    if configured_host:
        return configured_host

    for container_name in container_names:
        advertised_host = _docker_container_env_value(container_name, "MN_NETWORK_ADVERTISE_HOST")
        if advertised_host:
            return advertised_host

    advertised_host = _read_env_file(RUNTIME_COMPOSE_ENV).get("MN_NETWORK_ADVERTISE_HOST", "").strip()
    if advertised_host:
        return advertised_host

    return _advertised_network_host(host)

def _print_network_seed_ready(
    host: str,
    grpc_port: int,
    token: str,
    *,
    node_name: Optional[str] = None,
    docker_network_mode: Optional[str] = None,
    docker_network_name: Optional[str] = None,
    already_running: bool = False,
    worker_node: bool = False,
) -> None:
    node_name = node_name or _network_node_name(host)
    network_args = _docker_network_command_args(docker_network_mode, docker_network_name)
    details = [
        ("Host", host),
        ("gRPC", f"{host}:{grpc_port}"),
        ("Node", node_name),
        ("Token", token),
    ]
    if already_running:
        print_confirmed(
            console,
            "MirrorNeuron node ready",
            status="already running",
            details=details,
            next_steps=f"mn node join {host} --token {token}{network_args}",
        )
    elif worker_node:
        print_success_confirmation(
            console,
            "Worker node start",
            status="running",
            details=details,
            next_steps=f"mn node join {host} --token {token}{network_args}",
        )
    else:
        print_success_confirmation(
            console,
            "Node expose",
            status="running",
            details=details,
            next_steps=f"mn node join {host} --token {token}{network_args}",
        )

def _return_running_network_seed(
    host: Optional[str],
    grpc_port: int,
    container_names: tuple[str, ...] = (),
) -> str:
    token = _running_network_token(container_names)
    if not token:
        console.print("[red]Error: MirrorNeuron is already running, but no network token was found.[/red]")
        console.print(f"Expected a token in {NETWORK_TOKEN_FILE}.")
        raise typer.Exit(1)

    advertised_host = _running_network_host(host, container_names)
    node_name = _docker_container_env_value(container_names[0], "MN_NODE_NAME") if container_names else None
    docker_mode = _docker_container_env_value(container_names[0], "MN_DOCKER_NETWORK_MODE") if container_names else None
    docker_network = _docker_container_env_value(container_names[0], "MN_DOCKER_NETWORK_NAME") if container_names else None
    _print_network_seed_ready(
        advertised_host,
        grpc_port,
        token,
        node_name=node_name,
        docker_network_mode=docker_mode,
        docker_network_name=docker_network,
        already_running=True,
    )
    return token

def _compose_project_name() -> str:
    return (
        _read_env_file(RUNTIME_COMPOSE_ENV).get("COMPOSE_PROJECT_NAME", "").strip()
        or os.getenv("COMPOSE_PROJECT_NAME", "").strip()
        or "mirror-neuron"
    )

def _stop_local_runtime_for_worker() -> None:
    _stop_network_runtime()
    if runtime_compose_available():
        subprocess.run(runtime_compose_cmd("down"), stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
    else:
        subprocess.run(["docker", "rm", "-f", LOCAL_CORE_CONTAINER], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)

    for pid_file, _name in [
        *web_ui_pid_files(),
        *api_pid_files(),
        (BEAM_PID_FILE, "Legacy Core Service"),
    ]:
        if not pid_file.exists():
            continue
        try:
            pid = int(pid_file.read_text().strip())
            try:
                os.kill(pid, 0)
                kill_tree(pid)
            except OSError:
                pass
        except ValueError:
            pass
        try:
            pid_file.unlink(missing_ok=True)
        except OSError:
            logger.debug("Failed to remove runtime pid file %s during worker start", pid_file, exc_info=True)

def _clear_worker_redis_state() -> None:
    shutil.rmtree(DIR / "network-redis", ignore_errors=True)
    NETWORK_REDIS_ENV_FILE.unlink(missing_ok=True)

    if runtime_compose_available():
        subprocess.run(
            ["docker", "volume", "rm", "-f", f"{_compose_project_name()}_redis-data"],
            stderr=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
        )

def _start_worker_node(
    host: Optional[str] = None,
    grpc_port: int = int(DEFAULT_GRPC_PORT),
    dist_port: int = int(DEFAULT_DIST_PORT),
    redis_port: Optional[int] = None,
    docker_network_mode: Optional[str] = None,
    docker_network_name: Optional[str] = None,
) -> str:
    console.print("=> Preparing this box as a clean MirrorNeuron worker node...")
    _stop_local_runtime_for_worker()
    _clear_worker_redis_state()
    _refresh_network_token()
    return _start_network_seed(
        host=host,
        grpc_port=grpc_port,
        dist_port=dist_port,
        redis_port=redis_port,
        force_new_token=False,
        docker_network_mode=docker_network_mode,
        docker_network_name=docker_network_name,
        worker_node=True,
    )

def _start_network_seed(
    host: Optional[str] = None,
    grpc_port: int = int(DEFAULT_GRPC_PORT),
    dist_port: int = int(DEFAULT_DIST_PORT),
    redis_port: Optional[int] = None,
    force_new_token: bool = False,
    docker_network_mode: Optional[str] = None,
    docker_network_name: Optional[str] = None,
    worker_node: bool = False,
) -> str:
    if check_status(API_PID_FILE) == 0:
        return _return_running_network_seed(host, grpc_port, (LOCAL_CORE_CONTAINER,))

    if _docker_container_running(NETWORK_CORE_CONTAINER):
        return _return_running_network_seed(host, grpc_port, (NETWORK_CORE_CONTAINER,))

    if _docker_container_running(LOCAL_CORE_CONTAINER):
        return _return_running_network_seed(host, grpc_port, (LOCAL_CORE_CONTAINER,))

    host = (host or _detect_lan_ip()).strip() or "127.0.0.1"
    env = _runtime_base_env(runtime_compose_available())
    requested_mode = _docker_network_mode(docker_network_mode, default="disabled")
    container_network_mode = requested_mode
    use_internal_identity = _docker_network_uses_internal_identity(requested_mode)
    network_name = _docker_network_name(docker_network_name)
    epmd_port = _parse_configured_port(
        os.getenv("MN_EPMD_PORT", "").strip()
        or _read_env_file(RUNTIME_COMPOSE_ENV).get("MN_EPMD_PORT")
    ) or int(DEFAULT_EPMD_PORT)
    node_alias = _resolve_node_alias(env)
    node_name = _docker_node_name(node_alias) if use_internal_identity else _network_node_name(host)
    redis_alias = _docker_redis_alias(node_alias)
    if force_new_token:
        console.print("[yellow]--force-new-token is deprecated; run 'mn node refresh-token' to rotate the join token.[/yellow]")
    token = _resolve_network_token()
    external_redis_url = os.getenv("MN_REDIS_URL", "").strip()
    selected_redis_port = (
        _resolve_network_seed_redis_port(host, redis_port)
        if not external_redis_url and not use_internal_identity
        else None
    )
    redis_url = external_redis_url or (
        _network_redis_url(token, redis_alias, 6379)
        if use_internal_identity
        else _network_redis_url(token, host, selected_redis_port or REDIS_CONTAINER_PORT)
    )
    redis_public_host, redis_public_port_value = _host_port_from_target(
        external_redis_url,
        host,
        str(selected_redis_port or REDIS_CONTAINER_PORT),
    ) if external_redis_url else (
        (redis_alias, str(REDIS_CONTAINER_PORT))
        if use_internal_identity
        else (host, str(selected_redis_port))
    )
    try:
        redis_public_port = int(redis_public_port_value)
    except ValueError:
        redis_public_port = selected_redis_port or REDIS_CONTAINER_PORT

    _ensure_network_docker_network(container_network_mode, network_name)
    if not external_redis_url:
        console.print("=> Starting network Redis...")
        _start_network_redis(
            host,
            selected_redis_port,
            token,
            docker_network_mode=container_network_mode,
            docker_network_name=network_name,
            redis_alias=redis_alias,
            publish_host_port=not use_internal_identity,
        )

    env = _network_core_env(
        token=token,
        host=host,
        docker_network_mode=requested_mode,
        docker_network_name=network_name,
        node_alias=node_alias,
        node_name=node_name,
        cluster_nodes=node_name,
        grpc_port=grpc_port,
        epmd_port=epmd_port,
        dist_port=dist_port,
        redis_url=redis_url,
        redis_public_host=redis_public_host,
        redis_public_port=redis_public_port,
    )

    _ensure_runtime_grpc_tokens(env, persist_compose=runtime_compose_available())

    console.print("=> Starting MirrorNeuron core-only exposed node...")
    _start_network_core(
        env,
        host,
        grpc_port,
        dist_port,
        docker_network_mode=container_network_mode,
        docker_network_name=network_name,
        node_alias=node_alias,
        publish_cluster_ports=not use_internal_identity,
    )

    _print_network_seed_ready(
        host,
        grpc_port,
        token,
        node_name=node_name,
        docker_network_mode=requested_mode,
        docker_network_name=network_name,
        worker_node=worker_node,
    )
    return token

def _join_network(
    seed_host: str,
    token: str,
    host: Optional[str] = None,
    grpc_port: int = int(DEFAULT_GRPC_PORT),
    dist_port: int = int(DEFAULT_DIST_PORT),
    redis_port: Optional[int] = None,
    docker_network_mode: Optional[str] = None,
    docker_network_name: Optional[str] = None,
    action: str = "Node join",
) -> dict:
    from mn_sdk import Client
    from mn_cli.shared import client as local_client

    target = f"{seed_host}:{grpc_port}"
    local_host = (host or _detect_lan_ip()).strip()
    requested_mode = _docker_network_mode(docker_network_mode, default="disabled")
    network_name = _docker_network_name(docker_network_name)
    env = _runtime_base_env(runtime_compose_available())
    local_node_name = _network_node_name(local_host)
    if requested_mode != "disabled":
        _ensure_network_docker_network(requested_mode, network_name)
        if _docker_network_uses_internal_identity(requested_mode):
            local_node_name = _docker_node_name(_resolve_node_alias(env))
    _ensure_local_cluster_runtime_for_join(
        local_host=local_host,
        node_name=local_node_name,
        docker_network_mode=requested_mode,
        docker_network_name=network_name,
    )
    handshake = Client(target=target, auth_token="", timeout=10).network_handshake(
        token,
        node_name=local_node_name,
        node_info=_handshake_node_info(local_host, node_name=local_node_name),
    )
    remote_node = handshake.get("node_name") or _network_node_name(seed_host)
    redis_host, redis_port, redis_url = _validate_remote_redis_details(handshake, seed_host, token)

    console.print(f"=> Adding MirrorNeuron network node {remote_node} from {target}...")
    if _docker_network_uses_internal_identity(requested_mode):
        console.print("=> Received Docker-internal cluster wiring from the remote node.")
    else:
        console.print(f"=> Remote Redis advertised at {redis_host}:{redis_port}.")
    try:
        status = local_client.add_node(remote_node, token=token)
    except TypeError:
        status = local_client.add_node(remote_node)
    except Exception as exc:
        console.print(f"[red]Error: Could not add {remote_node} to the local cluster.[/red]")
        console.print("Check that the local MirrorNeuron core is running, and that the remote host and token are correct.")
        console.print(f"[dim]{exc}[/dim]")
        raise typer.Exit(1) from exc
    details: list[tuple[str, str]] = [("Node", remote_node)]
    if runtime_compose_available() or os.getenv("MN_REDIS_URL", "").strip():
        replication = _configure_worker_redis_replica(seed_host, handshake, token)
        if replication:
            details.append(("Replication", replication))
    if not _docker_network_uses_internal_identity(requested_mode):
        details.insert(1, ("Remote Redis", f"{redis_host}:{redis_port}"))
        details.append(("Remote Redis URL", redis_url))
    print_success_confirmation(
        console,
        action,
        status=status,
        details=details,
        next_steps=("mn node list", "mn resource list"),
    )
    return handshake

def _ensure_local_cluster_runtime_for_join(
    *,
    local_host: str,
    node_name: str,
    docker_network_mode: str,
    docker_network_name: str,
) -> None:
    if not runtime_compose_available():
        return

    env = _runtime_base_env(True)
    existing_node_name = str(env.get("MN_NODE_NAME") or "").strip()
    existing_advertise_host = str(env.get("MN_NETWORK_ADVERTISE_HOST") or "").strip()
    if (
        not _node_name_unset(existing_node_name)
        and existing_node_name == node_name
        and existing_advertise_host == local_host
    ):
        return

    primary_token = _resolve_network_token()
    console.print(f"=> Enabling cluster mode for this primary node as {node_name}...")
    env = _ensure_compose_native_port_settings(env)
    env = _ensure_compose_cluster_bind_settings(env, local_host)
    if _docker_network_uses_internal_identity(docker_network_mode):
        env = _ensure_compose_internal_redis_settings(env, token=primary_token)
    else:
        env = _ensure_compose_cluster_port_settings(env, token=primary_token, advertised_host=local_host)

    env["MN_NETWORK_JOIN_TOKEN"] = primary_token
    env["MN_NETWORK_ADVERTISE_HOST"] = local_host
    env["MN_NODE_NAME"] = node_name
    env["MN_CLUSTER_NODES"] = node_name
    env["MN_NODE_ROLE"] = env.get("MN_NODE_ROLE") or "runtime"
    env["MN_DIST_PORT"] = str(env.get("MN_DIST_PORT") or DEFAULT_DIST_PORT)
    env["MN_COOKIE"] = _derive_network_secret(primary_token, "cookie")
    env.setdefault("ERL_AFLAGS", _erl_aflags(env["MN_DIST_PORT"]))
    env = _ensure_node_advertisement_settings(env)

    _write_env_file_values(
        RUNTIME_COMPOSE_ENV,
        {
            "MN_NETWORK_ADVERTISE_HOST": env["MN_NETWORK_ADVERTISE_HOST"],
            "MN_NETWORK_JOIN_TOKEN": env["MN_NETWORK_JOIN_TOKEN"],
            "MN_NODE_NAME": env["MN_NODE_NAME"],
            "MN_CLUSTER_NODES": env["MN_CLUSTER_NODES"],
            "MN_NODE_ROLE": env["MN_NODE_ROLE"],
            "MN_DIST_PORT": env["MN_DIST_PORT"],
            "MN_COOKIE": env["MN_COOKIE"],
            "ERL_AFLAGS": env["ERL_AFLAGS"],
            "MN_DOCKER_NETWORK_MODE": docker_network_mode,
            "MN_DOCKER_NETWORK_NAME": docker_network_name,
        },
    )

    compose_env = _compose_runtime_env(env, None)
    try:
        subprocess.run(
            runtime_compose_cmd("up", "-d", "--force-recreate", "redis", "mirror-neuron-core"),
            check=True,
            stdout=subprocess.DEVNULL,
            env=compose_env,
        )
        _wait_for_local_cluster_grpc()
    except (FileNotFoundError, subprocess.CalledProcessError) as exc:
        console.print("[red]Error: Could not enable cluster mode for the local runtime.[/red]")
        console.print("Run 'mn runtime stop' and then 'mn runtime start --host <this-box-ip>' before joining the worker.")
        console.print(f"[dim]{exc}[/dim]")
        raise typer.Exit(1) from exc

def _wait_for_local_cluster_grpc(timeout_seconds: float = 10.0) -> None:
    from mn_cli.shared import client as local_client

    deadline = time.time() + timeout_seconds
    last_error: Optional[Exception] = None
    while time.time() < deadline:
        try:
            local_client.get_system_summary()
            return
        except Exception as exc:
            last_error = exc
            time.sleep(0.25)

    console.print("[red]Error: Local MirrorNeuron core did not become ready after enabling cluster mode.[/red]")
    if last_error is not None:
        console.print(f"[dim]{last_error}[/dim]")
    raise typer.Exit(1)

def _stop_network_runtime() -> None:
    for container in [NETWORK_CORE_CONTAINER, NETWORK_REDIS_CONTAINER]:
        subprocess.run(["docker", "rm", "-f", container], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)

def _detach_local_docker_node_if_matches(node_name: str) -> bool:
    alias = _configured_node_alias(_runtime_base_env(runtime_compose_available()))
    if not alias or node_name != _docker_node_name(alias):
        return False

    if runtime_compose_available():
        subprocess.run(
            runtime_compose_cmd("stop", "mirror-neuron-core"),
            stderr=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
        )
        return True

    stopped = False
    for container in (LOCAL_CORE_CONTAINER, NETWORK_CORE_CONTAINER):
        subprocess.run(["docker", "rm", "-f", container], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
        stopped = True
    return stopped

def check_status(pid_file: Path) -> int:
    if pid_file.exists():
        try:
            pid = int(pid_file.read_text().strip())
            os.kill(pid, 0)
            return 0 # Running
        except (ValueError, OSError):
            return 1 # Stale
    return 2 # Not running

def runtime_compose_available() -> bool:
    return RUNTIME_COMPOSE_FILE.exists() and RUNTIME_COMPOSE_ENV.exists()

def _read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return values

    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        values[key] = value
    return values

def _write_env_file_values(path: Path, updates: dict[str, str]) -> None:
    try:
        original_lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        original_lines = []

    lines: list[str] = []
    seen: set[str] = set()
    for line in original_lines:
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            key, _ = stripped.split("=", 1)
            if key in updates:
                lines.append(f"{key}={updates[key]}")
                seen.add(key)
                continue
        lines.append(line)

    for key, value in updates.items():
        if key not in seen:
            lines.append(f"{key}={value}")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    try:
        path.chmod(0o600)
    except OSError:
        logger.debug("Failed to chmod env file %s", path, exc_info=True)

def _runtime_base_env(compose_runtime: bool) -> dict[str, str]:
    env = _read_env_file(RUNTIME_COMPOSE_ENV) if compose_runtime else {}
    env.update(os.environ)
    return env

def _env_or_default(env: dict[str, str], key: str, default: str, legacy_default: Optional[str] = None) -> str:
    value = str(env.get(key) or "").strip()
    if os.getenv(key, "").strip():
        return os.getenv(key, "").strip()
    if not value or (legacy_default is not None and value == legacy_default):
        return default
    return value

def _valid_port_text(value: str, default: str) -> str:
    parsed = _parse_configured_port(value)
    if parsed is None:
        return default
    return str(parsed)

def _native_endpoint_host(host: str) -> str:
    normalized = (host or "").strip()
    if normalized in {"", "0.0.0.0", "::"}:
        return "127.0.0.1"
    return normalized

def _cluster_endpoint_host(env: dict[str, str], host: str) -> str:
    normalized = (host or "").strip()
    advertised = str(env.get("MN_NETWORK_ADVERTISE_HOST") or "").strip()
    if advertised and _network_publish_host(advertised) == "0.0.0.0":
        if normalized in {"", "0.0.0.0", "::", "127.0.0.1", "localhost", "::1"}:
            return advertised
    return _native_endpoint_host(normalized)

def _runtime_blueprint_env_updates(env: dict[str, str]) -> dict[str, str]:
    default_repo = str(env.get("MN_DEFAULT_BLUEPRINT_REPO") or os.getenv("MN_DEFAULT_BLUEPRINT_REPO") or DEFAULT_BLUEPRINT_REPO).strip()
    host_home_dir = str(
        env.get("MN_HOST_HOME_DIR")
        or env.get("MN_HOST_MN_DIR")
        or os.getenv("MN_HOST_HOME_DIR")
        or os.getenv("MN_HOST_MN_DIR")
        or DIR
    ).strip()
    configured_runs_root = str(env.get("MN_RUNS_ROOT") or os.getenv("MN_RUNS_ROOT") or "").strip()
    host_artifacts_dir = str(
        env.get("MN_HOST_ARTIFACTS_DIR") or os.getenv("MN_HOST_ARTIFACTS_DIR") or configured_runs_root
    ).strip()
    if not host_artifacts_dir:
        host_artifacts_dir = str(Path(host_home_dir).expanduser() / "runs")
    container_runs_root = str(
        env.get("MN_CONTAINER_RUNS_ROOT") or os.getenv("MN_CONTAINER_RUNS_ROOT") or DEFAULT_CONTAINER_RUNS_ROOT
    ).strip()
    updates: dict[str, str] = {
        "MN_DEFAULT_BLUEPRINT_REPO": default_repo,
        "MN_BLUEPRINT_REPO": str(env.get("MN_BLUEPRINT_REPO") or "").strip() or default_repo,
        "MN_HOST_ARTIFACTS_DIR": host_artifacts_dir,
        "MN_RUNS_ROOT": configured_runs_root or host_artifacts_dir,
        "MN_CONTAINER_RUNS_ROOT": container_runs_root,
        "MN_BLUEPRINT_WEB_UI_BIND_HOST": str(
            env.get("MN_BLUEPRINT_WEB_UI_BIND_HOST") or DEFAULT_BLUEPRINT_WEB_UI_BIND_HOST
        ).strip(),
        "MN_BLUEPRINT_WEB_UI_PUBLIC_HOST": str(
            env.get("MN_BLUEPRINT_WEB_UI_PUBLIC_HOST") or DEFAULT_BLUEPRINT_WEB_UI_PUBLIC_HOST
        ).strip(),
        "MN_BLUEPRINT_WEB_UI_PORT_START": str(
            env.get("MN_BLUEPRINT_WEB_UI_PORT_START") or DEFAULT_BLUEPRINT_WEB_UI_PORT_START
        ).strip(),
        "MN_BLUEPRINT_WEB_UI_PORT_END": str(
            env.get("MN_BLUEPRINT_WEB_UI_PORT_END") or DEFAULT_BLUEPRINT_WEB_UI_PORT_END
        ).strip(),
        "MN_BLUEPRINT_WEB_UI_PORT_ALLOCATION_MODE": str(
            env.get("MN_BLUEPRINT_WEB_UI_PORT_ALLOCATION_MODE") or DEFAULT_BLUEPRINT_WEB_UI_PORT_ALLOCATION_MODE
        ).strip(),
    }
    for key in ("MN_DEV_LOCAL_BLUEPRINT_REPO", "DEV_LOCAL_BLUEPRINT_REPO"):
        value = str(env.get(key) or "").strip()
        if value:
            updates[key] = value
    return updates

def _ensure_host_artifacts_dir(env: dict[str, str]) -> None:
    path_text = str(env.get("MN_HOST_ARTIFACTS_DIR") or env.get("MN_RUNS_ROOT") or "").strip()
    if not path_text:
        return
    try:
        Path(path_text).expanduser().mkdir(parents=True, exist_ok=True)
    except OSError:
        logger.warning("Failed to create shared artifact directory %s", path_text, exc_info=True)

def _runtime_endpoint_snapshot(env: dict[str, str], web_ui_available: bool = False) -> dict[str, object]:
    api_host = _native_endpoint_host(str(env.get("MN_API_HOST") or DEFAULT_HOST))
    api_port = _valid_port_text(str(env.get("MN_API_PORT") or DEFAULT_API_PORT), DEFAULT_API_PORT)
    api_base_url = str(env.get("MN_API_BASE_URL") or f"http://{api_host}:{api_port}/api/v1")
    grpc_host = _cluster_endpoint_host(
        env,
        str(env.get("MN_GRPC_BIND_HOST") or env.get("MN_CORE_HOST") or DEFAULT_HOST)
    )
    grpc_port = _valid_port_text(str(env.get("MN_GRPC_PORT") or DEFAULT_GRPC_PORT), DEFAULT_GRPC_PORT)
    grpc_target = str(env.get("MN_GRPC_TARGET") or "").strip()
    if not grpc_target:
        core_grpc_target = str(env.get("MN_CORE_GRPC_TARGET") or "").strip()
        target_host, target_port = _host_port_from_target(core_grpc_target, grpc_host, grpc_port)
        if core_grpc_target and _cluster_endpoint_host(env, target_host) == target_host:
            grpc_target = core_grpc_target
        else:
            grpc_target = f"{grpc_host}:{target_port}"
    snapshot: dict[str, object] = {
        "version": 1,
        "updated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "api": {
            "base_url": api_base_url,
            "host": api_host,
            "port": api_port,
        },
        "grpc": {
            "target": grpc_target,
            "host": grpc_host,
            "port": grpc_port,
        },
    }
    if web_ui_available:
        web_ui_host = _native_endpoint_host(str(env.get("MN_WEB_UI_HOST") or DEFAULT_HOST))
        web_ui_port = _valid_port_text(str(env.get("MN_WEB_UI_PORT") or DEFAULT_WEB_UI_PORT), DEFAULT_WEB_UI_PORT)
        snapshot["web_ui"] = {
            "url": f"http://{web_ui_host}:{web_ui_port}",
            "host": web_ui_host,
            "port": web_ui_port,
        }
    return snapshot

def _write_runtime_endpoints_file(env: dict[str, str], web_ui_available: bool = False) -> dict[str, object]:
    snapshot = _runtime_endpoint_snapshot(env, web_ui_available=web_ui_available)
    RUNTIME_ENDPOINTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    RUNTIME_ENDPOINTS_FILE.write_text(json.dumps(snapshot, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    try:
        RUNTIME_ENDPOINTS_FILE.chmod(0o600)
    except OSError:
        logger.debug("Failed to chmod runtime endpoint file %s", RUNTIME_ENDPOINTS_FILE, exc_info=True)
    return snapshot

def _ensure_compose_native_port_settings(env: dict[str, str]) -> dict[str, str]:
    adjusted = dict(env)
    grpc_port = _valid_port_text(
        _env_or_default(adjusted, "MN_GRPC_PORT", DEFAULT_GRPC_PORT, LEGACY_GRPC_PORT),
        DEFAULT_GRPC_PORT,
    )
    api_port = _valid_port_text(
        _env_or_default(adjusted, "MN_API_PORT", DEFAULT_API_PORT, LEGACY_API_PORT),
        DEFAULT_API_PORT,
    )
    dist_port = _valid_port_text(
        _env_or_default(adjusted, "MN_DIST_PORT", DEFAULT_DIST_PORT, LEGACY_DIST_PORT),
        DEFAULT_DIST_PORT,
    )
    web_ui_port = _valid_port_text(
        _env_or_default(adjusted, "MN_WEB_UI_PORT", DEFAULT_WEB_UI_PORT, LEGACY_WEB_UI_PORT),
        DEFAULT_WEB_UI_PORT,
    )
    openshell_port = _valid_port_text(
        _env_or_default(
            adjusted,
            "OPENSHELL_GATEWAY_PORT",
            DEFAULT_OPENSHELL_GATEWAY_PORT,
            LEGACY_OPENSHELL_GATEWAY_PORT,
        ),
        DEFAULT_OPENSHELL_GATEWAY_PORT,
    )
    openshell_bind_host = adjusted.get("OPENSHELL_GATEWAY_BIND_HOST") or "127.0.0.1"
    openshell_endpoint = _env_or_default(
        adjusted,
        "OPENSHELL_GATEWAY_ENDPOINT",
        f"http://{_native_endpoint_host(openshell_bind_host)}:{openshell_port}",
        f"http://127.0.0.1:{LEGACY_OPENSHELL_GATEWAY_PORT}",
    )
    if not os.getenv("OPENSHELL_GATEWAY_ENDPOINT", "").strip() and openshell_endpoint == (
        f"https://127.0.0.1:{LEGACY_OPENSHELL_GATEWAY_PORT}"
    ):
        openshell_endpoint = f"http://{_native_endpoint_host(openshell_bind_host)}:{openshell_port}"
    core_grpc_target = adjusted.get("MN_CORE_GRPC_TARGET") or f"localhost:{grpc_port}"
    if not os.getenv("MN_CORE_GRPC_TARGET", "").strip() and core_grpc_target == f"localhost:{LEGACY_GRPC_PORT}":
        core_grpc_target = f"localhost:{grpc_port}"

    updates = {
        "MN_GRPC_BIND_HOST": adjusted.get("MN_GRPC_BIND_HOST") or "127.0.0.1",
        "MN_GRPC_PORT": grpc_port,
        "MN_CORE_GRPC_TARGET": core_grpc_target,
        "MN_API_HOST": adjusted.get("MN_API_HOST") or DEFAULT_HOST,
        "MN_API_PORT": api_port,
        "MN_DIST_PORT": dist_port,
        "MN_WEB_UI_HOST": adjusted.get("MN_WEB_UI_HOST") or DEFAULT_HOST,
        "MN_WEB_UI_PORT": web_ui_port,
        "OPENSHELL_GATEWAY_BIND_HOST": openshell_bind_host,
        "OPENSHELL_GATEWAY_PORT": openshell_port,
        "OPENSHELL_GATEWAY_ENDPOINT": openshell_endpoint,
    }
    updates.update(_runtime_blueprint_env_updates(adjusted))
    if _erl_aflags_needs_update(adjusted.get("ERL_AFLAGS"), dist_port):
        updates["ERL_AFLAGS"] = _erl_aflags(dist_port)

    adjusted.update(updates)
    _write_env_file_values(RUNTIME_COMPOSE_ENV, updates)
    return adjusted

def _ensure_compose_cluster_bind_settings(env: dict[str, str], advertised_host: str) -> dict[str, str]:
    adjusted = dict(env)
    publish_host = _network_publish_host(advertised_host)
    updates: dict[str, str] = {}
    loopback_hosts = {"", "127.0.0.1", "localhost", "::1"}

    current = str(adjusted.get("MN_GRPC_BIND_HOST") or "").strip()
    if not os.getenv("MN_GRPC_BIND_HOST", "").strip():
        if publish_host == "0.0.0.0" and current in loopback_hosts:
            updates["MN_GRPC_BIND_HOST"] = publish_host
        elif not current:
            updates["MN_GRPC_BIND_HOST"] = publish_host

    if updates:
        adjusted.update(updates)
        _write_env_file_values(RUNTIME_COMPOSE_ENV, updates)
    return adjusted

def _runtime_compose_cluster_override_file() -> Path:
    return DIR / RUNTIME_CLUSTER_OVERRIDE_FILE

def _write_runtime_compose_cluster_override() -> None:
    override = """services:
  redis:
    ports:
      - "${MN_REDIS_BIND_HOST:-127.0.0.1}:${MN_REDIS_PORT:-56379}:6379"
  mirror-neuron-core:
    ports:
      - "${MN_EPMD_BIND_HOST:-127.0.0.1}:${MN_EPMD_PORT:-54369}:${MN_EPMD_PORT:-54369}"
      - "${MN_DIST_BIND_HOST:-127.0.0.1}:${MN_DIST_PORT:-54370}:${MN_DIST_PORT:-54370}"
"""
    path = _runtime_compose_cluster_override_file()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(override, encoding="utf-8")

def _ensure_compose_cluster_port_settings(
    env: dict[str, str],
    *,
    token: str,
    advertised_host: str,
    redis_port: Optional[int] = None,
) -> dict[str, str]:
    adjusted = dict(env)
    publish_host = _network_publish_host(advertised_host)
    env_redis_port = os.getenv("MN_REDIS_PORT", "").strip()
    selected_redis_port = _resolve_published_redis_port(
        bind_host=publish_host,
        configured_port=redis_port if redis_port is not None else (env_redis_port or adjusted.get("MN_REDIS_PORT")),
        explicit=redis_port is not None or bool(env_redis_port),
        owner_container=COMPOSE_REDIS_CONTAINER,
    )
    redis_password = _derive_network_secret(token, "redis")
    updates = {
        "MN_REDIS_PASSWORD": redis_password,
        "MN_REDIS_BIND_HOST": publish_host,
        "MN_REDIS_PORT": str(selected_redis_port),
        "MN_NETWORK_REDIS_HOST": advertised_host,
        "MN_NETWORK_REDIS_PORT": str(selected_redis_port),
        "MN_EPMD_BIND_HOST": publish_host,
        "MN_DIST_BIND_HOST": publish_host,
    }
    adjusted.update(updates)
    _write_env_file_values(RUNTIME_COMPOSE_ENV, updates)
    _write_runtime_compose_cluster_override()
    return adjusted

def _ensure_compose_internal_redis_settings(
    env: dict[str, str],
    *,
    token: str,
    network_redis_host: Optional[str] = None,
    network_redis_port: Optional[int] = None,
) -> dict[str, str]:
    adjusted = dict(env)
    redis_password = _derive_network_secret(token, "redis")
    redis_host = os.getenv("MN_NETWORK_REDIS_HOST", "").strip() or network_redis_host or "redis"
    redis_port = network_redis_port or REDIS_CONTAINER_PORT

    adjusted["MN_REDIS_PASSWORD"] = redis_password
    adjusted["MN_REDIS_URL"] = f"redis://:{redis_password}@redis:{REDIS_CONTAINER_PORT}/0"
    adjusted["MN_CONTEXT_REDIS_URL"] = f"redis://:{redis_password}@redis:{REDIS_CONTAINER_PORT}/1"
    adjusted.setdefault("MN_NETWORK_JOIN_TOKEN", token)
    adjusted["MN_NETWORK_REDIS_HOST"] = redis_host
    adjusted["MN_NETWORK_REDIS_PORT"] = str(redis_port)

    _write_env_file_values(
        RUNTIME_COMPOSE_ENV,
        {
            "MN_NETWORK_JOIN_TOKEN": token,
            "MN_REDIS_PASSWORD": redis_password,
            "MN_REDIS_URL": adjusted["MN_REDIS_URL"],
            "MN_CONTEXT_REDIS_URL": adjusted["MN_CONTEXT_REDIS_URL"],
            "MN_NETWORK_REDIS_HOST": adjusted["MN_NETWORK_REDIS_HOST"],
            "MN_NETWORK_REDIS_PORT": adjusted["MN_NETWORK_REDIS_PORT"],
        },
    )
    return adjusted

def _resolve_network_seed_redis_port(host: str, requested_port: Optional[int]) -> int:
    persisted = _read_env_file(NETWORK_REDIS_ENV_FILE).get("MN_REDIS_PORT")
    env_port = os.getenv("MN_REDIS_PORT", "").strip()
    explicit = requested_port is not None or bool(env_port)
    configured_port: object = requested_port if requested_port is not None else (env_port or persisted)
    publish_host = _network_publish_host(host)
    selected_port = _resolve_published_redis_port(
        bind_host=publish_host,
        configured_port=configured_port,
        explicit=explicit,
        owner_container=NETWORK_REDIS_CONTAINER,
    )
    _write_env_file_values(NETWORK_REDIS_ENV_FILE, {"MN_REDIS_PORT": str(selected_port)})
    return selected_port

def _validate_remote_redis_details(handshake: dict, seed_host: str, token: str) -> tuple[str, int, str]:
    redis_host = str(handshake.get("redis_host") or "").strip()
    redis_port = _parse_configured_port(handshake.get("redis_port"))
    redis_url = str(handshake.get("redis_url") or "").strip()

    if not redis_host or redis_port is None or not redis_url:
        console.print(f"[red]Error: Remote node at {seed_host} did not advertise complete Redis details.[/red]")
        raise typer.Exit(1)

    parsed = urlparse(redis_url)
    if parsed.scheme not in {"redis", "rediss"} or not parsed.hostname or not parsed.port:
        console.print(f"[red]Error: Remote node at {seed_host} returned an invalid Redis URL.[/red]")
        raise typer.Exit(1)
    if parsed.password != _derive_network_secret(token, "redis"):
        console.print(f"[red]Error: Remote node at {seed_host} did not advertise token-authenticated Redis.[/red]")
        raise typer.Exit(1)

    return redis_host, redis_port, redis_url

def _redis_password_from_url(redis_url: str) -> str:
    try:
        return urlparse(redis_url).password or ""
    except Exception:
        return ""

def _usable_remote_host(host: str, fallback: str) -> str:
    normalized = str(host or "").strip()
    if normalized in {"", "0.0.0.0", "::", "redis", "localhost", "127.0.0.1"}:
        return fallback
    return normalized

def _primary_redis_details(fallback_token: str) -> Optional[tuple[str, int, str]]:
    env = _runtime_base_env(runtime_compose_available())
    redis_url = str(env.get("MN_REDIS_URL") or "").strip()
    parsed = urlparse(redis_url) if redis_url else None
    host = (
        str(env.get("MN_NETWORK_REDIS_HOST") or "").strip()
        or (parsed.hostname if parsed else "")
        or _advertised_network_host(None)
    )
    port = (
        _parse_configured_port(env.get("MN_NETWORK_REDIS_PORT"))
        or _parse_configured_port(env.get("MN_REDIS_PORT"))
        or _parse_configured_port(parsed.port if parsed else None)
        or REDIS_CONTAINER_PORT
    )
    password = (
        str(env.get("MN_REDIS_PASSWORD") or "").strip()
        or (_redis_password_from_url(redis_url) if redis_url else "")
    )
    if not password:
        primary_token = _running_network_token((LOCAL_CORE_CONTAINER, NETWORK_CORE_CONTAINER)) or fallback_token
        password = _derive_network_secret(primary_token, "redis")
    if not host or not port or not password:
        return None
    return host, port, password

def _redis_encode_command(args: tuple[str, ...]) -> bytes:
    chunks = [f"*{len(args)}\r\n".encode("utf-8")]
    for arg in args:
        payload = str(arg).encode("utf-8")
        chunks.append(f"${len(payload)}\r\n".encode("utf-8"))
        chunks.append(payload + b"\r\n")
    return b"".join(chunks)

def _redis_read_line(stream) -> str:
    line = stream.readline()
    if not line:
        raise OSError("Redis closed the connection")
    return line.rstrip(b"\r\n").decode("utf-8", errors="replace")

def _redis_read_response(stream):
    prefix = stream.read(1)
    if not prefix:
        raise OSError("Redis closed the connection")
    kind = prefix.decode("ascii", errors="replace")
    if kind == "+":
        return _redis_read_line(stream)
    if kind == "-":
        raise RuntimeError(_redis_read_line(stream))
    if kind == ":":
        return int(_redis_read_line(stream))
    if kind == "$":
        length = int(_redis_read_line(stream))
        if length < 0:
            return None
        data = stream.read(length)
        stream.read(2)
        return data.decode("utf-8", errors="replace")
    if kind == "*":
        length = int(_redis_read_line(stream))
        if length < 0:
            return None
        return [_redis_read_response(stream) for _ in range(length)]
    raise RuntimeError(f"unexpected Redis response prefix {kind!r}")

def _redis_command(host: str, port: int, password: str, *args: str):
    with socket.create_connection((host, port), timeout=5) as conn:
        stream = conn.makefile("rb")
        if password:
            conn.sendall(_redis_encode_command(("AUTH", password)))
            _redis_read_response(stream)
        conn.sendall(_redis_encode_command(tuple(str(arg) for arg in args)))
        return _redis_read_response(stream)

def _configure_worker_redis_replica(
    worker_host: str,
    worker_handshake: dict,
    token: str,
) -> str | None:
    primary = _primary_redis_details(token)
    if primary is None:
        console.print("[yellow]Warning: Could not determine primary Redis details; worker Redis replication was skipped.[/yellow]")
        return None

    primary_host, primary_port, primary_password = primary
    worker_redis_host, worker_redis_port, worker_redis_url = _validate_remote_redis_details(
        worker_handshake,
        worker_host,
        token,
    )
    worker_redis_host = _usable_remote_host(worker_redis_host, worker_host)
    worker_password = _redis_password_from_url(worker_redis_url) or _derive_network_secret(token, "redis")

    try:
        _redis_command(
            worker_redis_host,
            worker_redis_port,
            worker_password,
            "CONFIG",
            "SET",
            "masterauth",
            primary_password,
        )
        _redis_command(
            worker_redis_host,
            worker_redis_port,
            worker_password,
            "REPLICAOF",
            primary_host,
            str(primary_port),
        )
        try:
            _redis_command(primary_host, primary_port, primary_password, "WAIT", "1", "1000")
        except Exception:
            logger.debug("Primary Redis WAIT after worker replica setup failed", exc_info=True)
    except Exception as exc:
        console.print("[red]Error: Could not configure worker Redis replication.[/red]")
        console.print(f"Primary Redis: {primary_host}:{primary_port}")
        console.print(f"Worker Redis:  {worker_redis_host}:{worker_redis_port}")
        console.print(f"[dim]{exc}[/dim]")
        raise typer.Exit(1) from exc

    console.print(
        f"=> Worker Redis {worker_redis_host}:{worker_redis_port} is replicating from "
        f"{primary_host}:{primary_port}."
    )
    return f"{worker_redis_host}:{worker_redis_port} -> {primary_host}:{primary_port}"

def runtime_compose_cmd(*args: str) -> list[str]:
    cmd = [
        "docker",
        "compose",
        "--env-file",
        str(RUNTIME_COMPOSE_ENV),
        "-f",
        str(RUNTIME_COMPOSE_FILE),
    ]
    cluster_override = _runtime_compose_cluster_override_file()
    if cluster_override.exists():
        cmd.extend(["-f", str(cluster_override)])
    cmd.extend(args)
    return cmd

def web_ui_pid_files() -> tuple[tuple[Path, str], ...]:
    paths = [
        (WEB_UI_WATCHDOG_PID_FILE, "Web UI watchdog"),
        (WEB_UI_PID_FILE, "Web UI"),
        (DEFAULT_DIR / "pids" / "web-ui-watchdog.pid", "Web UI watchdog"),
        (DEFAULT_DIR / "pids" / "web-ui.pid", "Web UI"),
    ]
    unique: list[tuple[Path, str]] = []
    seen: set[str] = set()
    for pid_file, name in paths:
        key = str(pid_file)
        if key in seen:
            continue
        seen.add(key)
        unique.append((pid_file, name))
    return tuple(unique)

def api_pid_files() -> tuple[tuple[Path, str], ...]:
    paths = [
        (API_WATCHDOG_PID_FILE, "REST API watchdog"),
        (API_PID_FILE, "REST API"),
        (DEFAULT_DIR / "pids" / "api-watchdog.pid", "REST API watchdog"),
        (DEFAULT_DIR / "pids" / "api.pid", "REST API"),
    ]
    unique: list[tuple[Path, str]] = []
    seen: set[str] = set()
    for pid_file, name in paths:
        key = str(pid_file)
        if key in seen:
            continue
        seen.add(key)
        unique.append((pid_file, name))
    return tuple(unique)

def kill_tree(parent_pid: int):
    try:
        os.kill(parent_pid, 0)
    except OSError:
        logger.debug("Process %s is not running", parent_pid)
        return
    
    try:
        children = subprocess.check_output(['pgrep', '-P', str(parent_pid)], stderr=subprocess.DEVNULL)
        for child_pid in children.decode().split():
            if child_pid.strip():
                kill_tree(int(child_pid.strip()))
    except subprocess.CalledProcessError:
        pass
    
    try:
        logger.info("Stopping process %s", parent_pid)
        os.kill(parent_pid, signal.SIGTERM)
    except OSError:
        logger.exception("Failed to stop process %s", parent_pid)
        pass

def find_web_ui_dir() -> Optional[Path]:
    for web_ui_dir in WEB_UI_DIRS:
        if _web_ui_dist_dir(web_ui_dir) is not None:
            return web_ui_dir
    return None

def _web_ui_dist_dir(web_ui_dir: Path) -> Optional[Path]:
    for candidate in (web_ui_dir / "dist", web_ui_dir):
        if (candidate / "index.html").exists():
            return candidate
    return None

def _web_ui_watchdog_script() -> str:
    return r"""
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

config = json.loads(sys.argv[1])
command = config["command"]
cwd = config["cwd"]
pid_file = Path(config["pid_file"])
log_file = Path(config["log_file"])
restart_delay = float(config.get("restart_delay", 2))
stopping = False
child = None

def log(message):
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    with log_file.open("a", encoding="utf-8") as handle:
        handle.write(f"[watchdog {timestamp}] {message}\n")

def request_stop(_signum, _frame):
    global stopping
    stopping = True
    if child is not None and child.poll() is None:
        try:
            child.terminate()
        except OSError:
            pass

signal.signal(signal.SIGTERM, request_stop)
signal.signal(signal.SIGINT, request_stop)

pid_file.parent.mkdir(parents=True, exist_ok=True)
log_file.parent.mkdir(parents=True, exist_ok=True)

try:
    while not stopping:
        with log_file.open("a", encoding="utf-8", buffering=1) as output:
            child = subprocess.Popen(
                command,
                cwd=cwd,
                stdout=output,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                env=os.environ.copy(),
                start_new_session=True,
            )
            pid_file.write_text(str(child.pid), encoding="utf-8")
            log(f"started web ui child pid={child.pid}")
            while not stopping:
                exit_code = child.poll()
                if exit_code is not None:
                    break
                time.sleep(1)

        if stopping:
            break

        log(f"web ui child exited code={exit_code}; restarting in {restart_delay:g}s")
        time.sleep(restart_delay)
finally:
    if child is not None and child.poll() is None:
        try:
            child.terminate()
            child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            try:
                child.kill()
            except OSError:
                pass
        except OSError:
            pass
    try:
        pid_file.unlink()
    except OSError:
        pass
    log("watchdog stopped")
"""

def _api_command() -> Optional[list[str]]:
    api_bin = VENV_DIR / "bin" / "mn-api"
    if api_bin.exists():
        return [str(api_bin)]
    return None

def _api_http_url(api_host: str, api_port: str, path: str = "/api/v1/health") -> str:
    display_host = _native_endpoint_host(api_host)
    if ":" in display_host and not display_host.startswith("["):
        display_host = f"[{display_host}]"
    normalized_path = path if path.startswith("/") else f"/{path}"
    return f"http://{display_host}:{_valid_port_text(str(api_port), DEFAULT_API_PORT)}{normalized_path}"

def _wait_for_api(api_host: str, api_port: str, *, timeout_seconds: float = 10.0) -> bool:
    url = _api_http_url(api_host, api_port, "/api/v1/health")
    deadline = time.monotonic() + max(timeout_seconds, 0.0)
    last_error: Optional[Exception] = None

    while True:
        try:
            with urllib.request.urlopen(url, timeout=0.75) as response:
                status = getattr(response, "status", None)
                if status is None:
                    status = response.getcode()
                if int(status) < 500:
                    try:
                        payload = json.loads(response.read(4096).decode("utf-8"))
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        payload = {}
                    if not isinstance(payload, dict) or str(payload.get("status") or "").lower() in {"", "ok"}:
                        return True
        except Exception as exc:
            last_error = exc

        if time.monotonic() >= deadline:
            if last_error is not None:
                logger.warning("REST API did not respond at %s: %s", url, last_error)
            return False
        time.sleep(0.25)

def _start_api_watchdog(env: dict[str, str]) -> subprocess.Popen:
    command = _api_command()
    if command is None:
        raise FileNotFoundError("mn-api")
    API_PID_FILE.parent.mkdir(parents=True, exist_ok=True)
    API_LOG.parent.mkdir(parents=True, exist_ok=True)
    API_WATCHDOG_LOG.parent.mkdir(parents=True, exist_ok=True)
    config = {
        "command": command,
        "cwd": str(Path.cwd()),
        "pid_file": str(API_PID_FILE),
        "log_file": str(API_LOG),
        "restart_delay": env.get("MN_API_RESTART_DELAY_SECONDS", DEFAULT_WEB_UI_RESTART_DELAY_SECONDS),
    }
    with open(API_WATCHDOG_LOG, "w") as out:
        return subprocess.Popen(
            [sys.executable, "-c", _web_ui_watchdog_script(), json.dumps(config)],
            stdout=out,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            env=env,
            start_new_session=True,
        )

def _start_api_if_installed(runtime_env: Optional[dict[str, str]] = None) -> bool:
    if _api_command() is None:
        console.print("[yellow]=> Warning: mn-api not found, skipping.[/yellow]")
        return False

    env = os.environ.copy()
    if runtime_env:
        env.update(runtime_env)
    api_host = env.get("MN_API_HOST") or _api_host()
    api_port = _valid_port_text(str(env.get("MN_API_PORT") or DEFAULT_API_PORT), DEFAULT_API_PORT)
    env["MN_API_HOST"] = api_host
    env["MN_API_PORT"] = api_port

    watchdog_status = check_status(API_WATCHDOG_PID_FILE)
    child_status = check_status(API_PID_FILE)
    if watchdog_status == 0:
        if _wait_for_api(api_host, api_port, timeout_seconds=5.0):
            console.print("[yellow]=> REST API watchdog is already running, skipping.[/yellow]")
            return True
        try:
            watchdog_pid = int(API_WATCHDOG_PID_FILE.read_text().strip())
            console.print("[yellow]=> REST API watchdog is running, but the API is not responding; restarting it.[/yellow]")
            kill_tree(watchdog_pid)
            time.sleep(1)
        except (ValueError, OSError):
            pass
        API_WATCHDOG_PID_FILE.unlink(missing_ok=True)
        API_PID_FILE.unlink(missing_ok=True)
    elif watchdog_status == 1:
        API_WATCHDOG_PID_FILE.unlink(missing_ok=True)

    if child_status == 0:
        try:
            pid = int(API_PID_FILE.read_text().strip())
            console.print(f"=> Restarting existing REST API (PID: {pid}) under watchdog...")
            kill_tree(pid)
            time.sleep(1)
        except (ValueError, OSError):
            pass
        API_PID_FILE.unlink(missing_ok=True)
    elif child_status == 1:
        API_PID_FILE.unlink(missing_ok=True)

    console.print(f"=> Starting mn-api watchdog (REST on port {api_port})...")
    try:
        p_api_watchdog = _start_api_watchdog(env)
    except FileNotFoundError:
        console.print("[yellow]=> Warning: mn-api not found, skipping.[/yellow]")
        return False
    API_WATCHDOG_PID_FILE.parent.mkdir(parents=True, exist_ok=True)
    API_WATCHDOG_PID_FILE.write_text(str(p_api_watchdog.pid))
    if _wait_for_api(api_host, api_port, timeout_seconds=10.0):
        console.print(f"   [green][Started][/green] REST API watchdog (PID: {p_api_watchdog.pid})")
    else:
        console.print(
            f"   [yellow][Started][/yellow] REST API watchdog (PID: {p_api_watchdog.pid}); "
            f"waiting for {_api_http_url(api_host, api_port)} to respond."
        )
    return True

def _web_ui_command(web_ui_host: str, web_ui_port: str) -> list[str]:
    web_ui_server = VENV_DIR / "bin" / "mn-web-ui-server"
    if web_ui_server.exists():
        return [str(web_ui_server)]
    return [sys.executable, "-m", "mn_api.web_ui_server"]

def _start_web_ui_watchdog(web_ui_dir: Path, env: dict[str, str], web_ui_host: str, web_ui_port: str) -> subprocess.Popen:
    config = {
        "command": _web_ui_command(web_ui_host, web_ui_port),
        "cwd": str(web_ui_dir),
        "pid_file": str(WEB_UI_PID_FILE),
        "log_file": str(WEB_UI_LOG),
        "restart_delay": env.get("MN_WEB_UI_RESTART_DELAY_SECONDS", DEFAULT_WEB_UI_RESTART_DELAY_SECONDS),
    }
    with open(WEB_UI_WATCHDOG_LOG, "w") as out:
        return subprocess.Popen(
            [sys.executable, "-c", _web_ui_watchdog_script(), json.dumps(config)],
            stdout=out,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            env=env,
            start_new_session=True,
        )

def _web_ui_http_url(web_ui_host: str, web_ui_port: str, path: str = "/") -> str:
    display_host = _native_endpoint_host(web_ui_host)
    if ":" in display_host and not display_host.startswith("["):
        display_host = f"[{display_host}]"
    normalized_path = path if path.startswith("/") else f"/{path}"
    return f"http://{display_host}:{_valid_port_text(str(web_ui_port), DEFAULT_WEB_UI_PORT)}{normalized_path}"

def _wait_for_web_ui(web_ui_host: str, web_ui_port: str, *, timeout_seconds: float = 10.0) -> bool:
    url = _web_ui_http_url(web_ui_host, web_ui_port, "/health")
    deadline = time.monotonic() + max(timeout_seconds, 0.0)
    last_error: Optional[Exception] = None

    while True:
        try:
            with urllib.request.urlopen(url, timeout=0.75) as response:
                status = getattr(response, "status", None)
                if status is None:
                    status = response.getcode()
                if int(status) < 500:
                    try:
                        payload = json.loads(response.read(4096).decode("utf-8"))
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        payload = {}
                    if (
                        isinstance(payload, dict)
                        and str(payload.get("status") or "").lower() == "ok"
                        and payload.get("component") == "web-ui"
                    ):
                        return True
        except Exception as exc:
            last_error = exc

        if time.monotonic() >= deadline:
            if last_error is not None:
                logger.warning("Web UI did not respond at %s: %s", url, last_error)
            return False
        time.sleep(0.25)

def _host_port_from_target(target: str, default_host: str, default_port: str) -> tuple[str, str]:
    if "://" in target:
        parsed = urlparse(target)
        return parsed.hostname or default_host, str(parsed.port or default_port)

    if target.startswith("[") and "]:" in target:
        host, port = target.rsplit("]:", 1)
        return host.lstrip("["), port

    if ":" in target:
        host, port = target.rsplit(":", 1)
        return host or default_host, port or default_port

    return target or default_host, default_port

def _redis_host_port(ip: Optional[str]) -> tuple[str, str]:
    default_host = ip or _redis_host()
    default_url = f"redis://{default_host}:6379/0"
    return _host_port_from_target(
        os.getenv("MN_REDIS_URL", default_url),
        default_host,
        "6379",
    )

def _native_service_endpoints(ip: Optional[str] = None, web_ui_available: bool = False) -> list[dict[str, str]]:
    runtime_env = _runtime_base_env(runtime_compose_available())
    rows: list[dict[str, str]] = []

    if runtime_compose_available():
        grpc_host = _cluster_endpoint_host(runtime_env, runtime_env.get("MN_GRPC_BIND_HOST", "127.0.0.1"))
        grpc_port = runtime_env.get("MN_GRPC_PORT", DEFAULT_GRPC_PORT)
        api_host = _native_endpoint_host(runtime_env.get("MN_API_HOST") or _api_host())
        api_port = runtime_env.get("MN_API_PORT", DEFAULT_API_PORT)
        include_internal_cluster_ports = False
    else:
        core_host = runtime_env.get("MN_CORE_HOST") or _core_host()
        grpc_host, grpc_port = _host_port_from_target(
            runtime_env.get(
                "MN_GRPC_TARGET",
                runtime_env.get("MN_CORE_GRPC_TARGET", f"{core_host}:{DEFAULT_GRPC_PORT}"),
            ),
            core_host,
            runtime_env.get("MN_GRPC_PORT", DEFAULT_GRPC_PORT),
        )
        grpc_host = _cluster_endpoint_host(runtime_env, grpc_host)
        api_host = runtime_env.get("MN_API_HOST") or _api_host()
        api_port = runtime_env.get("MN_API_PORT", DEFAULT_API_PORT)
        docker_mode = _docker_network_mode(runtime_env.get("MN_DOCKER_NETWORK_MODE"), default="disabled")
        include_internal_cluster_ports = not _docker_network_uses_internal_identity(docker_mode)
        redis_host, redis_port = _redis_host_port(ip)
        redis_host = _cluster_endpoint_host(runtime_env, redis_host)
        epmd_host = _cluster_endpoint_host(runtime_env, runtime_env.get("MN_EPMD_HOST") or _epmd_host())
        epmd_port = runtime_env.get("MN_EPMD_PORT", DEFAULT_EPMD_PORT)
        dist_host = _cluster_endpoint_host(runtime_env, runtime_env.get("MN_DIST_HOST") or _dist_host())
        dist_port = runtime_env.get("MN_DIST_PORT", DEFAULT_DIST_PORT)

    rows.extend(
        [
            {"service": "Core gRPC", "host": grpc_host, "port": str(grpc_port), "target": f"{grpc_host}:{grpc_port}"},
            {
                "service": "REST API",
                "host": api_host,
                "port": str(api_port),
                "target": f"http://{api_host}:{api_port}/api/v1",
            },
        ]
    )
    if include_internal_cluster_ports:
        rows.extend(
            [
                {
                    "service": "Redis",
                    "host": redis_host,
                    "port": str(redis_port),
                    "target": f"redis://{redis_host}:{redis_port}/0",
                },
                {
                    "service": "Erlang EPMD",
                    "host": epmd_host,
                    "port": str(epmd_port),
                    "target": f"{epmd_host}:{epmd_port}",
                },
                {
                    "service": "Erlang dist",
                    "host": dist_host,
                    "port": str(dist_port),
                    "target": f"{dist_host}:{dist_port}",
                },
            ]
        )
    if web_ui_available:
        web_ui_host = runtime_env.get("MN_WEB_UI_HOST") or _web_ui_host()
        web_ui_port = runtime_env.get("MN_WEB_UI_PORT", DEFAULT_WEB_UI_PORT)
        rows.append(
            {
                "service": "Web UI",
                "host": web_ui_host,
                "port": str(web_ui_port),
                "target": f"http://{web_ui_host}:{web_ui_port}",
            }
        )
    return rows

def native_service_ports() -> list[dict[str, str]]:
    return _native_service_endpoints(web_ui_available=True)

def _print_service_endpoints(ip: Optional[str], web_ui_available: bool):
    rows = _native_service_endpoints(ip=ip, web_ui_available=web_ui_available)

    table = Table(title="Service endpoints", show_header=True, header_style="bold")
    table.add_column("Service")
    table.add_column("Host")
    table.add_column("Port")
    table.add_column("URL / target")

    for row in rows:
        table.add_row(row["service"], row["host"], row["port"], row["target"])

    console.print(table)
    runtime_env = _runtime_base_env(runtime_compose_available())
    docker_mode = _docker_network_mode(runtime_env.get("MN_DOCKER_NETWORK_MODE"), default="disabled")
    if _docker_network_uses_internal_identity(docker_mode):
        console.print("[dim]Redis and Erlang cluster traffic use Docker internal networking.[/dim]")

def _start_web_ui_if_installed(runtime_env: Optional[dict[str, str]] = None) -> bool:
    web_ui_dir = find_web_ui_dir()
    if not web_ui_dir:
        return False
    web_ui_dist_dir = _web_ui_dist_dir(web_ui_dir)
    if web_ui_dist_dir is None:
        return False

    env = os.environ.copy()
    if runtime_env:
        env.update(runtime_env)
    web_ui_host = env.get("MN_WEB_UI_HOST") or _web_ui_host()
    env["MN_WEB_UI_HOST"] = web_ui_host
    env.setdefault("MN_API_HOST", _api_host())
    env.setdefault("MN_API_PORT", os.getenv("MN_API_PORT", DEFAULT_API_PORT))
    web_ui_port = _valid_port_text(
        str(env.get("MN_WEB_UI_PORT") or os.getenv("MN_WEB_UI_PORT", DEFAULT_WEB_UI_PORT)),
        DEFAULT_WEB_UI_PORT,
    )
    env["MN_WEB_UI_PORT"] = web_ui_port
    env["MN_WEB_UI_DIST_DIR"] = str(web_ui_dist_dir)

    watchdog_status = check_status(WEB_UI_WATCHDOG_PID_FILE)
    child_status = check_status(WEB_UI_PID_FILE)
    if (
        watchdog_status == 2
        and child_status == 2
        and _wait_for_web_ui(web_ui_host, web_ui_port, timeout_seconds=1.0)
    ):
        console.print("[yellow]=> Web UI is already responding, advertising existing instance.[/yellow]")
        return True
    if watchdog_status == 0:
        if _wait_for_web_ui(web_ui_host, web_ui_port, timeout_seconds=5.0):
            console.print("[yellow]=> Web UI watchdog is already running, skipping.[/yellow]")
            return True
        try:
            watchdog_pid = int(WEB_UI_WATCHDOG_PID_FILE.read_text().strip())
            console.print("[yellow]=> Web UI watchdog is running, but the page is not responding; restarting it.[/yellow]")
            kill_tree(watchdog_pid)
            time.sleep(1)
        except (ValueError, OSError):
            pass
        WEB_UI_WATCHDOG_PID_FILE.unlink(missing_ok=True)
        WEB_UI_PID_FILE.unlink(missing_ok=True)
    if watchdog_status == 1:
        WEB_UI_WATCHDOG_PID_FILE.unlink(missing_ok=True)

    if child_status == 0:
        try:
            pid = int(WEB_UI_PID_FILE.read_text().strip())
            console.print(f"=> Restarting existing Web UI (PID: {pid}) under watchdog...")
            kill_tree(pid)
            time.sleep(1)
        except (ValueError, OSError):
            pass
        WEB_UI_PID_FILE.unlink(missing_ok=True)
    elif child_status == 1:
        WEB_UI_PID_FILE.unlink(missing_ok=True)

    console.print(f"=> Starting mn-web-ui watchdog (static server on {web_ui_host}:{web_ui_port})...")
    try:
        p_web_watchdog = _start_web_ui_watchdog(web_ui_dir, env, web_ui_host, web_ui_port)
    except FileNotFoundError:
        console.print("[yellow]=> Warning: Python runtime not found, skipping Web UI.[/yellow]")
        return False
    WEB_UI_WATCHDOG_PID_FILE.write_text(str(p_web_watchdog.pid))
    if _wait_for_web_ui(web_ui_host, web_ui_port, timeout_seconds=10.0):
        console.print(f"   [green][Started][/green] Web UI watchdog (PID: {p_web_watchdog.pid})")
    else:
        console.print(
            f"   [yellow][Started][/yellow] Web UI watchdog (PID: {p_web_watchdog.pid}); "
            f"waiting for {_web_ui_http_url(web_ui_host, web_ui_port)} to respond."
        )
    return True

def _start_server(
    ip: str = None,
    *,
    token: Optional[str] = None,
    host: Optional[str] = None,
    grpc_port: int = int(DEFAULT_GRPC_PORT),
    dist_port: int = int(DEFAULT_DIST_PORT),
    redis_port: Optional[int] = None,
    docker_network_mode: Optional[str] = None,
    docker_network_name: Optional[str] = None,
):
    if check_status(API_PID_FILE) == 0:
        if ip:
            console.print("[red]Error: MirrorNeuron API is already running.[/red]")
            console.print("Use 'mn runtime stop' to stop it first.")
            raise typer.Exit(1)
        console.print("[yellow]=> MirrorNeuron API is already running; checking runtime sidecars...[/yellow]")
        compose_runtime = runtime_compose_available()
        env = _runtime_base_env(compose_runtime)
        if not compose_runtime:
            for key, value in _runtime_grpc_tokens_from_running_container().items():
                if value and not str(env.get(key) or "").strip():
                    env[key] = value
        env = _ensure_runtime_grpc_tokens(env, persist_compose=compose_runtime)
        if compose_runtime:
            env = _ensure_compose_native_port_settings(env)
            if not _docker_container_running("mirror-neuron-core"):
                console.print("=> MirrorNeuron Core is not running; starting Docker runtime (Compose)...")
                try:
                    subprocess.run(runtime_compose_cmd("up", "-d"), check=True, stdout=subprocess.DEVNULL, env=env)
                    console.print("   [green][Started][/green] Docker runtime (Compose project: mirror-neuron)")
                except (FileNotFoundError, subprocess.CalledProcessError):
                    console.print("[red]Failed to start MirrorNeuron Docker runtime.[/red]")
                    raise typer.Exit(1)
        env.setdefault("MN_API_HOST", _api_host())
        env.setdefault("MN_API_PORT", DEFAULT_API_PORT)
        env.setdefault("MN_WEB_UI_HOST", _web_ui_host())
        env.setdefault("MN_WEB_UI_PORT", DEFAULT_WEB_UI_PORT)
        _start_api_if_installed(env)
        web_ui_available = _start_web_ui_if_installed(env)
        endpoint_snapshot = _write_runtime_endpoints_file(env, web_ui_available=web_ui_available)
        console.print(f"   Runtime endpoints: {RUNTIME_ENDPOINTS_FILE}")
        logger.info("Refreshed MirrorNeuron runtime endpoints: %s", endpoint_snapshot.get("api", {}))
        _print_service_endpoints(None, web_ui_available)
        return

    compose_runtime = runtime_compose_available()
    if not compose_runtime:
        try:
            docker_status = subprocess.run(["docker", "inspect", "-f", "{{.State.Running}}", "mirror-neuron-core"], capture_output=True, text=True)
            if docker_status.stdout.strip() == "true":
                console.print("[red]Error: MirrorNeuron Core (Docker) is already running.[/red]")
                console.print("Use 'mn runtime stop' to stop it first.")
                raise typer.Exit(1)
        except FileNotFoundError:
            console.print("[red]Error: Docker is not installed or not in PATH.[/red]")
            raise typer.Exit(1)

    PID_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    if ip and not token:
        console.print("[red]Error: mn node join requires --token from the main node.[/red]")
        raise typer.Exit(1)

    network_token = token or _resolve_network_token()
    if token:
        _write_network_token(network_token)
    env = _runtime_base_env(compose_runtime)
    persisted_join_profile_before_network = bool(compose_runtime and not ip and _persisted_join_profile(env))
    mode_override = docker_network_mode or os.getenv("MN_DOCKER_NETWORK_MODE", "").strip()
    requested_docker_mode = _docker_network_mode(
        mode_override or None,
        default="disabled",
    )
    network_name = _docker_network_name(docker_network_name)
    use_internal_identity = _docker_network_uses_internal_identity(requested_docker_mode)
    node_alias = _resolve_node_alias(env) if requested_docker_mode != "disabled" else ""
    if requested_docker_mode != "disabled":
        _ensure_docker_network(requested_docker_mode, network_name)
        env.update(_docker_network_env(requested_docker_mode, network_name, node_alias))
    else:
        env["MN_DOCKER_NETWORK_MODE"] = "disabled"
    advertised_host = _advertised_network_host(host)
    local_node_name = (
        _docker_node_name(node_alias)
        if use_internal_identity
        else _network_node_name(advertised_host)
    )
    join_handshake = (
        _handshake_with_main_node(
            ip,
            network_token,
            grpc_port,
            local_host=advertised_host,
            local_node_name=local_node_name,
        )
        if ip
        else None
    )
    if join_handshake:
        _validate_remote_redis_details(join_handshake, ip, network_token)
    reconnecting_joined_node = bool(compose_runtime and not ip and _persisted_join_profile(env))
    if compose_runtime:
        env = _ensure_compose_native_port_settings(env)
        env = _ensure_compose_cluster_bind_settings(env, advertised_host)
        if not reconnecting_joined_node:
            env = _ensure_compose_internal_redis_settings(
                env,
                token=network_token,
                network_redis_host=(
                    _docker_redis_alias(node_alias)
                    if use_internal_identity and node_alias
                    else None
                ),
                network_redis_port=(
                    REDIS_CONTAINER_PORT
                    if use_internal_identity and node_alias
                    else None
                ),
            )
            if not join_handshake:
                env = _ensure_compose_cluster_port_settings(
                    env,
                    token=network_token,
                    advertised_host=advertised_host,
                    redis_port=redis_port,
                )
    else:
        env.update(_runtime_blueprint_env_updates(env))
    _ensure_host_artifacts_dir(env)

    if join_handshake:
        seed_node_name = join_handshake["node_name"]
        seed_redis_host = join_handshake["redis_host"]
        seed_redis_port = redis_port or _parse_port(join_handshake["redis_port"], REDIS_CONTAINER_PORT)
    elif reconnecting_joined_node:
        seed_node_name = str(env.get("MN_CLUSTER_NODES") or "").strip()
        seed_redis_host = (
            str(env.get("MN_NETWORK_REDIS_HOST") or "").strip()
            or _network_node_host(seed_node_name)
            or advertised_host
        )
        parsed_redis_url = urlparse(str(env.get("MN_REDIS_URL") or ""))
        seed_redis_port = redis_port or (
            _parse_configured_port(env.get("MN_NETWORK_REDIS_PORT"))
            or _parse_configured_port(parsed_redis_url.port)
            or REDIS_CONTAINER_PORT
        )
    else:
        seed_node_name = local_node_name
        if use_internal_identity and node_alias:
            seed_redis_host = _docker_redis_alias(node_alias)
            seed_redis_port = REDIS_CONTAINER_PORT
        else:
            seed_redis_host = advertised_host
            seed_redis_port = redis_port or (
                _parse_port(
                    env.get("MN_NETWORK_REDIS_PORT") or env.get("MN_REDIS_PORT") or os.getenv("MN_REDIS_PORT"),
                    REDIS_CONTAINER_PORT,
                )
            )

    console.print("===========================================")
    if ip:
        console.print(f"Joining Cluster at {ip} in Detached Mode...")
    else:
        console.print("Starting Services in Detached Mode...")
    console.print("===========================================")

    env.setdefault("MN_CORE_HOST", "0.0.0.0")
    env.setdefault("MN_API_HOST", _api_host())
    env.setdefault("MN_REDIS_HOST", _redis_host())
    env.setdefault("MN_EPMD_HOST", "0.0.0.0")
    env.setdefault("MN_DIST_HOST", "0.0.0.0")
    env.setdefault("MN_WEB_UI_HOST", _web_ui_host())
    env.setdefault("MN_GRPC_PORT", str(grpc_port))
    env.setdefault("MN_API_PORT", DEFAULT_API_PORT)
    env.setdefault("MN_EPMD_PORT", DEFAULT_EPMD_PORT)
    env.setdefault("MN_WEB_UI_PORT", DEFAULT_WEB_UI_PORT)
    env.setdefault("MN_BLUEPRINT_WEB_UI_BIND_HOST", DEFAULT_BLUEPRINT_WEB_UI_BIND_HOST)
    env.setdefault("MN_BLUEPRINT_WEB_UI_PUBLIC_HOST", DEFAULT_BLUEPRINT_WEB_UI_PUBLIC_HOST)
    env.setdefault("MN_BLUEPRINT_WEB_UI_PORT_START", DEFAULT_BLUEPRINT_WEB_UI_PORT_START)
    env.setdefault("MN_BLUEPRINT_WEB_UI_PORT_END", DEFAULT_BLUEPRINT_WEB_UI_PORT_END)
    env.setdefault("MN_BLUEPRINT_WEB_UI_PORT_ALLOCATION_MODE", DEFAULT_BLUEPRINT_WEB_UI_PORT_ALLOCATION_MODE)
    env.setdefault("MN_CORE_GRPC_TARGET", f"localhost:{env.get('MN_GRPC_PORT', DEFAULT_GRPC_PORT)}")
    env["MN_NETWORK_JOIN_TOKEN"] = network_token
    env["MN_NETWORK_ADVERTISE_HOST"] = advertised_host
    env["MN_NETWORK_REDIS_HOST"] = seed_redis_host
    env["MN_NETWORK_REDIS_PORT"] = str(seed_redis_port)
    if requested_docker_mode != "disabled" and node_alias:
        env.update(_docker_network_env(requested_docker_mode, network_name, node_alias))
    if _generated_node_setting_should_update("MN_NODE_NAME", env.get("MN_NODE_NAME"), local_node_name):
        env["MN_NODE_NAME"] = local_node_name
    if not str(env.get("MN_NODE_ROLE") or "").strip():
        env["MN_NODE_ROLE"] = "runtime"
    if ip or not compose_runtime:
        env["MN_DIST_PORT"] = str(dist_port)
    else:
        env.setdefault("MN_DIST_PORT", str(dist_port))
    explicit_cookie = os.getenv("MN_COOKIE", "").strip()
    if explicit_cookie and explicit_cookie != "mirrorneuron":
        env["MN_COOKIE"] = explicit_cookie
    else:
        env["MN_COOKIE"] = _derive_network_secret(network_token, "cookie")
    env.setdefault("MN_GRPC_BIND_HOST", "0.0.0.0")
    env.setdefault("MN_EPMD_BIND_HOST", "0.0.0.0")
    env.setdefault("MN_DIST_BIND_HOST", "0.0.0.0")
    env.setdefault("ERL_EPMD_ADDRESS", "0.0.0.0")
    env.setdefault("ERL_AFLAGS", _erl_aflags(env["MN_DIST_PORT"]))
    env = _ensure_node_advertisement_settings(env)

    if join_handshake:
        redis_url = _redis_url_with_public_endpoint(
            join_handshake["redis_url"],
            seed_redis_host,
            seed_redis_port,
        )
        env["MN_REDIS_URL"] = redis_url
        env["MN_CONTEXT_REDIS_URL"] = _redis_url_with_database(redis_url, "1")
        env["MN_CLUSTER_NODES"] = seed_node_name
        if compose_runtime:
            _write_env_file_values(
                RUNTIME_COMPOSE_ENV,
                {
                    "MN_NODE_NAME": env["MN_NODE_NAME"],
                    "MN_CLUSTER_NODES": seed_node_name,
                    "MN_REDIS_URL": redis_url,
                    "MN_CONTEXT_REDIS_URL": env["MN_CONTEXT_REDIS_URL"],
                    "MN_NETWORK_REDIS_HOST": seed_redis_host,
                    "MN_NETWORK_REDIS_PORT": str(seed_redis_port),
                    **(
                        _docker_network_env(requested_docker_mode, network_name, node_alias)
                        if requested_docker_mode != "disabled" and node_alias
                        else {}
                    ),
                    "MN_COOKIE": env["MN_COOKIE"],
                },
            )
    else:
        if (
            not reconnecting_joined_node
            and _generated_cluster_setting_should_update(env.get("MN_CLUSTER_NODES"), local_node_name)
        ):
            env["MN_CLUSTER_NODES"] = local_node_name
        if compose_runtime and not reconnecting_joined_node and _compose_supports_redis_password():
            redis_password = _derive_network_secret(network_token, "redis")
            env["MN_REDIS_PASSWORD"] = redis_password
            env["MN_REDIS_URL"] = f"redis://:{redis_password}@redis:{REDIS_CONTAINER_PORT}/0"
            env["MN_CONTEXT_REDIS_URL"] = f"redis://:{redis_password}@redis:{REDIS_CONTAINER_PORT}/1"

    if compose_runtime:
        _write_env_file_values(
            RUNTIME_COMPOSE_ENV,
            {
                "MN_NETWORK_ADVERTISE_HOST": env["MN_NETWORK_ADVERTISE_HOST"],
                "MN_NODE_NAME": env["MN_NODE_NAME"],
                "MN_NODE_ROLE": env["MN_NODE_ROLE"],
                "MN_CLUSTER_NODES": env["MN_CLUSTER_NODES"],
                "MN_NETWORK_REDIS_HOST": env["MN_NETWORK_REDIS_HOST"],
                "MN_NETWORK_REDIS_PORT": env["MN_NETWORK_REDIS_PORT"],
                "MN_DIST_PORT": env["MN_DIST_PORT"],
                "MN_NODE_DISPLAY_NAME": env["MN_NODE_DISPLAY_NAME"],
                "MN_NODE_GPU_COUNT": env["MN_NODE_GPU_COUNT"],
                "MN_NODE_MODELS": env.get("MN_NODE_MODELS", ""),
                "MN_NODE_RUNTIME_MODELS": env.get("MN_NODE_RUNTIME_MODELS", ""),
                "MN_DOCKER_NETWORK_MODE": env.get("MN_DOCKER_NETWORK_MODE", "disabled"),
                "MN_DOCKER_NETWORK_NAME": env.get("MN_DOCKER_NETWORK_NAME", network_name),
                **(
                    _docker_network_env(requested_docker_mode, network_name, node_alias)
                    if requested_docker_mode != "disabled" and node_alias
                    else {}
                ),
            },
        )

    env = _ensure_runtime_grpc_tokens(env, persist_compose=compose_runtime)
    if compose_runtime:
        _write_env_file_values(RUNTIME_COMPOSE_ENV, {"MN_COOKIE": env["MN_COOKIE"]})

    if compose_runtime:
        env = _compose_runtime_env(env, ip)
        console.print("=> Starting MirrorNeuron Docker runtime (Compose)...")
        logger.info("Starting MirrorNeuron Docker Compose runtime")
        try:
            subprocess.run(runtime_compose_cmd("up", "-d"), check=True, stdout=subprocess.DEVNULL, env=env)
            console.print("   [green][Started][/green] Docker runtime (Compose project: mirror-neuron)")
        except (FileNotFoundError, subprocess.CalledProcessError):
            console.print("[red]Failed to start MirrorNeuron Docker Compose runtime.[/red]")
            raise typer.Exit(1)
    else:
        console.print("=> Starting MirrorNeuron Core Service (Docker)...")
        logger.info("Starting MirrorNeuron Core Docker container")
        subprocess.run(["docker", "rm", "-f", "mirror-neuron-core"], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)

        cmd = ["docker", "run", "-d", "--name", "mirror-neuron-core"]

        cmd.extend(["-e", f"MN_NODE_NAME={env['MN_NODE_NAME']}"])
        cmd.extend(["-e", f"MN_COOKIE={env['MN_COOKIE']}"])
        cmd.extend(["-e", f"MN_GRPC_AUTH_TOKEN={env['MN_GRPC_AUTH_TOKEN']}"])
        cmd.extend(["-e", f"{GRPC_ADMIN_TOKEN_ENV}={env[GRPC_ADMIN_TOKEN_ENV]}"])
        cmd.extend(["-e", f"MN_NETWORK_JOIN_TOKEN={env['MN_NETWORK_JOIN_TOKEN']}"])
        cmd.extend(["-e", f"MN_NETWORK_ADVERTISE_HOST={env['MN_NETWORK_ADVERTISE_HOST']}"])
        cmd.extend(["-e", f"MN_NETWORK_REDIS_HOST={env['MN_NETWORK_REDIS_HOST']}"])
        cmd.extend(["-e", f"MN_NETWORK_REDIS_PORT={env['MN_NETWORK_REDIS_PORT']}"])
        cmd.extend(["-e", f"MN_CLUSTER_NODES={env['MN_CLUSTER_NODES']}"])
        if env.get("MN_NODE_ALIAS"):
            cmd.extend(["-e", f"MN_NODE_ALIAS={env['MN_NODE_ALIAS']}"])
        if env.get("MN_DOCKER_NETWORK_MODE"):
            cmd.extend(["-e", f"MN_DOCKER_NETWORK_MODE={env['MN_DOCKER_NETWORK_MODE']}"])
        if env.get("MN_DOCKER_NETWORK_NAME"):
            cmd.extend(["-e", f"MN_DOCKER_NETWORK_NAME={env['MN_DOCKER_NETWORK_NAME']}"])
        cmd.extend(["-e", f"MN_NODE_ROLE={env['MN_NODE_ROLE']}"])
        cmd.extend(["-e", f"MN_NODE_DISPLAY_NAME={env['MN_NODE_DISPLAY_NAME']}"])
        cmd.extend(["-e", f"MN_NODE_GPU_COUNT={env['MN_NODE_GPU_COUNT']}"])
        cmd.extend(["-e", f"MN_GRPC_PORT={env['MN_GRPC_PORT']}"])
        cmd.extend(["-e", f"MN_DIST_PORT={env['MN_DIST_PORT']}"])
        cmd.extend(["-e", f"MN_RUNS_ROOT={env.get('MN_CONTAINER_RUNS_ROOT', DEFAULT_CONTAINER_RUNS_ROOT)}"])
        cmd.extend(["-e", f"ERL_AFLAGS={env['ERL_AFLAGS']}"])

        core_publish_host = _docker_publish_host(env["MN_CORE_HOST"])
        system_name = os.uname().sysname
        if requested_docker_mode != "disabled" and node_alias:
            cmd.extend(_docker_network_run_args(requested_docker_mode, network_name, node_alias))

        if system_name == "Darwin":
            cmd.extend(["-p", f"{core_publish_host}:{env['MN_GRPC_PORT']}:{env['MN_GRPC_PORT']}"])
            if requested_docker_mode == "disabled":
                epmd_publish_host = _docker_publish_host(env["MN_EPMD_HOST"])
                dist_publish_host = _docker_publish_host(env["MN_DIST_HOST"])
                cmd.extend(["-p", f"{epmd_publish_host}:{env['MN_EPMD_PORT']}:4369"])
                cmd.extend(["-p", f"{dist_publish_host}:{env['MN_DIST_PORT']}:{env['MN_DIST_PORT']}"])
            cmd.extend(["-e", f"MN_REDIS_URL={env.get('MN_REDIS_URL', 'redis://host.docker.internal:6379/0')}"])
            cmd.extend(["-e", "MN_EXECUTOR_MAX_CONCURRENCY=50"])
        else:
            if requested_docker_mode == "disabled":
                cmd.extend(["--network", "host"])
            cmd.extend(["-e", "MN_EXECUTOR_MAX_CONCURRENCY=50"])
            if env.get("MN_REDIS_URL"):
                cmd.extend(["-e", f"MN_REDIS_URL={env['MN_REDIS_URL']}"])

        if system_name == "Darwin":
            cmd.extend(["-e", "MN_CORE_HOST=0.0.0.0"])
        else:
            cmd.extend(["-e", f"MN_CORE_HOST={env['MN_CORE_HOST']}"])
            cmd.extend(["-e", f"MN_REDIS_HOST={env['MN_REDIS_HOST']}"])
            cmd.extend(["-e", f"ERL_EPMD_ADDRESS={env['MN_EPMD_HOST']}"])

        for env_name in [
            "SLACK_BOT_TOKEN",
            "SLACK_DEFAULT_CHANNEL",
            "SLACK_API_BASE_URL",
            "MN_SLACK_BOT_TOKEN",
            "MN_SLACK_DEFAULT_CHANNEL",
            "MN_SLACK_API_BASE_URL",
        ]:
            if os.getenv(env_name):
                cmd.extend(["-e", env_name])

        openshell_container_config_dir = Path(
            os.getenv(
                "OPENSHELL_CONTAINER_CONFIG_DIR",
                str(Path.home() / ".config" / "openshell-mirror-neuron"),
            )
        )
        openshell_config_dir = openshell_container_config_dir
        if not (openshell_config_dir / "gateways" / "openshell").is_dir():
            openshell_config_dir = Path.home() / ".config" / "openshell"
        if (openshell_config_dir / "gateways" / "openshell").is_dir():
            cmd.extend(["-v", f"{openshell_config_dir}:/root/.config/openshell:ro"])
            cmd.extend(["-v", f"{openshell_config_dir}:/opt/mirror_neuron/.config/openshell:ro"])

        host_home_dir = str(env.get("MN_HOST_HOME_DIR") or env.get("MN_HOST_MN_DIR") or DIR)
        host_artifacts_dir = str(env.get("MN_HOST_ARTIFACTS_DIR") or Path(host_home_dir).expanduser() / "runs")
        container_runs_root = str(env.get("MN_CONTAINER_RUNS_ROOT") or DEFAULT_CONTAINER_RUNS_ROOT)
        cmd.extend(["-v", f"{host_home_dir}:/root/.mn"])
        cmd.extend(["-v", f"{host_home_dir}:/opt/mirror_neuron/.mn"])
        cmd.extend(["-v", f"{host_artifacts_dir}:{container_runs_root}"])
        cmd.extend(["-v", f"{host_artifacts_dir}:/opt/mirror_neuron/.mn/runs"])

        cmd.append("mirror-neuron-core:latest")

        try:
            subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL)
            console.print("   [green][Started][/green] Core Service (Docker: mirror-neuron-core)")
        except subprocess.CalledProcessError:
            console.print("[red]Failed to start Core Service Docker container.[/red]")
            raise typer.Exit(1)

    console.print("=> Waiting for Elixir to boot...")
    time.sleep(3)

    api_started = _start_api_if_installed(env)

    web_ui_available = _start_web_ui_if_installed(env)
    if api_started:
        endpoint_snapshot = _write_runtime_endpoints_file(env, web_ui_available=web_ui_available)
        console.print(f"   Runtime endpoints: {RUNTIME_ENDPOINTS_FILE}")
        logger.info("Wrote MirrorNeuron runtime endpoints: %s", endpoint_snapshot.get("api", {}))

    _print_service_endpoints(ip, web_ui_available)
    network_args = _docker_network_command_args(requested_docker_mode, network_name)
    details = [
        ("Network token", network_token),
        ("Core log", BEAM_LOG),
        ("API log", API_LOG),
    ]
    if WEB_UI_LOG.exists():
        details.append(("Web log", WEB_UI_LOG))
    print_success_confirmation(
        console,
        "Runtime start",
        status=f"joining cluster at {ip}" if ip else "running",
        details=details,
        next_steps=(
            "mn runtime start --worker-node",
            f"mn node join <worker-host> --token <worker-token>{network_args}",
            "mn runtime stop",
        ),
    )
