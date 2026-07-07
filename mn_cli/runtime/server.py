import os
import json
import hashlib
import ipaddress
import re
import signal
import secrets
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import urlparse
import typer
from rich.console import Console
from rich.table import Table
from mn_sdk.blueprint_source import DEFAULT_BLUEPRINT_REPO, normalize_blueprint_repo_value
from mn_sdk.native_resources import node_resource_environment
from mn_cli.config import CliConfig
from mn_cli.libs.ui import print_confirmed, print_success_confirmation
from mn_cli.logging_config import configure_logging
from mn_cli.runtime_state import (
    mn_home as _runtime_mn_home,
    read_env_file as _runtime_read_env_file,
    remove_env_file_keys as _runtime_remove_env_file_keys,
    write_env_file_values as _runtime_write_env_file_values,
    write_private_text,
)

console = Console()
logger = configure_logging("mn-cli", CliConfig.from_env().log_path)
GRPC_ADMIN_TOKEN_ENV = "MN_GRPC_ADMIN_TOKEN"
GRPC_AUTH_TOKEN_FILE_ENV = "MN_GRPC_AUTH_TOKEN_FILE"
GRPC_ADMIN_TOKEN_FILE_ENV = "MN_GRPC_ADMIN_TOKEN_FILE"
FIXED_GRPC_AUTH_TOKEN = "mirror_neuron_password"
FIXED_GRPC_ADMIN_TOKEN = "mirror_neuron_password_admin"
DEV_REDIS_PASSWORD = "mirror_neuron_redis_dev"

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

def _distributed_core_command() -> list[str]:
    command = (
        "if [ -x \"bin/mirror_neuron\" ]; then "
        "if [ -n \"${MN_NODE_NAME:-}\" ]; then "
        "if [ -z \"${MN_COOKIE:-}\" ] || [ \"${MN_COOKIE:-}\" = \"mirrorneuron\" ]; then "
        "echo \"MN_COOKIE must be set to a non-default secret when MN_NODE_NAME enables distributed Erlang\" >&2; exit 1; "
        "fi; "
        "unset ERL_EPMD_ADDRESS; "
        "epmd_bin=\"$(find erts-* -path '*/bin/epmd' -type f | head -n 1)\"; "
        "if [ -n \"$epmd_bin\" ]; then \"$epmd_bin\" -daemon; else epmd -daemon; fi; "
        "export RELEASE_DISTRIBUTION=name; "
        "export RELEASE_NODE=\"$MN_NODE_NAME\"; "
        "export RELEASE_COOKIE=\"$MN_COOKIE\"; "
        "else "
        "export RELEASE_DISTRIBUTION=none; "
        "fi; "
        "exec bin/mirror_neuron start; "
        "fi; "
        "if [ -n \"${MN_NODE_NAME:-}\" ]; then "
        "if [ -z \"${MN_COOKIE:-}\" ] || [ \"${MN_COOKIE:-}\" = \"mirrorneuron\" ]; then "
        "echo \"MN_COOKIE must be set to a non-default secret when MN_NODE_NAME enables distributed Erlang\" >&2; exit 1; "
        "fi; "
        "dist_port=\"${MN_DIST_PORT:-4370}\"; "
        "unset ERL_EPMD_ADDRESS; "
        "epmd -daemon; "
        "exec elixir --name \"$MN_NODE_NAME\" --cookie \"$MN_COOKIE\" --erl "
        "\"-kernel inet_dist_listen_min ${dist_port} inet_dist_listen_max ${dist_port}\" -S mix run --no-halt; "
        "else "
        "exec mix run --no-halt; "
        "fi"
    )
    return ["sh", "-c", command]

def _mn_home() -> Path:
    return _runtime_mn_home()


DIR = _mn_home()
PID_DIR = DIR / "pids"
LOG_DIR = DIR / "logs"
BEAM_PID_FILE = PID_DIR / "beam.pid"
API_PID_FILE = PID_DIR / "api.pid"
API_WATCHDOG_PID_FILE = PID_DIR / "api-watchdog.pid"
NATIVE_SDK_GRPC_PID_FILE = PID_DIR / "native-sdk-grpc.pid"
NATIVE_SDK_GRPC_WATCHDOG_PID_FILE = PID_DIR / "native-sdk-grpc-watchdog.pid"
WEB_UI_PID_FILE = PID_DIR / "web-ui.pid"
WEB_UI_WATCHDOG_PID_FILE = PID_DIR / "web-ui-watchdog.pid"
API_TOKEN_FILE = DIR / "api.token"
REDIS_PASSWORD_FILE = DIR / "redis.password"
BEAM_LOG = LOG_DIR / "beam.log"
API_LOG = LOG_DIR / "api.log"
API_WATCHDOG_LOG = LOG_DIR / "api-watchdog.log"
NATIVE_SDK_GRPC_LOG = LOG_DIR / "native-sdk-grpc.log"
NATIVE_SDK_GRPC_WATCHDOG_LOG = LOG_DIR / "native-sdk-grpc-watchdog.log"
WEB_UI_LOG = LOG_DIR / "web-ui.log"
WEB_UI_WATCHDOG_LOG = LOG_DIR / "web-ui-watchdog.log"
VENV_DIR = Path.home() / ".local" / "share" / "mn_venv"
RUNTIME_COMPOSE_FILE = DIR / "docker-compose.yml"
RUNTIME_COMPOSE_ENV = DIR / "docker-compose.env"
RUNTIME_ENDPOINTS_FILE = DIR / "runtime-endpoints.json"
RUNTIME_MODELS_OVERRIDE_FILE = "docker-compose.models.yml"
RUNTIME_WORKERS_OVERRIDE_FILE = "docker-compose.workers.yml"
RUNTIME_MODEL_RUNNER_PROXY_OVERRIDE_FILE = "docker-compose.model-runner-proxy.yml"
RUNTIME_SYNCTHING_OVERRIDE_FILE = "docker-compose.syncthing.yml"
LITELLM_GATEWAY_CONFIG_DIR = DIR / "models" / "litellm-gateway"
LITELLM_GATEWAY_CONFIG_FILE = LITELLM_GATEWAY_CONFIG_DIR / "config.yaml"
DEFAULT_LLM_MODEL_RUNNER_MODEL = "gemma4:e2b"
DEFAULT_CONTEXT_MODEL_RUNNER_MODEL = "hf.co/homerquan/mn-context-engine-model-v-Q4_K_M"
DEFAULT_MODEL_RUNNER_PROXY_PORT = "12435"
DEFAULT_MEMBRANE_REPO = "MirrorNeuronLab/Membrane"
DEFAULT_MEMBRANE_ENGINE_IMAGE_REPOSITORY = (
    "us-central1-docker.pkg.dev/mirrorneuron-public-packages/"
    "mirrorneuron-runtime/membrane-context-engine"
)
PUBLIC_GAR_PROJECT_PATH = "/mirrorneuron-public-packages/"
CONTEXT_ENGINE_SERVICE = "membrane-context-engine"
CONTEXT_ENGINE_CONTAINER = "mirror-neuron-context-engine"
CONTEXT_ENGINE_MODEL_CONTAINER = "mirror-neuron-context-engine-model"
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


def _source_checkout_api_dir() -> Optional[Path]:
    checkout_dir = Path(__file__).resolve().parents[2]
    api_dir = checkout_dir / "mn-api"
    return api_dir if (api_dir / "mn_api").is_dir() else None


def _source_checkout_sdk_dir() -> Optional[Path]:
    checkout_dir = Path(__file__).resolve().parents[2]
    sdk_dir = checkout_dir / "mn-python-sdk"
    return sdk_dir if (sdk_dir / "mn_sdk").is_dir() else None


def _sidecar_workdir() -> Path:
    try:
        return Path.cwd()
    except OSError:
        return DIR


def _prepend_pythonpath(env: dict[str, str], path: Path) -> None:
    current = env.get("PYTHONPATH")
    value = str(path)
    if current:
        parts = [part for part in current.split(os.pathsep) if part]
        if value in parts:
            return
        env["PYTHONPATH"] = os.pathsep.join([value, *parts])
    else:
        env["PYTHONPATH"] = value


def _web_ui_dirs() -> tuple[Path, ...]:
    paths = [
        DIR / "webui",
        DIR / "web-ui-source",
    ]
    source_web_ui_dir = _source_checkout_web_ui_dir()
    if source_web_ui_dir is not None:
        paths.append(source_web_ui_dir)
    return _unique_paths(paths)


WEB_UI_DIRS = _web_ui_dirs()
DEFAULT_HOST = "localhost"
DEFAULT_GRPC_PORT = "55051"
DEFAULT_API_PORT = "54001"
DEFAULT_NATIVE_SDK_GRPC_PORT = "55052"
DEFAULT_NATIVE_SDK_GRPC_HOST = "127.0.0.1"
DEFAULT_NATIVE_SDK_GRPC_TARGET_HOST = "host.docker.internal"
DEFAULT_NATIVE_SDK_GRPC_COMPOSE_SERVICE = "mn-native-sdk-grpc"


def _ensure_litellm_gateway_host_config() -> None:
    try:
        LITELLM_GATEWAY_CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        if not LITELLM_GATEWAY_CONFIG_FILE.exists():
            LITELLM_GATEWAY_CONFIG_FILE.write_text('{"model_list":[]}\n', encoding="utf-8")
    except OSError as exc:
        logger.warning("Could not prepare LiteLLM gateway config path: %s", exc)
        console.print(
            "[yellow]=> Warning: LiteLLM gateway config is not host-writable; "
            "model route sync may fail until file ownership is fixed.[/yellow]"
        )
DEFAULT_EPMD_PORT = "54369"
DEFAULT_DIST_PORT = "54370"
DEFAULT_WEB_UI_PORT = "55173"
DEFAULT_WEB_UI_RESTART_DELAY_SECONDS = "2"
DEFAULT_OPENSHELL_GATEWAY_PORT = "58080"
DEFAULT_ARTIFACT_PORT = "55660"
DEFAULT_BLUEPRINT_WEB_UI_BIND_HOST = "0.0.0.0"
DEFAULT_BLUEPRINT_WEB_UI_PUBLIC_HOST = "localhost"
DEFAULT_BLUEPRINT_WEB_UI_PORT_START = "61000"
DEFAULT_BLUEPRINT_WEB_UI_PORT_END = "61049"
DEFAULT_BLUEPRINT_WEB_UI_PORT_ALLOCATION_MODE = "prepublished"
DEFAULT_CONTAINER_RUNS_ROOT = "/root/.mn/runs"
DEFAULT_CONTAINER_BLOB_STORE_ROOT = "/root/.mn/blobs"
DEFAULT_RUNTIME_SHARED_STORAGE_ROOT = "/root/.mn/shared"
DEFAULT_RUNTIME_BUNDLE_CACHE_DIR = f"{DEFAULT_RUNTIME_SHARED_STORAGE_ROOT}/bundle_cache"
DEFAULT_REDIS_IMAGE = "redis:8"
DEFAULT_SYNCTHING_IMAGE = "syncthing/syncthing:latest"
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
DEFAULT_REDIS_SENTINEL_PORT = 26379
DEFAULT_SYNCTHING_GUI_PORT = 58384
DEFAULT_SYNCTHING_SYNC_PORT = 22000
SYNCTHING_CONTAINER_GUI_PORT = 8384
SYNCTHING_CONTAINER_SYNC_PORT = 22000
SYNCTHING_FOLDER_ID = "mirror-neuron-shared"
SYNCTHING_FOLDER_LABEL = "MirrorNeuron shared storage"
SYNCTHING_FOLDER_PATH = "/var/syncthing/MirrorNeuronShared"
SYNCTHING_COMPOSE_SERVICE = "syncthing"
NETWORK_TOKEN_FILE = DIR / "network.token"
NETWORK_REDIS_ENV_FILE = DIR / "network-redis.env"
SYNCTHING_API_KEY_FILE = DIR / "syncthing.api-key"
SYNCTHING_CONFIG_DIR = DIR / "syncthing"
DEFAULT_DOCKER_NETWORK_NAME = "mirror-neuron-runtime"
NETWORK_DOCKER_NETWORK = DEFAULT_DOCKER_NETWORK_NAME
LOCAL_CORE_CONTAINER = "mirror-neuron-core"
COMPOSE_REDIS_CONTAINER = "mirror-neuron-redis"
COMPOSE_SENTINEL_CONTAINER = "mirror-neuron-sentinel"
SYNCTHING_CONTAINER = "mirror-neuron-syncthing"
NETWORK_CORE_CONTAINER = "mirror-neuron-network-core"
NETWORK_REDIS_CONTAINER = "mirror-neuron-network-redis"
NETWORK_SENTINEL_CONTAINER = "mirror-neuron-network-sentinel"
RUNTIME_CLUSTER_OVERRIDE_FILE = "docker-compose.cluster.yml"
JOIN_CLAIM_FILE = DIR / "cluster-join-claim.json"
JOIN_OWNER_ENV_KEYS = {
    "MN_JOIN_OWNER_NODE",
    "MN_JOIN_OWNER_HOST",
    "MN_JOIN_OWNER_GRPC_PORT",
    "MN_JOIN_WORKER_NODE",
}
DEPRECATED_RUNTIME_ENV_KEYS = {
    "MN_ARTIFACT_AUTH_TOKEN",
    "MN_MIRROR_NEURON_GRPC_ADMIN_TOKEN",
    "MN_HOST_MN_DIR",
    "MN_HOST_SHARED_ARTIFACT_ROOT",
    "MN_CONTAINER_SHARED_ARTIFACT_ROOT",
}


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

def _command_stdout(command: list[str], timeout: int = 5) -> str:
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
    except (FileNotFoundError, subprocess.SubprocessError):
        return ""
    return result.stdout if result.returncode == 0 else ""

def _detect_host_cpu_model() -> str:
    explicit = os.getenv("MN_NODE_CPU_MODEL", "").strip()
    if explicit:
        return explicit

    system_name = os.uname().sysname
    if system_name == "Darwin":
        return _command_stdout(["sysctl", "-n", "machdep.cpu.brand_string"]).strip()

    if system_name == "Linux":
        try:
            cpuinfo = Path("/proc/cpuinfo").read_text(encoding="utf-8", errors="ignore")
        except OSError:
            return ""
        for line in cpuinfo.splitlines():
            if ":" not in line:
                continue
            key, value = line.split(":", 1)
            if key.strip().lower() in {"model name", "hardware", "processor", "cpu model"}:
                return value.strip()

    return ""

def _version_after(text: str, label: str) -> str:
    match = re.search(rf"{re.escape(label)}\s*[:=\- ]*\s*([0-9]+(?:\.[0-9]+)*)", text, re.I)
    return match.group(1) if match else ""

def _detect_host_gpu_profile() -> dict[str, str]:
    profile: dict[str, str] = {}
    system_name = os.uname().sysname

    if system_name == "Darwin":
        output = _command_stdout(["system_profiler", "SPDisplaysDataType"], timeout=10)
        for line in output.splitlines():
            if "Chipset Model:" not in line:
                continue
            name = line.split(":", 1)[1].strip()
            if name:
                return {
                    "MN_NODE_GPU_VENDOR": "apple",
                    "MN_NODE_GPU_DRIVER": "metal",
                    "MN_NODE_GPU_TYPE": "apple/gpu",
                    "MN_NODE_GPU_NAME": name,
                }

    if system_name == "Linux":
        output = _command_stdout(
            ["nvidia-smi", "--query-gpu=name,driver_version", "--format=csv,noheader,nounits"]
        )
        if output.strip():
            first = [part.strip() for part in output.splitlines()[0].split(",", 1)]
            profile = {
                "MN_NODE_GPU_VENDOR": "nvidia",
                "MN_NODE_GPU_DRIVER": "cuda",
                "MN_NODE_GPU_TYPE": "nvidia/gpu",
                "MN_NODE_GPU_NAME": first[0],
            }
            if len(first) > 1 and first[1]:
                profile["MN_NODE_GPU_DRIVER_VERSION"] = first[1]
            cuda = _version_after(_command_stdout(["nvidia-smi"]), "CUDA Version:")
            if cuda:
                profile["MN_NODE_GPU_API_VERSION"] = cuda
            return profile

        lspci = _command_stdout(["lspci", "-mm"])
        for line in lspci.splitlines():
            normalized = line.lower()
            if not any(kind in normalized for kind in ("vga compatible controller", "3d controller", "display controller")):
                continue
            quoted = re.findall(r'"([^"]*)"', line)
            name = " ".join(part for part in quoted[1:3] if part).strip() or line
            if "amd" in normalized or "advanced micro devices" in normalized or "radeon" in normalized:
                rocm = os.getenv("ROCM_VERSION", "").strip() or os.getenv("HIP_VERSION", "").strip()
                profile = {
                    "MN_NODE_GPU_VENDOR": "amd",
                    "MN_NODE_GPU_DRIVER": "rocm",
                    "MN_NODE_GPU_TYPE": "amd/gpu",
                    "MN_NODE_GPU_NAME": name,
                }
                if rocm:
                    profile["MN_NODE_GPU_API_VERSION"] = rocm
                return profile
            if "intel" in normalized:
                return {
                    "MN_NODE_GPU_VENDOR": "intel",
                    "MN_NODE_GPU_DRIVER": "intel",
                    "MN_NODE_GPU_TYPE": "intel/gpu",
                    "MN_NODE_GPU_NAME": name,
                }

    return {}

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
    try:
        adjusted.update(node_resource_environment(env=adjusted))
    except Exception:
        logger.debug("Failed to build SDK node resource advertisement", exc_info=True)

    if not str(adjusted.get("MN_NODE_DISPLAY_NAME") or os.getenv("MN_NODE_DISPLAY_NAME", "")).strip():
        adjusted["MN_NODE_DISPLAY_NAME"] = _node_display_name()

    if not str(adjusted.get("MN_NODE_CPU_MODEL") or "").strip():
        cpu_model = _detect_host_cpu_model()
        if cpu_model:
            adjusted["MN_NODE_CPU_MODEL"] = cpu_model

    detected_gpu_count = _detect_host_gpu_count()
    existing_gpu_count = _parse_gpu_count(adjusted.get("MN_NODE_GPU_COUNT"))
    if os.getenv("MN_NODE_GPU_COUNT", "").strip():
        adjusted["MN_NODE_GPU_COUNT"] = str(detected_gpu_count)
    elif detected_gpu_count > 0 or existing_gpu_count is None:
        adjusted["MN_NODE_GPU_COUNT"] = str(detected_gpu_count)

    if detected_gpu_count > 0:
        for key, value in _detect_host_gpu_profile().items():
            if value and not str(adjusted.get(key) or "").strip():
                adjusted[key] = value

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
    write_private_text(cookie_file, f"{generated_cookie}\n")
    return generated_cookie

def _resolve_grpc_auth_token() -> str:
    return FIXED_GRPC_AUTH_TOKEN

def _resolve_grpc_admin_token() -> str:
    return FIXED_GRPC_ADMIN_TOKEN

def _runtime_env_name(env: Optional[dict[str, str]] = None) -> str:
    values = env or {}
    return str(values.get("MN_ENV") or os.getenv("MN_ENV") or "dev").strip().lower()

def _runtime_env_is_prod(env: Optional[dict[str, str]] = None) -> bool:
    return _runtime_env_name(env) in {"prod", "production"}

def _derive_redis_password(admin_token: str) -> str:
    material = f"mirror-neuron:redis:{admin_token}".encode("utf-8")
    return hashlib.sha256(material).hexdigest()

def _resolve_redis_password(env: Optional[dict[str, str]] = None) -> str:
    if not _runtime_env_is_prod(env):
        return DEV_REDIS_PASSWORD

    admin_token = str((env or {}).get(GRPC_ADMIN_TOKEN_ENV) or "").strip() or _resolve_grpc_admin_token()
    password = _derive_redis_password(admin_token)
    write_private_text(REDIS_PASSWORD_FILE, f"{password}\n")
    return password

def _resolve_api_token() -> str:
    env_token = os.getenv("MN_API_TOKEN", "").strip()
    if env_token:
        write_private_text(API_TOKEN_FILE, f"{env_token}\n")
        return env_token
    try:
        existing_token = API_TOKEN_FILE.read_text(encoding="utf-8").strip()
        if existing_token:
            return existing_token
    except OSError:
        pass
    generated_token = secrets.token_hex(32)
    write_private_text(API_TOKEN_FILE, f"{generated_token}\n")
    return generated_token

def _ensure_runtime_api_token(env: dict[str, str], *, persist_compose: bool = False) -> dict[str, str]:
    resolved = dict(env)
    if str(resolved.get("MN_ENV") or "").strip().lower() != "prod":
        return resolved
    if not str(resolved.get("MN_API_TOKEN") or "").strip():
        resolved["MN_API_TOKEN"] = _resolve_api_token()
    else:
        write_private_text(API_TOKEN_FILE, f"{resolved['MN_API_TOKEN'].strip()}\n")
    if persist_compose and runtime_compose_available():
        _write_env_file_values(RUNTIME_COMPOSE_ENV, {"MN_API_TOKEN": resolved["MN_API_TOKEN"]})
    return resolved

def _ensure_runtime_grpc_tokens(env: dict[str, str], *, persist_compose: bool = False) -> dict[str, str]:
    resolved = dict(env)
    fixed_tokens = {
        "MN_GRPC_AUTH_TOKEN": FIXED_GRPC_AUTH_TOKEN,
        GRPC_ADMIN_TOKEN_ENV: FIXED_GRPC_ADMIN_TOKEN,
    }
    stale_token_keys = {
        "MN_MIRROR_NEURON_GRPC_ADMIN_TOKEN",
        GRPC_AUTH_TOKEN_FILE_ENV,
        GRPC_ADMIN_TOKEN_FILE_ENV,
    }
    for key in stale_token_keys:
        resolved.pop(key, None)
    resolved.update(fixed_tokens)
    if persist_compose:
        _write_env_file_values(RUNTIME_COMPOSE_ENV, fixed_tokens)
        _remove_env_file_keys(RUNTIME_COMPOSE_ENV, stale_token_keys)
    return resolved

def _grpc_tokens_from_handshake(handshake: Optional[dict]) -> dict[str, str]:
    return {
        "MN_GRPC_AUTH_TOKEN": FIXED_GRPC_AUTH_TOKEN,
        GRPC_ADMIN_TOKEN_ENV: FIXED_GRPC_ADMIN_TOKEN,
    }

SHARED_STORAGE_ENV_KEYS = (
    "MN_HOST_SHARED_STORAGE_ROOT",
    "MN_SHARED_STORAGE_ROOT",
    "MN_RUNTIME_SHARED_STORAGE_ROOT",
    "MN_CONTAINER_SHARED_STORAGE_ROOT",
    "MN_BUNDLE_CACHE_DIR",
)
NODE_ADVERTISEMENT_ENV_KEYS = (
    "MN_NODE_HARDWARE_JSON",
    "MN_NODE_DISPLAY_NAME",
    "MN_NODE_CPU_CORES",
    "MN_NODE_CPU_MODEL",
    "MN_NODE_MEMORY_TOTAL_MB",
    "MN_NODE_MEMORY_AVAILABLE_MB",
    "MN_NODE_DISK_TOTAL_MB",
    "MN_NODE_DISK_AVAILABLE_MB",
    "MN_NODE_HOST_PATHS",
    "MN_NODE_RUNTIME_DRIVERS",
    "MN_NODE_GPU",
    "MN_NODE_GPU_COUNT",
    "MN_NODE_GPU_VENDOR",
    "MN_NODE_GPU_DRIVER",
    "MN_NODE_GPU_TYPE",
    "MN_NODE_GPU_NAME",
    "MN_NODE_GPU_API_VERSION",
    "MN_NODE_GPU_DRIVER_VERSION",
    "MN_NODE_GPU_MEMORY_TOTAL_MB",
    "MN_NODE_GPU_MEMORY_FREE_MB",
    "MN_NODE_GPU_MEMORY_USED_MB",
    "MN_NODE_GPU_SHARED_MEMORY",
    "MN_NODE_GPU_INTEGRATED",
    "MN_NODE_GPU_UNIFIED_MEMORY",
    "MN_NODE_GPU_UNIFIED_MEMORY_MB",
)

def _shared_storage_env_values(host_root: str, runtime_root: str) -> dict[str, str]:
    runtime_root = str(runtime_root or DEFAULT_RUNTIME_SHARED_STORAGE_ROOT).strip() or DEFAULT_RUNTIME_SHARED_STORAGE_ROOT
    expanded_host_root = str(Path(host_root).expanduser())
    return {
        "MN_HOST_SHARED_STORAGE_ROOT": expanded_host_root,
        "MN_SHARED_STORAGE_ROOT": expanded_host_root,
        "MN_RUNTIME_SHARED_STORAGE_ROOT": runtime_root,
        "MN_CONTAINER_SHARED_STORAGE_ROOT": runtime_root,
        "MN_BUNDLE_CACHE_DIR": f"{runtime_root.rstrip('/')}/bundle_cache",
    }

def _shared_storage_env_from_runtime_env(env: dict[str, str]) -> dict[str, str]:
    host_root, runtime_root, _bundle_cache = _network_shared_storage_roots(env)
    return _shared_storage_env_values(host_root, runtime_root)

def _shared_storage_roots_from_handshake(handshake: Optional[dict]) -> tuple[str, str]:
    if not isinstance(handshake, dict):
        return "", DEFAULT_RUNTIME_SHARED_STORAGE_ROOT

    node_info = handshake.get("node_info")
    if not isinstance(node_info, dict):
        node_info = {}

    host_root = str(
        handshake.get("host_shared_storage_root")
        or node_info.get("host_shared_storage_root")
        or ""
    ).strip()
    runtime_root = str(
        handshake.get("runtime_shared_storage_root")
        or node_info.get("runtime_shared_storage_root")
        or DEFAULT_RUNTIME_SHARED_STORAGE_ROOT
    ).strip() or DEFAULT_RUNTIME_SHARED_STORAGE_ROOT
    return host_root, runtime_root

def _shared_storage_env_from_handshake(handshake: Optional[dict]) -> dict[str, str]:
    host_root, runtime_root = _shared_storage_roots_from_handshake(handshake)
    if not host_root:
        return {}

    expanded_host_root = Path(host_root).expanduser()
    if not expanded_host_root.exists():
        console.print(
            "[yellow]Warning: remote shared-storage root is not a local path:[/yellow] "
            f"{host_root}"
        )
        console.print(
            "[yellow]         Joined nodes keep a local shared-storage root; use Syncthing replication "
            "instead of mounting a remote host path.[/yellow]"
        )
        return {}

    return _shared_storage_env_values(str(expanded_host_root), runtime_root)

def _truthy_env(value: object) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on", "required"}

SYNCTHING_ENV_KEYS = (
    "MN_SYNCTHING_ENABLED",
    "MN_SYNCTHING_IMAGE",
    "MN_SYNCTHING_API_KEY",
    "MN_SYNCTHING_DEVICE_ID",
    "MN_SYNCTHING_ADVERTISE_HOST",
    "MN_SYNCTHING_BIND_HOST",
    "MN_SYNCTHING_GUI_PORT",
    "MN_SYNCTHING_SYNC_PORT",
    "MN_SYNCTHING_FOLDER_ID",
    "MN_SYNCTHING_FOLDER_PATH",
)

def _syncthing_enabled(env: dict[str, str]) -> bool:
    value = str(env.get("MN_SYNCTHING_ENABLED") or os.getenv("MN_SYNCTHING_ENABLED") or "auto").strip().lower()
    return value not in {"0", "false", "no", "n", "off", "disabled"}

def _syncthing_required(env: dict[str, str]) -> bool:
    return _truthy_env(env.get("MN_SYNCTHING_REQUIRED") or os.getenv("MN_SYNCTHING_REQUIRED"))

def _syncthing_warn_or_fail(message: str, env: dict[str, str]) -> None:
    if _syncthing_required(env):
        console.print(f"[red]Error: {message}[/red]")
        raise typer.Exit(1)
    console.print(f"[yellow]Warning: {message}[/yellow]")

def _syncthing_image(env: dict[str, str]) -> str:
    return str(env.get("MN_SYNCTHING_IMAGE") or os.getenv("MN_SYNCTHING_IMAGE") or DEFAULT_SYNCTHING_IMAGE).strip()

def _syncthing_api_key(env: dict[str, str]) -> str:
    configured = str(env.get("MN_SYNCTHING_API_KEY") or os.getenv("MN_SYNCTHING_API_KEY") or "").strip()
    if configured:
        return configured
    try:
        existing = SYNCTHING_API_KEY_FILE.read_text(encoding="utf-8").strip()
        if existing:
            return existing
    except OSError:
        pass
    DIR.mkdir(parents=True, exist_ok=True)
    generated = secrets.token_urlsafe(32)
    write_private_text(SYNCTHING_API_KEY_FILE, f"{generated}\n")
    return generated

def _syncthing_host_shared_root(env: dict[str, str]) -> str:
    host_root, _runtime_root, _bundle_cache = _network_shared_storage_roots(env)
    return host_root

def _syncthing_folder_id(env: dict[str, str]) -> str:
    return str(env.get("MN_SYNCTHING_FOLDER_ID") or os.getenv("MN_SYNCTHING_FOLDER_ID") or SYNCTHING_FOLDER_ID).strip()

def _syncthing_folder_path(env: dict[str, str]) -> str:
    return str(env.get("MN_SYNCTHING_FOLDER_PATH") or os.getenv("MN_SYNCTHING_FOLDER_PATH") or SYNCTHING_FOLDER_PATH).strip()

def _runtime_compose_syncthing_override_file() -> Path:
    return RUNTIME_COMPOSE_FILE.parent / RUNTIME_SYNCTHING_OVERRIDE_FILE

def _compose_file_has_syncthing(path: Path) -> bool:
    try:
        return bool(re.search(r"(?m)^  syncthing:\s*$", path.read_text(encoding="utf-8")))
    except OSError:
        return False

def _write_runtime_compose_syncthing_override() -> Path:
    path = _runtime_compose_syncthing_override_file()
    override = """services:
  syncthing:
    image: ${MN_SYNCTHING_IMAGE:-syncthing/syncthing:latest}
    container_name: mirror-neuron-syncthing
    profiles:
      - syncthing
    restart: unless-stopped
    user: "0:0"
    environment:
      STGUIADDRESS: 0.0.0.0:8384
      STGUIAPIKEY: ${MN_SYNCTHING_API_KEY:-}
      STHOMEDIR: /var/syncthing/config
      MN_HOST_SHARED_STORAGE_ROOT: ${MN_HOST_SHARED_STORAGE_ROOT:-${MN_SHARED_STORAGE_ROOT:-${MN_HOST_SHARED_ARTIFACT_ROOT:-./mn/shared}}}
      MN_SYNCTHING_API_KEY: ${MN_SYNCTHING_API_KEY:-}
    ports:
      - "${MN_SYNCTHING_BIND_HOST:-0.0.0.0}:${MN_SYNCTHING_GUI_PORT:-58384}:8384/tcp"
      - "${MN_SYNCTHING_BIND_HOST:-0.0.0.0}:${MN_SYNCTHING_SYNC_PORT:-22000}:22000/tcp"
    volumes:
      - ${MN_HOST_HOME_DIR:-${MN_HOST_MN_DIR:-./mn}}/syncthing:/var/syncthing/config:rw
      - ${MN_HOST_SHARED_STORAGE_ROOT:-${MN_SHARED_STORAGE_ROOT:-${MN_HOST_SHARED_ARTIFACT_ROOT:-./mn/shared}}}:${MN_SYNCTHING_FOLDER_PATH:-/var/syncthing/MirrorNeuronShared}:rw
    networks:
      - runtime
"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(override, encoding="utf-8")
    return path

def _ensure_compose_syncthing_service_definition() -> bool:
    if not runtime_compose_available():
        return False
    if _compose_file_has_syncthing(RUNTIME_COMPOSE_FILE):
        return True
    if _compose_file_has_syncthing(_runtime_compose_syncthing_override_file()):
        return True
    _write_runtime_compose_syncthing_override()
    return True

def _start_compose_syncthing_service(env: dict[str, str], updates: dict[str, str]) -> None:
    _write_env_file_values(RUNTIME_COMPOSE_ENV, updates)
    compose_env = _compose_runtime_env({**env, **updates}, None)
    compose_env["COMPOSE_PROFILES"] = _compose_profiles_with(
        compose_env.get("COMPOSE_PROFILES"),
        "syncthing",
    )
    _remove_non_mirror_neuron_container(SYNCTHING_CONTAINER)
    subprocess.run(
        runtime_compose_cmd("up", "-d", SYNCTHING_COMPOSE_SERVICE),
        check=True,
        stdout=subprocess.DEVNULL,
        env=compose_env,
    )

def _resolve_syncthing_port(
    env: dict[str, str],
    *,
    key: str,
    default: int,
    bind_host: str,
    target_port: int,
) -> int:
    configured = _parse_configured_port(env.get(key) or os.getenv(key))
    current = _published_container_port(SYNCTHING_CONTAINER, target_port)
    if current and not configured:
        return current
    preferred = configured or default
    if _port_available_or_owned(bind_host, preferred, SYNCTHING_CONTAINER, target_port):
        return preferred
    if configured:
        console.print(f"[red]Error: {key}={configured} is already in use.[/red]")
        raise typer.Exit(1)
    for candidate in range(default + 1, default + 50):
        if _port_available_or_owned(bind_host, candidate, SYNCTHING_CONTAINER, target_port):
            return candidate
    console.print(f"[red]Error: No Syncthing port is available near {default}.[/red]")
    raise typer.Exit(1)

def _syncthing_request(
    host: str,
    port: int,
    api_key: str,
    method: str,
    path: str,
    body: Optional[dict[str, Any]] = None,
    *,
    timeout: float = 5.0,
) -> Any:
    payload = None
    headers = {"X-API-Key": api_key}
    if body is not None:
        payload = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    url = f"http://{host}:{port}{path}"
    request = urllib.request.Request(url, data=payload, headers=headers, method=method)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        raw = response.read()
    if not raw:
        return None
    try:
        return json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError:
        return raw.decode("utf-8", errors="replace")

def _wait_for_syncthing_api(host: str, port: int, api_key: str, *, timeout_seconds: float = 20.0) -> None:
    deadline = time.time() + timeout_seconds
    last_error: Optional[Exception] = None
    while time.time() < deadline:
        try:
            _syncthing_request(host, port, api_key, "GET", "/rest/system/ping", timeout=1.0)
            return
        except Exception as exc:
            last_error = exc
            time.sleep(0.5)
    raise RuntimeError(f"Syncthing API at {host}:{port} did not become ready: {last_error}")

def _syncthing_status(host: str, port: int, api_key: str) -> dict[str, Any]:
    status = _syncthing_request(host, port, api_key, "GET", "/rest/system/status")
    return status if isinstance(status, dict) else {}

def _ensure_syncthing_folder(info: dict[str, Any], peers: tuple[dict[str, Any], ...] = ()) -> None:
    if not info.get("enabled"):
        return
    host = str(info["api_host"])
    port = int(info["gui_port"])
    api_key = str(info["api_key"])
    folder_id = str(info["folder_id"])
    folder_path = str(info["folder_path"])
    device_id = str(info["device_id"])

    config = _syncthing_request(host, port, api_key, "GET", "/rest/config")
    if not isinstance(config, dict):
        return

    devices = config.setdefault("devices", [])
    folders = config.setdefault("folders", [])
    peer_devices = []
    for peer in peers:
        peer_id = str(peer.get("device_id") or "").strip()
        if not peer_id:
            continue
        peer_devices.append({"deviceID": peer_id})
        existing = next((item for item in devices if item.get("deviceID") == peer_id), None)
        addresses = [f"tcp://{peer.get('host')}:{peer.get('sync_port')}"] if peer.get("host") and peer.get("sync_port") else ["dynamic"]
        if existing is None:
            devices.append({"deviceID": peer_id, "name": str(peer.get("name") or peer_id), "addresses": addresses})
        else:
            existing["addresses"] = addresses

    folder = next((item for item in folders if item.get("id") == folder_id), None)
    folder_devices = [{"deviceID": device_id}, *peer_devices]
    if folder is None:
        folders.append(
            {
                "id": folder_id,
                "label": SYNCTHING_FOLDER_LABEL,
                "path": folder_path,
                "type": "sendreceive",
                "rescanIntervalS": 15,
                "devices": folder_devices,
            }
        )
    else:
        folder["path"] = folder_path
        folder["type"] = "sendreceive"
        existing_ids = {str(item.get("deviceID") or "") for item in folder.get("devices", [])}
        for item in folder_devices:
            if item["deviceID"] not in existing_ids:
                folder.setdefault("devices", []).append(item)

    _syncthing_request(host, port, api_key, "PUT", "/rest/config", config)
    try:
        _syncthing_request(host, port, api_key, "POST", "/rest/system/restart", timeout=2.0)
    except Exception:
        pass
    try:
        _wait_for_syncthing_api(host, port, api_key, timeout_seconds=15.0)
    except Exception:
        pass

def _ensure_syncthing_for_runtime(env: dict[str, str], *, advertised_host: str) -> dict[str, str]:
    if not _syncthing_enabled(env):
        return env
    host_root = _syncthing_host_shared_root(env)
    try:
        Path(host_root).expanduser().mkdir(parents=True, exist_ok=True)
        SYNCTHING_CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        _syncthing_warn_or_fail(f"could not create Syncthing shared-storage directories: {exc}", env)
        return env

    api_key = _syncthing_api_key(env)
    bind_host = _network_publish_host(advertised_host)
    gui_port = _resolve_syncthing_port(
        env,
        key="MN_SYNCTHING_GUI_PORT",
        default=DEFAULT_SYNCTHING_GUI_PORT,
        bind_host=bind_host,
        target_port=SYNCTHING_CONTAINER_GUI_PORT,
    )
    sync_port = _resolve_syncthing_port(
        env,
        key="MN_SYNCTHING_SYNC_PORT",
        default=DEFAULT_SYNCTHING_SYNC_PORT,
        bind_host=bind_host,
        target_port=SYNCTHING_CONTAINER_SYNC_PORT,
    )
    image = _syncthing_image(env)
    folder_id = _syncthing_folder_id(env)
    folder_path = _syncthing_folder_path(env)
    updates = {
        "MN_SYNCTHING_ENABLED": "auto",
        "MN_SYNCTHING_IMAGE": image,
        "MN_SYNCTHING_API_KEY": api_key,
        "MN_SYNCTHING_ADVERTISE_HOST": advertised_host,
        "MN_SYNCTHING_BIND_HOST": bind_host,
        "MN_SYNCTHING_GUI_PORT": str(gui_port),
        "MN_SYNCTHING_SYNC_PORT": str(sync_port),
        "MN_SYNCTHING_FOLDER_ID": folder_id,
        "MN_SYNCTHING_FOLDER_PATH": folder_path,
    }

    if _ensure_compose_syncthing_service_definition():
        try:
            _start_compose_syncthing_service(env, updates)
        except Exception as exc:
            _syncthing_warn_or_fail(f"could not start Compose Syncthing sidecar: {exc}", env)
            return env
    else:
        recreate = not _docker_container_running(SYNCTHING_CONTAINER)
        if not recreate:
            current_root = _docker_container_env_value(SYNCTHING_CONTAINER, "MN_HOST_SHARED_STORAGE_ROOT")
            current_api_key = _docker_container_env_value(SYNCTHING_CONTAINER, "MN_SYNCTHING_API_KEY")
            current_user = _docker_container_user(SYNCTHING_CONTAINER)
            if current_root != host_root or current_api_key != api_key or current_user != "0:0":
                recreate = True
        if recreate:
            subprocess.run(["docker", "rm", "-f", SYNCTHING_CONTAINER], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
            command = [
                "docker",
                "run",
                "-d",
                "--name",
                SYNCTHING_CONTAINER,
                "--restart",
                "unless-stopped",
                "--user",
                "0:0",
                "-e",
                "STGUIADDRESS=0.0.0.0:8384",
                "-e",
                f"STGUIAPIKEY={api_key}",
                "-e",
                "STHOMEDIR=/var/syncthing/config",
                "-e",
                f"MN_HOST_SHARED_STORAGE_ROOT={host_root}",
                "-e",
                f"MN_SYNCTHING_API_KEY={api_key}",
                "-p",
                f"{bind_host}:{gui_port}:{SYNCTHING_CONTAINER_GUI_PORT}/tcp",
                "-p",
                f"{bind_host}:{sync_port}:{SYNCTHING_CONTAINER_SYNC_PORT}/tcp",
                "-v",
                f"{SYNCTHING_CONFIG_DIR}:/var/syncthing/config:rw",
                "-v",
                f"{host_root}:{folder_path}:rw",
                image,
            ]
            subprocess.run(command, check=True, stdout=subprocess.DEVNULL)
        else:
            subprocess.run(["docker", "start", SYNCTHING_CONTAINER], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    try:
        _wait_for_syncthing_api("127.0.0.1", gui_port, api_key)
        status = _syncthing_status("127.0.0.1", gui_port, api_key)
    except Exception as exc:
        _syncthing_warn_or_fail(f"Syncthing shared-storage sidecar is not reachable: {exc}", env)
        return env

    device_id = str(status.get("myID") or env.get("MN_SYNCTHING_DEVICE_ID") or "").strip()
    updates["MN_SYNCTHING_DEVICE_ID"] = device_id
    env.update(updates)
    if runtime_compose_available():
        _write_env_file_values(RUNTIME_COMPOSE_ENV, updates)
    if device_id:
        _ensure_syncthing_folder(_syncthing_node_info(env, advertised_host))
    console.print(f"=> Syncthing shared storage ready at {advertised_host}:{sync_port} ({host_root})")
    return env

def _syncthing_node_info(env: dict[str, str], advertised_host: str) -> dict[str, Any]:
    if not _syncthing_enabled(env):
        return {"enabled": False}
    return {
        "enabled": True,
        "device_id": str(env.get("MN_SYNCTHING_DEVICE_ID") or "").strip(),
        "api_key": str(env.get("MN_SYNCTHING_API_KEY") or "").strip(),
        "api_host": "127.0.0.1",
        "host": str(env.get("MN_SYNCTHING_ADVERTISE_HOST") or advertised_host).strip(),
        "gui_port": _parse_configured_port(env.get("MN_SYNCTHING_GUI_PORT")) or DEFAULT_SYNCTHING_GUI_PORT,
        "sync_port": _parse_configured_port(env.get("MN_SYNCTHING_SYNC_PORT")) or DEFAULT_SYNCTHING_SYNC_PORT,
        "folder_id": _syncthing_folder_id(env),
        "folder_path": _syncthing_folder_path(env),
    }

def _syncthing_info_from_handshake(handshake: Optional[dict]) -> dict[str, Any]:
    if not isinstance(handshake, dict):
        return {}
    node_info = handshake.get("node_info")
    if not isinstance(node_info, dict):
        return {}
    syncthing = node_info.get("syncthing")
    return syncthing if isinstance(syncthing, dict) else {}

def _connect_syncthing_peers(local_info: dict[str, Any], remote_info: dict[str, Any]) -> bool:
    if not local_info.get("enabled") or not remote_info.get("enabled"):
        return False
    if not local_info.get("device_id") or not remote_info.get("device_id"):
        return False
    try:
        _ensure_syncthing_folder(local_info, (remote_info,))
        remote_api_info = dict(remote_info)
        remote_api_info["api_host"] = str(remote_info.get("host") or "")
        if remote_api_info["api_host"]:
            _ensure_syncthing_folder(remote_api_info, (local_info,))
        return True
    except Exception as exc:
        console.print(f"[yellow]Warning: could not connect Syncthing shared-storage peers: {exc}[/yellow]")
        return False

def _compose_shared_storage_env_changed(updates: dict[str, str]) -> bool:
    if not updates:
        return False
    current = _read_env_file(RUNTIME_COMPOSE_ENV)
    return any(str(current.get(key) or "").strip() != str(value or "").strip() for key, value in updates.items())

def _persist_compose_shared_storage_env(updates: dict[str, str]) -> bool:
    if not updates or not runtime_compose_available():
        return False
    changed = _compose_shared_storage_env_changed(updates)
    _write_env_file_values(RUNTIME_COMPOSE_ENV, updates)
    return changed

def _recreate_compose_core_for_shared_storage(env: dict[str, str]) -> None:
    if not runtime_compose_available():
        return
    console.print("=> Recreating MirrorNeuron core so shared-storage changes are visible...")
    try:
        subprocess.run(
            runtime_compose_cmd("up", "-d", "--force-recreate", "mirror-neuron-core"),
            check=True,
            stdout=subprocess.DEVNULL,
            env=_compose_runtime_env(env, None),
        )
    except (FileNotFoundError, subprocess.CalledProcessError) as exc:
        from mn_sdk.errors import AppError

        raise AppError(
            "MN_EXECUTION_FAILED",
            "Could not recreate MirrorNeuron core after shared-storage settings changed.",
            internal_message=str(exc),
            hint="Run 'mn runtime stop' and then restart the runtime after shared-storage settings are corrected.",
            exit_code=1,
            http_status=500,
            cause=exc,
        ) from exc

def _runtime_grpc_tokens_from_running_container() -> dict[str, str]:
    return {
        "MN_GRPC_AUTH_TOKEN": FIXED_GRPC_AUTH_TOKEN,
        GRPC_ADMIN_TOKEN_ENV: FIXED_GRPC_ADMIN_TOKEN,
    }

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
    write_private_text(NETWORK_TOKEN_FILE, f"{token}\n")

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
    write_private_text(alias_file, f"{alias}\n")

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
    cluster_node_names = _split_env_list(cluster_nodes)
    if _generated_network_node_name(node_name) and node_name in cluster_node_names:
        return any(node != node_name for node in cluster_node_names)
    return (
        _generated_network_node_name(cluster_nodes)
        and _generated_network_node_name(node_name)
        and cluster_nodes != node_name
    )

def _cluster_nodes_with(*node_names: object) -> str:
    nodes: list[str] = []
    for node_name in node_names:
        for part in _split_env_list(node_name):
            if part and part not in nodes:
                nodes.append(part)
    return ",".join(nodes)

def _joined_cluster_seed_node(env: dict[str, str], local_node_name: str) -> str:
    for node_name in _split_env_list(env.get("MN_CLUSTER_NODES")):
        if node_name != local_node_name:
            return node_name
    return str(env.get("MN_CLUSTER_NODES") or "").strip()

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

def _native_sdk_grpc_node_info(
    advertised_host: str,
    env: Optional[dict[str, str]] = None,
) -> dict[str, object]:
    values = env or _runtime_base_env(runtime_compose_available())
    host = str(
        values.get("MN_NATIVE_SDK_GRPC_ADVERTISE_HOST")
        or values.get("MN_NETWORK_ADVERTISE_HOST")
        or advertised_host
        or ""
    ).strip()
    port = _parse_port(
        values.get("MN_NATIVE_SDK_GRPC_ADVERTISE_PORT")
        or values.get("MN_NATIVE_SDK_GRPC_PORT")
        or DEFAULT_NATIVE_SDK_GRPC_PORT,
        int(DEFAULT_NATIVE_SDK_GRPC_PORT),
    )
    bind_host = str(values.get("MN_NATIVE_SDK_GRPC_HOST") or DEFAULT_NATIVE_SDK_GRPC_HOST).strip()
    target = f"{host}:{port}" if host and port else ""
    return {
        "enabled": bool(host and port and _native_sdk_grpc_command() is not None),
        "host": host,
        "port": port,
        "target": target,
        "bind_host": bind_host,
    }

def _handshake_node_info(
    local_host: str,
    node_name: Optional[str] = None,
    grpc_port: Optional[int | str] = None,
) -> dict[str, object]:
    hostname = ""
    try:
        hostname = socket.gethostname().strip()
    except OSError:
        pass
    runtime_env = _runtime_base_env(runtime_compose_available())
    shared_env = _shared_storage_env_from_runtime_env(runtime_env)
    grpc_port_value = _parse_port(
        grpc_port
        or os.getenv("MN_GRPC_ADVERTISE_PORT")
        or os.getenv("MN_GRPC_PORT")
        or DEFAULT_GRPC_PORT,
        int(DEFAULT_GRPC_PORT),
    )

    return {
        "node_name": node_name or _network_node_name(local_host),
        "display_name": _node_display_name(),
        "hostname": hostname,
        "grpc_host": local_host,
        "grpc_port": grpc_port_value,
        "native_sdk_grpc": _native_sdk_grpc_node_info(local_host, runtime_env),
        "gpu_count": _detect_host_gpu_count(),
        "host_shared_storage_root": shared_env["MN_HOST_SHARED_STORAGE_ROOT"],
        "runtime_shared_storage_root": shared_env["MN_RUNTIME_SHARED_STORAGE_ROOT"],
        "syncthing": _syncthing_node_info(shared_env, local_host),
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
        _handshake_node_info(
            local_host,
            node_name=advertised_node_name,
            grpc_port=os.getenv("MN_GRPC_ADVERTISE_PORT") or os.getenv("MN_GRPC_PORT"),
        )
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
        _raise_join_handshake_error(exc, target)

    if _node_name_unset(handshake.get("node_name")):
        handshake["node_name"] = _network_node_name(seed_host)
    return handshake

def _raise_join_handshake_error(exc: Exception, target: str) -> None:
    from mn_sdk.errors import AppError

    text = str(exc)
    code = None
    code_fn = getattr(exc, "code", None)
    if callable(code_fn):
        try:
            code = code_fn()
        except Exception:
            code = None
    code_name = str(getattr(code, "name", code or "")).upper()

    if "already join a cluster" in text or code_name == "ALREADY_EXISTS":
        raise AppError(
            "MN_ALREADY_JOINED",
            "already join a cluster",
            internal_message=text,
            hint="Stop the worker runtime on that box before joining it to a different master.",
            exit_code=1,
            http_status=409,
            cause=exc,
        ) from exc

    raise AppError(
        "MN_EXECUTION_FAILED",
        f"Could not join MirrorNeuron node at {target}.",
        internal_message=text,
        hint="Check the host, gRPC port, and token printed by 'mn runtime start' on the main box.",
        exit_code=1,
        http_status=500,
        cause=exc,
    ) from exc

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

def _running_core_has_stale_grpc_tokens(container_name: str = "mirror-neuron-core") -> bool:
    expected = {
        "MN_GRPC_AUTH_TOKEN": FIXED_GRPC_AUTH_TOKEN,
        GRPC_ADMIN_TOKEN_ENV: FIXED_GRPC_ADMIN_TOKEN,
    }
    return any(_docker_container_env_value(container_name, key) != value for key, value in expected.items())

def _docker_container_user(name: str) -> Optional[str]:
    try:
        result = subprocess.run(
            ["docker", "inspect", "-f", "{{.Config.User}}", name],
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        return None
    if result.returncode != 0:
        return None
    return result.stdout.strip()

def _ensure_network_docker_network(mode: str = "bridge", name: Optional[str] = None) -> None:
    _ensure_docker_network(mode, name or NETWORK_DOCKER_NETWORK)

def _network_redis_url(token: str, redis_host: str, redis_port: int, password: Optional[str] = None) -> str:
    password = password or _resolve_redis_password()
    return f"redis://:{password}@{redis_host}:{redis_port}/0"

REDIS_HA_ENV_KEYS = (
    "MN_REDIS_HA_MODE",
    "MN_REDIS_SENTINELS",
    "MN_REDIS_SENTINEL_MASTER",
    "MN_REDIS_SENTINEL_HOST_MAP",
    "MN_REDIS_DB",
    "MN_REDIS_USERNAME",
    "MN_REDIS_PASSWORD",
    "MN_REDIS_SENTINEL_USERNAME",
    "MN_REDIS_SENTINEL_PASSWORD",
    "MN_REDIS_SENTINEL_PORT",
    "MN_REDIS_WAIT_REPLICAS",
    "MN_REDIS_WAIT_TIMEOUT_MS",
    "MN_REDIS_RECONNECT_ATTEMPTS",
    "MN_REDIS_RECONNECT_BACKOFF_MS",
    "MN_REDIS_RECONNECT_MAX_BACKOFF_MS",
)

def _redis_ha_mode(env: dict[str, str], *, cluster: bool = False) -> str:
    configured = str(os.getenv("MN_REDIS_HA_MODE") or env.get("MN_REDIS_HA_MODE") or "").strip().lower()
    if configured:
        return configured
    return "sentinel" if cluster else "single"

def _redis_ha_enabled(env: dict[str, str], *, cluster: bool = False) -> bool:
    return _redis_ha_mode(env, cluster=cluster) == "sentinel"

def _redis_sentinel_port(env: dict[str, str]) -> int:
    configured = (
        os.getenv("MN_REDIS_SENTINEL_PORT", "").strip()
        or str(env.get("MN_REDIS_SENTINEL_PORT") or "").strip()
    )
    return _parse_configured_port(configured) or DEFAULT_REDIS_SENTINEL_PORT

def _redis_sentinel_master(env: dict[str, str]) -> str:
    return (
        os.getenv("MN_REDIS_SENTINEL_MASTER", "").strip()
        or str(env.get("MN_REDIS_SENTINEL_MASTER") or "").strip()
        or "mirror-neuron"
    )

def _redis_sentinels(env: dict[str, str], advertised_host: str) -> str:
    configured = (
        os.getenv("MN_REDIS_SENTINELS", "").strip()
        or str(env.get("MN_REDIS_SENTINELS") or "").strip()
    )
    if configured:
        return configured
    return f"{advertised_host}:{_redis_sentinel_port(env)}"

def _ensure_redis_ha_settings(
    env: dict[str, str],
    *,
    advertised_host: str,
    cluster: bool,
) -> dict[str, str]:
    adjusted = dict(env)
    mode = _redis_ha_mode(adjusted, cluster=cluster)
    adjusted["MN_REDIS_HA_MODE"] = mode
    if mode != "sentinel":
        return adjusted

    redis_password = str(adjusted.get("MN_REDIS_PASSWORD") or "").strip() or _resolve_redis_password(adjusted)
    adjusted["MN_REDIS_PASSWORD"] = redis_password
    adjusted.setdefault("MN_REDIS_SENTINELS", _redis_sentinels(adjusted, advertised_host))
    adjusted.setdefault("MN_REDIS_SENTINEL_MASTER", _redis_sentinel_master(adjusted))
    adjusted.setdefault("MN_REDIS_DB", os.getenv("MN_REDIS_DB", "").strip() or "0")
    adjusted.setdefault("MN_REDIS_SENTINEL_PORT", str(_redis_sentinel_port(adjusted)))
    adjusted.setdefault("MN_REDIS_SENTINEL_PASSWORD", os.getenv("MN_REDIS_SENTINEL_PASSWORD", "").strip() or redis_password)
    adjusted.setdefault("MN_REDIS_WAIT_REPLICAS", os.getenv("MN_REDIS_WAIT_REPLICAS", "").strip() or "1")
    adjusted.setdefault("MN_REDIS_WAIT_TIMEOUT_MS", os.getenv("MN_REDIS_WAIT_TIMEOUT_MS", "").strip() or "1000")
    for key in REDIS_HA_ENV_KEYS:
        value = os.getenv(key, "").strip()
        if value:
            adjusted[key] = value
    return adjusted

def _redis_ha_env_from_handshake(handshake: Optional[dict]) -> dict[str, str]:
    if not isinstance(handshake, dict):
        return {}
    node_info = handshake.get("node_info")
    redis_ha = node_info.get("redis_ha") if isinstance(node_info, dict) else None
    if not isinstance(redis_ha, dict):
        return {}
    updates: dict[str, str] = {}
    mapping = {
        "mode": "MN_REDIS_HA_MODE",
        "sentinels": "MN_REDIS_SENTINELS",
        "sentinel_master": "MN_REDIS_SENTINEL_MASTER",
        "sentinel_host_map": "MN_REDIS_SENTINEL_HOST_MAP",
        "db": "MN_REDIS_DB",
        "sentinel_port": "MN_REDIS_SENTINEL_PORT",
        "wait_replicas": "MN_REDIS_WAIT_REPLICAS",
        "wait_timeout_ms": "MN_REDIS_WAIT_TIMEOUT_MS",
        "reconnect_attempts": "MN_REDIS_RECONNECT_ATTEMPTS",
        "reconnect_backoff_ms": "MN_REDIS_RECONNECT_BACKOFF_MS",
        "reconnect_max_backoff_ms": "MN_REDIS_RECONNECT_MAX_BACKOFF_MS",
    }
    for source, target in mapping.items():
        value = str(redis_ha.get(source) or "").strip()
        if value:
            updates[target] = value
    if updates.get("MN_REDIS_HA_MODE") == "sentinel":
        redis_password = _redis_password_from_url(str(handshake.get("redis_url") or ""))
        if redis_password:
            updates.setdefault("MN_REDIS_PASSWORD", redis_password)
            updates.setdefault("MN_REDIS_SENTINEL_PASSWORD", redis_password)
    return updates

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
    host_shared_storage_root, runtime_shared_storage_root, runtime_bundle_cache_dir = (
        _network_shared_storage_roots(env)
    )
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
            "MN_HOST_SHARED_STORAGE_ROOT": host_shared_storage_root,
            "MN_SHARED_STORAGE_ROOT": runtime_shared_storage_root,
            "MN_RUNTIME_SHARED_STORAGE_ROOT": runtime_shared_storage_root,
            "MN_CONTAINER_SHARED_STORAGE_ROOT": runtime_shared_storage_root,
            "MN_BUNDLE_CACHE_DIR": runtime_bundle_cache_dir,
            "MN_DIST_PORT": str(dist_port),
            "MN_COOKIE": _derive_network_secret(token, "cookie"),
            "MN_GRPC_AUTH_TOKEN": FIXED_GRPC_AUTH_TOKEN,
            GRPC_ADMIN_TOKEN_ENV: FIXED_GRPC_ADMIN_TOKEN,
            "MN_NATIVE_SDK_GRPC_HOST": env.get("MN_NATIVE_SDK_GRPC_HOST") or "0.0.0.0",
            "MN_NATIVE_SDK_GRPC_PORT": _valid_port_text(
                str(env.get("MN_NATIVE_SDK_GRPC_PORT") or DEFAULT_NATIVE_SDK_GRPC_PORT),
                DEFAULT_NATIVE_SDK_GRPC_PORT,
            ),
            "MN_NATIVE_SDK_GRPC_ADVERTISE_HOST": env.get("MN_NATIVE_SDK_GRPC_ADVERTISE_HOST") or host,
            "MN_NATIVE_SDK_GRPC_ADVERTISE_PORT": _valid_port_text(
                str(
                    env.get("MN_NATIVE_SDK_GRPC_ADVERTISE_PORT")
                    or env.get("MN_NATIVE_SDK_GRPC_PORT")
                    or DEFAULT_NATIVE_SDK_GRPC_PORT
                ),
                DEFAULT_NATIVE_SDK_GRPC_PORT,
            ),
            "MN_LITELLM_GATEWAY_BIND_HOST": env.get("MN_LITELLM_GATEWAY_BIND_HOST") or "0.0.0.0",
            "MN_LITELLM_GATEWAY_PORT": _valid_port_text(
                str(env.get("MN_LITELLM_GATEWAY_PORT") or "4000"),
                "4000",
            ),
            "ERL_EPMD_ADDRESS": "0.0.0.0",
            "ERL_EPMD_PORT": str(epmd_port),
            "ERL_AFLAGS": _erl_aflags(dist_port),
        }
    )
    env["MN_NATIVE_SDK_GRPC_TARGET"] = env.get("MN_NATIVE_SDK_GRPC_TARGET") or _network_native_sdk_grpc_target(
        env,
        docker_network_mode=docker_network_mode,
    )
    env = _ensure_redis_ha_settings(env, advertised_host=host, cluster=True)
    env = _ensure_node_advertisement_settings(env)
    return env


def _network_native_sdk_grpc_target(env: dict[str, str], *, docker_network_mode: str) -> str:
    native_port = _valid_port_text(
        str(env.get("MN_NATIVE_SDK_GRPC_PORT") or DEFAULT_NATIVE_SDK_GRPC_PORT),
        DEFAULT_NATIVE_SDK_GRPC_PORT,
    )
    return f"{DEFAULT_NATIVE_SDK_GRPC_TARGET_HOST}:{native_port}"


def _persist_worker_compose_foundation_env(env: dict[str, str]) -> None:
    if not runtime_compose_available():
        return
    updates = {
        "MN_NETWORK_ADVERTISE_HOST": env.get("MN_NETWORK_ADVERTISE_HOST", ""),
        "MN_NATIVE_SDK_GRPC_HOST": env.get("MN_NATIVE_SDK_GRPC_HOST", DEFAULT_NATIVE_SDK_GRPC_HOST),
        "MN_NATIVE_SDK_GRPC_PORT": env.get("MN_NATIVE_SDK_GRPC_PORT", DEFAULT_NATIVE_SDK_GRPC_PORT),
        "MN_NATIVE_SDK_GRPC_ADVERTISE_HOST": env.get("MN_NATIVE_SDK_GRPC_ADVERTISE_HOST", ""),
        "MN_NATIVE_SDK_GRPC_ADVERTISE_PORT": env.get(
            "MN_NATIVE_SDK_GRPC_ADVERTISE_PORT",
            env.get("MN_NATIVE_SDK_GRPC_PORT", DEFAULT_NATIVE_SDK_GRPC_PORT),
        ),
        "MN_NATIVE_SDK_GRPC_TARGET": env.get("MN_NATIVE_SDK_GRPC_TARGET", ""),
        "MN_LITELLM_GATEWAY_BIND_HOST": env.get("MN_LITELLM_GATEWAY_BIND_HOST", ""),
        "MN_LITELLM_GATEWAY_PORT": env.get("MN_LITELLM_GATEWAY_PORT", "4000"),
        "MN_NODE_NAME": env.get("MN_NODE_NAME", ""),
        "MN_NODE_ROLE": env.get("MN_NODE_ROLE", "runtime"),
        "MN_DOCKER_NETWORK_MODE": env.get("MN_DOCKER_NETWORK_MODE", "disabled"),
        "MN_DOCKER_NETWORK_NAME": env.get("MN_DOCKER_NETWORK_NAME", ""),
    }
    _write_env_file_values(RUNTIME_COMPOSE_ENV, updates)


def _start_worker_compose_foundation_services(env: dict[str, str]) -> None:
    if not runtime_compose_available():
        return
    services = ["mn-litellm-proxy"]
    try:
        compose_text = RUNTIME_COMPOSE_FILE.read_text(encoding="utf-8")
    except OSError:
        compose_text = ""
    if "mn-native-sdk-grpc:" in compose_text:
        services.insert(0, "mn-native-sdk-grpc")
    available_services = [service for service in services if f"{service}:" in compose_text]
    if not available_services:
        return
    _persist_worker_compose_foundation_env(env)
    if "mn-litellm-proxy" in available_services:
        _ensure_litellm_gateway_host_config()
    try:
        subprocess.run(
            runtime_compose_cmd("up", "-d", *available_services),
            check=True,
            stdout=subprocess.DEVNULL,
            env={**os.environ.copy(), **env},
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        console.print(
            "[yellow]=> Warning: worker Compose gateway services did not start; "
            "remote LLM routing may be unavailable on this node.[/yellow]"
        )


def _network_shared_storage_roots(env: dict[str, str]) -> tuple[str, str, str]:
    runtime_root = str(
        env.get("MN_RUNTIME_SHARED_STORAGE_ROOT")
        or env.get("MN_CONTAINER_SHARED_STORAGE_ROOT")
        or DEFAULT_RUNTIME_SHARED_STORAGE_ROOT
    ).strip() or DEFAULT_RUNTIME_SHARED_STORAGE_ROOT
    host_home_dir = Path(str(env.get("MN_HOST_HOME_DIR") or DIR)).expanduser()
    host_root = str(env.get("MN_HOST_SHARED_STORAGE_ROOT") or "").strip()
    legacy_shared_root = str(env.get("MN_SHARED_STORAGE_ROOT") or "").strip()
    if not host_root and legacy_shared_root and legacy_shared_root != runtime_root:
        host_root = legacy_shared_root
    if not host_root:
        host_root = str(host_home_dir / "shared")
    bundle_cache_dir = str(
        env.get("MN_BUNDLE_CACHE_DIR")
        or f"{runtime_root.rstrip('/')}/bundle_cache"
    ).strip()
    return str(Path(host_root).expanduser()), runtime_root, bundle_cache_dir

def _docker_env_args(env: dict[str, str]) -> list[str]:
    args: list[str] = []
    for key in sorted(env):
        if key.startswith("MN_") or key in {"ERL_AFLAGS", "ERL_EPMD_ADDRESS", "ERL_EPMD_PORT"}:
            args.extend(["-e", f"{key}={env[key]}"])
    return args

def _network_redis_image() -> str:
    return os.getenv("MN_REDIS_IMAGE", "").strip() or DEFAULT_REDIS_IMAGE

def _docker_host_socket() -> Optional[Path]:
    candidates = [
        os.getenv("DOCKER_HOST_SOCKET", "").strip(),
        "/var/run/docker.sock",
        str(Path.home() / ".docker" / "run" / "docker.sock"),
    ]
    for candidate in candidates:
        if not candidate:
            continue
        path = Path(candidate).expanduser()
        if path.exists():
            return path
    return None

def _network_core_bind_args(env: Optional[dict[str, str]] = None) -> list[str]:
    args: list[str] = []
    env = env or os.environ
    host_shared_storage_root, runtime_shared_storage_root, _runtime_bundle_cache_dir = (
        _network_shared_storage_roots(env)
    )
    shared_dir = Path(host_shared_storage_root)
    shared_dir.mkdir(parents=True, exist_ok=True)

    for host_path, container_path in (
        (DIR, "/root/.mn"),
        (DIR, "/opt/mirror_neuron/.mn"),
        (DIR, str(DIR)),
        (shared_dir, runtime_shared_storage_root),
        (shared_dir, "/opt/mirror_neuron/.mn/shared"),
    ):
        args.extend(["-v", f"{host_path}:{container_path}:rw"])

    args.extend(_docker_worker_bind_args())
    return args

def _docker_worker_bind_args() -> list[str]:
    args: list[str] = []
    socket_path = _docker_host_socket()
    if socket_path is not None:
        args.extend(["-v", f"{socket_path}:/var/run/docker.sock:rw"])
        docker_cli = shutil.which("docker") if sys.platform.startswith("linux") else None
        if docker_cli:
            args.extend(["-v", f"{docker_cli}:/usr/local/bin/docker:ro"])
    return args

def _start_network_redis(
    host: str,
    redis_port: Optional[int],
    token: str,
    *,
    redis_password: Optional[str] = None,
    docker_network_mode: str,
    docker_network_name: str,
    redis_alias: str,
    publish_host_port: bool = False,
) -> None:
    subprocess.run(["docker", "rm", "-f", NETWORK_REDIS_CONTAINER], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
    publish_host = _network_publish_host(host)
    password = redis_password or _resolve_redis_password()
    data_dir = DIR / "network-redis"
    data_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        "docker",
        "run",
        "-d",
        "--name",
        NETWORK_REDIS_CONTAINER,
        *_docker_network_run_args(docker_network_mode, docker_network_name, redis_alias),
        "-e",
        f"MN_REDIS_PASSWORD={password}",
        "-v",
        f"{data_dir}:/data",
        _network_redis_image(),
        "sh",
        "-c",
        (
            "exec redis-server --appendonly yes "
            "--requirepass \"$MN_REDIS_PASSWORD\" "
            "--masterauth \"$MN_REDIS_PASSWORD\""
        ),
    ]
    if publish_host_port:
        volume_index = cmd.index("-v")
        cmd[volume_index:volume_index] = ["-p", f"{publish_host}:{redis_port or REDIS_CONTAINER_PORT}:6379"]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL)
    _force_network_redis_primary()


def _force_network_redis_primary() -> None:
    command = [
        "docker",
        "exec",
        NETWORK_REDIS_CONTAINER,
        "sh",
        "-c",
        (
            "until redis-cli -a \"$MN_REDIS_PASSWORD\" --no-auth-warning PING >/dev/null 2>&1; do "
            "sleep 0.2; "
            "done; "
            "redis-cli -a \"$MN_REDIS_PASSWORD\" --no-auth-warning REPLICAOF NO ONE >/dev/null; "
            "redis-cli -a \"$MN_REDIS_PASSWORD\" --no-auth-warning SET mn:network-redis:write-probe ok EX 30 >/dev/null; "
            "redis-cli -a \"$MN_REDIS_PASSWORD\" --no-auth-warning CONFIG REWRITE >/dev/null 2>&1 || true"
        ),
    ]
    subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def _force_compose_redis_primary() -> None:
    command = [
        "docker",
        "exec",
        COMPOSE_REDIS_CONTAINER,
        "sh",
        "-c",
        (
            "until redis-cli -a \"$MN_REDIS_PASSWORD\" --no-auth-warning PING >/dev/null 2>&1; do "
            "sleep 0.2; "
            "done; "
            "redis-cli -a \"$MN_REDIS_PASSWORD\" --no-auth-warning REPLICAOF NO ONE >/dev/null; "
            "redis-cli -a \"$MN_REDIS_PASSWORD\" --no-auth-warning SET mn:compose-redis:write-probe ok EX 30 >/dev/null; "
            "redis-cli -a \"$MN_REDIS_PASSWORD\" --no-auth-warning CONFIG REWRITE >/dev/null 2>&1 || true"
        ),
    ]
    subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def _redis_conf_quote(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'

def _start_redis_sentinel(
    *,
    container_name: str,
    data_dir: Path,
    advertised_host: str,
    redis_host: str,
    redis_port: int,
    env: dict[str, str],
    docker_network_mode: str,
    docker_network_name: str,
    sentinel_alias: str,
    publish_host_port: bool,
) -> None:
    if not _redis_ha_enabled(env, cluster=True):
        subprocess.run(["docker", "rm", "-f", container_name], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
        return

    subprocess.run(["docker", "rm", "-f", container_name], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
    data_dir.mkdir(parents=True, exist_ok=True)
    sentinel_port = _redis_sentinel_port(env)
    redis_password = str(env.get("MN_REDIS_PASSWORD") or _resolve_redis_password(env))
    sentinel_password = str(env.get("MN_REDIS_SENTINEL_PASSWORD") or redis_password)
    master_name = _redis_sentinel_master(env)
    quorum = str(env.get("MN_REDIS_SENTINEL_QUORUM") or os.getenv("MN_REDIS_SENTINEL_QUORUM") or "1")
    publish_host = _network_publish_host(advertised_host)
    sentinel_conf = "\n".join(
        [
            "port 26379",
            "bind 0.0.0.0",
            "protected-mode no",
            "dir /data",
            "sentinel resolve-hostnames yes",
            "sentinel announce-hostnames no",
            f"sentinel announce-ip {advertised_host}",
            f"sentinel announce-port {sentinel_port}",
            f"requirepass {_redis_conf_quote(sentinel_password)}",
            f"sentinel sentinel-pass {_redis_conf_quote(sentinel_password)}",
            f"sentinel monitor {master_name} {redis_host} {redis_port} {quorum}",
            f"sentinel auth-pass {master_name} {_redis_conf_quote(redis_password)}",
            f"sentinel down-after-milliseconds {master_name} 5000",
            f"sentinel failover-timeout {master_name} 60000",
            f"sentinel parallel-syncs {master_name} 1",
            "",
        ]
    )
    cmd = [
        "docker",
        "run",
        "-d",
        "--name",
        container_name,
        *_docker_network_run_args(docker_network_mode, docker_network_name, sentinel_alias),
        "-v",
        f"{data_dir}:/data",
        _network_redis_image(),
        "sh",
        "-c",
        f"cat > /data/sentinel.conf <<'EOF'\n{sentinel_conf}EOF\nexec redis-server /data/sentinel.conf --sentinel",
    ]
    if publish_host_port:
        volume_index = cmd.index("-v")
        cmd[volume_index:volume_index] = ["-p", f"{publish_host}:{sentinel_port}:26379"]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL)

def _start_network_sentinel(
    host: str,
    redis_port: int,
    env: dict[str, str],
    *,
    docker_network_mode: str,
    docker_network_name: str,
    redis_alias: str,
    publish_host_port: bool,
) -> None:
    redis_host = redis_alias if _docker_network_uses_internal_identity(docker_network_mode) else host
    sentinel_alias = f"{redis_alias.rsplit('-redis', 1)[0]}-sentinel"
    _start_redis_sentinel(
        container_name=NETWORK_SENTINEL_CONTAINER,
        data_dir=DIR / "network-sentinel",
        advertised_host=host,
        redis_host=redis_host,
        redis_port=REDIS_CONTAINER_PORT if redis_host == redis_alias else redis_port,
        env=env,
        docker_network_mode=docker_network_mode,
        docker_network_name=docker_network_name,
        sentinel_alias=sentinel_alias,
        publish_host_port=publish_host_port,
    )

def _start_compose_sentinel(advertised_host: str, env: dict[str, str]) -> None:
    redis_port = _parse_configured_port(env.get("MN_REDIS_PORT")) or REDIS_DYNAMIC_PORT_START
    _start_redis_sentinel(
        container_name=COMPOSE_SENTINEL_CONTAINER,
        data_dir=DIR / "compose-sentinel",
        advertised_host=advertised_host,
        redis_host=advertised_host,
        redis_port=redis_port,
        env=env,
        docker_network_mode="disabled",
        docker_network_name=NETWORK_DOCKER_NETWORK,
        sentinel_alias="mirror-neuron-sentinel",
        publish_host_port=True,
    )

def _configure_worker_sentinel_primary(
    worker_host: str,
    worker_handshake: dict,
    *,
    primary_host: str,
    primary_port: int,
    primary_password: str,
) -> str | None:
    node_info = worker_handshake.get("node_info")
    redis_ha = node_info.get("redis_ha") if isinstance(node_info, dict) else None
    if not isinstance(redis_ha, dict) or str(redis_ha.get("mode") or "").strip().lower() != "sentinel":
        return None

    sentinel_port = _parse_configured_port(redis_ha.get("sentinel_port")) or DEFAULT_REDIS_SENTINEL_PORT
    sentinel_password = str(redis_ha.get("sentinel_password") or "").strip() or _redis_password_from_url(
        str(worker_handshake.get("redis_url") or "")
    )
    master_name = str(redis_ha.get("sentinel_master") or "mirror-neuron").strip() or "mirror-neuron"
    sentinel_host = _usable_remote_host(worker_host, worker_host)
    try:
        _redis_command(sentinel_host, sentinel_port, sentinel_password, "SENTINEL", "REMOVE", master_name)
    except Exception:
        logger.debug("Worker Sentinel remove before monitor failed", exc_info=True)
    try:
        _redis_command(
            sentinel_host,
            sentinel_port,
            sentinel_password,
            "SENTINEL",
            "MONITOR",
            master_name,
            primary_host,
            str(primary_port),
            "1",
        )
        _redis_command(
            sentinel_host,
            sentinel_port,
            sentinel_password,
            "SENTINEL",
            "SET",
            master_name,
            "auth-pass",
            primary_password,
        )
        _redis_command(
            sentinel_host,
            sentinel_port,
            sentinel_password,
            "SENTINEL",
            "SET",
            master_name,
            "down-after-milliseconds",
            "5000",
        )
    except Exception as exc:
        from mn_sdk.errors import AppError

        raise AppError(
            "MN_EXECUTION_FAILED",
            "Could not configure worker Redis Sentinel.",
            internal_message=str(exc),
            hint="Check that the worker Sentinel port is reachable and retry the node join.",
            exit_code=1,
            http_status=500,
            cause=exc,
        ) from exc
    return f"{sentinel_host}:{sentinel_port} -> {primary_host}:{primary_port}"

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
        *([] if os.uname().sysname == "Darwin" else ["--add-host", "host.docker.internal:host-gateway"]),
        *_docker_network_run_args(docker_network_mode, docker_network_name, node_alias),
        *port_args,
        *_network_core_bind_args(env),
        *env_args,
        "mirror-neuron-core:latest",
        *_distributed_core_command(),
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

    running_tokens = _runtime_grpc_tokens_from_running_container()
    if running_tokens:
        _ensure_runtime_grpc_tokens(running_tokens, persist_compose=runtime_compose_available())

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
        try:
            from mn_sdk.native_resources import cleanup_docker_worker_services

            cleanup_docker_worker_services(all_services=True)
        except Exception:
            logger.debug("Failed to prune DockerWorker Compose services during local runtime stop", exc_info=True)
        subprocess.run(["docker", "rm", "-f", COMPOSE_SENTINEL_CONTAINER], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
        subprocess.run(["docker", "rm", "-f", SYNCTHING_CONTAINER], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
    else:
        subprocess.run(["docker", "rm", "-f", LOCAL_CORE_CONTAINER], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
        subprocess.run(["docker", "rm", "-f", SYNCTHING_CONTAINER], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)

    for pid_file, _name in [
        *web_ui_pid_files(),
        *api_pid_files(),
        *native_sdk_grpc_pid_files(),
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
    leave_joined_cluster_before_stop()
    _stop_local_runtime_for_worker()
    _clear_join_owner_metadata()
    _clear_worker_redis_state()
    env_token = os.getenv("MN_NETWORK_JOIN_TOKEN", "").strip()
    if env_token:
        _write_network_token(env_token)
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
    env = _ensure_runtime_grpc_tokens(env, persist_compose=runtime_compose_available())
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
    redis_password = _resolve_redis_password(env)
    selected_redis_port = (
        _resolve_network_seed_redis_port(host, redis_port)
        if not external_redis_url and not use_internal_identity
        else None
    )
    redis_url = external_redis_url or (
        _network_redis_url(token, redis_alias, 6379, redis_password)
        if use_internal_identity
        else _network_redis_url(token, host, selected_redis_port or REDIS_CONTAINER_PORT, redis_password)
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
    if container_network_mode == "disabled" and _docker_host_socket() is not None:
        _ensure_docker_network("bridge", network_name)
    if not external_redis_url:
        console.print("=> Starting network Redis...")
        _start_network_redis(
            host,
            selected_redis_port,
            token,
            redis_password=redis_password,
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
    env = _ensure_installed_runtime_model_env(env)
    env = _ensure_runtime_grpc_tokens(env, persist_compose=runtime_compose_available())
    if not external_redis_url:
        _start_network_sentinel(
            host,
            redis_public_port,
            env,
            docker_network_mode=container_network_mode,
            docker_network_name=network_name,
            redis_alias=redis_alias,
            publish_host_port=not use_internal_identity,
        )
    env = _ensure_syncthing_for_runtime(env, advertised_host=host)
    _start_native_sdk_grpc_if_installed(env)
    if worker_node:
        _start_worker_compose_foundation_services(env)

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

    target = f"{seed_host}:{grpc_port}"
    local_host = (host or _detect_lan_ip()).strip()
    requested_mode = _docker_network_mode(docker_network_mode, default="disabled")
    network_name = _docker_network_name(docker_network_name)
    env = _runtime_base_env(runtime_compose_available())
    env.update(_shared_storage_env_from_runtime_env(env))
    env = _ensure_syncthing_for_runtime(env, advertised_host=local_host)
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
    try:
        handshake = Client(target=target, auth_token="", timeout=10).network_handshake(
            token,
            node_name=local_node_name,
            node_info=_handshake_node_info(
                local_host,
                node_name=local_node_name,
                grpc_port=env.get("MN_GRPC_PORT"),
            ),
        )
    except Exception as exc:
        _raise_join_handshake_error(exc, target)
    _connect_syncthing_peers(
        _syncthing_node_info(env, local_host),
        _syncthing_info_from_handshake(handshake),
    )
    remote_node = handshake.get("node_name") or _network_node_name(seed_host)
    redis_host, redis_port, redis_url = _validate_remote_redis_details(handshake, seed_host, token)
    from mn_cli.shared import client as local_client

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
        from mn_sdk.errors import AppError

        raise AppError(
            "MN_EXECUTION_FAILED",
            f"Could not add {remote_node} to the local cluster.",
            internal_message=str(exc),
            hint="Check that the local MirrorNeuron core is running, and that the remote host and token are correct.",
            exit_code=1,
            http_status=500,
            cause=exc,
        ) from exc
    status = _confirm_joined_node(local_client, remote_node, token, status)
    if runtime_compose_available():
        _persist_compose_cluster_node(remote_node)
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


def _confirm_joined_node(
    local_client: Any,
    remote_node: str,
    token: str,
    status: str,
    *,
    attempts: int = 4,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> str:
    for attempt in range(max(1, attempts)):
        if _summary_has_active_node(local_client, remote_node):
            return status
        if attempt >= attempts - 1:
            break
        sleep_fn(0.75)
        try:
            status = local_client.add_node(remote_node, token=token)
        except TypeError:
            status = local_client.add_node(remote_node)

    from mn_sdk.errors import AppError

    raise AppError(
        "MN_EXECUTION_FAILED",
        f"Could not confirm {remote_node} joined the local cluster.",
        hint="Run 'mn node list' and retry the join if the worker is still not visible as healthy.",
        exit_code=1,
        http_status=500,
    )


def _summary_has_active_node(local_client: Any, remote_node: str) -> bool:
    try:
        summary = json.loads(local_client.get_system_summary())
    except Exception:
        return False

    for node in summary.get("nodes") or []:
        if not isinstance(node, dict) or node.get("name") != remote_node:
            continue
        status = str(node.get("status") or "").strip().lower()
        return (
            status in {"healthy", "joining"}
            and node.get("scheduling_eligible") is not False
            and node.get("operator_disconnect") is not True
        )
    return False

def _persist_compose_cluster_node(node_name: str) -> None:
    node_name = str(node_name or "").strip()
    if not node_name:
        return

    env = _read_env_file(RUNTIME_COMPOSE_ENV)
    nodes = _split_env_list(env.get("MN_CLUSTER_NODES"))
    if node_name not in nodes:
        nodes.append(node_name)
        _write_env_file_values(RUNTIME_COMPOSE_ENV, {"MN_CLUSTER_NODES": ",".join(nodes)})

def _persist_join_owner_metadata(
    *,
    owner_node: str,
    owner_host: str,
    owner_grpc_port: int | str,
    worker_node: str,
) -> None:
    owner_node = str(owner_node or "").strip()
    owner_host = str(owner_host or "").strip()
    worker_node = str(worker_node or "").strip()
    if not owner_node or not worker_node:
        return

    _write_env_file_values(
        RUNTIME_COMPOSE_ENV,
        {
            "MN_JOIN_OWNER_NODE": owner_node,
            "MN_JOIN_OWNER_HOST": owner_host or _network_node_host(owner_node),
            "MN_JOIN_OWNER_GRPC_PORT": str(owner_grpc_port or DEFAULT_GRPC_PORT),
            "MN_JOIN_WORKER_NODE": worker_node,
        },
    )

def _clear_join_owner_metadata() -> None:
    _remove_env_file_keys(RUNTIME_COMPOSE_ENV, JOIN_OWNER_ENV_KEYS)
    try:
        JOIN_CLAIM_FILE.unlink(missing_ok=True)
    except OSError:
        pass

def _joined_cluster_owner_metadata() -> Optional[dict[str, str]]:
    env = _read_env_file(RUNTIME_COMPOSE_ENV) if RUNTIME_COMPOSE_ENV.exists() else {}

    metadata = _joined_cluster_owner_metadata_from_env(env)
    if metadata:
        return metadata
    return _joined_cluster_owner_metadata_from_claim(env)

def _joined_cluster_owner_metadata_from_env(env: dict[str, str]) -> Optional[dict[str, str]]:
    owner_node = str(env.get("MN_JOIN_OWNER_NODE") or "").strip()
    worker_node = str(env.get("MN_JOIN_WORKER_NODE") or env.get("MN_NODE_NAME") or "").strip()
    if not owner_node or not worker_node:
        return None

    owner_host = str(env.get("MN_JOIN_OWNER_HOST") or "").strip() or _network_node_host(owner_node)
    owner_port = str(env.get("MN_JOIN_OWNER_GRPC_PORT") or DEFAULT_GRPC_PORT).strip()
    if not owner_host:
        return None

    return {
        "owner_node": owner_node,
        "owner_host": owner_host,
        "owner_grpc_port": owner_port,
        "worker_node": worker_node,
        "auth_token": str(env.get("MN_GRPC_AUTH_TOKEN") or "").strip(),
    }

def _joined_cluster_owner_metadata_from_claim(env: dict[str, str]) -> Optional[dict[str, str]]:
    try:
        claim = json.loads(JOIN_CLAIM_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None

    owner_node = str(claim.get("owner_node") or "").strip()
    worker_node = str(env.get("MN_NODE_NAME") or "").strip()
    if not worker_node:
        advertised_host = str(env.get("MN_NETWORK_ADVERTISE_HOST") or "").strip()
        worker_node = _network_node_name(advertised_host) if advertised_host else ""
    if not owner_node or not worker_node:
        return None

    owner_host = str(claim.get("owner_grpc_host") or "").strip() or _network_node_host(owner_node)
    owner_port = str(claim.get("owner_grpc_port") or DEFAULT_GRPC_PORT).strip()
    if not owner_host:
        return None

    return {
        "owner_node": owner_node,
        "owner_host": owner_host,
        "owner_grpc_port": owner_port,
        "worker_node": worker_node,
        "auth_token": str(env.get("MN_GRPC_AUTH_TOKEN") or "").strip(),
    }

def leave_joined_cluster_before_stop() -> bool:
    metadata = _joined_cluster_owner_metadata()
    if not metadata:
        return False

    from mn_sdk import Client

    target = f"{metadata['owner_host']}:{metadata['owner_grpc_port']}"
    try:
        Client(target=target, auth_token=metadata.get("auth_token") or "", timeout=3).remove_node(
            metadata["worker_node"]
        )
        console.print(f"=> Removed {metadata['worker_node']} from cluster {metadata['owner_node']}.")
        return True
    except Exception as exc:
        console.print(
            f"[yellow]=> Could not notify cluster {metadata['owner_node']} before stopping; "
            "heartbeat cleanup will remove this worker.[/yellow]"
        )
        logger.debug("Best-effort cluster leave before stop failed: %s", exc, exc_info=True)
        return False
    finally:
        _clear_join_owner_metadata()

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
    env.update(_shared_storage_env_from_runtime_env(env))
    env = _ensure_syncthing_for_runtime(env, advertised_host=local_host)
    _persist_compose_shared_storage_env(
        {
            **{key: env[key] for key in SHARED_STORAGE_ENV_KEYS if key in env},
            **{key: env[key] for key in SYNCTHING_ENV_KEYS if key in env},
        }
    )
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
    env = _ensure_runtime_grpc_tokens(env, persist_compose=True)
    env = _ensure_compose_native_port_settings(env)
    env = _ensure_compose_cluster_bind_settings(env, local_host)
    if _docker_network_uses_internal_identity(docker_network_mode):
        env = _ensure_compose_internal_redis_settings(env, token=primary_token)
    else:
        env = _ensure_compose_cluster_port_settings(env, token=primary_token, advertised_host=local_host)

    env["MN_NETWORK_JOIN_TOKEN"] = primary_token
    env["MN_NETWORK_ADVERTISE_HOST"] = local_host
    env["MN_NODE_NAME"] = node_name
    env["MN_MODEL_SERVICE_NODE_NAME"] = node_name
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
            "MN_MODEL_SERVICE_NODE_NAME": env["MN_MODEL_SERVICE_NODE_NAME"],
            "MN_CLUSTER_NODES": env["MN_CLUSTER_NODES"],
            "MN_NODE_ROLE": env["MN_NODE_ROLE"],
            "MN_DIST_PORT": env["MN_DIST_PORT"],
            "MN_COOKIE": env["MN_COOKIE"],
            "ERL_AFLAGS": env["ERL_AFLAGS"],
            "MN_DOCKER_NETWORK_MODE": docker_network_mode,
            "MN_DOCKER_NETWORK_NAME": docker_network_name,
            **{key: env[key] for key in NODE_ADVERTISEMENT_ENV_KEYS if key in env},
            **{key: env[key] for key in SHARED_STORAGE_ENV_KEYS if key in env},
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
        _force_compose_redis_primary()
        _start_compose_sentinel(local_host, compose_env)
        _wait_for_local_cluster_grpc()
    except (FileNotFoundError, subprocess.CalledProcessError) as exc:
        from mn_sdk.errors import AppError

        raise AppError(
            "MN_EXECUTION_FAILED",
            "Could not enable cluster mode for the local runtime.",
            internal_message=str(exc),
            hint="Run 'mn runtime stop' and then 'mn runtime start --host <this-box-ip>' before joining the worker.",
            exit_code=1,
            http_status=500,
            cause=exc,
        ) from exc

def _wait_for_local_cluster_grpc(
    timeout_seconds: float = 10.0,
    *,
    core_client: Any | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
    time_fn: Callable[[], float] = time.time,
) -> None:
    from mn_cli.shared import client as local_client

    grpc_client = core_client if core_client is not None else local_client
    deadline = time_fn() + timeout_seconds
    last_error: Optional[Exception] = None
    while time_fn() < deadline:
        try:
            grpc_client.get_system_summary()
            return
        except Exception as exc:
            last_error = exc
            sleep_fn(0.25)

    console.print("[red]Error: Local MirrorNeuron core did not become ready after enabling cluster mode.[/red]")
    if last_error is not None:
        console.print(f"[dim]{last_error}[/dim]")
    raise typer.Exit(1)

def _stop_network_runtime() -> None:
    for container in [NETWORK_CORE_CONTAINER, NETWORK_REDIS_CONTAINER, NETWORK_SENTINEL_CONTAINER]:
        subprocess.run(["docker", "rm", "-f", container], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
    if runtime_compose_available():
        try:
            _remove_non_mirror_neuron_container(SYNCTHING_CONTAINER)
        except RuntimeError:
            pass
    else:
        subprocess.run(["docker", "rm", "-f", SYNCTHING_CONTAINER], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)

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
    return _runtime_read_env_file(path)

def _write_env_file_values(path: Path, updates: dict[str, str]) -> None:
    _runtime_write_env_file_values(path, updates)

def _remove_env_file_keys(path: Path, keys: set[str]) -> None:
    _runtime_remove_env_file_keys(path, keys)

def _remove_compose_file_env_keys(path: Path, keys: set[str]) -> None:
    if not keys:
        return

    try:
        original_lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return

    lines: list[str] = []
    changed = False
    for line in original_lines:
        stripped = line.strip()
        remove = any(
            stripped.startswith(f"{key}:")
            or stripped == f"- {key}"
            or stripped.startswith(f"- {key}=")
            for key in keys
        )
        if remove:
            changed = True
            continue
        lines.append(line)

    if changed:
        path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")

def _runtime_base_env(compose_runtime: bool) -> dict[str, str]:
    if compose_runtime:
        _remove_env_file_keys(RUNTIME_COMPOSE_ENV, DEPRECATED_RUNTIME_ENV_KEYS)
        _remove_compose_file_env_keys(RUNTIME_COMPOSE_FILE, DEPRECATED_RUNTIME_ENV_KEYS)
        _remove_runtime_compose_models_override()
    env = _read_env_file(RUNTIME_COMPOSE_ENV) if compose_runtime else {}
    env.update(os.environ)
    env = _ensure_installed_runtime_model_env(env)
    for key in DEPRECATED_RUNTIME_ENV_KEYS:
        env.pop(key, None)
    return env

def _ensure_installed_runtime_model_env(env: dict[str, str]) -> dict[str, str]:
    refs = _installed_catalog_runtime_models()
    services = _installed_catalog_runtime_services()
    if not refs:
        if not services:
            return env

    env = dict(env)
    if refs:
        existing = _split_env_list(env.get("MN_NODE_RUNTIME_MODELS"))
        seen = {ref.lower() for ref in existing}
        for ref in refs:
            if ref.lower() not in seen:
                existing.append(ref)
                seen.add(ref.lower())
        env["MN_NODE_RUNTIME_MODELS"] = ",".join(existing)
    if services:
        env["MN_MODEL_SERVICES_JSON"] = _merge_model_services_json(
            env.get("MN_MODEL_SERVICES_JSON"),
            services,
        )
    return env

def _installed_catalog_runtime_models() -> list[str]:
    try:
        from mn_sdk import (
            dmr_api_list_models,
            docker_model_match_keys,
            docker_model_name,
            list_model_entries,
            load_model_catalog,
        )

        installed_models = dmr_api_list_models(timeout=3)
        installed_keys = {
            key
            for model in installed_models
            for key in docker_model_match_keys(model)
        }
        refs: list[str] = []
        for entry in list_model_entries(load_model_catalog()):
            target = docker_model_name(entry)
            if not target:
                continue
            if docker_model_match_keys(target) & installed_keys:
                refs.append(str(entry.get("id") or target))
        return sorted(set(refs))
    except Exception:
        logger.debug("Could not infer installed runtime models", exc_info=True)
        return []

def _installed_catalog_runtime_services() -> list[dict[str, Any]]:
    try:
        from mn_sdk import (
            dmr_api_list_models,
            docker_model_match_keys,
            docker_model_name,
            list_model_entries,
            load_model_catalog,
            model_service_instance,
        )

        installed_models = dmr_api_list_models(timeout=3)
        installed_keys = {
            key
            for model in installed_models
            for key in docker_model_match_keys(model)
        }
        services: list[dict[str, Any]] = []
        for entry in list_model_entries(load_model_catalog()):
            target = docker_model_name(entry)
            if not target:
                continue
            if docker_model_match_keys(target) & installed_keys:
                services.append(model_service_instance(entry))
        return services
    except Exception:
        logger.debug("Could not infer installed runtime model services", exc_info=True)
        return []

def record_runtime_model_install(entry: dict[str, Any]) -> Optional[Path]:
    docker_model = str(entry.get("model") or entry.get("docker_model") or "").strip()
    if not docker_model:
        return None

    model_id = str(entry.get("id") or docker_model).strip()
    existing = _read_env_file(RUNTIME_COMPOSE_ENV) if RUNTIME_COMPOSE_ENV.exists() else {}
    refs = _split_env_list(existing.get("MN_NODE_RUNTIME_MODELS"))
    if model_id and model_id.lower() not in {ref.lower() for ref in refs}:
        refs.append(model_id)

    updates = {"MN_NODE_RUNTIME_MODELS": ",".join(refs)}
    try:
        from mn_sdk import model_service_instance

        updates["MN_MODEL_SERVICES_JSON"] = _merge_model_services_json(
            existing.get("MN_MODEL_SERVICES_JSON"),
            [model_service_instance(entry)],
        )
    except Exception:
        logger.debug("Could not record runtime model service advertisement", exc_info=True)
    if _is_default_llm_model(entry) or not str(existing.get("MN_LLM_MODEL_RUNNER_MODEL") or "").strip():
        updates["MN_LLM_MODEL_RUNNER_MODEL"] = DEFAULT_LLM_MODEL_RUNNER_MODEL

    RUNTIME_COMPOSE_ENV.parent.mkdir(parents=True, exist_ok=True)
    _write_env_file_values(RUNTIME_COMPOSE_ENV, updates)
    _remove_runtime_compose_models_override()
    return None

def _merge_model_services_json(existing: object, services: list[dict[str, Any]]) -> str:
    merged: list[dict[str, Any]] = []
    raw = str(existing or "").strip()
    if raw:
        try:
            decoded = json.loads(raw)
        except json.JSONDecodeError:
            decoded = []
        values = decoded.get("services") if isinstance(decoded, dict) else decoded
        if isinstance(values, dict):
            values = list(values.values())
        if isinstance(values, list):
            merged.extend(item for item in values if isinstance(item, dict))
    merged.extend(service for service in services if isinstance(service, dict))
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for service in merged:
        key = json.dumps(service, sort_keys=True, default=str)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(service)
    return json.dumps({"services": deduped}, sort_keys=True, separators=(",", ":"))

def ensure_context_engine_runtime(*, force: bool = False) -> dict[str, str]:
    if not runtime_compose_available():
        raise RuntimeError(
            "MirrorNeuron runtime Compose files were not found. Run the installer or mn runtime start first."
        )

    env = _runtime_base_env(True)
    profiles = _compose_profiles_with(env.get("COMPOSE_PROFILES"), "context")
    model = str(env.get("MN_CONTEXT_MODEL_RUNNER_MODEL") or DEFAULT_CONTEXT_MODEL_RUNNER_MODEL)
    engine_image = _context_engine_release_image(env)
    use_engine_image = _context_engine_image_mode_enabled(env, engine_image)
    updates = {
        "COMPOSE_PROFILES": profiles,
        "MN_CONTEXT_MODEL_RUNNER_MODEL": model,
    }
    source_dir: Path | None = None
    if use_engine_image:
        updates["ENGINE_IMAGE"] = engine_image
        updates["MN_MEMBRANE_ENGINE_IMAGE"] = engine_image
        _remove_env_file_keys(RUNTIME_COMPOSE_ENV, {"MEMBRANE_DIR"})
        env.pop("MEMBRANE_DIR", None)
    else:
        source_dir = _ensure_context_engine_source(env)
        updates["MEMBRANE_DIR"] = str(source_dir)
    _write_env_file_values(RUNTIME_COMPOSE_ENV, updates)
    env.update(updates)

    _remove_non_mirror_neuron_container(CONTEXT_ENGINE_CONTAINER)
    _remove_non_mirror_neuron_container(CONTEXT_ENGINE_MODEL_CONTAINER)
    _ensure_docker_model_runner()
    model_status = _install_context_engine_model(model)

    already_running = _docker_container_running(CONTEXT_ENGINE_CONTAINER)
    if force or not already_running:
        compose_env = env
        anonymous_docker_config: Path | None = None
        if use_engine_image:
            compose_env, anonymous_docker_config = _anonymous_public_gar_docker_env(env, engine_image)
        compose_process_env = _compose_subprocess_env(compose_env)
        try:
            if use_engine_image:
                subprocess.run(
                    runtime_compose_cmd("pull", CONTEXT_ENGINE_SERVICE),
                    check=True,
                    stdout=subprocess.DEVNULL,
                    env=compose_process_env,
                )
                up_args = ("up", "-d", "--no-build", CONTEXT_ENGINE_SERVICE)
            else:
                subprocess.run(
                    runtime_compose_cmd("build", CONTEXT_ENGINE_SERVICE),
                    check=True,
                    stdout=subprocess.DEVNULL,
                    env=compose_process_env,
                )
                up_args = ("up", "-d", CONTEXT_ENGINE_SERVICE)
            subprocess.run(
                runtime_compose_cmd(*up_args),
                check=True,
                stdout=subprocess.DEVNULL,
                env=compose_process_env,
            )
        finally:
            if anonymous_docker_config is not None:
                shutil.rmtree(anonymous_docker_config, ignore_errors=True)
        status = "restarted" if already_running and force else "started"
    else:
        status = "already_running"

    return {
        "status": status,
        "service": CONTEXT_ENGINE_SERVICE,
        "container": CONTEXT_ENGINE_CONTAINER,
        "model": model,
        "model_status": model_status,
        "compose_profiles": profiles,
        **({"engine_image": engine_image} if use_engine_image else {"membrane_dir": str(source_dir)}),
    }

def _compose_subprocess_env(env: dict[str, str]) -> dict[str, str]:
    merged = dict(os.environ)
    merged.update({str(key): str(value) for key, value in env.items()})
    return merged

def _docker_model_inspect_ok(model: str) -> bool:
    if not model:
        return False
    return _docker_command_ok(["docker", "model", "inspect", model])

def _install_context_engine_model(model: str) -> str:
    model = str(model or "").strip()
    if not model:
        return "skipped"
    if _docker_model_inspect_ok(model):
        return "already_installed"
    pull_result = subprocess.run(
        ["docker", "model", "pull", model],
        capture_output=True,
        text=True,
        timeout=900,
    )
    if pull_result.returncode != 0 and not _docker_model_inspect_ok(model):
        output = _subprocess_error_output(pull_result)
        raise RuntimeError(f"Failed to install context engine model {model}: {output}")
    run_result = subprocess.run(
        ["docker", "model", "run", "--detach", model],
        capture_output=True,
        text=True,
        timeout=300,
    )
    if run_result.returncode != 0:
        output = _subprocess_error_output(run_result)
        if "already" not in output.lower():
            raise RuntimeError(f"Failed to start context engine model {model}: {output}")
    return "installed"

def _subprocess_error_output(result: subprocess.CompletedProcess[str]) -> str:
    output = f"{result.stderr or ''}\n{result.stdout or ''}".strip()
    return output or f"exit code {result.returncode}"

def _anonymous_public_gar_docker_env(env: dict[str, str], image: str) -> tuple[dict[str, str], Path | None]:
    if not _is_public_gar_image(image):
        return env, None

    registry_host = _docker_image_registry_host(image)
    config_dir = Path(tempfile.mkdtemp(prefix="mn-public-gar-docker-config-"))
    source_config = _docker_config_dir(env) / "config.json"
    config: dict[str, Any] = {}
    if source_config.exists():
        try:
            loaded = json.loads(source_config.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                config = loaded
        except Exception:
            logger.debug("Could not read Docker config for public GAR pull; using anonymous config", exc_info=True)

    config = _docker_config_without_public_gar_credentials(config, registry_host)
    (config_dir / "config.json").write_text(json.dumps(config, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _expose_docker_cli_plugins(source_config.parent, config_dir)
    _expose_docker_contexts(source_config.parent, config_dir)

    next_env = dict(env)
    next_env["DOCKER_CONFIG"] = str(config_dir)
    return next_env, config_dir

def _expose_docker_cli_plugins(source_config_dir: Path, target_config_dir: Path) -> None:
    source_plugins = source_config_dir / "cli-plugins"
    if not source_plugins.is_dir():
        return
    target_plugins = target_config_dir / "cli-plugins"
    try:
        os.symlink(source_plugins, target_plugins, target_is_directory=True)
    except Exception:
        try:
            shutil.copytree(source_plugins, target_plugins, symlinks=True)
        except Exception:
            logger.debug("Could not expose Docker CLI plugins for anonymous GAR pull", exc_info=True)

def _expose_docker_contexts(source_config_dir: Path, target_config_dir: Path) -> None:
    source_contexts = source_config_dir / "contexts"
    if not source_contexts.is_dir():
        return
    target_contexts = target_config_dir / "contexts"
    try:
        os.symlink(source_contexts, target_contexts, target_is_directory=True)
    except Exception:
        try:
            shutil.copytree(source_contexts, target_contexts, symlinks=True)
        except Exception:
            logger.debug("Could not expose Docker contexts for anonymous GAR pull", exc_info=True)

def _is_public_gar_image(image: str) -> bool:
    text = str(image or "").strip()
    return PUBLIC_GAR_PROJECT_PATH in text and _docker_image_registry_host(text).endswith(".pkg.dev")

def _docker_image_registry_host(image: str) -> str:
    return str(image or "").split("/", 1)[0].strip().lower()

def _docker_config_dir(env: dict[str, str]) -> Path:
    configured = str(env.get("DOCKER_CONFIG") or os.environ.get("DOCKER_CONFIG") or "").strip()
    return Path(configured).expanduser() if configured else Path.home() / ".docker"

def _docker_config_without_public_gar_credentials(config: dict[str, Any], registry_host: str) -> dict[str, Any]:
    sanitized = dict(config)
    sanitized.pop("credsStore", None)
    for key in ("credHelpers", "auths"):
        value = sanitized.get(key)
        if not isinstance(value, dict):
            continue
        filtered = {
            str(registry): details
            for registry, details in value.items()
            if not _docker_registry_matches_public_gar(str(registry), registry_host)
        }
        if filtered:
            sanitized[key] = filtered
        else:
            sanitized.pop(key, None)
    return sanitized

def _docker_registry_matches_public_gar(registry: str, registry_host: str) -> bool:
    normalized = registry.strip().lower()
    normalized = normalized.removeprefix("https://").removeprefix("http://").split("/", 1)[0]
    return normalized == registry_host or normalized.endswith(".pkg.dev")

def _compose_profiles_with(value: object, required_profile: str) -> str:
    profiles: list[str] = []
    seen: set[str] = set()
    for raw_part in str(value or "").split(","):
        profile = raw_part.strip()
        if not profile:
            continue
        key = profile.lower()
        if key in seen:
            continue
        profiles.append(profile)
        seen.add(key)
    if required_profile.lower() not in seen:
        profiles.append(required_profile)
    return ",".join(profiles)

def _normalized_release_image_tag(value: object) -> str:
    tag = str(value or "").strip()
    if not tag:
        return ""
    return tag if tag.startswith("v") else f"v{tag}"

def _context_engine_release_image(env: dict[str, str]) -> str:
    explicit = str(
        os.getenv("MN_MEMBRANE_ENGINE_IMAGE")
        or env.get("MN_MEMBRANE_ENGINE_IMAGE")
        or os.getenv("MN_CONTEXT_ENGINE_IMAGE")
        or env.get("MN_CONTEXT_ENGINE_IMAGE")
        or os.getenv("ENGINE_IMAGE")
        or env.get("ENGINE_IMAGE")
        or ""
    ).strip()
    if explicit:
        return explicit

    tag = _normalized_release_image_tag(
        os.getenv("MN_MEMBRANE_ENGINE_IMAGE_TAG")
        or env.get("MN_MEMBRANE_ENGINE_IMAGE_TAG")
        or os.getenv("MN_RUNTIME_MODULE_VERSION")
        or env.get("MN_RUNTIME_MODULE_VERSION")
        or os.getenv("MN_PACKAGE_VERSION")
        or env.get("MN_PACKAGE_VERSION")
    )
    if not tag:
        return ""
    repository = str(
        os.getenv("MN_MEMBRANE_ENGINE_IMAGE_REPOSITORY")
        or env.get("MN_MEMBRANE_ENGINE_IMAGE_REPOSITORY")
        or DEFAULT_MEMBRANE_ENGINE_IMAGE_REPOSITORY
    ).strip().rstrip("/")
    return f"{repository}:{tag}" if repository else ""

def _context_engine_image_mode_enabled(env: dict[str, str], image: str) -> bool:
    mode = str(os.getenv("MN_MEMBRANE_SOURCE_MODE") or env.get("MN_MEMBRANE_SOURCE_MODE") or "").strip().lower()
    if mode in {"source", "git", "checkout", "local"}:
        return False
    if mode in {"image", "docker", "gar", "release"}:
        return bool(image)
    return bool(image and image != "mirror-neuron-memory-engine:latest")

def _context_engine_git_url(env: dict[str, str]) -> str:
    explicit = str(os.getenv("MN_MEMBRANE_GIT_URL") or env.get("MN_MEMBRANE_GIT_URL") or "").strip()
    if explicit:
        return explicit
    repo = str(os.getenv("MN_MEMBRANE_REPO") or env.get("MN_MEMBRANE_REPO") or DEFAULT_MEMBRANE_REPO).strip()
    return f"https://github.com/{repo}.git"

def _context_engine_source_candidates(env: dict[str, str]) -> list[Path]:
    checkout_dir = Path(__file__).resolve().parents[2]
    raw_candidates = [
        os.getenv("MN_MEMBRANE_DIR"),
        env.get("MN_MEMBRANE_DIR"),
        env.get("MEMBRANE_DIR"),
        str(checkout_dir / "Membrane"),
        str(checkout_dir.parent / "Membrane"),
        str(Path.cwd() / "Membrane"),
        str(Path.cwd().parent / "Membrane"),
    ]
    candidates: list[Path] = []
    seen: set[str] = set()
    for raw in raw_candidates:
        value = str(raw or "").strip()
        if not value:
            continue
        path = Path(value).expanduser()
        key = str(path)
        if key in seen:
            continue
        candidates.append(path)
        seen.add(key)
    return candidates

def _valid_context_engine_source(path: Path) -> bool:
    return (path / "Dockerfile").is_file()

def _ensure_context_engine_source(env: dict[str, str]) -> Path:
    for candidate in _context_engine_source_candidates(env):
        if _valid_context_engine_source(candidate):
            resolved = candidate.resolve()
            _remove_dockerfile_frontend_directive(resolved / "Dockerfile")
            return resolved

    target = Path(str(env.get("MEMBRANE_DIR") or DIR / "Membrane")).expanduser()
    if target.exists() and not target.is_dir():
        raise RuntimeError(f"MEMBRANE_DIR is not a directory: {target}")
    if target.exists() and not _valid_context_engine_source(target):
        if not any(target.iterdir()):
            target.rmdir()
        else:
            raise RuntimeError(
                f"Membrane source at {target} is missing Dockerfile. "
                "Set MN_MEMBRANE_DIR to a valid Membrane checkout or remove the invalid path."
            )

    if not target.exists():
        target.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(
            ["git", "clone", _context_engine_git_url(env), str(target)],
            check=True,
            stdout=subprocess.DEVNULL,
        )
    else:
        subprocess.run(
            ["git", "-C", str(target), "pull", "--ff-only"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    if not _valid_context_engine_source(target):
        raise RuntimeError(f"Membrane source at {target} is missing Dockerfile after checkout.")
    resolved = target.resolve()
    _remove_dockerfile_frontend_directive(resolved / "Dockerfile")
    return resolved

def _remove_dockerfile_frontend_directive(dockerfile: Path) -> None:
    try:
        lines = dockerfile.read_text(encoding="utf-8").splitlines()
    except OSError:
        return
    if not lines:
        return
    first = lines[0].strip()
    if not (first.startswith("# syntax=docker/dockerfile:") or first.startswith("# syntax = docker/dockerfile:")):
        return
    dockerfile.write_text("\n".join(lines[1:]) + ("\n" if len(lines) > 1 else ""), encoding="utf-8")

def _remove_non_mirror_neuron_container(name: str) -> None:
    try:
        result = subprocess.run(
            ["docker", "container", "inspect", "-f", "{{ index .Config.Labels \"com.docker.compose.project\" }}", name],
            capture_output=True,
            text=True,
        )
    except FileNotFoundError as exc:
        raise RuntimeError("Docker is not installed or not in PATH.") from exc
    if result.returncode != 0:
        return
    if result.stdout.strip() == "mirror-neuron":
        return
    subprocess.run(["docker", "rm", "-f", name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def _docker_command_ok(command: list[str]) -> bool:
    try:
        result = subprocess.run(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except FileNotFoundError:
        return False
    return result.returncode == 0

def _docker_model_command_available() -> bool:
    try:
        result = subprocess.run(["docker", "--help"], capture_output=True, text=True)
    except FileNotFoundError:
        return False
    if result.returncode != 0:
        return False
    output = f"{result.stdout}\n{result.stderr}"
    return any(line.strip().split()[0].rstrip("*") == "model" for line in output.splitlines() if line.strip())

def _ensure_docker_model_runner() -> None:
    if not _docker_model_command_available():
        raise RuntimeError(
            "Docker Model Runner CLI is not available. Upgrade Docker Desktop/Engine to a version "
            "with 'docker model' support."
        )
    if _docker_command_ok(["docker", "model", "status"]):
        return
    if _docker_command_ok(["docker", "desktop", "enable", "model-runner"]) and _docker_command_ok(
        ["docker", "model", "status"]
    ):
        return
    if _docker_command_ok(["docker", "model", "install-runner", "--help"]):
        subprocess.run(["docker", "model", "install-runner"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        subprocess.run(["docker", "model", "start-runner"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if _docker_command_ok(["docker", "model", "status"]):
            return
    raise RuntimeError(
        "Docker Model Runner is not ready. Enable it in Docker Desktop Settings > AI, "
        "or run 'docker model install-runner' and 'docker model start-runner'."
    )

def _runtime_compose_models_override_file() -> Path:
    return RUNTIME_COMPOSE_FILE.parent / RUNTIME_MODELS_OVERRIDE_FILE

def _remove_runtime_compose_models_override() -> None:
    path = _runtime_compose_models_override_file()
    try:
        path.unlink()
    except FileNotFoundError:
        return

def _is_default_llm_model(entry: dict[str, Any]) -> bool:
    values = [
        entry.get("id"),
        entry.get("model"),
        entry.get("docker_model"),
        entry.get("api_model"),
        *list(entry.get("aliases") or []),
    ]
    normalized = {str(value or "").strip().lower() for value in values if str(value or "").strip()}
    return bool(
        normalized
        & {
            "default",
            "gemma4",
            "gemma4:e2b",
            "gemma4-e2b",
            "ai/gemma4:e2b",
            "gemme4",
            "gemme4:e2b",
            DEFAULT_LLM_MODEL_RUNNER_MODEL.lower(),
        }
    )

def _split_env_list(value: object) -> list[str]:
    return [part.strip() for part in str(value or "").split(",") if part.strip()]

def _yaml_double_quote_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')

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
    runtime_env = str(env.get("MN_ENV") or os.getenv("MN_ENV") or "dev").strip().lower()
    blueprint_source = str(env.get("MN_BLUEPRINT_SOURCE") or os.getenv("MN_BLUEPRINT_SOURCE") or "github").strip().lower()
    if blueprint_source not in {"github", "local"}:
        blueprint_source = "github"
    blueprint_repo = normalize_blueprint_repo_value(
        str(env.get("MN_BLUEPRINT_REPO") or os.getenv("MN_BLUEPRINT_REPO") or DEFAULT_BLUEPRINT_REPO).strip()
    )
    blueprint_local = str(env.get("MN_BLUEPRINT_LOCAL") or os.getenv("MN_BLUEPRINT_LOCAL") or "").strip()
    host_home_dir = str(
        env.get("MN_HOST_HOME_DIR")
        or os.getenv("MN_HOST_HOME_DIR")
        or DIR
    ).strip()
    configured_runs_root = str(env.get("MN_RUNS_ROOT") or os.getenv("MN_RUNS_ROOT") or "").strip()
    host_artifacts_dir = str(
        env.get("MN_HOST_ARTIFACTS_DIR") or os.getenv("MN_HOST_ARTIFACTS_DIR") or configured_runs_root
    ).strip()
    if not host_artifacts_dir:
        host_artifacts_dir = str(Path(host_home_dir).expanduser() / "runs")
    host_blob_store_dir = str(
        env.get("MN_HOST_BLOB_STORE_DIR") or os.getenv("MN_HOST_BLOB_STORE_DIR") or ""
    ).strip()
    if not host_blob_store_dir:
        host_blob_store_dir = str(Path(host_home_dir).expanduser() / "blobs")
    host_shared_storage_root = str(
        env.get("MN_HOST_SHARED_STORAGE_ROOT")
        or env.get("MN_SHARED_STORAGE_ROOT")
        or os.getenv("MN_HOST_SHARED_STORAGE_ROOT")
        or os.getenv("MN_SHARED_STORAGE_ROOT")
        or ""
    ).strip()
    if not host_shared_storage_root:
        host_shared_storage_root = str(Path(host_home_dir).expanduser() / "shared")
    container_runs_root = str(
        env.get("MN_CONTAINER_RUNS_ROOT") or os.getenv("MN_CONTAINER_RUNS_ROOT") or DEFAULT_CONTAINER_RUNS_ROOT
    ).strip()
    container_blob_store_root = str(
        env.get("MN_CONTAINER_BLOB_STORE_ROOT")
        or os.getenv("MN_CONTAINER_BLOB_STORE_ROOT")
        or DEFAULT_CONTAINER_BLOB_STORE_ROOT
    ).strip()
    runtime_shared_storage_root = str(
        env.get("MN_RUNTIME_SHARED_STORAGE_ROOT")
        or env.get("MN_CONTAINER_SHARED_STORAGE_ROOT")
        or os.getenv("MN_RUNTIME_SHARED_STORAGE_ROOT")
        or os.getenv("MN_CONTAINER_SHARED_STORAGE_ROOT")
        or DEFAULT_RUNTIME_SHARED_STORAGE_ROOT
    ).strip()
    runtime_bundle_cache_dir = str(
        env.get("MN_BUNDLE_CACHE_DIR")
        or os.getenv("MN_BUNDLE_CACHE_DIR")
        or f"{runtime_shared_storage_root.rstrip('/')}/bundle_cache"
    ).strip()
    updates: dict[str, str] = {
        "MN_ENV": runtime_env,
        "MN_BLUEPRINT_SOURCE": blueprint_source,
        "MN_BLUEPRINT_REPO": blueprint_repo,
        "MN_BLUEPRINT_LOCAL": blueprint_local,
        "MN_HOST_ARTIFACTS_DIR": host_artifacts_dir,
        "MN_HOST_BLOB_STORE_DIR": host_blob_store_dir,
        "MN_SHARED_STORAGE_ROOT": host_shared_storage_root,
        "MN_HOST_SHARED_STORAGE_ROOT": host_shared_storage_root,
        "MN_RUNS_ROOT": configured_runs_root or host_artifacts_dir,
        "MN_CONTAINER_RUNS_ROOT": container_runs_root,
        "MN_CONTAINER_BLOB_STORE_ROOT": container_blob_store_root,
        "MN_RUNTIME_SHARED_STORAGE_ROOT": runtime_shared_storage_root,
        "MN_CONTAINER_SHARED_STORAGE_ROOT": runtime_shared_storage_root,
        "MN_BUNDLE_CACHE_DIR": runtime_bundle_cache_dir,
        "MN_BLOB_STORE_ROOT": container_blob_store_root,
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
    return updates

def _ensure_host_artifacts_dir(env: dict[str, str]) -> None:
    for path_text in (
        str(env.get("MN_HOST_ARTIFACTS_DIR") or env.get("MN_RUNS_ROOT") or "").strip(),
        str(env.get("MN_SHARED_STORAGE_ROOT") or env.get("MN_HOST_SHARED_STORAGE_ROOT") or "").strip(),
    ):
        if not path_text:
            continue
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
    grpc_port = _valid_port_text(
        str(env.get("MN_GRPC_ADVERTISE_PORT") or env.get("MN_GRPC_PORT") or DEFAULT_GRPC_PORT),
        DEFAULT_GRPC_PORT,
    )
    grpc_target = str(env.get("MN_GRPC_TARGET") or "").strip()
    if not grpc_target:
        grpc_target = f"{grpc_host}:{grpc_port}"
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
    native_sdk_host = _native_endpoint_host(str(env.get("MN_NATIVE_SDK_GRPC_HOST") or DEFAULT_NATIVE_SDK_GRPC_HOST))
    native_sdk_port = _valid_port_text(
        str(env.get("MN_NATIVE_SDK_GRPC_PORT") or DEFAULT_NATIVE_SDK_GRPC_PORT),
        DEFAULT_NATIVE_SDK_GRPC_PORT,
    )
    snapshot["native_sdk_grpc"] = {
        "target": str(env.get("MN_NATIVE_SDK_GRPC_TARGET") or f"{DEFAULT_NATIVE_SDK_GRPC_TARGET_HOST}:{native_sdk_port}"),
        "host": native_sdk_host,
        "port": native_sdk_port,
    }
    return snapshot

def _read_runtime_api_health(api_host: str, api_port: str, *, timeout_seconds: float = 2.0) -> Optional[dict[str, Any]]:
    url = _api_http_url(api_host, api_port, "/api/v1/health")
    try:
        with urllib.request.urlopen(url, timeout=timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))
            return payload if isinstance(payload, dict) else None
    except Exception:
        return None

def _is_url_like(value: str) -> bool:
    normalized = str(value or "").strip()
    if re.fullmatch(r"[\w.-]+@[\w.-]+:[^\s]+", normalized):
        return True
    try:
        parsed = urlparse(normalized)
    except Exception:
        return False
    return parsed.scheme in {"http", "https", "ssh", "git"} and bool(parsed.netloc)

def _normalize_blueprint_location(value: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        return ""
    if _is_url_like(normalized):
        if re.fullmatch(r"[\w.-]+@[\w.-]+:[^\s]+", normalized):
            host, _, repo_path = normalized.partition(":")
            return f"{host.lower()}:{repo_path.rstrip('/').removesuffix('.git')}"
        parsed = urlparse(normalized)
        repo_path = parsed.path.rstrip("/").removesuffix(".git")
        return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}{repo_path}"
    return str(Path(normalized).expanduser().resolve())

def _same_blueprint_location(active: str, expected: str) -> bool:
    active_normalized = _normalize_blueprint_location(active)
    expected_normalized = _normalize_blueprint_location(expected)
    return bool(active_normalized and expected_normalized and active_normalized == expected_normalized)

def _same_runtime_path(active: str, expected: str) -> bool:
    active_path = str(active or "").strip()
    expected_path = str(expected or "").strip()
    if not active_path or not expected_path:
        return False
    try:
        return Path(active_path).expanduser().resolve() == Path(expected_path).expanduser().resolve()
    except OSError:
        return active_path == expected_path

def _expected_blueprint_location(env: dict[str, str]) -> str:
    source = str(env.get("MN_BLUEPRINT_SOURCE") or "github").strip().lower()
    if source == "local":
        return str(env.get("MN_BLUEPRINT_LOCAL") or "").strip()
    return str(env.get("MN_BLUEPRINT_REPO") or DEFAULT_BLUEPRINT_REPO).strip()

def _runtime_api_config_mismatches(env: dict[str, str], health: Optional[dict[str, Any]]) -> list[tuple[str, str, str]]:
    if not health:
        return []
    mismatches: list[tuple[str, str, str]] = []
    expected_env = str(env.get("MN_ENV") or "dev").strip().lower()
    active_env = str(health.get("env") or health.get("mn_env") or "").strip().lower()
    if active_env and expected_env and active_env != expected_env:
        mismatches.append(("MN_ENV", active_env, expected_env))

    expected_blueprint = _expected_blueprint_location(env)
    active_blueprint = str(
        health.get("active_blueprint_location")
        or health.get("activeBlueprintLocation")
        or health.get("blueprint_repo")
        or health.get("blueprintRepo")
        or ""
    ).strip()
    if active_blueprint and expected_blueprint and not _same_blueprint_location(active_blueprint, expected_blueprint):
        mismatches.append(("blueprint repo", active_blueprint, expected_blueprint))

    expected_runs_root = str(env.get("MN_RUNS_ROOT") or env.get("MN_HOST_ARTIFACTS_DIR") or "").strip()
    active_runs_root = str(health.get("runs_root") or health.get("runsRoot") or "").strip()
    if active_runs_root and expected_runs_root and not _same_runtime_path(active_runs_root, expected_runs_root):
        mismatches.append(("runs root", active_runs_root, expected_runs_root))
    return mismatches

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
    grpc_advertise_port = _valid_port_text(
        str(adjusted.get("MN_GRPC_ADVERTISE_PORT") or grpc_port),
        grpc_port,
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
    artifact_port = _valid_port_text(
        _env_or_default(adjusted, "MN_ARTIFACT_PORT", DEFAULT_ARTIFACT_PORT),
        DEFAULT_ARTIFACT_PORT,
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
    grpc_target = adjusted.get("MN_GRPC_TARGET") or f"localhost:{grpc_port}"
    if not os.getenv("MN_GRPC_TARGET", "").strip() and grpc_target == f"localhost:{LEGACY_GRPC_PORT}":
        grpc_target = f"localhost:{grpc_port}"
    native_sdk_port = _valid_port_text(
        _env_or_default(adjusted, "MN_NATIVE_SDK_GRPC_PORT", DEFAULT_NATIVE_SDK_GRPC_PORT),
        DEFAULT_NATIVE_SDK_GRPC_PORT,
    )
    native_sdk_host = adjusted.get("MN_NATIVE_SDK_GRPC_HOST") or DEFAULT_NATIVE_SDK_GRPC_HOST
    native_sdk_proxy_port = _valid_port_text(
        str(adjusted.get("MN_NATIVE_SDK_GRPC_PROXY_PORT") or native_sdk_port),
        native_sdk_port,
    )
    native_sdk_proxy_target_host = (
        adjusted.get("MN_NATIVE_SDK_GRPC_PROXY_TARGET_HOST") or DEFAULT_NATIVE_SDK_GRPC_TARGET_HOST
    )
    native_sdk_proxy_target_port = _valid_port_text(
        str(adjusted.get("MN_NATIVE_SDK_GRPC_PROXY_TARGET_PORT") or native_sdk_port),
        native_sdk_port,
    )
    existing_native_sdk_target = str(adjusted.get("MN_NATIVE_SDK_GRPC_TARGET") or "").strip()
    legacy_native_sdk_target = f"{DEFAULT_NATIVE_SDK_GRPC_TARGET_HOST}:{native_sdk_port}"
    if existing_native_sdk_target and existing_native_sdk_target != legacy_native_sdk_target:
        native_sdk_target = existing_native_sdk_target
    else:
        native_sdk_target = f"{DEFAULT_NATIVE_SDK_GRPC_COMPOSE_SERVICE}:{native_sdk_proxy_port}"

    updates = {
        "MN_GRPC_BIND_HOST": adjusted.get("MN_GRPC_BIND_HOST") or "127.0.0.1",
        "MN_GRPC_PORT": grpc_port,
        "MN_GRPC_ADVERTISE_PORT": grpc_advertise_port,
        "MN_GRPC_TARGET": grpc_target,
        "MN_API_HOST": adjusted.get("MN_API_HOST") or DEFAULT_HOST,
        "MN_API_PORT": api_port,
        "MN_NATIVE_SDK_GRPC_HOST": native_sdk_host,
        "MN_NATIVE_SDK_GRPC_PORT": native_sdk_port,
        "MN_NATIVE_SDK_GRPC_TARGET": native_sdk_target,
        "MN_NATIVE_SDK_GRPC_PROXY_PORT": native_sdk_proxy_port,
        "MN_NATIVE_SDK_GRPC_PROXY_TARGET_HOST": native_sdk_proxy_target_host,
        "MN_NATIVE_SDK_GRPC_PROXY_TARGET_PORT": native_sdk_proxy_target_port,
        "MN_DOCKER_MODEL_RUNNER_PROXY_ENABLED": adjusted.get("MN_DOCKER_MODEL_RUNNER_PROXY_ENABLED") or "true",
        "MN_DOCKER_MODEL_RUNNER_PROXY_BIND_HOST": adjusted.get("MN_DOCKER_MODEL_RUNNER_PROXY_BIND_HOST") or "0.0.0.0",
        "MN_DOCKER_MODEL_RUNNER_PROXY_PORT": _valid_port_text(
            str(adjusted.get("MN_DOCKER_MODEL_RUNNER_PROXY_PORT") or DEFAULT_MODEL_RUNNER_PROXY_PORT),
            DEFAULT_MODEL_RUNNER_PROXY_PORT,
        ),
        "MN_DOCKER_MODEL_RUNNER_PROXY_TARGET_HOST": adjusted.get("MN_DOCKER_MODEL_RUNNER_PROXY_TARGET_HOST")
        or "host.docker.internal",
        "MN_DOCKER_MODEL_RUNNER_PROXY_TARGET_PORT": _valid_port_text(
            str(adjusted.get("MN_DOCKER_MODEL_RUNNER_PROXY_TARGET_PORT") or "12434"),
            "12434",
        ),
        "MN_DIST_PORT": dist_port,
        "MN_WEB_UI_HOST": adjusted.get("MN_WEB_UI_HOST") or DEFAULT_HOST,
        "MN_WEB_UI_PORT": web_ui_port,
        "MN_ARTIFACT_ENABLED": adjusted.get("MN_ARTIFACT_ENABLED") or "true",
        "MN_ARTIFACT_BIND_HOST": adjusted.get("MN_ARTIFACT_BIND_HOST") or "0.0.0.0",
        "MN_ARTIFACT_PUBLISH_HOST": adjusted.get("MN_ARTIFACT_PUBLISH_HOST") or "127.0.0.1",
        "MN_ARTIFACT_PORT": artifact_port,
        "MN_ARTIFACT_ADVERTISE_URL": adjusted.get("MN_ARTIFACT_ADVERTISE_URL")
        or f"http://127.0.0.1:{artifact_port}",
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
    redis_password = _resolve_redis_password(adjusted)
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
    adjusted = _ensure_redis_ha_settings(adjusted, advertised_host=advertised_host, cluster=True)
    if adjusted.get("MN_REDIS_HA_MODE") == "sentinel":
        updates.update({key: adjusted[key] for key in REDIS_HA_ENV_KEYS if key in adjusted})
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
    redis_password = _resolve_redis_password(adjusted)
    redis_host = network_redis_host or os.getenv("MN_NETWORK_REDIS_HOST", "").strip() or "redis"
    redis_port = network_redis_port or REDIS_CONTAINER_PORT

    adjusted["MN_REDIS_PASSWORD"] = redis_password
    adjusted["MN_REDIS_URL"] = f"redis://:{redis_password}@redis:{REDIS_CONTAINER_PORT}/0"
    adjusted["MN_CONTEXT_REDIS_URL"] = f"redis://:{redis_password}@redis:{REDIS_CONTAINER_PORT}/1"
    adjusted.setdefault("MN_NETWORK_JOIN_TOKEN", token)
    adjusted["MN_NETWORK_REDIS_HOST"] = redis_host
    adjusted["MN_NETWORK_REDIS_PORT"] = str(redis_port)
    adjusted = _ensure_redis_ha_settings(adjusted, advertised_host=str(adjusted.get("MN_NETWORK_ADVERTISE_HOST") or redis_host), cluster=True)

    updates = {
            "MN_NETWORK_JOIN_TOKEN": token,
            "MN_REDIS_PASSWORD": redis_password,
            "MN_REDIS_URL": adjusted["MN_REDIS_URL"],
            "MN_CONTEXT_REDIS_URL": adjusted["MN_CONTEXT_REDIS_URL"],
            "MN_NETWORK_REDIS_HOST": adjusted["MN_NETWORK_REDIS_HOST"],
            "MN_NETWORK_REDIS_PORT": adjusted["MN_NETWORK_REDIS_PORT"],
    }
    if adjusted.get("MN_REDIS_HA_MODE") == "sentinel":
        updates.update({key: adjusted[key] for key in REDIS_HA_ENV_KEYS if key in adjusted})
    _write_env_file_values(RUNTIME_COMPOSE_ENV, updates)
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
    if not parsed.password:
        console.print(f"[red]Error: Remote node at {seed_host} did not advertise password-authenticated Redis.[/red]")
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
        password = _resolve_redis_password(env)
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
    worker_password = _redis_password_from_url(worker_redis_url)
    if worker_redis_host == primary_host and int(worker_redis_port) == int(primary_port):
        console.print(
            "[yellow]Worker Redis replication skipped: worker advertised the primary Redis endpoint.[/yellow]"
        )
        return None

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
        _configure_worker_sentinel_primary(
            worker_host,
            worker_handshake,
            primary_host=primary_host,
            primary_port=primary_port,
            primary_password=primary_password,
        )
        try:
            _redis_command(primary_host, primary_port, primary_password, "WAIT", "1", "1000")
        except Exception:
            logger.debug("Primary Redis WAIT after worker replica setup failed", exc_info=True)
    except Exception as exc:
        from mn_sdk.errors import AppError

        raise AppError(
            "MN_EXECUTION_FAILED",
            "Could not configure worker Redis replication.",
            internal_message=str(exc),
            hint="Check that both Redis endpoints are reachable and retry the node join.",
            exit_code=1,
            http_status=500,
            cause=exc,
        ) from exc

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
    model_runner_proxy_override = RUNTIME_COMPOSE_FILE.parent / RUNTIME_MODEL_RUNNER_PROXY_OVERRIDE_FILE
    if model_runner_proxy_override.exists():
        cmd.extend(["-f", str(model_runner_proxy_override)])
    syncthing_override = _runtime_compose_syncthing_override_file()
    if syncthing_override.exists():
        cmd.extend(["-f", str(syncthing_override)])
    workers_override = RUNTIME_COMPOSE_FILE.parent / RUNTIME_WORKERS_OVERRIDE_FILE
    if workers_override.exists() and _runtime_compose_args_need_worker_override(args):
        cmd.extend(["-f", str(workers_override)])
    cmd.extend(args)
    return cmd

def _runtime_compose_args_need_worker_override(args: tuple[str, ...]) -> bool:
    return bool(args) and args[0] in {"down", "stop", "rm", "ps", "logs"}

def _legacy_checkout_pid_dir() -> Path:
    checkout_dir = Path(__file__).resolve().parents[2]
    return checkout_dir / "MirrorNeuron" / "pids"

def web_ui_pid_files() -> tuple[tuple[Path, str], ...]:
    legacy_pid_dir = _legacy_checkout_pid_dir()
    paths = [
        (WEB_UI_WATCHDOG_PID_FILE, "Web UI watchdog"),
        (WEB_UI_PID_FILE, "Web UI"),
        (DIR / "pids" / "web-ui-watchdog.pid", "Web UI watchdog"),
        (DIR / "pids" / "web-ui.pid", "Web UI"),
        (legacy_pid_dir / "web-ui-watchdog.pid", "Web UI watchdog"),
        (legacy_pid_dir / "web-ui.pid", "Web UI"),
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
    legacy_pid_dir = _legacy_checkout_pid_dir()
    paths = [
        (API_WATCHDOG_PID_FILE, "REST API watchdog"),
        (API_PID_FILE, "REST API"),
        (DIR / "pids" / "api-watchdog.pid", "REST API watchdog"),
        (DIR / "pids" / "api.pid", "REST API"),
        (legacy_pid_dir / "api-watchdog.pid", "REST API watchdog"),
        (legacy_pid_dir / "api.pid", "REST API"),
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

def native_sdk_grpc_pid_files() -> tuple[tuple[Path, str], ...]:
    legacy_pid_dir = _legacy_checkout_pid_dir()
    paths = [
        (NATIVE_SDK_GRPC_WATCHDOG_PID_FILE, "Native SDK gRPC watchdog"),
        (NATIVE_SDK_GRPC_PID_FILE, "Native SDK gRPC"),
        (DIR / "pids" / "native-sdk-grpc-watchdog.pid", "Native SDK gRPC watchdog"),
        (DIR / "pids" / "native-sdk-grpc.pid", "Native SDK gRPC"),
        (legacy_pid_dir / "native-sdk-grpc-watchdog.pid", "Native SDK gRPC watchdog"),
        (legacy_pid_dir / "native-sdk-grpc.pid", "Native SDK gRPC"),
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

def stop_matching_sidecar_processes(command_name: str, display_name: str) -> bool:
    try:
        output = subprocess.check_output(
            ["pgrep", "-fl", command_name],
            stderr=subprocess.DEVNULL,
            text=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False

    current_pid = os.getpid()
    stopped = False
    seen: set[int] = set()
    for line in output.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        pid_text, _, _command = stripped.partition(" ")
        try:
            pid = int(pid_text)
        except ValueError:
            continue
        if pid == current_pid or pid in seen:
            continue
        seen.add(pid)
        console.print(f"   Stopping orphaned {display_name} sidecar process (PID: {pid})...")
        kill_tree(pid)
        stopped = True
    if stopped:
        time.sleep(1)
    return stopped

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

def _native_sdk_grpc_command() -> Optional[list[str]]:
    native_bin = VENV_DIR / "bin" / "mn-native-sdk-grpc"
    if native_bin.exists():
        return [str(native_bin)]
    if os.getenv("MN_NATIVE_SDK_GRPC_SOURCE", "").strip().lower() in {"1", "true", "yes", "on"}:
        return [sys.executable, "-m", "mn_sdk.native_runtime_service"]
    try:
        import importlib.util

        if importlib.util.find_spec("mn_sdk.native_runtime_service") is not None:
            return [sys.executable, "-m", "mn_sdk.native_runtime_service"]
    except (ImportError, ValueError):
        pass
    return None

def _start_native_sdk_grpc_watchdog(env: dict[str, str]) -> subprocess.Popen:
    command = _native_sdk_grpc_command()
    if command is None:
        raise FileNotFoundError("mn-native-sdk-grpc")
    NATIVE_SDK_GRPC_PID_FILE.parent.mkdir(parents=True, exist_ok=True)
    NATIVE_SDK_GRPC_LOG.parent.mkdir(parents=True, exist_ok=True)
    NATIVE_SDK_GRPC_WATCHDOG_LOG.parent.mkdir(parents=True, exist_ok=True)
    config = {
        "command": command,
        "cwd": str(_sidecar_workdir()),
        "pid_file": str(NATIVE_SDK_GRPC_PID_FILE),
        "log_file": str(NATIVE_SDK_GRPC_LOG),
        "restart_delay": env.get("MN_NATIVE_SDK_GRPC_RESTART_DELAY_SECONDS", DEFAULT_WEB_UI_RESTART_DELAY_SECONDS),
    }
    with open(NATIVE_SDK_GRPC_WATCHDOG_LOG, "w") as out:
        return subprocess.Popen(
            [sys.executable, "-c", _web_ui_watchdog_script(), json.dumps(config)],
            stdout=out,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            env=env,
            start_new_session=True,
        )

def _wait_for_tcp(host: str, port: str, *, timeout_seconds: float = 5.0) -> bool:
    deadline = time.monotonic() + max(timeout_seconds, 0.0)
    target_host = _native_endpoint_host(host)
    target_port = _parse_configured_port(port)
    if target_port is None:
        return False
    while True:
        try:
            with socket.create_connection((target_host, target_port), timeout=1.0):
                return True
        except OSError:
            if time.monotonic() >= deadline:
                return False
            time.sleep(0.2)

def _start_native_sdk_grpc_if_installed(
    runtime_env: Optional[dict[str, str]] = None,
    *,
    restart_running: bool = False,
    restart_reason: str = "",
) -> bool:
    if _native_sdk_grpc_command() is None:
        console.print("[yellow]=> Warning: mn-native-sdk-grpc not found; native model preparation forwarding is unavailable.[/yellow]")
        return False

    env = os.environ.copy()
    if runtime_env:
        env.update(runtime_env)
    source_sdk_dir = _source_checkout_sdk_dir()
    if source_sdk_dir is not None:
        _prepend_pythonpath(env, source_sdk_dir)
    native_host = env.get("MN_NATIVE_SDK_GRPC_HOST") or DEFAULT_NATIVE_SDK_GRPC_HOST
    native_port = _valid_port_text(str(env.get("MN_NATIVE_SDK_GRPC_PORT") or DEFAULT_NATIVE_SDK_GRPC_PORT), DEFAULT_NATIVE_SDK_GRPC_PORT)
    env["MN_NATIVE_SDK_GRPC_HOST"] = native_host
    env["MN_NATIVE_SDK_GRPC_PORT"] = native_port

    watchdog_status = check_status(NATIVE_SDK_GRPC_WATCHDOG_PID_FILE)
    child_status = check_status(NATIVE_SDK_GRPC_PID_FILE)
    if watchdog_status == 0:
        if _wait_for_tcp(native_host, native_port, timeout_seconds=2.0) and not restart_running:
            console.print("[yellow]=> Native SDK gRPC watchdog is already running, skipping.[/yellow]")
            return True
        if restart_running:
            detail = f" ({restart_reason})" if restart_reason else ""
            console.print(f"[yellow]=> Native SDK gRPC watchdog is already running; restarting it{detail}.[/yellow]")
        else:
            console.print("[yellow]=> Native SDK gRPC watchdog is running, but the service is not responding; restarting it.[/yellow]")
        try:
            watchdog_pid = int(NATIVE_SDK_GRPC_WATCHDOG_PID_FILE.read_text().strip())
            kill_tree(watchdog_pid)
            time.sleep(1)
        except (ValueError, OSError):
            pass
        NATIVE_SDK_GRPC_WATCHDOG_PID_FILE.unlink(missing_ok=True)
        NATIVE_SDK_GRPC_PID_FILE.unlink(missing_ok=True)
    elif watchdog_status == 1:
        NATIVE_SDK_GRPC_WATCHDOG_PID_FILE.unlink(missing_ok=True)

    if child_status == 0:
        try:
            pid = int(NATIVE_SDK_GRPC_PID_FILE.read_text().strip())
            console.print(f"=> Restarting existing Native SDK gRPC service (PID: {pid}) under watchdog...")
            kill_tree(pid)
            time.sleep(1)
        except (ValueError, OSError):
            pass
        NATIVE_SDK_GRPC_PID_FILE.unlink(missing_ok=True)
    elif child_status == 1:
        NATIVE_SDK_GRPC_PID_FILE.unlink(missing_ok=True)

    stop_matching_sidecar_processes("mn-native-sdk-grpc", "Native SDK gRPC")

    console.print(f"=> Starting Native SDK gRPC watchdog (model prep on {native_host}:{native_port})...")
    try:
        p_watchdog = _start_native_sdk_grpc_watchdog(env)
    except FileNotFoundError:
        console.print("[yellow]=> Warning: mn-native-sdk-grpc not found; native model preparation forwarding is unavailable.[/yellow]")
        return False
    NATIVE_SDK_GRPC_WATCHDOG_PID_FILE.parent.mkdir(parents=True, exist_ok=True)
    NATIVE_SDK_GRPC_WATCHDOG_PID_FILE.write_text(str(p_watchdog.pid))
    if _wait_for_tcp(native_host, native_port, timeout_seconds=10.0):
        console.print(f"   [green][Started][/green] Native SDK gRPC watchdog (PID: {p_watchdog.pid})")
    else:
        console.print(
            f"   [yellow][Started][/yellow] Native SDK gRPC watchdog (PID: {p_watchdog.pid}); "
            f"waiting for {native_host}:{native_port} to respond."
        )
    return True

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
        "cwd": str(_sidecar_workdir()),
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

def _start_api_if_installed(
    runtime_env: Optional[dict[str, str]] = None,
    *,
    restart_running: bool = False,
    restart_reason: str = "",
) -> bool:
    if _api_command() is None:
        console.print("[yellow]=> Warning: mn-api not found, skipping.[/yellow]")
        return False

    env = os.environ.copy()
    if runtime_env:
        env.update(runtime_env)
    source_api_dir = _source_checkout_api_dir()
    if source_api_dir is not None:
        _prepend_pythonpath(env, source_api_dir)
    api_host = env.get("MN_API_HOST") or _api_host()
    api_port = _valid_port_text(str(env.get("MN_API_PORT") or DEFAULT_API_PORT), DEFAULT_API_PORT)
    env["MN_API_HOST"] = api_host
    env["MN_API_PORT"] = api_port

    watchdog_status = check_status(API_WATCHDOG_PID_FILE)
    child_status = check_status(API_PID_FILE)
    if watchdog_status == 0:
        if _wait_for_api(api_host, api_port, timeout_seconds=5.0) and not restart_running:
            console.print("[yellow]=> REST API watchdog is already running, skipping.[/yellow]")
            return True
        if restart_running:
            detail = f" ({restart_reason})" if restart_reason else ""
            console.print(f"[yellow]=> REST API watchdog is already running; restarting it{detail}.[/yellow]")
        else:
            console.print("[yellow]=> REST API watchdog is running, but the API is not responding; restarting it.[/yellow]")
        try:
            watchdog_pid = int(API_WATCHDOG_PID_FILE.read_text().strip())
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

    if _wait_for_api(api_host, api_port, timeout_seconds=1.0):
        console.print("[yellow]=> REST API is responding without the current watchdog; restarting it under watchdog.[/yellow]")
    stop_matching_sidecar_processes("mn-api", "REST API")

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
            runtime_env.get("MN_GRPC_TARGET", f"{core_host}:{DEFAULT_GRPC_PORT}"),
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
    stopped_untracked = False
    if (
        watchdog_status == 2
        and child_status == 2
        and _wait_for_web_ui(web_ui_host, web_ui_port, timeout_seconds=1.0)
    ):
        console.print("[yellow]=> Web UI is responding without the current watchdog; restarting it under watchdog.[/yellow]")
        stopped_untracked = stop_matching_sidecar_processes("mn-web-ui-server", "Web UI")
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

    if not stopped_untracked:
        stop_matching_sidecar_processes("mn-web-ui-server", "Web UI")

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

def _build_core_docker_run_command(
    env: dict[str, str],
    *,
    requested_docker_mode: str,
    network_name: str,
    node_alias: str,
) -> list[str]:
    cmd = ["docker", "run", "-d", "--name", "mirror-neuron-core"]

    cmd.extend(["-e", f"MN_NODE_NAME={env['MN_NODE_NAME']}"])
    cmd.extend(["-e", f"MN_COOKIE={env['MN_COOKIE']}"])
    cmd.extend(["-e", f"MN_GRPC_AUTH_TOKEN={env.get('MN_GRPC_AUTH_TOKEN', FIXED_GRPC_AUTH_TOKEN)}"])
    cmd.extend(["-e", f"{GRPC_ADMIN_TOKEN_ENV}={env.get(GRPC_ADMIN_TOKEN_ENV, FIXED_GRPC_ADMIN_TOKEN)}"])
    cmd.extend(["-e", f"MN_NETWORK_JOIN_TOKEN={env['MN_NETWORK_JOIN_TOKEN']}"])
    cmd.extend(["-e", f"MN_NETWORK_ADVERTISE_HOST={env['MN_NETWORK_ADVERTISE_HOST']}"])
    if env.get("MN_MODEL_SERVICE_NODE_NAME"):
        cmd.extend(["-e", f"MN_MODEL_SERVICE_NODE_NAME={env['MN_MODEL_SERVICE_NODE_NAME']}"])
    if env.get("MN_NATIVE_SDK_GRPC_TARGET"):
        cmd.extend(["-e", f"MN_NATIVE_SDK_GRPC_TARGET={env['MN_NATIVE_SDK_GRPC_TARGET']}"])
    if env.get("MN_NATIVE_SDK_GRPC_HOST"):
        cmd.extend(["-e", f"MN_NATIVE_SDK_GRPC_HOST={env['MN_NATIVE_SDK_GRPC_HOST']}"])
    if env.get("MN_NATIVE_SDK_GRPC_PORT"):
        cmd.extend(["-e", f"MN_NATIVE_SDK_GRPC_PORT={env['MN_NATIVE_SDK_GRPC_PORT']}"])
    if env.get("MN_NATIVE_SDK_GRPC_ADVERTISE_HOST"):
        cmd.extend(["-e", f"MN_NATIVE_SDK_GRPC_ADVERTISE_HOST={env['MN_NATIVE_SDK_GRPC_ADVERTISE_HOST']}"])
    if env.get("MN_NATIVE_SDK_GRPC_ADVERTISE_PORT"):
        cmd.extend(["-e", f"MN_NATIVE_SDK_GRPC_ADVERTISE_PORT={env['MN_NATIVE_SDK_GRPC_ADVERTISE_PORT']}"])
    cmd.extend(["-e", f"MN_NETWORK_REDIS_HOST={env['MN_NETWORK_REDIS_HOST']}"])
    cmd.extend(["-e", f"MN_NETWORK_REDIS_PORT={env['MN_NETWORK_REDIS_PORT']}"])
    cmd.extend(["-e", f"MN_ARTIFACT_ENABLED={env['MN_ARTIFACT_ENABLED']}"])
    cmd.extend(["-e", f"MN_ARTIFACT_BIND_HOST={env['MN_ARTIFACT_BIND_HOST']}"])
    cmd.extend(["-e", f"MN_ARTIFACT_PORT={env['MN_ARTIFACT_PORT']}"])
    cmd.extend(["-e", f"MN_ARTIFACT_ADVERTISE_URL={env['MN_ARTIFACT_ADVERTISE_URL']}"])
    cmd.extend(["-e", f"MN_CLUSTER_NODES={env['MN_CLUSTER_NODES']}"])
    if env.get("MN_NODE_ALIAS"):
        cmd.extend(["-e", f"MN_NODE_ALIAS={env['MN_NODE_ALIAS']}"])
    if env.get("MN_DOCKER_NETWORK_MODE"):
        cmd.extend(["-e", f"MN_DOCKER_NETWORK_MODE={env['MN_DOCKER_NETWORK_MODE']}"])
    if env.get("MN_DOCKER_NETWORK_NAME"):
        cmd.extend(["-e", f"MN_DOCKER_NETWORK_NAME={env['MN_DOCKER_NETWORK_NAME']}"])
    cmd.extend(["-e", f"MN_NODE_ROLE={env['MN_NODE_ROLE']}"])
    for node_env_key in NODE_ADVERTISEMENT_ENV_KEYS:
        if env.get(node_env_key):
            cmd.extend(["-e", f"{node_env_key}={env[node_env_key]}"])
    cmd.extend(["-e", f"MN_GRPC_PORT={env['MN_GRPC_PORT']}"])
    cmd.extend(["-e", f"MN_GRPC_ADVERTISE_PORT={env.get('MN_GRPC_ADVERTISE_PORT', env['MN_GRPC_PORT'])}"])
    cmd.extend(["-e", f"MN_DIST_PORT={env['MN_DIST_PORT']}"])
    cmd.extend(["-e", f"MN_RUNS_ROOT={env.get('MN_CONTAINER_RUNS_ROOT', DEFAULT_CONTAINER_RUNS_ROOT)}"])
    cmd.extend(["-e", f"MN_SHARED_STORAGE_ROOT={env.get('MN_RUNTIME_SHARED_STORAGE_ROOT', DEFAULT_RUNTIME_SHARED_STORAGE_ROOT)}"])
    cmd.extend(["-e", f"MN_RUNTIME_SHARED_STORAGE_ROOT={env.get('MN_RUNTIME_SHARED_STORAGE_ROOT', DEFAULT_RUNTIME_SHARED_STORAGE_ROOT)}"])
    cmd.extend(["-e", f"ERL_AFLAGS={env['ERL_AFLAGS']}"])

    core_publish_host = _docker_publish_host(env["MN_CORE_HOST"])
    system_name = os.uname().sysname
    if requested_docker_mode != "disabled" and node_alias:
        cmd.extend(_docker_network_run_args(requested_docker_mode, network_name, node_alias))
    if system_name != "Darwin":
        cmd.extend(["--add-host", "host.docker.internal:host-gateway"])

    if system_name == "Darwin":
        cmd.extend(["-p", f"{core_publish_host}:{env['MN_GRPC_PORT']}:{env['MN_GRPC_PORT']}"])
        cmd.extend([
            "-p",
            f"{env['MN_ARTIFACT_PUBLISH_HOST']}:{env['MN_ARTIFACT_PORT']}:{env['MN_ARTIFACT_PORT']}",
        ])
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

    host_home_dir = str(env.get("MN_HOST_HOME_DIR") or DIR)
    host_artifacts_dir = str(env.get("MN_HOST_ARTIFACTS_DIR") or Path(host_home_dir).expanduser() / "runs")
    container_runs_root = str(env.get("MN_CONTAINER_RUNS_ROOT") or DEFAULT_CONTAINER_RUNS_ROOT)
    host_blob_store_dir = str(env.get("MN_HOST_BLOB_STORE_DIR") or Path(host_home_dir).expanduser() / "blobs")
    container_blob_store_root = str(env.get("MN_CONTAINER_BLOB_STORE_ROOT") or DEFAULT_CONTAINER_BLOB_STORE_ROOT)
    host_shared_storage_root = str(env.get("MN_HOST_SHARED_STORAGE_ROOT") or env.get("MN_SHARED_STORAGE_ROOT") or Path(host_home_dir).expanduser() / "shared")
    runtime_shared_storage_root = str(env.get("MN_RUNTIME_SHARED_STORAGE_ROOT") or DEFAULT_RUNTIME_SHARED_STORAGE_ROOT)
    runtime_bundle_cache_dir = str(env.get("MN_BUNDLE_CACHE_DIR") or f"{runtime_shared_storage_root.rstrip('/')}/bundle_cache")
    cmd.extend(["-e", f"MN_HOST_SHARED_STORAGE_ROOT={host_shared_storage_root}"])
    cmd.extend(["-v", f"{host_home_dir}:/root/.mn"])
    cmd.extend(["-v", f"{host_home_dir}:/opt/mirror_neuron/.mn"])
    cmd.extend(["-v", f"{host_artifacts_dir}:{container_runs_root}"])
    cmd.extend(["-v", f"{host_artifacts_dir}:/opt/mirror_neuron/.mn/runs"])
    cmd.extend(["-v", f"{host_blob_store_dir}:{container_blob_store_root}"])
    cmd.extend(["-v", f"{host_blob_store_dir}:/opt/mirror_neuron/.mn/blobs"])
    cmd.extend(["-v", f"{host_shared_storage_root}:{runtime_shared_storage_root}"])
    cmd.extend(["-v", f"{host_shared_storage_root}:/opt/mirror_neuron/.mn/shared"])
    cmd.extend(_docker_worker_bind_args())
    cmd.extend(["-e", f"MN_BLOB_STORE_ROOT={container_blob_store_root}"])
    cmd.extend(["-e", f"MN_BUNDLE_CACHE_DIR={runtime_bundle_cache_dir}"])

    cmd.extend(["mirror-neuron-core:latest", *_distributed_core_command()])
    return cmd

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
        for key, value in _runtime_grpc_tokens_from_running_container().items():
            if value:
                env[key] = value
        env = _ensure_runtime_grpc_tokens(env, persist_compose=compose_runtime)
        env = _ensure_runtime_api_token(env, persist_compose=compose_runtime)
        force_runtime_recreate = False
        if compose_runtime:
            env = _ensure_compose_native_port_settings(env)
            env = _ensure_syncthing_for_runtime(
                env,
                advertised_host=_advertised_network_host(host),
            )
            if host:
                env, force_runtime_recreate = _prepare_running_compose_exposure(
                    env,
                    host=host,
                    token=token,
                    redis_port=redis_port,
                    docker_network_mode=docker_network_mode,
                    docker_network_name=docker_network_name,
                )
            core_running = _docker_container_running("mirror-neuron-core")
            if not core_running:
                console.print("=> MirrorNeuron Core is not running; starting Docker runtime (Compose)...")
            elif _running_core_has_stale_grpc_tokens():
                console.print("=> MirrorNeuron Core has stale gRPC tokens; recreating Docker runtime (Compose)...")
                force_runtime_recreate = True
            try:
                compose_args = ["up", "-d"]
                if force_runtime_recreate:
                    compose_args.extend(["--force-recreate", "redis", "mirror-neuron-core"])
                _ensure_litellm_gateway_host_config()
                subprocess.run(runtime_compose_cmd(*compose_args), check=True, stdout=subprocess.DEVNULL, env=env)
                if not core_running:
                    console.print("   [green][Started][/green] Docker runtime (Compose project: mirror-neuron)")
            except (FileNotFoundError, subprocess.CalledProcessError):
                console.print("[red]Failed to start MirrorNeuron Docker runtime.[/red]")
                raise typer.Exit(1)
        env.setdefault("MN_API_HOST", _api_host())
        env.setdefault("MN_API_PORT", DEFAULT_API_PORT)
        env.setdefault("MN_WEB_UI_HOST", _web_ui_host())
        env.setdefault("MN_WEB_UI_PORT", DEFAULT_WEB_UI_PORT)
        api_host = str(env.get("MN_API_HOST") or DEFAULT_HOST)
        api_port = _valid_port_text(str(env.get("MN_API_PORT") or DEFAULT_API_PORT), DEFAULT_API_PORT)
        api_health = _read_runtime_api_health(api_host, api_port)
        api_mismatches = _runtime_api_config_mismatches(env, api_health)
        if api_mismatches:
            mismatch_details = ", ".join(
                f"{key} {active or '(unset)'} -> {expected or '(unset)'}"
                for key, active, expected in api_mismatches
            )
            console.print(f"[yellow]=> REST API runtime config changed; restarting API ({mismatch_details}).[/yellow]")
        _start_api_if_installed(
            env,
            restart_running=bool(api_mismatches),
            restart_reason="runtime config changed",
        )
        _start_native_sdk_grpc_if_installed(
            env,
            restart_running=True,
            restart_reason="runtime already running",
        )
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
    env.update(_shared_storage_env_from_runtime_env(env))
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
    env = _ensure_syncthing_for_runtime(env, advertised_host=advertised_host)
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
        env.update(_grpc_tokens_from_handshake(join_handshake))
        env.update(_redis_ha_env_from_handshake(join_handshake))
        _connect_syncthing_peers(
            _syncthing_node_info(env, advertised_host),
            _syncthing_info_from_handshake(join_handshake),
        )
    env = _ensure_runtime_api_token(env, persist_compose=compose_runtime)
    env = _ensure_runtime_grpc_tokens(env, persist_compose=compose_runtime)
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
        seed_node_name = _joined_cluster_seed_node(env, local_node_name)
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
    env.setdefault("MN_GRPC_TARGET", f"localhost:{env.get('MN_GRPC_PORT', DEFAULT_GRPC_PORT)}")
    env.setdefault("MN_GRPC_ADVERTISE_PORT", env.get("MN_GRPC_PORT", DEFAULT_GRPC_PORT))
    env.setdefault("MN_NATIVE_SDK_GRPC_HOST", DEFAULT_NATIVE_SDK_GRPC_HOST)
    env.setdefault("MN_NATIVE_SDK_GRPC_PORT", DEFAULT_NATIVE_SDK_GRPC_PORT)
    env.setdefault("MN_NATIVE_SDK_GRPC_TARGET", f"{DEFAULT_NATIVE_SDK_GRPC_TARGET_HOST}:{env['MN_NATIVE_SDK_GRPC_PORT']}")
    env["MN_NETWORK_JOIN_TOKEN"] = network_token
    env["MN_NETWORK_ADVERTISE_HOST"] = advertised_host
    env["MN_NETWORK_REDIS_HOST"] = seed_redis_host
    env["MN_NETWORK_REDIS_PORT"] = str(seed_redis_port)
    artifact_port = _valid_port_text(
        str(env.get("MN_ARTIFACT_PORT") or DEFAULT_ARTIFACT_PORT),
        DEFAULT_ARTIFACT_PORT,
    )
    env["MN_ARTIFACT_ENABLED"] = str(env.get("MN_ARTIFACT_ENABLED") or "true")
    env["MN_ARTIFACT_BIND_HOST"] = str(env.get("MN_ARTIFACT_BIND_HOST") or "0.0.0.0")
    env["MN_ARTIFACT_PUBLISH_HOST"] = _network_publish_host(advertised_host)
    env["MN_ARTIFACT_PORT"] = artifact_port
    env["MN_ARTIFACT_ADVERTISE_URL"] = f"http://{advertised_host}:{artifact_port}"
    if not ip:
        env = _ensure_syncthing_for_runtime(env, advertised_host=advertised_host)
    if requested_docker_mode != "disabled" and node_alias:
        env.update(_docker_network_env(requested_docker_mode, network_name, node_alias))
    if _generated_node_setting_should_update("MN_NODE_NAME", env.get("MN_NODE_NAME"), local_node_name):
        env["MN_NODE_NAME"] = local_node_name
    if _generated_node_setting_should_update(
        "MN_MODEL_SERVICE_NODE_NAME",
        env.get("MN_MODEL_SERVICE_NODE_NAME"),
        env["MN_NODE_NAME"],
    ):
        env["MN_MODEL_SERVICE_NODE_NAME"] = env["MN_NODE_NAME"]
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
        cluster_nodes = _cluster_nodes_with(seed_node_name, local_node_name)
        env["MN_CLUSTER_NODES"] = cluster_nodes
        _persist_join_owner_metadata(
            owner_node=seed_node_name,
            owner_host=ip or _network_node_host(seed_node_name),
            owner_grpc_port=grpc_port,
            worker_node=local_node_name,
        )
        if compose_runtime:
            _write_env_file_values(
                RUNTIME_COMPOSE_ENV,
                {
                    "MN_NODE_NAME": env["MN_NODE_NAME"],
                    "MN_CLUSTER_NODES": cluster_nodes,
                    "MN_REDIS_URL": redis_url,
                    "MN_CONTEXT_REDIS_URL": env["MN_CONTEXT_REDIS_URL"],
                    "MN_NATIVE_SDK_GRPC_HOST": env.get("MN_NATIVE_SDK_GRPC_HOST", DEFAULT_NATIVE_SDK_GRPC_HOST),
                    "MN_NATIVE_SDK_GRPC_PORT": env.get("MN_NATIVE_SDK_GRPC_PORT", DEFAULT_NATIVE_SDK_GRPC_PORT),
                    "MN_NATIVE_SDK_GRPC_TARGET": env.get("MN_NATIVE_SDK_GRPC_TARGET", ""),
                    "MN_NATIVE_SDK_GRPC_PROXY_PORT": env.get("MN_NATIVE_SDK_GRPC_PROXY_PORT", ""),
                    "MN_NATIVE_SDK_GRPC_PROXY_TARGET_HOST": env.get("MN_NATIVE_SDK_GRPC_PROXY_TARGET_HOST", ""),
                    "MN_NATIVE_SDK_GRPC_PROXY_TARGET_PORT": env.get("MN_NATIVE_SDK_GRPC_PROXY_TARGET_PORT", ""),
                    "MN_NETWORK_REDIS_HOST": seed_redis_host,
                    "MN_NETWORK_REDIS_PORT": str(seed_redis_port),
                    "MN_ARTIFACT_ENABLED": env["MN_ARTIFACT_ENABLED"],
                    "MN_ARTIFACT_BIND_HOST": env["MN_ARTIFACT_BIND_HOST"],
                    "MN_ARTIFACT_PUBLISH_HOST": env["MN_ARTIFACT_PUBLISH_HOST"],
                    "MN_ARTIFACT_PORT": env["MN_ARTIFACT_PORT"],
                    "MN_ARTIFACT_ADVERTISE_URL": env["MN_ARTIFACT_ADVERTISE_URL"],
                    **{key: env[key] for key in NODE_ADVERTISEMENT_ENV_KEYS if key in env},
                    **{key: env[key] for key in SHARED_STORAGE_ENV_KEYS if key in env},
                    **{key: env[key] for key in SYNCTHING_ENV_KEYS if key in env},
                    **{key: env[key] for key in REDIS_HA_ENV_KEYS if key in env},
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
            redis_password = _resolve_redis_password(env)
            env["MN_REDIS_PASSWORD"] = redis_password
            env["MN_REDIS_URL"] = f"redis://:{redis_password}@redis:{REDIS_CONTAINER_PORT}/0"
            env["MN_CONTEXT_REDIS_URL"] = f"redis://:{redis_password}@redis:{REDIS_CONTAINER_PORT}/1"

    if compose_runtime:
        _write_env_file_values(
            RUNTIME_COMPOSE_ENV,
            {
                "MN_NETWORK_ADVERTISE_HOST": env["MN_NETWORK_ADVERTISE_HOST"],
                "MN_NATIVE_SDK_GRPC_HOST": env.get("MN_NATIVE_SDK_GRPC_HOST", DEFAULT_NATIVE_SDK_GRPC_HOST),
                "MN_NATIVE_SDK_GRPC_PORT": env.get("MN_NATIVE_SDK_GRPC_PORT", DEFAULT_NATIVE_SDK_GRPC_PORT),
                "MN_NATIVE_SDK_GRPC_TARGET": env.get("MN_NATIVE_SDK_GRPC_TARGET", ""),
                "MN_NATIVE_SDK_GRPC_PROXY_PORT": env.get("MN_NATIVE_SDK_GRPC_PROXY_PORT", ""),
                "MN_NATIVE_SDK_GRPC_PROXY_TARGET_HOST": env.get("MN_NATIVE_SDK_GRPC_PROXY_TARGET_HOST", ""),
                "MN_NATIVE_SDK_GRPC_PROXY_TARGET_PORT": env.get("MN_NATIVE_SDK_GRPC_PROXY_TARGET_PORT", ""),
                "MN_MODEL_SERVICE_NODE_NAME": env.get("MN_MODEL_SERVICE_NODE_NAME", ""),
                "MN_NODE_NAME": env["MN_NODE_NAME"],
                "MN_NODE_ROLE": env["MN_NODE_ROLE"],
                "MN_CLUSTER_NODES": env["MN_CLUSTER_NODES"],
                "MN_NETWORK_REDIS_HOST": env["MN_NETWORK_REDIS_HOST"],
                "MN_NETWORK_REDIS_PORT": env["MN_NETWORK_REDIS_PORT"],
                "MN_ARTIFACT_ENABLED": env["MN_ARTIFACT_ENABLED"],
                "MN_ARTIFACT_BIND_HOST": env["MN_ARTIFACT_BIND_HOST"],
                "MN_ARTIFACT_PUBLISH_HOST": env["MN_ARTIFACT_PUBLISH_HOST"],
                "MN_ARTIFACT_PORT": env["MN_ARTIFACT_PORT"],
                "MN_ARTIFACT_ADVERTISE_URL": env["MN_ARTIFACT_ADVERTISE_URL"],
                "MN_DIST_PORT": env["MN_DIST_PORT"],
                **{key: env[key] for key in NODE_ADVERTISEMENT_ENV_KEYS if key in env},
                "MN_NODE_MODELS": env.get("MN_NODE_MODELS", ""),
                "MN_NODE_RUNTIME_MODELS": env.get("MN_NODE_RUNTIME_MODELS", ""),
                "MN_DOCKER_NETWORK_MODE": env.get("MN_DOCKER_NETWORK_MODE", "disabled"),
                "MN_DOCKER_NETWORK_NAME": env.get("MN_DOCKER_NETWORK_NAME", network_name),
                **{key: env[key] for key in SHARED_STORAGE_ENV_KEYS if key in env},
                **{key: env[key] for key in SYNCTHING_ENV_KEYS if key in env},
                **{key: env[key] for key in REDIS_HA_ENV_KEYS if key in env},
                **(
                    _docker_network_env(requested_docker_mode, network_name, node_alias)
                    if requested_docker_mode != "disabled" and node_alias
                    else {}
                ),
            },
        )

    if compose_runtime:
        _write_env_file_values(RUNTIME_COMPOSE_ENV, {"MN_COOKIE": env["MN_COOKIE"]})

    if compose_runtime:
        env = _compose_runtime_env(env, ip)
        console.print("=> Starting MirrorNeuron Docker runtime (Compose)...")
        logger.info("Starting MirrorNeuron Docker Compose runtime")
        try:
            _ensure_litellm_gateway_host_config()
            subprocess.run(runtime_compose_cmd("up", "-d"), check=True, stdout=subprocess.DEVNULL, env=env)
            if not ip and not reconnecting_joined_node:
                _force_compose_redis_primary()
                _start_compose_sentinel(advertised_host, env)
            console.print("   [green][Started][/green] Docker runtime (Compose project: mirror-neuron)")
        except (FileNotFoundError, subprocess.CalledProcessError):
            console.print("[red]Failed to start MirrorNeuron Docker Compose runtime.[/red]")
            raise typer.Exit(1)
    else:
        console.print("=> Starting MirrorNeuron Core Service (Docker)...")
        logger.info("Starting MirrorNeuron Core Docker container")
        subprocess.run(["docker", "rm", "-f", "mirror-neuron-core"], stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)

        cmd = _build_core_docker_run_command(
            env,
            requested_docker_mode=requested_docker_mode,
            network_name=network_name,
            node_alias=node_alias,
        )

        try:
            subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL)
            console.print("   [green][Started][/green] Core Service (Docker: mirror-neuron-core)")
        except subprocess.CalledProcessError:
            console.print("[red]Failed to start Core Service Docker container.[/red]")
            raise typer.Exit(1)

    console.print("=> Waiting for Elixir to boot...")
    time.sleep(3)

    _start_native_sdk_grpc_if_installed(env)
    api_started = _start_api_if_installed(env)

    web_ui_available = _start_web_ui_if_installed(env)
    if api_started:
        endpoint_snapshot = _write_runtime_endpoints_file(env, web_ui_available=web_ui_available)
        console.print(f"   Runtime endpoints: {RUNTIME_ENDPOINTS_FILE}")
        logger.info("Wrote MirrorNeuron runtime endpoints: %s", endpoint_snapshot.get("api", {}))

    _print_service_endpoints(ip, web_ui_available)

def _prepare_running_compose_exposure(
    env: dict[str, str],
    *,
    host: str,
    token: Optional[str],
    redis_port: Optional[int],
    docker_network_mode: Optional[str],
    docker_network_name: Optional[str],
) -> tuple[dict[str, str], bool]:
    network_token = token or _resolve_network_token()
    advertised_host = _advertised_network_host(host)
    requested_mode = _docker_network_mode(
        docker_network_mode or os.getenv("MN_DOCKER_NETWORK_MODE", "").strip() or None,
        default="disabled",
    )
    network_name = _docker_network_name(docker_network_name)
    use_internal_identity = _docker_network_uses_internal_identity(requested_mode)
    node_alias = _resolve_node_alias(env) if requested_mode != "disabled" else ""

    if requested_mode != "disabled":
        _ensure_docker_network(requested_mode, network_name)
        env.update(_docker_network_env(requested_mode, network_name, node_alias))
    else:
        env["MN_DOCKER_NETWORK_MODE"] = "disabled"

    local_node_name = _docker_node_name(node_alias) if use_internal_identity else _network_node_name(advertised_host)
    env.update(_shared_storage_env_from_runtime_env(env))
    env = _ensure_syncthing_for_runtime(env, advertised_host=advertised_host)
    env = _ensure_compose_cluster_bind_settings(env, advertised_host)

    if use_internal_identity and node_alias:
        env = _ensure_compose_internal_redis_settings(
            env,
            token=network_token,
            network_redis_host=_docker_redis_alias(node_alias),
            network_redis_port=REDIS_CONTAINER_PORT,
        )
    else:
        env = _ensure_compose_cluster_port_settings(
            env,
            token=network_token,
            advertised_host=advertised_host,
            redis_port=redis_port,
        )

    env["MN_NETWORK_ADVERTISE_HOST"] = advertised_host
    env["MN_NETWORK_JOIN_TOKEN"] = network_token
    if _generated_node_setting_should_update("MN_NODE_NAME", env.get("MN_NODE_NAME"), local_node_name):
        env["MN_NODE_NAME"] = local_node_name
    if _generated_node_setting_should_update(
        "MN_MODEL_SERVICE_NODE_NAME",
        env.get("MN_MODEL_SERVICE_NODE_NAME"),
        env["MN_NODE_NAME"],
    ):
        env["MN_MODEL_SERVICE_NODE_NAME"] = env["MN_NODE_NAME"]
    if _generated_cluster_setting_should_update(env.get("MN_CLUSTER_NODES"), local_node_name):
        env["MN_CLUSTER_NODES"] = local_node_name
    env["MN_NODE_ROLE"] = env.get("MN_NODE_ROLE") or "runtime"
    env["MN_DIST_PORT"] = str(env.get("MN_DIST_PORT") or DEFAULT_DIST_PORT)
    env["MN_COOKIE"] = _derive_network_secret(network_token, "cookie")
    if _erl_aflags_needs_update(env.get("ERL_AFLAGS"), env["MN_DIST_PORT"]):
        env["ERL_AFLAGS"] = _erl_aflags(env["MN_DIST_PORT"])
    env = _ensure_node_advertisement_settings(env)

    _write_env_file_values(
        RUNTIME_COMPOSE_ENV,
        {
            "MN_NETWORK_ADVERTISE_HOST": env["MN_NETWORK_ADVERTISE_HOST"],
            "MN_NETWORK_JOIN_TOKEN": env["MN_NETWORK_JOIN_TOKEN"],
            "MN_NODE_NAME": env["MN_NODE_NAME"],
            "MN_MODEL_SERVICE_NODE_NAME": env["MN_MODEL_SERVICE_NODE_NAME"],
            "MN_CLUSTER_NODES": env["MN_CLUSTER_NODES"],
            "MN_NODE_ROLE": env["MN_NODE_ROLE"],
            "MN_DIST_PORT": env["MN_DIST_PORT"],
            "MN_COOKIE": env["MN_COOKIE"],
            "ERL_AFLAGS": env["ERL_AFLAGS"],
            "MN_DOCKER_NETWORK_MODE": requested_mode,
            "MN_DOCKER_NETWORK_NAME": network_name,
            **{key: env[key] for key in NODE_ADVERTISEMENT_ENV_KEYS if key in env},
            **{key: env[key] for key in SHARED_STORAGE_ENV_KEYS if key in env},
            **{key: env[key] for key in SYNCTHING_ENV_KEYS if key in env},
            **{key: env[key] for key in REDIS_HA_ENV_KEYS if key in env},
        },
    )
    return env, True
