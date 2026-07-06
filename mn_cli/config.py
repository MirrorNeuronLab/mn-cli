from __future__ import annotations

import importlib
import json
import os
import shlex
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping
from urllib.parse import urlparse

from mn_cli.sdk_path import add_local_sdk_path

add_local_sdk_path("runtime_config.py")


TRUE_VALUES = {"1", "true", "yes", "on"}
FALSE_VALUES = {"0", "false", "no", "off"}
SECRET_MARKERS = ("SECRET", "TOKEN", "PASSWORD", "API_KEY", "COOKIE", "CREDENTIAL")


class ConfigError(ValueError):
    """Raised when MirrorNeuron configuration is missing or invalid."""


@dataclass(frozen=True)
class ConfigField:
    name: str
    kind: str = "str"
    default: Any = None
    required: bool = False
    sensitive: bool = False
    choices: frozenset[str] | None = None


CONFIG_FIELDS: dict[str, ConfigField] = {
    "MN_ENV": ConfigField("MN_ENV", default="dev", choices=frozenset({"dev", "test", "prod"})),
    "MN_HOME": ConfigField("MN_HOME", kind="path"),
    "MN_LOG_LEVEL": ConfigField(
        "MN_LOG_LEVEL",
        default="info",
        choices=frozenset({"debug", "info", "warning", "error", "critical"}),
    ),
    "MN_LOG_MAX_BYTES": ConfigField("MN_LOG_MAX_BYTES", kind="int", default=1048576),
    "MN_LOG_BACKUP_COUNT": ConfigField("MN_LOG_BACKUP_COUNT", kind="int", default=5),
    "MN_CLI_LOG_PATH": ConfigField("MN_CLI_LOG_PATH", kind="path"),
    "MN_CLI_OUTPUT": ConfigField("MN_CLI_OUTPUT", default="rich", choices=frozenset({"rich", "plain"})),
    "MN_GRPC_TARGET": ConfigField("MN_GRPC_TARGET", default="localhost:55051"),
    "MN_GRPC_TIMEOUT_SECONDS": ConfigField("MN_GRPC_TIMEOUT_SECONDS", kind="float", default=10.0),
    "MN_GRPC_AUTH_TOKEN": ConfigField("MN_GRPC_AUTH_TOKEN", sensitive=True),
    "MN_GRPC_ADMIN_TOKEN": ConfigField("MN_GRPC_ADMIN_TOKEN", sensitive=True),
    "MN_GRPC_AUTH_TOKEN_FILE": ConfigField("MN_GRPC_AUTH_TOKEN_FILE", kind="path", sensitive=True),
    "MN_GRPC_ADMIN_TOKEN_FILE": ConfigField("MN_GRPC_ADMIN_TOKEN_FILE", kind="path", sensitive=True),
    "MN_API_BASE_URL": ConfigField("MN_API_BASE_URL", kind="url"),
    "MN_API_HOST": ConfigField("MN_API_HOST", default="localhost"),
    "MN_API_PORT": ConfigField("MN_API_PORT", kind="int", default=54001),
    "MN_WEB_UI_URL": ConfigField("MN_WEB_UI_URL", kind="url"),
    "MN_WEB_UI_HOST": ConfigField("MN_WEB_UI_HOST", default="localhost"),
    "MN_WEB_UI_PORT": ConfigField("MN_WEB_UI_PORT", kind="int", default=55173),
    "MN_REDIS_URL": ConfigField("MN_REDIS_URL", kind="url"),
    "MN_ALLOWED_ORIGINS": ConfigField("MN_ALLOWED_ORIGINS", kind="list", default=()),
    "MN_DISABLE_UPDATE_CHECK": ConfigField("MN_DISABLE_UPDATE_CHECK", kind="bool", default=False),
    "MN_UPDATE_CHECK_INTERVAL_SECONDS": ConfigField("MN_UPDATE_CHECK_INTERVAL_SECONDS", kind="int", default=86400),
    "MN_CORE_REPO": ConfigField("MN_CORE_REPO"),
    "MN_RUN_DETACH_LOG_SECONDS": ConfigField("MN_RUN_DETACH_LOG_SECONDS", kind="float", default=30.0),
    "MN_RUN_LOG_POLL_INTERVAL_SECONDS": ConfigField("MN_RUN_LOG_POLL_INTERVAL_SECONDS", kind="float", default=0.5),
    "MN_RUN_DISABLE_LIVE_SCREEN": ConfigField("MN_RUN_DISABLE_LIVE_SCREEN", kind="bool", default=False),
    "MN_RUN_BACKGROUND_EVENT_RELAY": ConfigField("MN_RUN_BACKGROUND_EVENT_RELAY", kind="bool", default=True),
    "MN_RUN_EVENT_RELAY_POLL_SECONDS": ConfigField("MN_RUN_EVENT_RELAY_POLL_SECONDS", kind="float"),
    "MN_RUN_EVENT_RELAY_MAX_SECONDS": ConfigField("MN_RUN_EVENT_RELAY_MAX_SECONDS", kind="float"),
    "MN_RESOURCE_WAIT_RETRY_MS": ConfigField("MN_RESOURCE_WAIT_RETRY_MS", kind="int", default=30000),
    "MN_PRE_LAUNCH_TIMEOUT_SECONDS": ConfigField("MN_PRE_LAUNCH_TIMEOUT_SECONDS", kind="float", default=30.0),
    "MN_POST_LAUNCH_TIMEOUT_SECONDS": ConfigField("MN_POST_LAUNCH_TIMEOUT_SECONDS", kind="float", default=10.0),
    "MN_BLUEPRINT_RESOURCE_STALE_SECONDS": ConfigField("MN_BLUEPRINT_RESOURCE_STALE_SECONDS", kind="int", default=3600),
    "MN_BLUEPRINT_WEB_UI_BIND_HOST": ConfigField("MN_BLUEPRINT_WEB_UI_BIND_HOST"),
    "MN_BLUEPRINT_WEB_UI_HOST": ConfigField("MN_BLUEPRINT_WEB_UI_HOST"),
    "MN_BLUEPRINT_WEB_UI_PUBLIC_HOST": ConfigField("MN_BLUEPRINT_WEB_UI_PUBLIC_HOST"),
    "MN_BLUEPRINT_WEB_UI_BASE_URL": ConfigField("MN_BLUEPRINT_WEB_UI_BASE_URL", kind="url"),
    "MN_BLUEPRINT_WEB_UI_PORT_START": ConfigField("MN_BLUEPRINT_WEB_UI_PORT_START", kind="int"),
    "MN_BLUEPRINT_WEB_UI_PORT_END": ConfigField("MN_BLUEPRINT_WEB_UI_PORT_END", kind="int"),
    "MN_BLUEPRINT_WEB_UI_START_TIMEOUT_SECONDS": ConfigField("MN_BLUEPRINT_WEB_UI_START_TIMEOUT_SECONDS", kind="float", default=5.0),
    "MN_BLUEPRINT_SOURCE": ConfigField("MN_BLUEPRINT_SOURCE"),
    "MN_BLUEPRINT_REPO": ConfigField("MN_BLUEPRINT_REPO"),
    "MN_BLUEPRINT_LOCAL": ConfigField("MN_BLUEPRINT_LOCAL", kind="path"),
    "MN_BLUEPRINT_INSTALLS_DIR": ConfigField("MN_BLUEPRINT_INSTALLS_DIR", kind="path"),
    "MN_BLUEPRINT_PYTHON_ENVS_DIR": ConfigField("MN_BLUEPRINT_PYTHON_ENVS_DIR", kind="path"),
    "MN_GENERATED_BLUEPRINT_BUNDLES_DIR": ConfigField("MN_GENERATED_BLUEPRINT_BUNDLES_DIR", kind="path"),
    "MN_BUNDLE_CACHE_DIR": ConfigField("MN_BUNDLE_CACHE_DIR", kind="path"),
    "MN_TEMP_DIR": ConfigField("MN_TEMP_DIR", kind="path"),
    "MN_RUNS_ROOT": ConfigField("MN_RUNS_ROOT", kind="path"),
    "MN_LOGS_ROOT": ConfigField("MN_LOGS_ROOT", kind="path"),
    "MN_OUTPUT_HOME": ConfigField("MN_OUTPUT_HOME", kind="path"),
    "MN_USER_HOME": ConfigField("MN_USER_HOME", kind="path"),
    "MN_WORKSPACE_ROOT": ConfigField("MN_WORKSPACE_ROOT", kind="path"),
    "MN_SKILLS_ROOT": ConfigField("MN_SKILLS_ROOT", kind="path"),
    "MN_AGENTS_ROOT": ConfigField("MN_AGENTS_ROOT", kind="path"),
    "MN_SHARED_STORAGE_ROOT": ConfigField("MN_SHARED_STORAGE_ROOT", kind="path"),
    "MN_HOST_SHARED_STORAGE_ROOT": ConfigField("MN_HOST_SHARED_STORAGE_ROOT", kind="path"),
    "MN_RUNTIME_SHARED_STORAGE_ROOT": ConfigField("MN_RUNTIME_SHARED_STORAGE_ROOT", kind="path"),
    "MN_CONTAINER_SHARED_STORAGE_ROOT": ConfigField("MN_CONTAINER_SHARED_STORAGE_ROOT", kind="path"),
    "MN_HOST_ARTIFACTS_DIR": ConfigField("MN_HOST_ARTIFACTS_DIR", kind="path"),
    "MN_HOST_BLOB_STORE_DIR": ConfigField("MN_HOST_BLOB_STORE_DIR", kind="path"),
    "MN_BLOB_STORE_ROOT": ConfigField("MN_BLOB_STORE_ROOT", kind="path"),
    "MN_ARTIFACT_ADVERTISE_URL": ConfigField("MN_ARTIFACT_ADVERTISE_URL", kind="url"),
    "MN_ARTIFACT_PORT": ConfigField("MN_ARTIFACT_PORT", kind="int"),
    "MN_INLINE_PAYLOAD_MAX_BYTES": ConfigField("MN_INLINE_PAYLOAD_MAX_BYTES", kind="int"),
    "MN_NODE_NAME": ConfigField("MN_NODE_NAME"),
    "MN_NODE_ALIAS": ConfigField("MN_NODE_ALIAS"),
    "MN_NODE_DISPLAY_NAME": ConfigField("MN_NODE_DISPLAY_NAME"),
    "MN_NODE_GPU_COUNT": ConfigField("MN_NODE_GPU_COUNT", kind="int"),
    "MN_NODE_GPU": ConfigField("MN_NODE_GPU"),
    "MN_NODE_CPU_MODEL": ConfigField("MN_NODE_CPU_MODEL"),
    "MN_NETWORK_ADVERTISE_HOST": ConfigField("MN_NETWORK_ADVERTISE_HOST"),
    "MN_NETWORK_JOIN_TOKEN": ConfigField("MN_NETWORK_JOIN_TOKEN", sensitive=True),
    "MN_API_TOKEN": ConfigField("MN_API_TOKEN", sensitive=True),
    "MN_COOKIE": ConfigField("MN_COOKIE", sensitive=True),
    "MN_REDIS_PORT": ConfigField("MN_REDIS_PORT", kind="int"),
    "MN_REDIS_HOST": ConfigField("MN_REDIS_HOST"),
    "MN_DOCKER_NETWORK_NAME": ConfigField("MN_DOCKER_NETWORK_NAME"),
    "MN_DOCKER_WORKER_NETWORK": ConfigField("MN_DOCKER_WORKER_NETWORK"),
    "MN_DOCKER_NETWORK_MODE": ConfigField("MN_DOCKER_NETWORK_MODE"),
    "MN_LLM_PROVIDER": ConfigField("MN_LLM_PROVIDER"),
    "MN_LLM_MODEL": ConfigField("MN_LLM_MODEL"),
    "MN_LLM_RUNTIME_MODEL": ConfigField("MN_LLM_RUNTIME_MODEL"),
    "MN_LLM_API_BASE": ConfigField("MN_LLM_API_BASE", kind="url"),
    "MN_LLM_API_KEY": ConfigField("MN_LLM_API_KEY", sensitive=True),
    "MN_LLM_TIMEOUT_SECONDS": ConfigField("MN_LLM_TIMEOUT_SECONDS", kind="float"),
    "MN_LLM_MAX_TOKENS": ConfigField("MN_LLM_MAX_TOKENS", kind="int"),
    "MN_LLM_NUM_RETRIES": ConfigField("MN_LLM_NUM_RETRIES", kind="int"),
    "MN_LLM_RETRY_BACKOFF_SECONDS": ConfigField("MN_LLM_RETRY_BACKOFF_SECONDS", kind="float"),
}


@dataclass(frozen=True)
class AppConfig:
    app_name: str
    mn_env: str
    values: Mapping[str, Any]
    effective_env: Mapping[str, str]
    loaded_files: tuple[Path, ...] = field(default_factory=tuple)

    def get(self, name: str, default: Any = None) -> Any:
        if name in self.values:
            return self.values[name]
        return self.effective_env.get(name, default)

    def env(self, name: str, default: str = "") -> str:
        value = self.effective_env.get(name)
        if value is not None:
            return value
        configured = self.values.get(name)
        return str(configured) if configured is not None else default

    def path(self, name: str, default: Path | None = None) -> Path | None:
        value = self.get(name)
        if value is None:
            return default
        return value if isinstance(value, Path) else Path(str(value)).expanduser()

    def redacted_values(self) -> dict[str, Any]:
        redacted: dict[str, Any] = {}
        for name, value in self.values.items():
            redacted[name] = "<redacted>" if _is_sensitive(name) else value
        return redacted


@dataclass(frozen=True)
class CliConfig:
    grpc_target: str = "localhost:55051"
    grpc_timeout_seconds: float | None = 10.0
    grpc_auth_token: str = ""
    grpc_admin_token: str = ""
    api_base_url: str = ""
    api_token: str = ""
    log_path: Path = field(default_factory=lambda: _default_logs_root() / "cli.log")
    output_mode: str = "rich"

    @classmethod
    def from_env(cls, *, env: Mapping[str, str] | None = None, root: str | Path | None = None) -> "CliConfig":
        app_config = load_config(env=env, root=root, app_name="mn-cli")
        runtime_config = _build_runtime_config(
            app_config.effective_env,
            resolve_tokens=True,
        )
        log_path = app_config.path("MN_CLI_LOG_PATH")
        return cls(
            grpc_target=runtime_config.grpc_target,
            grpc_timeout_seconds=runtime_config.grpc_timeout_seconds,
            grpc_auth_token=runtime_config.grpc_auth_token,
            grpc_admin_token=runtime_config.grpc_admin_token,
            api_base_url=runtime_config.api_base_url,
            api_token=str(app_config.get("MN_API_TOKEN", "")),
            log_path=log_path or (_default_logs_root(app_config.effective_env) / "cli.log"),
            output_mode=str(app_config.get("MN_CLI_OUTPUT", "rich")),
        )


def load_config(
    *,
    env: Mapping[str, str] | None = None,
    root: str | Path | None = None,
    app_name: str = "mirrorneuron",
    required_keys: Iterable[str] = (),
) -> AppConfig:
    real_env = {str(key): str(value) for key, value in (env if env is not None else os.environ).items()}
    config_root = Path(root).expanduser() if root is not None else Path.cwd()
    mn_env = _normalize_mn_env(real_env.get("MN_ENV"))
    loaded_files, dotenv_values = _load_dotenv_layers(config_root, mn_env)
    effective_env = {**dotenv_values, **real_env}
    values = _parse_config_values(effective_env, required_keys=required_keys)
    return AppConfig(
        app_name=app_name,
        mn_env=str(values["MN_ENV"]),
        values=values,
        effective_env=effective_env,
        loaded_files=tuple(loaded_files),
    )


def bootstrap_environment(*, root: str | Path | None = None) -> AppConfig:
    """Load dotenv defaults into os.environ without overwriting real env vars."""
    real_keys = set(os.environ)
    config_root = Path(root).expanduser() if root is not None else Path.cwd()
    mn_env = _normalize_mn_env(os.environ.get("MN_ENV"))
    _, dotenv_values = _load_dotenv_layers(config_root, mn_env)
    for key, value in dotenv_values.items():
        if key not in real_keys:
            os.environ[key] = value
    return load_config(root=config_root)


def supported_config_keys() -> tuple[str, ...]:
    return tuple(CONFIG_FIELDS)


def _build_runtime_config(env: Mapping[str, str], *, resolve_tokens: bool) -> Any:
    module = _runtime_config_module()
    home = _runtime_mn_home(env)
    runtime_env = _read_runtime_env_file(home / "docker-compose.env")
    runtime_endpoints = _read_json_object(home / "runtime-endpoints.json")
    shared_root, runtime_shared_root, shared_configured = _resolve_shared_storage_roots(
        env,
        runtime_env,
        home,
    )
    web_ui_url, web_ui_advertised = _resolve_web_ui_url(env, runtime_env, runtime_endpoints)
    auth_token = getattr(module, "DEFAULT_GRPC_AUTH_TOKEN", "")
    admin_token = getattr(module, "DEFAULT_GRPC_ADMIN_TOKEN", "")
    if resolve_tokens:
        auth_token = _resolve_token("MN_GRPC_AUTH_TOKEN", "grpc_auth.token", env, runtime_env, home)
        admin_token = _resolve_token("MN_GRPC_ADMIN_TOKEN", "grpc_admin.token", env, runtime_env, home)
    return module.RuntimeConfig(
        mn_home=home,
        runtime_env=runtime_env,
        runtime_endpoints=runtime_endpoints,
        grpc_target=_resolve_grpc_target(env, runtime_env, runtime_endpoints),
        api_base_url=_resolve_api_base_url(env, runtime_env, runtime_endpoints),
        web_ui_url=web_ui_url,
        web_ui_advertised=web_ui_advertised,
        grpc_timeout_seconds=_resolve_grpc_timeout(env, runtime_env),
        grpc_auth_token=auth_token,
        grpc_admin_token=admin_token,
        shared_storage_root=shared_root,
        runtime_shared_storage_root=runtime_shared_root,
        shared_storage_configured=shared_configured,
    )


def _runtime_config_module() -> Any:
    add_local_sdk_path("runtime_config.py")
    return importlib.import_module("mn_sdk.runtime_config")


def _default_logs_root(env: Mapping[str, str] | None = None) -> Path:
    module = _runtime_config_module()
    if hasattr(module, "default_logs_root"):
        return module.default_logs_root(env)
    values = env if env is not None else os.environ
    configured_logs = str(values.get("MN_LOGS_ROOT") or "").strip()
    configured_home = str(values.get("MN_HOME") or "").strip()
    home = Path(configured_home).expanduser() if configured_home else Path(str(values.get("HOME") or Path.home())).expanduser() / ".mn"
    if configured_logs:
        return _parse_path(configured_logs, {**values, "MN_HOME": str(home)})
    return home / "logs"


def _runtime_mn_home(env: Mapping[str, str]) -> Path:
    configured_home = _env_value(env, "MN_HOME")
    if configured_home:
        return Path(configured_home).expanduser()
    configured_user_home = _env_value(env, "HOME")
    if configured_user_home:
        return Path(configured_user_home).expanduser() / ".mn"
    return Path.home() / ".mn"


def _read_runtime_env_file(path: Path) -> dict[str, str]:
    parsed = _read_dotenv(path)
    return parsed or {}


def _read_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.expanduser().read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _resolve_grpc_target(
    env: Mapping[str, str],
    runtime_env: Mapping[str, str],
    runtime_endpoints: Mapping[str, Any],
) -> str:
    explicit_target = _first_env(env, "MN_GRPC_TARGET")
    if explicit_target:
        return explicit_target
    if _has_any_env(env, "MN_CORE_HOST", "MN_GRPC_PORT"):
        host = _env_value(env, "MN_CORE_HOST") or _env_value(runtime_env, "MN_CORE_HOST") or "localhost"
        port = _env_value(env, "MN_GRPC_PORT") or _env_value(runtime_env, "MN_GRPC_PORT") or "55051"
        return f"{host}:{port}"
    endpoint_target = _grpc_target_from_endpoints(runtime_endpoints)
    if endpoint_target:
        return endpoint_target
    runtime_target = _first_env(runtime_env, "MN_GRPC_TARGET")
    if runtime_target:
        return runtime_target
    host = _env_value(runtime_env, "MN_CORE_HOST") or "localhost"
    port = _env_value(runtime_env, "MN_GRPC_PORT") or "55051"
    return f"{host}:{port}"


def _resolve_api_base_url(
    env: Mapping[str, str],
    runtime_env: Mapping[str, str],
    runtime_endpoints: Mapping[str, Any],
) -> str:
    explicit_url = _env_value(env, "MN_API_BASE_URL")
    if explicit_url:
        return explicit_url.rstrip("/")
    if _has_any_env(env, "MN_API_HOST", "MN_API_PORT"):
        host = _env_value(env, "MN_API_HOST") or _env_value(runtime_env, "MN_API_HOST") or "localhost"
        port = _env_value(env, "MN_API_PORT") or _env_value(runtime_env, "MN_API_PORT") or "54001"
        return f"http://{host}:{port}/api/v1"
    endpoint_url = _endpoint_url(runtime_endpoints, "api", "base_url", suffix="/api/v1")
    if endpoint_url:
        return endpoint_url.rstrip("/")
    runtime_url = _env_value(runtime_env, "MN_API_BASE_URL")
    if runtime_url:
        return runtime_url.rstrip("/")
    host = _env_value(runtime_env, "MN_API_HOST") or "localhost"
    port = _env_value(runtime_env, "MN_API_PORT") or "54001"
    return f"http://{host}:{port}/api/v1"


def _resolve_web_ui_url(
    env: Mapping[str, str],
    runtime_env: Mapping[str, str],
    runtime_endpoints: Mapping[str, Any],
) -> tuple[str, bool]:
    explicit_url = _env_value(env, "MN_WEB_UI_URL")
    if explicit_url:
        return explicit_url.rstrip("/"), True
    if _has_any_env(env, "MN_WEB_UI_HOST", "MN_WEB_UI_PORT"):
        host = _env_value(env, "MN_WEB_UI_HOST") or _env_value(runtime_env, "MN_WEB_UI_HOST") or "localhost"
        port = _env_value(env, "MN_WEB_UI_PORT") or _env_value(runtime_env, "MN_WEB_UI_PORT") or "55173"
        return f"http://{host}:{port}", True
    endpoint_url = _endpoint_url(runtime_endpoints, "web_ui", "url")
    if endpoint_url:
        return endpoint_url.rstrip("/"), True
    runtime_url = _env_value(runtime_env, "MN_WEB_UI_URL")
    if runtime_url:
        return runtime_url.rstrip("/"), True
    host = _env_value(runtime_env, "MN_WEB_UI_HOST") or "localhost"
    port = _env_value(runtime_env, "MN_WEB_UI_PORT") or "55173"
    return f"http://{host}:{port}", False


def _resolve_grpc_timeout(env: Mapping[str, str], runtime_env: Mapping[str, str]) -> float | None:
    value = _env_value(env, "MN_GRPC_TIMEOUT_SECONDS") or _env_value(runtime_env, "MN_GRPC_TIMEOUT_SECONDS") or "10"
    if value.lower() in {"", "none", "0"}:
        return None
    try:
        resolved = float(value)
    except ValueError as exc:
        raise ConfigError("MN_GRPC_TIMEOUT_SECONDS must be a number, 0, or none") from exc
    if resolved < 0:
        raise ConfigError("MN_GRPC_TIMEOUT_SECONDS must be a positive number, 0, or none")
    return resolved


def _resolve_shared_storage_roots(
    env: Mapping[str, str],
    runtime_env: Mapping[str, str],
    mn_home: Path,
) -> tuple[str, str, bool]:
    host_root = _first_env(env, "MN_HOST_SHARED_STORAGE_ROOT", "MN_SHARED_STORAGE_ROOT")
    if not host_root:
        host_root = _first_env(runtime_env, "MN_HOST_SHARED_STORAGE_ROOT", "MN_SHARED_STORAGE_ROOT")
    configured = bool(host_root)
    if not host_root:
        runs_root = _env_value(env, "MN_RUNS_ROOT") or _env_value(runtime_env, "MN_RUNS_ROOT")
        if runs_root:
            host_root = str((Path(runs_root).expanduser().resolve().parent / "shared").resolve())
        else:
            host_root = str(mn_home / "shared")

    runtime_root = _first_env(env, "MN_RUNTIME_SHARED_STORAGE_ROOT", "MN_CONTAINER_SHARED_STORAGE_ROOT")
    if not runtime_root:
        runtime_root = _first_env(runtime_env, "MN_RUNTIME_SHARED_STORAGE_ROOT", "MN_CONTAINER_SHARED_STORAGE_ROOT")
    return host_root, (runtime_root or host_root).rstrip("/") or "/", configured


def _resolve_token(
    name: str,
    filename: str,
    env: Mapping[str, str],
    runtime_env: Mapping[str, str],
    mn_home: Path,
) -> str:
    token = _env_value(env, name)
    if token:
        return token
    for file_env in (f"{name}_FILE",):
        token = _read_text_stripped(_env_value(env, file_env))
        if token:
            return token
        token = _read_text_stripped(_env_value(runtime_env, file_env))
        if token:
            return token
    token = _read_text_stripped(str(mn_home / filename))
    if token:
        return token
    return _env_value(runtime_env, name)


def _read_text_stripped(path: str) -> str:
    if not path:
        return ""
    try:
        return Path(path).expanduser().read_text(encoding="utf-8").strip()
    except OSError:
        return ""


def _grpc_target_from_endpoints(runtime_endpoints: Mapping[str, Any]) -> str:
    grpc = runtime_endpoints.get("grpc") if isinstance(runtime_endpoints, Mapping) else None
    if not isinstance(grpc, Mapping):
        return ""
    target = str(grpc.get("target") or "").strip()
    if target:
        return target
    host = str(grpc.get("host") or "").strip()
    port = str(grpc.get("port") or "").strip()
    return f"{host}:{port}" if host and port else ""


def _endpoint_url(runtime_endpoints: Mapping[str, Any], section_name: str, url_name: str, *, suffix: str = "") -> str:
    section = runtime_endpoints.get(section_name) if isinstance(runtime_endpoints, Mapping) else None
    if not isinstance(section, Mapping):
        return ""
    url = str(section.get(url_name) or "").strip()
    if url:
        return url
    host = str(section.get("host") or "").strip()
    port = str(section.get("port") or "").strip()
    return f"http://{host}:{port}{suffix}" if host and port else ""


def _first_env(env: Mapping[str, Any], *names: str) -> str:
    for name in names:
        value = _env_value(env, name)
        if value:
            return value
    return ""


def _has_any_env(env: Mapping[str, Any], *names: str) -> bool:
    return any(_env_value(env, name) for name in names)


def _env_value(env: Mapping[str, Any], name: str) -> str:
    value = env.get(name)
    return str(value).strip() if value is not None else ""



def _load_dotenv_layers(root: Path, mn_env: str) -> tuple[list[Path], dict[str, str]]:
    values: dict[str, str] = {}
    loaded: list[Path] = []
    for path in (root / ".env", root / f".env.{_dotenv_suffix(mn_env)}"):
        parsed = _read_dotenv(path)
        if parsed is None:
            continue
        loaded.append(path)
        values.update(parsed)
    return loaded, values


def _read_dotenv(path: Path) -> dict[str, str] | None:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return None

    values: dict[str, str] = {}
    for line in lines:
        parsed = _parse_dotenv_line(line)
        if parsed is None:
            continue
        key, value = parsed
        values[key] = value
    return values


def _parse_dotenv_line(line: str) -> tuple[str, str] | None:
    stripped = line.strip()
    if not stripped or stripped.startswith("#") or "=" not in stripped:
        return None
    if stripped.startswith("export "):
        stripped = stripped[len("export ") :].strip()
    key, value = stripped.split("=", 1)
    key = key.strip()
    if not key:
        return None
    return key, _parse_dotenv_value(value)


def _parse_dotenv_value(value: str) -> str:
    value = value.strip()
    if not value:
        return ""
    try:
        parts = shlex.split(value, comments=True, posix=True)
    except ValueError:
        return value.strip("\"'")
    return parts[0] if parts else ""


def _parse_config_values(
    effective_env: Mapping[str, str],
    *,
    required_keys: Iterable[str],
) -> dict[str, Any]:
    values: dict[str, Any] = {}
    required = set(required_keys)
    for name, field_spec in CONFIG_FIELDS.items():
        raw_value = effective_env.get(name)
        if name == "MN_ENV":
            raw_value = _normalize_mn_env(raw_value)
        if raw_value in (None, ""):
            if field_spec.default is None:
                if field_spec.required or name in required:
                    raise ConfigError(f"Missing required config variable: {name}")
                continue
            raw_value = field_spec.default
        values[name] = _parse_typed_value(field_spec, raw_value, effective_env)

    unknown_required = required.difference(CONFIG_FIELDS)
    for name in sorted(unknown_required):
        if not str(effective_env.get(name, "")).strip():
            raise ConfigError(f"Missing required config variable: {name}")
        values[name] = effective_env[name]

    return values


def _parse_typed_value(field_spec: ConfigField, value: Any, effective_env: Mapping[str, str]) -> Any:
    try:
        if field_spec.kind == "str":
            parsed = str(value).strip()
        elif field_spec.kind == "int":
            parsed = int(str(value).strip())
        elif field_spec.kind == "float":
            raw = str(value).strip().lower()
            parsed = None if raw in {"", "none"} else float(raw)
        elif field_spec.kind == "bool":
            parsed = _parse_bool(value, field_spec.name)
        elif field_spec.kind == "list":
            parsed = _parse_list(value)
        elif field_spec.kind == "url":
            parsed = _parse_url(value, field_spec.name)
        elif field_spec.kind == "path":
            parsed = _parse_path(value, effective_env)
        else:
            raise ConfigError(f"Unsupported config type for {field_spec.name}: {field_spec.kind}")
    except ValueError as exc:
        raise ConfigError(f"Invalid value for {field_spec.name}: expected {field_spec.kind}") from exc

    if field_spec.choices is not None and parsed not in field_spec.choices:
        choices = ", ".join(sorted(field_spec.choices))
        raise ConfigError(f"Invalid value for {field_spec.name}: expected one of {choices}")
    return parsed


def _parse_bool(value: Any, name: str) -> bool:
    raw = str(value).strip().lower()
    if raw in TRUE_VALUES:
        return True
    if raw in FALSE_VALUES:
        return False
    raise ConfigError(f"Invalid value for {name}: expected bool")


def _parse_list(value: Any) -> tuple[str, ...]:
    if isinstance(value, (list, tuple)):
        return tuple(str(item).strip() for item in value if str(item).strip())
    return tuple(item.strip() for item in str(value).split(",") if item.strip())


def _parse_url(value: Any, name: str) -> str:
    raw = str(value).strip()
    if not raw:
        return ""
    parsed = urlparse(raw)
    if not parsed.scheme or not parsed.netloc:
        raise ConfigError(f"Invalid value for {name}: expected URL")
    return raw.rstrip("/")


def _parse_path(value: Any, effective_env: Mapping[str, str]) -> Path:
    raw = str(value).strip()
    mn_home = effective_env.get("MN_HOME")
    if mn_home:
        raw = raw.replace("${MN_HOME}", mn_home).replace("$MN_HOME", mn_home)
    return Path(os.path.expandvars(raw)).expanduser()


def _normalize_mn_env(value: str | None) -> str:
    raw = str(value or "dev").strip().lower()
    if raw in {"", "dev", "development"}:
        return "dev"
    if raw in {"prod", "production"}:
        return "prod"
    if raw == "test":
        return "test"
    return raw


def _dotenv_suffix(mn_env: str) -> str:
    return "prod" if mn_env in {"prod", "production"} else _normalize_mn_env(mn_env)


def _is_sensitive(name: str) -> bool:
    field_spec = CONFIG_FIELDS.get(name)
    if field_spec and field_spec.sensitive:
        return True
    return any(marker in name.upper() for marker in SECRET_MARKERS)
