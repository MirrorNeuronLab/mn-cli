# ruff: noqa: I001
"""CLI configuration facade backed by the canonical SDK loader and schema."""

from __future__ import annotations

import importlib
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from mn_cli.sdk_path import add_local_sdk_path

add_local_sdk_path("runtime_config.py")

from mn_sdk.config import (
    SUPPORTED_CONFIG_KEYS as SDK_CONFIG_KEYS,
    ConfigError,
    bootstrap_environment as sdk_bootstrap_environment,
    is_sensitive_key as sdk_is_sensitive_key,
    load_config_source,
    normalize_mn_env,
    parse_bool,
    parse_csv,
    parse_float,
    parse_int,
    parse_path,
    parse_str,
    parse_timeout_seconds,
    parse_url,
    parse_url_or_auto,
)
from mn_sdk.config.types import MISSING
from mn_sdk.runtime_config import RuntimeConfig, default_logs_root


TRUE_VALUES = {"1", "true", "yes", "on"}
FALSE_VALUES = {"0", "false", "no", "off"}
SECRET_MARKERS = ("SECRET", "TOKEN", "PASSWORD", "API_KEY", "COOKIE", "CREDENTIAL")


@dataclass(frozen=True)
class ConfigField:
    name: str
    kind: str = "str"
    default: Any = None
    required: bool = False
    sensitive: bool = False
    choices: frozenset[str] | None = None


def _sdk_config_field(key: Any) -> ConfigField:
    parser_name = getattr(key.parser, "__name__", "parse_str")
    kind = {
        "parse_bool": "bool",
        "parse_csv": "list",
        "parse_float": "float",
        "parse_int": "int",
        "parse_path": "path",
        "parse_timeout_seconds": "float",
        "parse_url": "url",
        "parse_url_or_auto": "url_or_auto",
    }.get(parser_name, "str")
    return ConfigField(
        key.name,
        kind=kind,
        default=None if key.default is MISSING else key.default,
        required=key.required,
        sensitive=key.is_sensitive,
        choices=(
            frozenset(str(choice) for choice in key.choices)
            if key.choices is not None
            else None
        ),
    )


CLI_CONFIG_FIELDS: dict[str, ConfigField] = {
    "MN_CLI_LOG_PATH": ConfigField("MN_CLI_LOG_PATH", kind="path"),
    "MN_CLI_OUTPUT": ConfigField(
        "MN_CLI_OUTPUT", default="rich", choices=frozenset({"rich", "plain"})
    ),
    "MN_REDIS_URL": ConfigField("MN_REDIS_URL", kind="url"),
    "MN_ALLOWED_ORIGINS": ConfigField("MN_ALLOWED_ORIGINS", kind="list", default=()),
    "MN_DISABLE_UPDATE_CHECK": ConfigField(
        "MN_DISABLE_UPDATE_CHECK", kind="bool", default=False
    ),
    "MN_UPDATE_CHECK_INTERVAL_SECONDS": ConfigField(
        "MN_UPDATE_CHECK_INTERVAL_SECONDS", kind="int", default=86400
    ),
    "MN_CORE_REPO": ConfigField("MN_CORE_REPO"),
    "MN_RUN_DETACH_LOG_SECONDS": ConfigField(
        "MN_RUN_DETACH_LOG_SECONDS", kind="float", default=30.0
    ),
    "MN_RUN_LOG_POLL_INTERVAL_SECONDS": ConfigField(
        "MN_RUN_LOG_POLL_INTERVAL_SECONDS", kind="float", default=0.5
    ),
    "MN_RUN_DISABLE_LIVE_SCREEN": ConfigField(
        "MN_RUN_DISABLE_LIVE_SCREEN", kind="bool", default=False
    ),
    "MN_RESOURCE_WAIT_RETRY_MS": ConfigField(
        "MN_RESOURCE_WAIT_RETRY_MS", kind="int", default=30000
    ),
    "MN_BLUEPRINT_RESOURCE_STALE_SECONDS": ConfigField(
        "MN_BLUEPRINT_RESOURCE_STALE_SECONDS", kind="int", default=3600
    ),
    "MN_BLUEPRINT_WEB_UI_BIND_HOST": ConfigField("MN_BLUEPRINT_WEB_UI_BIND_HOST"),
    "MN_BLUEPRINT_WEB_UI_HOST": ConfigField("MN_BLUEPRINT_WEB_UI_HOST"),
    "MN_BLUEPRINT_WEB_UI_PUBLIC_HOST": ConfigField("MN_BLUEPRINT_WEB_UI_PUBLIC_HOST"),
    "MN_BLUEPRINT_WEB_UI_BASE_URL": ConfigField(
        "MN_BLUEPRINT_WEB_UI_BASE_URL", kind="url"
    ),
    "MN_BLUEPRINT_WEB_UI_PORT_START": ConfigField(
        "MN_BLUEPRINT_WEB_UI_PORT_START", kind="int"
    ),
    "MN_BLUEPRINT_WEB_UI_PORT_END": ConfigField(
        "MN_BLUEPRINT_WEB_UI_PORT_END", kind="int"
    ),
    "MN_AUTO_PORT_START": ConfigField("MN_AUTO_PORT_START", kind="int", default=62000),
    "MN_AUTO_PORT_END": ConfigField("MN_AUTO_PORT_END", kind="int", default=62049),
    "MN_MCP_CONTAINER_LOOPBACK_PROXY": ConfigField(
        "MN_MCP_CONTAINER_LOOPBACK_PROXY", kind="bool", default=False
    ),
    "MN_BLUEPRINT_WEB_UI_START_TIMEOUT_SECONDS": ConfigField(
        "MN_BLUEPRINT_WEB_UI_START_TIMEOUT_SECONDS", kind="float", default=5.0
    ),
    "MN_BLUEPRINT_INSTALLS_DIR": ConfigField("MN_BLUEPRINT_INSTALLS_DIR", kind="path"),
    "MN_BLUEPRINT_PYTHON_ENVS_DIR": ConfigField(
        "MN_BLUEPRINT_PYTHON_ENVS_DIR", kind="path"
    ),
    "MN_GENERATED_BLUEPRINT_BUNDLES_DIR": ConfigField(
        "MN_GENERATED_BLUEPRINT_BUNDLES_DIR", kind="path"
    ),
    "MN_BUNDLE_CACHE_DIR": ConfigField("MN_BUNDLE_CACHE_DIR", kind="path"),
    "MN_TEMP_DIR": ConfigField("MN_TEMP_DIR", kind="path"),
    "MN_OUTPUT_HOME": ConfigField("MN_OUTPUT_HOME", kind="path"),
    "MN_USER_HOME": ConfigField("MN_USER_HOME", kind="path"),
    "MN_SYNCTHING_RESCAN_INTERVAL_SECONDS": ConfigField(
        "MN_SYNCTHING_RESCAN_INTERVAL_SECONDS", kind="int", default=3600
    ),
    "MN_HOST_ARTIFACTS_DIR": ConfigField("MN_HOST_ARTIFACTS_DIR", kind="path"),
    "MN_HOST_BLOB_STORE_DIR": ConfigField("MN_HOST_BLOB_STORE_DIR", kind="path"),
    "MN_BLOB_STORE_ROOT": ConfigField("MN_BLOB_STORE_ROOT", kind="path"),
    "MN_ARTIFACT_ADVERTISE_URL": ConfigField("MN_ARTIFACT_ADVERTISE_URL", kind="url"),
    "MN_ARTIFACT_PORT": ConfigField("MN_ARTIFACT_PORT", kind="int"),
    "MN_NODE_NAME": ConfigField("MN_NODE_NAME"),
    "MN_NODE_ALIAS": ConfigField("MN_NODE_ALIAS"),
    "MN_NODE_DISPLAY_NAME": ConfigField("MN_NODE_DISPLAY_NAME"),
    "MN_NODE_GPU_COUNT": ConfigField("MN_NODE_GPU_COUNT", kind="int"),
    "MN_NODE_GPU": ConfigField("MN_NODE_GPU"),
    "MN_NODE_CPU_MODEL": ConfigField("MN_NODE_CPU_MODEL"),
    "MN_NETWORK_ADVERTISE_HOST": ConfigField("MN_NETWORK_ADVERTISE_HOST"),
    "MN_NETWORK_JOIN_TOKEN": ConfigField("MN_NETWORK_JOIN_TOKEN", sensitive=True),
    "MN_COOKIE": ConfigField("MN_COOKIE", sensitive=True),
    "MN_REDIS_PORT": ConfigField("MN_REDIS_PORT", kind="int"),
    "MN_REDIS_HOST": ConfigField("MN_REDIS_HOST"),
    "MN_DOCKER_NETWORK_NAME": ConfigField("MN_DOCKER_NETWORK_NAME"),
    "MN_DOCKER_WORKER_NETWORK": ConfigField("MN_DOCKER_WORKER_NETWORK"),
    "MN_DOCKER_NETWORK_MODE": ConfigField("MN_DOCKER_NETWORK_MODE"),
}

CONFIG_FIELDS: dict[str, ConfigField] = {
    **{key.name: _sdk_config_field(key) for key in SDK_CONFIG_KEYS},
    **CLI_CONFIG_FIELDS,
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
        if name in self.effective_env:
            return self.effective_env[name]
        configured = self.values.get(name)
        return str(configured) if configured is not None else default

    def path(self, name: str, default: Path | None = None) -> Path | None:
        value = self.get(name)
        if value in (None, ""):
            return default
        return value if isinstance(value, Path) else Path(str(value)).expanduser()

    def redacted_values(self) -> dict[str, Any]:
        return {
            name: ("<redacted>" if _is_sensitive(name) else value)
            for name, value in self.values.items()
        }


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
    def from_env(
        cls,
        *,
        env: Mapping[str, str] | None = None,
        root: str | Path | None = None,
    ) -> CliConfig:
        app_config = load_config(env=env, root=root, app_name="mn-cli")
        runtime_config = RuntimeConfig.from_env(
            env=app_config.effective_env,
            env_dir=root,
        )
        log_path = app_config.path("MN_CLI_LOG_PATH")
        return cls(
            grpc_target=runtime_config.grpc_target,
            grpc_timeout_seconds=runtime_config.grpc_timeout_seconds,
            grpc_auth_token=runtime_config.grpc_auth_token,
            grpc_admin_token=runtime_config.grpc_admin_token,
            api_base_url=runtime_config.api_base_url,
            api_token=str(app_config.get("MN_API_TOKEN", "")),
            log_path=log_path
            or (_default_logs_root(app_config.effective_env) / "cli.log"),
            output_mode=str(app_config.get("MN_CLI_OUTPUT", "rich")),
        )


def load_config(
    *,
    env: Mapping[str, str] | None = None,
    root: str | Path | None = None,
    app_name: str = "mirrorneuron",
    required_keys: Iterable[str] = (),
) -> AppConfig:
    source = load_config_source(env=env, env_dir=root)
    values = _parse_config_values(
        source.effective_env,
        required_keys=required_keys,
        explicit_blank_keys={
            name for name, value in source.real_env.items() if not str(value).strip()
        },
    )
    return AppConfig(
        app_name=app_name,
        mn_env=str(values["MN_ENV"]),
        values=values,
        effective_env=source.effective_env,
        loaded_files=source.loaded_files,
    )


def bootstrap_environment(*, root: str | Path | None = None) -> AppConfig:
    sdk_bootstrap_environment(env_dir=root)
    return load_config(root=root)


def supported_config_keys() -> tuple[str, ...]:
    return tuple(CONFIG_FIELDS)


def _build_runtime_config(
    env: Mapping[str, str], *, resolve_tokens: bool
) -> RuntimeConfig:
    del resolve_tokens
    return RuntimeConfig.from_env(env=env)


def _runtime_config_module() -> Any:
    add_local_sdk_path("runtime_config.py")
    return importlib.import_module("mn_sdk.runtime_config")


def _default_logs_root(env: Mapping[str, str] | None = None) -> Path:
    return default_logs_root(env)


def _parse_config_values(
    effective_env: Mapping[str, str],
    *,
    required_keys: Iterable[str],
    explicit_blank_keys: set[str] | None = None,
) -> dict[str, Any]:
    values: dict[str, Any] = {}
    required = set(required_keys)
    explicit_blanks = explicit_blank_keys or set()
    for name, field_spec in CONFIG_FIELDS.items():
        raw_value: Any = effective_env.get(name)
        if name == "MN_ENV":
            raw_value = normalize_mn_env(str(raw_value or "dev"))
        if raw_value in (None, ""):
            if name in explicit_blanks:
                if field_spec.required or name in required:
                    raise ConfigError(f"Missing required config variable: {name}")
                values[name] = ""
                continue
            if field_spec.default is None:
                if field_spec.required or name in required:
                    raise ConfigError(f"Missing required config variable: {name}")
                continue
            raw_value = field_spec.default
        values[name] = _parse_typed_value(field_spec, raw_value, effective_env)

    for name in sorted(required.difference(CONFIG_FIELDS)):
        if not str(effective_env.get(name, "")).strip():
            raise ConfigError(f"Missing required config variable: {name}")
        values[name] = effective_env[name]
    return values


def _parse_typed_value(
    field_spec: ConfigField,
    value: Any,
    effective_env: Mapping[str, str],
) -> Any:
    try:
        raw = str(value)
        if field_spec.kind == "str":
            parsed = parse_str(field_spec.name, raw)
        elif field_spec.kind == "int":
            parsed = parse_int(field_spec.name, raw)
        elif field_spec.kind == "float":
            if field_spec.name == "MN_GRPC_TIMEOUT_SECONDS":
                parsed = parse_timeout_seconds(field_spec.name, raw)
            elif raw.strip().lower() in {"", "none"}:
                parsed = None
            else:
                parsed = parse_float(field_spec.name, raw)
        elif field_spec.kind == "bool":
            parsed = parse_bool(field_spec.name, raw)
        elif field_spec.kind == "list":
            parsed = tuple(parse_csv(field_spec.name, raw))
        elif field_spec.kind == "url":
            parsed = parse_url(field_spec.name, raw)
        elif field_spec.kind == "url_or_auto":
            parsed = parse_url_or_auto(field_spec.name, raw)
        elif field_spec.kind == "path":
            mn_home = str(effective_env.get("MN_HOME") or "")
            expanded = raw.replace("${MN_HOME}", mn_home).replace("$MN_HOME", mn_home)
            parsed = parse_path(field_spec.name, expanded)
        else:
            raise ConfigError(
                f"Unsupported config type for {field_spec.name}: {field_spec.kind}"
            )
    except (ConfigError, ValueError) as exc:
        raise ConfigError(
            f"Invalid value for {field_spec.name}: expected {field_spec.kind}"
        ) from exc

    if field_spec.choices is not None and str(parsed) not in field_spec.choices:
        choices = ", ".join(sorted(field_spec.choices))
        raise ConfigError(
            f"Invalid value for {field_spec.name}: expected one of {choices}"
        )
    return parsed


def _is_sensitive(name: str) -> bool:
    field_spec = CONFIG_FIELDS.get(name)
    return bool(field_spec and field_spec.sensitive) or sdk_is_sensitive_key(name)


__all__ = [
    "CLI_CONFIG_FIELDS",
    "CONFIG_FIELDS",
    "AppConfig",
    "CliConfig",
    "ConfigError",
    "ConfigField",
    "bootstrap_environment",
    "load_config",
    "supported_config_keys",
]
