from __future__ import annotations

import logging

import pytest

from mn_cli.config import CliConfig, ConfigError, load_config


def test_env_default_loading(tmp_path):
    (tmp_path / ".env").write_text("MN_API_PORT=8000\nMN_LOG_LEVEL=debug\n", encoding="utf-8")

    config = load_config(env={}, root=tmp_path)

    assert config.get("MN_API_PORT") == 8000
    assert config.get("MN_LOG_LEVEL") == "debug"
    assert tmp_path / ".env" in config.loaded_files


def test_environment_file_overrides_base_env_file(tmp_path):
    (tmp_path / ".env").write_text("MN_API_PORT=8000\n", encoding="utf-8")
    (tmp_path / ".env.test").write_text("MN_API_PORT=9000\n", encoding="utf-8")

    config = load_config(env={"MN_ENV": "test"}, root=tmp_path)

    assert config.get("MN_API_PORT") == 9000


def test_real_environment_overrides_dotenv_values(tmp_path):
    (tmp_path / ".env").write_text("MN_API_PORT=8000\n", encoding="utf-8")

    config = load_config(env={"MN_API_PORT": "8080"}, root=tmp_path)

    assert config.get("MN_API_PORT") == 8080


def test_mn_env_defaults_to_dev_and_loads_env_dev(tmp_path):
    (tmp_path / ".env.dev").write_text("MN_LOG_LEVEL=warning\n", encoding="utf-8")

    config = load_config(env={}, root=tmp_path)

    assert config.mn_env == "dev"
    assert config.get("MN_LOG_LEVEL") == "warning"


@pytest.mark.parametrize("value", ["dev", "development"])
def test_dev_aliases_load_env_dev(tmp_path, value):
    (tmp_path / ".env.dev").write_text("MN_API_HOST=127.0.0.2\n", encoding="utf-8")

    config = load_config(env={"MN_ENV": value}, root=tmp_path)

    assert config.mn_env == "dev"
    assert config.get("MN_API_HOST") == "127.0.0.2"


def test_test_env_loads_env_test(tmp_path):
    (tmp_path / ".env.test").write_text("MN_API_HOST=127.0.0.3\n", encoding="utf-8")

    config = load_config(env={"MN_ENV": "test"}, root=tmp_path)

    assert config.mn_env == "test"
    assert config.get("MN_API_HOST") == "127.0.0.3"


@pytest.mark.parametrize("value", ["prod", "production"])
def test_prod_aliases_load_env_prod_when_present(tmp_path, value):
    (tmp_path / ".env.prod").write_text("MN_API_PORT=8080\n", encoding="utf-8")

    config = load_config(env={"MN_ENV": value}, root=tmp_path)

    assert config.mn_env == "prod"
    assert config.get("MN_API_PORT") == 8080


def test_production_loads_without_dotenv_files(tmp_path):
    config = load_config(env={"MN_ENV": "production", "MN_HOME": "/var/lib/mirrorneuron"}, root=tmp_path)

    assert config.mn_env == "prod"
    assert str(config.get("MN_HOME")) == "/var/lib/mirrorneuron"


def test_missing_required_variable_has_clear_error(tmp_path):
    with pytest.raises(ConfigError, match="Missing required config variable: MN_LLM_API_KEY"):
        load_config(env={}, root=tmp_path, required_keys=("MN_LLM_API_KEY",))


def test_invalid_type_has_clear_error(tmp_path):
    (tmp_path / ".env").write_text("MN_API_PORT=not-a-number\n", encoding="utf-8")

    with pytest.raises(ConfigError, match="Invalid value for MN_API_PORT: expected int"):
        load_config(env={}, root=tmp_path)


def test_typed_parsing_supports_bool_list_url_and_path(tmp_path):
    config = load_config(
        env={
            "MN_DISABLE_UPDATE_CHECK": "yes",
            "MN_ALLOWED_ORIGINS": "http://localhost:5173,https://example.test",
            "MN_API_BASE_URL": "http://localhost:8000/api/v1/",
            "MN_HOME": str(tmp_path / "home"),
            "MN_RUNS_ROOT": "$MN_HOME/runs",
        },
        root=tmp_path,
    )

    assert config.get("MN_DISABLE_UPDATE_CHECK") is True
    assert config.get("MN_ALLOWED_ORIGINS") == ("http://localhost:5173", "https://example.test")
    assert config.get("MN_API_BASE_URL") == "http://localhost:8000/api/v1"
    assert config.get("MN_RUNS_ROOT") == tmp_path / "home" / "runs"


def test_secret_values_are_redacted_from_config_diagnostics(tmp_path, caplog):
    config = load_config(env={"MN_LLM_API_KEY": "super-secret-value"}, root=tmp_path)

    with caplog.at_level(logging.INFO):
        logging.getLogger("mn-cli.test").info("config=%s", config.redacted_values())

    assert "super-secret-value" not in caplog.text
    assert "<redacted>" in caplog.text


def test_cli_config_uses_same_loader_for_dotenv_defaults(tmp_path):
    (tmp_path / ".env.dev").write_text("MN_CLI_OUTPUT=plain\nMN_GRPC_TIMEOUT_SECONDS=3\n", encoding="utf-8")

    config = CliConfig.from_env(env={}, root=tmp_path)

    assert config.output_mode == "plain"
    assert config.grpc_timeout_seconds == 3.0


def test_config_loader_is_reusable_for_api_code(tmp_path):
    config = load_config(env={"MN_ENV": "production", "MN_API_PORT": "8080"}, root=tmp_path, app_name="mn-api")

    assert config.app_name == "mn-api"
    assert config.get("MN_API_PORT") == 8080
