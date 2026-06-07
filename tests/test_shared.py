from __future__ import annotations

import importlib
import sys
from types import SimpleNamespace

import pytest


@pytest.fixture(autouse=True)
def restore_shared_module():
    yield
    sys.modules.pop("mn_cli.shared", None)


def _fresh_shared(monkeypatch, tmp_path, client_class):
    sys.modules.pop("mn_cli.shared", None)
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("MN_CORE_GRPC_TARGET", raising=False)
    monkeypatch.delenv("MN_CORE_HOST", raising=False)
    monkeypatch.delenv("MN_HOME", raising=False)
    monkeypatch.delenv("MIRROR_NEURON_HOME", raising=False)
    monkeypatch.delenv("MN_GRPC_AUTH_TOKEN", raising=False)
    monkeypatch.delenv("MN_GRPC_AUTH_TOKEN_FILE", raising=False)
    monkeypatch.delenv("MN_GRPC_TARGET", raising=False)
    monkeypatch.delenv("MN_GRPC_ADMIN_TOKEN_FILE", raising=False)
    monkeypatch.delenv("MN_GRPC_TIMEOUT_SECONDS", raising=False)
    monkeypatch.setitem(sys.modules, "mn_sdk", SimpleNamespace(Client=client_class))
    return importlib.import_module("mn_cli.shared")


def test_shared_client_omits_admin_token_for_older_sdk(monkeypatch, tmp_path):
    calls = []

    class OldClient:
        def __init__(self, target=None, timeout=None, auth_token=None):
            calls.append(
                {
                    "target": target,
                    "timeout": timeout,
                    "auth_token": auth_token,
                }
            )

    monkeypatch.setenv("MN_GRPC_ADMIN_TOKEN", "admin-secret")

    _fresh_shared(monkeypatch, tmp_path, OldClient)

    assert calls == [
        {
            "target": "localhost:55051",
            "timeout": 10.0,
            "auth_token": "",
        }
    ]


def test_shared_client_passes_admin_token_for_current_sdk(monkeypatch, tmp_path):
    calls = []

    class CurrentClient:
        def __init__(self, target=None, timeout=None, auth_token=None, admin_token=None):
            calls.append(
                {
                    "target": target,
                    "timeout": timeout,
                    "auth_token": auth_token,
                    "admin_token": admin_token,
                }
            )

    monkeypatch.setenv("MN_GRPC_ADMIN_TOKEN", "admin-secret")

    _fresh_shared(monkeypatch, tmp_path, CurrentClient)

    assert calls == [
        {
            "target": "localhost:55051",
            "timeout": 10.0,
            "auth_token": "",
            "admin_token": "admin-secret",
        }
    ]


def test_shared_client_reads_runtime_env_target_and_tokens(monkeypatch, tmp_path):
    calls = []
    monkeypatch.delenv("MN_GRPC_ADMIN_TOKEN", raising=False)
    monkeypatch.delenv("MN_MIRROR_NEURON_GRPC_ADMIN_TOKEN", raising=False)
    state_dir = tmp_path / ".mn"
    state_dir.mkdir()
    (state_dir / "docker-compose.env").write_text(
        "\n".join(
            [
                "MN_GRPC_PORT=55111",
                "MN_CORE_GRPC_TARGET=127.0.0.1:55111",
                "MN_GRPC_AUTH_TOKEN=auth-from-state",
                "MN_GRPC_ADMIN_TOKEN=admin-from-state",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    class CurrentClient:
        def __init__(self, target=None, timeout=None, auth_token=None, admin_token=None):
            calls.append(
                {
                    "target": target,
                    "timeout": timeout,
                    "auth_token": auth_token,
                    "admin_token": admin_token,
                }
            )

    _fresh_shared(monkeypatch, tmp_path, CurrentClient)

    assert calls == [
        {
            "target": "127.0.0.1:55111",
            "timeout": 10.0,
            "auth_token": "auth-from-state",
            "admin_token": "admin-from-state",
        }
    ]


def test_shared_client_prefers_runtime_endpoint_over_stale_core_target(monkeypatch, tmp_path):
    calls = []
    monkeypatch.delenv("MN_GRPC_ADMIN_TOKEN", raising=False)
    monkeypatch.delenv("MN_MIRROR_NEURON_GRPC_ADMIN_TOKEN", raising=False)
    state_dir = tmp_path / ".mn"
    state_dir.mkdir()
    (state_dir / "docker-compose.env").write_text(
        "MN_CORE_GRPC_TARGET=localhost:55051\n"
        "MN_GRPC_AUTH_TOKEN=auth-from-state\n",
        encoding="utf-8",
    )
    (state_dir / "runtime-endpoints.json").write_text(
        '{"grpc":{"target":"192.168.4.20:55051"}}\n',
        encoding="utf-8",
    )

    class CurrentClient:
        def __init__(self, target=None, timeout=None, auth_token=None, admin_token=None):
            calls.append(
                {
                    "target": target,
                    "timeout": timeout,
                    "auth_token": auth_token,
                    "admin_token": admin_token,
                }
            )

    _fresh_shared(monkeypatch, tmp_path, CurrentClient)

    assert calls == [
        {
            "target": "192.168.4.20:55051",
            "timeout": 10.0,
            "auth_token": "auth-from-state",
            "admin_token": "",
        }
    ]


def test_shared_client_reads_refreshed_token_files(monkeypatch, tmp_path):
    calls = []
    state_dir = tmp_path / ".mn"
    state_dir.mkdir()
    (state_dir / "grpc_auth.token").write_text("auth-from-file\n", encoding="utf-8")
    (state_dir / "grpc_admin.token").write_text("admin-from-file\n", encoding="utf-8")
    monkeypatch.delenv("MN_GRPC_ADMIN_TOKEN", raising=False)
    monkeypatch.delenv("MN_MIRROR_NEURON_GRPC_ADMIN_TOKEN", raising=False)

    class CurrentClient:
        def __init__(self, target=None, timeout=None, auth_token=None, admin_token=None):
            calls.append(
                {
                    "target": target,
                    "timeout": timeout,
                    "auth_token": auth_token,
                    "admin_token": admin_token,
                }
            )

    _fresh_shared(monkeypatch, tmp_path, CurrentClient)

    assert calls == [
        {
            "target": "localhost:55051",
            "timeout": 10.0,
            "auth_token": "auth-from-file",
            "admin_token": "admin-from-file",
        }
    ]


def test_shared_client_reads_token_files_before_stale_runtime_env(monkeypatch, tmp_path):
    calls = []
    monkeypatch.delenv("MN_GRPC_ADMIN_TOKEN", raising=False)
    monkeypatch.delenv("MN_MIRROR_NEURON_GRPC_ADMIN_TOKEN", raising=False)
    state_dir = tmp_path / ".mn"
    state_dir.mkdir()
    (state_dir / "docker-compose.env").write_text(
        "MN_GRPC_AUTH_TOKEN=stale-auth-from-state\n"
        "MN_GRPC_ADMIN_TOKEN=stale-admin-from-state\n",
        encoding="utf-8",
    )
    (state_dir / "grpc_auth.token").write_text("auth-from-file\n", encoding="utf-8")
    (state_dir / "grpc_admin.token").write_text("admin-from-file\n", encoding="utf-8")

    class CurrentClient:
        def __init__(self, target=None, timeout=None, auth_token=None, admin_token=None):
            calls.append(
                {
                    "target": target,
                    "timeout": timeout,
                    "auth_token": auth_token,
                    "admin_token": admin_token,
                }
            )

    _fresh_shared(monkeypatch, tmp_path, CurrentClient)

    assert calls == [
        {
            "target": "localhost:55051",
            "timeout": 10.0,
            "auth_token": "auth-from-file",
            "admin_token": "admin-from-file",
        }
    ]
