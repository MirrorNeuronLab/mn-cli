from __future__ import annotations

import importlib
import sys
from types import SimpleNamespace


def _fresh_shared(monkeypatch, tmp_path, client_class):
    sys.modules.pop("mn_cli.shared", None)
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("MN_CORE_GRPC_TARGET", raising=False)
    monkeypatch.delenv("MN_CORE_HOST", raising=False)
    monkeypatch.delenv("MN_GRPC_AUTH_TOKEN", raising=False)
    monkeypatch.delenv("MN_GRPC_TARGET", raising=False)
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

    monkeypatch.setenv("MN_MIRROR_NEURON_GRPC_ADMIN_TOKEN", "admin-secret")

    _fresh_shared(monkeypatch, tmp_path, OldClient)

    assert calls == [
        {
            "target": "localhost:50051",
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

    monkeypatch.setenv("MN_MIRROR_NEURON_GRPC_ADMIN_TOKEN", "admin-secret")

    _fresh_shared(monkeypatch, tmp_path, CurrentClient)

    assert calls == [
        {
            "target": "localhost:50051",
            "timeout": 10.0,
            "auth_token": "",
            "admin_token": "admin-secret",
        }
    ]
