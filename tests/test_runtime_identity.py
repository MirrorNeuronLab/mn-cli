import json
import os
from concurrent.futures import ProcessPoolExecutor

import pytest

from mn_cli.runtime.identity import identity_health, read_identity, resolve_identity


def _resolve(home):
    return resolve_identity(home)


def test_new_identity_survives_restart_and_is_private(tmp_path):
    name = resolve_identity(tmp_path)
    assert name.startswith("mirror_neuron_") and name.endswith("@127.0.0.1")
    assert resolve_identity(tmp_path) == name
    assert (tmp_path / "node-identity.json").stat().st_mode & 0o777 == 0o600


def test_concurrent_start_uses_one_identity(tmp_path):
    with ProcessPoolExecutor(max_workers=4) as pool:
        assert len(set(pool.map(_resolve, [tmp_path] * 8))) == 1


def test_adopts_existing_ip_name_without_renaming(tmp_path):
    assert resolve_identity(tmp_path, configured="mirror_neuron@10.0.4.27") == "mirror_neuron@10.0.4.27"
    assert resolve_identity(tmp_path, observed="mirror_neuron@10.0.4.27") == "mirror_neuron@10.0.4.27"
    with pytest.raises(RuntimeError, match="Conflicting"):
        resolve_identity(tmp_path, explicit="mirror_neuron@10.0.4.28")


def test_conflicting_migration_leaves_identity_unwritten(tmp_path):
    with pytest.raises(RuntimeError, match="Conflicting"):
        resolve_identity(tmp_path, configured="a@host", observed="b@host")
    assert not (tmp_path / "node-identity.json").exists()


def test_running_core_evidence_must_agree_with_saved_configuration(tmp_path):
    with pytest.raises(RuntimeError, match="Conflicting"):
        resolve_identity(tmp_path, configured="a@host", observed="a@host", runtime_name="b@host")
    assert not (tmp_path / "node-identity.json").exists()


def test_probe_reads_actual_self_identity_and_closes_transport():
    from types import SimpleNamespace
    from unittest.mock import Mock
    from mn_cli.runtime.identity import probe_core_identity
    client = SimpleNamespace(channel=Mock(), get_system_summary=lambda: '{"nodes":[{"name":"a@host","self?":true}]}')
    assert probe_core_identity({"MN_NODE_NAME": "a@host"}, client_factory=lambda **_: client)
    client.channel.close.assert_called_once()


@pytest.mark.parametrize("value", ["nonode@nohost", "bad name@host", "a@", "@host", "a@b@c", "a"])
def test_explicit_invalid_name_is_rejected(tmp_path, value):
    with pytest.raises(RuntimeError, match="Invalid"):
        resolve_identity(tmp_path, explicit=value)


def test_interrupted_write_preserves_record(tmp_path, monkeypatch):
    import mn_cli.runtime.identity as identity
    def fail(*args):
        raise OSError("interrupted")
    monkeypatch.setattr(os, "replace", fail)
    with pytest.raises(OSError):
        resolve_identity(tmp_path)
    assert read_identity(tmp_path) == ""
    assert not list(tmp_path.glob(".node-identity.json.*"))


def test_corrupt_record_is_not_replaced(tmp_path):
    path = tmp_path / "node-identity.json"
    path.write_text('{"version":')
    with pytest.raises(RuntimeError):
        resolve_identity(tmp_path)
    assert path.read_text() == '{"version":'


def test_readiness_requires_expected_and_actual_name():
    assert identity_health("a@host", "a@host")["valid"]
    for expected, actual in [("", "a@host"), ("a@host", "nonode@nohost"), ("a@host", "b@host")]:
        assert not identity_health(expected, actual)["valid"]
