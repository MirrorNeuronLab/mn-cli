import re
import shutil
from concurrent.futures import ThreadPoolExecutor

from mn_cli.runtime.credentials import resolve_grpc_token


def test_tokens_are_private_stable_and_distinct_per_installation(tmp_path, monkeypatch):
    monkeypatch.delenv("MN_GRPC_AUTH_TOKEN", raising=False)
    monkeypatch.delenv("MN_GRPC_ADMIN_TOKEN", raising=False)
    home = tmp_path / "runtime home"
    with ThreadPoolExecutor(max_workers=8) as pool:
        tokens = list(pool.map(lambda _: resolve_grpc_token(home, "auth"), range(32)))
    assert len(set(tokens)) == 1
    assert re.fullmatch(r"[0-9a-f]{64}", tokens[0])
    assert (home / "grpc_auth.token").stat().st_mode & 0o777 == 0o600
    assert resolve_grpc_token(home, "admin") != tokens[0]
    shutil.rmtree(home)
    assert resolve_grpc_token(home, "auth") != tokens[0]


def test_legacy_defaults_are_replaced_and_saved_files_override_stale_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("MN_GRPC_AUTH_TOKEN", "mirror_neuron_password")
    (tmp_path / "grpc_auth.token").write_text("mirror_neuron_password")
    token = resolve_grpc_token(tmp_path, "auth")
    assert token != "mirror_neuron_password"
    monkeypatch.setenv("MN_GRPC_AUTH_TOKEN", "stale-inherited-token")
    assert resolve_grpc_token(tmp_path, "auth") == token
