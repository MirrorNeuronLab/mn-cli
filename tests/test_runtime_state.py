from __future__ import annotations

import stat

from mn_cli import runtime_state


def test_read_env_file_ignores_comments_blanks_and_malformed_lines(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n"
        "# comment\n"
        "FOO=bar\n"
        "MALFORMED\n"
        " SPACED = value \n",
        encoding="utf-8",
    )

    assert runtime_state.read_env_file(env_file) == {
        "FOO": "bar",
        "SPACED ": " value",
    }


def test_write_env_file_values_updates_in_place_and_preserves_context(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("# keep\nFOO=old\n\nBAR=1\n", encoding="utf-8")

    runtime_state.write_env_file_values(env_file, {"FOO": "new", "BAZ": "3"})

    assert env_file.read_text(encoding="utf-8").splitlines() == [
        "# keep",
        "FOO=new",
        "",
        "BAR=1",
        "BAZ=3",
    ]
    assert stat.S_IMODE(env_file.stat().st_mode) == 0o600


def test_remove_env_file_keys_returns_whether_file_changed(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("# keep\nFOO=1\nBAR=2\n", encoding="utf-8")

    assert runtime_state.remove_env_file_keys(env_file, {"FOO"}) is True
    assert env_file.read_text(encoding="utf-8") == "# keep\nBAR=2\n"
    assert runtime_state.remove_env_file_keys(env_file, {"MISSING"}) is False


def test_write_private_text_creates_private_parent_file(tmp_path):
    token_file = tmp_path / "nested" / "token"

    runtime_state.write_private_text(token_file, "secret\n")

    assert token_file.read_text(encoding="utf-8") == "secret\n"
    assert stat.S_IMODE(token_file.stat().st_mode) == 0o600


def test_read_json_file_returns_none_for_missing_or_invalid_json(tmp_path):
    assert runtime_state.read_json_file(tmp_path / "missing.json") is None

    invalid = tmp_path / "invalid.json"
    invalid.write_text("{", encoding="utf-8")
    assert runtime_state.read_json_file(invalid) is None

    valid = tmp_path / "valid.json"
    valid.write_text('{"ok": true}', encoding="utf-8")
    assert runtime_state.read_json_file(valid) == {"ok": True}


def test_read_json_object_returns_empty_dict_for_missing_invalid_or_non_object_json(tmp_path):
    assert runtime_state.read_json_object(tmp_path / "missing.json") == {}

    invalid = tmp_path / "invalid.json"
    invalid.write_text("{", encoding="utf-8")
    assert runtime_state.read_json_object(invalid) == {}

    non_object = tmp_path / "array.json"
    non_object.write_text("[]", encoding="utf-8")
    assert runtime_state.read_json_object(non_object) == {}

    valid = tmp_path / "valid.json"
    valid.write_text('{"ok": true}', encoding="utf-8")
    assert runtime_state.read_json_object(valid) == {"ok": True}
