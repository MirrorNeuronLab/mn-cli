import json

import pytest
import typer

from mn_cli.libs import blueprint_repository as repo


def test_custom_blueprint_storage_dir_is_sanitized_and_stable(monkeypatch, tmp_path):
    monkeypatch.setattr(repo.os.path, "expanduser", lambda path: str(tmp_path) if path == repo.CUSTOM_BLUEPRINT_STORAGE_ROOT else path)

    first = repo.custom_blueprint_storage_dir("https://example.test/acme/customer blueprints.git")
    second = repo.custom_blueprint_storage_dir("https://example.test/acme/customer blueprints.git/")

    assert first == second
    assert first.parent == tmp_path
    assert first.name.startswith("customer-blueprints-")


def test_load_blueprint_index_rejects_entries_without_paths_when_required(tmp_path):
    index_path = tmp_path / "index.json"
    index_path.write_text(json.dumps([{"id": "bp-1"}, "bad-entry"]))

    with pytest.raises(repo.BlueprintIndexError, match="must include a string path"):
        repo.load_blueprint_index(index_path, require_paths=True)


def test_load_blueprint_index_rejects_non_object_entries(tmp_path):
    index_path = tmp_path / "index.json"
    index_path.write_text(json.dumps([{"id": "bp-1", "path": "bp"}, "bad-entry"]))

    with pytest.raises(repo.BlueprintIndexError, match="entry 1 must be a JSON object"):
        repo.load_blueprint_index(index_path)


def test_ensure_blueprint_source_offline_missing_cache_exits(monkeypatch, tmp_path):
    monkeypatch.setattr(repo.os.path, "expanduser", lambda _path: str(tmp_path / "blueprints"))

    with pytest.raises(typer.Exit):
        repo.ensure_blueprint_source(
            source=None,
            blueprint_repo=None,
            update=False,
            offline=True,
            revision=None,
        )
