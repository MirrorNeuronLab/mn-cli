from __future__ import annotations

import hashlib
import json
import subprocess

from mn_cli.libs.airgap import (
    AIRGAP_MARKER_SCHEMA_VERSION,
    BACKUP_SCHEMA_VERSION,
    compatibility_profile,
    hydrate_extracted_airgap,
    hydrate_payload_models,
)
from mn_cli.libs.artifacts import stage_bundle_payload_assets


def test_stage_bundle_payload_assets_always_streams_models_to_blob_store(
    tmp_path, monkeypatch
):
    bundle = tmp_path / "bundle"
    model = bundle / "payloads" / "models" / "demo.gguf"
    model.parent.mkdir(parents=True)
    model.write_bytes(b"gguf")
    manifest: dict = {}
    blob_root = tmp_path / "blobs"
    monkeypatch.setenv("MN_HOST_BLOB_STORE_DIR", str(blob_root))

    payloads = stage_bundle_payload_assets(manifest, bundle)

    assert payloads == {}
    ref = manifest["metadata"]["mn_artifacts"]["blob_refs"][0]
    assert ref["payload_path"] == "models/demo.gguf"
    assert (blob_root / ref["sha256"][:2] / ref["sha256"]).read_bytes() == b"gguf"


def test_hydrate_payload_model_uses_docker_model_package(tmp_path):
    bundle = tmp_path / "bundle"
    model = bundle / "payloads" / "models" / "demo.gguf"
    model.parent.mkdir(parents=True)
    model.write_bytes(b"gguf")
    manifest = {
        "runtime": {
            "models": {
                "primary": {
                    "provider": "docker_model_runner",
                    "runtime_model": "demo/airgap:latest",
                    "source": {
                        "type": "payload",
                        "path": "models/demo.gguf",
                        "format": "gguf",
                    },
                }
            }
        }
    }
    calls = []

    def run(command, **_kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(
            command,
            1 if command[:3] == ["docker", "model", "inspect"] else 0,
            stdout="",
            stderr="",
        )

    result = hydrate_payload_models(bundle, manifest, command_runner=run)

    assert result == [
        {
            "id": "primary",
            "model": "demo/airgap:latest",
            "status": "packaged",
        }
    ]
    assert calls[-1] == [
        "docker",
        "model",
        "package",
        "--gguf",
        str(model),
        "demo/airgap:latest",
    ]


def test_extracted_airgap_bundle_hydrates_blob_and_packages_model(tmp_path):
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    wheelhouse = tmp_path / "airgap" / "python" / "wheelhouse"
    wheelhouse.mkdir(parents=True)
    model_bytes = b"portable model"
    sha256 = hashlib.sha256(model_bytes).hexdigest()
    archived_blob = tmp_path / "airgap" / "blobs" / sha256
    archived_blob.parent.mkdir(parents=True)
    archived_blob.write_bytes(model_bytes)
    (bundle / ".mn-airgap.json").write_text(
        json.dumps(
            {
                "schema_version": AIRGAP_MARKER_SCHEMA_VERSION,
                "capsule_manifest": "../mn-backup.json",
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "mn-backup.json").write_text(
        json.dumps(
            {
                "schema_version": BACKUP_SCHEMA_VERSION,
                "air_gap": {
                    "enabled": True,
                    "network": "forbidden",
                    "compatibility": compatibility_profile(),
                    "assets": [
                        {
                            "kind": "blob",
                            "archive_path": f"airgap/blobs/{sha256}",
                            "sha256": sha256,
                            "size_bytes": len(model_bytes),
                        }
                    ],
                },
            }
        ),
        encoding="utf-8",
    )
    manifest = {
        "runtime": {
            "models": {
                "primary": {
                    "runtime_model": "demo/offline:latest",
                    "source": {
                        "type": "payload",
                        "path": "models/demo.gguf",
                        "format": "gguf",
                    },
                }
            }
        },
        "metadata": {
            "mn_artifacts": {
                "blob_refs": [
                    {
                        "payload_path": "models/demo.gguf",
                        "sha256": sha256,
                        "size_bytes": len(model_bytes),
                    }
                ]
            }
        },
    }
    calls = []

    def run(command, **_kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(
            command,
            1 if command[:3] == ["docker", "model", "inspect"] else 0,
            stdout="",
            stderr="",
        )

    result = hydrate_extracted_airgap(
        bundle,
        manifest,
        command_runner=run,
        runtime_env={
            "MN_HOME": str(tmp_path / "mn-home"),
            "MN_HOST_BLOB_STORE_DIR": str(tmp_path / "mn-home" / "blobs"),
        },
    )

    assert result["air_gapped"] is True
    assert result["wheelhouse"] == str(wheelhouse)
    assert result["models"][0]["status"] == "packaged"
    assert calls[-1][-1] == "demo/offline:latest"
    assert calls[-1][4].endswith(sha256)
