from __future__ import annotations

import hashlib
import json
import os
import platform
import shutil
import subprocess
from pathlib import Path, PurePosixPath
from typing import Any, Callable

from mn_sdk.runtime_config import resolve_mn_home

from mn_cli.libs.artifacts import blob_store_path, install_blob_file


BACKUP_SCHEMA_VERSION = "mn.backup.v2"
AIRGAP_MARKER_SCHEMA_VERSION = "mn.airgap.bundle.v1"


class AirGapError(RuntimeError):
    pass


def compatibility_profile() -> dict[str, str]:
    return {
        "os": platform.system().lower(),
        "architecture": platform.machine().lower(),
        "python_implementation": platform.python_implementation().lower(),
        "python_version": platform.python_version(),
        "python_abi": getattr(__import__("sys").implementation, "cache_tag", "") or "",
    }


def validate_compatibility(expected: dict[str, Any]) -> None:
    actual = compatibility_profile()
    mismatches = [
        f"{key}: expected {expected.get(key)!r}, found {actual.get(key)!r}"
        for key in ("os", "architecture", "python_implementation", "python_abi")
        if expected.get(key) and expected.get(key) != actual.get(key)
    ]
    if mismatches:
        raise AirGapError(
            "Air-gapped backup is incompatible with this runtime: "
            + "; ".join(mismatches)
        )


def hydrate_payload_models(
    bundle_dir: Path,
    manifest: dict[str, Any],
    *,
    command_runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    runtime_env: dict[str, str] | None = None,
) -> list[dict[str, str]]:
    runtime = manifest.get("runtime") if isinstance(manifest.get("runtime"), dict) else {}
    models = runtime.get("models") if isinstance(runtime.get("models"), dict) else {}
    refs = _blob_refs(manifest)
    results: list[dict[str, str]] = []
    for model_id, declaration in models.items():
        if not isinstance(declaration, dict):
            continue
        source = declaration.get("source")
        if not isinstance(source, dict) or source.get("type") != "payload":
            continue
        target = str(
            declaration.get("runtime_model")
            or declaration.get("model")
            or model_id
        ).strip()
        if not target:
            raise AirGapError(f"runtime.models.{model_id} is missing runtime_model")
        inspect = command_runner(
            ["docker", "model", "inspect", target],
            capture_output=True,
            text=True,
            check=False,
        )
        if inspect.returncode == 0:
            results.append({"id": str(model_id), "model": target, "status": "present"})
            continue

        model_format = str(source.get("format") or "").strip().lower()
        logical_path = _safe_payload_path(source.get("path"), "models")
        model_path = _resolve_payload_asset(
            bundle_dir,
            logical_path,
            refs,
            runtime_env=runtime_env,
            directory=model_format == "safetensors",
        )
        format_flag = {
            "gguf": "--gguf",
            "safetensors": "--safetensors-dir",
            "dduf": "--dduf",
        }.get(model_format)
        if format_flag is None:
            raise AirGapError(
                f"runtime.models.{model_id}.source.format must be gguf, safetensors, or dduf"
            )
        command = ["docker", "model", "package", format_flag, str(model_path)]
        for field, flag in (
            ("mmproj_path", "--mmproj"),
            ("chat_template_path", "--chat-template"),
            ("license_path", "--license"),
        ):
            if not source.get(field):
                continue
            auxiliary = _resolve_payload_asset(
                bundle_dir,
                _safe_payload_path(source[field], "models"),
                refs,
                runtime_env=runtime_env,
            )
            command.extend([flag, str(auxiliary)])
        context_size = declaration.get("context_size")
        if isinstance(context_size, int) and context_size > 0:
            command.extend(["--context-size", str(context_size)])
        command.append(target)
        packaged = command_runner(
            command,
            capture_output=True,
            text=True,
            check=False,
        )
        if packaged.returncode != 0:
            detail = (packaged.stderr or packaged.stdout or "docker model package failed").strip()
            raise AirGapError(f"Could not package payload model {target}: {detail}")
        results.append({"id": str(model_id), "model": target, "status": "packaged"})
    return results


def hydrate_extracted_airgap(
    bundle_dir: Path,
    manifest: dict[str, Any],
    *,
    command_runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    runtime_env: dict[str, str] | None = None,
) -> dict[str, Any]:
    marker_path = bundle_dir / ".mn-airgap.json"
    if not marker_path.is_file():
        return {"air_gapped": False, "models": [], "wheelhouse": ""}
    marker = _read_object(marker_path, "air-gap bundle marker")
    if marker.get("schema_version") != AIRGAP_MARKER_SCHEMA_VERSION:
        raise AirGapError(
            f"Unsupported air-gap marker schema {marker.get('schema_version')!r}"
        )
    capsule_reference = str(marker.get("capsule_manifest") or "").replace(
        "\\", "/"
    )
    if not capsule_reference or PurePosixPath(capsule_reference).is_absolute():
        raise AirGapError(
            f"Unsafe capsule path: {marker.get('capsule_manifest')!r}"
        )
    capsule_path = (
        bundle_dir / Path(*PurePosixPath(capsule_reference).parts)
    ).resolve()
    capsule_root = bundle_dir.parent.resolve()
    try:
        capsule_path.relative_to(capsule_root)
    except ValueError as exc:
        raise AirGapError("Air-gap marker escapes the extracted capsule") from exc
    capsule = _read_object(capsule_path, "air-gap capsule manifest")
    if capsule.get("schema_version") != BACKUP_SCHEMA_VERSION:
        raise AirGapError(
            f"Unsupported backup schema {capsule.get('schema_version')!r}"
        )
    airgap = capsule.get("air_gap") if isinstance(capsule.get("air_gap"), dict) else {}
    if airgap.get("enabled") is not True:
        raise AirGapError("Air-gap marker points to a backup without air_gap.enabled")
    validate_compatibility(airgap.get("compatibility") or {})
    root = capsule_path.parent
    for asset in airgap.get("assets") or []:
        if not isinstance(asset, dict):
            continue
        archive_path = _safe_relative(asset.get("archive_path"))
        source = (root / archive_path).resolve()
        try:
            source.relative_to(root.resolve())
        except ValueError as exc:
            raise AirGapError("Air-gap asset escapes the capsule") from exc
        kind = str(asset.get("kind") or "")
        if kind == "blob":
            install_blob_file(
                source,
                str(asset.get("sha256") or ""),
                runtime_env=runtime_env,
            )
        elif kind == "docker_image":
            loaded = command_runner(
                ["docker", "image", "load", "--input", str(source)],
                capture_output=True,
                text=True,
                check=False,
            )
            if loaded.returncode != 0:
                detail = (loaded.stderr or loaded.stdout or "docker image load failed").strip()
                raise AirGapError(f"Could not load air-gap Docker image: {detail}")
    wheelhouse = root / "airgap" / "python" / "wheelhouse"
    manifest.setdefault("metadata", {})["mn_airgap"] = {
        "wheelhouse": str(wheelhouse) if wheelhouse.is_dir() else "",
        "network": "forbidden",
    }
    models = hydrate_payload_models(
        bundle_dir,
        manifest,
        command_runner=command_runner,
        runtime_env=runtime_env,
    )
    return {
        "air_gapped": True,
        "models": models,
        "wheelhouse": str(wheelhouse) if wheelhouse.is_dir() else "",
    }


def offline_environment(wheelhouse: str | Path | None = None) -> dict[str, str]:
    values = {
        "MN_AIR_GAPPED": "1",
        "MN_OFFLINE": "1",
        "PIP_NO_INDEX": "1",
        "PIP_DISABLE_PIP_VERSION_CHECK": "1",
    }
    if wheelhouse:
        values["PIP_FIND_LINKS"] = str(wheelhouse)
    return values


def _resolve_payload_asset(
    bundle_dir: Path,
    logical_path: str,
    refs: dict[str, dict[str, Any]],
    *,
    runtime_env: dict[str, str] | None,
    directory: bool = False,
) -> Path:
    physical = bundle_dir / "payloads" / Path(*PurePosixPath(logical_path).parts)
    if (directory and physical.is_dir()) or (not directory and physical.is_file()):
        return physical.resolve()
    if not directory:
        ref = refs.get(logical_path)
        if ref is None:
            raise AirGapError(f"Payload asset is unavailable: payloads/{logical_path}")
        path = blob_store_path(str(ref.get("sha256") or ""), runtime_env=runtime_env)
        if not path.is_file():
            raise AirGapError(f"Payload blob is unavailable: {ref.get('sha256')}")
        return path

    prefix = logical_path.rstrip("/") + "/"
    children = {
        path: ref for path, ref in refs.items() if path.startswith(prefix)
    }
    if not children:
        raise AirGapError(f"Payload model directory is unavailable: payloads/{logical_path}")
    identity = hashlib.sha256(
        json.dumps(
            {
                path: str(ref.get("sha256") or "")
                for path, ref in sorted(children.items())
            },
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    target = (
        resolve_mn_home(runtime_env or os.environ)
        / "airgap"
        / "materialized"
        / identity
    )
    for child_path, ref in children.items():
        relative = PurePosixPath(child_path).relative_to(PurePosixPath(logical_path))
        source = blob_store_path(str(ref.get("sha256") or ""), runtime_env=runtime_env)
        destination = target / Path(*relative.parts)
        destination.parent.mkdir(parents=True, exist_ok=True)
        if not destination.exists():
            try:
                os.link(source, destination)
            except OSError:
                shutil.copy2(source, destination)
    return target


def _blob_refs(manifest: dict[str, Any]) -> dict[str, dict[str, Any]]:
    metadata = manifest.get("metadata") if isinstance(manifest.get("metadata"), dict) else {}
    artifacts = metadata.get("mn_artifacts") if isinstance(metadata.get("mn_artifacts"), dict) else {}
    refs = artifacts.get("blob_refs") if isinstance(artifacts.get("blob_refs"), list) else []
    return {
        str(item.get("payload_path")): item
        for item in refs
        if isinstance(item, dict)
        and isinstance(item.get("payload_path"), str)
        and item.get("payload_path")
    }


def _safe_payload_path(value: Any, prefix: str) -> str:
    text = str(value or "").replace("\\", "/").strip("/")
    path = PurePosixPath(text)
    if (
        not text
        or path.is_absolute()
        or ".." in path.parts
        or not path.parts
        or path.parts[0] != prefix
    ):
        raise AirGapError(f"Unsafe payload path: {value!r}")
    return path.as_posix()


def _safe_relative(value: Any) -> str:
    text = str(value or "").replace("\\", "/")
    path = PurePosixPath(text)
    if not text or path.is_absolute() or ".." in path.parts:
        raise AirGapError(f"Unsafe capsule path: {value!r}")
    return path.as_posix()


def _read_object(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AirGapError(f"Malformed {label}: {path}") from exc
    if not isinstance(value, dict):
        raise AirGapError(f"{label} must be a JSON object")
    return value
