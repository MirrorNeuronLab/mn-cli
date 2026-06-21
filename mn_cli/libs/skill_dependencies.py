from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any


GAR_PIP_INDEX_URL = "https://us-central1-python.pkg.dev/mirrorneuron-public-packages/agent-skills/simple/"
PYPI_PIP_INDEX_URL = "https://pypi.org/simple"
DEFAULT_SKILL_PACKAGE_VERSION = "1.2.7"


def normalize_package_name(value: str) -> str:
    return re.sub(r"[-_.]+", "-", value).lower()


def normalize_skill_dependency_version(value: Any) -> str:
    text = str(value or "").strip()
    if text.lower().startswith("v"):
        text = text[1:]
    if not text:
        raise ValueError("skill dependency version must not be empty")
    return text


def skill_dependency_records(manifest: dict[str, Any] | None) -> list[dict[str, str]]:
    raw = manifest.get("skill_dependencies") if isinstance(manifest, dict) else None
    if raw in (None, []):
        return []
    if not isinstance(raw, list):
        raise ValueError("skill_dependencies must be a list")

    records_by_package: dict[str, dict[str, str]] = {}
    for item in raw:
        if not isinstance(item, dict):
            raise ValueError("skill_dependencies entries must be objects")
        dep_type = str(item.get("type") or "").strip()
        source = str(item.get("source") or "").strip()
        name = str(item.get("name") or "").strip()
        version = normalize_skill_dependency_version(item.get("version"))
        if dep_type != "pip" or source != "gar" or not name:
            raise ValueError(
                "skill_dependencies entries must use type='pip', source='gar', name, and version"
            )
        key = normalize_package_name(name)
        existing = records_by_package.get(key)
        if existing and existing["version"] != version:
            raise ValueError(f"conflicting GAR versions declared for {name}")
        records_by_package[key] = {
            "type": dep_type,
            "source": source,
            "name": name,
            "version": version,
        }
    return list(records_by_package.values())


def skill_dependency_package_names(manifest: dict[str, Any] | None) -> set[str]:
    return {normalize_package_name(record["name"]) for record in skill_dependency_records(manifest)}


def pinned_skill_dependency_requirements(manifest: dict[str, Any] | None) -> list[str]:
    return [
        f"{record['name']}=={record['version']}"
        for record in skill_dependency_records(manifest)
    ]


def gar_requirement_lines(manifest: dict[str, Any] | None) -> list[str]:
    requirements = pinned_skill_dependency_requirements(manifest)
    if not requirements:
        return []
    return [
        "--index-url",
        GAR_PIP_INDEX_URL,
        "--extra-index-url",
        PYPI_PIP_INDEX_URL,
        *requirements,
    ]


def gar_requirements_text(manifest: dict[str, Any] | None) -> str:
    lines = gar_requirement_lines(manifest)
    return "\n".join(lines).strip() + ("\n" if lines else "")


def requirement_package_name(requirement: str) -> str | None:
    text = requirement.strip()
    if not text or text.startswith("-"):
        return None
    match = re.match(r"([A-Za-z0-9_.-]+)", text)
    return normalize_package_name(match.group(1)) if match else None


def without_requirements_for_packages(lines: Iterable[str], package_names: set[str]) -> list[str]:
    output: list[str] = []
    for line in lines:
        package = requirement_package_name(line)
        if package and package in package_names:
            continue
        output.append(line)
    return output
