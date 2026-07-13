"""Compatibility exports for the SDK-owned skill dependency contract."""

from mn_sdk.skill_dependencies import (
    DEFAULT_SKILL_PACKAGE_VERSION,
    GAR_PIP_INDEX_URL,
    PYPI_PIP_INDEX_URL,
    gar_requirement_lines,
    gar_requirements_file_lines,
    gar_requirements_text,
    normalize_package_name,
    normalize_skill_dependency_version,
    pinned_skill_dependency_requirements,
    requirement_package_name,
    skill_dependency_package_names,
    skill_dependency_records,
    without_requirements_for_packages,
)

__all__ = [
    "DEFAULT_SKILL_PACKAGE_VERSION",
    "GAR_PIP_INDEX_URL",
    "PYPI_PIP_INDEX_URL",
    "gar_requirement_lines",
    "gar_requirements_file_lines",
    "gar_requirements_text",
    "normalize_package_name",
    "normalize_skill_dependency_version",
    "pinned_skill_dependency_requirements",
    "requirement_package_name",
    "skill_dependency_package_names",
    "skill_dependency_records",
    "without_requirements_for_packages",
]
