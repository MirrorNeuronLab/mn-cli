"""Prepare Core's transport-only environment for native Python workers."""

from pathlib import Path

import mn_sdk
from mn_sdk.dependency_versions import local_project_version, local_sdk_version
from mn_sdk.skill_dependencies import GAR_PIP_INDEX_URL, PYPI_PIP_INDEX_URL


def prepare_hostlocal_python_bridge(*, timeout: float) -> Path:
    from mn_cli.libs.run_cmds.handlers.doctor import _doctor_prepare_python_env_from_content

    sdk_root = Path(mn_sdk.__file__).resolve().parent.parent
    common = sdk_root / "packages" / "common"
    versions = None
    roots = None
    if sdk_root.joinpath("pyproject.toml").is_file() and common.joinpath("pyproject.toml").is_file():
        packages = [str(sdk_root), str(common)]
        versions = {str(sdk_root): local_sdk_version(sdk_root), str(common): local_project_version(common)}
        roots = [sdk_root]
    else:
        packages = ["--index-url", GAR_PIP_INDEX_URL, "--extra-index-url", PYPI_PIP_INDEX_URL,
                    "mirrorneuron-python-sdk>1.3,<2"]
    return _doctor_prepare_python_env_from_content(
        blueprint_id="mn-native-host-python-bridge-v1", node_id="native_host_bridge",
        packages=packages, requirements_content="", timeout=timeout,
        local_source_roots=roots, local_source_versions=versions, force_local_core=True,
    )
