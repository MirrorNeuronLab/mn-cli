"""CLI compatibility facade for SDK-owned submission preparation.

Blueprint manifest transformation, payload staging, and runtime dependency setup
are shared submission concerns.  Their implementation lives in
``mn_sdk.submission_preparation`` so the CLI and API submit identical bundles.
"""

from mn_sdk.submission_preparation import *  # noqa: F403
from mn_sdk.submission_preparation import (  # noqa: F401
    _ensure_docker_worker_requirements_install,
    _local_skill_dependency_source_records,
    _local_skill_requirements_text,
    _requirements_text,
    _runtime_web_ui_submission_context,
    _safe_dependency_source_name,
)
