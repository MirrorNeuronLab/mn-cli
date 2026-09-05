"""Canonical source packages for CLI launch tests."""

import json
from pathlib import Path

from mn_sdk.blueprints.authoring import write_blueprint_definition


def write_package_manifest(path: Path, content: str) -> None:
    config_path = path.parent / "config/default.json"
    config = json.loads(config_path.read_text()) if config_path.exists() else None
    definition = json.loads(content)
    definition.setdefault(
        "id",
        definition.get("graph_id") or definition.get("job_name") or "test-workflow",
    )
    if "flow" in definition:
        flow = definition.pop("flow")
        if "nodes" in flow:
            definition["agents"] = flow
        else:
            definition["workflow"] = flow
    definition.pop("graph_id", None)
    for key in ("initial_inputs", "entrypoints"):
        if key in definition:
            definition.setdefault("runtime", {})[key] = definition.pop(key)
    definition.setdefault(
        "workflow",
        {
            "workflow_id": definition.get("id", "test-workflow"),
            "steps": [{"id": "run"}],
            "edges": [],
        },
    )
    write_blueprint_definition(path.parent, definition, config=config)
