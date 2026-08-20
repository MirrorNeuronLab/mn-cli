def workflow_manifest(fields: dict | None = None) -> dict:
    values = dict(fields or {})
    nodes = values.pop("nodes", [])
    workflow_id = str(
        values.get("id")
        or values.get("graph_id")
        or values.get("job_name")
        or "test-workflow"
    )
    values.pop("apiVersion", None)
    values.pop("kind", None)
    return {
        "apiVersion": "mn.workflow/v1",
        "kind": "Workflow",
        "id": workflow_id,
        "contract": values.pop("contract", {}),
        "agents": values.pop("agents", {"nodes": nodes}),
        "runtime": values.pop("runtime", {}),
        **values,
    }
