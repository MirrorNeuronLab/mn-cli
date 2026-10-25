"""Human-readable placement from Core's current Run projection."""

import math


def _record(value):
    return value if isinstance(value, dict) else {}


def _text(value):
    return value.strip() if isinstance(value, str) else ""


def _number(value):
    return value if type(value) in (int, float) and math.isfinite(value) and value >= 0 else None


def _memory(value):
    return f"{value / 1024:g} GB" if value >= 1024 else f"{value:g} MB"


def run_info_display(record):
    placement = _record(record.get("placement"))
    reservation = _record(placement.get("reservation"))
    owner = _text(placement.get("owner_node"))
    label = _text(placement.get("node_label"))
    node = f"{label} ({owner})" if label and owner and label != owner else label or owner or "Not reported"
    state = reservation.get("state")
    amounts = {}
    if state == "reserved":
        cpu = _number(reservation.get("cpu_cores"))
        ram = _number(reservation.get("memory_mb"))
        gpu = _number(reservation.get("gpu_count"))
        gpu_memory = _number(reservation.get("gpu_memory_mb"))
        disk = _number(reservation.get("disk_mb"))
        if cpu is not None:
            amounts["reserved_cpu"] = f"{cpu:g} {'core' if cpu == 1 else 'cores'}"
        if ram is not None:
            amounts["reserved_ram"] = _memory(ram)
        if gpu is not None:
            amounts["reserved_gpus"] = f"{gpu:g}"
        if gpu_memory is not None and gpu_memory > 0:
            amounts["reserved_gpu_memory"] = _memory(gpu_memory)
        if disk is not None and disk > 0:
            amounts["reserved_disk"] = _memory(disk)
        resources = "Whole node" if reservation.get("mode") == "exclusive_node" else "Shared"
        if not amounts:
            resources += "; amounts not reported"
    else:
        resources = {"queued": "None; waiting for resources", "unreserved": "No resources reserved",
                     "unavailable": "Unavailable"}.get(state, "Not reported")
    return {"assigned_node": node, "reserved_resources": resources, **amounts,
            **{key: value for key, value in record.items() if key != "placement"}}
