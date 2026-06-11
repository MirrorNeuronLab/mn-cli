from mn_cli.libs.resource_cmds import (
    ensure_combined_resource_totals,
    resource_number,
)


def test_ensure_combined_resource_totals_combines_and_normalizes_nodes():
    payload = {
        "status": "ok",
        "nodes": [
            {
                "node": "node-a",
                "cpu_cores": "2",
                "gpu_count": 1,
                "gpu_memory_total_mb": 2048,
                "gpu_memory_free_mb": 1024,
                "memory_gb": "8.25",
                "disk_gb": "100",
            },
            {
                "node": "node-b",
                "cpu_cores": 3,
                "gpu_count": "2",
                "gpu_memory_total_gb": 1.5,
                "gpu_memory_free_gb": 0.5,
                "memory_total_gb": 4,
                "disk_available_gb": "20.4",
            },
            "ignored",
        ],
    }

    enriched = ensure_combined_resource_totals(payload)

    assert enriched["status"] == "ok"
    assert enriched["nodes"][0]["memory_total_gb"] == 8.25
    assert enriched["nodes"][1]["gpu_memory_total_mb"] == 1536
    assert enriched["combined"]["cpu_cores"] == 5
    assert enriched["combined"]["gpu_count"] == 3
    assert enriched["combined"]["gpu_memory_total_gb"] == 3.5
    assert enriched["combined"]["gpu_memory_total_mb"] == 3584
    assert enriched["combined"]["memory_gb"] == 12.25
    assert enriched["combined"]["disk_gb"] == 100.0
    assert enriched["combined"]["disk_available_gb"] == 20.4


def test_resource_totals_passthrough_and_invalid_numbers():
    payload = ["not", "a", "dict"]

    assert ensure_combined_resource_totals(payload) is payload
    assert resource_number("not-a-number") == 0.0
    assert resource_number(None) == 0.0
