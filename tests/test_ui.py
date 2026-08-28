import re
from contextlib import contextmanager
from io import StringIO

from rich.console import Console

from mn_cli.libs.ui import (
    JobMonitorState,
    activity,
    generate_detached_panel,
    generate_run_submitted_panel,
    generate_summary_panel,
    generate_live_layout,
    generate_workflow_progress_layout,
    print_confirmed,
    print_error,
    print_info,
    print_success_confirmation,
    print_warning,
)


def _capture_console(*, no_color: bool = True, width: int = 120) -> tuple[Console, StringIO]:
    stream = StringIO()
    return Console(file=stream, force_terminal=False, no_color=no_color, width=width), stream


def test_print_success_confirmation_outputs_structured_lines():
    console, stream = _capture_console()

    print_success_confirmation(
        console,
        "Node join",
        status="connected",
        details=[("Node", "mirror_neuron@192.168.4.173"), ("Remote Redis", "192.168.4.173:56380")],
        next_steps=("mn node list", "mn resource show"),
    )

    output = [line.strip() for line in stream.getvalue().splitlines() if line.strip()]
    assert any(line == "✓ Node join successful." for line in output)
    assert any("Status:" in line and "connected" in line for line in output)
    assert any("Node:" in line and "mirror_neuron@192.168.4.173" in line for line in output)
    assert any("Remote Redis:" in line and "192.168.4.173:56380" in line for line in output)
    assert any("Next:" in line for line in output)
    assert "Details" not in stream.getvalue()


def test_print_success_confirmation_keeps_detail_labels_readable():
    console, stream = _capture_console(width=100)

    print_success_confirmation(
        console,
        "Worker node start",
        status="running",
        details=[
            ("Host", "192.168.4.173"),
            ("gRPC", "192.168.4.173:55051"),
            ("Node", "mirror_neuron@192.168.4.173"),
            ("Token", "bdf21a9c1f101cce95786862b19ab5b0ac1f5d196d2183e7bcec5b4bc9ec6c4d"),
        ],
        next_steps="mn node add 192.168.4.173 --token bdf21a9c1f101cce95786862b19ab5b0ac1f5d196d2183e7bcec5b4bc9ec6c4d",
    )

    output = stream.getvalue()
    assert "Status:" in output
    assert "Host:" in output
    assert "gRPC:" in output
    assert "Node:" in output
    assert "Token:" in output
    assert "Next:" in output
    assert "\n S " not in output


def test_print_confirmed_skips_empty_values_and_supports_plain_mode(monkeypatch):
    console, stream = _capture_console(no_color=True)

    monkeypatch.setenv("MN_CLI_OUTPUT", "plain")

    print_confirmed(
        console,
        "MirrorNeuron update",
        status="up to date",
        details={"Current": "1.2.3", "Latest": None, "": "ignored"},
        next_steps="",
    )

    assert stream.getvalue().splitlines() == [
        "MirrorNeuron update confirmed.",
        "Status: up to date",
        "Current: 1.2.3",
    ]


def test_status_messages_share_concise_prefixes():
    console, stream = _capture_console()

    print_info(console, "Starting runtime…")
    print_warning(console, "The optional Web UI is unavailable.")
    print_error(console, "The runtime did not respond.", code="MN_RUNTIME_TIMEOUT")

    assert stream.getvalue().splitlines() == [
        "→ Starting runtime…",
        "! Warning: The optional Web UI is unavailable.",
        "× Error: (MN_RUNTIME_TIMEOUT) The runtime did not respond.",
    ]


def test_activity_uses_a_spinner_for_an_interactive_terminal(monkeypatch):
    events = []

    class SpinnerConsole:
        file = object()

        @contextmanager
        def status(self, message, *, spinner):
            events.append(("start", message, spinner))
            yield
            events.append(("end", message, spinner))

    monkeypatch.delenv("MN_CLI_OUTPUT", raising=False)
    monkeypatch.setattr("mn_cli.terminal.use_progress", lambda _stream: True)

    with activity(SpinnerConsole(), "Resuming run run-1…"):
        events.append(("work",))

    assert events == [
        ("start", "[cyan]Resuming run run-1…[/cyan]", "dots"),
        ("work",),
        ("end", "[cyan]Resuming run run-1…[/cyan]", "dots"),
    ]


def test_activity_skips_transient_output_in_plain_mode(monkeypatch):
    class SpinnerConsole:
        file = object()

        def status(self, *_args, **_kwargs):
            raise AssertionError("plain output must not start a spinner")

    monkeypatch.setenv("MN_CLI_OUTPUT", "plain")
    monkeypatch.setattr("mn_cli.terminal.use_progress", lambda _stream: True)

    with activity(SpinnerConsole(), "Resuming run run-1…"):
        pass


def test_run_identity_panels_keep_job_and_run_ids_distinct_at_supported_widths(
    tmp_path, monkeypatch
):
    for output_mode in ("rich", "plain"):
        monkeypatch.setenv("MN_CLI_OUTPUT", output_mode)
        for width in (60, 120):
            monkeypatch.setenv("COLUMNS", str(width))
            console, stream = _capture_console(width=width)
            console.print(
                generate_run_submitted_panel(
                    bundle_name="vc_assistant",
                    job_id="job-vc-assistant",
                    run_id="vc-assistant-run-1",
                    payload_count=721,
                    log_dir=tmp_path,
                    follow_seconds=30,
                )
            )
            console.print(
                generate_detached_panel(
                    "vc-assistant-run-1",
                    tmp_path,
                    "running",
                    117,
                    job_id="job-vc-assistant",
                )
            )
            console.print(
                generate_summary_panel(
                    "vc-assistant-run-1",
                    "completed",
                    tmp_path,
                    job_id="job-vc-assistant",
                )
            )

            output = stream.getvalue()
            assert len(re.findall(r"Job ID:\s+job-vc-assistant", output)) == 3
            assert len(re.findall(r"Run ID:\s+vc-assistant-run-1", output)) == 3
            assert "Blueprint Run ID" not in output
            assert "mn run watch vc-assistant-run-1" in output


def test_workflow_monitor_renders_minimal_runtime_model_status_below_agent_progress():
    for width in (80, 160):
        console = Console(record=True, width=width)
        console.print(
            generate_workflow_progress_layout(
                "ros-run",
                {
                    "workflow_id": "ros_amr_controller",
                    "status": "running",
                    "elapsed_seconds": 72,
                    "steps": [],
                    "runtime_model_preparations": [
                        {
                            "request_id": "nemotron-pull",
                            "model": "nemotron-3.5-lightning:latest",
                            "source_model": "hf.co/bartowski/NVIDIA-Nemotron-3.5-Lightning-30B-A3B-GGUF:Q4_K_M",
                            "dmr_artifact": "nemotron-3.5-lightning:latest",
                            "node": "spark",
                            "phase": "downloading",
                            "current_bytes": 12 * 1024**3,
                            "total_bytes": 24 * 1024**3,
                            "percent": 50,
                            "elapsed_ms": 72_000,
                            "last_update_age_ms": 61_000,
                            "stalled": True,
                        }
                    ],
                },
            )
        )
        rendered = console.export_text()

        status = "Preparing Nemotron 3.5 Lightning on spark…"
        assert status in rendered
        assert rendered.index("Agents  |  0 agents") < rendered.index(status)
        assert sum("Preparing " in line for line in rendered.splitlines()) == 1
        assert "Runtime model preparation" not in rendered
        assert "12.0 GiB / 24.0 GiB (50%)" not in rendered
        assert "still preparing" not in rendered


def test_workflow_monitor_omits_token_counts_in_overview_and_agent_detail():
    progress = {
        "workflow_id": "metered-workflow",
        "status": "running",
        "elapsed_seconds": 9,
        "resource_tokens": 333,
        "steps": [
            {
                "id": "work",
                "label": "Work",
                "status": "running",
                "current": True,
                "total_count": 1,
                "agents": [
                    {
                        "id": "worker",
                        "status": "running",
                        "progress": 0.5,
                        "tokens_used": 42,
                        "token_budget": 100,
                        "resources": {
                            "cpu_percent": 5,
                            "total_tokens": 999,
                            "nested": {"input_tokens": 22, "memory_mb": 3},
                        },
                    }
                ],
            }
        ],
    }

    overview = Console(record=True, width=160)
    overview.print(generate_workflow_progress_layout("run-metered", progress))
    overview_text = overview.export_text()
    assert "tokens" not in overview_text.lower()
    assert "333" not in overview_text
    assert "42" not in overview_text

    state = JobMonitorState()
    state.detail_mode = True
    detail = Console(record=True, width=160)
    detail.print(generate_workflow_progress_layout("run-metered", progress, state=state))
    detail_text = detail.export_text()
    assert "tokens" not in detail_text.lower()
    assert "999" not in detail_text
    assert "22" not in detail_text
    assert '"cpu_percent": 5' in detail_text
    assert '"memory_mb": 3' in detail_text


def test_monitor_agent_tables_omit_mail_column():
    agent = {
        "id": "worker",
        "status": "running",
        "progress": 0.5,
        "mailbox_depth": 7,
    }
    workflow_progress = {
        "workflow_id": "clean-workflow",
        "status": "running",
        "steps": [
            {
                "id": "work",
                "label": "Work",
                "status": "running",
                "current": True,
                "total_count": 1,
                "agents": [agent],
            }
        ],
    }

    workflow_console = Console(record=True, width=160)
    workflow_console.print(
        generate_workflow_progress_layout("run-workflow", workflow_progress)
    )
    assert "mail" not in workflow_console.export_text().lower()

    live_console = Console(record=True, width=160)
    live_console.print(
        generate_live_layout(
            "run-live",
            {
                "job": {"name": "Live job"},
                "summary": {"status": "running"},
                "agents": [agent],
            },
        )
    )
    assert "mail" not in live_console.export_text().lower()
