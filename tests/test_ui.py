from contextlib import contextmanager
from io import StringIO

from rich.console import Console

from mn_cli.libs.ui import (
    activity,
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
