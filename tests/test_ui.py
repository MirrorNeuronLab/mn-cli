from io import StringIO

from rich.console import Console

from mn_cli.libs.ui import print_confirmed, print_error, print_info, print_success_confirmation, print_warning


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
        next_steps=("mn node list", "mn resource list"),
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
        next_steps="mn node join 192.168.4.173 --token bdf21a9c1f101cce95786862b19ab5b0ac1f5d196d2183e7bcec5b4bc9ec6c4d",
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
