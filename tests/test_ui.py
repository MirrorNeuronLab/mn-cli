from io import StringIO

from rich.console import Console

from mn_cli.libs.ui import print_confirmed, print_success_confirmation


def _capture_console(*, no_color: bool = True) -> tuple[Console, StringIO]:
    stream = StringIO()
    return Console(file=stream, force_terminal=False, no_color=no_color, width=120), stream


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
    assert any(line == "Node join successful." for line in output)
    assert any("Status:" in line and "connected" in line for line in output)
    assert any("Node: mirror_neuron@192.168.4.173" in line for line in output)
    assert any("Remote Redis: 192.168.4.173:56380" in line for line in output)
    assert any("Next:" in line for line in output)


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
