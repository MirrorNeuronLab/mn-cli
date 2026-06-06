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

    assert stream.getvalue().splitlines() == [
        "Node join successful.",
        "Status: connected",
        "Node: mirror_neuron@192.168.4.173",
        "Remote Redis: 192.168.4.173:56380",
        "Next: mn node list",
        "Next: mn resource list",
    ]


def test_print_confirmed_skips_empty_values_and_supports_plain_output():
    console, stream = _capture_console(no_color=True)

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
