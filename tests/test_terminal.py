from mn_cli import terminal


class DummyStream:
    def __init__(self, tty: bool):
        self._tty = tty

    def isatty(self):
        return self._tty


def test_no_color_environment_disables_color(monkeypatch):
    monkeypatch.setenv("NO_COLOR", "1")

    assert terminal.color_disabled("rich") is True


def test_plain_output_mode_disables_color(monkeypatch):
    monkeypatch.delenv("NO_COLOR", raising=False)

    assert terminal.color_disabled("plain") is True


def test_progress_requires_interactive_non_ci_output(monkeypatch):
    monkeypatch.delenv("CI", raising=False)
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)

    assert terminal.use_progress(DummyStream(True)) is True
    assert terminal.use_progress(DummyStream(False)) is False

    monkeypatch.setenv("CI", "true")
    assert terminal.use_progress(DummyStream(True)) is False
