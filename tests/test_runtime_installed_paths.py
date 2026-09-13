"""Installed sidecars follow MN_HOME rather than a second global tools tree."""
import json
import os
import subprocess
import sys
from pathlib import Path


def test_sidecars_resolve_from_custom_runtime_home(tmp_path):
    runtime = tmp_path / "custom mn"
    commands = ["mn-api", "mn-native-sdk-grpc", "mn-web-ui-server"]
    (runtime / "venv/bin").mkdir(parents=True)
    for command in commands:
        (runtime / "venv/bin" / command).touch()
    result = subprocess.run(
        [sys.executable, "-c", '''
import json
from mn_cli.runtime import server
print(json.dumps([server._api_command(), server._native_sdk_grpc_command(), server._web_ui_command("localhost", "55173")]))
'''],
        env={**os.environ, "MN_HOME": str(runtime)},
        capture_output=True, text=True, timeout=30, check=False,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == [[str(runtime / "venv/bin" / name)] for name in commands]
