from __future__ import annotations

import json
import os
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.request
from importlib import metadata
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console

from mn_cli.libs.ui import print_confirmed, print_success_confirmation
from mn_cli.server_cmds import DIR, WEB_UI_DIRS, _start_server

console = Console()


CHECK_FILE = DIR / ".update-check.json"
INSTALL_METADATA_FILE = DIR / "install_metadata.json"
CHECK_INTERVAL_SECONDS = int(os.getenv("MN_UPDATE_CHECK_INTERVAL_SECONDS", "86400"))
PYPI_PACKAGES = [
    "mirrorneuron-python-sdk",
    "mirrorneuron-cli",
    "mirrorneuron-api",
]
NPM_PACKAGE = "mirrorneuron-web-ui"
CORE_REPO = os.getenv("MN_CORE_REPO", "MirrorNeuronLab/MirrorNeuron")


def maybe_prompt_for_update(command_name: Optional[str] = None) -> None:
    if command_name in {"update", "stop"}:
        return
    if _local_source_install():
        return
    if os.getenv("MN_DISABLE_UPDATE_CHECK", "").lower() in {"1", "true", "yes"}:
        return
    if os.getenv("CI", "").lower() == "true":
        return
    if not sys.stdin.isatty():
        return
    if not _check_due():
        return

    _record_check()
    try:
        available = get_available_updates()
    except Exception:
        return

    if not available:
        return

    console.print("\n[yellow]A MirrorNeuron update is available.[/yellow]")
    _print_updates(available)
    console.print(
        "[bold red]Updating will stop all MirrorNeuron components and all running jobs.[/bold red]"
    )
    console.print(
        "[yellow]Please update only when no important jobs are running. "
        "Backward compatibility is not guaranteed between releases.[/yellow]"
    )
    if typer.confirm("Do you want to update now?", default=False):
        perform_update(available)


def update(
    yes: bool = typer.Option(False, "--yes", "-y", help="Update without prompting."),
    check_only: bool = typer.Option(
        False, "--check-only", help="Only check for updates; do not install them."
    ),
) -> None:
    """Check for released package updates and optionally install them."""

    if _local_source_install():
        console.print(
            "[yellow]Local source install detected; release updates are skipped. "
            "Run install_local.sh from your checkout to refresh local components.[/yellow]"
        )
        return

    try:
        available = get_available_updates()
    except Exception as exc:
        console.print(f"[red]Could not check for updates: {exc}[/red]")
        raise typer.Exit(1) from exc

    if not available:
        print_confirmed(console, "MirrorNeuron update", status="up to date")
        return

    console.print("[yellow]A MirrorNeuron update is available.[/yellow]")
    _print_updates(available)

    if check_only:
        return

    console.print(
        "[bold red]Updating will stop all MirrorNeuron components and all running jobs.[/bold red]"
    )
    console.print(
        "[yellow]Please update only when no important jobs are running. "
        "Backward compatibility is not guaranteed between releases.[/yellow]"
    )

    if not yes and not typer.confirm("Do you want to update now?", default=False):
        console.print("[yellow]Update cancelled.[/yellow]")
        return

    perform_update(available)


def get_available_updates() -> list[dict[str, str]]:
    updates = []

    for package_name in PYPI_PACKAGES:
        current = _installed_python_version(package_name)
        latest = _pypi_latest_version(package_name)
        if current != latest:
            updates.append(
                {
                    "component": package_name,
                    "current": current or "not installed",
                    "latest": latest,
                    "kind": "pypi",
                }
            )

    if _web_ui_installed():
        current = _installed_npm_version()
        latest = _npm_latest_version(NPM_PACKAGE)
        if current != latest:
            updates.append(
                {
                    "component": NPM_PACKAGE,
                    "current": current or "not installed",
                    "latest": latest,
                    "kind": "npm",
                }
            )

    current_core = _installed_core_tag()
    latest_core = _github_latest_release()["tag_name"]
    if current_core != latest_core:
        updates.append(
            {
                "component": "MirrorNeuron core",
                "current": current_core or "unknown",
                "latest": latest_core,
                "kind": "core",
            }
        )

    return updates


def perform_update(available: list[dict[str, str]] | None = None) -> None:
    available = available if available is not None else get_available_updates()
    if not available:
        print_confirmed(console, "MirrorNeuron update", status="up to date")
        return

    console.print("=> Stopping MirrorNeuron components...")
    from mn_cli.libs.sys_cmds import stop

    stop()

    if any(item["kind"] == "pypi" for item in available):
        _update_python_packages()

    if any(item["kind"] == "npm" for item in available):
        _update_web_ui()

    if any(item["kind"] == "core" for item in available):
        _update_core()

    _record_check()
    print_success_confirmation(
        console,
        "MirrorNeuron update",
        status="installed",
        details={"Components": ", ".join(item["component"] for item in available)},
        next_steps="mn runtime health",
    )
    console.print("=> Restarting MirrorNeuron...")
    _start_server()


def _print_updates(updates: list[dict[str, str]]) -> None:
    for item in updates:
        console.print(
            f"  - {item['component']}: {item['current']} -> {item['latest']}"
        )


def _check_due() -> bool:
    try:
        data = json.loads(CHECK_FILE.read_text())
        checked_at = float(data.get("checked_at", 0))
    except Exception:
        return True
    return time.time() - checked_at >= CHECK_INTERVAL_SECONDS


def _record_check() -> None:
    CHECK_FILE.parent.mkdir(parents=True, exist_ok=True)
    CHECK_FILE.write_text(json.dumps({"checked_at": time.time()}))

def _local_source_install() -> bool:
    try:
        data = json.loads(INSTALL_METADATA_FILE.read_text())
    except Exception:
        data = {}

    if data.get("install_type") == "local_source":
        return True

    return (DIR / "core-source").exists() or (DIR / "cli-source").exists()


def _json_url(url: str) -> dict:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "mirrorneuron-cli-updater",
        },
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        return json.loads(response.read().decode("utf-8"))


def _installed_python_version(package_name: str) -> str | None:
    try:
        return metadata.version(package_name)
    except metadata.PackageNotFoundError:
        return None


def _pypi_latest_version(package_name: str) -> str:
    return _json_url(f"https://pypi.org/pypi/{package_name}/json")["info"]["version"]


def _web_ui_installed() -> bool:
    return any((path / "package.json").exists() for path in WEB_UI_DIRS)


def _installed_npm_version() -> str | None:
    for web_ui_dir in WEB_UI_DIRS:
        if not (web_ui_dir / "package.json").exists():
            continue
        result = subprocess.run(
            ["npm", "list", NPM_PACKAGE, "--json", "--depth=0"],
            cwd=web_ui_dir,
            capture_output=True,
            text=True,
            check=False,
        )
        try:
            data = json.loads(result.stdout or "{}")
        except json.JSONDecodeError:
            continue
        version = data.get("dependencies", {}).get(NPM_PACKAGE, {}).get("version")
        if version:
            return version
    return None


def _npm_latest_version(package_name: str) -> str:
    return _json_url(f"https://registry.npmjs.org/{package_name}/latest")["version"]


def _github_latest_release() -> dict:
    return _json_url(f"https://api.github.com/repos/{CORE_REPO}/releases/latest")


def _installed_core_tag() -> str | None:
    try:
        data = json.loads(INSTALL_METADATA_FILE.read_text())
    except Exception:
        return None
    return data.get("core_release_tag")


def _docker_platform() -> str:
    result = subprocess.run(
        ["docker", "version", "--format", "{{.Server.Arch}}"],
        capture_output=True,
        text=True,
        check=False,
    )
    arch = (result.stdout.strip() or os.uname().machine).lower()
    if arch in {"arm64", "aarch64"}:
        return "linux-arm64"
    if arch in {"amd64", "x86_64"}:
        return "linux-x64"
    raise RuntimeError(f"Unsupported Docker architecture {arch!r}.")


def _update_python_packages() -> None:
    console.print("=> Updating Python packages from PyPI...")
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--upgrade",
            "mirrorneuron-python-sdk",
            "mirrorneuron-cli",
            "mirrorneuron-api",
        ],
        check=True,
    )


def _update_web_ui() -> None:
    console.print("=> Updating Web UI from npm...")
    for web_ui_dir in WEB_UI_DIRS:
        if (web_ui_dir / "package.json").exists():
            subprocess.run(
                [
                    "npm",
                    "--prefix",
                    str(web_ui_dir),
                    "install",
                    "mirrorneuron-web-ui@latest",
                ],
                check=True,
            )
            return


def _core_asset_url(release: dict, platform: str) -> str:
    suffix = f"-{platform}-otp-release.tar.gz"
    for asset in release.get("assets", []):
        name = asset.get("name", "")
        if name.endswith(suffix):
            return asset["browser_download_url"]
    raise RuntimeError(f"Could not find core release asset ending with {suffix}.")


def _download(url: str, target: Path) -> None:
    request = urllib.request.Request(url, headers={"User-Agent": "mirrorneuron-cli-updater"})
    with urllib.request.urlopen(request, timeout=60) as response:
        target.write_bytes(response.read())


def _update_core() -> None:
    console.print("=> Updating MirrorNeuron core from GitHub Release...")
    release = _github_latest_release()
    tag = release["tag_name"]
    platform = _docker_platform()
    asset_url = _core_asset_url(release, platform)

    with tempfile.TemporaryDirectory(prefix="mirrorneuron-core-update-") as temp_dir:
        root = Path(temp_dir)
        tarball = root / "core.tar.gz"
        context_dir = root / "docker-context"
        context_dir.mkdir()

        _download(asset_url, tarball)
        if DIR.exists():
            for child in DIR.iterdir():
                if child.name in {".pids", ".logs", ".update-check.json"}:
                    continue
                if child.is_dir():
                    subprocess.run(["rm", "-rf", str(child)], check=True)
                else:
                    child.unlink()
        DIR.mkdir(parents=True, exist_ok=True)

        with tarfile.open(tarball) as archive:
            archive.extractall(DIR)

        subprocess.run(["cp", "-R", str(DIR / "mirror_neuron"), str(context_dir)], check=True)
        (context_dir / "Dockerfile").write_text(
            """FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \\
    bash \\
    ca-certificates \\
    curl \\
    libgcc-s1 \\
    libstdc++6 \\
    libssl3 \\
    ncurses-bin \\
    openssl \\
    procps \\
    && rm -rf /var/lib/apt/lists/*

ARG OPENSHELL_VERSION=v0.0.47
RUN set -eux; \\
    arch="$(dpkg --print-architecture)"; \\
    case "$arch" in \\
      arm64) openshell_target="aarch64-unknown-linux-musl"; openshell_sha="a6aa05593aa5bd6936bbb87fa3958510c1a6d82ef11b8ed8498e884de50847c0" ;; \\
      amd64) openshell_target="x86_64-unknown-linux-musl"; openshell_sha="75ea23c19c23a931ac34b274f719c60dd20c6f788f2a4551862ec17572d84c17" ;; \\
      *) echo "unsupported architecture for OpenShell: $arch" >&2; exit 1 ;; \\
    esac; \\
    curl -fLsS -o /tmp/openshell.tar.gz \\
      "https://github.com/NVIDIA/OpenShell/releases/download/${OPENSHELL_VERSION}/openshell-${openshell_target}.tar.gz"; \\
    echo "${openshell_sha}  /tmp/openshell.tar.gz" | sha256sum -c -; \\
    tar -xzf /tmp/openshell.tar.gz -C /usr/local/bin openshell; \\
    chmod 0755 /usr/local/bin/openshell; \\
    rm -f /tmp/openshell.tar.gz; \\
    openshell --version

ARG CORE_RELEASE_TAG
LABEL org.opencontainers.image.version="${CORE_RELEASE_TAG}"

WORKDIR /opt/mirror_neuron
COPY mirror_neuron /opt/mirror_neuron

ENV HOME=/opt/mirror_neuron
EXPOSE 55051 4369 54370

CMD ["bin/mirror_neuron", "foreground"]
"""
        )
        subprocess.run(
            [
                "docker",
                "build",
                "--build-arg",
                f"CORE_RELEASE_TAG={tag}",
                "-t",
                "mirror-neuron-core:latest",
                str(context_dir),
            ],
            check=True,
        )

    _write_install_metadata(tag, platform, asset_url)


def _write_install_metadata(tag: str, platform: str, asset_url: str) -> None:
    DIR.mkdir(parents=True, exist_ok=True)
    INSTALL_METADATA_FILE.write_text(
        json.dumps(
            {
                "core_release_tag": tag,
                "core_platform": platform,
                "core_asset_url": asset_url,
                "updated_at": time.time(),
            },
            indent=2,
        )
        + "\n"
    )
