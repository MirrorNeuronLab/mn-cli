from __future__ import annotations

import base64
import json
import os
import posixpath
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
import tomllib
import urllib.request
from importlib import metadata
from pathlib import Path
from pathlib import PurePosixPath
from typing import Any, Optional, TypedDict
from urllib.parse import quote

import typer
from rich.console import Console

from mn_cli.libs.ui import print_confirmed, print_info, print_success_confirmation
from mn_cli.runtime_state import read_json_object
from mn_cli.server_cmds import (
    DIR,
    RUNTIME_COMPOSE_ENV,
    RUNTIME_COMPOSE_FILE,
    WEB_UI_DIRS,
    _start_server,
    _write_env_file_values,
)

console = Console()


CHECK_FILE = DIR / ".update-check.json"
INSTALL_METADATA_FILE = DIR / "install_metadata.json"
CHECK_INTERVAL_SECONDS = int(os.getenv("MN_UPDATE_CHECK_INTERVAL_SECONDS", "86400"))
PYTHON_PACKAGES = [
    "mirrorneuron-python-sdk",
    "mirrorneuron-cli",
    "mirrorneuron-api",
]
NPM_PACKAGE = "mirrorneuron-web-ui"
CORE_REPO = os.getenv("MN_CORE_REPO", "MirrorNeuronLab/MirrorNeuron")
DEPLOY_REPO = os.getenv("MN_DEPLOY_REPO", "MirrorNeuronLab/mn-deploy")
DEPLOY_REF = os.getenv("MN_DEPLOY_REF", "main")
DEPLOY_SUPPORT_DIRECTORY = "install_support"
GAR_PYTHON_INDEX_URL = os.getenv(
    "MN_PIP_INDEX_URL",
    os.getenv(
        "MN_PYTHON_INDEX_URL",
        "https://us-central1-python.pkg.dev/mirrorneuron-public-packages/agent-skills/simple/",
    ),
)
PYTHON_EXTRA_INDEX_URL = os.getenv(
    "MN_PIP_EXTRA_INDEX_URL",
    os.getenv("MN_PYTHON_EXTRA_INDEX_URL", "https://pypi.org/simple"),
)
CORE_INSTALL_PRESERVE_NAMES = frozenset({".pids", ".logs", ".update-check.json"})
STABLE_RELEASE_TAG = re.compile(r"^v(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)$")
WEB_UI_VERSION_PATTERN = re.compile(
    r"MN_WEB_UI_PACKAGE_VERSION:\s*\$\{MN_WEB_UI_PACKAGE_VERSION:-(?P<version>[^}]+)\}"
)


class ReleasePlan(TypedDict):
    release_tag: str
    python_versions: dict[str, str]
    web_ui_version: str


CORE_DOCKERFILE = """FROM debian:bookworm-slim

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

ARG DOCKER_CLI_VERSION=29.2.1
RUN set -eux; \\
    arch="$(dpkg --print-architecture)"; \\
    case "$arch" in \\
      arm64) docker_target="aarch64" ;; \\
      amd64) docker_target="x86_64" ;; \\
      *) echo "unsupported architecture for Docker CLI: $arch" >&2; exit 1 ;; \\
    esac; \\
    curl -fLsS -o /tmp/docker-cli.tgz \\
      "https://download.docker.com/linux/static/stable/${docker_target}/docker-${DOCKER_CLI_VERSION}.tgz"; \\
    tar -xzf /tmp/docker-cli.tgz -C /tmp docker/docker; \\
    install -m 0755 /tmp/docker/docker /usr/local/bin/docker; \\
    rm -rf /tmp/docker /tmp/docker-cli.tgz; \\
    docker --version

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
            "Run mn-deploy/install.sh --mode local from your checkout to refresh local components.[/yellow]"
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
    release_plan = _release_plan()
    updates: list[dict[str, str]] = []

    for package_name in PYTHON_PACKAGES:
        current = _installed_python_version(package_name)
        latest = release_plan["python_versions"][package_name]
        if current != latest:
            updates.append(
                {
                    "component": package_name,
                    "current": current or "not installed",
                    "latest": latest,
                    "kind": "python",
                    "release_tag": release_plan["release_tag"],
                }
            )

    if _web_ui_installed():
        current = _installed_npm_version()
        latest = release_plan["web_ui_version"]
        if current != latest:
            updates.append(
                {
                    "component": NPM_PACKAGE,
                    "current": current or "not installed",
                    "latest": latest,
                    "kind": "npm",
                    "release_tag": release_plan["release_tag"],
                }
            )

    current_core = _installed_core_tag()
    latest_core = release_plan["release_tag"]
    if current_core != latest_core:
        updates.append(
            {
                "component": "MirrorNeuron core",
                "current": current_core or "unknown",
                "latest": latest_core,
                "kind": "core",
                "release_tag": release_plan["release_tag"],
            }
        )

    return updates


def perform_update(available: list[dict[str, str]] | None = None) -> None:
    available = available if available is not None else get_available_updates()
    if not available:
        print_confirmed(console, "MirrorNeuron update", status="up to date")
        return

    print_info(console, "Stopping MirrorNeuron components…")
    from mn_cli.libs.sys_cmds import stop

    stop()

    python_updates = [item for item in available if item["kind"] == "python"]
    if python_updates:
        _update_python_packages(python_updates)

    npm_update = next((item for item in available if item["kind"] == "npm"), None)
    if npm_update is not None:
        _update_web_ui(npm_update["latest"])

    core_update = next((item for item in available if item["kind"] == "core"), None)
    if core_update is not None:
        _update_core(core_update["latest"])

    _record_check()
    print_success_confirmation(
        console,
        "MirrorNeuron update",
        status="installed",
        details={"Components": ", ".join(item["component"] for item in available)},
        next_steps="mn runtime health",
    )
    print_info(console, "Restarting MirrorNeuron…")
    _start_server()


def _print_updates(updates: list[dict[str, str]]) -> None:
    for item in updates:
        console.print(f"  - {item['component']}: {item['current']} -> {item['latest']}")


def _check_due() -> bool:
    data = read_json_object(CHECK_FILE)
    try:
        checked_at = float(data.get("checked_at", 0))
    except (TypeError, ValueError):
        return True
    return time.time() - checked_at >= CHECK_INTERVAL_SECONDS


def _record_check() -> None:
    CHECK_FILE.parent.mkdir(parents=True, exist_ok=True)
    CHECK_FILE.write_text(json.dumps({"checked_at": time.time()}))


def _local_source_install() -> bool:
    data = read_json_object(INSTALL_METADATA_FILE)
    if data.get("install_type") == "local_source":
        return True

    return (DIR / "core-source").exists() or (DIR / "cli-source").exists()


def _json_url(url: str) -> Any:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "mirrorneuron-cli-updater",
        },
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        return json.loads(response.read().decode("utf-8"))


def _github_contents_url(path: str, *, ref: str) -> str:
    repository = quote(DEPLOY_REPO, safe="/")
    asset_path = quote(path.strip("/"), safe="/")
    return f"https://api.github.com/repos/{repository}/contents/{asset_path}?ref={quote(ref, safe='')}"


def _github_contents(path: str, *, ref: str) -> Any:
    return _json_url(_github_contents_url(path, ref=ref))


def _github_contents_text(path: str, *, ref: str) -> str:
    content = _github_contents(path, ref=ref)
    if not isinstance(content, dict) or content.get("type") != "file":
        raise RuntimeError(f"Expected a file at {path} in {DEPLOY_REPO}@{ref}.")
    if content.get("encoding") != "base64" or not isinstance(
        content.get("content"), str
    ):
        raise RuntimeError(f"GitHub did not return base64 file content for {path}.")
    try:
        raw = base64.b64decode(content["content"].replace("\n", ""), validate=True)
        return raw.decode("utf-8")
    except (UnicodeDecodeError, ValueError) as exc:
        raise RuntimeError(f"GitHub returned invalid content for {path}.") from exc


def _release_tag_sort_key(tag: str) -> tuple[int, int, int]:
    match = STABLE_RELEASE_TAG.fullmatch(tag)
    if match is None:
        raise ValueError(f"Invalid stable release tag: {tag}")
    return tuple(int(match.group(part)) for part in ("major", "minor", "patch"))


def _latest_release_support_tag() -> str:
    entries = _github_contents(DEPLOY_SUPPORT_DIRECTORY, ref=DEPLOY_REF)
    if not isinstance(entries, list):
        raise RuntimeError(
            f"Expected a directory listing for {DEPLOY_SUPPORT_DIRECTORY}."
        )

    tags = [
        entry.get("name")
        for entry in entries
        if isinstance(entry, dict)
        and entry.get("type") == "dir"
        and isinstance(entry.get("name"), str)
        and STABLE_RELEASE_TAG.fullmatch(entry["name"])
    ]
    if not tags:
        raise RuntimeError(
            f"No stable release snapshots were found in {DEPLOY_SUPPORT_DIRECTORY}."
        )
    return max(tags, key=_release_tag_sort_key)


def _release_plan() -> ReleasePlan:
    release_tag = _latest_release_support_tag()
    index_path = (
        f"{DEPLOY_SUPPORT_DIRECTORY}/{release_tag}/package-index/python-packages.toml"
    )
    compose_path = f"{DEPLOY_SUPPORT_DIRECTORY}/{release_tag}/docker-compose.yml"
    try:
        package_index = tomllib.loads(
            _github_contents_text(index_path, ref=release_tag)
        )
    except tomllib.TOMLDecodeError as exc:
        raise RuntimeError(f"Release package index is invalid: {index_path}.") from exc

    python_versions = {
        package.get("name"): str(package.get("version", "")).lstrip("vV")
        for package in package_index.get("packages", [])
        if isinstance(package, dict)
        and isinstance(package.get("name"), str)
        and str(package.get("version", "")).strip()
    }
    missing_packages = [
        name for name in PYTHON_PACKAGES if not python_versions.get(name)
    ]
    if missing_packages:
        joined = ", ".join(missing_packages)
        raise RuntimeError(
            f"Release package index is missing required packages: {joined}."
        )

    compose = _github_contents_text(compose_path, ref=release_tag)
    web_ui_match = WEB_UI_VERSION_PATTERN.search(compose)
    if web_ui_match is None:
        raise RuntimeError(
            f"Release Compose template is missing the Web UI package version: {compose_path}."
        )

    return {
        "release_tag": release_tag,
        "python_versions": {name: python_versions[name] for name in PYTHON_PACKAGES},
        "web_ui_version": web_ui_match.group("version").strip().lstrip("vV"),
    }


def _installed_python_version(package_name: str) -> str | None:
    try:
        return metadata.version(package_name)
    except metadata.PackageNotFoundError:
        return None


def _web_ui_installed() -> bool:
    return _web_ui_compose_enabled() or any(
        (path / "package.json").exists() for path in WEB_UI_DIRS
    )


def _web_ui_compose_enabled() -> bool:
    if not RUNTIME_COMPOSE_FILE.is_file():
        return False
    try:
        compose = RUNTIME_COMPOSE_FILE.read_text(encoding="utf-8")
        environment = _read_env_file(RUNTIME_COMPOSE_ENV)
    except OSError:
        return False
    if re.search(r"^  web-ui:\s*$", compose, flags=re.MULTILINE) is None:
        return False
    profiles = {
        profile.strip()
        for profile in environment.get("COMPOSE_PROFILES", "").split(",")
        if profile.strip()
    }
    return "web-ui" in profiles


def _read_env_file(path: Path) -> dict[str, str]:
    environment: dict[str, str] = {}
    if not path.is_file():
        return environment
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        environment[key.strip()] = value.strip()
    return environment


def _installed_npm_version() -> str | None:
    if _web_ui_compose_enabled():
        return _read_env_file(RUNTIME_COMPOSE_ENV).get("MN_WEB_UI_PACKAGE_VERSION")

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


def _installed_core_tag() -> str | None:
    data = read_json_object(INSTALL_METADATA_FILE)
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


def _update_python_packages(updates: list[dict[str, str]]) -> None:
    requirements = [f"{item['component']}=={item['latest']}" for item in updates]
    print_info(console, "Updating Python packages from the release-pinned GAR index…")
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--upgrade",
            "--index-url",
            GAR_PYTHON_INDEX_URL,
            "--extra-index-url",
            PYTHON_EXTRA_INDEX_URL,
            *requirements,
        ],
        check=True,
    )


def _update_web_ui(version: str) -> None:
    if _web_ui_compose_enabled():
        print_info(
            console,
            "Updating Web UI package version in the runtime Compose configuration…",
        )
        _write_env_file_values(
            RUNTIME_COMPOSE_ENV, {"MN_WEB_UI_PACKAGE_VERSION": version}
        )
        return

    print_info(console, "Updating Web UI from the release-pinned npm package…")
    for web_ui_dir in WEB_UI_DIRS:
        if (web_ui_dir / "package.json").exists():
            subprocess.run(
                [
                    "npm",
                    "--prefix",
                    str(web_ui_dir),
                    "install",
                    f"{NPM_PACKAGE}@{version}",
                ],
                check=True,
            )
            return


def _core_asset_url(release_tag: str, platform: str) -> str:
    return (
        f"https://github.com/{CORE_REPO}/releases/download/{release_tag}/"
        f"MirrorNeuron-{release_tag}-{platform}-otp-release.tar.gz"
    )


def _download(url: str, target: Path) -> None:
    request = urllib.request.Request(
        url, headers={"User-Agent": "mirrorneuron-cli-updater"}
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        target.write_bytes(response.read())


def _safe_extract_tar(archive: tarfile.TarFile, target: Path) -> None:
    target.mkdir(parents=True, exist_ok=True)
    for member in archive.getmembers():
        _validate_tar_member(member)
    archive.extractall(target, filter=tarfile.fully_trusted_filter)


def _validate_tar_member(member: tarfile.TarInfo) -> None:
    if not _safe_archive_path(member.name):
        raise RuntimeError(
            f"Core release archive contains an unsafe path: {member.name!r}"
        )

    if member.ischr() or member.isblk() or member.isfifo():
        raise RuntimeError(
            f"Core release archive contains an unsupported special file: {member.name!r}"
        )

    if member.issym():
        link_target = _normalized_symlink_target(member.name, member.linkname)
        if not _safe_archive_path(member.linkname) or not _safe_archive_path(
            link_target
        ):
            raise RuntimeError(
                f"Core release archive contains an unsafe symlink: {member.name!r} -> {member.linkname!r}"
            )
    elif member.islnk() and not _safe_archive_path(member.linkname):
        raise RuntimeError(
            f"Core release archive contains an unsafe hard link: {member.name!r} -> {member.linkname!r}"
        )


def _safe_archive_path(name: str) -> bool:
    if not isinstance(name, str) or not name:
        return False
    path = PurePosixPath(name)
    return not path.is_absolute() and ".." not in path.parts


def _normalized_symlink_target(member_name: str, link_name: str) -> str:
    if PurePosixPath(link_name).is_absolute():
        return link_name
    parent = PurePosixPath(member_name).parent
    return posixpath.normpath(str(parent / link_name))


def _prepare_core_docker_context(context_dir: Path, install_dir: Path) -> None:
    shutil.copytree(install_dir / "mirror_neuron", context_dir / "mirror_neuron")
    (context_dir / "Dockerfile").write_text(CORE_DOCKERFILE, encoding="utf-8")


def _clear_core_install_dir(install_dir: Path) -> None:
    if install_dir.exists():
        for child in install_dir.iterdir():
            if child.name in CORE_INSTALL_PRESERVE_NAMES:
                continue
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
    install_dir.mkdir(parents=True, exist_ok=True)


def _update_core(tag: str) -> None:
    print_info(console, f"Updating MirrorNeuron core from release {tag}…")
    platform = _docker_platform()
    asset_url = _core_asset_url(tag, platform)

    with tempfile.TemporaryDirectory(prefix="mirrorneuron-core-update-") as temp_dir:
        root = Path(temp_dir)
        tarball = root / "core.tar.gz"
        context_dir = root / "docker-context"
        context_dir.mkdir()

        _download(asset_url, tarball)
        _clear_core_install_dir(DIR)

        with tarfile.open(tarball) as archive:
            _safe_extract_tar(archive, DIR)

        _prepare_core_docker_context(context_dir, DIR)
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
    data = read_json_object(INSTALL_METADATA_FILE)
    data.update(
        {
            "core_release_tag": tag,
            "core_platform": platform,
            "core_asset_url": asset_url,
            "updated_at": time.time(),
        }
    )
    INSTALL_METADATA_FILE.write_text(json.dumps(data, indent=2) + "\n")
