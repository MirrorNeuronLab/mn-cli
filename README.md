# MirrorNeuron CLI

Command-line interface for operating a local MirrorNeuron installation.

The CLI submits workflow bundles, monitors jobs, manages the local runtime services, and runs blueprint workflows through a shared command surface.

## Features

- Submit, inspect, pause, resume, cancel, and clear runtime jobs.
- Stream job events and inspect dead-letter records.
- Start and stop the local MirrorNeuron components.
- Run blueprint catalog entries or local blueprint folders.
- Export blueprint run artifacts as Markdown or static HTML.
- Check for released package updates and install them after user confirmation.

## Tech Stack

| Area | Tooling |
| --- | --- |
| Runtime | Python 3.11+ |
| CLI framework | Typer |
| Terminal rendering | Rich |
| Core client | `mirrorneuron-python-sdk` |
| Packaging | setuptools with setuptools-scm |

## Prerequisites

- Python 3.11 or newer.
- A MirrorNeuron core reachable over gRPC.
- Docker for the default local core and Redis workflow.
- Optional: the released-package installer from `mn-deploy`, which installs and wires the CLI automatically.

## Installation

The released-package installer installs this package and exposes `mn` on your `PATH`.

Standalone install:

```bash
pip install mirrorneuron-cli
```

Developer install:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `MN_GRPC_TARGET` | `localhost:50051` | Core gRPC target. |
| `MN_CORE_GRPC_TARGET` | unset | Fallback core gRPC target. |
| `MN_GRPC_TIMEOUT_SECONDS` | `10` | RPC timeout. Use `0` or `none` to disable. |
| `MN_GRPC_AUTH_TOKEN` | unset | Optional bearer metadata for protected gateways. |
| `MN_CLI_LOG_PATH` | `~/.mn/logs/cli.log` | CLI log file path. |
| `MN_CLI_OUTPUT` | `rich` | Set to `plain` to disable Rich formatting. |
| `MN_DISABLE_UPDATE_CHECK` | unset | Set to `1`, `true`, or `yes` to disable automatic update checks. |
| `MN_UPDATE_CHECK_INTERVAL_SECONDS` | `86400` | Minimum time between automatic update checks. |
| `MN_CORE_REPO` | `MirrorNeuronLab/MirrorNeuron` | GitHub repository used for core release update checks. |

## Usage

Check the installed CLI version:

```bash
mn --version
```

Check the runtime:

```bash
mn nodes
mn metrics
```

Submit and inspect a workflow:

```bash
mn validate ./bundle
mn run ./bundle
mn list
mn unfinished
mn status <job_id>
mn monitor <job_id>
```

Manage jobs:

```bash
mn pause <job_id>
mn resume <job_id>
mn cancel <job_id>
mn dead-letters <job_id>
mn clear
```

Manage local services:

```bash
mn start
mn stop
```

## Blueprint Commands

Blueprint commands are grouped under `mn blueprint`.

```bash
mn blueprint list
mn blueprint install
mn blueprint update
mn blueprint cleanup
mn blueprint cleanup --blueprint-id <blueprint_id>
mn blueprint uninstall
mn blueprint run <blueprint_id>
mn blueprint --blueprint-repo https://github.com/MirrorNeuronLab/customer-blueprints run <blueprint_id>
mn blueprint run ./path/to/bundle_or_source_blueprint
mn blueprint run <blueprint_id> --offline
mn blueprint run <blueprint_id> --revision <git_sha_or_tag>
mn blueprint monitor --follow
mn blueprint tail <run_id>
mn blueprint compare <run_a> <run_b>
mn blueprint export <run_id> --format markdown
mn blueprint export <run_id> --format html
```

`mn blueprint run` accepts either an installed blueprint ID or a local folder. If the folder is already a bundle, the CLI submits it directly. If the folder is a Python source blueprint, the CLI generates a bundle under:

```text
~/.mn/generated_blueprint_bundles/<run_id>/
```

Catalog runs use the cached blueprint library by default. Run `mn blueprint update` or pass `--update` when you want to refresh the local cache.

`mn blueprint update` also checks for blueprints removed from the catalog and cleans blueprint-owned runtime resources, including cached Python virtualenvs, `~/.mn/runs/<run_id>` records, `~/.mn/generated_blueprint_bundles/<run_id>` bundles, local bundle-cache entries, and Docker resources labelled with `mirrorneuron.blueprint_id=<blueprint_id>` or `com.mirrorneuron.blueprint_id=<blueprint_id>`. Use `mn blueprint cleanup` to run the same dead-resource check manually, or `mn blueprint cleanup --blueprint-id <id>` to remove resources for one deleted blueprint. Use `--dry-run` to preview removals. Cleanup is lifecycle-driven and explicit; there is no hidden scheduled housekeeping job.

Use `mn blueprint --blueprint-repo <repo-url> ...` to read catalog commands from a different blueprint repository, including a private repository your Git credentials can access. Custom repositories are cached separately under `~/.mn/blueprint_repos/`, and the repository root must contain a valid `index.json` JSON list of blueprint entries.

Blueprint run artifacts are stored under:

```text
~/.mn/runs/<run_id>/
```

Use `--runs-root <path>` with `monitor`, `tail`, `compare`, or `export` to inspect a custom run directory.

## Updates

The CLI checks for released package updates in interactive terminals. When an update is available, it asks for confirmation before making changes.

Updating stops all MirrorNeuron components and running jobs. Update only when no important jobs are running. Backward compatibility is not guaranteed between releases.

Manual update commands:

```bash
mn update --check-only
mn update
mn update --yes
```

The updater checks:

- PyPI packages: SDK, blueprint support skill, CLI, and API.
- npm package: Web UI, when installed.
- GitHub Releases: MirrorNeuron core OTP tarball.

## Testing

```bash
python3 -m pytest -q
```

## Deployment

For normal local installs, use `mn-deploy/install_new.sh`. It installs the CLI from PyPI, the Web UI from npm, and the core from GitHub Release OTP tarballs.

For custom deployments, install `mirrorneuron-cli` into a managed virtual environment and set `MN_GRPC_TARGET` to the runtime gateway.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| `mn` is not found | Ensure `~/.local/bin` is on `PATH`. |
| Runtime commands fail | Confirm the core is running and `MN_GRPC_TARGET` is correct. |
| Output contains terminal control codes | Set `MN_CLI_OUTPUT=plain`. |
| Update prompt is unwanted | Set `MN_DISABLE_UPDATE_CHECK=1`. |
| Web UI is not started by `mn start` | Confirm the Web UI was installed by the released-package installer. |

## Contributing

Keep command names stable and add tests for new commands, flags, and error paths. Prefer shared SDK methods over direct protocol handling.

## License

MIT.
