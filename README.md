# MirrorNeuron CLI

The official Command Line Interface for managing the MirrorNeuron distributed runtime system.

## Installation
*Note: This tool is installed automatically and symlinked globally as `mn` by the MirrorNeuron `install.sh` script.*

```bash
pip install mn-cli
```

## Commands
Powered by Typer and Rich, this CLI provides elegant formatting for job lifecycle management:

```bash
mn nodes                 # View system summary and available executor pools
mn submit ./flow.json    # Submit a new workflow manifest
mn list                  # View all active and completed jobs
mn status <job_id>       # Detailed execution state of a specific job
mn monitor <job_id>      # Stream live execution events
mn cancel <job_id>       # Gracefully terminate a workflow
mn metrics               # Inspect runtime metrics summary
mn dead-letters <job_id> # Inspect dead-letter events
```

## Blueprint Commands

Blueprint execution and observability are grouped under `mn blueprint` so users have one CLI entry point:

```bash
mn blueprint list
mn blueprint install
mn blueprint update
mn blueprint run <blueprint_id>
mn blueprint run ./path/to/bundle_or_source_blueprint
mn blueprint run <blueprint_id> --offline
mn blueprint run <blueprint_id> --revision <git_sha_or_tag>
mn blueprint monitor --follow
mn blueprint tail <run_id>
mn blueprint compare <run_a> <run_b>
mn blueprint export <run_id> --format markdown
mn blueprint export <run_id> --format html
```

`mn blueprint run` accepts either an installed blueprint ID or a local folder. If the local folder is already a bundle, the CLI submits it directly; if it is a Python source blueprint, the CLI generates a bundle under `~/.mn/generated_blueprint_bundles/<run_id>/` first. Catalog runs use the cached blueprint library by default so runs are not silently changed by a network pull. Use `mn blueprint update` or `mn blueprint run <id> --update` when you want to refresh the cache. Each blueprint submission pre-generates a shared `MN_RUN_ID`, injects `MN_BLUEPRINT_CONFIG_JSON` into runtime workers, and prints both the blueprint run ID and runtime job ID.

Blueprint run artifacts are read from the shared run store at `~/.mn/runs/<run_id>/`.
Use `--runs-root <path>` with `monitor`, `tail`, `compare`, or `export` when inspecting a custom run directory.
When a blueprint registers a shared or custom web UI, `monitor` shows the local URL and `export --format html` creates a static report page.

## Configuration

All overrides use `MN_` env vars:

- `MN_GRPC_TARGET`: core gRPC target.
- `MN_GRPC_TIMEOUT_SECONDS`: RPC timeout; `0` or `none` disables it.
- `MN_GRPC_AUTH_TOKEN`: optional bearer metadata for protected core gateways.
- `MN_CLI_LOG_PATH`: error log path.
- `MN_CLI_OUTPUT=plain`: disable Rich color output.
