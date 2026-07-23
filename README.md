# MirrorNeuron CLI

`mn-cli` provides the `mn` command for validating and running blueprints,
inspecting runtime state, managing jobs, exporting artifacts, and starting local
services installed by `mn-deploy`.

## Quick Start

Install locally and run tests:

```bash
python3.11 -m venv .venv
. .venv/bin/activate
.venv/bin/python -m pip install -e .
.venv/bin/python -m pytest -q
```

Try the CLI:

```bash
mn --version
mn node list
mn blueprint run message_routing_trace
```

## Fast runtime-model orchestration tests

Model-aware blueprint launch logic is testable without Core, Docker, DMR,
LiteLLM, SSH, or a network. `RuntimeModelDependencies` supplies the model
catalog, resource report, system summary, `BlueprintModelOps`, and gateway
effects used by the real `run_bundle` handler. The reusable
`tests/runtime_model_fakes.py` cluster records model preparation, remote-route
reconciliation, and LiteLLM synchronization in memory.

Run the focused gate from this workspace:

```bash
../mn-system-tests/.venv/bin/python -m pytest -q \
  tests/test_run_cmds_models.py tests/test_run_cmds_run.py \
  -k "adaptive_model_placement or injected_remote_installed_state or injected_cluster"
```

The runtime-selection scenarios are:

- a local-only 16 GB Apple node validates the portable Gemma fallback policy;
- adding a healthy 128 GB CUDA node validates that Nemotron is feasible;
- already-installed remote models remain usable without a second install;
- the first SDK model call selects and prepares the owner node, then uses that
  node's reachable LiteLLM gateway route.

`mn blueprint run --debug` prints the deferred policy for blueprint-declared
foundational LLMs. RAG and OCR model details are owned by their skills and
appear only in runtime events when those skills first call the SDK wrapper.
Those events report the selected model/node, fallback reason, and install/reuse state. Debug mode
also prints DockerWorker build commands and complete captured build output,
including builds performed through a remote node's native SDK service.

Live Spark checks are a separate, opt-in boundary smoke after this injected
gate passes; they are not the development loop for placement policy.

Override blueprint config for one run without changing `config/overwrite.json`:

```bash
mn blueprint run --folder ./vc_assistant \
  --set document_sources.folder_path=/path/to/documents \
  --set execution.debug=true
```

Repeat `--set` for multiple values. Values use JSON types when possible and
otherwise remain strings.

## Stable jobs and execution runs

Create a reusable job once, then start independent runs that share its declared
job data:

```bash
mn job create ./vc_assistant --job-id vc-diligence
mn job inspect vc-diligence
mn job start vc-diligence --inputs run-input.json
mn job runs vc-diligence

mn run status <run-id>
mn run pause <run-id>
mn run resume <run-id>
mn run cancel <run-id>
```

`job_id` is the stable configuration and data owner. `run_id` is one
execution and the identity used for control, logs, output, retention, and run
deletion. Starting the same job again creates another run; retrying a run does
not. Use `mn blueprint run --job-id <job-id>` to run an existing definition.
Without that option, blueprint run creates an ephemeral stable job and starts
its first run.

Lifecycle commands are deliberately separate:

```bash
mn job archive vc-diligence            # retains shared data
mn job reset-data vc-diligence         # confirms; clears/reseeds and advances generation
mn run delete <terminal-run-id>         # confirms; never deletes shared data
mn job delete vc-diligence              # confirms; permanently deletes definition and data
```

The legacy `mn job status/pause/resume/cancel <old-job-id>` commands remain
execution-oriented v1 compatibility commands. Prefer `mn run ...` for v2.

## Durable bulk operations

`mn job cancel-all`, `mn job clear`, `mn node reconcile`, and `mn node drain`
start a durable Core operation and render item updates in completion order.
`MN_CLI_OUTPUT=plain` emits stable `→`, `✓`, and `! Warning:` progress lines;
the rich terminal shows live counters and recent results.

If the owner of a cancelled job is offline, `cancellation_pending` means the
request was accepted and cleanup is queued for that node's rejoin. It is not a
command failure. Ctrl+C detaches without aborting the operation; reattach with:

```bash
mn operation status op-…
mn operation watch op-…
```

`mn job clear` is destructive and requires confirmation unless `--yes` is
provided.

## Configuration

Configuration is loaded by `mn_cli.config`. `.env` files provide defaults, and
real environment variables always override them. `MN_ENV` selects the
environment-specific defaults file and defaults to `dev` when unset.

Precedence:

```text
real environment variables
> .env.${MN_ENV}
> .env
> built-in safe defaults
```

Development:

```bash
export MN_ENV=dev
cp .env.example .env.dev
mn --version
```

Tests:

```bash
export MN_ENV=test
mn --version
```

Production does not require any `.env` file. Provide deployment-specific values
through the real environment:

```bash
export MN_ENV=production
export MN_HOME=/var/lib/mirrorneuron
export MN_LOG_LEVEL=info
export MN_API_HOST=0.0.0.0
export MN_API_PORT=8080
mn runtime status
```

Keep secrets, credentials, production hostnames, production database URLs, cloud
credentials, and user-specific local paths out of source files. Use
environment variables or uncommitted `.env` files instead.

## Details

- [MirrorNeuron Component Guide](../mn-docs/component-guide.md#cli)
- [CLI Reference](../mn-docs/cli.md)
- [Environment Variables](../mn-docs/env_variables.md)
- [Monitor Guide](../mn-docs/monitor.md)

## Release Updates

`mn runtime update` and the periodic interactive update check use the newest
stable `install_support/v*` snapshot in `MirrorNeuronLab/mn-deploy` as their
release plan. The snapshot pins the Core release tag, the SDK/CLI/API Python
package versions, and the Web UI npm version. The updater installs the exact
Python package versions from the public GAR `agent-skills` index and configures
the exact Web UI npm version for Docker Compose; it does not follow a source
branch, package-manager `latest` tag, or the Core repository's latest-release
endpoint.

The Core remains a versioned GitHub Release binary because it is not a Python
or npm package. Its release asset URL is constructed from the same support
snapshot tag. For private mirrors, set `MN_DEPLOY_REPO`, `MN_DEPLOY_REF`,
`MN_PIP_INDEX_URL`, or `MN_PIP_EXTRA_INDEX_URL` before running the command.

## Notes

- A running MirrorNeuron core is required for live runtime commands.
- The default gRPC target comes from `MN_GRPC_TARGET`, then local deployment
  settings, then `localhost:55051`.
- Use `mn blueprint validate` before `mn blueprint run --folder` when checking a local bundle.
- Validation honors first-use runtime-model installation, so a compatible
  declared model need not already be installed.
- `mn blueprint run` validates model declarations but does not install models.
  Workers select, install, and route each managed model on its first actual use.
- Docker workers receive a worker-reachable model-control target and use the
  SDK to select the best cluster node independently for LLM and for model
  specifications supplied at runtime by RAG and OCR skills.
- `default` is a LiteLLM model group, not a concrete model: it prefers
  `nemotron3` and falls back to `gemma4:e2b` when no healthy node can run
  Nemotron. The existing cluster model monitor
  rebuilds these routes as nodes join, rejoin, or leave; incomplete peer
  snapshots retain the last safe routes until departure is confirmed.
  Gateway route names and `fallback_model` are read from the SDK's merged model
  catalog, including `~/.mn/models/catalog.json` (or `$MN_HOME`) and the
  highest-priority `MN_MODEL_CATALOG_PATH` override.
- `--debug` retains complete Docker build diagnostics and prints deferred model
  policies. Actual model/node selection appears later in runtime events.
