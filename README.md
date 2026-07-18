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

The required scenarios are:

- a local-only 16 GB Apple node selects the portable Gemma model;
- adding a healthy 128 GB CUDA node selects Nemotron and prepares every DMR
  model on that node;
- already-installed remote models remain usable without a second install;
- the submitter's LiteLLM receives the selected node's reachable LiteLLM
  upstream, while submitted workers receive only their local
  `mn-litellm-proxy:4000` endpoint; the selected node's proxy owns the direct
  route to its local DMR.

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
- `mn blueprint run` checks the complete effective model set before preparation;
  if no node can satisfy it, the command reports per-node capacity reasons and
  does not start any model installation.
- The selected node is carried through model, context, and DockerWorker
  preparation. Workers call their local LiteLLM gateway; it routes to the
  selected node's cluster-reachable LiteLLM gateway, which owns the direct
  local DMR route. This supports a selected node on another machine, such as
  Spark, even when DMR itself is not bound to the LAN.
- `default` is a LiteLLM model group, not a concrete model: it prefers
  `nemotron3` and falls back to `gemma4:e2b`. The existing cluster model monitor
  rebuilds these routes as nodes join, rejoin, or leave; incomplete peer
  snapshots retain the last safe routes until departure is confirmed.
  Gateway route names and `fallback_model` are read from the SDK's merged model
  catalog, including `~/.mn/models/catalog.json` (or `$MN_HOME`) and the
  highest-priority `MN_MODEL_CATALOG_PATH` override.
