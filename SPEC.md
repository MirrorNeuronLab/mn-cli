# MirrorNeuron CLI Specification

## Purpose

`mn-cli` provides the `mn` command used to install, validate, run, inspect, and
operate local MirrorNeuron workflows and services. It is the terminal adapter
over the MirrorNeuron Python SDK and Core gRPC interfaces.

This specification applies only to this repository. It does not redefine the
runtime, SDK, API, or blueprint contracts it consumes.

## Public Surface

The root command registers these operator-facing families:

- `blueprint`: catalog, validation, installation, execution, and outputs;
- `job`: submission, inspection, control, backup/restore, monitor, and result;
- `node`: cluster membership, exposure, drain, reconcile, and maintenance;
- `operation`: durable group-operation status and reattachment;
- `runtime`: start, stop, status, health, doctor, sidecars, and updates;
- `resource`, `service`, and `model`: local and cluster capability management;
- `deployment`: versioned deployment operations; and
- `schedule`, `trigger`, and `event`: scheduled/event-driven execution.

`mn_cli/main.py` and each Typer sub-application are authoritative for exact
commands and options. Public command names, option meanings, exit codes, and
machine-readable output are compatibility-sensitive.

## Behavior Boundary

The CLI owns:

- parsing terminal arguments and environment-backed configuration;
- interactive confirmations and human-readable rendering;
- plain/machine-readable terminal behavior;
- local process, Docker, Redis, sidecar, and cluster service orchestration; and
- conversion of SDK/runtime failures into actionable terminal errors.

The CLI delegates reusable manifest conversion, submission preparation, model
resolution, workflow progress, and runtime client behavior to
`mirrorneuron-python-sdk`. It must not become an independent implementation of
those contracts.

## Output Contract

- Default output is concise, human readable, and action oriented.
- `MN_CLI_OUTPUT=plain` removes terminal decoration and stays stable enough for
  automation. `NO_COLOR` removes color without removing meaning.
- Rich result panels are reserved for lifecycle results; routine mutations use
  a compact status and summary.
- Errors identify the failed operation and provide a stable code or next action
  when available. Internal diagnostics appear only in debug/verbose mode.
- Interactive monitors must preserve keyboard accessibility and clearly show
  selection without relying on reverse-video backgrounds.
- Durable group operations render item completion in arrival order. Ctrl+C
  detaches while leaving Core work active and prints the operation ID. A
  `cancellation_pending` item is accepted success with queued remote cleanup;
  explicit item failures retain a nonzero final exit code.

## Safety

- Commands that delete, clear, uninstall, cancel broadly, expose listeners, or
  alter cluster membership require deliberate user intent.
- Values from manifests, catalogs, the filesystem, environment, SDK, gRPC, and
  subprocesses are untrusted and must be validated or safely rendered.
- Secrets, bearer tokens, passwords, and unredacted environment values must not
  be printed or logged.
- Unit tests use fakes and temporary paths; normal tests do not mutate the real
  `~/.mn`, start services, or access the network.

## Runtime-Model Launch Contract

`mn blueprint run` resolves the complete effective model set before performing
model preparation. One hardware-fitness decision is reused for workflow
placement and node-local preparation. A capable 128 GB cluster node therefore
wins over a smaller submitter for a medium model, while a local-only node uses
the declared portable fallback.

The selected node's cluster-reachable LiteLLM endpoint is the submitter
gateway's upstream. The selected-node gateway owns the direct route to its
node-local DMR. Worker configuration receives only a local LiteLLM endpoint and
logical aliases, never a remote node's DMR URL as the worker-facing API base.
Already-installed and newly-installed models follow the same routing
projection.

`default` is a logical LiteLLM model group. When a medium route is available it
aliases to Nemotron and has Gemma as its fallback; without a medium route it
aliases to Gemma. `run_cluster_model_monitor` remains the single dynamic route
lifecycle: complete joined-node inventories add routes, complete membership
after departure removes routes, and incomplete snapshots do not destructively
replace the last known route set.

Owner-gateway model names are resolved from each merged SDK catalog entry:
`route_aliases` takes precedence over the canonical entry ID. The normal SDK
catalog precedence applies, so `$MN_HOME/models/catalog.json` and
`MN_MODEL_CATALOG_PATH` can replace route aliases and fallback metadata without
changing CLI code.

The orchestration boundary is injectable through `RuntimeModelDependencies`.
Fast tests must provide a catalog, resource report, system summary,
`BlueprintModelOps`, and LiteLLM gateway effects and execute the real planning
and run-handler code. Live Core, Docker, DMR, SSH, and network access are not
permitted in this unit gate.

## Configuration

`mn_cli.config` loads configuration with real environment variables taking
precedence over `.env` defaults selected by `MN_ENV`. Runtime connection comes
from explicit configuration or the installed runtime endpoint metadata. New
public keys require schema/config code, `.env.example`, README, and test updates.

Release updates resolve a versioned package plan from the newest stable
`mn-deploy/install_support/v*` snapshot, not from component-repository source
branches or package-manager `latest` aliases. The plan pins the Core release
tag, Python package versions, and Web UI version. Python updates use the
configured GAR Python index (with a configurable extra index for dependencies);
the Web UI receives its pinned npm version through the installed Compose
environment. `MN_DEPLOY_REPO`, `MN_DEPLOY_REF`, `MN_PIP_INDEX_URL`, and
`MN_PIP_EXTRA_INDEX_URL` are the supported update-source overrides.

## Compatibility

Breaking changes include removing or renaming commands/options, changing option
defaults or side effects, altering exit codes, changing JSON/plain field names,
or weakening confirmations. Such changes require explicit migration treatment
and cross-consumer tests. Additive commands and options must not change omitted
behavior.

## Verification

```bash
python -m ruff check .
python -m pytest
python -m build
```

Changes to CLI/API parity or shared behavior also require the corresponding
contract suite in `mn-system-tests`, but this repository's own tests remain the
primary gate for command and presentation behavior.
