# MirrorNeuron CLI Specification

## Purpose

`mn-cli` provides the `mn` command used to install, validate, run, inspect, and
operate local MirrorNeuron workflows and services. It is the terminal adapter
over the MirrorNeuron Python SDK and Core gRPC interfaces.

This specification applies only to this repository. It does not redefine the
runtime, SDK, API, or blueprint contracts it consumes.

## Public Surface

The root command registers these operator-facing families:

- `blueprint`: catalog, validation, registration, execution, and report export;
- `job`: durable definition creation, inspection, archive, data reset, and start;
- `run`: listing, inspection, and lifecycle control of executions;
- `node`: cluster membership, drain, reconcile, and maintenance;
- `operation`: durable group-operation inspection and reattachment;
- `runtime`: start, stop, aggregate status, doctor, sidecars, and updates;
- `resource`, `service`, and `model`: local and cluster capability management.

`mn_cli/main.py` and each Typer sub-application are authoritative for exact
commands and options. Public command names, option meanings, exit codes, and
machine-readable output are compatibility-sensitive.

```text
blueprint  list add show update remove run validate doctor cleanup export
job        list create show start archive reset-data delete
run        list show watch logs result resources compare pause resume cancel delete
run human  list respond ack
model      list add show update remove doctor
runtime    start stop status doctor restart-sidecars ensure-context-engine update
node       list show add remove reconcile drain undrain maintenance refresh-token
operation  show watch
resource   show usage set
service    list show
```

Obsolete deployment, global-schedule, event, backup, inline-job, and
`stable-job` command groups are not registered. The root diagnostic flag is
`--debug`; `--verbose` is not registered.

`mn runtime start` has one federation-capable mode. It starts an independent
Core with its own writable coordination store and prints the advertised host,
gRPC endpoint, node identity, active federation join token, and an exact
`mn node add` command. The former `--worker` option is removed and must fail as
a usage error with a migration hint to use `mn runtime start`.

`mn blueprint run --web-ui-host HOST --web-ui-port PORT` projects per-run
listener overrides into `web_ui.service.host` and `web_ui.service.port`.
Blueprint manifest/config bindings remain responsible for mapping those
settings into the executable service declaration.

## Behavior Boundary

The CLI owns:

- parsing terminal arguments and environment-backed configuration;
- interactive confirmations and human-readable rendering;
- plain/machine-readable terminal behavior;
- local process, Docker, Redis, sidecar, and cluster service orchestration; and
- pre-submission preparation of job-scoped OpenShell sandboxes and their
  concrete runtime configuration; and
- streaming blueprint payloads into content-addressed storage and preparing
  local payload models; and
- conversion of SDK/runtime failures into actionable terminal errors.

The CLI delegates reusable manifest conversion, submission preparation, model
resolution, workflow progress, and runtime client behavior to
`mirrorneuron-python-sdk`. It must not become an independent implementation of
those contracts.

## Output Contract

- Every leaf command accepts `--json`. One-shot commands return exactly one
  `mn.cli/v1` envelope; followed and watch commands emit `mn.cli.stream/v1`
  NDJSON records.
- Default output is concise, human readable, and action oriented. Results use
  stdout while progress, warnings, and errors use stderr.
- Interactive Rich sessions show a transient spinner while a run pause, resume,
  or cancel request is in flight. JSON and plain output omit transient progress
  so their output remains automation-safe.
- `MN_CLI_OUTPUT=plain` removes terminal decoration and stays stable enough for
  automation. `NO_COLOR` removes color without removing meaning.
- Rich result panels are reserved for lifecycle results; routine mutations use
  a compact status and summary.
- Errors use the same JSON envelope with `ok: false` and sanitized
  `code/message/hint/details`. Internal diagnostics appear only with `--debug`.
- Exit codes are `0` success, `1` operational/critical diagnostics, `2`
  usage/validation/not-found/confirmation, `13` authorization, and `130`
  interruption except an intentional watcher detach.
- Interactive monitors must preserve keyboard accessibility and clearly show
  selection without relying on reverse-video backgrounds.
- Durable group operations render item completion in arrival order. Ctrl+C
  detaches while leaving Core work active and prints the operation ID. A
  `cancellation_pending` item is accepted success with queued remote cleanup;
  explicit item failures retain a nonzero final exit code.

## Safety

- Commands that delete, remove, cancel broadly, expose listeners, or
  alter cluster membership require deliberate user intent.
- Values from manifests, catalogs, the filesystem, environment, SDK, gRPC, and
  subprocesses are untrusted and must be validated or safely rendered.
- Secrets, bearer tokens, passwords, and unredacted environment values must not
  be printed or logged, except that a successful `mn runtime start`
  intentionally displays its active federation join token and exact add-node
  command to the invoking operator. Structured diagnostics and unrelated
  commands continue to redact that credential.
- Unit tests use fakes and temporary paths; normal tests do not mutate the real
  `~/.mn`, start services, or access the network.
- Durable-job archive retains shared data. Job-data reset, terminal-run delete,
  and permanent job delete require confirmation. Run cleanup must never be
  presented as deleting durable job data. Permanent job deletion also removes
  all historical runs and definition-owned runtime resources.
- `--yes` answers confirmation only, `--force` overrides one documented
  precondition but never supplies consent, and `--dry-run` never mutates.
  Destructive JSON/non-interactive commands require `--yes`.
## Durable Job/Run Contract

`mn job list/create/show/start/archive/reset-data/delete` addresses durable job
definitions. `mn run list/show/watch/logs/result/resources/compare/pause/resume/cancel/delete`
addresses executions and always
accepts `run_id`. A durable `job_id` owns configuration, schedules, and job data;
every intentional start gets a distinct run identity, while attempts retain
their run. CLI output must label and persist both fields without treating them
as aliases.

`mn blueprint run` creates a durable job and first run by default, or starts a
new run of the `--job-id` definition. For an explicit existing job, the command
first prepares and atomically installs the current executable bundle while
preserving job data, schedules, and prior run history. `mn job start` and
scheduled dispatch remain source independent and reuse the stored bundle.
Blueprint launches use the SDK run-store writer for the job/run mapping and
sanitized source-facing monitor manifest; API launches consume the same
contract so both surfaces render the same public workflow steps.
Historical execution-control commands are not registered under `mn job`.
`mn run result` materializes the structured terminal result for completed,
failed, and cancelled runs so worker diagnostics remain available before an
operator deletes the run.

## Runtime-Model Launch Contract

The public `mn model` command surface is exactly `list`, `add`, `show`,
`update`, `remove`, and `doctor`. `add` accepts either one catalog/arbitrary DMR
reference or one canonical provider JSON file. DMR placement chooses the best
eligible cluster node unless `--local` or `--node` is supplied. Provider files
are validated in full, including required environment references, before the
SDK registry changes. If the requested DMR artifact is already installed on an
eligible local or cluster node, `add` adopts that artifact and registers it
without reinstalling it.

`list` renders registered models and discovered unmanaged DMR artifacts;
`--available` also includes catalog-only choices. Machine records expose
explicit kind, state, registration, installation, routing, node, catalog, and
verification facts. Mutating commands support `--json`. `remove` is ID-based,
requires confirmation or `--yes`, preserves blueprint ownership unless
`--force`, and deletes a DMR artifact unless `--keep-artifact` is used.
Provider removal never deletes its source JSON.

The removed `install`, `proxy`, and `remote` command trees have no compatibility
aliases. Reusable provider parsing, registry persistence, resolution, and
gateway projection remain SDK-owned; the CLI owns input parsing, confirmation,
placement/fan-out orchestration, progress, and rendering.

`mn model add ... --default` records exactly one operator-selected default in
the SDK registry. It may be a DMR registration or a single-model provider file.
The selected route precedes Nemotron and Gemma; the built-ins remain ordered
fallbacks. Selecting another default does not remove the earlier registration,
and removing the selected registration restores built-in selection.

`mn blueprint run` validates the effective blueprint-declared foundational LLM
models without installing or routing them. RAG and OCR model specifications are
not launch declarations; their skills pass them to the SDK on first use, so
each consumer may choose the best compatible cluster node independently.
`mn blueprint validate` applies that same first-use policy without side effects:
it accepts a compatible deferred model while still rejecting unknown models or
models with no feasible hardware/fallback path.

The selected node's cluster-reachable LiteLLM endpoint is the submitter
gateway's upstream. The selected-node gateway owns the direct route to its
node-local DMR. Worker configuration receives only a local LiteLLM endpoint and
logical aliases, never a remote node's DMR URL as the worker-facing API base.
Already-installed and newly-installed models follow the same routing
projection.

Workflows that use node-local runners are pinned to one feasible runtime node
before submission. Accelerator requirements select by available accelerator
headroom; CPU-only HostLocal workflows prefer the submitting node to avoid an
unnecessary cluster boundary.
The hard `node.name` constraint is reapplied after topology lowering so
generated controls cannot split from executors. Runtime health and join
diagnostics expose the coordination-store identity and writable-primary state;
nodes using divergent Redis datasets or a read-only replica are rejected
before membership or launch.

Context-memory preparation uses the local Compose lifecycle when placement
selects the submitting node; only genuinely remote selected nodes use the
native runtime preparation boundary.
Prepared HostLocal Python environments retain separate host and Core-visible
paths; submissions use the configured Core cache mount so console-script
entrypoints resolve inside a containerized local runtime.
For a distributed workflow forwarded to a federated owner, HostLocal Python
environments are prepared on that owner even though no single-node placement
marker is added to the workflow.
Background output relays poll the durable job ID, while run-store paths retain
the distinct execution run ID.
An explicitly configured non-default gRPC target is not treated as the local
managed Docker Core merely because a standard Core container is also running.
The local Docker runtime constrains automatic service ports to its published
`MN_AUTO_PORT_START`-`MN_AUTO_PORT_END` range and binds that publication to
host loopback. Its container-loopback proxy marker is passed only to services
that explicitly request it from the blueprint.

Detached output relays remain active until the run becomes terminal unless
`MN_RUN_EVENT_RELAY_MAX_SECONDS` explicitly supplies an operator limit. A
blueprint's stream-duration budget does not truncate output materialization.

The blueprint run adapter must not prepare models. A logical `default`
declaration remains blueprint-owned intent; the runtime SDK first uses the
operator-selected registry default, then chooses Nemotron on a healthy 48
GB-or-above accelerator node or Gemma when no compatible Nemotron node exists.
Debug launch output reports the effective deferred fallback policy and complete
DockerWorker build command/output details.
Skill-owned RAG/OCR model details are absent from launch preparation and appear
in runtime events only when invoked. Runtime events report the actual model,
selected node, install/reuse state, fallback reason, and duration.

`default` is a logical LiteLLM model group. When a medium route is available it
aliases to Nemotron and has Gemma as its fallback; without a medium route it
aliases to Gemma. `run_cluster_model_monitor` remains the single dynamic route
lifecycle: complete joined-node inventories add routes, complete membership
after departure removes routes, and incomplete snapshots do not destructively
replace the last known route set.
When a local node's dynamic address changes, the monitor rehomes only a local
DMR registration whose former owner is absent and whose artifact is confirmed
locally installed, then rebuilds its gateway route from the live endpoint. It
does not use a hostname unless that hostname is independently resolvable by
the participating nodes.

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
environment. An update is offered only when the release-plan component version
is strictly newer than the installed stable version; a stale snapshot cannot
offer or install a downgrade. `MN_DEPLOY_REPO`, `MN_DEPLOY_REF`,
`MN_PIP_INDEX_URL`, and `MN_PIP_EXTRA_INDEX_URL` are the supported
update-source overrides.

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
