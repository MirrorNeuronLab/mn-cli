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
- `runtime`: start, stop, aggregate status, doctor, sidecars, and upgrades;
- `resource`, `service`, and `model`: local and cluster capability management.

`mn_cli/main.py` and each Typer sub-application are authoritative for exact
commands and options. Public command names, option meanings, exit codes, and
machine-readable output are compatibility-sensitive.

```text
blueprint  list add show update remove run validate doctor cleanup export
job        list create show start archive reset-data delete
run        list show watch logs result resources compare pause resume cancel delete
run human  list respond ack
model      list add show probe update remove doctor
runtime    start stop status doctor cleanup restart-sidecars ensure-context-engine upgrade
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
When the advertised gRPC port is the default (`55051`), the add-node command
omits `--grpc-port`; a non-default advertised port is included explicitly.
The advertised local identity is persisted in `$MN_HOME/docker-compose.env` and
is automatically used by Docker Compose blueprint submission; an explicitly
exported identity still takes precedence.
When Syncthing shared storage is enabled (the default), `mn node add` treats
reciprocal Syncthing device and shared-folder registration as a precondition of
Core federation. It verifies both sidecars after configuration; a failure
creates no new Core peer registration.

`mn node remove NODE --yes` removes that reciprocal federated-peer registration
from the current Core. It requires the same deliberate confirmation as other
cluster-membership mutations, does not expose join credentials, and leaves
owner-local jobs intact until the peer is joined again.

`mn blueprint run --web-ui-host HOST --web-ui-port PORT` projects per-run
listener overrides into `web_ui.service.host` and `web_ui.service.port`.
Blueprint manifest/config bindings remain responsible for mapping those
settings into the executable service declaration.
For a declared `web_ui` service or deferred job-scoped UI handle, `--web-ui`
reports the local job dashboard route. A deferred handle can appear after job
submission, but its canonical local route is stable. Both declaration paths
use `mirrorneuron-web-ui-skill` to persist the service handle and its permitted
HTTP and WebSocket companion ports, so the local Web UI server can proxy a
selected remote node without exposing its LAN URL to the browser.

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
- `mn job list` renders a durable definition's canonical Type (`service` or
  `batch`) and Node (`owner_node`), the Core runtime that owns its definition
  and job data. It does not imply a human or account owner.
- `mn node list` renders node-specific fields: Node, Hostname, Status, and a
  Role that combines connection mode with job-ownership eligibility. Nodes are
  not artifacts, so the table does not include generic Kind, Owner, or Updated
  columns. `mn node show <node>` provides the full endpoint and capability
  record.
- Human-readable validation failures use wrapped Rich tables, omitting empty
  columns so long requirements and remediation steps remain legible at narrow
  terminal widths.
- Interactive Rich sessions show a transient spinner while a run pause, resume,
  or cancel request is in flight. JSON and plain output omit transient progress
  so their output remains automation-safe.
- `MN_CLI_OUTPUT=plain` removes terminal decoration and stays stable enough for
  automation. `NO_COLOR` removes color without removing meaning.
- Rich result panels are reserved for lifecycle results; routine mutations use
  a compact status and summary.
- Errors use the same JSON envelope with `ok: false` and sanitized
  `code/message/hint/details`. Internal diagnostics appear only with `--debug`.
- Error messages name the command resource and action when known. Missing
  resources point to the corresponding list command, and a timed-out mutation
  states that its outcome is uncertain so operators check current state before
  retrying. Transport failures are rendered without raw gRPC details, while
  Core remains responsible for semantic status codes.
- Exit codes are `0` success, `1` operational/critical diagnostics, `2`
  usage/validation/not-found/confirmation, `13` authorization, and `130`
  interruption except an intentional watcher detach.
- Interactive monitors must preserve keyboard accessibility and clearly show
  selection without relying on reverse-video backgrounds.
- The workflow monitor header shows workflow, run, and job identity without
  rendering the blueprint description above live progress.
- Reattachment prefers the exact saved run manifest/projection even when it is
  intentionally too sanitized for resubmission. A missing blueprint ID must
  never cause the monitor to select an unrelated first catalog entry.
- Interactive workflow monitors show a fixed-height, timestamped event tail at
  the bottom. The tail automatically advances to the newest workflow events
  without displacing progress or agent details.
- While Core verifies an SDK-staged local-input inventory on a remote owner,
  the workflow monitor renders `Waiting for staged inputs on <node>` from its
  `submission_storage_waiting` event rather than implying source-agent work.
- Interactive run monitors omit LLM token totals and budgets. The CLI preserves
  the underlying resource telemetry for dedicated commands and structured
  consumers.
- Human-readable run submission, detach, terminal-summary, and watch output
  labels the durable Job ID and execution Run ID separately.
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
- Durable-job archive retains shared data. Job-data reset and run delete,
  and permanent job delete require confirmation. Run cleanup must never be
  presented as deleting durable job data. Permanent job deletion also removes
  all historical runs and definition-owned runtime resources, automatically
  cancelling and clearing any attached active runs first. Run delete likewise
  cancels and clears an active run before detaching it. A federated archive
  accepted while its owner is unavailable is reported as `archive_pending`,
  not as a completed archive, and `mn job list` reflects that pending state. A
  confirmed federated delete accepted while its owner is unavailable is reported
  as `delete_pending`; the CLI does not attempt submitter-local cleanup, the
  stale job disappears from normal lists, and owner cleanup replays when that
  runtime reconnects.
- Permanent job and run deletion use the SDK's bounded extended cleanup
  deadline so the configured 10-second general RPC deadline does not interrupt
  owner-node forwarding or resource cleanup.
- `--yes` answers confirmation only, `--force` overrides one documented
  precondition but never supplies consent, and `--dry-run` never mutates.
  Destructive JSON/non-interactive commands require `--yes`.
## Durable Job/Run Contract

`mn job list/create/show/start/archive/reset-data/delete` addresses durable job
definitions. `mn run list/show/watch/logs/result/resources/compare/pause/resume/cancel/delete`
addresses executions and always
accepts `run_id`. A durable `job_id` owns configuration, schedules, and job data;
every intentional batch start gets a distinct run identity, while attempts
retain their run. Only `type: service` jobs have one attached run. Ordinary
second starts fail with `service_run_exists`; `mn job start --force` explicitly
replaces it with a fresh run ID and always confirms interactively or requires
`--yes`. CLI output must label and persist both fields without treating them as
aliases.
The interactive workflow monitor renders a running service step as `live` and
a downstream not-yet-activated step as `waiting`; it does not present either as
completed merely to advance a long-running workflow.

`mn blueprint run` creates a durable job and first run by default, or starts a
new run of the `--job-id` definition. For an explicit existing job, the command
first prepares and atomically installs the current executable bundle while
preserving job data, schedules, and prior run history. `mn job start` and
scheduled dispatch remain source independent and reuse the stored bundle.
For an existing service job, `mn blueprint run --replace-existing-run --job-id
...` performs destructive run replacement after confirmation. This option is
separate from blueprint validation `--force`.
Blueprint launches use the SDK run-store writer for the job/run mapping and
sanitized source-facing monitor manifest; API launches consume the same
contract so both surfaces render the same public workflow steps.
Historical execution-control commands are not registered under `mn job`.
`mn run result` materializes the structured terminal result for completed,
failed, and cancelled runs so worker diagnostics remain available before an
operator deletes the run.

## Runtime-Model Launch Contract

The public `mn model` command surface is exactly `list`, `add`, `show`, `probe`,
`update`, `remove`, and `doctor`. `add` accepts either one catalog/arbitrary DMR
reference or one canonical provider JSON file. DMR placement chooses the best
eligible cluster node unless `--local` or one or more repeatable `--node`
targets are supplied. `--local` and `--node` may be combined to install one
logical model on multiple eligible nodes. All targets are preflighted before
installation. Successful replicas remain registered after a partial execution
failure, and retrying an already recorded target is idempotent. Provider files
are validated in full, including required environment references, before the
SDK registry changes. If the requested DMR artifact is already installed on an
eligible local or cluster node, `add` adopts that artifact and registers it
without reinstalling it.

`probe` with no model argument force-tests every model in the federation-wide
inventory returned by `list`; an explicit model argument retains single-model
operation. A batch continues after individual failures, returns every per-model
result, and uses a failing exit status if any probe fails. Each probe tests
embeddings, image input, strict JSON Schema output, SSE streaming, and thinking
against the model's managed LiteLLM route, then stores the effective matrix in
the SDK model-catalog overlay. `--capabilities` accepts a comma-separated subset
using the SDK's canonical names or aliases and applies it to every selected
model. For a DMR artifact selected on the local node, the command first runs the
same contract directly against Docker Model Runner and fails if LiteLLM changes
any result. For provider or remote-owner routes, the direct path is reported as
not run; the command does not bypass a remote owner's loopback-only DMR
endpoint.

`list` renders registered models and the discovered federation-wide DMR
inventory; `--available` also includes catalog-only choices. A discovered
artifact has state `ready` when it is installed and routed, or `installed`
while routing is pending. Registry ownership is exposed separately and is not
used as a health state. Machine records expose
explicit kind, state, registration, installation, routing, node, catalog, and
verification facts, including one health record per physical installation.
Mutating commands support `--json`. `update` targets all recorded DMR
installations by default and accepts `--local` and repeatable `--node`
selection. `remove` is ID-based, requires confirmation or `--yes`, preserves
blueprint ownership unless `--force`, and deletes a DMR artifact unless
`--keep-artifact` is used. A replicated model requires `--local`, repeatable
`--node`, or explicit `--all-nodes`; an untargeted removal remains valid for a
single installation. Provider removal never deletes its source JSON.

The removed `install`, `proxy`, and `remote` command trees have no compatibility
aliases. Reusable provider parsing, registry persistence, resolution, and
gateway projection remain SDK-owned; the CLI owns input parsing, confirmation,
placement/fan-out orchestration, progress, and rendering.

`mn model add ... --default` records exactly one operator-selected default in
the SDK registry. It may be a DMR registration or a single-model provider file.
The selected route precedes Nemotron and Gemma; the built-ins remain ordered
fallbacks. Selecting another default does not remove the earlier registration,
and removing the selected registration restores built-in selection.

`mn blueprint run` blocks before job submission until every effective
blueprint-declared runtime model is selected, installed or reused, and routed
through the selected node's LiteLLM gateway. This applies equally to logical
defaults and explicit catalog IDs such as `nemotron3:q4_K_M`. RAG and OCR
models that are supplied dynamically by a skill remain first-use SDK requests;
the SDK holds that call while it prepares the requested model. `mn blueprint
validate` remains side-effect free and only checks declaration validity and
hardware/fallback feasibility.

Every node's LiteLLM gateway projects the merged healthy cluster model
inventory. A public logical group contains one deployment per physical
installation, ordered local first and balanced with LiteLLM's `least-busy`
router. Each deployment forwards to a private owner-qualified route on the
selected node's gateway; only that route reaches the owner-local DMR endpoint.
Public-to-public proxy forwarding is rejected to prevent routing loops. Worker
configuration receives only its local LiteLLM endpoint and logical aliases,
never private route names or a remote node's DMR URL. Already-installed and
newly-installed models follow the same routing projection.

Workflows that use node-local runners are pinned to one feasible runtime node
before submission. Accelerator requirements select by available accelerator
headroom; CPU-only HostLocal workflows prefer the submitting node to avoid an
unnecessary cluster boundary.
The hard `node.name` constraint is reapplied after topology lowering so
generated controls cannot split from executors. Runtime health and join
diagnostics expose the coordination-store identity and writable-primary state;
nodes using divergent Redis datasets or a read-only replica are rejected
before membership or launch.

Context-memory preparation uses the local runtime lifecycle when placement
selects the submitting node; only genuinely remote selected nodes use the
native runtime preparation boundary. `mn runtime ensure-context-engine` is the
explicit package-preparation command: it pulls the configured released GAR
image, while `mn blueprint run` only starts an image already prepared by that
command or an installer and never builds Membrane source.
Runtime startup makes the host-native SDK gRPC service responsive before it
starts or recreates Core. Repeated `mn runtime start` calls reuse a responsive
native service so definition-scoped response engines are not discarded or
raced during Core recovery. Recreating Core also restarts the local API so its
gRPC credentials and client identity cannot remain stale.
Prepared HostLocal Python environments retain separate host and Core-visible
paths; submissions use the configured Core cache mount so console-script
entrypoints resolve inside a containerized local runtime.
For a distributed workflow forwarded to a federated owner, HostLocal Python
environments are prepared on that owner even though no single-node placement
marker is added to the workflow.
Background output relays poll the execution run ID, which is also the
run-store identity. Durable job IDs remain definition-scoped.
An explicitly configured non-default gRPC target is not treated as the local
managed Docker Core merely because a standard Core container is also running.
The local Docker runtime constrains automatic service ports to its published
`MN_AUTO_PORT_START`-`MN_AUTO_PORT_END` range and binds that publication to
host loopback. Its container-loopback proxy marker is passed only to services
that explicitly request it from the blueprint.

Detached output relays remain active until the run becomes terminal unless
`MN_RUN_EVENT_RELAY_MAX_SECONDS` explicitly supplies an operator limit. A
blueprint's stream-duration budget does not truncate output materialization.

The blueprint run adapter prepares declared models before it submits a job. A
logical `default` remains blueprint-owned intent; launch uses the
operator-selected registry default, then chooses Nemotron on a healthy 48
GB-or-above accelerator node or Gemma when no compatible Nemotron node exists.
Explicit catalog declarations keep their exact artifact identity. Debug launch
output reports the selected model, node, install/reuse result, and complete
DockerWorker build command/output details.
Skill-owned RAG/OCR model details are absent from launch preparation and appear
in runtime events only when invoked. The first-use SDK path holds the requesting
call while the model is prepared, then reports the actual model, selected node,
install/reuse state, fallback reason, and duration. `mn blueprint run` and
`mn run watch` render additive `runtime_model_install_progress` events without
changing existing lifecycle event names. The interactive monitor renders a
compact `Preparing <model> on <node>…` status below the workflow and agent
progress grid. The underlying events retain phase, timing, source-to-final-tag
mapping, and DMR byte telemetry for logs and structured consumers. Missing byte
progress does not fail the job; only an actual DMR or prepare-RPC error does.

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

The operator-facing topology, registration and departure sequence, grace
periods, admission formula, queue scope, and diagnostics are documented in
[Cross-node model routing through LiteLLM](docs/cross-node-litellm-routing.md).

The orchestration boundary is injectable through `RuntimeModelDependencies`.
Fast tests must provide a catalog, resource report, system summary,
`BlueprintModelOps`, and LiteLLM gateway effects and execute the real planning
and run-handler code. Live Core, Docker, DMR, SSH, and network access are not
permitted in this unit gate.

## Configuration

`mn_cli.config` is a compatibility facade over `mn_sdk.config`. It composes
CLI-only presentation/orchestration keys with the SDK schema and applies
`environment > .env.<profile> > .env > defaults`, including explicit blank
environment overrides. Runtime connection and token-file resolution use the
SDK `RuntimeConfig`; the CLI does not carry a copied resolver. Operators select
model policy with `MN_MODEL_CATALOG_PATH`; semantic defaults and fallback links
live inside that catalog rather than in CLI constants or extra environment
variables. New public keys require schema/config code, `.env.example`, README,
and test updates.

Public workflow reconstruction and activity compaction call the SDK projection
helpers. Terminal ordering may supply observed events, but the CLI does not
maintain a separate workflow-policy implementation.

After a successful interactive `mn runtime start`, the CLI checks whether a
newer release is available and, when one is found, prints an advisory that
names `mn runtime upgrade`. This check never prompts for or installs an
upgrade, and it is not run for other commands. `mn runtime upgrade` is the
explicit, confirmation-protected installation path; the former `mn runtime
update` command returns a migration hint.

Release upgrades resolve a versioned package plan from the newest stable
`mn-deploy/install_support/v*` snapshot, not from component-repository source
branches or package-manager `latest` aliases. The plan pins the Core release
tag, Python package versions, and Web UI version. Python updates use the
configured GAR Python index (with a configurable extra index for dependencies);
the Web UI receives its pinned npm version through the installed Compose
environment. An upgrade is offered only when the release-plan component version
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
