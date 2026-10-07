# MirrorNeuron CLI Specification

Blueprint preflight delegates placement mode and single-node eligibility to the
shared SDK. `runtime.placement.must_run_local: true` hard-pins execution and Job
ownership to the submitting node, rejects remote `--node` and distributed mode,
and overrides the environment's single-node opt-out. `requirements.os` is checked
against that node's hardware platform; `darwin` requires macOS. Local failures
do not authorize selecting another computer.

`mn runtime ensure-context-engine` prepares the authenticated CPU Membrane package.
Blueprints declare Markdown memory with `mn.context` / `text_memory.enabled`.
Preparation uses Markdown storage and one DuckDB index per job; it does not
prepare a GPU compressor. The private `context_auth.token` is reused across
starts and forwarded through runtime settings. `text_memory.enabled=false`
disables blueprint preparation, including when its source descriptor is enabled.
The matching v2 service, SDK and persistent Compose template must be installed
before a live run. Optional detailed logs use `MN_CONTEXT_OBSERVABILITY=true`.

Model listings retain all discovered DMR installations in one model row,
including unregistered artifacts shared by the local node and multiple remote
owners. Deduplicating model rows must not discard their owner endpoints.

Interactive `mn run watch` keeps completed, failed, and cancelled runs open for inspection until `q` or Ctrl+C. Terminal snapshots stop polling; redirected output still finishes automatically. Child tasks appear beneath their parent sub-workflow with descriptive labels, stable instance IDs, round, status and elapsed time; `[` / `]` page tasks and `f` follows the active or failed task.

The CLI waits for shared-storage output materialization after terminal blueprint
runs. The default deadline is 120 seconds and is configurable with
`MN_OUTPUT_COPY_TIMEOUT_SECONDS`. Incomplete copies are reported as errors by
the SDK materializer rather than acknowledged as complete.

Job creation delegates hardware placement and selected-owner routing to the
shared SDK before preparing and submitting native resources. Explicit `--node`
constrains preparation as well as ownership. Starts reuse the stored definition.
`mn blueprint run` executes the SDK-owned input validation used by
`mn blueprint validate` before runtime resource checks, model preparation, local
hooks, or submission. Missing required inputs therefore fail without creating a
runtime job and include the configured `--set` remediation path.
Compiled child-workflow template IDs are valid runtime binding targets even
though they are not fixed parent DAG steps. Unknown bindings remain invalid.

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
job        list create show start archive reset-data delete backup restore
run        list show watch logs result resources compare pause resume cancel delete
run human  list respond ack
model      list add show probe start stop unload update remove doctor
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
use `mn-python-sdk-web-ui` to persist the service handle and its permitted
HTTP and WebSocket companion ports, so the local Web UI server can proxy a
selected remote node without exposing its LAN URL to the browser.

## Behavior Boundary

OpenShell preparation resolves the submitting host's CLI from `~/.local/bin`
or `PATH` before building images or registering sandboxes. A missing executable
raises `MN_FAILED_PRECONDITION` with installation guidance; it is not a missing
blueprint. The host CLI must match the deployed gateway version.
Local gateway detection follows the same explicit-endpoint, explicit-gateway,
managed-runtime, and named-gateway precedence as sandbox preparation. Managed
loopback endpoints do not require named gateway metadata. Their image builds
use the Docker CLI and its configured context.
Shared OpenShell sandboxes are registered against the durable job ID and
definition submission ID, never the blueprint run label. Sandbox identities are
unique per definition revision. Successful submission commits that preparation
record; reconciliation preserves resources referenced by the durable definition.

OpenShell dependency contexts use the SDK's local-source staging contract,
including preserved setuptools-scm version metadata and dependency extras.

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

Blocking blueprint-launch and job-creation stages expose descriptive activity
messages and elapsed time. Interactive terminals use a transient spinner;
plain/non-interactive human output uses ten-second stderr heartbeats without
terminal control sequences. JSON emits no added progress. Activity reporting
ends on return, error, or interruption and never retries or cancels runtime work.
Only measured upstream progress may be presented as a percentage.

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
- A public API progress snapshot is authoritative for live `run watch` step
  state. Local event replay is only used to reconstruct older runtime-node
  snapshots or when the API stream is unavailable.
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
- When terminal materialization copies both the internal run store and a
  configured user output folder, human-readable output reports only the final
  user-facing destination. Both copies remain available.
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
retain their run.
Run list/show may display a mapped replicated run record when Core no longer
has the run. Nonterminal stored records are `unknown`, never asserted live.
Run listing checks Core for owners named by local run records, including
archived definitions and runs beyond the first page, before using `unknown`.
`mn job create` runs host-side command input validators and records their result
before storing the definition, so later source-independent starts never pass an
unvalidated command rule to Core.
Only `type: service` jobs have one attached run. Ordinary
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
`start`, `stop`, `unload`, `update`, `remove`, and `doctor`. `add` accepts either one catalog/arbitrary DMR
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

Catalog `source: "docker"` selects the SDK's NVIDIA Docker delivery; explicit
`source: "dmr"` and omitted source use Docker Model Runner. Both sources use
the managed placement, registration, owner-gateway, and run residency contracts.
Docker recipes and credential resolution remain SDK-owned; NGC environment
references resolve on the native owner rather than the submitter.
`add`, `update`, and preparation cache/configure models without starting
inference. Adopting an installed local Docker model does not start it.
Owner inference admission starts stopped Docker containers or recreates missing
containers from owned cache/images; DMR loads on inference. The last run/request
reference releases model memory. Docker models use restart policy `no`.
Diagnostics treat confirmed cached/stopped Docker installations as `idle` and
usable for on-demand inference, without starting them; uncertain state remains
unavailable. Local direct capability probes use temporary request ownership
through readiness and cleanup.
`start`, `stop`, and `unload` accept a model, `--node`, `--local`, and `--json`.
They infer a single registered owner, require selection for replicas, dispatch
to the owner native service, and never install missing artifacts. Docker start
waits for readiness; stop/unload stop the container while retaining image/cache.
Provider registrations have no managed container lifecycle. Docker diagnostics
use NIM readiness and container state, not DMR or node health as a substitute.
Local direct capability probes use the configured container API port/model ID.

`show` without an argument returns every entry in the merged catalog and marks
the configured default/fallback chain with `default`. Its JSON data is
`{"models": [...]}`. An explicit model retains the single-detail shape. Both
forms are static and perform no live Docker, gateway, or hardware probes.
Bare `add --default` prepares the first feasible configured default, respecting
`--local`, repeated `--node`, backend, context, and force options. It reuses an
existing registration and never records a fallback as an operator override.
An explicit model or provider file with `--default` retains default-selection
semantics.

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


## Canonical blueprint packages

All blueprint folders and ZIPs use the blueprint/v1 manifest schema and role
documents owned by `mn_sdk.blueprints`. Catalog indexes contain ordered package
paths only. Catalog reads are data-only; explicit compilation produces Core's
runtime manifest. Default configuration, local overwrites, and invocation
values resolve through the SDK. Payload assembly stages one resolved descriptor;
launch environment is passed explicitly. Confirmed failure permits owned
resource rollback; uncertain submission acknowledgements require reconciliation.
See `mn-docs/blueprint-standard.md` for document ownership and extension schemas.

## Explicit skill dependency versions

Skill dependencies must declare a version in the manifest or runtime package
index, or carry an explicit version constraint in a configured requirement.
An unversioned skill fails preparation before installation; no global skill
version or SDK-version fallback is applied. References to skills already
versioned in the manifest use that declaration. Development staging preserves
the SDK's own static project version or, for the running SDK source checkout,
its installed distribution version. Missing SDK version metadata is an error.

Blueprint SDK capabilities use the canonical `dependencies.json.packages` list,
with full distribution names, `type: pip`, `source: gar`, and exact versions.
The SDK owns resolution: local development uses source projects and ignores
package/skill release pins; binary mode retains GAR requirements. This applies
to HostLocal and DockerWorker submissions, including blueprint-owned skills.

The workflow monitor event feed prefers explicit activity messages over worker
identifiers, including bounded tool-query previews and outcomes. Long entries
are ellipsized; recorded events retain the bounded message.

The installed API, native SDK, and Web UI executables are resolved under
`$MN_HOME/venv/bin` (default `~/.mn/venv/bin`), alongside runtime state.

## Automatic federation storage recovery

The supervised native runtime checks shared-storage pairing every 30 seconds,
independently of model reconciliation. Incomplete pairing is retried every five
seconds, including when an already-registered peer starts after the local runtime.
Each pass reloads persisted sidecar settings and reads current authenticated
Syncthing device identities before restoring reciprocal device and folder
registration. Unchanged configurations are not rewritten. Disabled storage and
unavailable peers never trigger resets, data deletion, or workflow restarts.
The monitor uses existing federation authorization and never joins unknown nodes.
Credential changes that invalidate existing federation access require node rejoin.

Install the updated CLI and SDK and restart the native runtime service to enable
the monitor. No runtime reset or blueprint modification is required. Registration
is distinct from completed file transfer; DockerWorker preparation still verifies
the staged context and its readiness marker before building.
# Desktop identity and reconnect contract

Independent desktop federation stores a version-1 node identity under MN_HOME,
using atomic private writes and an OS file lock. Startup adopts only consistent
saved/configured/container identity evidence; it does not derive identity from
the current advertised address. Runtime status validates the actual Core node
against the saved identity and fails readiness for unnamed or mismatched nodes.
`mn runtime reconnect` writes an atomic version-1 runtime-network.json containing
node_name, host, and grpc_port for Core to consume without restarting. It never
changes ownership, direct Erlang membership, or federation credentials.

The workflow monitor separates fixed phases from runtime-created sub-workflow steps.
The child panel shows task IDs, parent, round, phase, status, elapsed time and failure
reason. It follows active work, with `[` / `]` paging and `f` to resume following;
counts include newly discovered tasks. Four child rows and a three-event tail keep
the child view compact. On macOS, attached runs and detached output
relays hold an idle-sleep assertion for their lifetime. Display sleep remains allowed;
explicit sleep is not prevented. An explicit relay time limit also ends its assertion.

## Shared admission error contract

CLI and API use `mn_sdk.error_catalog.ERROR_CATALOG` for admission error codes,
category, safe message, remediation hint, HTTP status and retryability. Existing
codes remain stable. Unknown errors retain `MN_EXECUTION_FAILED`; raw exception
text never becomes an admission explanation. Applications must branch on codes,
not message text. `AppError.category` and `AppError.retryable` are additive.

| Problem code | Symbolic code | Category | HTTP | Retryable |
| --- | --- | --- | --- | --- |
| 1001 | `MN_MEMORY_REQUIREMENT_UNMET` | hardware | 422 | false |
| 1002 | `MN_CPU_REQUIREMENT_UNMET` | hardware | 422 | false |
| 1003 | `MN_GPU_REQUIREMENT_UNMET` | hardware | 422 | false |
| 2001 | `MN_GPU_MEMORY_UNAVAILABLE` | capacity | 503 | true |
| 2002 | `MN_DISK_UNAVAILABLE` | capacity | 503 | true |
| 2003 | `MN_RESOURCE_EXHAUSTED` | capacity | 503 | true |
| 3001 | `MN_SCHEDULING_UNAVAILABLE` | scheduling | 503 | true |
| 4001 | `MN_PLACEMENT_UNSATISFIED` | placement | 422 | false |

Hardware errors require a configuration or hardware change. Capacity and
scheduling errors may succeed after availability changes; retryable is not a
promise of success or authorization to replay a submission automatically.
A mixed placement failure reports all observed blockers rather than claiming
that every node lacks memory. Unknown/mixed causes require inspection.

Placement errors include bounded `details.blockers` with code, safe message,
one-based node index, and (when measured) required/available amounts, `unit`,
`resource`, and `operator` (`>=` or `>`). A validated `node_label` may identify
an explicitly reported friendly PC name; raw runtime node IDs and addresses are
not labels. Node indices refer to the sorted placement snapshot, not persistent
node IDs. Preparation host-memory requirements use total capacity. Core run
admission reports memory available for runs after limits and reservations.
GPU device memory uses free capacity, preserving a reported zero. Values are
shown in GiB (1024 MiB), matching existing placement conversion. Missing
measurements are not invented. Public summaries contain no paths, secrets,
raw messages, or arbitrary diagnostics; they show up to eight blockers, while
structured details contain up to 100. Truncated reports retain the general
placement code rather than claiming a cluster-wide resource shortage.

Core appends a bounded `mn_admission_v1` JSON marker to `placement_failed:` gRPC
details. The SDK validates the versioned blocker fields, owns the numeric error
catalog and messages, and emits the same `AppError` through CLI and REST. For
example: `spark has 8.17 GiB of free GPU memory; this work requires 48 GiB.`
Code `MN_GPU_MEMORY_UNAVAILABLE` / `2001` suggests stopping other GPU workloads
or unloading unused models before retrying. Busy CPU/GPU reservations use
`MN_RESOURCE_EXHAUSTED` / `2003`, while insufficient CPU/GPU hardware retains
`1002` / `1003`. Requirements and scheduling decisions remain unchanged.
Malformed, unknown, or legacy generic device errors do not imply a memory cause.
Legacy `RuntimeError` catches still work for preparation placement failures;
normalization retains structured identity through launch exception wrappers.

Core's legacy overload and no-schedulable-node markers remain normalized at
run-start boundaries. Other operations retain their existing transport error
interpretation. No protobuf shape or numeric code changes are required. Deploy
the updated Core and SDK together to enable measured run-admission errors;
older Core versions retain their existing, less specific error behavior.

### Numeric problem codes for automation

`problem_code` is a stable integer shared across SDK errors, CLI JSON, API
Problem Details and individual placement blockers. It is independent of HTTP
status and process exit code. Human CLI output also prints the numeric code.
Existing symbolic `code` remains backward compatible. SDK consumers use
`ProblemCode` (IntEnum), `PROBLEM_CODES` (numeric-to-symbolic lookup), and
`problem_code(symbol)`; `ERROR_CATALOG` holds admission defaults.
Assigned values must never be renumbered or reused. Message wording can evolve
without changing a problem code. Clients must handle unrecognized values as
unknown errors and must not automatically retry them. An unregistered SDK
symbol maps to 9000; an unexpected execution failure maps to 9001.

Ranges reserve related problem families: 1xxx hardware, 2xxx capacity,
3xxx scheduling, 4xxx placement, 5xxx input/configuration,
6xxx access/resource state, 7xxx runtime/transport, 8xxx cancellation,
9xxx unknown/internal failures. Exact codes, rather than ranges or message
matching, drive automated remediation. `category` provides a finer label.

Example API problem (HTTP 422):

```json
{
  "problem_code": 1001,
  "code": "MN_MEMORY_REQUIREMENT_UNMET",
  "category": "hardware",
  "status": 422,
  "detail": "Host memory: requires 48 GiB; available 24 GiB.",
  "hint": "Select a node with enough memory or reduce the workflow's memory requirement.",
  "retryable": false
}
```

```python
from mn_sdk import ProblemCode

if response["problem_code"] == ProblemCode.MEMORY_REQUIREMENT_UNMET:
    # Choose a larger runtime or change requirements before submitting again.
    pass
```

## Job performance

Run `mn job analysis <job_id>` for all recorded execution statistics, or add
`--json` for the shared SDK/API result in the standard CLI JSON `data` field. Counts distinguish successful, failed,
cancelled, running, paused, and other unfinished runs. Duration excludes pauses;
missing/partial measurements and estimated tokens are explicit. Plain mode and
`NO_COLOR` remain supported. This read-only command does not start the job.

## Failed-run recovery operation

`mn run retry <run-id>` plans and submits a manual checkpoint retry.
`--dry-run`, repeatable `--set path=value` and standard JSON output are supported.
Planning returns preserved/retried steps, adjustable fields and attempt/checkpoint
selection. Submission uses a request idempotency key. Explicit original
`--expected-attempt` and `--checkpoint-revision` with `--idempotency-key` support
resubmission after a lost response. Core remains the recovery authority.

Resume is reserved for paused work and points failed runs to retry. List/show
identify `record_source: runtime|history`; stored-only recovery is unverified or
blocked with a practical reason. A missing stored control record and an unavailable
runtime are distinct from an entirely unknown run ID.


Managed Markdown context turns may bind a trusted serving-tokenizer integration
with `MN_CONTEXT_TOKEN_COUNTER_FACTORY=package.module:create_counter`. The factory
receives `request`, `scope` and `principal` keyword arguments and returns a
Membrane `VerifiedCounter` calibrated against actual provider prompt usage for
that request's serving route, including tools and schema framing. Workers receive
the setting through native/runtime preparation. No factory means counting remains
unavailable; errors or lexical/byte estimates never authorize dispatch. Install
the serving integration in the worker environment before enabling live managed
turns. Tokenization and context processing must remain on CPU.

Context preparation needs no dedicated compression model. Full-runtime optional
compaction uses the normal LiteLLM `default` route only after CPU preparation
cannot fit a complete request.

## Job backup and restore

```bash
# Pause active runs first; offline dependencies are included by default.
mn job backup <job-id> --output /path/to/job-backup.zip
# On the destination, create a new definition and start a fresh run.
mn job restore --input /path/to/job-backup.zip --start
# To restore without starting, omit --start; --job-id selects a new identity.
mn job restore --input /path/to/job-backup.zip --job-id restored-job
```

These commands use `mn.backup.v3`. The ZIP contains the executable bundle,
configuration, job data, available run history/events/artifacts, staged inputs and
outputs, payload model files, transitive Python wheels, and declared Docker images.
`--no-air-gapped` omits the offline wheel/image capsule. Missing model assets,
unavailable images, unsupported remote service dependencies, or active runs fail
backup instead of producing an incomplete offline capsule. Backup never replaces
an existing destination file.

Restore checks all ZIP paths and hashes, compatible OS/architecture/Python ABI,
and destination CPU, RAM, disk, GPU and runner requirements before allocating
resources. It creates independent storage and native resources without catalog
access or blueprint hiring. Historical executions remain evidence under the new
job data directory; schedules are recreated paused with new identities. Restore
never replays source executions. A failed start keeps the new job ready for retry.

HostLocal wheels are built for the actual execution Python, including Docker
Core's Linux Python, and checked against the destination before installation.
Captured dependencies retain the versions installed in the source environments.
The native preparation service creates fresh environments from the complete
wheel set with package indexes and dependency URL resolution disabled.

The destination must already have compatible MirrorNeuron, Python and Docker /
Docker Model Runner installations. The capsule supplies job dependencies, rather
than operating-system or runtime installers. Keep it private: configuration and
local data may contain sensitive values. Core, SDK, CLI and API must be upgraded
together for the new streamed backup RPCs. `mn blueprint export <run-id>` remains
a run report export (JSON/Markdown/HTML), with no job restore counterpart.
