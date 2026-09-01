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
mn runtime start
mn node list
mn blueprint run message_routing_trace
```

`mn runtime start` starts a normal, federation-capable Core. There is no
separate worker mode: every Core owns its Redis state, can own jobs, and runs
all agents for those jobs locally. The successful start output includes the
advertised host, gRPC port, node identity, a join token, and the exact
`mn node add` command to run from another Core. Treat the displayed token as a
credential, keep terminal output private, and use `mn node refresh-token` when
it must be rotated.

The command omits `--grpc-port` when the advertised endpoint uses the default
port (`55051`); it includes the option when a non-default port is required.

With the default Syncthing shared storage enabled, `mn node add` configures and
verifies both sidecars before registering the Core federation pair. A failed
shared-storage connection creates no new Core federation pair and tells the
operator to correct the sidecars before retrying.

Runtime startup readies the host-native SDK service before Core. Calling
`mn runtime start` again reuses a healthy native service, preserving warm
definition-scoped response engines while the remaining runtime is reconciled.
If reconciliation recreates Core, the CLI also restarts the local API so its
gRPC client identity matches the recreated runtime.

`mn node list` shows each node's hostname, health, and role (connection mode
plus job-ownership eligibility) without artifact-only fields such as kind,
owner, or update time. Use `mn node show <node>` for the complete endpoint and
capability record.

Remove a reciprocal federation registration with explicit confirmation:

```bash
mn node remove mirror_neuron@spark --yes
```

This detaches the peer from the current Core only; it does not delete the
peer's owner-local jobs or data.

Static HTML exports use the optional `mirrorneuron-web-ui-skill` package. With
the MirrorNeuron GAR package index configured, install it with:

```bash
.venv/bin/python -m pip install "mirrorneuron-cli[web-ui]"
```

## Model operations

`mn model` exposes one type-aware workflow: `list`, `add`, `show`, `probe`,
`update`, `remove`, and `doctor`. Add a catalog or arbitrary DMR reference with
`mn model add <MODEL>`, or register canonical provider JSON with
`mn model add --file <definition.json>`. Registrations are stored in
`$MN_HOME/models/registry.json`; provider secrets remain environment-variable
references. Use `mn model list --available` to include catalog-only choices.
Add `--default` to make one newly added DMR or provider model the logical
default ahead of the built-in Nemotron/Gemma fallback chain. Provider files
used with `--default` must contain exactly one model.
When the requested DMR artifact is already installed locally or on a cluster
node, `mn model add` reuses it and creates the same managed registry record
without pulling a second copy.

A DMR model may be installed on more than one eligible node. Repeat `--node`
and combine it with `--local` to add all requested replicas in one operation:

```bash
mn model add small --local --node spark
mn model add medium --node spark --node gpu-2
```

The CLI preflights every target before installation, records successful
replicas if a later target fails, and makes retries idempotent. `mn model list
--json` and `mn model doctor` report each installation separately. An
untargeted `model update` updates all recorded replicas. Removing a replicated
model requires `--local`, one or more `--node` values, or `--all-nodes`.

Force a live capability evaluation and save the effective LiteLLM-facing
matrix in the SDK model-catalog overlay:

```bash
mn model probe gemma4:e2b
mn model probe nemotron-3.5-lightning:latest --json
mn model probe gemma4:e2b --capabilities image,json-schema,stream,thinking
```

The default probe covers embeddings, image input, strict JSON Schema output,
SSE streaming, and thinking. When the selected DMR artifact is local, the CLI
runs the identical probe directly against Docker Model Runner and fails if the
LiteLLM result differs. Remote-owner and provider models are tested through the
managed LiteLLM route without exposing or bypassing the owner's direct endpoint.

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

The run monitor header keeps the workflow, run, and job identity but omits the
blueprint description so progress begins immediately below it.
Its lower section includes a fixed-height, timestamped event tail that follows
the newest workflow events without growing the monitor.
Reattachment uses the manifest projection saved for that exact run; it never
guesses an unrelated catalog blueprint when older run metadata lacks a
`blueprint_id`.
When a remote owner is verifying SDK-staged local inputs, the same monitor
shows `Waiting for staged inputs on <node>` until Core dispatches the workflow.
The interactive monitor intentionally omits LLM token totals and budgets because
runtime event counters are not authoritative; resource telemetry remains
available to its dedicated commands and structured consumers.

For a deferred first-use DMR pull, the run monitor keeps model preparation to a
single compact `Preparing <model> on <node>…` status below the workflow and agent
progress grid. Model events still carry detailed phase, timing, and byte telemetry
for logs and structured consumers; a lack of byte progress does not itself fail the
job.

Live Spark checks are a separate, opt-in boundary smoke after this injected
gate passes; they are not the development loop for placement policy.

CPU-only HostLocal workflows stay on the submitting runtime node by default.
When the local Core runs in Docker, prepared HostLocal Python environments are
reported to submissions through the Core-visible cache mount rather than the
host filesystem path.
Automatic HostLocal service ports use `MN_AUTO_PORT_START` through
`MN_AUTO_PORT_END` (62000-62049 by default in the local Docker runtime). That
range is published only on host loopback; the runtime's internal proxy marker
allows the service process to accept Docker forwarding without advertising a
non-loopback endpoint.
Detached runs keep their output relay alive until terminal state unless
`MN_RUN_EVENT_RELAY_MAX_SECONDS` is explicitly set.

Override blueprint config for one run without changing `config/overwrite.json`:

```bash
mn blueprint run ./vc_assistant \
  --set document_sources.folder_path=/path/to/documents \
  --set execution.debug=true
```

Repeat `--set` for multiple values. Values use JSON types when possible and
otherwise remain strings.

Normal `mn blueprint run` commands validate declared required inputs from the
merged configuration before submitting a job. For
`input_validation.required: ["input_folder"]`, supply
`--set inputs.payload.input_folder=/path/to/source` or configure a non-empty
default. `--force` intentionally bypasses input validation.

For a blueprint-owned web service, override the listener without editing its
checked-in config:

```bash
mn blueprint run ./cctv_operator --web-ui \
  --web-ui-host 0.0.0.0 \
  --web-ui-port 61017
```

`--web-ui-host` and `--web-ui-port` set `web_ui.service.host` and
`web_ui.service.port` for that run. A wildcard host exposes the service to
reachable peers; the blueprint is responsible for its authentication and
network-safety contract.

When a blueprint declares a `web_ui` service or a deferred job-scoped Web UI,
`--web-ui` reports the local `/jobs/<job_id>/ui` dashboard route. Deferred
handles may appear after job submission, but the reported route is stable.
Docker Compose service handles include an allowlist for the dashboard's
declared video and WebSocket companions, so the local Web UI server can proxy
a selected remote node without sending the browser directly to that node's LAN
IP.

## Stable jobs and execution runs

Create a reusable job once, then start independent runs that share its declared
job data:

```bash
mn job create ./vc_assistant --job-id vc-diligence
mn job show vc-diligence
mn job start vc-diligence --inputs run-input.json
mn run list --job vc-diligence

mn run show <run-id>
mn run pause <run-id>
mn run resume <run-id>
mn run cancel <run-id>
```

In an interactive terminal, run pause, resume, and cancel display a spinner
while Core processes the request. JSON and `MN_CLI_OUTPUT=plain` output remain
free of transient progress so they are safe for automation.

`job_id` is the durable configuration and data owner. `run_id` is one
execution and the identity used for control, logs, output, retention, and run
deletion. Starting the same job again creates another run; retrying a run does
not. Use `mn blueprint run --job-id <job-id>` to run an existing definition.
Without that option, blueprint run creates a durable job and starts
its first run. Human-readable submission, detach, summary, and watch output
labels the durable Job ID and execution Run ID separately.

`mn job list` shows each definition's canonical Type (`service` or `batch`) and
its Node. Node is the Core runtime (`owner_node`, such as
`mirror_neuron@spark`) that durably owns the definition and its job data; it is
not a human or account owner.

With `--job-id`, the CLI prepares the currently installed blueprint revision
and atomically replaces the inactive job's executable bundle before starting
the run. Job data, schedules, and earlier run history are preserved. In
contrast, `mn job start` and scheduled dispatches are source independent and
reuse the stored definition-scoped submission and Docker services.

Lifecycle commands are deliberately separate:

```bash
mn job archive vc-diligence            # retains shared data
mn job reset-data vc-diligence         # confirms; clears/reseeds and advances generation
mn run delete <run-id>                  # confirms; cancels an active run, then removes it; never deletes shared data
mn job delete vc-diligence              # confirms; deletes all runs, runtime resources, definition, and data
```

Permanent job deletion automatically cancels and clears attached active runs
before removing the definition and shared data. Run deletion does the same for
an active run before detaching it. If an archive must wait for an
unavailable owner runtime, the command and `mn job list` report
`archive_pending` until federation replay settles it.
If a confirmed deletion must wait for an unavailable owner, `mn job delete`
reports `delete_pending` and skips submitter-local cleanup; the stale job is
hidden from normal lists while Core replays the owner cleanup on reconnect.

Job and run deletion allow up to five minutes for Core cleanup (with a small
client-side forwarding margin), including when the definition belongs to a
federated owner node. If any mutating command still times out, the CLI warns
that the owner may still be processing it; inspect the current job or run state
before retrying. Missing identifiers are reported directly, with the relevant
list command instead of a generic execution failure.

Execution status and controls use `mn run ...`; attached blueprint progress
uses the canonical workflow-progress stream and the same public-step contract as the
launch-time monitor.

## Durable operations

`mn node reconcile` and `mn node drain` start durable Core operations and
render item updates in completion order. `MN_CLI_OUTPUT=plain` emits stable
`→`, `✓`, and `! Warning:` progress lines; the rich terminal shows live
counters and recent results.

If the owner of a cancelled job is offline, `cancellation_pending` means the
request was accepted and cleanup is queued for that node's rejoin. It is not a
command failure. Ctrl+C detaches without aborting the operation; reattach with:

```bash
mn operation show op-…
mn operation watch op-…
```

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

`mn runtime start --host <LAN-IP>` persists the local runtime identity in
`$MN_HOME/docker-compose.env`. Blueprint launches automatically use that
identity for Compose placement, so `MN_NETWORK_ADVERTISE_HOST` does not need to
be exported for ordinary local submissions. An explicitly exported value still
overrides the persisted identity.

Keep secrets, credentials, production hostnames, production database URLs, cloud
credentials, and user-specific local paths out of source files. Use
environment variables or uncommitted `.env` files instead.

## Details

- [MirrorNeuron Component Guide](../mn-docs/component-guide.md#cli)
- [CLI Reference](../mn-docs/cli.md)
- [Environment Variables](../mn-docs/env_variables.md)
- [Monitor Guide](../mn-docs/monitor.md)

## Release Updates

After a successful interactive `mn runtime start`, the CLI checks the newest
stable `install_support/v*` snapshot in `MirrorNeuronLab/mn-deploy`. If a newer
release is available, it only prints a reminder to run `mn runtime upgrade`; it
never prompts for or installs an upgrade during startup or any other command.

`mn runtime upgrade` is the explicit, confirmation-protected installation
command. Its release plan pins the Core release tag, the SDK/CLI/API Python
package versions, and the Web UI npm version. The upgrader installs the exact
Python package versions from the public GAR `agent-skills` index and configures
the exact Web UI npm version for Docker Compose; it does not follow a source
branch, package-manager `latest` tag, or the Core repository's latest-release
endpoint. A component is shown as an upgrade only when the snapshot version is
strictly newer than the installed stable version, so a stale snapshot cannot
offer a downgrade. The former `mn runtime update` command now points to
`mn runtime upgrade`.

The Core remains a versioned GitHub Release binary because it is not a Python
or npm package. Its release asset URL is constructed from the same support
snapshot tag. For private mirrors, set `MN_DEPLOY_REPO`, `MN_DEPLOY_REF`,
`MN_PIP_INDEX_URL`, or `MN_PIP_EXTRA_INDEX_URL` before running the command.

## Notes

- A running MirrorNeuron core is required for live runtime commands.
- Validation failures render in wrapped terminal tables, so long requirements
  and recommended fixes remain readable in narrow terminals.
- The default gRPC target comes from `MN_GRPC_TARGET`, then local deployment
  settings, then `localhost:55051`.
- Use `mn blueprint validate` before `mn blueprint run ./folder` when checking a local bundle.
- Validation honors first-use runtime-model preparation, so a compatible
  declared model need not already be installed.
- `mn blueprint run` validates model declarations but does not install models.
  Workers select, install, and route each managed model on its first actual use.
- Docker workers receive a worker-reachable model-control target and use the
  SDK to select the best cluster node independently for LLM and for model
  specifications supplied at runtime by RAG and OCR skills.
- Node-local workflows are hard-pinned as a whole after topology lowering.
  Runtime health rejects nodes whose coordination-store identity differs from
  the submitting Core or whose Redis endpoint is read-only.
- OpenShell workers that reuse a job-scoped sandbox are prepared before
  submission; the submitted node receives the concrete sandbox name and SSH
  host instead of asking Core to create host resources.
- `default` is a LiteLLM model group, not a concrete model. Its preferred and
  fallback entries come from the SDK catalog's `defaults.llm.model` and
  per-entry `fallback_model` links. The existing cluster model monitor
  rebuilds these routes as nodes join, rejoin, or leave; incomplete peer
  snapshots retain the last safe routes until departure is confirmed.
  Gateway route names and `fallback_model` are read from the SDK's merged model
  catalog, including `~/.mn/models/catalog.json` (or `$MN_HOME`) and the
  highest-priority `MN_MODEL_CATALOG_PATH` override. When a local runtime's DHCP
  address changes, the monitor rehomes only DMR registrations whose artifact is
  confirmed on the local host and whose former owner is absent from live
  membership. It then rebuilds gateway routes from the current live node
  address; this avoids treating an unverified `.local` name as a cluster
  endpoint.
- `--debug` retains complete Docker build diagnostics and prints deferred model
  policies. Actual model/node selection appears later in runtime events.

Shared configuration parsing and defaults are owned by `mn_sdk.config`.
`mn_cli.config` remains a source-compatible facade that composes CLI-only keys
with the SDK schema. Layering is `environment > .env.<profile> > .env >
defaults`, and an explicitly blank environment value overrides dotenv. Set
`MN_MODEL_CATALOG_PATH` in `.env` to select an operator catalog containing both
semantic defaults and model entries; there are no separate preferred/fallback
model-name environment variables.
