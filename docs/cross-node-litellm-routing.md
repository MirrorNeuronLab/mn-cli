# Cross-node model routing through LiteLLM

MirrorNeuron runs one LiteLLM Proxy on every runtime node. Workers always call
the proxy on their execution node and request a public logical model name such
as `default`, `small`, or `medium`. They do not select a machine, use a remote
Docker Model Runner URL, or receive MirrorNeuron's private owner-route names.

This document describes the generated routing topology, how model deployments
enter and leave it, and how concurrent work is balanced across replicas.

## Routing topology

For a public request, the ingress proxy chooses one physical deployment from
the logical model group:

```text
Worker
  | model="medium"
  v
LiteLLM Proxy on the worker's node
  |-- local deployment --> local private owner route --> local DMR
  `-- remote deployment -> remote LiteLLM Proxy
                            `-> private owner route --> remote DMR
```

Every physical installation contributes one deployment to each of its public
logical aliases. If `medium` is installed on both `mini` and `spark`, both
proxies contain a two-deployment `medium` group. Each proxy orders its own
installation first, so the same generated cluster inventory produces a local
preference on every node.

Private routes use an owner-qualified name under `__mn_owner__/`. They are an
internal proxy-to-proxy transport contract. MirrorNeuron filters them from
worker configuration, `mn model list`, and managed model diagnostics.
Forwarding from one public group to another node's public group is rejected;
remote deployments may target only private owner routes. This prevents a
request from bouncing between proxies.

## Sources of truth

The routing table is generated from four kinds of state:

- live Core cluster membership and each node's advertised addresses;
- revisioned runtime model inventories published by the nodes;
- the SDK model registry and model catalog, including logical aliases and the
  `default` fallback chain; and
- manually configured external/provider routes, which retain their existing
  precedence and lifecycle.

Cluster-managed remote records are tagged `managed_by: mirror-neuron-cluster`.
Only the cluster model monitor replaces or removes those records. Reconciliation
does not delete user-managed external routes.

The generated gateway config is stored under
`$MN_HOME/models/litellm-gateway/config.yaml`. It is generated state even
though the serialized document is also valid JSON. Do not edit it by hand.
Change the model registry, catalog, or external provider definition and let
MirrorNeuron regenerate it.

## Registration when a node joins

Registration is inventory-driven; there is no separate LiteLLM administration
API to call.

1. The joining node inventories the models physically present in its Docker
   Model Runner.
2. It publishes a model-status snapshot to Core with a content revision.
3. Each node's native SDK process runs the cluster model monitor. The monitor
   reads live membership and the revisioned inventory for every live node.
4. It creates one deployment identity from the owner node, runtime model, and
   owner proxy endpoint. Only identical physical deployments are deduplicated.
5. Each node rebuilds its local proxy config. It excludes a remote hop to
   itself because its direct local owner route is authoritative.
6. The config is written atomically. If it changed, or the proxy is unhealthy
   or missing expected public models, MirrorNeuron recreates only the LiteLLM
   service and verifies its model inventory. A failed reload restores the
   previous config before retrying.

`mn node add` performs an immediate reconciliation after a successful join.
The background monitor then keeps the routes current. Model add and update
operations also trigger reconciliation, so a newly installed replica normally
appears without waiting for the next monitor interval.

The monitor defaults are:

```text
MN_CLUSTER_MODEL_MONITOR_ENABLED=true
MN_CLUSTER_MODEL_MONITOR_INTERVAL_SECONDS=15
MN_CLUSTER_MODEL_MONITOR_RETRY_MIN_SECONDS=1
MN_CLUSTER_MODEL_MONITOR_RETRY_MAX_SECONDS=30
MN_CLUSTER_MODEL_MONITOR_NODE_MISSING_GRACE_SECONDS=90
```

Its durable last-seen and retry state is stored at
`$MN_HOME/models/cluster-model-monitor.json` unless
`MN_CLUSTER_MODEL_MONITOR_STATE_PATH` overrides it.

## Deregistration and node departure

### Explicit model removal

Model removal changes the installation registry first and then reconciles the
cluster routes. For a replicated model, the target must be explicit:

```bash
# Remove only the Spark registration and artifact.
mn model remove small --node spark

# Deregister Spark but retain its physical DMR artifact.
mn model remove small --node spark --keep-artifact

# Remove every registered installation.
mn model remove small --all-nodes
```

`--local` targets the local installation. An untargeted removal remains valid
only when exactly one installation is recorded. Removing one replica rebuilds
the logical group around the remaining replicas; removing the last replica
removes that cluster-managed public group.

### Disconnected node

A transient disconnect must not immediately erase a usable deployment. Two
separate grace periods protect the cluster:

1. Core attempts reconnection and keeps the node in a disconnected state for
   `MN_NODE_DISCONNECT_GRACE_MS`, which defaults to 30 seconds.
2. The model monitor remembers a recently observed node for
   `MN_CLUSTER_MODEL_MONITOR_NODE_MISSING_GRACE_SECONDS`, which defaults to 90
   seconds.

While a peer is expected but its membership or inventory snapshot is missing,
reconciliation is incomplete and runs in non-replacing mode. Existing
cluster-managed remote records remain in place. After the node is absent past
the model-monitor grace period, the remaining membership becomes authoritative:
the monitor replaces the cluster-managed remote set, removes deployments owned
by the departed node, regenerates the proxy config, and recalculates `default`.

If the node rejoins during the grace period, it republishes its inventory and
the existing deployment is refreshed instead of removed and recreated.

During a stale-route window, LiteLLM can retry another replica when one exists.
If the disconnected node owned the only deployment, requests can fail until it
returns or the model is installed elsewhere; retaining metadata during the
grace period does not make an unreachable decoder healthy.

## Logical groups and `default`

Every physical installation of the same logical model is emitted as a separate
LiteLLM deployment with the same `model_name`:

```text
model_name: medium
  deployment: mini owner route   order=1 on mini, order=2 elsewhere
  deployment: spark owner route  order=1 on spark, order=2 elsewhere
```

`default` is a LiteLLM group alias, not a physical model. Its preferred group
and fallback chain come from the merged SDK catalog. With the standard catalog:

- `default` selects `medium` when a medium deployment is available;
- the `medium` group contains every healthy medium replica; and
- `small` is the catalog fallback when no medium route exists or a request
  exhausts the configured medium deployments.

An operator-selected `mn model add ... --default` is authoritative. If that
route fails, MirrorNeuron does not silently substitute a built-in model and
pretend the explicit selection succeeded.

## Load balancing and decoder protection

The generated LiteLLM router uses `least-busy`. Cluster deployments also carry
a deterministic preference order:

- local deployment: `order=1`;
- remote deployments: `order=2`, sorted deterministically by node and
  endpoint.

When deployments are equally idle, sequential requests stay local. When the
local deployment is already decoding, overlapping work spills to an idle
remote replica.

Each private owner route sets `max_parallel_requests: 1`. The queue callback on
the owner proxy is the physical decoder lock. If another ingress proxy reaches
a busy owner, that private route returns a retryable `owner_busy` response
immediately; it does not queue privately. The ingress LiteLLM router can then
try another deployment.

The public logical group owns the FIFO admission queue. With
`MN_LITELLM_MAX_CONCURRENT_REQUESTS=auto`, its capacity equals the number of
configured deployments in that group. An integer value is a hard upper bound:

```text
effective public capacity = min(replica count, configured integer limit)
```

For example, three `medium` installations admit three overlapping medium
requests through one ingress proxy. A fourth request waits until a public slot
is released. Completion and embedding calls use separate lanes.

Relevant limits are:

```text
MN_LITELLM_MAX_CONCURRENT_REQUESTS=auto
MN_LITELLM_MAX_CONCURRENT_EMBEDDINGS=1
MN_LITELLM_MAX_QUEUED_REQUESTS=64
MN_LITELLM_QUEUE_TIMEOUT_SECONDS=1800
MN_LITELLM_MAX_SLOT_SECONDS=3600
```

The public queues are per ingress proxy, not one centralized cluster queue.
Private owner locks provide cluster-wide decoder protection when several
ingress proxies choose the same installation. Under cross-ingress saturation,
a caller may receive a retryable owner-busy failure when no alternate replica
is available; queue fairness is guaranteed only among requests entering the
same proxy and logical group.

Cluster replicas set `cooldown_time: 0` because `owner_busy` is a capacity
signal rather than evidence that a deployment is unhealthy. A genuine request
failure can still advance to another LiteLLM deployment. Membership and model
inventory reconciliation, rather than a long LiteLLM cooldown, ultimately
removes an unavailable node from the generated group.

## Operations and diagnostics

Install or adopt one logical model on multiple nodes with one command:

```bash
mn model add small --local --node spark
```

Repeated adds are idempotent and append only missing replicas. All targets are
hardware-preflighted before installation begins. If execution later partially
fails, successful installations remain recorded and a retry safely fills the
missing targets.

Inspect the public lifecycle and per-installation health with:

```bash
mn model list --json
mn model show small --json
mn model doctor small --json
```

Use the queue response headers and structured logs to diagnose admission:

- `x-mn-llm-queue-wait-ms` reports time spent waiting;
- `x-mn-llm-queue-pool` identifies completion or embedding admission;
- `x-mn-llm-max-concurrent` reports the effective group capacity; and
- `runtime_llm_request_*` log events record queue, admission, rejection, and
  release transitions without logging prompts or response content.

If a route does not appear after a join, check live node membership, each
node's published model inventory, the cluster-model-monitor state file, and
the LiteLLM service health. Do not work around an incomplete snapshot by
deleting the generated route ledger; the monitor deliberately retains the last
complete route set until departure is confirmed.
