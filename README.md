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

Override blueprint config for one run without changing `config/overwrite.json`:

```bash
mn blueprint run --folder ./vc_assistant \
  --set document_sources.folder_path=/path/to/documents \
  --set execution.debug=true
```

Repeat `--set` for multiple values. Values use JSON types when possible and
otherwise remain strings.

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
