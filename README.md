# MirrorNeuron CLI

`mn-cli` provides the `mn` command for validating and running blueprints,
inspecting runtime state, managing jobs, exporting artifacts, and starting local
services installed by `mn-deploy`.

## Quick Start

Install locally and run tests:

```bash
python3 -m venv .venv
. .venv/bin/activate
python3 -m pip install -e .
python3 -m pytest -q
```

Try the CLI:

```bash
mn --version
mn node list
mn blueprint run message_routing_trace
```

## Details

- [MirrorNeuron Component Guide](../mn-docs/component-guide.md#cli)
- [CLI Reference](../mn-docs/cli.md)
- [Environment Variables](../mn-docs/env_variables.md)
- [Monitor Guide](../mn-docs/monitor.md)

## Notes

- A running MirrorNeuron core is required for live runtime commands.
- The default gRPC target comes from `MN_GRPC_TARGET`, then local deployment
  settings, then `localhost:55051`.
- Use `mn blueprint validate` before `mn blueprint run --folder` when checking a local bundle.
