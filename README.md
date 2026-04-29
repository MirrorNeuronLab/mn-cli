# MirrorNeuron CLI

The official Command Line Interface for managing the MirrorNeuron distributed runtime system.

## Installation
*Note: This tool is installed automatically and symlinked globally as `mn` by the MirrorNeuron `install.sh` script.*

```bash
pip install mn-cli
```

## Commands
Powered by Typer and Rich, this CLI provides elegant formatting for job lifecycle management:

```bash
mn nodes                 # View system summary and available executor pools
mn submit ./flow.json    # Submit a new workflow manifest
mn list                  # View all active and completed jobs
mn status <job_id>       # Detailed execution state of a specific job
mn monitor <job_id>      # Stream live execution events
mn cancel <job_id>       # Gracefully terminate a workflow
mn metrics               # Inspect runtime metrics summary
mn dead-letters <job_id> # Inspect dead-letter events
```

## Configuration

All overrides use `MIRROR_NEURON_` env vars:

- `MIRROR_NEURON_GRPC_TARGET`: core gRPC target.
- `MIRROR_NEURON_GRPC_TIMEOUT_SECONDS`: RPC timeout; `0` or `none` disables it.
- `MIRROR_NEURON_GRPC_AUTH_TOKEN`: optional bearer metadata for protected core gateways.
- `MIRROR_NEURON_CLI_LOG_PATH`: error log path.
- `MIRROR_NEURON_CLI_OUTPUT=plain`: disable Rich color output.
