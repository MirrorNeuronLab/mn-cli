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
```
