# AGENTS.md

Instructions for coding agents working in this repository. These instructions
apply only to `mn-cli`.

## Start Here

Read `SPEC.md`, `README.md`, `pyproject.toml`, the relevant command module, and
the closest tests before editing. Check `git status` and preserve unrelated
changes.

The CLI is an adapter over `mirrorneuron-python-sdk`. Keep reusable runtime,
manifest, model, and workflow behavior in the SDK; keep command parsing,
terminal interaction, local service orchestration, and presentation here.

## Repository Map

- `mn_cli/main.py`: Typer root app and command registration.
- `mn_cli/libs/`: command families and shared CLI helpers.
- `mn_cli/libs/run_cmds/`: split implementation of blueprint run/monitor/result
  behavior; preserve the established import facade.
- `mn_cli/libs/ui.py`: shared status, summary, and result presentation.
- `mn_cli/runtime/`: local Redis, Docker, cluster, server, sidecar, path, and
  storage orchestration.
- `mn_cli/config.py`: environment loading and precedence.
- `mn_cli/error_handler.py`: stable user-facing error conversion.
- `mn_cli/schemas/`: packaged schemas.
- `tests/`: command, runtime, output, and error behavior.

## Command and Output Contracts

- Keep the command groups registered in `main.py` discoverable and preserve
  established option names, exit codes, and automation behavior.
- Presentation-only work must not change control flow, requests, or data
  contracts.
- Use `mn_cli.libs.ui` for user-facing status:
  - `✓` completed successfully;
  - `→` progress or lifecycle information;
  - `! Warning:` non-fatal conditions;
  - `× Error:` actionable failures, with a code when available.
- Routine confirmations are compact status plus borderless key/value output.
  Reserve result panels for rich job/run lifecycle results.
- `MN_CLI_OUTPUT=plain` and `NO_COLOR` must remain predictable for scripts.
  Do not add decorative or color-only meaning to plain output.
- Interactive prompts must have a non-interactive alternative. Destructive
  commands require explicit confirmation or an established force/yes flag.
- Never print secrets, auth headers, environment dumps, or unsanitized exception
  details. Diagnostic output must stay behind `--debug`/`--verbose`.

## Implementation Rules

- Reuse SDK APIs instead of copying transport or manifest logic.
- Treat runtime responses, catalog files, paths, and user values as untrusted.
- Keep local runtime mutations in the relevant `mn_cli.runtime` or command
  helper, not in rendering utilities.
- Preserve test injection points. Unit tests must not require a live runtime,
  Docker daemon, Redis, model, or network unless explicitly marked as an
  integration test.
- Update `README.md` and `SPEC.md` for public command, config, or output changes.
- Do not hand-edit `mirrorneuron_cli.egg-info`.

## Verification

```bash
python -m pytest tests/test_<area>.py -q
python -m ruff check .
python -m pytest
python -m build
```

When changing help or terminal output, exercise both ordinary and plain modes
and test narrow and wide terminal widths where relevant.

## Issue-Fixing Policy

- Fix the root cause in the owning layer unless the user explicitly requests a
  temporary workaround.
- Do not add compatibility shims or fallback branches that mask a broken
  primary path.
- Keep intentional compatibility behavior narrow, documented, and tested.
