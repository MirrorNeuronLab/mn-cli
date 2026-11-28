# mn-cli

The Rust package produces the `mn` executable. Its clap command tree covers blueprints, jobs, runs, schedules, models, operations, services, nodes, resources and the local runtime. Use `mn --help` or `mn <group> <command> --help` for the available options. Commands support the v1 JSON output contract.

`mn run watch RUN_ID` opens a live dashboard in an interactive terminal. Use ↑ / ↓ to select an agent, Enter to inspect it, and Backspace to return to the overview. Use `[` and `]` to page through child steps, and `f` to follow the active child again. Finished runs stay open for inspection. Press `q` or Ctrl+C to close the dashboard at any time.

Redirected output or `MN_CLI_OUTPUT=plain` prints progress as lines. `mn run watch RUN_ID --json` emits versioned stream records.

The Elapsed column uses the first attempt's start time and includes retries. Active rows tick every second. Finished rows retain their final duration, and an em dash marks steps that never started. During a disconnect, the dashboard freezes its last observation until it receives a fresh snapshot. The JSON completion record includes every step when attaching to a finished run.

The production `mn` command runs `mn.wasm` under host Wasmtime and calls `mn-client-support.wasm` over WIT. For a development install, place the three repositories beside `mn-deploy` and run `./install_dev.sh --no-runtime`.

From a sibling `mn-deploy` checkout, run `./install.sh --no-runtime` to install in `~/.mn`. Connect with `mn node add HOST --grpc-port 55051 --token "$RUNTIME_NODE_TOKEN"` and inspect the connection with `mn runtime status --json`.
