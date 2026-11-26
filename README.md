# mn-cli

The Rust package produces the `mn` executable. Its clap command tree covers blueprints, jobs, runs, schedules, models, operations, services, nodes, resources and the local runtime. Use `mn --help` or `mn <group> <command> --help` for the available options. Commands support the v1 JSON output contract.

The production `mn` command runs `mn.wasm` under host Wasmtime and calls `mn-client-support.wasm` over WIT. For a development install, place the three repositories beside `mn-deploy` and run `./install_dev.sh --no-runtime`.

From a sibling `mn-deploy` checkout, run `./install.sh --no-runtime` to install in `~/.mn`. Connect with `mn node add HOST --grpc-port 55051 --token "$RUNTIME_NODE_TOKEN"` and inspect the connection with `mn runtime status --json`.
