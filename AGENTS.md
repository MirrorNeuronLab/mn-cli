# mn-cli

## Rust layout

- Keep `src/main.rs` and binaries under `src/bin/` thin. Start the runtime and call the library entry point.
- Declare modules and export the public interface in `src/lib.rs`. Keep configuration and argument definitions in `src/config.rs` and error types or error presentation in `src/error.rs`.
- Group code by feature. Use the same domain names across client crates (`runs`, `jobs`, `nodes`, `catalog`) wherever they own the same concept. `runs.rs` owns run commands and `runs/monitor.rs` owns the live monitor. Keep job, node, runtime, and blueprint commands in their respective feature modules.
- Use `feature.rs` alongside `feature/submodule.rs` for a growing domain. Choose a descriptive name for each submodule and keep helpers private to their feature.
- Put unit tests in a `#[cfg(test)]` module next to the behavior. Use `tests/` for integration tests that drive a public interface or binary.

## Compatibility and verification

Use `../mn-cli-old` to check command paths, flag semantics, exit codes and machine output. Exercise the live monitor with a changing stream and a detached or piped terminal. Keep client-to-client calls on WIT and runtime calls on the existing gRPC interface.

Run `cargo fmt --check`, `cargo test --locked`, and `cargo clippy --locked --all-targets -- -D warnings` from this crate.
