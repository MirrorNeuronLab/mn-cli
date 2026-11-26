use crate::{client::App, input::confirm};
use mn_client_support::{Error, Result, connection};
use serde_json::Value;
pub(crate) async fn execute(a: &App, action: &str, v: &Value) -> Result<Value> {
    if connection::role(&a.home)? == "client" {
        if action == "status" {
            return a.ipc.status().await;
        }
        return Err(Error::new(
            422,
            "MN_RUNTIME_NOT_INSTALLED",
            "No runtime is installed in client mode.",
        )
        .hint("Connect with mn node add. Start the optional API with mn-api."));
    }
    let exe = a.home.join("bin/mn-runtime");
    if !exe.is_file() {
        return Err(Error::new(
            503,
            "MN_RUNTIME_NOT_INSTALLED",
            "The installed runtime lifecycle executable is missing.",
        ));
    }
    if (action == "cleanup" && v["dry_run"] != true)
        || (action == "upgrade" && v["check_only"] != true)
    {
        confirm(v, "Apply runtime changes")?;
    }
    let mut cmd = tokio::process::Command::new(exe);
    cmd.env("MN_HOME", &a.home).arg(action).arg("--json");
    for name in ["host", "grpc_port", "timeout"] {
        if !v[name].is_null() {
            cmd.arg(format!("--{}", name.replace('_', "-"))).arg(
                v[name]
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| v[name].to_string()),
            );
        }
    }
    for name in [
        "api",
        "web_ui",
        "dry_run",
        "include_cache",
        "force",
        "yes",
        "check_only",
        "repair",
    ] {
        if v[name] == true || (name == "yes" && action == "upgrade" && v["check_only"] != true) {
            cmd.arg(format!("--{}", name.replace('_', "-")));
        }
    }
    let res = cmd.output().await.map_err(|_| Error::storage())?;
    let body: Value = res
        .stdout
        .split(|b| *b == b'\n')
        .rev()
        .find(|l| !l.is_empty())
        .and_then(|l| serde_json::from_slice(l).ok())
        .ok_or_else(Error::protocol)?;
    if !res.status.success() {
        return Err(Error::new(
            503,
            body["code"]
                .as_str()
                .unwrap_or("MN_RUNTIME_LIFECYCLE_FAILED"),
            body["message"]
                .as_str()
                .unwrap_or("The runtime command failed."),
        ));
    }
    Ok(body)
}
