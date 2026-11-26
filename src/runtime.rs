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
    let mut args = vec![action.to_owned(), "--json".into()];
    for name in ["host", "grpc_port", "timeout"] {
        if !v[name].is_null() {
            args.push(format!("--{}", name.replace('_', "-")));
            args.push(
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
            args.push(format!("--{}", name.replace('_', "-")));
        }
    }
    #[cfg(not(target_os = "wasi"))]
    let (success, output) = {
        let res = tokio::process::Command::new(exe)
            .env("MN_HOME", &a.home)
            .args(&args)
            .output()
            .await
            .map_err(|_| Error::storage())?;
        (res.status.success(), res.stdout)
    };
    #[cfg(target_os = "wasi")]
    let (success, output) = mn_client_support::wasi::utility(
        "runtime",
        &args.iter().map(String::as_str).collect::<Vec<_>>(),
        std::time::Duration::from_secs(3600),
    )
    .await
    .map_err(|_| Error::storage())?;
    let body: Value = output
        .split(|b| *b == b'\n')
        .rev()
        .find(|l| !l.is_empty())
        .and_then(|l| serde_json::from_slice(l).ok())
        .ok_or_else(Error::protocol)?;
    if !success {
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
