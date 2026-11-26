use crate::{
    client::App,
    error::unsupported,
    input::{confirm, pick, s},
};
use mn_client_support::{Error, Result};
use serde_json::{Value, json};
pub(crate) async fn execute(a: &App, action: &str, v: &Value) -> Result<Value> {
    let name = s(v, "node_name");
    let mut q = json!({"node_id":name});
    match action {
        "add" => {
            a.ipc
                .connect(
                    s(v, "host"),
                    v["token"].as_str(),
                    v["grpc_port"]
                        .as_u64()
                        .map(|n| u16::try_from(n).map_err(|_| Error::invalid("Invalid port.")))
                        .transpose()?,
                    v["runtime_home"].as_str(),
                )
                .await
        }
        "remove" => {
            confirm(v, "Remove this runtime from the shared cluster")?;
            a.ipc.remove(name).await
        }
        "list" => a.items("list_nodes", json!({})).await,
        "show" => a
            .items("list_nodes", json!({}))
            .await?
            .as_array()
            .and_then(|items| {
                items
                    .iter()
                    .find(|i| {
                        ["node_id", "node_name", "node", "name", "id"]
                            .iter()
                            .any(|k| i[k] == name)
                    })
                    .cloned()
            })
            .ok_or_else(|| {
                Error::new(
                    404,
                    "MN_REMOTE_NODE_NOT_FOUND",
                    "The remote node was not found.",
                )
            }),
        "refresh-token" => Err(unsupported(
            "Manage the runtime credential on its node and reconnect with mn node add.",
        )),
        "drain" => {
            q["request"] = pick(
                v,
                &[
                    "reason",
                    "deadline",
                    "dry_run",
                    "wait",
                    "ignore_system_jobs",
                ],
            );
            let (pinned, _) = a.pin().await?;
            let r = pinned.get("create_node_drain", q).await?;
            if v["wait"] == true {
                crate::operations::watch(&pinned, r, true).await
            } else {
                Ok(r)
            }
        }
        "undrain" => {
            a.ipc
                .command(
                    "node.undrain",
                    json!({"node_name":name,"options":pick(v, &["reason", "mark_eligible"])}),
                    30,
                )
                .await
        }
        "maintenance" => {
            q["request"] = json!({"maintenance":v["enable"],"reason":v["reason"]});
            a.get("update_node", q).await
        }
        _ => {
            q["request"] = pick(v, &["reason", "dry_run"]);
            a.get("create_node_reconciliation", q).await
        }
    }
}
