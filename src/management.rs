use super::*;
use futures_util::TryStreamExt;
use std::time::Duration;
use tokio_util::io::ReaderStream;

pub async fn execute(a: &App, c: &[&str], v: &Value) -> Result<Value> {
    let action = c[1];
    match c[0] {
        "node" => node(a, action, v).await,
        "runtime" => lifecycle(a, action, v).await,
        "job" => job(a, &c[1..], v).await,
        "model" => {
            let mut opts = v.clone();
            if let Some(options) = opts.as_object_mut() {
                options.remove("debug");
                options.remove("json_output");
            }
            if action == "remove" && v["dry_run"] != true {
                confirm(v, "Remove this runtime model")?;
                opts["yes"] = true.into();
            }
            a.ipc.command(&format!("model.{action}"), opts, 3600).await
        }
        "resource" => match action {
            "show" => a.get("get_runtime_resources", json!({})).await,
            "usage" => a.get("get_metrics", json!({})).await,
            _ => {
                a.ipc
                    .command(
                        "resource.set",
                        pick(v, &["cpu", "gpu", "memory", "disk"]),
                        30,
                    )
                    .await
            }
        },
        "schedule" => {
            let mut q = pick(v, &["schedule_id"]);
            q["owner_node"] = v["node"].clone();
            match action {
                "list" => a.items("list_schedules", q).await,
                "show" => a.get("get_schedule", q).await,
                "cancel" => a.conditional("get_schedule", "delete_schedule", q).await,
                _ => {
                    q["request"] = json!({"schedule":{"run_at":v["at"]}});
                    a.conditional("get_schedule", "update_schedule", q).await
                }
            }
        }
        "service" => {
            let mut q = pick(v, &["name", "node", "job_id", "tag"]);
            if !v["status"].is_null() {
                q["service_status"] = v["status"].clone();
            }
            q["passing_only"] = json!(v["all_statuses"] != true);
            if action == "list" {
                a.items("list_services", q).await
            } else {
                a.get("get_service_resolution", q).await
            }
        }
        _ => Err(Error::invalid("Unknown command.")),
    }
}
async fn node(a: &App, action: &str, v: &Value) -> Result<Value> {
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
            let r = a.get("create_node_drain", q).await?;
            if v["wait"] == true {
                runs::operation(a, r).await
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
async fn lifecycle(a: &App, action: &str, v: &Value) -> Result<Value> {
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
async fn job(a: &App, c: &[&str], v: &Value) -> Result<Value> {
    let mut q = pick(v, &["job_id"]);
    let action = c[0];
    match action {
        "list" => a.items("list_jobs", pick(v, &["include_archived"])).await,
        "show" | "analysis" => {
            a.get(
                if action == "show" {
                    "get_job"
                } else {
                    "get_job_analysis"
                },
                q,
            )
            .await
        }
        "create" => {
            let mut body = pick(v, &["job_id"]);
            body["blueprint_id"] = v["bundle"].clone();
            body["resolved_configuration"] = read_object(v, "config")?;
            if !v["node"].is_null() {
                body["owner_node"] = v["node"].clone();
            }
            if blueprints::local_source(s(v, "bundle")) {
                let res = blueprints::local(
                    a,
                    "create-job",
                    json!({"target":blueprints::absolute(s(v,"bundle"))?}),
                    body,
                    &key(),
                )
                .await?;
                if res["operation_id"].is_string() {
                    let finished = runs::operation(a, res).await?;
                    return Ok(finished.get("result").cloned().unwrap_or(finished));
                }
                return Ok(res);
            }
            a.get(
                "prepare_job",
                json!({"request":body,"idempotency_key":key()}),
            )
            .await
        }
        "start" => {
            if v["force"] == true {
                confirm(v, "Permanently replace the service run and its history")?;
            }
            q["request"] = json!({"run_id":v["run_id"],"inputs":read_object(v,"inputs")?,"admission_policy":blueprints::policy(v),"replace_existing_run":v["force"]});
            q["idempotency_key"] = key().into();
            a.get("create_job_run", q).await
        }
        "archive" => {
            q["request"] = json!({"status":"archived"});
            a.conditional("get_job", "update_job", q).await
        }
        "delete" | "reset-data" => {
            if v["dry_run"] == true {
                return Ok(json!({"dry_run":true,"job":a.get("get_job",q).await?}));
            }
            confirm(v, "Permanently change this job and its data")?;
            if action == "delete" {
                a.conditional("get_job", "delete_job", q).await
            } else {
                q["idempotency_key"] = key().into();
                a.get("create_job_data_reset", q).await
            }
        }
        "relocate" => {
            if v["abort"] == true {
                return a
                    .ipc
                    .command(
                        "job.abort-relocation",
                        json!({"job_id":v["job_id"],"target_node":v["node"]}),
                        30,
                    )
                    .await;
            }
            q["request"] = json!({"target_node":v["node"],"transfer_id":key().replace('-', "")});
            a.get("relocate_job", q).await
        }
        "restore" => {
            let file = tokio::fs::File::open(connection::expand(Path::new(s(v, "input_path"))))
                .await
                .map_err(|_| Error::invalid("Cannot read the requested backup ZIP."))?;
            a.ipc
                .upload(
                    "restore_job_backup",
                    pick(v, &["job_id", "start"]),
                    Box::pin(
                        ReaderStream::new(file)
                            .map_err(|_| Error::invalid("Could not finish reading backup.")),
                    ),
                )
                .await?
                .checked()
        }
        "backup" => {
            a.download(
                "create_job_backup",
                q,
                Some(&connection::expand(Path::new(s(v, "output")))),
            )
            .await
        }
        "mailbox" => match c[1] {
            "list" => {
                a.get(
                    "list_job_mailbox",
                    pick(
                        v,
                        &[
                            "job_id",
                            "after",
                            "limit",
                            "include_acknowledged",
                            "kind",
                            "reply_to",
                        ],
                    ),
                )
                .await
            }
            "ack" => {
                a.get("acknowledge_job_mail", pick(v, &["job_id", "message_id"]))
                    .await
            }
            _ => {
                let mut b = pick(v, &["subject", "text", "kind", "reply_to", "request_id"]);
                b["recipient_job_id"] = v["recipient"].clone();
                if b["request_id"].is_null() {
                    b["request_id"] = key().into();
                }
                if !v["attachment"].is_null() {
                    b["attachment"] = read_object(v, "attachment")?;
                }
                q["request"] = b;
                a.get("send_job_mail", q).await
            }
        },
        "skills" => match c[1] {
            "list" => a.get("list_job_skills", q).await,
            "add" => {
                q["value"] = read_object(v, "declaration")?;
                q["idempotency_key"] = key().into();
                let r = a.get("add_job_skill", q).await?;
                if r["operation_id"].is_string() {
                    runs::operation(a, r).await
                } else {
                    Ok(r)
                }
            }
            _ => {
                q["name"] = v["name"].clone();
                q["value"] = json!({"enabled":c[1]=="enable"});
                a.get("set_job_skill", q).await
            }
        },
        "integrations" if c[1] == "list" => a.get("list_job_integrations", q).await,
        "integrations" | "connect" => {
            if s(v, "provider") != "slack" {
                return Err(unsupported(
                    "This job integration requires the slack provider.",
                ));
            }
            match c.get(1).copied() {
                Some("add") => a.get("add_job_slack", q).await,
                Some(op) => {
                    q["operation"] = op.into();
                    q["value"] = json!({});
                    a.get("control_job_slack", q).await
                }
                None => connect_job(a, q, v).await,
            }
        }
        _ => Err(Error::invalid("Unknown job command.")),
    }
}
async fn connect_job(a: &App, mut q: Value, v: &Value) -> Result<Value> {
    q["value"] = pick(v, &["name", "session"]);
    let mut r = a.get("connect_job_slack", q.clone()).await?;
    if r["operation_id"].is_string() {
        runs::operation(a, r).await?;
        r = a.get("connect_job_slack", q.clone()).await?;
    }
    if r["delivery"] == "enabled" || (a.json && v["channel"].is_null()) {
        return Ok(r);
    }
    q["value"]["session"] = r["session"].clone();
    if let Some(url) = r["url"].as_str() {
        eprintln!("Authorize this workspace at {url}");
    }
    let until = Instant::now() + Duration::from_secs(600);
    while r["authorization"] == "pending" {
        if Instant::now() >= until {
            return Err(Error::invalid("Authorization expired. Run connect again."));
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
        r = a.get("connect_job_slack", q.clone()).await?;
    }
    let channels = r["channels"].as_array().ok_or_else(Error::protocol)?;
    let mut channel = s(v, "channel").to_owned();
    if channel.is_empty() && io::stdin().is_terminal() {
        for c in channels {
            eprintln!("{} {}", c["id"], c["name"]);
        }
        eprint!("Public channel ID: ");
        io::stdin()
            .read_line(&mut channel)
            .map_err(|_| Error::invalid("Channel selection cancelled."))?;
    }
    if !channels.iter().any(|c| c["id"] == channel.trim()) {
        return Err(Error::invalid(
            "Select a listed public channel with --channel.",
        ));
    }
    q["value"]["channel"] = channel.trim().into();
    a.get("connect_job_slack", q).await
}
