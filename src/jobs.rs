use crate::{
    blueprints,
    client::App,
    error::unsupported,
    input::{confirm, key, pick, read_object, s},
    runs,
};
use futures_util::TryStreamExt;
use mn_client_support::{Error, Result, connection};
use serde_json::{Value, json};
use std::io::{self, IsTerminal};
use std::path::Path;
use std::time::{Duration, Instant};
use tokio_util::io::ReaderStream;
pub(crate) async fn execute(a: &App, c: &[&str], v: &Value) -> Result<Value> {
    let mut q = pick(v, &["job_id"]);
    let action = c[0];
    match action {
        "monitor" => {
            let (pinned, _) = a.pin().await?;
            let runs = pinned
                .items(
                    "list_job_runs",
                    json!({"job_id":v["job_id"],"include_terminal":true}),
                )
                .await?;
            let id = monitor_run(&runs, v["run_id"].as_str())?;
            runs::watch(&pinned, id, None).await
        }
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
                    let finished = crate::operations::watch(a, res, false).await?;
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
                    crate::operations::watch(a, r, true).await
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
        crate::operations::watch(a, r, true).await?;
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

fn monitor_run<'a>(runs: &'a Value, selected: Option<&str>) -> Result<&'a str> {
    let rows = runs.as_array().ok_or_else(Error::protocol)?;
    let row = rows
        .iter()
        .filter(|r| r["run_id"].is_string() || r["id"].is_string())
        .filter(|r| selected.is_none_or(|id| r["run_id"] == id || r["id"] == id))
        .max_by_key(|r| {
            (
                !runs::terminal(r),
                r["created_at"].as_str(),
                r["run_id"].as_str().or(r["id"].as_str()),
            )
        });
    row.and_then(|r| r["run_id"].as_str().or(r["id"].as_str()))
        .ok_or_else(|| Error::new(404, "MN_NOT_FOUND", "No matching run exists for this job."))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn monitor_selects_the_latest_active_run_or_the_requested_job_run() {
        let rows = json!([{"id":"older","status":"running","created_at":"2025-01-01"},
            {"run_id":"done","status":"completed","created_at":"2025-01-04"},
            {"run_id":"new","status":"running","created_at":"2025-01-02"}]);
        assert_eq!(monitor_run(&rows, None).unwrap(), "new");
        assert_eq!(monitor_run(&rows, Some("done")).unwrap(), "done");
        assert!(monitor_run(&rows, Some("foreign")).is_err());
        assert!(monitor_run(&json!([]), None).is_err());
    }
}
