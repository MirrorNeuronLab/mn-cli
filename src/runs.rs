mod monitor;
mod retry;
use crate::{
    client::App,
    input::{confirm, pick, s},
};
use mn_client_support::{Error, Result, connection};
pub(crate) use monitor::watch;
use serde_json::{Value, json};
use std::{collections::HashSet, path::Path, time::Duration};

pub(crate) fn terminal(v: &Value) -> bool {
    matches!(
        s(v, "status").to_ascii_lowercase().as_str(),
        "complete"
            | "completed"
            | "failed"
            | "cancelled"
            | "succeeded"
            | "deleted"
            | "stopped"
            | "dry_run"
    )
}
pub async fn execute(a: &App, c: &[&str], v: &Value) -> Result<Value> {
    let action = c[1];
    let mut q = pick(v, &["run_id"]);
    if c[0] == "operation" {
        let r = a.get("get_operation", pick(v, &["operation_id"])).await?;
        return if action == "watch" {
            crate::operations::watch(a, r, true).await
        } else {
            Ok(r)
        };
    }
    match action {
        "show" | "info" => a.get("get_run", q).await,
        "watch" => watch(a, s(v, "run_id"), None).await,
        "pause" | "resume" | "cancel" => {
            q["request"] = json!({"desired_state":match action{"pause"=>"paused","resume"=>"running",_=>"cancelled"},"queued_only":v["queued_only"]==true});
            a.get("update_run", q).await
        }
        "delete" => {
            if v["dry_run"] == true {
                return Ok(json!({"dry_run":true,"run":a.get("get_run",q).await?}));
            }
            confirm(v, "Permanently delete this run")?;
            a.get("delete_run", q).await
        }
        "compare" => {
            let mut r = json!({});
            for k in ["run_a", "run_b"] {
                r[k] = a.get("get_run", json!({"run_id":v[k]})).await?;
            }
            Ok(r)
        }
        "retry" => retry::execute(a, q, v).await,
        "result" => {
            let id = s(v, "run_id");
            let dir = if let Some(dest) = v["output"].as_str() {
                connection::expand(Path::new(dest))
            } else {
                let segment = mn_client_support::resources::resource_id(id);
                if matches!(segment.as_str(), "." | ".." | "") {
                    return Err(Error::invalid("Invalid run ID."));
                }
                a.home.join("outputs").join(segment)
            };
            std::fs::create_dir_all(&dir).map_err(|_| Error::storage())?;
            a.download(
                "get_run_final_artifact",
                q.clone(),
                Some(&dir.join("final_artifact.json")),
            )
            .await?;
            let mut files = vec!["final_artifact.json".to_string()];
            let outputs = a.items("list_run_outputs", q.clone()).await?;
            for (n, o) in outputs.as_array().unwrap().iter().enumerate() {
                let idx = o
                    .get("index")
                    .map(|v| v.as_u64().ok_or_else(Error::protocol))
                    .transpose()?
                    .unwrap_or(n as u64);
                q["output_index"] = idx.into();
                let name = format!("output-{idx}");
                a.download("download_run_output", q.clone(), Some(&dir.join(&name)))
                    .await?;
                files.push(name);
            }
            Ok(json!({"run_id":id,"output":dir,"files":files}))
        }
        "human" => match c[2] {
            "list" => {
                if v["pending"] == true {
                    q["request_status"] = "pending".into();
                }
                a.items("list_run_human_requests", q).await
            }
            "respond" => {
                q["request_id"] = v["request_id"].clone();
                q["request"] = json!({"response":pick(v,&["decision","notes","reviewer"])});
                a.get("create_run_human_response", q).await
            }
            _ => {
                q["request_id"] = v["notice_id"].clone();
                q["request"] = json!({"note":v["reviewer"]});
                a.get("create_run_human_acknowledgement", q).await
            }
        },
        "list" | "resources" | "logs" => observe(a, action, v).await,
        _ => Err(Error::invalid("Unknown run command.")),
    }
}
async fn observe(a: &App, action: &str, v: &Value) -> Result<Value> {
    let live = v["follow"] == true || v["live"] == true;
    let mut seen = HashSet::new();
    loop {
        let value = match action {
            "list" => {
                let (op, q) = if v["job"].is_string() {
                    ("list_job_runs", json!({"job_id":v["job"]}))
                } else {
                    ("list_runs", json!({}))
                };
                let mut rows = a.items(op, q).await?;
                let rows = rows.as_array_mut().unwrap();
                if v["blueprint"].is_string() {
                    rows.retain(|r| r["blueprint_id"] == v["blueprint"]);
                }
                rows.truncate(v["limit"].as_u64().unwrap_or(20) as usize);
                json!({"items":rows})
            }
            "resources" => {
                a.get(
                    "get_run_resources",
                    pick(v, &["run_id", "window", "bucket"]),
                )
                .await?
            }
            _ => {
                let mut items = vec![];
                let channel = s(v, "channel");
                if !matches!(channel, "logs" | "events" | "all") {
                    return Err(Error::invalid("--channel must be logs, events or all."));
                }
                for ch in ["logs", "events"] {
                    if channel != "all" && ch != channel {
                        continue;
                    }
                    let (op, q) = if ch == "logs" {
                        ("list_run_logs", pick(v, &["run_id", "level"]))
                    } else {
                        ("list_run_events", pick(v, &["run_id"]))
                    };
                    let rows = a.items(op, q).await?;
                    let rows = rows.as_array().unwrap();
                    items.extend_from_slice(
                        &rows[rows
                            .len()
                            .saturating_sub(v["lines"].as_u64().unwrap_or(50) as usize)..],
                    );
                }
                json!({"items":items})
            }
        };
        if !live {
            return Ok(value);
        }
        if action == "logs" {
            for row in value["items"].as_array().unwrap() {
                if seen.insert(row.to_string()) {
                    a.emit("event", row.clone());
                }
            }
        } else {
            a.emit("snapshot", value);
        }
        let delay = v["interval"].as_f64().unwrap_or(1.0).clamp(0.1, 86400.0);
        tokio::select! {
            _ = crate::interrupt() => {
                let mut result = json!({"detached": true});
                if action == "resources" {
                    result["run_id"] = v["run_id"].clone();
                }
                return Ok(result);
            }
            _ = tokio::time::sleep(Duration::from_secs_f64(delay)) => {}
        }
    }
}
