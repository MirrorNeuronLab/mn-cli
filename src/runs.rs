use super::*;
use std::{collections::HashSet, time::Duration};
fn terminal(v: &Value) -> bool {
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
pub async fn operation(a: &App, mut r: Value) -> Result<Value> {
    while !terminal(&r) {
        let id = r["operation_id"]
            .as_str()
            .ok_or_else(Error::protocol)?
            .to_owned();
        a.emit("progress", r);
        tokio::time::sleep(Duration::from_secs(1)).await;
        r = a.get("get_operation", json!({"operation_id":id})).await?;
    }
    if matches!(s(&r, "status"), "cancelled" | "failed") {
        return Err(Error::new(
            409,
            "MN_REMOTE_OPERATION_FAILED",
            "The remote operation did not complete successfully.",
        ));
    }
    Ok(r)
}
pub async fn watch(a: &App, id: &str, seconds: Option<f64>) -> Result<Value> {
    let start = Instant::now();
    let q = json!({"run_id":id});
    let mut last = Value::Null;
    loop {
        let r = a.get("get_run", q.clone()).await?;
        if r != last {
            a.emit("snapshot", r.clone());
            last = r.clone();
        }
        if terminal(&r) {
            return Ok(r);
        }
        if seconds.is_some_and(|s| start.elapsed().as_secs_f64() >= s) {
            break;
        }
        a.emit(
            "progress",
            a.get("get_run_workflow_progress", q.clone()).await?,
        );
        tokio::select! {_=tokio::signal::ctrl_c()=>break,_=tokio::time::sleep(Duration::from_secs_f64(seconds.map_or(1.0,|s|(s-start.elapsed().as_secs_f64()).clamp(0.0,1.0))))=>{}}
    }
    Ok(json!({"run_id":id,"detached":true}))
}
pub async fn execute(a: &App, c: &[&str], v: &Value) -> Result<Value> {
    let action = c[1];
    let mut q = pick(v, &["run_id"]);
    if c[0] == "operation" {
        let r = a.get("get_operation", pick(v, &["operation_id"])).await?;
        return if action == "watch" {
            operation(a, r).await
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
        "retry" => retry(a, q, v).await,
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
        tokio::select! {_=tokio::signal::ctrl_c()=>return Ok(json!({"detached":true})),_=tokio::time::sleep(Duration::from_secs_f64(v["interval"].as_f64().unwrap_or(1.0).clamp(0.1,86400.0)))=>{}}
    }
}
async fn retry(a: &App, mut q: Value, v: &Value) -> Result<Value> {
    let mut overrides = json!({});
    for item in v["settings"].as_array().into_iter().flatten() {
        let (k, text) = item
            .as_str()
            .and_then(|s| s.split_once('='))
            .ok_or_else(|| Error::invalid("Retry settings must use path=value."))?;
        let val = serde_json::from_str(text).unwrap_or_else(|_| json!(text));
        if k.split('.')
            .any(|s| s.is_empty() || !s.chars().all(|c| c.is_alphanumeric() || c == '_'))
            || overrides.get(k).is_some()
            || !(val.is_string() || val.is_i64() || val.is_u64())
        {
            return Err(Error::invalid("Invalid or duplicate retry setting."));
        }
        overrides[k] = val;
    }
    let mut body = pick(v, &["expected_attempt", "checkpoint_revision"]);
    if v["expected_attempt"].is_null() != v["checkpoint_revision"].is_null()
        || (!v["expected_attempt"].is_null()
            && (v["idempotency_key"].is_null() || v["dry_run"] == true))
    {
        return Err(Error::invalid(
            "Supply the expected attempt, checkpoint revision and idempotency key together.",
        ));
    }
    if body == json!({}) {
        q["request"] = json!({"configuration_overrides":overrides});
        let plan = a.get("plan_run_retry", q.clone()).await?;
        if v["dry_run"] == true {
            return Ok(plan);
        }
        if plan["eligible"] != true {
            return Err(Error::new(
                409,
                "MN_REMOTE_RETRY_UNAVAILABLE",
                "This run is ineligible for retry.",
            ));
        }
        body = pick(&plan, &["expected_attempt", "checkpoint_revision"]);
    }
    body["configuration_overrides"] = overrides;
    body["admission_policy"] = blueprints::policy(v).into();
    q["request"] = body;
    let id = v["idempotency_key"]
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(key);
    q["idempotency_key"] = json!(id);
    a.emit("retry_selection", pick(&q, &["idempotency_key", "request"]));
    let mut r = a.get("retry_run", q).await?;
    r["idempotency_key"] = id.into();
    Ok(r)
}
