use crate::{
    client::App,
    error::unsupported,
    input::{confirm, key, pick},
    runs,
};
use mn_client_support::{Error, Result, connection};
use serde_json::{Value, json};
use std::path::Path;

pub fn policy(v: &Value) -> &'static str {
    if v["queue"] == true {
        "enqueue_if_busy"
    } else {
        "start_now"
    }
}
pub fn local_source(target: &str) -> bool {
    !target.is_empty()
        && (target.starts_with(['.', '/', '~'])
            || target.contains(['/', '\\'])
            || Path::new(target).exists())
}
pub fn absolute(s: &str) -> Result<String> {
    let p = connection::expand(Path::new(s));
    let p = p
        .canonicalize()
        .or_else(|_| std::path::absolute(p))
        .map_err(|_| Error::invalid("Invalid local source path."))?;
    Ok(p.to_string_lossy().into_owned())
}
pub async fn local(a: &App, action: &str, opts: Value, body: Value, id: &str) -> Result<Value> {
    let (p, home) = a.pin().await?;
    if home.is_empty() {
        return Err(unsupported(
            "Local sources require an attached local runtime. Use mn node add local --runtime-home <PATH>.",
        ));
    }
    let mut args = json!({"runtime_home":home,"action":action,"options":opts,"request_id":id});
    if !body.is_null() {
        args["request"] = body;
    }
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3600);
    match p.ipc.command("blueprint.local", args, 3600).await {
        Err(error)
            if action == "run"
                && error.code == "MN_RUNTIME_UNAVAILABLE"
                && error.details.get("submission_id").is_none() =>
        {
            tokio::time::timeout_at(deadline, local_run_receipt(&p, &home, id))
                .await
                .ok()
                .flatten()
                .ok_or(error)
        }
        result => result,
    }
}

async fn local_run_receipt(app: &App, home: &str, request_id: &str) -> Option<Value> {
    use mn_client_support::control::Method;
    use tokio::time::{Duration, Instant, sleep};

    let args = json!({"runtime_home":home,"request_id":request_id});
    let mut last_contact = Instant::now();
    loop {
        let read = app
            .ipc
            .call(Method::Read, "blueprint.local.receipt", args.clone(), "", 5)
            .await;
        match read.and_then(|reply| reply.checked()) {
            Ok(value) if value["status"] == "completed" => {
                return value.get("result").filter(|r| r.is_object()).cloned();
            }
            Ok(value) if value["status"] == "pending" => last_contact = Instant::now(),
            Err(e)
                if matches!(
                    e.code.as_str(),
                    "MN_RUNTIME_UNAVAILABLE" | "MN_RUNTIME_TIMEOUT"
                ) && last_contact.elapsed() < Duration::from_secs(30) => {}
            _ => return None,
        }
        sleep(Duration::from_secs(1)).await;
    }
}
fn schedule(v: &Value) -> Result<Option<Value>> {
    let schedule = v["schedule"].as_str();
    let auto = v["auto_schedule"] == true;
    if (auto && schedule.is_some()) || ((auto || schedule.is_some()) && v["queue"] == true) {
        return Err(Error::invalid(
            "Choose one of --queue, --auto-schedule and --schedule.",
        ));
    }
    let Some(raw) = schedule.or(auto.then_some("auto")) else {
        return Ok(None);
    };
    let raw = raw.trim();
    if raw.is_empty() {
        return Err(Error::invalid("Empty schedule."));
    }
    if raw.starts_with('{') {
        return serde_json::from_str(raw)
            .map(Some)
            .map_err(|_| Error::invalid("Invalid schedule JSON."));
    }
    let by = if auto {
        "mn run --auto-schedule"
    } else {
        "mn run --schedule"
    };
    let lower = raw.to_ascii_lowercase();
    if matches!(
        lower.as_str(),
        "auto" | "resource" | "resource_wait" | "resource-wait"
    ) {
        let ms = std::env::var("MN_RESOURCE_WAIT_RETRY_MS")
            .unwrap_or_else(|_| "30000".into())
            .parse::<u64>()
            .map_err(|_| Error::invalid("Invalid retry interval."))?;
        return Ok(Some(
            json!({"kind":"resource_wait","retry_interval_ms":ms,"metadata":{"requested_by":by}}),
        ));
    }
    let (num, unit) = [
        ("ms", 1.),
        ("s", 1000.),
        ("m", 60000.),
        ("h", 3600000.),
        ("d", 86400000.),
    ]
    .into_iter()
    .find_map(|(s, m)| lower.strip_suffix(s).map(|n| (n, m)))
    .unwrap_or((&lower, 1000.));
    let mut result = json!({"kind":"delayed","metadata":{"requested_by":by}});
    if let Ok(n) = num.parse::<f64>()
        && n >= 0.
        && n.is_finite()
    {
        result["delay_ms"] = json!((n * unit) as u64);
    } else {
        result["run_at"] = raw.into();
    }
    Ok(Some(result))
}
fn config_section<'a>(value: &'a mut Value, name: &str) -> Result<&'a mut Value> {
    let section = &mut value[name];
    if section.is_null() {
        *section = json!({});
    }
    if !section.is_object() {
        return Err(Error::invalid(&format!(
            "The {name} configuration must be an object."
        )));
    }
    Ok(section)
}

fn run_body(v: &Value) -> Result<Value> {
    let mut config = json!({});
    for value in v["set_values"].as_array().into_iter().flatten() {
        let (path, text) = value
            .as_str()
            .and_then(|s| s.split_once('='))
            .ok_or_else(|| Error::invalid("--set requires dotted.path=value."))?;
        let mut dest = &mut config;
        for segment in path.trim().split('.') {
            if segment.is_empty() {
                return Err(Error::invalid("Empty --set path segment."));
            }
            if !dest.is_object() {
                *dest = json!({});
            }
            dest = &mut dest[segment];
        }
        *dest = serde_json::from_str(text).unwrap_or_else(|_| json!(text));
    }
    for flag in ["benchmark", "debug", "fake_skills"] {
        if v[flag] == true {
            config_section(&mut config, "execution")?[flag] = true.into();
        }
    }
    for (flag, k) in [("web_ui_host", "host"), ("web_ui_port", "port")] {
        if !v[flag].is_null() {
            let ui = config_section(&mut config, "web_ui")?;
            config_section(ui, "service")?[k] = v[flag].clone();
        }
    }
    let mut body = pick(
        v,
        &[
            "run_id",
            "job_id",
            "owner_node",
            "force",
            "fake_llm",
            "fake_skills",
            "replace_existing_run",
        ],
    );
    body["admission_policy"] = policy(v).into();
    body["config_overrides"] = config;
    if let Some(s) = schedule(v)? {
        body["schedule"] = s;
    }
    if v["replace_existing_run"] == true {
        if !v["job_id"].is_string() {
            return Err(Error::invalid("--replace-existing-run requires --job-id."));
        }
        confirm(v, "Permanently replace this service run and its history")?;
    }
    Ok(body)
}
pub async fn execute(a: &App, c: &[&str], v: &Value) -> Result<Value> {
    let action = c[1];
    if action == "list" {
        return a.ipc.catalog(v["blueprint_repo"].as_str(), None).await;
    }
    if action == "export" {
        let dest = v["output"]
            .as_str()
            .map(|v| connection::expand(Path::new(v)));
        return a
            .download(
                "export_run",
                json!({"run_id":v["run_id"],"format":v["output_format"]}),
                dest.as_deref(),
            )
            .await;
    }
    let id = key();
    let target = ["target", "blueprint_id", "bundle_path"]
        .iter()
        .find_map(|k| v[k].as_str())
        .unwrap_or_default();
    if matches!(action, "remove" | "update") && target.is_empty() && v["all_"] != true {
        return Err(Error::invalid("Supply a blueprint ID or --all."));
    }
    if matches!(action, "remove" | "cleanup") && v["dry_run"] != true {
        confirm(v, "Remove blueprint resources")?;
    }
    let local_required = local_source(target)
        || ["source", "blueprint_repo", "revision"]
            .iter()
            .any(|k| v[k].is_string())
        || v["offline"] == true
        || (action == "remove" && v["all_"] == true)
        || (action == "cleanup"
            && (!target.is_empty()
                || ["include_files", "include_dead", "include_docker"]
                    .iter()
                    .any(|k| v[k] == false)));
    let body = if action == "run" {
        run_body(v)?
    } else {
        Value::Null
    };
    let scheduled = !body["schedule"].is_null();
    let mut result = if local_required {
        let mut opts = v.clone();
        let opts_map = opts.as_object_mut().unwrap();
        if action != "doctor" {
            opts_map.remove("json_output");
        }
        if !matches!(action, "run" | "doctor") {
            opts_map.remove("debug");
            if opts_map.get("blueprint_repo").is_some_and(Value::is_null) {
                opts_map.remove("blueprint_repo");
            }
        }
        for k in ["target", "bundle_path", "source", "blueprint_repo"] {
            if let Some(p) = opts[k].as_str()
                && !p.contains("://")
                && !p.starts_with("git@")
                && (p.starts_with(['.', '~', '/']) || Path::new(p).exists())
            {
                opts[k] = json!(absolute(p)?);
            }
        }
        local(a, action, opts, body, &id).await?
    } else {
        let mut q = json!({"blueprint_id":target});
        let op = match action {
            "show" => "get_blueprint",
            "add" => {
                q["request"] = pick(v, &["force"]);
                "create_blueprint_addition"
            }
            "validate" | "doctor" => {
                q["request"] = json!({});
                "create_blueprint_validation"
            }
            "remove" => {
                q["request"] = pick(
                    v,
                    &["keep_resources", "keep_models", "remove_models", "dry_run"],
                );
                "create_blueprint_removal"
            }
            "cleanup" => {
                q = json!({"request":pick(v,&["dry_run"])});
                "create_blueprint_cleanup"
            }
            "update" => {
                q = json!({});
                "create_blueprint_catalog_refresh"
            }
            "run" => {
                if v["update"] == true {
                    let r = a
                        .get(
                            "create_blueprint_catalog_refresh",
                            json!({"idempotency_key":key()}),
                        )
                        .await?;
                    crate::operations::watch(a, r, true).await?;
                }
                q["request"] = body;
                "create_blueprint_run"
            }
            _ => return Err(Error::invalid("Unknown blueprint command.")),
        };
        if !matches!(action, "validate" | "doctor" | "show") {
            q["idempotency_key"] = id.clone().into();
        }
        a.get(op, q).await?
    };
    if action == "run" {
        result["idempotency_key"] = id.into();
        if v["web_ui"] == true && result["job_id"].is_string() {
            result["web_ui"] = a.get("get_job_ui", pick(&result, &["job_id"])).await?;
        }
        if !scheduled
            && v["detached"] != true
            && let Some(run) = result["run_id"].as_str()
        {
            runs::watch(a, run, v["follow_seconds"].as_f64()).await?;
        }
    }
    Ok(result)
}
