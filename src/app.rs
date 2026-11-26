use crate::{
    blueprints,
    client::App,
    input::{confirm, pick},
    runs,
};
use crate::{config, display, error::replacement, jobs, nodes, output::redact, runtime};
use chrono::Utc;
use mn_client_support::{Error, Result, connection};
use serde_json::{Value, json};
use std::time::Instant;
pub async fn run() {
    #[cfg(target_os = "wasi")]
    if let Err(e) = mn_client_support::wasi::enter_cwd() {
        eprintln!("mn: {e}");
        exit(1);
    }
    let spec = config::Group::root();
    let mut cmd = spec.build();
    let argv: Vec<_> = std::env::args_os().collect();
    let json_output = argv.iter().any(|arg| arg == "--json");
    let matches = match cmd.clone().try_get_matches_from(&argv) {
        Ok(m) => m,
        Err(error) if json_output && error.use_stderr() => {
            let mut detail = json!(replacement(&argv).map_or_else(
                || error.to_string(),
                |command| format!("This command was removed. Use {command}.")
            ));
            redact(&mut detail);
            println!(
                "{}",
                json!({"schema":"mn.cli/v1","ok":false,"error":{"code":"MN_VALIDATION_FAILED","message":detail,"details":{}},"warnings":[],"meta":{"command":"mn","timestamp":Utc::now().to_rfc3339(),"duration_ms":0}})
            );
            exit(error.exit_code());
        }
        Err(error) => {
            if error.use_stderr()
                && let Some(command) = replacement(&argv)
            {
                eprintln!("This command was removed. Use {command}.");
                exit(error.exit_code());
            }
            let _ = error.print();
            exit(error.exit_code())
        }
    };
    let mut selected = &matches;
    let mut branch = &mut cmd;
    while let Some((name, m)) = selected.subcommand() {
        branch = branch.find_subcommand_mut(name).unwrap();
        selected = m;
    }
    if branch.has_subcommands() {
        branch.print_help().unwrap();
        println!();
        return;
    }
    let start = Instant::now();
    let mut parts = vec![];
    let mut v = json!({"json_output":json_output});
    let result = async {
        spec.values(&matches, &mut parts, &mut v)
            .map_err(|e| Error::invalid(&e))?;
        let a = App::new(connection::home()?, v["json_output"] == true);
        let names = parts.iter().map(String::as_str).collect::<Vec<_>>();
        let result = match names[0] {
            "run" | "operation" => runs::execute(&a, &names, &v).await,
            "blueprint" => blueprints::execute(&a, &names, &v).await,
            _ => execute(&a, &names, &v).await,
        };
        if a.streaming.get() {
            match result {
                Ok(data) => {
                    a.emit("complete", data);
                    return Ok(None);
                }
                Err(e) => {
                    a.emit("error", json!({"code":e.code,"message":e.message}));
                    exit(e.exit_code);
                }
            }
        }
        result.map(Some)
    }
    .await;
    let meta = json!({"command":format!("mn {}",parts.join(" ")),"timestamp":Utc::now().to_rfc3339(),"duration_ms":start.elapsed().as_millis()});
    match result {
        Ok(Some(mut data)) => {
            if data.is_array() {
                data = json!({"items":data});
            }
            if data.is_null() {
                data = json!({"status":"complete"});
            }
            redact(&mut data);
            if v["json_output"] == true {
                println!(
                    "{}",
                    json!({"schema":"mn.cli/v1","ok":true,"data":data,"warnings":[],"meta":meta})
                );
            } else {
                display::print(&parts, &data);
            }
        }
        Ok(None) => {}
        Err(e) => {
            let mut error =
                json!({"code":e.code,"message":e.message,"hint":e.hint,"details":e.details});
            redact(&mut error);
            if v["json_output"] == true {
                println!(
                    "{}",
                    json!({"schema":"mn.cli/v1","ok":false,"error":error,"warnings":[],"meta":meta})
                );
            } else {
                let message = error["message"].as_str().unwrap_or_default();
                eprintln!("× Error: {}: {message}", e.code);
                if let Some(hint) = error["hint"].as_str()
                    && !message.ends_with(hint)
                {
                    eprintln!("{hint}");
                }
                let debug = matches.get_flag("debug")
                    || v["debug"] == true
                    || std::env::var("MN_DEBUG").is_ok_and(|value| {
                        matches!(
                            value.trim().to_ascii_lowercase().as_str(),
                            "1" | "true" | "yes" | "on"
                        )
                    });
                if debug
                    && error["details"]
                        .as_object()
                        .is_some_and(|fields| !fields.is_empty())
                {
                    eprintln!("{}", error["details"]);
                }
            }
            exit(e.exit_code);
        }
    }
}

async fn execute(a: &App, c: &[&str], v: &Value) -> Result<Value> {
    let action = c[1];
    match c[0] {
        "node" => nodes::execute(a, action, v).await,
        "runtime" => runtime::execute(a, action, v).await,
        "job" => jobs::execute(a, &c[1..], v).await,
        "model" => {
            let mut opts = v.clone();
            if let Some(options) = opts.as_object_mut() {
                options.remove("debug");
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

fn exit(code: i32) -> ! {
    #[cfg(target_os = "wasi")]
    {
        use std::io::Write;
        let _ = std::io::stdout().flush();
        let _ = std::io::stderr().flush();
        wasip2::cli::exit::exit_with_code(code as u8);
        unreachable!();
    }
    #[cfg(not(target_os = "wasi"))]
    std::process::exit(code)
}
