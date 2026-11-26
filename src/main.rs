mod blueprints;
mod client;
mod display;
mod management;
mod parser;
mod runs;

use chrono::Utc;
use client::App;
use mn_client_support::{Error, Result, connection};
use serde_json::{Value, json};
use std::{
    io::{self, IsTerminal, Write},
    path::Path,
    time::Instant,
};
fn pick(v: &Value, keys: &[&str]) -> Value {
    Value::Object(
        keys.iter()
            .filter_map(|k| {
                v.get(*k)
                    .filter(|v| !v.is_null())
                    .map(|v| (k.to_string(), v.clone()))
            })
            .collect(),
    )
}
fn s<'a>(v: &'a Value, k: &str) -> &'a str {
    v[k].as_str().unwrap_or_default()
}
fn confirm(v: &Value, action: &str) -> Result<()> {
    if v["yes"] == true {
        return Ok(());
    }
    if v["json_output"] != true && io::stdin().is_terminal() {
        eprint!("{action}? [y/N] ");
        let _ = io::stderr().flush();
        let mut answer = String::new();
        if io::stdin().read_line(&mut answer).is_ok()
            && matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes")
        {
            return Ok(());
        }
    }
    let mut error = Error::new(
        422,
        "MN_CONFIRMATION_REQUIRED",
        &format!("{action} requires --yes."),
    );
    error.exit_code = 2;
    Err(error)
}
fn read_object(v: &Value, k: &str) -> Result<Value> {
    if let Some(p) = v[k].as_str() {
        std::fs::read(connection::expand(Path::new(p)))
            .ok()
            .and_then(|b| serde_json::from_slice::<Value>(&b).ok())
            .filter(Value::is_object)
            .ok_or_else(|| Error::invalid("Supply a readable JSON object file."))
    } else {
        Ok(json!({}))
    }
}
fn key() -> String {
    uuid::Uuid::new_v4().to_string()
}
fn unsupported(msg: &str) -> Error {
    Error::new(422, "MN_CLIENT_UNSUPPORTED", msg)
}
fn redact(v: &mut Value) {
    use regex::Regex;
    use std::sync::LazyLock;
    static KEY: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"(?i)(authorization|bearer|cookie|credential|password|secret|(^|[\s_-])token($|[\s_-])|(^|[\s_-])api[\s_-]?key($|[\s_-]))").unwrap()
    });
    static TEXT: LazyLock<[Regex; 3]> = LazyLock::new(|| {
        [
        r"(?i)(bearer\s+)[A-Za-z0-9._~+/=-]+",
        r"(?i)\b([A-Z0-9_]*(?:TOKEN|SECRET|PASSWORD|API_KEY|COOKIE|CREDENTIAL)[A-Z0-9_]*=)[^\s]+",
        r"(?i)([?&](?:token|access_token|api_key|key|signature|password|secret)=)[^&\s]+"
    ].map(|r|Regex::new(r).unwrap())
    });
    match v {
        Value::Object(map) => {
            for (key, field) in map {
                if KEY.is_match(key)
                    && !key.to_ascii_lowercase().ends_with("_env")
                    && !field.is_null()
                    && !field
                        .as_str()
                        .is_some_and(|v| v.is_empty() || v.starts_with("os.environ/"))
                {
                    *field = json!("[redacted]");
                } else {
                    redact(field);
                }
            }
        }
        Value::Array(a) => a.iter_mut().for_each(redact),
        Value::String(s) => {
            for re in TEXT.iter() {
                if re.is_match(s) {
                    *s = re.replace_all(s, "${1}[redacted]").into_owned();
                }
            }
        }
        _ => {}
    }
}
fn replacement(argv: &[std::ffi::OsString]) -> Option<&'static str> {
    let mut path = argv
        .iter()
        .skip(1)
        .filter_map(|s| s.to_str())
        .filter(|s| !s.starts_with('-'));
    Some(match (path.next()?, path.next()?) {
        ("blueprint", "install") => "mn blueprint add",
        ("blueprint", "uninstall") => "mn blueprint remove",
        ("blueprint", "monitor") => "mn run list --follow",
        ("blueprint", "tail") => "mn run logs RUN_ID --channel events",
        ("blueprint", "logs") => "mn run logs RUN_ID --channel logs",
        ("blueprint", "stream") => "mn run logs RUN_ID --channel all --follow",
        ("blueprint", "resources") => "mn run resources RUN_ID",
        ("blueprint", "compare") => "mn run compare RUN_A RUN_B",
        ("job", "inspect") => "mn job show",
        ("job", "runs") => "mn run list --job JOB_ID",
        ("run", "status") => "mn run show",
        ("run", "monitor") => "mn run watch",
        ("runtime", "health") => "mn runtime status (or mn runtime doctor for diagnostics)",
        ("runtime", "metrics") => "mn resource usage",
        ("runtime", "update") => "mn runtime upgrade",
        ("node", "join") => "mn node add HOST --token TOKEN",
        ("node", "expose") => "mn runtime start",
        ("node", "leave") => "mn node remove",
        ("operation", "status") => "mn operation show",
        ("resource", "list") => "mn resource show",
        ("resource", "ports") => "mn runtime status",
        ("service", "resolve") => "mn service show",
        ("service", "check") => "mn blueprint doctor",
        ("model", "install") => "mn model add",
        ("model", "proxy") => "mn model add --file DEFINITION.json",
        ("model", "remote") => "mn model add --file DEFINITION.json",
        _ => return None,
    })
}
#[tokio::main(flavor = "current_thread")]
async fn main() {
    let spec = parser::Group::root();
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
            std::process::exit(error.exit_code());
        }
        Err(error) => {
            if error.use_stderr()
                && let Some(command) = replacement(&argv)
            {
                eprintln!("This command was removed. Use {command}.");
                std::process::exit(error.exit_code());
            }
            error.exit()
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
            _ => management::execute(&a, &names, &v).await,
        };
        if a.streaming.get() {
            match result {
                Ok(data) => {
                    a.emit("complete", data);
                    return Ok(None);
                }
                Err(e) => {
                    a.emit("error", json!({"code":e.code,"message":e.message}));
                    std::process::exit(e.exit_code);
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
            if v["json_output"] == true {
                let mut error =
                    json!({"code":e.code,"message":e.message,"hint":e.hint,"details":e.details});
                redact(&mut error);
                println!(
                    "{}",
                    json!({"schema":"mn.cli/v1","ok":false,"error":error,"warnings":[],"meta":meta})
                );
            } else {
                eprintln!("× Error: {e}");
                if let Some(h) = e.hint {
                    eprintln!("{h}");
                }
            }
            std::process::exit(e.exit_code);
        }
    }
}
