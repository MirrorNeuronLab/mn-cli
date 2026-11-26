use mn_client_support::{Error, Result, connection};
use serde_json::{Value, json};
use std::{
    io::{self, IsTerminal, Write},
    path::Path,
};
pub(crate) fn pick(v: &Value, keys: &[&str]) -> Value {
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
pub(crate) fn s<'a>(v: &'a Value, k: &str) -> &'a str {
    v[k].as_str().unwrap_or_default()
}
pub(crate) fn confirm(v: &Value, action: &str) -> Result<()> {
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
pub(crate) fn read_object(v: &Value, k: &str) -> Result<Value> {
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
pub(crate) fn key() -> String {
    uuid::Uuid::new_v4().to_string()
}
