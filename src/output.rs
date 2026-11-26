use serde_json::{Value, json};
pub(crate) fn redact(v: &mut Value) {
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stream_records_and_single_results_share_nested_secret_redaction() {
        let mut v = json!({"items":[{"password":"value","key_env":"os.environ/KEY","note":"Bearer private"}]});
        redact(&mut v);
        assert_eq!(
            v["items"][0],
            json!({"password":"[redacted]","key_env":"os.environ/KEY","note":"Bearer [redacted]"})
        );
    }
}
