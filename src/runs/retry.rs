use crate::{
    blueprints,
    client::App,
    input::{key, pick},
};
use mn_client_support::{Error, Result};
use serde_json::{Value, json};

fn settings(values: &Value) -> Result<Value> {
    let mut overrides = json!({});
    for item in values.as_array().into_iter().flatten() {
        let (path, text) = item
            .as_str()
            .and_then(|s| s.split_once('='))
            .ok_or_else(|| Error::invalid("Retry settings must use path=value."))?;
        for segment in path.split('.') {
            if !segment.chars().any(char::is_alphanumeric)
                || !segment.chars().all(|ch| ch == '_' || ch.is_alphanumeric())
            {
                return Err(Error::invalid("Invalid retry setting path."));
            }
        }
        if overrides.get(path).is_some() {
            return Err(Error::invalid("Duplicate retry setting."));
        }
        let value: Value = serde_json::from_str(text).unwrap_or_else(|_| json!(text));
        if !(value.is_string() || value.is_i64() || value.is_u64()) {
            return Err(Error::invalid(
                "Retry settings must be integers or strings.",
            ));
        }
        overrides[path] = value;
    }
    Ok(overrides)
}

pub(super) async fn execute(app: &App, mut query: Value, args: &Value) -> Result<Value> {
    let overrides = settings(&args["settings"])?;
    let mut body = pick(args, &["expected_attempt", "checkpoint_revision"]);
    let supplied_key = args["idempotency_key"].as_str().filter(|s| !s.is_empty());
    if args["expected_attempt"].is_null() != args["checkpoint_revision"].is_null()
        || (!args["expected_attempt"].is_null()
            && (supplied_key.is_none() || args["dry_run"] == true))
    {
        return Err(Error::invalid(
            "Supply the expected attempt, checkpoint revision and idempotency key together.",
        ));
    }

    let (runtime, _) = app.pin().await?;
    if body == json!({}) {
        query["request"] = json!({"configuration_overrides": overrides});
        let plan = runtime.get("plan_run_retry", query.clone()).await?;
        if args["dry_run"] == true {
            return Ok(plan);
        }
        if plan["eligible"] != true {
            let mut error = Error::new(
                409,
                "MN_REMOTE_RETRY_UNAVAILABLE",
                "The remote run is not eligible for retry.",
            );
            *error.details = plan;
            return Err(error);
        }
        body = pick(&plan, &["expected_attempt", "checkpoint_revision"]);
        if body.as_object().is_none_or(|fields| fields.len() != 2) {
            return Err(Error::protocol());
        }
    }
    body["configuration_overrides"] = overrides;
    body["admission_policy"] = blueprints::policy(args).into();
    let key = supplied_key.map(str::to_owned).unwrap_or_else(key);
    let mut selection = body.clone();
    selection["idempotency_key"] = json!(key);
    app.emit("retry_selection", selection);
    query["request"] = body;
    query["idempotency_key"] = json!(key);
    let mut result = runtime.get("retry_run", query).await?;
    result["idempotency_key"] = key.into();
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_settings_preserve_flat_keys_and_scalar_types() {
        assert_eq!(
            settings(&json!(["agent_2.retries=3", "model.name=a=b"])).unwrap(),
            json!({"agent_2.retries":3,"model.name":"a=b"})
        );
        for values in [
            json!(["_._=2"]),
            json!(["a..b=1"]),
            json!(["a=1", "a=2"]),
            json!(["a=true"]),
            json!(["a=0.5"]),
            json!(["a=[1]"]),
        ] {
            assert!(settings(&values).is_err());
        }
    }
}
