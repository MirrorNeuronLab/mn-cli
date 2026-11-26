//! Follow a durable preparation, including stages reported before a run exists.
use crate::{
    client::{
        App,
        events::{self, Feed},
    },
    output::redact,
    runs,
};
use futures_util::{StreamExt, stream};
use mn_client_support::{Error, Result};
use serde_json::{Value, json};
use std::time::Duration;

fn progress_line(operation: &Value) -> String {
    let mut progress = operation["progress"].clone();
    redact(&mut progress);
    let mut words = Vec::new();
    for key in ["stage", "label", "detail"] {
        if let Some(s) = progress[key].as_str().filter(|s| !s.is_empty()) {
            words.push(
                s.chars()
                    .filter(|ch| !ch.is_control())
                    .take(180)
                    .collect::<String>(),
            );
        }
    }
    if let Some(p) = progress["percent"]
        .as_f64()
        .filter(|p| p.is_finite() && (0.0..=100.).contains(p))
    {
        words.push(format!("{p}%"));
    }
    let status = operation["status"].as_str().unwrap_or("pending");
    if words.is_empty() {
        format!("Preparation · {status}")
    } else {
        format!("Preparation · {status} · {}", words.join(" · "))
    }
}

fn finished(operation: Value) -> Result<Value> {
    if matches!(operation["status"].as_str(), Some("failed" | "cancelled")) {
        let mut detail = String::from("The remote operation did not complete successfully.");
        if let Some(message) = operation.pointer("/error/detail").and_then(Value::as_str) {
            detail = message.to_string();
        }
        Err(Error::new(409, "MN_REMOTE_OPERATION_FAILED", &detail)
            .hint("Inspect the remote operation for validation and preparation errors."))
    } else {
        Ok(operation)
    }
}

pub(crate) async fn watch(app: &App, initial: Value, stream_json: bool) -> Result<Value> {
    if runs::terminal(&initial) {
        return finished(initial);
    }
    let id = initial["operation_id"]
        .as_str()
        .ok_or_else(Error::protocol)?
        .to_owned();
    let (pinned, _) = app.pin().await?;
    let pinned = &pinned;
    let id = id.as_str();
    let polls = async_stream::try_stream! {
        yield initial;
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            yield pinned.get("get_operation", json!({"operation_id": id})).await?;
        }
    };
    let events = events::follow(pinned, Feed::Operation, id).filter_map(|event| async move {
        event
            .data
            .get("status")
            .is_some()
            .then_some(Ok::<Value, Error>(event.data))
    });
    let updates = stream::select(polls, events);
    futures_util::pin_mut!(updates);
    let mut previous = Value::Null;
    let mut previous_line = String::new();
    loop {
        tokio::select! {
            Some(item) = updates.next() => {
                let operation: Value = item?;
                if runs::terminal(&operation) {
                    return finished(operation);
                }
                if previous != operation {
                    if app.json {
                        if stream_json {
                            app.emit("progress", operation.clone());
                        }
                    } else {
                        let line = progress_line(&operation);
                        if line != previous_line {
                            println!("{line}");
                            previous_line = line;
                        }
                    }
                    previous = operation;
                }
            }
            _ = tokio::signal::ctrl_c() => return Ok(json!({"operation_id": id, "detached": true})),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn renders_reported_stage_before_the_run_is_created() {
        let line = progress_line(
            &json!({"status":"running","progress":{"stage":"prepare_bundle","label":"Preparing OpenShell sandbox","percent":23,"diagnostic":"private"}}),
        );
        assert!(
            line.contains("Preparing OpenShell sandbox")
                && line.contains("23%")
                && !line.contains("private")
        );
    }
}
