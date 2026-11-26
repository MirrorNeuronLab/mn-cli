//! Presentation of public preparation telemetry. Model identifiers and worker diagnostics stay private.
use serde_json::Value;

fn number(value: &Value) -> Option<f64> {
    value.as_f64().filter(|n| n.is_finite() && *n >= 0.)
}

pub(super) fn message(data: &Value, kind: &str) -> String {
    if kind.to_ascii_lowercase().contains("openshell") || data["kind"] == "openshell" {
        let stage = data["stage"].as_str().unwrap_or("");
        let task = match stage {
            "client_install" => "installing the CLI",
            "gateway_setup" => "preparing the gateway",
            "image_build" => "building the sandbox image",
            "gateway_connection" => "connecting to the gateway",
            "sandbox_create" => "creating the sandbox",
            _ => "preparing the sandbox",
        };
        return match (kind, data["status"].as_str()) {
            (k, _) if k.to_ascii_lowercase().contains("fail") => {
                format!("OpenShell failed while {task}")
            }
            (_, Some("failed")) => format!("OpenShell failed while {task}"),
            (_, Some("completed" | "ready")) => "OpenShell sandbox ready".into(),
            _ => format!("OpenShell · {task}"),
        };
    }
    let subject = match data["kind"].as_str() {
        Some("dependencies") => "dependencies",
        Some("service") => "service",
        _ => match data["purpose"].as_str() {
            Some("ocr") => "OCR model",
            Some("asr" | "stt" | "transcription") => "transcription model",
            Some("embeddings" | "rag" | "embedding") => "embedding model",
            Some("vision") => "vision model",
            Some("speech") => "speech model",
            Some("llm") => "language model",
            _ => "model",
        },
    };
    let phase = data["phase"].as_str().unwrap_or_default();
    let verb = match kind {
        "runtime_model_ready" => "Prepared",
        "runtime_model_install_failed" => "Couldn't prepare",
        "runtime_model_wait_finished" => "Finished waiting for",
        "runtime_preparation_finished" => match data["status"].as_str() {
            Some("completed") => "Prepared",
            Some("cancelled") => "Stopped preparing",
            _ => "Couldn't prepare",
        },
        _ => match phase {
            "ready" if !kind.ends_with("_progress") => "Prepared",
            "downloading" => "Downloading",
            "loading" | "starting" => "Loading",
            "waiting" => "Waiting for",
            "failed" => "Couldn't prepare",
            "cancelled" => "Stopped preparing",
            _ => "Preparing",
        },
    };
    let mut label = format!("{verb} the {subject}");
    if matches!(
        (phase, verb),
        ("downloading", "Downloading") | ("loading", "Loading")
    ) {
        let percent = number(&data["percent"]).or_else(|| {
            let total = number(&data["total_bytes"])?;
            (total > 0.)
                .then(|| number(&data["current_bytes"]).map(|current| current / total * 100.))
                .flatten()
        });
        if let Some(percent) = percent {
            let rounded = (percent.min(100.) * 10.).round() / 10.;
            label.push_str(&format!(" ({rounded}%)"));
        }
    }
    label
}

pub(super) fn event_message(kind: &str, event: &Value) -> Option<String> {
    let payload = event.get("payload").unwrap_or(event);
    if kind.starts_with("runtime_model_")
        || kind.starts_with("runtime_preparation_")
        || kind.to_ascii_lowercase().contains("openshell")
    {
        Some(message(payload, kind))
    } else if kind == "submission_storage_waiting" {
        let node = payload["node"].as_str().unwrap_or("the execution node");
        Some(format!("Waiting for staged inputs on {node}"))
    } else {
        event["message"]
            .as_str()
            .or(payload["message"].as_str())
            .map(str::to_owned)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn percentages_use_measurements_only_during_transfer() {
        let mut model = json!({"model":"private-id","purpose":"rag","phase":"downloading","current_bytes":3,"total_bytes":10});
        assert_eq!(message(&model, ""), "Downloading the embedding model (30%)");
        model["percent"] = json!(110);
        assert!(message(&model, "").ends_with("(100%)"));
        model["phase"] = json!("starting");
        assert_eq!(message(&model, ""), "Loading the embedding model");
        assert_eq!(
            message(&model, "runtime_model_ready"),
            "Prepared the embedding model"
        );
    }
    #[test]
    fn openshell_exposes_only_public_stage_copy() {
        let event = json!({"payload":{"stage":"image_build","diagnostic":"private log"}});
        assert_eq!(
            event_message("openshell.preparation.failed", &event).as_deref(),
            Some("OpenShell failed while building the sandbox image")
        );
    }
}
