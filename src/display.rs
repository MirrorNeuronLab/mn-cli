use serde_json::Value;

fn text(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(s) => s.clone(),
        _ => value.to_string(),
    }
}

fn fields(value: &Value) {
    if let Some(object) = value.as_object() {
        for (key, item) in object {
            println!("  {:<16}  {}", key.replace('_', " "), text(item));
        }
    } else {
        println!("{}", text(value));
    }
}

pub fn print(path: &[String], value: &Value) {
    if let Some(message) = value["admission"]["message"].as_str() {
        println!("{message}");
    }
    if value["admission"]["state"] == "queued" {
        println!("Run queued");
        for key in ["job_id", "run_id", "status"] {
            if let Some(item) = value.get(key) {
                println!("  {:<16}  {}", key.replace('_', " "), text(item));
            }
        }
        return;
    }
    let command = path.join(" ");
    println!("{command}");
    let Some(items) = value["items"].as_array() else {
        fields(value);
        return;
    };
    if items.is_empty() {
        println!("No items found.");
        return;
    }
    let candidates: &[&str] = match command.as_str() {
        "blueprint list" => &["id", "name", "tagline", "type"],
        "job integrations list" => &["provider", "authorization", "delivery", "channel_name"],
        "job skills list" => &["name", "type", "source", "version", "ref", "enabled"],
        "job mailbox list" => &["sequence", "id", "sender_job_id", "subject", "status"],
        _ => &[
            "id",
            "blueprint_id",
            "job_id",
            "run_id",
            "node_name",
            "node",
            "name",
            "model",
            "status",
            "level",
            "message",
            "type",
        ],
    };
    let columns: Vec<_> = candidates
        .iter()
        .copied()
        .filter(|key| items.iter().any(|row| row.get(key).is_some()))
        .collect();
    if columns.is_empty() {
        for item in items {
            fields(item);
        }
        return;
    }
    let mut rows = vec![
        columns
            .iter()
            .map(|key| key.replace('_', " "))
            .collect::<Vec<_>>(),
    ];
    rows.extend(
        items
            .iter()
            .map(|item| columns.iter().map(|key| text(&item[key])).collect()),
    );
    let widths: Vec<_> = (0..columns.len())
        .map(|i| {
            rows.iter()
                .map(|row| row[i].chars().count())
                .max()
                .unwrap_or(0)
        })
        .collect();
    for row in rows {
        let cells: Vec<_> = row
            .iter()
            .zip(&widths)
            .map(|(cell, width)| format!("{cell:<width$}"))
            .collect();
        println!("{}", cells.join("  ").trim_end());
    }
}
