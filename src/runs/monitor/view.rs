use super::preparation;
use crate::output::redact;
use serde_json::Value;
use std::collections::VecDeque;
use unicode_width::UnicodeWidthChar;

const CHILD_PAGE: usize = 4;

pub(super) fn safe_line(text: &str, width: usize) -> String {
    let mut value = Value::String(text.into());
    redact(&mut value);
    let mut used = 0;
    value
        .as_str()
        .unwrap_or_default()
        .chars()
        .filter(|ch| {
            !ch.is_control() && !matches!(*ch, '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
        })
        .take_while(|ch| {
            used += ch.width().unwrap_or(0);
            used <= width
        })
        .collect()
}

fn rows<'a>(value: &'a Value, key: &str) -> impl Iterator<Item = &'a Value> {
    value[key]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|v| v.is_object())
}
fn label(row: &Value) -> &str {
    ["label", "name", "id", "agent_id"]
        .iter()
        .find_map(|k| row[k].as_str())
        .unwrap_or("unnamed")
}
fn status(row: &Value) -> &str {
    row["status"].as_str().unwrap_or("pending")
}

#[derive(Default)]
pub(super) struct View {
    pub run: Value,
    pub progress: Value,
    feed: VecDeque<String>,
    pub agent: usize,
    pub detail: bool,
    pub child_page: Option<usize>,
}

impl View {
    pub fn record(&mut self, kind: &str, event: &Value) {
        let text =
            preparation::event_message(kind, event).unwrap_or_else(|| kind.replace('_', " "));
        let line = safe_line(&text, 240);
        if !line.is_empty() && self.feed.back() != Some(&line) {
            if self.feed.len() == 5 {
                self.feed.pop_front();
            }
            self.feed.push_back(line);
        }
    }
    pub fn progress(&mut self, progress: Value) {
        if self.progress == progress {
            return;
        }
        for event in rows(&progress, "recent_events") {
            if !rows(&self.progress, "recent_events").any(|old| old == event) {
                self.record(event["type"].as_str().unwrap_or("event"), event);
            }
        }
        self.progress = progress;
    }
    fn children(&self) -> Vec<&Value> {
        rows(&self.progress, "steps")
            .filter(|s| s["parent_step_id"].is_string())
            .collect()
    }
    fn child_offset(&self) -> usize {
        let children = self.children();
        let last = children.len().saturating_sub(1);
        self.child_page
            .unwrap_or_else(|| {
                children
                    .iter()
                    .position(|s| status(s) == "running")
                    .or_else(|| {
                        children
                            .iter()
                            .position(|s| matches!(status(s), "failed" | "blocked"))
                    })
                    .unwrap_or(last)
                    / CHILD_PAGE
            })
            .min(last / CHILD_PAGE)
            * CHILD_PAGE
    }
    pub fn page_children(&mut self, delta: isize) {
        self.child_page = Some((self.child_offset() / CHILD_PAGE).saturating_add_signed(delta));
    }
    fn agents(&self) -> Vec<&Value> {
        let current = &self.progress["current_step"];
        let step = if current.is_object() && current["agents"].is_array() {
            Some(current)
        } else {
            rows(&self.progress, "steps").find(|s| {
                s["current"] == true
                    || self.progress["current_step_ids"]
                        .as_array()
                        .is_some_and(|ids| ids.contains(&s["id"]))
                    || status(s) == "running"
            })
        };
        let group = step
            .filter(|s| s["agents"].is_array())
            .unwrap_or(&self.progress);
        rows(group, "agents").collect()
    }
    pub fn move_agent(&mut self, by: isize) {
        self.agent = self
            .agent
            .saturating_add_signed(by)
            .min(self.agents().len().saturating_sub(1));
    }
    pub fn lines(&self, id: &str, width: usize, height: usize, interactive: bool) -> Vec<String> {
        let progress = &self.progress;
        let state = progress["status"]
            .as_str()
            .or(self.run["status"].as_str())
            .unwrap_or("connecting");
        let mut lines = vec![format!("Run {id}  ·  {state}")];
        if let Some(seconds) = progress["elapsed_seconds"]
            .as_f64()
            .filter(|s| s.is_finite() && *s >= 0.)
        {
            let seconds = seconds as u64;
            lines[0].push_str(&format!("  ·  {}:{:02}", seconds / 60, seconds % 60));
        }
        if let Some(name) = progress["workflow_id"]
            .as_str()
            .or(self.run["blueprint_id"].as_str())
        {
            lines.push(format!("Workflow  {name}"));
        }
        // Reserve space above the step list for live preparation and the latest event.
        for item in rows(progress, "preparations").take(if interactive { 3 } else { usize::MAX }) {
            lines.push(preparation::message(item, ""));
        }
        if let Some(warning) = progress["monitor_warning"].as_str() {
            lines.push(format!("Warning  {warning}"));
        }
        if let Some(event) = self.feed.back() {
            lines.push(format!("Latest  {event}"));
        }
        let steps: Vec<_> = rows(progress, "steps")
            .filter(|s| !s["parent_step_id"].is_string())
            .collect();
        let done = steps
            .iter()
            .filter(|s| matches!(status(s), "completed" | "done" | "skipped"))
            .count();
        if !steps.is_empty() {
            lines.push(format!("Steps  {done}/{} complete", steps.len()));
            let visible = if interactive {
                (height / 4).clamp(1, 8)
            } else {
                steps.len()
            };
            let focus = steps
                .iter()
                .position(|step| step["current"] == true || status(step) == "running")
                .unwrap_or(0);
            let start = focus
                .saturating_sub(visible / 2)
                .min(steps.len().saturating_sub(visible));
            for step in steps.iter().skip(start).take(visible) {
                let counts = step["total_count"]
                    .as_u64()
                    .map(|n| format!("  {}/{n}", step["done_count"].as_u64().unwrap_or(0)))
                    .unwrap_or_default();
                lines.push(format!("  {:<10} {}{counts}", status(step), label(step)));
            }
        }
        let children = self.children();
        if !children.is_empty() {
            let start = self.child_offset();
            lines.push(format!(
                "Sub-workflow steps  {}–{} of {}",
                start + 1,
                (start + CHILD_PAGE).min(children.len()),
                children.len()
            ));
            for child in children.into_iter().skip(start).take(CHILD_PAGE) {
                lines.push(format!("  {:<10} {}", status(child), label(child)));
            }
        }
        let agents = self.agents();
        let selected = self.agent.min(agents.len().saturating_sub(1));
        if !agents.is_empty() {
            lines.push(format!("Agents  {}", agents.len()));
            for (i, agent) in agents
                .iter()
                .enumerate()
                .skip(selected.saturating_sub(2))
                .take(6)
            {
                if self.detail && i != selected {
                    continue;
                }
                let pointer = if i == selected { ">" } else { " " };
                let measured = agent["progress"]
                    .as_f64()
                    .filter(|v| (0.0..=1.0).contains(v))
                    .map(|v| format!("  {:.0}%", v * 100.))
                    .unwrap_or_default();
                lines.push(format!(
                    "{pointer} {:<10} {}{measured}",
                    status(agent),
                    label(agent)
                ));
                for prep in rows(agent, "preparations") {
                    lines.push(format!("    {}", preparation::message(prep, "")));
                }
                if self.detail {
                    for key in ["activity_summary", "last_activity", "node", "step_id"] {
                        if let Some(value) = agent[key].as_str() {
                            lines.push(format!("    {key}  {value}"));
                        }
                    }
                }
            }
        }
        if interactive && self.feed.len() > 1 && height.saturating_sub(lines.len()) > 3 {
            lines.push("Recent events".into());
            lines.extend(
                self.feed
                    .iter()
                    .rev()
                    .skip(1)
                    .take(3)
                    .map(|s| format!("  {s}")),
            );
        }
        if interactive && height > 1 {
            lines.truncate(height.saturating_sub(1));
            lines.push("↑↓ agents   Tab detail   [ ] child pages   f follow   q detach".into());
        }
        lines
            .into_iter()
            .take(height)
            .map(|s| safe_line(&s, width))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn bounds_and_sanitizes_untrusted_terminal_text() {
        assert_eq!(safe_line("a\x1b\n界z", 3), "a界");
        assert!(!safe_line("Bearer privacy-test", 80).contains("privacy-test"));
    }
    #[test]
    fn follows_active_children_and_keeps_the_latest_event_visible() {
        let mut view = View::default();
        let children:Vec<_>=(0..9).map(|i|json!({"id":format!("step-{i}"),"parent_step_id":"p","status":if i==5{"running"}else{"pending"}})).collect();
        view.progress(json!({"steps":children,"preparations":[{"kind":"model","phase":"downloading","percent":32}]}));
        view.record(
            "openshell.preparation.started",
            &json!({"stage":"gateway_setup"}),
        );
        let output = view.lines("r", 90, 15, true).join("\n");
        assert!(
            output.contains("5–8 of 9")
                && output.contains("32%")
                && output.contains("preparing the gateway")
        );
        view.page_children(-1);
        assert!(
            view.lines("r", 90, 15, true)
                .join("\n")
                .contains("1–4 of 9")
        );
    }
}
