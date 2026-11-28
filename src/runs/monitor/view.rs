use super::preparation;
use crate::output::redact;
use serde_json::Value;
use std::collections::VecDeque;
use std::time::{Duration, Instant};
use unicode_width::UnicodeWidthChar;

pub(super) const CHILD_PAGE: usize = 4;

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

pub(super) fn rows<'a>(value: &'a Value, key: &str) -> impl Iterator<Item = &'a Value> {
    value[key]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|v| v.is_object())
}
pub(super) fn label(row: &Value) -> &str {
    ["label", "name", "id", "agent_id"]
        .iter()
        .find_map(|k| row[k].as_str())
        .unwrap_or("unnamed")
}
pub(super) fn status(row: &Value) -> &str {
    row["status"].as_str().unwrap_or("pending")
}

pub(super) fn complete(row: &Value) -> bool {
    matches!(status(row), "completed" | "complete" | "done" | "succeeded")
}

pub(super) fn elapsed(value: &Value) -> Option<String> {
    let seconds = value["elapsed_seconds"]
        .as_f64()
        .filter(|s| s.is_finite() && *s >= 0.0)? as u64;
    Some(format!("{}:{:02}", seconds / 60, seconds % 60))
}

pub(super) fn step_counts(steps: &[&Value], service: bool) -> (usize, String) {
    let done = steps
        .iter()
        .filter(|s| complete(s) || service && matches!(status(s), "running" | "idle"))
        .count();
    let mut text = format!(
        "{done}/{} {}",
        steps.len(),
        if service { "ready" } else { "complete" }
    );
    for (state, count) in [
        (
            "failed",
            steps
                .iter()
                .filter(|s| matches!(status(s), "failed" | "error"))
                .count(),
        ),
        (
            "skipped",
            steps.iter().filter(|s| status(s) == "skipped").count(),
        ),
    ] {
        if count > 0 {
            text.push_str(&format!(" · {count} {state}"));
        }
    }
    (done, text)
}

pub(super) fn measured(row: &Value) -> Option<f64> {
    row["progress"].as_f64().filter(|v| (0.0..=1.0).contains(v))
}

pub(super) struct EventLine {
    pub message: String,
    pub time: Option<String>,
}

fn event_time(event: &Value) -> Option<String> {
    [event, &event["payload"]].into_iter().find_map(|source| {
        ["timestamp", "ts", "time", "created_at", "occurred_at"]
            .into_iter()
            .find_map(|field| {
                let value = &source[field];
                if let Some(text) = value.as_str() {
                    return chrono::DateTime::parse_from_rfc3339(text)
                        .ok()
                        .map(|time| time.format("%H:%M:%S").to_string());
                }
                let epoch = value.as_f64()?;
                let millis = if epoch.abs() >= 10_000_000_000.0 {
                    epoch as i64
                } else {
                    (epoch * 1000.0) as i64
                };
                chrono::DateTime::from_timestamp_millis(millis)
                    .map(|time| time.format("%H:%M:%S").to_string())
            })
    })
}

#[derive(Default)]
pub(super) struct View {
    pub run: Value,
    pub progress: Value,
    pub feed: VecDeque<EventLine>,
    agent: usize,
    pub detail: bool,
    pub child_page: Option<usize>,
    observed: Option<Instant>,
    disconnected: Option<Instant>,
}

impl View {
    pub fn record(&mut self, kind: &str, event: &Value) {
        let text =
            preparation::event_message(kind, event).unwrap_or_else(|| kind.replace('_', " "));
        let line = safe_line(&text, 240);
        if !line.is_empty() && !self.feed.back().is_some_and(|event| event.message == line) {
            if self.feed.len() == 5 {
                self.feed.pop_front();
            }
            self.feed.push_back(EventLine {
                message: line,
                time: event_time(event),
            });
        }
    }
    pub fn progress(&mut self, progress: Value) -> bool {
        for key in ["workflow_updated_at", "generated_at"] {
            if let (Some(old), Some(new)) = (self.progress[key].as_str(), progress[key].as_str()) {
                if new < old {
                    return false;
                }
                if new > old {
                    break;
                }
            }
        }
        self.observed = Some(Instant::now());
        self.disconnected = None;
        if self.progress == progress {
            return false;
        }
        for event in rows(&progress, "recent_events") {
            if !rows(&self.progress, "recent_events").any(|old| old == event) {
                self.record(event["type"].as_str().unwrap_or("event"), event);
            }
        }
        self.progress = progress;
        true
    }

    pub fn disconnect(&mut self) -> bool {
        if self.disconnected.is_some() {
            return false;
        }
        self.disconnected = Some(Instant::now());
        true
    }

    pub fn stale(&self) -> bool {
        !super::super::terminal(&self.progress)
            && (self.disconnected.is_some()
                || self
                    .observed
                    .is_some_and(|at| at.elapsed() >= Duration::from_secs(5)))
    }

    pub fn step_elapsed(&self, step: &Value) -> String {
        let Some(seconds) = step["elapsed_seconds"]
            .as_f64()
            .filter(|s| s.is_finite() && *s >= 0.0)
        else {
            return "—".into();
        };
        if step["started_at"].is_null()
            && (seconds == 0. || matches!(status(step), "pending" | "skipped"))
        {
            return "—".into();
        }
        if !step["ended_at"].is_null() {
            let hundredths = (seconds * 100.).round() as u64;
            return format!(
                "{}:{:02}.{:02}",
                hundredths / 6000,
                hundredths / 100 % 60,
                hundredths % 100
            );
        }
        let ticking = step["ended_at"].is_null()
            && matches!(status(step), "running" | "active" | "retry_wait")
            && !super::super::terminal(&self.progress);
        let extra = if ticking {
            self.observed.map_or(0., |at| {
                self.disconnected
                    .unwrap_or_else(Instant::now)
                    .saturating_duration_since(at)
                    .min(Duration::from_secs(5))
                    .as_secs_f64()
            })
        } else {
            0.
        };
        let value = (seconds + extra) as u64;
        format!("{}:{:02}", value / 60, value % 60)
    }
    pub fn state(&self) -> &str {
        self.progress["status"]
            .as_str()
            .or(self.run["status"].as_str())
            .unwrap_or("connecting")
    }

    pub fn workflow(&self) -> Option<&str> {
        self.progress["workflow_id"]
            .as_str()
            .or(self.run["blueprint_id"].as_str())
    }

    pub fn steps(&self) -> impl Iterator<Item = &Value> {
        rows(&self.progress, "steps").filter(|s| !s["parent_step_id"].is_string())
    }

    pub fn children(&self) -> Vec<&Value> {
        rows(&self.progress, "steps")
            .filter(|s| s["parent_step_id"].is_string())
            .collect()
    }

    pub fn child_offset(&self, children: &[&Value]) -> usize {
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
        let children = self.children();
        let page = self.child_offset(&children) / CHILD_PAGE;
        let last = children.len().saturating_sub(1) / CHILD_PAGE;
        self.child_page = Some(page.saturating_add_signed(delta).min(last));
    }

    pub fn current(&self, step: &Value) -> bool {
        !self.stale()
            && (step["current"] == true
                || self.progress["current_step_ids"]
                    .as_array()
                    .is_some_and(|ids| ids.contains(&step["id"]))
                || status(step) == "running")
    }

    pub fn agents(&self) -> Vec<&Value> {
        let mut agents: Vec<_> = rows(&self.progress, "steps")
            .filter(|step| self.current(step))
            .flat_map(|step| rows(step, "agents"))
            .collect();
        for group in [&self.progress["current_step"], &self.progress] {
            if agents.is_empty() {
                agents.extend(rows(group, "agents"));
            }
        }
        if agents.is_empty() && super::super::terminal(&self.run) {
            agents.extend(rows(&self.progress, "steps").flat_map(|step| rows(step, "agents")));
        }
        agents
    }

    pub fn selected_agent(&self, agent_count: usize) -> usize {
        self.agent.min(agent_count.saturating_sub(1))
    }

    pub fn move_agent(&mut self, by: isize) {
        let count = self.agents().len();
        self.agent = self
            .selected_agent(count)
            .saturating_add_signed(by)
            .min(count.saturating_sub(1));
    }

    pub fn plain_lines(&self, id: &str) -> Vec<String> {
        let state = self.state();
        let mut lines = vec![format!("Run {id}  ·  {state}")];
        if self.stale() {
            lines.push("Reconnecting · last observation is stale".into());
        }
        if let Some(time) = elapsed(&self.progress) {
            lines[0].push_str(&format!("  ·  {time}"));
        }
        if let Some(name) = self.workflow() {
            lines.push(format!("Workflow  {name}"));
        }
        for item in rows(&self.progress, "preparations") {
            lines.push(preparation::message(item, ""));
        }
        if let Some(warning) = self.progress["monitor_warning"].as_str() {
            lines.push(format!("Warning  {warning}"));
        }
        if let Some(event) = self.feed.back() {
            lines.push(format!("Latest  {}", event.message));
        }
        let steps: Vec<_> = self.steps().collect();
        if !steps.is_empty() {
            let (_, summary) = step_counts(&steps, self.progress["workflow_kind"] == "service");
            lines.push(format!("Steps  {summary}"));
            lines.push(format!("  {:<10} {:>7}  Step", "Status", "Elapsed"));
            for step in steps {
                let counts = step["total_count"]
                    .as_u64()
                    .map(|n| format!("  {}/{n}", step["done_count"].as_u64().unwrap_or(0)))
                    .unwrap_or_default();
                lines.push(format!(
                    "  {:<10} {:>7}  {}{counts}",
                    status(step),
                    self.step_elapsed(step),
                    label(step)
                ));
            }
        }
        let children = self.children();
        if !children.is_empty() {
            lines.push(format!("Sub-workflow steps  {}", children.len()));
            for child in children {
                lines.push(format!(
                    "  {:<10} {:>7}  {}",
                    status(child),
                    self.step_elapsed(child),
                    label(child)
                ));
            }
        }
        let agents = self.agents();
        let selected = self.selected_agent(agents.len());
        if !agents.is_empty() {
            lines.push(format!("Agents  {}", agents.len()));
            for (i, agent) in agents
                .iter()
                .enumerate()
                .skip(selected.saturating_sub(2))
                .take(6)
            {
                let pointer = if i == selected { ">" } else { " " };
                let measured = measured(agent)
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
            }
        }
        lines.into_iter().map(|s| safe_line(&s, 120)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn clocks_freeze_on_disconnect_and_counts_exclude_skips() {
        let mut view = View::default();
        let active = json!({"id":"plan","status":"running","started_at":"2026-01-01T00:00:00Z","elapsed_seconds":61.9});
        let skipped = json!({"id":"publish","status":"skipped","elapsed_seconds":0.0});
        view.progress(
            json!({"workflow_updated_at":"2026-01-02","status":"running","steps":[active,skipped]}),
        );
        let now = Instant::now();
        view.observed = Some(now - Duration::from_secs(4));
        view.disconnect();
        assert_eq!(view.step_elapsed(&active), "1:05");
        assert_eq!(view.step_elapsed(&skipped), "—");
        assert!(!view.current(&active));
        assert!(!view.progress(json!({"workflow_updated_at":"2026-01-01","status":"running"})));
        assert!(view.stale());
        assert_eq!(
            step_counts(
                &[
                    &json!({"status":"done"}),
                    &json!({"status":"failed"}),
                    &skipped
                ],
                false
            )
            .1,
            "1/3 complete · 1 failed · 1 skipped"
        );
    }
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
        let output = view.plain_lines("r").join("\n");
        assert!(
            output.contains("step-0")
                && output.contains("step-8")
                && view.child_offset(&view.children()) == 4
                && output.contains("32%")
                && output.contains("preparing the gateway")
        );
        view.page_children(-1);
        assert_eq!(view.child_offset(&view.children()), 0);
    }
}
