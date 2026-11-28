use super::{
    preparation,
    view::{
        CHILD_PAGE, View, complete, elapsed, label, measured, rows, safe_line, status, step_counts,
    },
};
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, BorderType, Cell, LineGauge, Paragraph, Row, Table, Wrap},
};
use serde_json::Value;

const MUTED: Style = Style::new().dim();
const EMPHASIS: Style = Style::new().bold();

pub(super) fn render(frame: &mut Frame<'_>, view: &View, id: &str, finished: bool) {
    let area = frame.area();
    let children = view.children();
    let footer = if finished { " Close" } else { " Detach" };
    let [main, keys] = Layout::vertical([Constraint::Fill(1), Constraint::Length(1)]).areas(area);
    if area.height < 12 {
        let focus = view.steps().find(|step| view.current(step));
        let mut lines = vec![Line::from(vec![
            status_span(view.state()),
            Span::raw(format!("  Run {}", safe_line(id, 240))),
        ])];
        if view.stale() {
            lines.push(Line::styled(
                "Reconnecting · last observation is stale",
                Color::Yellow,
            ));
        }
        if let Some(step) = focus {
            lines.push(Line::from(format!(
                "{}  {}",
                safe_line(label(step), 240),
                view.step_elapsed(step)
            )));
        }
        if let Some(event) = view.feed.back() {
            lines.push(Line::from(event.message.as_str()));
        }
        frame.render_widget(Paragraph::new(lines), main);
        frame.render_widget(Line::from(Vec::from(key_hint("q", footer))), keys);
        return;
    }

    let mut identity = Vec::new();
    if let Some(name) = view.workflow() {
        identity.push(Line::styled(
            safe_line(name, 240),
            EMPHASIS.fg(Color::LightCyan),
        ));
    }
    let run = format!("Run {}", safe_line(id, 240));
    let job = view.progress["stable_job_id"]
        .as_str()
        .or(view.run["job_id"].as_str())
        .filter(|id| !id.is_empty())
        .map(|id| format!("Job {}", safe_line(id, 240)));
    if let Some(job) = job {
        let combined = Line::from(format!("{run}  ·  {job}"));
        if combined.width() <= usize::from(area.width.saturating_sub(2)) {
            identity.push(combined.style(MUTED));
        } else {
            identity.push(Line::styled(run, MUTED));
            identity.push(Line::styled(job, MUTED));
        }
    } else {
        identity.push(Line::styled(run, MUTED));
    }
    let event_height = match area.height {
        26.. => 7,
        20.. => 5,
        _ => 3,
    };
    let [header, body, activity] = Layout::vertical([
        Constraint::Length(2 + identity.len() as u16),
        Constraint::Fill(1),
        Constraint::Length(event_height),
    ])
    .areas(main);
    let mut summary = vec![Span::raw(" "), status_span(view.state())];
    if view.stale() {
        summary.push(Span::styled(" · Reconnecting (stale)", Color::Yellow));
    }
    if let Some(time) = elapsed(&view.progress) {
        summary.push(Span::styled(format!("  ·  {time}"), MUTED));
    }
    summary.push(Span::raw(" "));
    frame.render_widget(
        Paragraph::new(identity)
            .block(panel(" Run watch ").title_top(Line::from(summary).right_aligned())),
        header,
    );

    let wide = body.width >= 60;
    let page_height = CHILD_PAGE as u16 + 2;
    let child_height =
        if !children.is_empty() && body.height >= page_height + if wide { 5 } else { 8 } {
            page_height
        } else {
            0
        };
    let [overview, child_area] =
        Layout::vertical([Constraint::Fill(1), Constraint::Length(child_height)]).areas(body);
    let [workflow_area, agent_area] = if wide {
        Layout::horizontal([Constraint::Percentage(38), Constraint::Fill(1)])
            .spacing(1)
            .areas(overview)
    } else if view.detail {
        [Rect::default(), overview]
    } else {
        Layout::vertical([
            Constraint::Length((overview.height / 3).max(3)),
            Constraint::Fill(1),
        ])
        .areas(overview)
    };

    let steps: Vec<_> = view.steps().collect();
    render_workflow(frame, workflow_area, view, &steps, &children);
    let child_area = if child_height == 0
        && !children.is_empty()
        && !view.detail
        && agent_area.height >= page_height
    {
        agent_area
    } else {
        render_agents(frame, agent_area, view);
        child_area
    };
    render_children(frame, child_area, view, &steps, &children);
    render_activity(frame, activity, view);

    let mut hints = Vec::from(key_hint("q", footer));
    hints.extend(key_hint("   ↑↓", " Agents"));
    if view.detail {
        hints.extend(key_hint("   Backspace", " Overview"));
    } else {
        hints.extend(key_hint("   Enter", " Details"));
    }
    if !child_area.is_empty() {
        hints.extend(key_hint("   [ ]", " Pages"));
        hints.extend(key_hint("   f", " Follow"));
    }
    frame.render_widget(Line::from(hints), keys);
}

fn panel<'a>(title: impl Into<Line<'a>>) -> Block<'a> {
    Block::bordered()
        .title(title)
        .title_style(EMPHASIS)
        .border_type(BorderType::Rounded)
        .border_style(Style::new().fg(Color::DarkGray))
}

fn status_color(state: &str) -> Color {
    match state {
        "completed" | "complete" | "done" | "succeeded" | "ready" => Color::Green,
        "running" | "active" | "live" | "connecting" | "preparing" => Color::Cyan,
        "failed" | "blocked" | "error" | "unknown" => Color::Red,
        "queued" | "paused" | "retry_wait" | "cancelled" | "canceled" => Color::Yellow,
        _ => Color::Reset,
    }
}

fn status_span(state: &str) -> Span<'static> {
    Span::styled(
        safe_line(state, 24).replace('_', " "),
        EMPHASIS.fg(status_color(state)),
    )
}

fn step_status(state: &str, compact: bool) -> Span<'static> {
    if !compact {
        return status_span(state);
    }
    let symbol = match state {
        "completed" | "complete" | "done" | "succeeded" | "ready" => "✓",
        "failed" | "blocked" | "error" => "×",
        "running" | "active" | "live" => "●",
        "paused" | "retry_wait" => "‖",
        "skipped" | "cancelled" | "canceled" => "–",
        _ => "○",
    };
    Span::styled(symbol, status_color(state))
}

fn row_label(row: &Value, selected: bool) -> Span<'static> {
    let name = safe_line(label(row), 240);
    if selected {
        Span::styled(format!("› {name}"), EMPHASIS.fg(Color::LightCyan))
    } else {
        Span::raw(format!("  {name}"))
    }
}

fn key_hint(key: &'static str, label: &'static str) -> [Span<'static>; 2] {
    [Span::styled(key, EMPHASIS), Span::styled(label, MUTED)]
}

fn window_start(count: usize, selected: usize, visible: usize) -> usize {
    selected
        .saturating_sub(visible / 2)
        .min(count.saturating_sub(visible))
}

fn render_workflow(
    frame: &mut Frame<'_>,
    area: Rect,
    view: &View,
    steps: &[&Value],
    children: &[&Value],
) {
    if area.is_empty() {
        return;
    }
    let block = panel(" Workflow ");
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if steps.is_empty() {
        frame.render_widget(Line::styled("Waiting for workflow steps…", MUTED), inner);
        return;
    }
    let service = view.progress["workflow_kind"] == "service";
    let (done, summary) = step_counts(steps, service);
    let [gauge, table] =
        Layout::vertical([Constraint::Length(1), Constraint::Fill(1)]).areas(inner);
    frame.render_widget(
        LineGauge::default()
            .ratio(done as f64 / steps.len() as f64)
            .label(Line::styled(summary, MUTED))
            .filled_style(Color::Green)
            .filled_symbol("━")
            .unfilled_style(Color::DarkGray),
        gauge,
    );
    let focused = steps
        .iter()
        .position(|step| view.current(step))
        .or_else(|| {
            steps
                .iter()
                .position(|step| matches!(status(step), "failed" | "blocked"))
        })
        .unwrap_or(0);
    let visible = usize::from(table.height.saturating_sub(1));
    let start = window_start(steps.len(), focused, visible);
    let compact = table.width < 36;
    let show_counts = table.width >= 38;
    let rows = steps.iter().skip(start).take(visible).map(|step| {
        let mut cells = vec![
            Cell::from(step_status(status(step), compact)),
            Cell::from(row_label(step, view.current(step))),
        ];
        if show_counts {
            let tasks = children
                .iter()
                .filter(|child| child["parent_step_id"] == step["id"]);
            let total = tasks.clone().count();
            let counts = if total > 0 {
                format!("{}/{total}", tasks.filter(|child| complete(child)).count())
            } else {
                let count = if service { "ready_count" } else { "done_count" };
                step["total_count"]
                    .as_u64()
                    .map_or_else(String::new, |total| {
                        format!("{}/{total}", step[count].as_u64().unwrap_or(0))
                    })
            };
            cells.push(Cell::from(Line::styled(counts, MUTED).right_aligned()));
        }
        cells.push(Cell::from(
            Line::styled(view.step_elapsed(step), MUTED).right_aligned(),
        ));
        Row::new(cells)
    });
    let mut widths = vec![
        Constraint::Length(if compact { 1 } else { 9 }),
        Constraint::Fill(1),
    ];
    if show_counts {
        widths.push(Constraint::Length(5));
    }
    widths.push(Constraint::Length(7));
    let mut headers = vec!["", "Step"];
    if show_counts {
        headers.push("");
    }
    headers.push("Elapsed");
    frame.render_widget(
        Table::new(rows, widths).header(Row::new(headers).style(MUTED)),
        table,
    );
}

fn render_agents(frame: &mut Frame<'_>, area: Rect, view: &View) {
    if area.is_empty() {
        return;
    }
    let agents = view.agents();
    let selected = view.selected_agent(agents.len());
    if view.detail {
        render_agent_detail(frame, area, agents.get(selected).copied());
        return;
    }
    let block = panel(format!(" Agents · {} ", agents.len()));
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if agents.is_empty() {
        frame.render_widget(Line::styled("No agents reported yet", MUTED), inner);
        return;
    }

    let show_activity = inner.width >= 42;
    let mut widths = vec![Constraint::Fill(1)];
    let mut headings = vec!["Agent"];
    if show_activity {
        widths[0] = Constraint::Length((inner.width / 3).clamp(16, 32));
        widths.push(Constraint::Fill(1));
        headings.push("Activity");
    }
    widths.push(Constraint::Length(if inner.width >= 30 { 12 } else { 9 }));
    headings.push("Progress");
    let show_header = inner.height > 1;
    let visible = usize::from(inner.height.saturating_sub(u16::from(show_header)));
    let start = window_start(agents.len(), selected, visible);
    let rows = agents
        .iter()
        .enumerate()
        .skip(start)
        .take(visible)
        .map(|(index, agent)| {
            let mut cells = vec![Cell::from(row_label(agent, index == selected))];
            if show_activity {
                cells.push(Cell::from(activity_summary(agent)));
            }
            cells.push(Cell::from(agent_progress(agent, inner.width >= 30)));
            Row::new(cells)
        });
    let mut table = Table::new(rows, widths);
    if show_header {
        table = table.header(Row::new(headings).style(MUTED));
    }
    frame.render_widget(table, inner);
}

fn activity_summary(agent: &Value) -> String {
    if let Some(prep) = rows(agent, "preparations").next() {
        return preparation::message(prep, "");
    }
    [
        "activity_summary",
        "working_on",
        "current_task",
        "last_activity",
        "role",
    ]
    .iter()
    .find_map(|field| agent[field].as_str().filter(|text| !text.is_empty()))
    .map(|text| safe_line(text, 240))
    .unwrap_or_default()
}

fn agent_progress(agent: &Value, bar: bool) -> Line<'static> {
    let state = status(agent);
    let Some(progress) = measured(agent) else {
        return Line::from(status_span(state));
    };
    let mut spans = Vec::new();
    if bar {
        let filled = (progress * 6.0).round() as usize;
        spans.push(Span::styled("━".repeat(filled), status_color(state)));
        spans.push(Span::styled("─".repeat(6 - filled), Color::DarkGray));
        spans.push(Span::raw(" "));
    }
    spans.push(Span::styled(
        format!("{:>3.0}%", progress * 100.0),
        status_color(state),
    ));
    Line::from(spans)
}

fn render_agent_detail(frame: &mut Frame<'_>, area: Rect, agent: Option<&Value>) {
    let block = panel(" Agent details ");
    let Some(agent) = agent else {
        frame.render_widget(Paragraph::new("No agent selected").block(block), area);
        return;
    };
    let mut state = vec![status_span(status(agent))];
    if measured(agent).is_some() {
        state.push(Span::raw("  "));
        state.extend(agent_progress(agent, area.width >= 30).spans);
    }
    let mut lines = vec![
        Line::styled(safe_line(label(agent), 240), EMPHASIS.fg(Color::LightCyan)),
        Line::from(state),
    ];
    let activity = activity_summary(agent);
    if !activity.is_empty() {
        lines.push(Line::from(activity));
    }
    for (title, value) in [
        ("Node", agent["node"].as_str().or(agent["node_id"].as_str())),
        ("Step", agent["step_id"].as_str()),
        (
            "Type",
            agent["agent_type"].as_str().or(agent["type"].as_str()),
        ),
    ] {
        if let Some(value) = value {
            lines.push(Line::from(vec![
                Span::styled(format!("{title}  "), MUTED),
                Span::raw(safe_line(value, 240)),
            ]));
        }
    }
    frame.render_widget(
        Paragraph::new(lines).block(block).wrap(Wrap { trim: true }),
        area,
    );
}

fn render_children(
    frame: &mut Frame<'_>,
    area: Rect,
    view: &View,
    steps: &[&Value],
    children: &[&Value],
) {
    if area.is_empty() || children.is_empty() {
        return;
    }
    let start = view.child_offset(children);
    let end = (start + CHILD_PAGE).min(children.len());
    let mode = if view.child_page.is_some() {
        "Browsing"
    } else {
        "Following"
    };
    let title = format!(
        " Child steps · {}–{end} of {} · {mode} ",
        start + 1,
        children.len()
    );
    let show_metadata = area.width >= 48;
    let rows = children[start..end].iter().map(|child| {
        let parent = steps
            .iter()
            .find(|step| step["id"] == child["parent_step_id"]);
        let mut task = parent.map_or_else(String::new, |step| {
            format!("{} › ", safe_line(label(step), 80))
        });
        let name = label(child);
        task.push_str(&safe_line(name, 160));
        if let Some(id) = child["id"].as_str().filter(|id| *id != name) {
            let parent_id = child["parent_step_id"].as_str().unwrap_or_default();
            let id = id
                .strip_prefix(parent_id)
                .and_then(|tail| tail.strip_prefix(':'))
                .unwrap_or(id);
            task.push_str(&format!(" · {}", safe_line(id, 120)));
        }
        let mut cells = vec![Cell::from(task), Cell::from(status_span(status(child)))];
        if show_metadata {
            let round = child["child_round"]
                .as_u64()
                .map_or_else(String::new, |n| format!("R{n}"));
            cells.push(Cell::from(Line::styled(round, MUTED).right_aligned()));
            cells.push(Cell::from(
                Line::styled(view.step_elapsed(child), MUTED).right_aligned(),
            ));
        }
        Row::new(cells)
    });
    let mut widths = vec![Constraint::Fill(1), Constraint::Length(10)];
    if show_metadata {
        widths.extend([Constraint::Length(4), Constraint::Length(7)]);
    }
    frame.render_widget(Table::new(rows, widths).block(panel(title)), area);
}

fn render_activity(frame: &mut Frame<'_>, area: Rect, view: &View) {
    let block = panel(" Recent events ");
    let capacity = usize::from(block.inner(area).height);
    let mut lines = Vec::new();
    if let Some(warning) = view.progress["monitor_warning"].as_str() {
        lines.push(Line::styled(
            format!("Warning  {}", safe_line(warning, 240)),
            Color::Yellow,
        ));
    }
    let event_row = usize::from(!view.feed.is_empty() && capacity > 1);
    for prep in
        rows(&view.progress, "preparations").take(capacity.saturating_sub(lines.len() + event_row))
    {
        lines.push(Line::styled(preparation::message(prep, ""), Color::Cyan));
    }
    let events = capacity.saturating_sub(lines.len());
    lines.extend(
        view.feed
            .iter()
            .skip(view.feed.len().saturating_sub(events))
            .map(|event| {
                let mut line = Line::default();
                if let Some(time) = &event.time {
                    line.push_span(Span::styled(format!("{time}  "), MUTED));
                }
                line.push_span(event.message.as_str());
                line
            }),
    );
    if lines.is_empty() {
        lines.push(Line::styled("Waiting for workflow events…", MUTED));
    }
    frame.render_widget(Paragraph::new(lines).block(block), area);
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::{Terminal, backend::TestBackend};
    use serde_json::json;

    #[test]
    fn dashboard_updates_progress_and_keeps_the_selected_agent_visible() {
        let mut terminal = Terminal::new(TestBackend::new(80, 24)).unwrap();
        let mut view = View::default();
        view.progress(json!({
            "workflow_id": "evidence-report",
            "stable_job_id": "job-1",
            "status": "running",
            "steps": [
                {"label": "Gather sources", "status": "completed"},
                {"label": "Compare evidence", "status": "running", "agents": [
                    {"id": "source-reader", "status": "running", "progress": 0.25},
                    {"id": "review-agent", "node": "node-east", "status": "running", "progress": 0.64}
                ]}
            ]
        }));
        view.record(
            "run.activity",
            &json!({
                "timestamp": "2026-11-27T12:34:50Z", "message": "Opening evidence"
            }),
        );
        let mut draw = |view: &View| {
            let frame = terminal
                .draw(|frame| render(frame, view, "run-1", false))
                .unwrap();
            frame
                .buffer
                .content
                .chunks(80)
                .map(|row| row.iter().map(|cell| cell.symbol()).collect::<String>())
                .collect::<Vec<_>>()
                .join("\n")
        };
        let overview = draw(&view);
        for text in [
            "Job job-1",
            "1/2 complete",
            "Compare evidence",
            "source-reader",
            "25%",
            "12:34:50  Opening evidence",
        ] {
            assert!(overview.contains(text), "missing {text} from {overview}");
        }

        view.move_agent(1);
        view.detail = true;
        let detail = draw(&view);
        for text in ["Agent details", "review-agent", "64%", "node-east"] {
            assert!(detail.contains(text), "missing {text} from {detail}");
        }
    }
}
