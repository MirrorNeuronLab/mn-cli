//! Follow one pinned run through push events, with periodic reconciliation and reconnect.
mod preparation;
#[cfg_attr(target_os = "wasi", path = "monitor/wasi_terminal.rs")]
mod terminal;
mod view;

use crate::client::App;
use crate::client::events::{self, Event, Feed};
use futures_util::{Stream, StreamExt};
use mn_client_support::{Error, Result};
use serde_json::{Value, json};
use std::time::Duration;
use terminal::{Input, Screen};
use tokio::time::{self, MissedTickBehavior};
use view::View;

enum Update {
    Run(Value),
    Progress(Value),
    Event(Event),
}

fn snapshots<'a>(app: &'a App, id: &'a str) -> impl Stream<Item = Result<Update>> + 'a {
    async_stream::try_stream! {
        let query = json!({"run_id": id});
        loop {
            yield Update::Run(app.get("get_run", query.clone()).await?);
            match app.get("get_run_workflow_progress", query.clone()).await {
                Ok(progress) => yield Update::Progress(progress),
                // A newly submitted run may not have a workflow projection yet.
                Err(e) if matches!(e.status, 404 | 425 | 503) => {}
                Err(e) => Err(e)?,
            }
            time::sleep(Duration::from_secs(1)).await;
        }
    }
}

fn apply(app: &App, view: &mut View, update: Update) -> Option<Value> {
    let (kind, data) = match update {
        Update::Run(run) => ("snapshot", run),
        Update::Progress(progress) => ("progress", progress),
        Update::Event(event) => match event.kind.as_str() {
            "run.snapshot" => ("progress", event.data),
            "run.completed" | "run.unavailable" => {
                if app.json && view.run != event.data {
                    app.emit("snapshot", event.data.clone());
                }
                view.run = event.data.clone();
                view.progress["status"] = event.data["status"].clone();
                return Some(event.data);
            }
            kind => {
                view.record(kind, &event.data);
                if app.json {
                    app.emit(
                        "event",
                        json!({"id":event.id,"type":kind,"data":event.data}),
                    );
                }
                return None;
            }
        },
    };
    if kind == "snapshot" {
        if app.json && view.run != data {
            app.emit(kind, data.clone());
        }
        view.run = data.clone();
        if super::terminal(&data) {
            view.progress["status"] = data["status"].clone();
            return Some(data);
        }
    } else if view.progress != data {
        if app.json {
            app.emit(kind, data.clone());
        }
        view.progress(data);
    }
    None
}

pub(crate) async fn watch(app: &App, id: &str, seconds: Option<f64>) -> Result<Value> {
    let terminal_error = |_: std::io::Error| {
        Error::new(
            500,
            "MN_MONITOR_IO_FAILED",
            "Cannot access the monitor terminal.",
        )
    };
    let mut screen = Screen::open(app.json).map_err(terminal_error)?;
    let mut view = View::default();
    if !app.json {
        screen.draw(&view, id).map_err(terminal_error)?;
    }
    let deadline = async {
        match seconds.and_then(|s| Duration::try_from_secs_f64(s).ok()) {
            Some(duration) => time::sleep(duration).await,
            None => std::future::pending().await,
        }
    };
    let follow = async {
        let (pinned, _) = app.pin().await?;
        let updates = futures_util::stream::select(
            snapshots(&pinned, id),
            events::follow(&pinned, Feed::Run, id).map(|e| Ok(Update::Event(e))),
        );
        futures_util::pin_mut!(updates);
        let mut tick = time::interval(Duration::from_millis(80));
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                Some(update) = updates.next() => {
                    let final_run = apply(app, &mut view, update?);
                    if !app.json {
                        screen.draw(&view, id).map_err(terminal_error)?;
                    }
                    if let Some(run) = final_run {
                        return Ok(run);
                    }
                }
                _ = tick.tick(), if screen.interactive() => {
                    match screen.input(&mut view).map_err(terminal_error)? {
                        Input::Idle => {}
                        Input::Redraw => screen.draw(&view, id).map_err(terminal_error)?,
                        Input::Detach => break,
                    }
                }
            }
        }
        Ok(json!({"run_id":id,"detached":true}))
    };
    tokio::select! {
        result = follow => result,
        _ = deadline => Ok(json!({"run_id": id, "detached": true})),
        _ = crate::interrupt() => Ok(json!({"run_id": id, "detached": true})),
    }
}
