//! Follow one pinned run through push events, with periodic reconciliation and reconnect.
mod dashboard;
mod preparation;
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
    Stale,
}

fn transient(e: &Error) -> bool {
    matches!(e.status, 408 | 425 | 429 | 502 | 503 | 504)
}

impl Update {
    fn finishes_run(&self) -> bool {
        match self {
            Self::Run(run) => super::terminal(run),
            Self::Event(event) => {
                matches!(event.kind.as_str(), "run.completed" | "run.unavailable")
            }
            Self::Progress(_) | Self::Stale => false,
        }
    }
}

fn snapshots<'a>(app: &'a App, id: &'a str) -> impl Stream<Item = Result<Update>> + 'a {
    async_stream::try_stream! {
        let query = json!({"run_id": id});
        let mut delay=Duration::from_millis(250);
        loop {
            match app.get("get_run", query.clone()).await {
                Ok(run) => yield Update::Run(run),
                Err(e) if transient(&e) => {
                    yield Update::Stale;
                    time::sleep(delay).await;
                    delay=(delay*2).min(Duration::from_secs(5));
                    continue;
                }
                Err(e) => Err(e)?,
            }
            match app.get("get_run_workflow_progress", query.clone()).await {
                Ok(progress) => yield Update::Progress(progress),
                // A newly submitted run may not have a workflow projection yet.
                Err(e) if e.status == 404 => {}
                Err(e) if transient(&e) => {
                    yield Update::Stale;
                    time::sleep(delay).await;
                    delay=(delay*2).min(Duration::from_secs(5));
                    continue;
                }
                Err(e) => Err(e)?,
            }
            delay=Duration::from_millis(250);
            time::sleep(Duration::from_secs(1)).await;
        }
    }
}

fn updates<'a>(app: &'a App, id: &'a str) -> impl Stream<Item = Result<Update>> + 'a {
    async_stream::try_stream! {
        let (pinned, _) = app.pin().await?;
        let stream = futures_util::stream::select(
            snapshots(&pinned, id),
            events::follow(&pinned, Feed::Run, id).map(|event| Ok(Update::Event(event))),
        );
        futures_util::pin_mut!(stream);
        while let Some(update) = stream.next().await {
            let update = update?;
            if update.finishes_run() {
                let mut delay=Duration::from_millis(250);
                loop {
                    match pinned.get("get_run_workflow_progress", json!({"run_id": id})).await {
                        Ok(progress) => {yield Update::Progress(progress);break;}
                        Err(e) if transient(&e) => {
                            yield Update::Stale;
                            time::sleep(delay).await;
                            delay=(delay*2).min(Duration::from_secs(5));
                        }
                        Err(e) if e.status == 404 => break,
                        Err(e) => Err(e)?,
                    }
                }
            }
            yield update;
        }
    }
}

fn apply(app: &App, view: &mut View, update: Update) -> Option<Value> {
    if matches!(update, Update::Stale)
        || matches!(&update, Update::Event(e) if e.kind=="run.observation_stale")
    {
        if view.disconnect() && app.json {
            app.emit("observation_stale", json!({"stale":true}));
        }
        return None;
    }
    let (kind, data) = match update {
        Update::Run(run) => ("snapshot", run),
        Update::Progress(progress) => ("progress", progress),
        Update::Stale => unreachable!(),
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
    } else if view.progress(data) && app.json {
        app.emit(kind, view.progress.clone());
    }
    None
}

pub(super) async fn watch(app: &App, id: &str) -> Result<Value> {
    monitor(app, id, None, true).await
}

pub(crate) async fn follow(app: &App, id: &str, seconds: Option<f64>) -> Result<Value> {
    monitor(app, id, seconds, false).await
}

async fn monitor(app: &App, id: &str, seconds: Option<f64>, inspect: bool) -> Result<Value> {
    let terminal_error = |_: std::io::Error| {
        Error::new(
            500,
            "MN_MONITOR_IO_FAILED",
            "Cannot access the monitor terminal.",
        )
    };
    let mut screen = Screen::open(app.json).map_err(terminal_error)?;
    let inspect = inspect && screen.interactive();
    let mut view = View::default();
    screen.draw(&view, id, false).map_err(terminal_error)?;
    let deadline = async {
        match seconds.and_then(|s| Duration::try_from_secs_f64(s).ok()) {
            Some(duration) => time::sleep(duration).await,
            None => std::future::pending().await,
        }
    };
    let interrupt = crate::interrupt();
    tokio::pin!(deadline, interrupt);
    let mut tick = time::interval(Duration::from_millis(80));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut stream = Some(Box::pin(updates(app, id)));
    let mut finished = None;
    let mut redraw = false;
    let mut clock = time::interval(Duration::from_secs(1));
    clock.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            Some(update) = async { stream.as_mut()?.next().await }, if stream.is_some() => {
                if let Some(mut run) = apply(app, &mut view, update?) {
                    for key in ["steps", "step_count"] {
                        if let Some(value) = view.progress.get(key) {run[key] = value.clone();}
                    }
                    if !inspect {
                        screen.draw(&view, id, true).map_err(terminal_error)?;
                        return Ok(run);
                    }
                    finished = Some(run);
                    stream = None;
                }
                if screen.interactive() {
                    // Collapse bursts of run events into one frame while keeping input responsive.
                    redraw = true;
                } else {
                    screen.draw(&view, id, false).map_err(terminal_error)?;
                }
            }
            _ = clock.tick(), if finished.is_none() => {
                if app.json {
                    if view.stale() && view.disconnect() {
                        app.emit("observation_stale", json!({"stale":true}));
                    }
                } else if screen.interactive() { redraw=true; }
                else {screen.draw(&view, id, false).map_err(terminal_error)?;}
            }
            _ = tick.tick(), if screen.interactive() => {
                match screen.input(&mut view).map_err(terminal_error)? {
                    Input::Idle => {}
                    Input::Redraw => redraw = true,
                    Input::Detach => break,
                }
                if redraw {
                    screen.draw(&view, id, finished.is_some()).map_err(terminal_error)?;
                    redraw = false;
                }
            }
            _ = &mut deadline => break,
            _ = &mut interrupt => break,
        }
    }
    Ok(finished.unwrap_or_else(|| json!({"run_id":id,"detached":true})))
}
