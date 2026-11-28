//! Decode the runtime's Server-Sent Events independently of gRPC chunk boundaries.
use super::App;
use futures_util::{Stream, StreamExt};
use mn_client_support::{Error, Result};
use serde_json::Value;
use std::time::Duration;
use tokio::time;

#[derive(Clone, Copy)]
pub(crate) enum Feed {
    Run,
    Operation,
}

const MAX_EVENT_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug)]
pub(crate) struct Event {
    pub id: Option<String>,
    pub kind: String,
    pub data: Value,
}

#[derive(Default)]
struct Decoder {
    line: Vec<u8>,
    data: String,
    kind: String,
    id: Option<String>,
    after_cr: bool,
}

impl Decoder {
    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<Event>> {
        let mut events = Vec::new();
        for &byte in bytes {
            if self.after_cr && byte == b'\n' {
                self.after_cr = false;
                continue;
            }
            self.after_cr = byte == b'\r';
            if byte == b'\n' || byte == b'\r' {
                if let Some(event) = self.finish_line()? {
                    events.push(event);
                }
            } else {
                if self.line.len() + self.data.len() >= MAX_EVENT_BYTES {
                    return Err(Error::protocol());
                }
                self.line.push(byte);
            }
        }
        Ok(events)
    }

    fn finish_line(&mut self) -> Result<Option<Event>> {
        let line = std::str::from_utf8(&self.line).map_err(|_| Error::protocol())?;
        let event = if line.is_empty() {
            let kind = std::mem::take(&mut self.kind);
            let id = self.id.take();
            let data = std::mem::take(&mut self.data);
            if data.is_empty() {
                None
            } else {
                let value: Value = serde_json::from_str(&data).map_err(|_| Error::protocol())?;
                // Core wraps the public record in a versioned event envelope.
                let (kind, data) = if let (Some(name), Some(payload)) =
                    (value["type"].as_str(), value.get("data"))
                {
                    (name.to_owned(), payload.clone())
                } else {
                    (kind, value)
                };
                Some(Event { id, kind, data })
            }
        } else {
            let (field, value) = line.split_once(':').unwrap_or((line, ""));
            let value = value.strip_prefix(' ').unwrap_or(value);
            match field {
                "data" => {
                    self.data.push_str(value);
                    self.data.push('\n');
                }
                "event" => self.kind = value.to_owned(),
                "id" if !value.contains('\0') => self.id = Some(value.to_owned()),
                _ => {}
            }
            None
        };
        self.line.clear();
        Ok(event)
    }
}

/// Core can replay its event history after a reconnect and assign fresh SSE IDs.
/// Keep a bounded fingerprint window for run history; snapshots are always delivered.
#[derive(Default)]
struct RunHistory {
    recent: std::collections::HashSet<u64>,
    order: std::collections::VecDeque<u64>,
}
impl RunHistory {
    fn insert(&mut self, event: &Event) -> bool {
        use std::hash::{DefaultHasher, Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        event.kind.hash(&mut hasher);
        event.data.hash(&mut hasher);
        let fingerprint = hasher.finish();
        if !self.recent.insert(fingerprint) {
            return false;
        }
        if self.order.len() >= 5000
            && let Some(old) = self.order.pop_front()
        {
            self.recent.remove(&old);
        }
        self.order.push_back(fingerprint);
        true
    }
}

pub(crate) fn follow<'a>(app: &'a App, feed: Feed, id: &'a str) -> impl Stream<Item = Event> + 'a {
    async_stream::stream! {
        let mut cursor = None;
        let mut history = RunHistory::default();
        let mut backoff = Duration::from_millis(250);
        loop {
            if let Ok(mut chunks) = app.events(feed, id, cursor.as_deref()).await {
                let mut decoder = Decoder::default();
                while let Some(Ok(chunk)) = chunks.next().await {
                    let Ok(events) = decoder.push(&chunk) else {
                        break;
                    };
                    for event in events {
                        backoff = Duration::from_millis(250);
                        if let Some(id) = &event.id {
                            if cursor.as_ref() == Some(id) {
                                continue;
                            }
                            cursor = Some(id.clone());
                        }
                        let replayable = matches!(feed, Feed::Run)
                            && !matches!(event.kind.as_str(), "run.snapshot" | "run.completed" | "run.unavailable");
                        if replayable && !history.insert(&event) {
                            continue;
                        }
                        yield event;
                    }
                }
            }
            if matches!(feed, Feed::Run) {
                yield Event { id: None, kind: "run.observation_stale".into(), data: Value::Null };
            }
            time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(5));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_chunk_boundary_decodes_the_same_frames() {
        let wire = ": heartbeat\r\n\r\nid: 7\r\nevent: run.snapshot\r\ndata: {\"type\":\"run.snapshot\",\r\ndata: \"data\":{\"name\":\"café\"}}\r\n\r\nid: 8\nevent: run.completed\ndata: {}\n\n".as_bytes();
        for boundary in 0..=wire.len() {
            let mut decoder = Decoder::default();
            let mut events = decoder.push(&wire[..boundary]).unwrap();
            events.extend(decoder.push(&wire[boundary..]).unwrap());
            assert_eq!(events.len(), 2);
            assert_eq!(events[0].id.as_deref(), Some("7"));
            assert_eq!(events[0].data["name"], "café");
            assert_eq!(events[1].kind, "run.completed");
        }
    }

    #[test]
    fn replayed_events_can_have_new_transport_ids() {
        let mut history = RunHistory::default();
        let mut event = Event {
            id: Some("2".into()),
            kind: "log".into(),
            data: serde_json::json!({"message":"ready","timestamp":1}),
        };
        assert!(history.insert(&event));
        event.id = Some("9".into());
        assert!(!history.insert(&event));
        event.data["timestamp"] = 2.into();
        assert!(history.insert(&event));
    }

    #[test]
    fn incomplete_records_are_never_emitted() {
        let mut decoder = Decoder::default();
        assert!(
            decoder
                .push(b": heartbeat\n\ndata: {\"unfin")
                .unwrap()
                .is_empty()
        );
        assert!(decoder.push(b"ished\n\n").is_err());
    }
}
