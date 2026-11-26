pub(crate) mod events;
use futures_util::StreamExt;
use mn_client_support::{
    Error, Result,
    adapter::Adapter,
    control::{Method, Reply},
    resources::{endpoint, resource_id},
};
use serde_json::{Value, json};
use std::io::Write;
use std::{
    cell::Cell,
    collections::HashSet,
    path::{Path, PathBuf},
    rc::Rc,
};
#[derive(Clone)]
pub struct App {
    pub ipc: Adapter,
    pub home: PathBuf,
    pub json: bool,
    pub streaming: Rc<Cell<bool>>,
    sequence: Rc<Cell<usize>>,
}
impl App {
    pub fn new(home: PathBuf, json: bool) -> Self {
        Self {
            ipc: Adapter::new(&home),
            home,
            json,
            streaming: Default::default(),
            sequence: Default::default(),
        }
    }
    pub fn emit(&self, kind: &str, mut data: Value) {
        crate::output::redact(&mut data);
        if self.json {
            self.streaming.set(true);
            self.sequence.set(self.sequence.get() + 1);
            let field = if kind == "error" { "error" } else { "data" };
            println!(
                "{}",
                json!({"schema":"mn.cli.stream/v1","sequence":self.sequence.get(),"type":kind,"timestamp":chrono::Utc::now().to_rfc3339(),(field):data})
            );
        } else {
            println!("{}", serde_json::to_string_pretty(&data).unwrap());
        }
    }
    pub async fn pin(&self) -> Result<(Self, String)> {
        let (ipc, local) = self.ipc.pin().await?;
        Ok((
            Self {
                ipc,
                ..self.clone()
            },
            local,
        ))
    }
    fn prepare(&self, action: &str, mut v: Value, stream: bool) -> Result<(Method, Value, u32)> {
        let ep = endpoint(action)?;
        let mut path = ep.path.clone();
        let mut query = json!({});
        let mut headers = json!({});
        for (key, param) in &ep.parameters {
            match param.source.as_str() {
                "principal" => v[key] = json!("surface"),
                "path" => {
                    let val = &v[key];
                    let encoded = resource_id(
                        &val.as_str()
                            .map(str::to_owned)
                            .unwrap_or_else(|| val.to_string()),
                    );
                    path = path
                        .replace(&format!("{{{key}}}"), &encoded)
                        .replace(&format!("{{{key}:path}}"), &encoded);
                }
                "query" if !v[key].is_null() => query[&param.name] = v[key].clone(),
                "header" if !v[key].is_null() => {
                    headers[param.name.to_ascii_lowercase()] = v[key].clone()
                }
                _ => {}
            }
        }
        let deadline = ep.deadline(&v);
        v[if stream { "_stream" } else { "_context" }] =
            json!({"path":path,"method":ep.method,"query":query,"headers":headers});
        Ok((ep.rpc(), v, deadline))
    }
    async fn events(
        &self,
        feed: events::Feed,
        id: &str,
        cursor: Option<&str>,
    ) -> Result<mn_client_support::control::Chunks> {
        let (action, query) = match feed {
            events::Feed::Run => (
                "stream_run_events",
                json!({"run_id":id,"interval":0.25,"last_event_id":cursor}),
            ),
            events::Feed::Operation => (
                "stream_operation_events",
                json!({"operation_id":id,"last_event_id":cursor}),
            ),
        };
        let (method, args, _) = self.prepare(action, query, true)?;
        let (reply, chunks) = self.ipc.download(method, action, args).await?;
        reply.checked()?;
        Ok(chunks)
    }
    pub async fn raw(&self, action: &str, v: Value) -> Result<Reply> {
        let (rpc, v, deadline) = self.prepare(action, v, false)?;
        let id = v["idempotency_key"].as_str().unwrap_or_default().to_owned();
        self.ipc.call(rpc, action, v, &id, deadline).await
    }
    pub async fn get(&self, action: &str, v: Value) -> Result<Value> {
        self.raw(action, v).await?.checked()
    }
    pub async fn items(&self, action: &str, mut v: Value) -> Result<Value> {
        let (pinned, _) = self.pin().await?;
        let mut seen = HashSet::new();
        let mut items = Vec::new();
        loop {
            let res = pinned.get(action, v.clone()).await?;
            items.extend(
                res["items"]
                    .as_array()
                    .ok_or_else(Error::protocol)?
                    .iter()
                    .cloned(),
            );
            if res["next_page_token"].is_null() || res["next_page_token"] == "" {
                break;
            }
            let token = res["next_page_token"]
                .as_str()
                .ok_or_else(Error::protocol)?;
            if !seen.insert(token.to_owned()) {
                return Err(Error::protocol());
            }
            v["page_token"] = json!(token);
        }
        Ok(Value::Array(items))
    }
    pub async fn conditional(&self, read: &str, action: &str, mut v: Value) -> Result<Value> {
        let (a, _) = self.pin().await?;
        let mut q = v.clone();
        q.as_object_mut().unwrap().remove("request");
        let r = a.raw(read, q).await?;
        let tag = r
            .headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("etag"))
            .map(|(_, v)| v.clone())
            .filter(|v| !v.is_empty());
        r.checked()?;
        v["if_match"] = json!(tag.ok_or_else(Error::protocol)?);
        a.get(action, v).await
    }
    pub async fn download(&self, action: &str, v: Value, target: Option<&Path>) -> Result<Value> {
        let (_, args, _) = self.prepare(action, v, true)?;
        let (reply, mut data) = self.ipc.download(Method::ReadFile, action, args).await?;
        reply.checked()?;
        if let Some(path) = target {
            let tmp = tempfile::NamedTempFile::new_in(
                path.parent()
                    .filter(|p| !p.as_os_str().is_empty())
                    .unwrap_or(Path::new(".")),
            )
            .map_err(|_| Error::invalid("Cannot create download file."))?;
            let mut file = tmp.as_file();
            while let Some(chunk) = data.next().await {
                file.write_all(&chunk?).map_err(|_| Error::storage())?;
            }
            file.flush().map_err(|_| Error::storage())?;
            tmp.persist(path).map_err(|_| Error::storage())?;
            Ok(json!({"path":path}))
        } else {
            let mut buf = Vec::new();
            while let Some(chunk) = data.next().await {
                buf.extend_from_slice(&chunk?);
            }
            Ok(json!({"export":String::from_utf8(buf).map_err(|_|Error::protocol())?}))
        }
    }
}
