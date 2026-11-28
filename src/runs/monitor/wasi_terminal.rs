#[path = "wasi_backend.rs"]
mod backend;

use super::{Input, Key, View};
pub(super) use backend::Backend;
use std::{
    io::{self, Write},
    path::PathBuf,
    time::{Duration, Instant},
};

pub(super) struct Session {
    dir: PathBuf,
    keys: Vec<u8>,
    escape_started: Option<Instant>,
    input: wasip2::io::streams::InputStream,
}

impl Session {
    pub fn open() -> io::Result<Option<Self>> {
        let Some(dir) = std::env::var_os("MN_WASI_HOST_DIR").map(PathBuf::from) else {
            return Ok(None);
        };
        std::fs::write(dir.join("terminal"), b"open")?;
        let session = Self {
            dir,
            keys: Vec::new(),
            escape_started: None,
            input: wasip2::cli::stdin::get_stdin(),
        };
        let mut out = io::stdout().lock();
        write!(out, "\x1b[?1049h\x1b[?25l")?;
        out.flush()?;
        Ok(Some(session))
    }

    pub fn backend(&self) -> Backend {
        Backend::new(self.dir.join("size"))
    }

    pub fn input(&mut self, view: &mut View) -> io::Result<Input> {
        self.keys.extend(self.input.read(128).unwrap_or_default());
        let mut action = Input::Idle;
        let mut keys = self.keys.as_slice();
        while let Some((&key, rest)) = keys.split_first() {
            keys = rest;
            let key = match key {
                3 => Key::Detach,
                27 => {
                    let sequence = matches!(keys.first(), Some(b'[' | b'O'));
                    let end = if sequence {
                        keys.iter()
                            .skip(1)
                            .position(|ch| (0x40..=0x7e).contains(ch))
                    } else {
                        None
                    };
                    if let Some(end) = end {
                        let code = keys[end + 1];
                        keys = &keys[end + 2..];
                        self.escape_started = None;
                        match code {
                            b'A' => Key::Up,
                            b'B' => Key::Down,
                            _ => continue,
                        }
                    } else if (keys.is_empty() || sequence)
                        && self
                            .escape_started
                            .get_or_insert_with(Instant::now)
                            .elapsed()
                            < Duration::from_millis(150)
                    {
                        // A terminal escape sequence can span multiple WASI reads.
                        let consumed = self.keys.len() - keys.len() - 1;
                        self.keys.drain(..consumed);
                        return Ok(action);
                    } else {
                        Key::Detach
                    }
                }
                b'\r' | b'\n' => Key::Enter,
                8 | 127 => Key::Backspace,
                b'\t' => Key::Tab,
                byte => Key::Char(char::from(byte)),
            };
            match key.apply(view) {
                Input::Detach => return Ok(Input::Detach),
                Input::Redraw => action = Input::Redraw,
                Input::Idle => {}
            }
        }
        self.keys.clear();
        Ok(action)
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        let mut out = io::stdout().lock();
        let _ = write!(out, "\x1b[0m\x1b[?25h\x1b[?1049l");
        let _ = out.flush();
        let _ = std::fs::remove_file(self.dir.join("terminal"));
    }
}
