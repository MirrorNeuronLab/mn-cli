use super::view::View;
use std::{
    io::{self, IsTerminal, Write},
    path::PathBuf,
    time::{Duration, Instant},
};
pub(super) enum Input {
    Idle,
    Redraw,
    Detach,
}
pub(super) struct Screen {
    session: Option<PathBuf>,
    previous: Vec<String>,
    keys: Vec<u8>,
    escape_started: Option<Instant>,
    input: wasip2::io::streams::InputStream,
}
impl Screen {
    pub fn open(json: bool) -> io::Result<Self> {
        let session = std::env::var_os("MN_WASI_HOST_DIR")
            .map(PathBuf::from)
            .filter(|_| {
                !json
                    && io::stdin().is_terminal()
                    && io::stdout().is_terminal()
                    && std::env::var("TERM").as_deref() != Ok("dumb")
                    && std::env::var("MN_CLI_OUTPUT").as_deref() != Ok("plain")
            });
        if let Some(dir) = &session {
            std::fs::write(dir.join("terminal"), b"open")?;
            print!("\x1b[?1049h\x1b[?25l");
        }
        Ok(Self {
            session,
            previous: vec![],
            keys: vec![],
            escape_started: None,
            input: wasip2::cli::stdin::get_stdin(),
        })
    }
    pub fn interactive(&self) -> bool {
        self.session.is_some()
    }
    pub fn draw(&mut self, view: &View, id: &str) -> io::Result<()> {
        let (width, height) = if let Some(dir) = &self.session {
            let size = std::fs::read_to_string(dir.join("size")).unwrap_or_default();
            let mut parts = size
                .split_whitespace()
                .filter_map(|p| p.parse::<usize>().ok());
            let height = parts.next().unwrap_or(24);
            (parts.next().unwrap_or(80), height)
        } else {
            (120, usize::from(u16::MAX))
        };
        let lines = view.lines(id, width, height, self.interactive());
        if lines == self.previous {
            return Ok(());
        }
        let mut out = io::stdout().lock();
        if self.interactive() {
            write!(out, "\x1b[H\x1b[2J")?;
            for (i, line) in lines.iter().enumerate() {
                write!(out, "\x1b[{};1H{line}", i + 1)?;
            }
        } else {
            for line in &lines {
                if !self.previous.contains(line) {
                    writeln!(out, "{line}")?;
                }
            }
        }
        self.previous = lines;
        out.flush()
    }
    pub fn input(&mut self, view: &mut View) -> io::Result<Input> {
        self.keys.extend(self.input.read(128).unwrap_or_default());
        if self.keys.is_empty() {
            return Ok(Input::Idle);
        }
        let mut keys = self.keys.as_slice();
        while let Some((&key, rest)) = keys.split_first() {
            keys = rest;
            match key {
                3 | b'q' => return Ok(Input::Detach),
                27 if keys.starts_with(b"[A") => {
                    self.escape_started = None;
                    view.move_agent(-1);
                    keys = &keys[2..];
                }
                27 if keys.starts_with(b"[B") => {
                    self.escape_started = None;
                    view.move_agent(1);
                    keys = &keys[2..];
                }
                27 if (keys.is_empty() || keys == b"[")
                    && self
                        .escape_started
                        .get_or_insert_with(Instant::now)
                        .elapsed()
                        < Duration::from_millis(150) =>
                {
                    // WASI can split a terminal arrow sequence across reads.
                    let consumed = self.keys.len() - keys.len() - 1;
                    self.keys.drain(..consumed);
                    return Ok(if consumed == 0 {
                        Input::Idle
                    } else {
                        Input::Redraw
                    });
                }
                27 => return Ok(Input::Detach),
                9 => view.detail = !view.detail,
                b'[' => view.page_children(-1),
                b']' => view.page_children(1),
                b'f' => view.child_page = None,
                _ => {}
            }
        }
        self.keys.clear();
        Ok(Input::Redraw)
    }
}
impl Drop for Screen {
    fn drop(&mut self) {
        if let Some(dir) = &self.session {
            print!("\x1b[?25h\x1b[?1049l");
            let _ = io::stdout().flush();
            let _ = std::fs::remove_file(dir.join("terminal"));
        }
    }
}
