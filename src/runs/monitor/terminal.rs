use super::{dashboard, view::View};
use ratatui::Terminal;
use std::{
    env,
    io::{self, IsTerminal, Write},
};

#[cfg(not(target_os = "wasi"))]
#[path = "native_terminal.rs"]
mod platform;
#[cfg(target_os = "wasi")]
#[path = "wasi_terminal.rs"]
mod platform;

#[derive(PartialEq)]
pub(super) enum Input {
    Idle,
    Redraw,
    Detach,
}

enum Key {
    Up,
    Down,
    Enter,
    Backspace,
    Tab,
    Char(char),
    Detach,
}

impl Key {
    fn apply(self, view: &mut View) -> Input {
        match self {
            Self::Up => view.move_agent(-1),
            Self::Down => view.move_agent(1),
            Self::Enter => view.detail = true,
            Self::Backspace => view.detail = false,
            Self::Tab => view.detail = !view.detail,
            Self::Char('[') => view.page_children(-1),
            Self::Char(']') => view.page_children(1),
            Self::Char('f') => view.child_page = None,
            Self::Char('q') | Self::Detach => return Input::Detach,
            Self::Char(_) => return Input::Idle,
        }
        Input::Redraw
    }
}

pub(super) struct Screen {
    live: Option<(Terminal<platform::Backend>, platform::Session)>,
    previous: Vec<String>,
    json: bool,
}

impl Screen {
    pub fn open(json: bool) -> io::Result<Self> {
        let live = if !json
            && io::stdin().is_terminal()
            && io::stdout().is_terminal()
            && env::var("TERM").as_deref() != Ok("dumb")
            && env::var("MN_CLI_OUTPUT").as_deref() != Ok("plain")
            && let Some(session) = platform::Session::open()?
        {
            Some((Terminal::new(session.backend())?, session))
        } else {
            None
        };
        Ok(Self {
            live,
            previous: Vec::new(),
            json,
        })
    }

    pub fn interactive(&self) -> bool {
        self.live.is_some()
    }

    pub fn draw(&mut self, view: &View, id: &str, finished: bool) -> io::Result<()> {
        if let Some((terminal, _)) = &mut self.live {
            terminal.draw(|frame| dashboard::render(frame, view, id, finished))?;
        } else if !self.json {
            let lines = view.plain_lines(id);
            if lines != self.previous {
                let mut out = io::stdout().lock();
                for line in &lines {
                    if !self.previous.contains(line) {
                        writeln!(out, "{line}")?;
                    }
                }
                out.flush()?;
                self.previous = lines;
            }
        }
        Ok(())
    }

    pub fn input(&mut self, view: &mut View) -> io::Result<Input> {
        let Some((terminal, session)) = &mut self.live else {
            return Ok(Input::Idle);
        };
        let input = session.input(view)?;
        // The Wasmtime launcher reports resizes in a file, even when input is idle.
        if input == Input::Idle && terminal.size()? != terminal.current_buffer_mut().area.as_size()
        {
            return Ok(Input::Redraw);
        }
        Ok(input)
    }
}
