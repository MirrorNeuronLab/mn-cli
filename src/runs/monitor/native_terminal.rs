use super::{Input, Key, View};
use ratatui::{
    backend::CrosstermBackend,
    crossterm::{
        cursor::{Hide, Show},
        event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
        execute,
        terminal::{self, EnterAlternateScreen, LeaveAlternateScreen},
    },
};
use std::{
    io::{self, Stdout},
    time::Duration,
};

pub(super) type Backend = CrosstermBackend<Stdout>;

pub(super) struct Session;

impl Session {
    pub fn open() -> io::Result<Option<Self>> {
        terminal::enable_raw_mode()?;
        let session = Self;
        execute!(io::stdout(), EnterAlternateScreen, Hide)?;
        Ok(Some(session))
    }

    pub fn backend(&self) -> Backend {
        CrosstermBackend::new(io::stdout())
    }

    pub fn input(&mut self, view: &mut View) -> io::Result<Input> {
        let mut action = Input::Idle;
        while event::poll(Duration::ZERO)? {
            let next = match event::read()? {
                Event::Resize(_, _) => Input::Redraw,
                Event::Key(key) if key.kind != KeyEventKind::Release => {
                    let key = match key.code {
                        KeyCode::Esc => Key::Detach,
                        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                            Key::Detach
                        }
                        KeyCode::Char(ch) => Key::Char(ch),
                        KeyCode::Up => Key::Up,
                        KeyCode::Down => Key::Down,
                        KeyCode::Enter => Key::Enter,
                        KeyCode::Backspace => Key::Backspace,
                        KeyCode::Tab => Key::Tab,
                        _ => continue,
                    };
                    key.apply(view)
                }
                _ => continue,
            };
            match next {
                Input::Detach => return Ok(next),
                Input::Redraw => action = next,
                Input::Idle => {}
            }
        }
        Ok(action)
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        let _ = execute!(io::stdout(), Show, LeaveAlternateScreen);
        let _ = terminal::disable_raw_mode();
    }
}
