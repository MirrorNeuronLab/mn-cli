use super::view::View;
use crossterm::{
    cursor::{Hide, MoveTo, Show},
    event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
    execute, queue,
    terminal::{self, Clear, ClearType, EnterAlternateScreen, LeaveAlternateScreen},
};
use std::{
    env,
    io::{self, IsTerminal, Write},
    time::Duration,
};

pub(super) enum Input {
    Idle,
    Redraw,
    Detach,
}

pub(super) struct Screen {
    interactive: bool,
    previous: Vec<String>,
}

impl Screen {
    pub fn open(json: bool) -> io::Result<Self> {
        let interactive = !json
            && io::stdin().is_terminal()
            && io::stdout().is_terminal()
            && env::var("TERM").as_deref() != Ok("dumb")
            && env::var("MN_CLI_OUTPUT").as_deref() != Ok("plain");
        let screen = Self {
            interactive,
            previous: vec![],
        };
        if interactive {
            terminal::enable_raw_mode()?;
            execute!(io::stdout(), EnterAlternateScreen, Hide)?;
        }
        Ok(screen)
    }
    pub fn draw(&mut self, view: &View, id: &str) -> io::Result<()> {
        let (width, height) = if self.interactive {
            terminal::size().unwrap_or((80, 24))
        } else {
            (120, u16::MAX)
        };
        let lines = view.lines(
            id,
            usize::from(width),
            usize::from(height),
            self.interactive,
        );
        if lines == self.previous {
            return Ok(());
        }
        let mut out = io::stdout().lock();
        if self.interactive {
            queue!(out, MoveTo(0, 0), Clear(ClearType::All))?;
            for (i, line) in lines.iter().enumerate() {
                queue!(out, MoveTo(0, i as u16))?;
                write!(out, "{line}")?;
            }
        } else {
            for line in &lines {
                if !self.previous.contains(line) {
                    writeln!(out, "{line}")?;
                }
            }
        }
        out.flush()?;
        self.previous = lines;
        Ok(())
    }
    pub fn interactive(&self) -> bool {
        self.interactive
    }
    pub fn input(&mut self, view: &mut View) -> io::Result<Input> {
        let mut action = Input::Idle;
        while event::poll(Duration::ZERO)? {
            let input = event::read()?;
            if matches!(input, Event::Resize(_, _)) {
                self.previous.clear();
                action = Input::Redraw;
            }
            if let Event::Key(key) = input {
                if key.kind == KeyEventKind::Release {
                    continue;
                }
                match key.code {
                    KeyCode::Char('q') | KeyCode::Esc => return Ok(Input::Detach),
                    KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        return Ok(Input::Detach);
                    }
                    KeyCode::Up => view.move_agent(-1),
                    KeyCode::Down => view.move_agent(1),
                    KeyCode::Tab => view.detail = !view.detail,
                    KeyCode::Char('[') => view.page_children(-1),
                    KeyCode::Char(']') => view.page_children(1),
                    KeyCode::Char('f') => view.child_page = None,
                    _ => continue,
                }
                action = Input::Redraw;
            }
        }
        Ok(action)
    }
}
impl Drop for Screen {
    fn drop(&mut self) {
        if self.interactive {
            let _ = execute!(io::stdout(), Show, LeaveAlternateScreen);
            let _ = terminal::disable_raw_mode();
        }
    }
}
