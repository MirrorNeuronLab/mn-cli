//! Ratatui's cell diffs over inherited WASI stdout. The launcher supplies the terminal size.
use ratatui::{
    backend::{Backend as RatatuiBackend, ClearType, WindowSize},
    buffer::Cell,
    layout::{Position, Size},
    style::{Color, Modifier},
};
use std::{
    io::{self, BufWriter, Stdout, Write},
    path::PathBuf,
};
use unicode_width::UnicodeWidthStr;

pub(in crate::runs::monitor::terminal) struct Backend {
    output: BufWriter<Stdout>,
    size_file: PathBuf,
    cursor: Position,
    color: bool,
}

impl Backend {
    pub(super) fn new(size_file: PathBuf) -> Self {
        Self {
            output: BufWriter::new(io::stdout()),
            size_file,
            cursor: Position::ORIGIN,
            color: std::env::var_os("NO_COLOR").is_none_or(|value| value.is_empty()),
        }
    }
}

impl RatatuiBackend for Backend {
    type Error = io::Error;

    fn draw<'a, I>(&mut self, content: I) -> io::Result<()>
    where
        I: Iterator<Item = (u16, u16, &'a Cell)>,
    {
        let mut next = None;
        let mut style = None;
        for (x, y, cell) in content {
            let position = Position::new(x, y);
            if next != Some(position) {
                self.set_cursor_position(position)?;
            }
            let current = (cell.fg, cell.bg, cell.modifier);
            if style != Some(current) {
                write!(self.output, "\x1b[0m")?;
                if self.color {
                    write_color(&mut self.output, cell.fg, 30)?;
                    write_color(&mut self.output, cell.bg, 40)?;
                }
                for (modifier, code) in [
                    (Modifier::BOLD, 1),
                    (Modifier::DIM, 2),
                    (Modifier::ITALIC, 3),
                    (Modifier::UNDERLINED, 4),
                    (Modifier::SLOW_BLINK, 5),
                    (Modifier::RAPID_BLINK, 6),
                    (Modifier::REVERSED, 7),
                    (Modifier::HIDDEN, 8),
                    (Modifier::CROSSED_OUT, 9),
                ] {
                    if cell.modifier.contains(modifier) {
                        write!(self.output, "\x1b[{code}m")?;
                    }
                }
                style = Some(current);
            }
            let symbol = cell.symbol();
            self.output.write_all(symbol.as_bytes())?;
            self.cursor = Position::new(x.saturating_add(symbol.width() as u16), y);
            next = Some(self.cursor);
        }
        if style.is_some() {
            write!(self.output, "\x1b[0m")?;
        }
        Ok(())
    }

    fn hide_cursor(&mut self) -> io::Result<()> {
        write!(self.output, "\x1b[?25l")
    }

    fn show_cursor(&mut self) -> io::Result<()> {
        write!(self.output, "\x1b[?25h")
    }

    fn get_cursor_position(&mut self) -> io::Result<Position> {
        Ok(self.cursor)
    }

    fn set_cursor_position<P: Into<Position>>(&mut self, position: P) -> io::Result<()> {
        self.cursor = position.into();
        let Position { x, y } = self.cursor;
        write!(
            self.output,
            "\x1b[{};{}H",
            u32::from(y) + 1,
            u32::from(x) + 1
        )
    }

    fn clear(&mut self) -> io::Result<()> {
        self.clear_region(ClearType::All)
    }

    fn clear_region(&mut self, region: ClearType) -> io::Result<()> {
        let command = match region {
            ClearType::All => "\x1b[2J",
            ClearType::AfterCursor => "\x1b[J",
            ClearType::BeforeCursor => "\x1b[1J",
            ClearType::CurrentLine => "\x1b[2K",
            ClearType::UntilNewLine => "\x1b[K",
        };
        self.output.write_all(command.as_bytes())
    }

    fn size(&self) -> io::Result<Size> {
        let size = std::fs::read_to_string(&self.size_file).unwrap_or_default();
        let mut dimensions = size.split_whitespace().map(|part| part.parse::<u16>().ok());
        let height = dimensions.next().flatten().filter(|n| *n > 0).unwrap_or(24);
        let width = dimensions.next().flatten().filter(|n| *n > 0).unwrap_or(80);
        Ok(Size::new(width, height))
    }

    fn window_size(&mut self) -> io::Result<WindowSize> {
        Ok(WindowSize {
            columns_rows: self.size()?,
            pixels: Size::default(),
        })
    }

    fn flush(&mut self) -> io::Result<()> {
        self.output.flush()
    }
}

fn write_color(out: &mut impl Write, color: Color, base: u8) -> io::Result<()> {
    let offset = match color {
        Color::Black => 0,
        Color::Red => 1,
        Color::Green => 2,
        Color::Yellow => 3,
        Color::Blue => 4,
        Color::Magenta => 5,
        Color::Cyan => 6,
        Color::Gray => 7,
        Color::DarkGray => 60,
        Color::LightRed => 61,
        Color::LightGreen => 62,
        Color::LightYellow => 63,
        Color::LightBlue => 64,
        Color::LightMagenta => 65,
        Color::LightCyan => 66,
        Color::White => 67,
        Color::Rgb(r, g, b) => return write!(out, "\x1b[{};2;{r};{g};{b}m", base + 8),
        Color::Indexed(n) => return write!(out, "\x1b[{};5;{n}m", base + 8),
        Color::Reset => return Ok(()),
    };
    write!(out, "\x1b[{}m", base + offset)
}
