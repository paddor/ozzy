//! The Logs screen: the broker process's recent diagnostics, newest first.

use super::{SELECTED, clamp};
use crate::{App, app::Selectable, format};
use ozzy_proto::admin::LogLevel;
use ratatui::{
    Frame,
    layout::{Constraint, Rect},
    style::Stylize,
    text::Span,
    widgets::{Block, Cell, Row, Table},
};

const WIDTHS: [Constraint; 4] = [
    Constraint::Length(11),
    Constraint::Length(5),
    Constraint::Length(16),
    Constraint::Min(0),
];

impl App {
    pub(super) fn draw_logs(&mut self, frame: &mut Frame<'_>, area: Rect) {
        let rows = self.logs.iter().rev().map(|entry| {
            let level = match entry.level {
                LogLevel::Info => Span::raw("info"),
                LogLevel::Warn => "warn".yellow(),
                LogLevel::Error => "error".red(),
            };
            Row::new([
                Cell::from(format::uptime(entry.uptime_ms)),
                Cell::from(level),
                Cell::from(entry.thread.clone()),
                Cell::from(entry.message.clone()),
            ])
        });
        let title = if self.logs.is_empty() {
            "Logs (none)"
        } else {
            "Logs (broker uptime, newest first)"
        };
        let table = Table::new(rows, WIDTHS)
            .header(Row::new(["Uptime", "Level", "Thread", "Message"]).bold())
            .block(Block::bordered().title(title))
            .row_highlight_style(SELECTED);
        self.page = area.height.saturating_sub(3) as usize;
        clamp(&mut self.log_table, self.logs.len());
        frame.render_stateful_widget(table, area, &mut self.log_table);
        self.target_rows(area, Selectable::Logs);
    }
}
