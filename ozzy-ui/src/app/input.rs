//! Keys and mouse events: screen and pane changes, commands, and selection.

use super::{Action, App, Command, Key, Mouse, Pane, Screen, Selectable, Target};
use ozzy_proto::admin::Request;
use ratatui::{layout::Position, widgets::TableState};

impl App {
    /// Ask to delete the selected group, when its plan names it and no
    /// command runs.
    fn ask_delete(&mut self) {
        if matches!(self.command, Some(Command::Running(_))) {
            return;
        }
        let Some(group) = self.selected_group() else {
            self.command = Some(Command::Failed("no group to delete".into()));
            return;
        };
        let (Some(topic), Some(status)) = (group.topic, group.status) else {
            let unnamed = format!("group {} has no plan here, so no name", group.label);
            self.command = Some(Command::Failed(unnamed));
            return;
        };
        let request = Request::DeleteGroup {
            topic: topic.to_owned(),
            name: status.name.clone(),
        };
        let label = group.label.clone();
        self.command = Some(Command::Asking { request, label });
    }

    /// Apply `key` and say what the frontend does next.
    pub fn key(&mut self, key: Key) -> Action {
        if matches!(self.command, Some(Command::Asking { .. }))
            && let Some(Command::Asking { request, label }) = self.command.take()
        {
            if key != Key::Char('y') {
                return Action::None;
            }
            self.command = Some(Command::Running(label));
            return Action::Command(request);
        }
        match key {
            Key::Char('q') | Key::Esc => return Action::Quit,
            Key::Char('r') => return Action::Refresh,
            Key::Char(digit @ '1'..='9') => {
                if let Some(&screen) = digit
                    .to_digit(10)
                    .and_then(|digit| self.screens().get(digit as usize - 1))
                {
                    self.screen = screen;
                }
            }
            Key::Tab | Key::BackTab => {
                let screens = self.screens();
                let step = if key == Key::Tab {
                    1
                } else {
                    screens.len() - 1
                };
                self.screen = screens[(self.tab() + step) % screens.len()];
            }
            Key::Char('z') => self.show_zero = !self.show_zero,
            Key::Char('d') if self.screen == Screen::Groups => self.ask_delete(),
            Key::Left | Key::Char('h') => self.focus = Pane::List,
            Key::Right | Key::Char('l') => self.focus = Pane::Detail,
            key => {
                if let (Some(movement), Some(table)) = (self.movement(key), self.focused()) {
                    self.scroll(table, movement);
                }
            }
        }
        Action::None
    }

    /// Apply `mouse` at terminal cell (`column`, `row`). A click picks a tab
    /// or a row; the wheel moves the selection of the table under it. Both
    /// focus the pane they hit.
    pub fn mouse(&mut self, mouse: Mouse, column: u16, row: u16) -> Action {
        let Some(&(area, target)) = self
            .targets
            .iter()
            .find(|(area, _)| area.contains(Position::new(column, row)))
        else {
            return Action::None;
        };
        match target {
            Target::Tab(screen) => {
                if mouse == Mouse::Click {
                    self.screen = screen;
                }
            }
            Target::Rows { table, offset } => {
                if let Some(pane) = table.pane() {
                    self.focus = pane;
                }
                match mouse {
                    Mouse::Click => {
                        let index = offset + usize::from(row - area.y);
                        if index < self.rows(table) {
                            self.table(table).select(Some(index));
                        }
                    }
                    Mouse::ScrollUp => self.scroll(table, Move::By(-1)),
                    Mouse::ScrollDown => self.scroll(table, Move::By(1)),
                }
            }
        }
        Action::None
    }

    fn movement(&self, key: Key) -> Option<Move> {
        let page = isize::try_from(self.page.max(1)).unwrap_or(isize::MAX);
        Some(match key {
            Key::Up | Key::Char('k') => Move::By(-1),
            Key::Down | Key::Char('j') => Move::By(1),
            Key::PageUp => Move::By(-page),
            Key::PageDown => Move::By(page),
            Key::Home | Key::Char('g') => Move::First,
            Key::End | Key::Char('G') => Move::Last,
            _ => return None,
        })
    }

    /// The table that selection keys move on this screen.
    fn focused(&self) -> Option<Selectable> {
        Some(match (self.screen, self.focus) {
            (Screen::Broker, _) => return None,
            (Screen::Partitions | Screen::Replication, _) => Selectable::Partitions,
            (Screen::Groups, Pane::List) => Selectable::Groups,
            (Screen::Groups, Pane::Detail) => Selectable::Cursors,
            (Screen::Threads, Pane::List) => Selectable::Threads,
            (Screen::Threads, Pane::Detail) => Selectable::Events,
            (Screen::Logs, _) => Selectable::Logs,
        })
    }

    fn rows(&self, table: Selectable) -> usize {
        match table {
            Selectable::Partitions => self.partition_rows().len(),
            Selectable::Groups => self.group_rows().len(),
            Selectable::Cursors => self.selected_group().map_or(0, |group| group.cursors.len()),
            Selectable::Threads => self.thread_names().len(),
            Selectable::Events => self.event_rows().len(),
            Selectable::Logs => self.logs.len(),
        }
    }

    pub(crate) fn table(&mut self, table: Selectable) -> &mut TableState {
        match table {
            Selectable::Partitions => &mut self.partition_table,
            Selectable::Groups => &mut self.group_table,
            Selectable::Cursors => &mut self.cursor_table,
            Selectable::Threads => &mut self.thread_table,
            Selectable::Events => &mut self.event_table,
            Selectable::Logs => &mut self.log_table,
        }
    }

    fn scroll(&mut self, table: Selectable, movement: Move) {
        let last = self.rows(table).saturating_sub(1);
        let state = self.table(table);
        let selected = state.selected().unwrap_or(0);
        state.select(Some(match movement {
            Move::By(step) => selected.saturating_add_signed(step).min(last),
            Move::First => 0,
            Move::Last => last,
        }));
    }
}

#[derive(Clone, Copy, Debug)]
enum Move {
    By(isize),
    First,
    Last,
}
