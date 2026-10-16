//! Console state. It changes only through replies, link changes, keys, and
//! mouse events, and reads no clock: rates come from the broker's uptime in
//! each reply.

use ozzy_proto::admin::{Broker, Connections, Counters, Partitions, Reply, Request};
use ratatui::{
    layout::{Position, Rect},
    widgets::TableState,
};

/// Console screens in tab order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Screen {
    /// Broker facts, cluster members, a partition summary, and key counters.
    Broker,
    /// One row per local partition: state, records, and operations.
    Partitions,
    /// Journal and follower progress per partition, links, and repairs.
    Replication,
    /// Consumer groups: members and each partition's cursor and lag.
    Groups,
    /// Every event counter, totaled or for one thread.
    Threads,
}

impl Screen {
    /// Every screen in tab order.
    pub const ALL: [Self; 5] = [
        Self::Broker,
        Self::Partitions,
        Self::Replication,
        Self::Groups,
        Self::Threads,
    ];
    /// A single broker's screens: it replicates nothing.
    const SINGLE: [Self; 4] = [Self::Broker, Self::Partitions, Self::Groups, Self::Threads];

    /// Tab title.
    pub fn title(self) -> &'static str {
        match self {
            Self::Broker => "Broker",
            Self::Partitions => "Partitions",
            Self::Replication => "Replication",
            Self::Groups => "Groups",
            Self::Threads => "Threads",
        }
    }
}

/// A key press, independent of the terminal or browser library.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Key {
    /// A printable character.
    Char(char),
    /// Arrow up.
    Up,
    /// Arrow down.
    Down,
    /// Arrow left.
    Left,
    /// Arrow right.
    Right,
    /// Page up.
    PageUp,
    /// Page down.
    PageDown,
    /// Home.
    Home,
    /// End.
    End,
    /// Tab.
    Tab,
    /// Shift+Tab.
    BackTab,
    /// Escape.
    Esc,
}

/// A mouse event, independent of the terminal or browser library.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mouse {
    /// The primary button went down.
    Click,
    /// The wheel turned up.
    ScrollUp,
    /// The wheel turned down.
    ScrollDown,
}

/// What the frontend does after a key or mouse event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    /// Nothing beyond drawing again.
    None,
    /// Leave the console.
    Quit,
    /// Refresh now instead of at the next interval.
    Refresh,
    /// Send a confirmed command; its reply goes to [`App::command_reply`].
    Command(Request),
}

/// The latest command, shown under the screen until the next one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Command {
    /// Waiting for `y`; any other key cancels.
    Asking {
        request: Request,
        label: String,
    },
    Running(String),
    Done(String),
    Failed(String),
}

/// The broker link as the frontend last saw it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Link {
    /// No broker has finished a handshake yet.
    Connecting,
    /// A broker finished its handshake.
    Up,
    /// The broker went away; screens show its last replies.
    Lost,
}

/// The panes of a two-pane screen: a list and the selected entry's detail.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Pane {
    List,
    Detail,
}

/// A table with a selected row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Selectable {
    /// Shared by the Partitions and Replication screens.
    Partitions,
    Groups,
    Cursors,
    Threads,
    Events,
}

impl Selectable {
    /// The pane the table fills, on a two-pane screen.
    fn pane(self) -> Option<Pane> {
        match self {
            Self::Partitions => None,
            Self::Groups | Self::Threads => Some(Pane::List),
            Self::Cursors | Self::Events => Some(Pane::Detail),
        }
    }
}

/// Something the last draw put where the mouse can reach it.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Target {
    /// A screen's tab title.
    Tab(Screen),
    /// A table's rows, starting at its first shown row.
    Rows { table: Selectable, offset: usize },
}

/// A reply taken at a broker uptime.
pub(crate) trait Sample {
    fn uptime_ms(&self) -> u64;
}

impl Sample for Counters {
    fn uptime_ms(&self) -> u64 {
        self.uptime_ms
    }
}

impl Sample for Partitions {
    fn uptime_ms(&self) -> u64 {
        self.uptime_ms
    }
}

/// The latest reply and the one before it, for rates.
#[derive(Debug)]
pub(crate) struct Samples<T> {
    pub(crate) current: Option<T>,
    previous: Option<T>,
}

impl<T: Sample> Samples<T> {
    const fn new() -> Self {
        Self {
            current: None,
            previous: None,
        }
    }

    /// Keep `sample`. Returns whether the broker restarted, which drops the
    /// previous sample.
    fn push(&mut self, sample: T) -> bool {
        let restarted = self
            .current
            .as_ref()
            .is_some_and(|current| sample.uptime_ms() < current.uptime_ms());
        self.previous = if restarted { None } else { self.current.take() };
        self.current = Some(sample);
        restarted
    }

    /// The previous sample and the milliseconds since it, when both exist.
    pub(crate) fn previous(&self) -> Option<(&T, u64)> {
        let current = self.current.as_ref()?;
        let previous = self.previous.as_ref()?;
        let elapsed = current.uptime_ms().checked_sub(previous.uptime_ms())?;
        (elapsed > 0).then_some((previous, elapsed))
    }
}

/// Everything one console shows. Frontends feed it replies, link changes,
/// and keys, then call [`App::draw`].
#[derive(Debug)]
pub struct App {
    pub(crate) endpoint: String,
    pub(crate) screen: Screen,
    pub(crate) link: Link,
    pub(crate) broker: Option<Broker>,
    /// Ask for broker facts again: at start, after a lost link, and after a
    /// restart.
    ask_broker: bool,
    pub(crate) counters: Samples<Counters>,
    pub(crate) partitions: Samples<Partitions>,
    pub(crate) connections: Option<Connections>,
    /// The latest failure; the next reply clears it.
    pub(crate) error: Option<String>,
    pub(crate) command: Option<Command>,
    /// Shared by the Partitions and Replication screens.
    pub(crate) partition_table: TableState,
    pub(crate) group_table: TableState,
    pub(crate) cursor_table: TableState,
    pub(crate) thread_table: TableState,
    pub(crate) event_table: TableState,
    pub(crate) focus: Pane,
    pub(crate) show_zero: bool,
    /// Rows a page key moves, from the last drawn table.
    pub(crate) page: usize,
    /// Where the last draw put tabs and table rows.
    pub(crate) targets: Vec<(Rect, Target)>,
}

impl App {
    /// A console of the broker at `endpoint`, before any reply.
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            screen: Screen::Broker,
            link: Link::Connecting,
            broker: None,
            ask_broker: true,
            counters: Samples::new(),
            partitions: Samples::new(),
            connections: None,
            error: None,
            command: None,
            partition_table: TableState::new().with_selected(0),
            group_table: TableState::new().with_selected(0),
            cursor_table: TableState::new().with_selected(0),
            thread_table: TableState::new().with_selected(0),
            event_table: TableState::new().with_selected(0),
            focus: Pane::List,
            show_zero: false,
            page: 10,
            targets: Vec::new(),
        }
    }

    /// The selected screen.
    pub fn screen(&self) -> Screen {
        self.screen
    }

    /// The screens in tab order. A single broker has no Replication screen.
    pub fn screens(&self) -> &'static [Screen] {
        if self.single() {
            &Screen::SINGLE
        } else {
            &Screen::ALL
        }
    }

    /// Whether the broker runs without peers.
    pub(crate) fn single(&self) -> bool {
        self.broker
            .as_ref()
            .is_some_and(|broker| broker.brokers.len() == 1)
    }

    /// The selected screen's tab position.
    pub(crate) fn tab(&self) -> usize {
        self.screens()
            .iter()
            .position(|&screen| screen == self.screen)
            .unwrap_or(0)
    }

    /// What the next refresh asks, in order.
    pub fn requests(&self) -> Vec<Request> {
        let mut requests = Vec::with_capacity(4);
        if self.ask_broker {
            requests.push(Request::Broker);
        }
        requests.extend([Request::Counters, Request::Partitions, Request::Connections]);
        requests
    }

    /// Whether a broker has finished its handshake with this console.
    pub fn link(&mut self, up: bool) {
        self.link = match (up, self.link) {
            (true, _) => Link::Up,
            (false, Link::Connecting) => Link::Connecting,
            (false, _) => {
                self.ask_broker = true;
                Link::Lost
            }
        };
    }

    /// Take a broker reply.
    pub fn reply(&mut self, reply: Reply) {
        self.error = None;
        match reply {
            Reply::Broker(broker) => {
                self.broker = Some(broker);
                self.ask_broker = false;
                if !self.screens().contains(&self.screen) {
                    self.screen = Screen::Partitions;
                }
            }
            Reply::Counters(counters) => self.ask_broker |= self.counters.push(counters),
            Reply::Partitions(mut partitions) => {
                self.keep_logs(&mut partitions);
                self.ask_broker |= self.partitions.push(partitions);
            }
            Reply::Connections(connections) => self.connections = Some(connections),
            // Only commands are answered with it.
            deleted @ Reply::GroupDeleted(_) => self.command_reply(deleted),
            Reply::Error(message) => self.error = Some(message),
        }
    }

    /// A busy journal answers without its committed state. Keep the last one
    /// the same process reported, so groups and offsets do not flicker.
    fn keep_logs(&self, partitions: &mut Partitions) {
        let Some(current) = &self.partitions.current else {
            return;
        };
        if partitions.uptime_ms < current.uptime_ms {
            return;
        }
        for row in partitions.rows.iter_mut().filter(|row| row.log.is_none()) {
            row.log = current
                .rows
                .iter()
                .find(|old| old.group == row.group)
                .and_then(|old| old.log.clone());
        }
    }

    /// A request failed: no reply in time, or one that did not decode.
    pub fn failed(&mut self, message: impl Into<String>) {
        self.error = Some(message.into());
    }

    /// Take the reply to the running command.
    pub fn command_reply(&mut self, reply: Reply) {
        let Some(Command::Running(label)) = self.command.take() else {
            return;
        };
        self.command = Some(match reply {
            Reply::GroupDeleted(Some(group)) => Command::Done(format!("deleted {label} ({group})")),
            Reply::GroupDeleted(None) => Command::Done(format!("no group {label}")),
            Reply::Error(message) => Command::Failed(message),
            _ => Command::Failed(format!("{label}: unexpected reply")),
        });
    }

    /// The running command failed: not sent, or no reply in time.
    pub fn command_failed(&mut self, message: impl Into<String>) {
        if let Some(Command::Running(label)) = &self.command {
            self.command = Some(Command::Failed(format!("{label}: {}", message.into())));
        }
    }

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
        })
    }

    fn rows(&self, table: Selectable) -> usize {
        match table {
            Selectable::Partitions => self.partition_rows().len(),
            Selectable::Groups => self.group_rows().len(),
            Selectable::Cursors => self.selected_group().map_or(0, |group| group.cursors.len()),
            Selectable::Threads => self.thread_names().len(),
            Selectable::Events => self.event_rows().len(),
        }
    }

    pub(crate) fn table(&mut self, table: Selectable) -> &mut TableState {
        match table {
            Selectable::Partitions => &mut self.partition_table,
            Selectable::Groups => &mut self.group_table,
            Selectable::Cursors => &mut self.cursor_table,
            Selectable::Threads => &mut self.thread_table,
            Selectable::Events => &mut self.event_table,
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
