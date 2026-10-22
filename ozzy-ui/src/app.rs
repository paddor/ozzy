//! Console state. It changes only through replies, link changes, keys, and
//! mouse events, and reads no clock: rates come from the broker's uptime in
//! each reply.

use crate::model::RecordRate;
use ozzy_proto::{
    GroupId,
    admin::{Broker, Connections, Counters, LogEntry, Partitions, Reply, Request},
};
use ratatui::{layout::Rect, widgets::TableState};
use std::collections::BTreeMap;

mod input;

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
    /// The broker process's recent diagnostics, newest first.
    Logs,
}

impl Screen {
    /// Every screen in tab order.
    pub const ALL: [Self; 6] = [
        Self::Broker,
        Self::Partitions,
        Self::Replication,
        Self::Groups,
        Self::Threads,
        Self::Logs,
    ];
    /// A single broker's screens: it replicates nothing.
    const SINGLE: [Self; 5] = [
        Self::Broker,
        Self::Partitions,
        Self::Groups,
        Self::Threads,
        Self::Logs,
    ];

    /// Tab title.
    pub fn title(self) -> &'static str {
        match self {
            Self::Broker => "Broker",
            Self::Partitions => "Partitions",
            Self::Replication => "Replication",
            Self::Groups => "Groups",
            Self::Threads => "Threads",
            Self::Logs => "Logs",
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
    Logs,
}

impl Selectable {
    /// The pane the table fills, on a two-pane screen.
    fn pane(self) -> Option<Pane> {
        match self {
            Self::Partitions | Self::Logs => None,
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
    pub(crate) logs: Vec<LogEntry>,
    pub(crate) log_table: TableState,
    /// Records per second of each partition; see [`RecordRate`].
    pub(crate) record_rates: BTreeMap<GroupId, RecordRate>,
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
            logs: Vec::new(),
            log_table: TableState::new().with_selected(0),
            record_rates: BTreeMap::new(),
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
        let mut requests = Vec::with_capacity(5);
        if self.ask_broker {
            requests.push(Request::Broker);
        }
        requests.extend([
            Request::Counters,
            Request::Partitions,
            Request::Connections,
            Request::Logs,
        ]);
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
                self.track_records(&partitions);
                self.keep_logs(&mut partitions);
                self.ask_broker |= self.partitions.push(partitions);
            }
            Reply::Connections(connections) => self.connections = Some(connections),
            Reply::Logs(logs) => self.logs = logs,
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
}
