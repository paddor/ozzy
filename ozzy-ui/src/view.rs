//! Screens drawn from [`App`] state, with ratatui widgets only.

use crate::{
    App, Screen,
    app::{Command, Link, Pane, Selectable, Target},
    format,
    model::{CursorRow, EventRow, GroupRow, OVERVIEW},
};
use ozzy_proto::admin::EventKind;
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Margin, Rect},
    style::{Color, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, Cell, Row, Table, TableState, Tabs},
};
use std::borrow::Cow;

mod logs;
mod partitions;

const SELECTED: Style = Style::new().bg(Color::DarkGray);
const FOCUSED: Style = Style::new().fg(Color::Cyan);
const CURSOR_COLUMNS: [Column; 5] = [
    column("Partition", 9, true),
    column("Owner", 36, true),
    column("Committed", 11, true),
    column("Next", 11, false),
    column("Lag", 9, true),
];
const CLUSTER_COLUMNS: [Column; 4] = [
    column("Broker", 6, true),
    column("ID", 36, false),
    column("Link", 11, true),
    column("Leads", 5, true),
];
const GROUP_COLUMNS: [Column; 2] = [column("Group", 5, true), column("Lag", 11, true)];
const MEMBER_COLUMNS: [Column; 3] = [
    column("Member", 6, true),
    column("Client", 36, false),
    column("Owns", 4, true),
];
/// Width of the label and value panes on the Broker screen; fits a UUID.
const FACTS: u16 = 53;
/// Widest label column; longer labels are cut.
const MAX_LABEL: u16 = 40;

impl App {
    /// Draw the selected screen with its tab bar and key hints.
    pub fn draw(&mut self, frame: &mut Frame<'_>) {
        let [top, body, bottom] = Layout::vertical([
            Constraint::Length(1),
            Constraint::Min(0),
            Constraint::Length(1),
        ])
        .areas(frame.area());
        self.targets.clear();
        self.draw_tabs(frame, top);
        match self.screen {
            Screen::Broker => self.draw_broker(frame, body),
            Screen::Partitions => self.draw_partitions(frame, body),
            Screen::Replication => self.draw_replication(frame, body),
            Screen::Groups => self.draw_groups(frame, body),
            Screen::Threads => self.draw_threads(frame, body),
            Screen::Logs => self.draw_logs(frame, body),
        }
        self.draw_hints(frame, bottom);
    }

    fn draw_tabs(&mut self, frame: &mut Frame<'_>, area: Rect) {
        let link = match self.link {
            Link::Connecting => "connecting".yellow(),
            Link::Up => "connected".green(),
            Link::Lost => "lost".red(),
        };
        let name = self.broker.as_ref().map_or("", |broker| &broker.name);
        let status = Line::from(vec![
            Span::raw(format!("{name} ")).bold(),
            link,
            Span::raw(" "),
        ]);
        let [tabs, status_area] = Layout::horizontal([
            Constraint::Min(0),
            Constraint::Length(status.width() as u16),
        ])
        .areas(area);
        let screens = self.screens();
        let titles = screens
            .iter()
            .enumerate()
            .map(|(index, screen)| format!("{} {}", index + 1, screen.title()))
            .collect::<Vec<_>>();
        // Tabs pads each title with a space on both sides and puts a
        // one-column divider between them.
        let mut x = tabs.x;
        for (&screen, title) in screens.iter().zip(&titles) {
            let width = title.len() as u16 + 2;
            let tab = Rect::new(x, tabs.y, width, 1).intersection(tabs);
            self.targets.push((tab, Target::Tab(screen)));
            x = x.saturating_add(width + 1);
        }
        frame.render_widget(
            Tabs::new(titles)
                .select(self.tab())
                .highlight_style(Style::new().reversed()),
            tabs,
        );
        frame.render_widget(status, status_area);
    }

    fn draw_hints(&self, frame: &mut Frame<'_>, area: Rect) {
        if let Some(Command::Asking { label, .. }) = &self.command {
            let prompt = format!("Delete group {label}? y deletes, any other key cancels");
            frame.render_widget(prompt.yellow().bold(), area);
            return;
        }
        let select = match self.screen {
            Screen::Broker => "",
            Screen::Partitions | Screen::Replication | Screen::Logs => "  Up/Down/PgUp/PgDn select",
            Screen::Groups => "  Left/Right pane  Up/Down select  d delete",
            Screen::Threads => "  Left/Right pane  Up/Down select  z zeros",
        };
        let hints = format!(
            "q quit  Tab/1-{} screen  r refresh{select}",
            self.screens().len()
        );
        let status = match (&self.error, &self.command) {
            (Some(error), _) | (None, Some(Command::Failed(error))) => error.clone().red(),
            (None, Some(Command::Running(label))) => format!("deleting {label}").yellow(),
            (None, Some(Command::Done(done))) => done.clone().green(),
            (None, Some(Command::Asking { .. }) | None) => Span::raw(""),
        };
        let [hints_area, status_area] =
            Layout::horizontal([Constraint::Length(hints.len() as u16), Constraint::Min(0)])
                .spacing(2)
                .areas(area);
        frame.render_widget(hints.dark_gray(), hints_area);
        frame.render_widget(Line::from(status).right_aligned(), status_area);
    }

    fn draw_broker(&self, frame: &mut Frame<'_>, area: Rect) {
        let facts = self.facts();
        let members = self
            .broker
            .as_ref()
            .map_or(0, |broker| broker.brokers.len());
        // Borders and the cluster table's header.
        let [top, bottom] = Layout::vertical([
            Constraint::Length(facts.len().max(members + 1) as u16 + 2),
            Constraint::Min(0),
        ])
        .areas(area);
        let [facts_area, cluster] =
            Layout::horizontal([Constraint::Length(FACTS), Constraint::Min(0)]).areas(top);
        let [summary, activity] =
            Layout::horizontal([Constraint::Length(FACTS), Constraint::Min(0)]).areas(bottom);
        frame.render_widget(pairs("Broker", facts), facts_area);
        if !self.single() {
            frame.render_widget(self.cluster(cluster.width), cluster);
        }
        frame.render_widget(pairs("Partitions", self.summary_pairs()), summary);
        // A single broker replicates nothing.
        let overview = OVERVIEW
            .into_iter()
            .filter(|(name, _)| !self.single() || !name.starts_with("replica_"))
            .collect::<Vec<_>>();
        frame.render_widget(counters("Activity", self.labeled(&overview)), activity);
    }

    fn facts(&self) -> Vec<(&'static str, String)> {
        let admin = ("Admin", self.endpoint.clone());
        let Some(broker) = &self.broker else {
            return vec![admin, ("", "waiting for the broker".into())];
        };
        let uptime = self
            .counters
            .current
            .as_ref()
            .map_or(broker.uptime_ms, |counters| counters.uptime_ms);
        let clients = self
            .connections
            .as_ref()
            .map_or_else(|| "?".into(), |links| format::count(links.clients.into()));
        vec![
            ("Name", broker.name.clone()),
            ("ID", broker.id.to_string()),
            ("PID", broker.pid.to_string()),
            ("Uptime", format::uptime(uptime)),
            ("Shards", broker.shards.to_string()),
            (
                "Clients",
                format!("{clients} of {}", format::count(broker.max_clients.into())),
            ),
            admin,
        ]
    }

    fn cluster(&self, width: u16) -> Table<'static> {
        let rows = self
            .broker
            .iter()
            .flat_map(|broker| &broker.brokers)
            .map(|(name, id)| {
                let cells = vec![
                    Cell::from(id.to_string()),
                    self.link_cell(*id),
                    Cell::from(self.leads(*id).to_string()),
                ];
                (name.clone(), cells)
            })
            .collect();
        fitted(&CLUSTER_COLUMNS, rows, width).block(Block::bordered().title("Cluster"))
    }

    /// This broker's link to `broker`.
    fn link_cell(&self, broker: ozzy_proto::NodeId) -> Cell<'static> {
        if self.broker.as_ref().is_some_and(|facts| facts.id == broker) {
            return Cell::from("this broker");
        }
        match self.link_up(broker) {
            Some(true) => "up".green().into(),
            Some(false) => "down".red().into(),
            None => Cell::from(""),
        }
    }

    fn summary_pairs(&self) -> Vec<(&'static str, String)> {
        let summary = self.summary();
        let missing = self.missing_shards();
        let mut pairs = vec![
            ("Local", summary.partitions.to_string()),
            ("Ready", summary.ready.to_string()),
        ];
        if !self.single() {
            pairs.extend([
                ("Electing", summary.electing.to_string()),
                ("Led here", summary.leading.to_string()),
            ]);
        }
        pairs.extend([
            ("Disk pending", summary.disk_pending.to_string()),
            (
                "Pending",
                format!(
                    "{} ops, {}",
                    format::count(summary.pending_operations),
                    format::bytes(summary.pending_body_bytes as f64)
                ),
            ),
            ("Reclaimed", format::bytes(summary.reclaimed_bytes as f64)),
            ("Commits/s", format::rate(summary.rate)),
            ("Records/s", format::rate(summary.records)),
            ("Groups", self.group_rows().len().to_string()),
        ]);
        if missing > 0 {
            pairs.push(("Shards silent", missing.to_string()));
        }
        pairs
    }

    fn draw_groups(&mut self, frame: &mut Frame<'_>, area: Rect) {
        let [panes, detail_area] =
            Layout::vertical([Constraint::Min(0), Constraint::Length(1)]).areas(area);
        let [list_area, right] =
            Layout::horizontal([Constraint::Length(30), Constraint::Min(0)]).areas(panes);
        let focused = self.focused_style();
        let groups = self.group_rows();
        let count = groups.len();
        let list = groups
            .iter()
            .map(|group| {
                let lag = Cell::from(format::count(group.lag()));
                (group.label.clone(), vec![lag])
            })
            .collect();
        let list = fitted(&GROUP_COLUMNS, list, list_area.width)
            .block(
                Block::bordered()
                    .title(if count == 0 {
                        "Groups (none)"
                    } else {
                        "Groups"
                    })
                    .border_style(focused(Pane::List)),
            )
            .row_highlight_style(SELECTED);
        let selected = groups.get(selected_index(&self.group_table, count));
        let members = selected
            .and_then(|group| group.status)
            .map_or(0, |status| status.members.len());
        let [members_area, cursors_area] =
            Layout::vertical([Constraint::Length(members as u16 + 3), Constraint::Min(0)])
                .areas(right);
        let members_table = members_table(selected, members_area.width);
        let cursor_rows = selected.map_or_else(Vec::new, |group| {
            group
                .cursors
                .iter()
                .map(|row| (row.partition.clone(), cursor_cells(row)))
                .collect()
        });
        let cursor_count = cursor_rows.len();
        let cursors = fitted(&CURSOR_COLUMNS, cursor_rows, cursors_area.width)
            .block(
                Block::bordered()
                    .title("Cursors (record offsets)")
                    .border_style(focused(Pane::Detail)),
            )
            .row_highlight_style(SELECTED);
        let detail = selected.map_or_else(String::new, |group| format!("group {}", group.id));
        self.page = cursors_area.height.saturating_sub(3) as usize;
        clamp(&mut self.group_table, count);
        clamp(&mut self.cursor_table, cursor_count);
        frame.render_stateful_widget(list, list_area, &mut self.group_table);
        self.target_rows(list_area, Selectable::Groups);
        frame.render_widget(members_table, members_area);
        frame.render_stateful_widget(cursors, cursors_area, &mut self.cursor_table);
        self.target_rows(cursors_area, Selectable::Cursors);
        frame.render_widget(detail.dark_gray(), detail_area);
    }

    fn draw_threads(&mut self, frame: &mut Frame<'_>, area: Rect) {
        let [threads_area, events_area] =
            Layout::horizontal([Constraint::Length(28), Constraint::Min(0)]).areas(area);
        let names = self
            .thread_names()
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>();
        clamp(&mut self.thread_table, names.len());
        let thread = self
            .thread_table
            .selected()
            .and_then(|index| names.get(index))
            .cloned()
            .unwrap_or_default();
        let events = self
            .event_rows()
            .iter()
            .map(|row| Row::new(event_cells(row)))
            .collect::<Vec<_>>();
        let zeros = if self.show_zero {
            ""
        } else {
            " (zeros hidden)"
        };
        let focused = self.focused_style();
        let threads = Table::new(
            names.into_iter().map(|name| Row::new([name])),
            [Constraint::Min(0)],
        )
        .header(header(["Thread"]))
        .block(Block::bordered().border_style(focused(Pane::List)))
        .row_highlight_style(SELECTED);
        let event_count = events.len();
        let events = Table::new(
            events,
            [
                Constraint::Min(24),
                Constraint::Length(16),
                Constraint::Length(10),
            ],
        )
        .header(header(["Event", "Value", "/s"]))
        .block(
            Block::bordered()
                .title(format!("{thread}{zeros}"))
                .border_style(focused(Pane::Detail)),
        )
        .row_highlight_style(SELECTED);
        self.page = area.height.saturating_sub(3) as usize;
        clamp(&mut self.event_table, event_count);
        frame.render_stateful_widget(threads, threads_area, &mut self.thread_table);
        frame.render_stateful_widget(events, events_area, &mut self.event_table);
        self.target_rows(threads_area, Selectable::Threads);
        self.target_rows(events_area, Selectable::Events);
    }

    /// Let the mouse reach the rows of `table`, drawn in `area` under a
    /// border and a header line.
    fn target_rows(&mut self, area: Rect, table: Selectable) {
        let inner = area.inner(Margin::new(1, 1));
        let rows = Rect {
            y: inner.y.saturating_add(1),
            height: inner.height.saturating_sub(1),
            ..inner
        };
        let offset = self.table(table).offset();
        self.targets.push((rows, Target::Rows { table, offset }));
    }

    /// The border style of `pane`: highlighted when it has the focus.
    fn focused_style(&self) -> impl Fn(Pane) -> Style + use<> {
        let focus = self.focus;
        move |pane| {
            if focus == pane { FOCUSED } else { Style::new() }
        }
    }
}

/// A table column: title, width, and whether a narrow screen keeps it.
#[derive(Clone, Debug)]
struct Column {
    title: Cow<'static, str>,
    width: u16,
    keep: bool,
}

const fn column(title: &'static str, width: u16, keep: bool) -> Column {
    Column {
        title: Cow::Borrowed(title),
        width,
        keep,
    }
}

/// A table of labeled rows within `width`. The label column fits the
/// longest label, up to [`MAX_LABEL`]; the others keep their widths. When
/// not every column fits, only the kept ones show.
fn fitted(
    columns: &[Column],
    rows: Vec<(String, Vec<Cell<'static>>)>,
    width: u16,
) -> Table<'static> {
    let longest = rows.iter().map(|(label, _)| label.len()).max().unwrap_or(0);
    let label = u16::try_from(longest)
        .unwrap_or(MAX_LABEL)
        .clamp(columns[0].width, MAX_LABEL);
    let widths = std::iter::once(label).chain(columns[1..].iter().map(|column| column.width));
    // Each column needs one space after it; the borders take two.
    let needed = widths.clone().map(|width| width + 1).sum::<u16>() + 1;
    let shown = |index: usize| needed <= width || columns[index].keep;
    let constraints = widths
        .enumerate()
        .filter(|(index, _)| shown(*index))
        .map(|(_, width)| Constraint::Length(width))
        .collect::<Vec<_>>();
    let titles = columns
        .iter()
        .enumerate()
        .filter(|(index, _)| shown(*index))
        .map(|(_, column)| Cell::from(column.title.clone()));
    let rows = rows.into_iter().map(|(label, cells)| {
        let cells = std::iter::once(Cell::from(label)).chain(cells);
        Row::new(
            cells
                .enumerate()
                .filter(|(index, _)| shown(*index))
                .map(|(_, cell)| cell),
        )
    });
    Table::new(rows, constraints).header(Row::new(titles).bold())
}

/// The members of `group`'s committed plan and how many cursors each owns.
fn members_table(group: Option<&GroupRow<'_>>, width: u16) -> Table<'static> {
    let status = group.and_then(|group| group.status);
    let rows = status
        .iter()
        .flat_map(|status| &status.members)
        .map(|(member, node)| {
            let owns = group.map_or(0, |group| {
                group
                    .cursors
                    .iter()
                    .filter(|row| row.cursor.member == Some(*member))
                    .count()
            });
            let cells = vec![Cell::from(node.to_string()), Cell::from(owns.to_string())];
            (member.to_string(), cells)
        })
        .collect();
    let title = status.map_or_else(
        || "Members".to_owned(),
        |status| format!("Members (plan revision {})", status.revision),
    );
    fitted(&MEMBER_COLUMNS, rows, width).block(Block::bordered().title(title))
}

/// The cells after the partition label.
fn cursor_cells(row: &CursorRow<'_>) -> Vec<Cell<'static>> {
    let owner = row
        .cursor
        .member
        .map_or_else(|| "none".dark_gray(), |member| member.to_string().into());
    let offset = |offset: Option<u64>| offset.map_or_else(|| "-".into(), format::count);
    vec![
        owner.into(),
        Cell::from(offset(row.cursor.offset)),
        Cell::from(format::count(row.next_offset)),
        Cell::from(offset(row.lag())),
    ]
}

/// Labeled counters with their totals and rates.
fn counters(title: &'static str, rows: Vec<(&'static str, EventRow<'_>)>) -> Table<'static> {
    let rows = rows.into_iter().map(|(label, row)| {
        let [_, value, rate] = event_cells(&row);
        Row::new([Cell::from(label), value, rate])
    });
    Table::new(
        rows,
        [
            Constraint::Min(20),
            Constraint::Length(16),
            Constraint::Length(10),
        ],
    )
    .header(header(["Counter", "Total", "/s"]))
    .block(Block::bordered().title(title))
}

/// Event name, value, and rate cells. Names ending in `_bytes` count bytes;
/// gauges can be negative for one thread.
fn event_cells(row: &EventRow<'_>) -> [Cell<'static>; 3] {
    let bytes = row.name.ends_with("_bytes");
    let amount = |value: f64| {
        if bytes {
            format::bytes(value)
        } else {
            format::count(value as u64)
        }
    };
    let value = match row.kind {
        EventKind::Gauge if row.value.cast_signed() < 0 => {
            format!("-{}", amount(row.value.cast_signed().unsigned_abs() as f64))
        }
        _ => amount(row.value as f64),
    };
    let rate = row
        .rate
        .map(|rate| {
            if bytes {
                format::bytes(rate)
            } else {
                format::rate(rate)
            }
        })
        .unwrap_or_default();
    [
        Cell::from(row.name.to_owned()),
        Cell::from(value),
        Cell::from(rate),
    ]
}

/// A two-column table of labels and values.
fn pairs(title: &'static str, pairs: Vec<(&'static str, String)>) -> Table<'static> {
    let rows = pairs
        .into_iter()
        .map(|(label, value)| Row::new([Cell::from(label.dark_gray()), Cell::from(value)]));
    Table::new(rows, [Constraint::Length(14), Constraint::Min(0)])
        .block(Block::bordered().title(title))
}

fn header(titles: impl IntoIterator<Item = &'static str>) -> Row<'static> {
    Row::new(titles).bold()
}

/// The row `table` selects once clamped to `rows`, as drawn.
fn selected_index(table: &TableState, rows: usize) -> usize {
    table.selected().unwrap_or(0).min(rows.saturating_sub(1))
}

/// Keep the selection inside `rows`.
fn clamp(table: &mut TableState, rows: usize) {
    table.select(match rows {
        0 => None,
        rows => Some(table.selected().unwrap_or(0).min(rows - 1)),
    });
}
