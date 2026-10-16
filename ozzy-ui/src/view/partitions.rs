//! The Partitions and Replication screens: one row per local partition.

use super::{Column, FACTS, SELECTED, clamp, column, counters, fitted, selected_index};
use crate::{
    App,
    app::Selectable,
    format,
    model::{Behind, PartitionRow, REPAIRS},
};
use ozzy_proto::admin::{PartitionLog, PartitionState, PartitionStatus};
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::Stylize,
    text::{Line, Span},
    widgets::{Block, Cell},
};
use std::borrow::Cow;

const PARTITION_COLUMNS: [Column; 18] = [
    column("Partition", 9, true),
    column("Shard", 5, true),
    column("State", 9, true),
    column("Leader", 10, true),
    column("View", 5, true),
    column("First offset", 12, false),
    column("Next offset", 12, true),
    column("Records/s", 9, true),
    column("Producers", 9, false),
    column("Segs", 4, false),
    column("Size", 8, false),
    column("Dedup", 6, false),
    column("Committed op", 12, true),
    column("Ops/s", 7, true),
    column("Pending", 8, true),
    column("Held", 8, true),
    column("Checks", 6, false),
    column("Reclaimed", 9, false),
];
const LINK_COLUMNS: [Column; 2] = [column("Broker", 6, true), column("Link", 11, true)];
/// Replication table columns before one column per broker.
const REPLICA_COLUMNS: [Column; 8] = [
    column("Partition", 9, true),
    column("Role", 9, true),
    column("View", 5, true),
    column("Accepted", 11, true),
    column("Written", 11, false),
    column("Durable", 11, true),
    column("Committed", 11, true),
    column("Applied", 11, false),
];
impl App {
    /// The table title, or a warning when shards did not answer.
    fn partitions_title(&self, title: &'static str) -> Line<'static> {
        match self.missing_shards() {
            0 => Line::from(title),
            missing => Line::from(vec![
                Span::raw(format!("{title}  ")),
                format!("silent shards: {missing}").red(),
            ]),
        }
    }

    pub(super) fn draw_partitions(&mut self, frame: &mut Frame<'_>, area: Rect) {
        let [table_area, detail_area] =
            Layout::vertical([Constraint::Min(0), Constraint::Length(1)]).areas(area);
        let rows = self.partition_rows();
        let count = rows.len();
        let detail = rows
            .get(selected_index(&self.partition_table, count))
            .map_or_else(String::new, partition_detail);
        let cells = rows
            .iter()
            .map(|row| (row.label(), self.partition_cells(row)))
            .collect();
        // A single broker leads every partition in view 0.
        let columns = PARTITION_COLUMNS
            .into_iter()
            .filter(|column| !self.single() || !matches!(&*column.title, "Leader" | "View"))
            .collect::<Vec<_>>();
        let table = fitted(&columns, cells, table_area.width)
            .block(Block::bordered().title(self.partitions_title("Partitions")))
            .row_highlight_style(SELECTED);
        self.page = table_area.height.saturating_sub(3) as usize;
        clamp(&mut self.partition_table, count);
        frame.render_stateful_widget(table, table_area, &mut self.partition_table);
        self.target_rows(table_area, Selectable::Partitions);
        frame.render_widget(detail.dark_gray(), detail_area);
    }

    /// The cells after the label; only the shard when it did not answer.
    fn partition_cells(&self, row: &PartitionRow<'_>) -> Vec<Cell<'static>> {
        let shard = Cell::from(
            row.name
                .map_or_else(String::new, |name| name.shard.to_string()),
        );
        let Some(status) = row.status else {
            return vec![shard, no_answer()];
        };
        let state = match (status.state, status.ready) {
            (PartitionState::Local, true) => "local".green(),
            (PartitionState::Normal, true) => "normal".green(),
            (PartitionState::Electing, _) => "electing".yellow(),
            (PartitionState::Local | PartitionState::Normal, false) => "not ready".yellow(),
        };
        let log = status.log.as_ref();
        let offset = |offset: Option<u64>| offset.map(format::count).unwrap_or_default();
        let replicated =
            (!self.single()).then(|| [self.leader(status), Cell::from(status.view.to_string())]);
        let storage = log.filter(|log| log.segments > 0);
        let dedup = log.and_then(|log| {
            let limit = log.dedup_limit?.get();
            Some(format!(
                "{}%",
                format::rate(log.dedup_entries as f64 * 100.0 / limit as f64)
            ))
        });
        let mut cells = vec![shard, state.into()];
        cells.extend(replicated.into_iter().flatten());
        cells.extend([
            Cell::from(offset(log.map(|log| log.first_offset))),
            Cell::from(offset(log.map(|log| log.next_offset))),
            Cell::from(row.records.map(format::rate).unwrap_or_default()),
            Cell::from(offset(log.map(|log| log.producers.into()))),
            Cell::from(offset(storage.map(|log| log.segments.into()))),
            Cell::from(
                storage
                    .map(|log| format::bytes(log.segment_bytes as f64))
                    .unwrap_or_default(),
            ),
            Cell::from(dedup.unwrap_or_default()),
            Cell::from(format::count(status.committed)),
            Cell::from(row.rate.map(format::rate).unwrap_or_default()),
            Cell::from(format::count(status.pending_operations)),
            Cell::from(format::bytes(status.pending_body_bytes as f64)),
            Cell::from(format::count(status.validation_cycles)),
            Cell::from(format::bytes(status.reclaimed_bytes as f64)),
        ]);
        cells
    }

    /// The leader's name, bold when it is this broker.
    fn leader(&self, status: &PartitionStatus) -> Cell<'static> {
        let local = self.broker.as_ref().map(|broker| broker.id);
        let leader = status.leader.map_or_else(
            || "unknown".yellow(),
            |leader| {
                let name = self
                    .broker_name(leader)
                    .map_or_else(|| leader.to_string(), str::to_owned);
                if Some(leader) == local {
                    name.bold()
                } else {
                    name.into()
                }
            },
        );
        leader.into()
    }

    pub(super) fn draw_replication(&mut self, frame: &mut Frame<'_>, area: Rect) {
        let brokers = self
            .broker
            .as_ref()
            .map_or_else(Vec::new, |broker| broker.brokers.clone());
        let [top, table_area] = Layout::vertical([
            Constraint::Length((brokers.len().max(REPAIRS.len()) + 3) as u16),
            Constraint::Min(0),
        ])
        .areas(area);
        let [links_area, repairs_area] =
            Layout::horizontal([Constraint::Length(FACTS), Constraint::Min(0)]).areas(top);
        let links = brokers
            .iter()
            .map(|(name, id)| (name.clone(), vec![self.link_cell(*id)]))
            .collect();
        let links =
            fitted(&LINK_COLUMNS, links, links_area.width).block(Block::bordered().title("Links"));
        let columns = REPLICA_COLUMNS
            .into_iter()
            .chain(brokers.iter().map(|(name, _)| Column {
                title: Cow::Owned(name.clone()),
                width: (name.len() as u16).max(7),
                keep: true,
            }))
            .collect::<Vec<_>>();
        let rows = self.partition_rows();
        let count = rows.len();
        let cells = rows
            .iter()
            .map(|row| {
                let cells = match row.status {
                    Some(status) => self.replica_cells(status, &brokers),
                    None => vec![no_answer()],
                };
                (row.label(), cells)
            })
            .collect();
        let table = fitted(&columns, cells, table_area.width)
            .block(Block::bordered().title(self.partitions_title(
                "Replication (operations; broker columns: operations behind, as known here)",
            )))
            .row_highlight_style(SELECTED);
        self.page = table_area.height.saturating_sub(3) as usize;
        clamp(&mut self.partition_table, count);
        frame.render_widget(links, links_area);
        frame.render_widget(counters("Repairs", self.labeled(&REPAIRS)), repairs_area);
        frame.render_stateful_widget(table, table_area, &mut self.partition_table);
        self.target_rows(table_area, Selectable::Partitions);
    }

    /// The cells after the label.
    fn replica_cells(
        &self,
        status: &PartitionStatus,
        brokers: &[(String, ozzy_proto::NodeId)],
    ) -> Vec<Cell<'static>> {
        let local = self.broker.as_ref().map(|broker| broker.id);
        let role = match status.state {
            PartitionState::Local => "local".green(),
            PartitionState::Electing => "electing".yellow(),
            PartitionState::Normal if status.leader.is_some() && status.leader == local => {
                "leader".bold()
            }
            PartitionState::Normal => "follower".into(),
        };
        let mut cells = vec![
            role.into(),
            Cell::from(status.view.to_string()),
            Cell::from(format::count(status.accepted)),
            Cell::from(format::count(status.written)),
            Cell::from(format::count(status.durable)),
            Cell::from(format::count(status.committed)),
            Cell::from(format::count(status.applied)),
        ];
        cells.extend(
            brokers
                .iter()
                .map(|(_, id)| match self.behind(status, *id) {
                    Behind::Leader => "leader".bold().into(),
                    Behind::Ops(0) => Cell::from("0"),
                    Behind::Ops(ops) => format::count(ops).yellow().into(),
                    Behind::Unknown => "-".dark_gray().into(),
                }),
        );
        cells
    }
}

/// The cell that marks a partition whose shard did not answer.
fn no_answer() -> Cell<'static> {
    "no answer".red().into()
}

/// The line under the partition table: the selected row's group, its
/// retention and deduplication limits, and what it waits for.
fn partition_detail(row: &PartitionRow<'_>) -> String {
    let note = row.status.map_or("no answer from its shard", |status| {
        if status.disk_pending {
            "journal action outstanding"
        } else {
            ""
        }
    });
    let limits = row
        .status
        .and_then(|status| status.log.as_ref())
        .map_or_else(String::new, limits);
    format!("group {}{limits}  {note}", row.group)
}

fn limits(log: &PartitionLog) -> String {
    let mut retention = [
        log.retention_age_millis
            .map(|age| format!("age {}", format::age(age.get()))),
        log.retention_bytes
            .map(|bytes| format!("size {}", format::bytes(bytes.get() as f64))),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join(", ");
    if retention.is_empty() {
        retention = "keep all".into();
    }
    let dedup = log.dedup_limit.map_or_else(String::new, |limit| {
        format!(
            "  dedup {} of {}",
            format::count(log.dedup_entries),
            format::count(limit.get())
        )
    });
    format!(
        "  retention {retention} (revision {}){dedup}",
        log.policy_revision
    )
}
