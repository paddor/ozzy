//! Rows the screens show, derived from the latest replies.

use crate::App;
use ozzy_proto::{
    ConsumerGroupId, GroupId, NodeId,
    admin::{
        Counters, EventKind, GroupCursor, GroupStatus, PartitionName, PartitionState,
        PartitionStatus, Partitions,
    },
};
use std::collections::BTreeMap;

/// Counters the Broker screen shows, with their labels.
pub(crate) const OVERVIEW: [(&str, &str); 13] = [
    ("native_proposal_committed", "Appends confirmed"),
    ("native_admission_refusal", "Appends refused"),
    ("client_limit_refusal", "Clients over limit"),
    ("dispatcher_input_paused", "Input paused"),
    ("io_failed", "I/O errors"),
    ("reader_publication", "Reader publications"),
    ("historical_read_delivery", "Historical reads"),
    ("replica_send", "Replica sends"),
    ("replica_receive", "Replica receives"),
    ("replica_publication_gap", "Replica gaps"),
    ("io_write_bytes", "Disk writes"),
    ("io_read_bytes", "Disk reads"),
    ("storage_cleanup_removed", "Files cleaned up"),
];

/// Counters the Replication screen shows: how often followers miss, repair,
/// and refetch operations.
pub(crate) const REPAIRS: [(&str, &str); 8] = [
    ("replica_publication_gap", "Publication gaps"),
    ("replica_receive_gap", "Receive gaps"),
    ("replica_repair", "Repairs sent"),
    ("replica_probe", "Status probes"),
    ("replica_receive_duplicate", "Duplicates"),
    ("replica_receive_window_full", "Window full"),
    ("replay_disk_fetch", "Disk fetches"),
    ("replica_payload_bytes", "Bytes sent"),
];

/// How far one broker is behind on a partition, as this broker knows it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Behind {
    /// The broker leads the partition.
    Leader,
    /// Operations behind: a follower's acknowledgment behind the leader's
    /// accepted operation, or this broker behind the leader's commit.
    Ops(u64),
    /// Not known here, for example another follower or an election.
    Unknown,
}

/// One consumer group: its plan from the coordinator partition and its
/// cursors from the partitions it reads. The plan is missing while the
/// coordinator's journal has not answered.
#[derive(Debug)]
pub(crate) struct GroupRow<'a> {
    pub(crate) id: ConsumerGroupId,
    /// `topic/name`, or the ID without a plan.
    pub(crate) label: String,
    /// The topic, known with the plan.
    pub(crate) topic: Option<&'a str>,
    pub(crate) status: Option<&'a GroupStatus>,
    pub(crate) cursors: Vec<CursorRow<'a>>,
}

/// A group's cursor on one partition.
#[derive(Debug)]
pub(crate) struct CursorRow<'a> {
    pub(crate) partition: String,
    pub(crate) cursor: &'a GroupCursor,
    /// The partition's next record offset.
    pub(crate) next_offset: u64,
}

impl CursorRow<'_> {
    /// Records not yet committed by the group.
    pub(crate) fn lag(&self) -> Option<u64> {
        let offset = self.cursor.offset?;
        Some(self.next_offset.saturating_sub(offset))
    }
}

impl GroupRow<'_> {
    /// Records the group has not committed, over every partition with a
    /// cursor.
    pub(crate) fn lag(&self) -> u64 {
        self.cursors.iter().filter_map(CursorRow::lag).sum()
    }
}

/// One local partition. Either half can be missing: names before the broker
/// facts arrive, status when its shard did not answer.
#[derive(Debug)]
pub(crate) struct PartitionRow<'a> {
    pub(crate) group: GroupId,
    pub(crate) name: Option<&'a PartitionName>,
    pub(crate) status: Option<&'a PartitionStatus>,
    /// Committed operations per second.
    pub(crate) rate: Option<f64>,
    /// Records per second.
    pub(crate) records: Option<f64>,
}

/// One partition's record rate between its last two logs. A busy journal
/// answers without its log; the screens show the previous one, which would
/// read as a stall, so only fresh logs count.
#[derive(Debug)]
pub(crate) struct RecordRate {
    uptime_ms: u64,
    next_offset: u64,
    rate: Option<f64>,
}

/// One event's value for the selected thread.
#[derive(Debug)]
pub(crate) struct EventRow<'a> {
    pub(crate) name: &'a str,
    pub(crate) kind: EventKind,
    pub(crate) value: u64,
    /// Per second, for counts only.
    pub(crate) rate: Option<f64>,
}

/// Totals over the local partitions.
#[derive(Debug, Default)]
pub(crate) struct Summary {
    pub(crate) partitions: usize,
    pub(crate) ready: usize,
    pub(crate) electing: usize,
    pub(crate) disk_pending: usize,
    pub(crate) leading: usize,
    pub(crate) pending_operations: u64,
    pub(crate) pending_body_bytes: u64,
    pub(crate) reclaimed_bytes: u64,
    pub(crate) rate: f64,
    pub(crate) records: f64,
}

/// The first entry of the Threads screen's list totals every thread.
pub(crate) const ALL_THREADS: &str = "all threads";

impl App {
    /// Partitions in directory order, then any the directory lacks.
    pub(crate) fn partition_rows(&self) -> Vec<PartitionRow<'_>> {
        let current = self.partitions.current.as_ref();
        let mut statuses = current
            .iter()
            .flat_map(|partitions| &partitions.rows)
            .map(|row| (row.group, row))
            .collect::<BTreeMap<_, _>>();
        let previous = self.partitions.previous().map(|(previous, elapsed)| {
            let rows = previous
                .rows
                .iter()
                .map(|row| (row.group, row.committed))
                .collect::<BTreeMap<_, _>>();
            (rows, elapsed)
        });
        let rate = |status: Option<&PartitionStatus>| {
            let status = status?;
            let (rows, elapsed) = previous.as_ref()?;
            per_second(status.committed, *rows.get(&status.group)?, *elapsed)
        };
        let names = self.broker.iter().flat_map(|broker| &broker.partitions);
        let mut rows = names
            .map(|name| {
                let status = statuses.remove(&name.group);
                PartitionRow {
                    group: name.group,
                    name: Some(name),
                    status,
                    rate: rate(status),
                    records: self.records(name.group),
                }
            })
            .collect::<Vec<_>>();
        rows.extend(statuses.into_values().map(|status| PartitionRow {
            group: status.group,
            name: None,
            status: Some(status),
            rate: rate(Some(status)),
            records: self.records(status.group),
        }));
        rows
    }

    fn records(&self, group: GroupId) -> Option<f64> {
        self.record_rates.get(&group)?.rate
    }

    /// Update record rates from the fresh logs of `partitions`. A restarted
    /// broker starts over.
    pub(crate) fn track_records(&mut self, partitions: &Partitions) {
        for row in &partitions.rows {
            let Some(log) = &row.log else {
                continue;
            };
            let now = RecordRate {
                uptime_ms: partitions.uptime_ms,
                next_offset: log.next_offset,
                rate: None,
            };
            let rate = self.record_rates.get(&row.group).and_then(|before| {
                let elapsed = now.uptime_ms.checked_sub(before.uptime_ms)?;
                per_second(now.next_offset, before.next_offset, elapsed)
            });
            self.record_rates
                .insert(row.group, RecordRate { rate, ..now });
        }
    }

    pub(crate) fn summary(&self) -> Summary {
        let local = self.broker.as_ref().map(|broker| broker.id);
        let mut summary = Summary::default();
        for row in self.partition_rows() {
            summary.partitions += 1;
            summary.rate += row.rate.unwrap_or(0.0);
            summary.records += row.records.unwrap_or(0.0);
            let Some(status) = row.status else {
                continue;
            };
            summary.ready += usize::from(status.ready);
            summary.electing += usize::from(status.state == PartitionState::Electing);
            summary.disk_pending += usize::from(status.disk_pending);
            summary.leading += usize::from(local.is_some() && status.leader == local);
            summary.pending_operations += status.pending_operations;
            summary.pending_body_bytes += status.pending_body_bytes;
            summary.reclaimed_bytes += status.reclaimed_bytes;
        }
        summary
    }

    /// How far `broker` is behind on the partition of `status`. A leader
    /// knows each follower's acknowledgment; a follower knows only its own
    /// distance to the leader's commit.
    pub(crate) fn behind(&self, status: &PartitionStatus, broker: NodeId) -> Behind {
        let Some(local) = self.broker.as_ref().map(|facts| facts.id) else {
            return Behind::Unknown;
        };
        if status.state == PartitionState::Electing {
            return Behind::Unknown;
        }
        if status.leader == Some(broker) {
            return Behind::Leader;
        }
        if broker == local {
            return Behind::Ops(status.leader_commit.saturating_sub(status.accepted));
        }
        status
            .followers
            .iter()
            .find(|(id, _)| *id == broker)
            .map_or(Behind::Unknown, |(_, acknowledged)| {
                Behind::Ops(status.accepted.saturating_sub(*acknowledged))
            })
    }

    /// Whether this broker has a session with `broker`; `None` for itself
    /// and before the first connections reply.
    pub(crate) fn link_up(&self, broker: NodeId) -> Option<bool> {
        self.connections
            .as_ref()?
            .brokers
            .iter()
            .find(|(id, _)| *id == broker)
            .map(|(_, up)| *up)
    }

    /// Consumer groups by label, from every partition's committed state.
    pub(crate) fn group_rows(&self) -> Vec<GroupRow<'_>> {
        let mut groups = BTreeMap::<ConsumerGroupId, GroupRow<'_>>::new();
        for row in self.partition_rows() {
            let Some(log) = row.status.and_then(|status| status.log.as_ref()) else {
                continue;
            };
            let partition = row.label();
            for status in &log.groups {
                let topic = row.name.map(|name| name.topic.as_str());
                let group = group_entry(&mut groups, status.id);
                group.label = format!("{}/{}", topic.unwrap_or("?"), status.name);
                group.topic = topic;
                group.status = Some(status);
            }
            for cursor in &log.cursors {
                group_entry(&mut groups, cursor.group)
                    .cursors
                    .push(CursorRow {
                        partition: partition.clone(),
                        cursor,
                        next_offset: log.next_offset,
                    });
            }
        }
        let mut rows = groups.into_values().collect::<Vec<_>>();
        rows.sort_by(|a, b| a.label.cmp(&b.label));
        rows
    }

    /// The Groups screen's selected group.
    pub(crate) fn selected_group(&self) -> Option<GroupRow<'_>> {
        let index = self.group_table.selected().unwrap_or(0);
        self.group_rows().into_iter().nth(index)
    }

    /// Shards that did not answer the latest partitions request.
    pub(crate) fn missing_shards(&self) -> u32 {
        self.partitions
            .current
            .as_ref()
            .map_or(0, |partitions| partitions.missing_shards)
    }

    /// How many local partitions each broker leads.
    pub(crate) fn leads(&self, broker: NodeId) -> usize {
        self.partitions.current.as_ref().map_or(0, |partitions| {
            partitions
                .rows
                .iter()
                .filter(|row| row.leader == Some(broker))
                .count()
        })
    }

    /// A configured broker's name.
    pub(crate) fn broker_name(&self, id: NodeId) -> Option<&str> {
        let broker = self.broker.as_ref()?;
        broker
            .brokers
            .iter()
            .find(|(_, broker)| *broker == id)
            .map(|(name, _)| name.as_str())
    }

    /// The Threads screen's list: every thread, then each live thread and
    /// `exited`.
    pub(crate) fn thread_names(&self) -> Vec<&str> {
        let threads = self
            .counters
            .current
            .iter()
            .flat_map(|counters| &counters.threads);
        std::iter::once(ALL_THREADS)
            .chain(threads.map(|(name, _)| name.as_str()))
            .collect()
    }

    /// The selected thread's events, without zero values unless shown.
    pub(crate) fn event_rows(&self) -> Vec<EventRow<'_>> {
        let selected = self.thread_table.selected().unwrap_or(0);
        let Some(current) = &self.counters.current else {
            return Vec::new();
        };
        let Some(values) = thread_values(current, selected) else {
            return Vec::new();
        };
        let before = self.counters.previous().and_then(|(previous, elapsed)| {
            let thread = match selected {
                0 => Some(previous.totals.as_slice()),
                index => same_thread(current, previous, index - 1),
            };
            thread
                .filter(|before| before.len() == values.len())
                .map(|before| (before, elapsed))
        });
        current
            .events
            .iter()
            .zip(values)
            .enumerate()
            .filter(|(_, (_, value))| self.show_zero || **value != 0)
            .map(|(index, ((name, kind), &value))| EventRow {
                name,
                kind: *kind,
                value,
                rate: before
                    .filter(|_| *kind == EventKind::Count)
                    .and_then(|(before, elapsed)| per_second(value, before[index], elapsed)),
            })
            .collect()
    }

    /// The labeled `events`, totaled over every thread, such as
    /// [`OVERVIEW`].
    pub(crate) fn labeled(
        &self,
        events: &[(&'static str, &'static str)],
    ) -> Vec<(&'static str, EventRow<'_>)> {
        let Some(current) = &self.counters.current else {
            return Vec::new();
        };
        let before = self.counters.previous();
        events
            .iter()
            .filter_map(|&(event, label)| {
                let index = current.events.iter().position(|(name, _)| name == event)?;
                let (name, kind) = &current.events[index];
                let value = *current.totals.get(index)?;
                let rate =
                    before
                        .filter(|_| *kind == EventKind::Count)
                        .and_then(|(before, elapsed)| {
                            per_second(value, *before.totals.get(index)?, elapsed)
                        });
                Some((
                    label,
                    EventRow {
                        name,
                        kind: *kind,
                        value,
                        rate,
                    },
                ))
            })
            .collect()
    }
}

impl PartitionRow<'_> {
    /// `topic/partition`, or the group ID before the broker facts arrive.
    pub(crate) fn label(&self) -> String {
        self.name.map_or_else(
            || self.group.to_string(),
            |name| format!("{}/{}", name.topic, name.partition),
        )
    }
}

fn group_entry<'a, 'b>(
    groups: &'b mut BTreeMap<ConsumerGroupId, GroupRow<'a>>,
    id: ConsumerGroupId,
) -> &'b mut GroupRow<'a> {
    groups.entry(id).or_insert_with(|| GroupRow {
        id,
        label: id.to_string(),
        topic: None,
        status: None,
        cursors: Vec::new(),
    })
}

/// Values of list entry `index`: 0 totals every thread.
fn thread_values(counters: &Counters, index: usize) -> Option<&[u64]> {
    match index {
        0 => Some(&counters.totals),
        index => counters
            .threads
            .get(index - 1)
            .map(|(_, values)| values.as_slice()),
    }
}

/// The previous values of `current.threads[index]`. Names can repeat, so
/// the thread is matched by name and its place among equal names.
fn same_thread<'a>(current: &Counters, previous: &'a Counters, index: usize) -> Option<&'a [u64]> {
    let name = &current.threads.get(index)?.0;
    let place = current.threads[..index]
        .iter()
        .filter(|(other, _)| other == name)
        .count();
    previous
        .threads
        .iter()
        .filter(|(other, _)| other == name)
        .nth(place)
        .map(|(_, values)| values.as_slice())
}

/// Change per second of a count that grew from `before` to `now`.
fn per_second(now: u64, before: u64, elapsed_ms: u64) -> Option<f64> {
    let grown = now.checked_sub(before)?;
    (elapsed_ms > 0).then(|| grown as f64 * 1000.0 / elapsed_ms as f64)
}
