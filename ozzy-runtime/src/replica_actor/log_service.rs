//! The shard side of an embedder's log: it follows the partition's role,
//! submits owner appends on its own proposal lane, and reads committed
//! records. Requests arrive on the log's channel, polled under the
//! partition's waker, so a request from another thread wakes only its
//! partition.

mod appends;
mod reads;
mod step_down;
mod trims;

use super::{ActorError, PartitionActor, PartitionStatus, ProposalSubmitter};
use crate::log::{Command, LogLimits, LogPort, LogRole, LogStatus};
use crate::replica_journal::{PartitionReadBuffer, PartitionReadLimits, ProposalBuffer};
use futures::task::AtomicWaker;
use ozzy_replication::{JournalGeneration, PipelineLimits, Scope};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

/// Requests one poll takes from the channel at most.
const TURN_COMMANDS: usize = 16;

/// The proposal lane, append arenas and read arena one actor lends a log.
/// A replacement actor lends a new lane.
#[derive(Debug)]
pub struct LogLane {
    submitter: ProposalSubmitter,
    arenas: Vec<ProposalBuffer>,
    reads: PartitionReadBuffer,
    read_limits: PartitionReadLimits,
    max_records: usize,
    operations: ozzy_journal::operation::OperationLimits,
}

impl LogLane {
    /// Take the actor's next proposal lane and lease `limits.appends`
    /// append arenas and one read arena, each within the journal's bounds.
    ///
    /// # Errors
    ///
    /// `StartupMismatch` while recovering or without a free lane;
    /// `Limits` when the journal pool is short of arenas.
    pub fn new(actor: &mut PartitionActor, limits: LogLimits) -> Result<Self, ActorError> {
        let (_, journal) = actor.read_access().ok_or(ActorError::StartupMismatch)?;
        let pipeline = journal.append_limits();
        let operation = journal.operation_limits();
        let read_bytes = limits.read_bytes.min(pipeline.max_body_bytes);
        let read_limits = PartitionReadLimits {
            max_records: limits.read_records.min(operation.max_records),
            max_parts: limits
                .read_records
                .saturating_mul(4)
                .min(operation.max_parts),
            max_payload_bytes: read_bytes,
        };
        let arena = journal.lease_append_buffer_with_limits(PipelineLimits {
            max_operations: 1,
            max_body_bytes: read_bytes,
        })?;
        // Most logs are never read here: the first read reserves.
        let reads = PartitionReadBuffer::lazy(arena, read_limits, Arc::new(AtomicWaker::new()));
        let append = PipelineLimits {
            max_operations: 1,
            max_body_bytes: limits.append_bytes.min(pipeline.max_body_bytes),
        };
        let arenas = (0..limits.appends)
            .map(|_| actor.lease_proposal_buffer_with_limits(append))
            .collect::<Result<Vec<_>, _>>()?;
        let submitter = actor.take_submitter().ok_or(ActorError::StartupMismatch)?;
        Ok(Self {
            submitter,
            arenas,
            reads,
            read_limits,
            max_records: operation.max_records,
            operations: operation,
        })
    }
}

/// The term this broker leads: the scope and journal generation its
/// appends must match.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Term {
    scope: Scope,
    generation: JournalGeneration,
}

impl Term {
    /// The authority a proposal of this term names.
    const fn authority(self) -> ozzy_proto::append::Authority {
        ozzy_proto::append::Authority {
            group_id: self.scope.group_id,
            config_epoch: self.scope.configuration_epoch,
            view: self.scope.view,
        }
    }
}

/// One log on one partition: its port, the lane of the current actor, and
/// the work in flight.
#[derive(Debug)]
pub(super) struct LogService {
    port: LogPort,
    lane: Option<LogLane>,
    term: Option<Term>,
    waiting: Option<Command>,
    appends: Vec<appends::Flight>,
    read: Option<reads::Reading>,
    trim: Option<trims::TrimFlight>,
    stepping: Option<step_down::Stepping>,
    /// When the last step-down started its view change.
    stepped_at: Option<Duration>,
}

/// A service that goes away, with its partition or its shard, leaves its
/// handles a stopped log.
impl Drop for LogService {
    fn drop(&mut self) {
        self.port.publish(LogStatus {
            role: LogRole::Stopped,
            lead: None,
            ..self.port.published()
        });
    }
}

impl LogService {
    pub(super) fn new(port: LogPort, lane: Option<LogLane>) -> Self {
        Self {
            port,
            lane,
            term: None,
            waiting: None,
            appends: Vec::new(),
            read: None,
            trim: None,
            stepping: None,
            stepped_at: None,
        }
    }

    /// Lend the service a new actor's lane after recovery.
    pub(super) fn attach(&mut self, lane: LogLane) {
        self.lane = Some(lane);
    }

    /// The actor is being replaced. Its lane goes; appends in flight may
    /// still commit, so they end as unknown.
    pub(super) fn detach(&mut self) {
        self.lane = None;
        self.term = None;
        self.read = None;
        self.refuse_step_down(crate::log::StepDownError::NotLeader);
        for flight in self.appends.drain(..) {
            flight.unknown();
        }
        if let Some(flight) = self.trim.take() {
            flight.unknown();
        }
        self.port.publish(LogStatus {
            role: LogRole::Electing,
            lead: None,
            ..self.port.published()
        });
    }

    pub(super) fn poll(
        &mut self,
        actor: &mut PartitionActor,
        cx: &mut Context<'_>,
        now: Duration,
    ) -> bool {
        let mut progress = self.observe(actor);
        progress |= self.finish_appends(cx);
        progress |= self.finish_trim(cx);
        progress |= self.poll_step_down(actor, now);
        progress |= self.poll_read(actor, cx);
        progress |= self.take_commands(actor, cx, now);
        progress
    }

    /// Follow the partition's role. A ready primary opens a term at the
    /// committed end and leads it in doubt while no follower answers;
    /// anything else ends it.
    fn observe(&mut self, actor: &mut PartitionActor) -> bool {
        let status = actor.status();
        let (_, local, _) = actor.native_identity();
        let primary = actor.authority_hint().primary;
        let role = if actor.leader_doubt() {
            LogRole::Doubt
        } else {
            LogRole::Leader
        };
        let Some((ticket, journal)) = actor.read_access() else {
            return self.lose(LogRole::Electing, self.port.published().view);
        };
        let Some(ticket) = ticket else {
            let (role, view) = match status {
                PartitionStatus::Local(_) => (LogRole::Stopped, 0),
                PartitionStatus::Replicated(status) => {
                    let follows = status.normal.is_some() && primary != local;
                    let role = if follows {
                        LogRole::Follower
                    } else {
                        LogRole::Electing
                    };
                    (role, status.scope.view)
                }
            };
            return self.lose(role, view);
        };
        let term = Term {
            scope: ticket.scope(),
            generation: ticket.generation(),
        };
        if self.term == Some(term) {
            // Doubt comes and goes within the term.
            let published = self.port.published();
            self.port.publish(LogStatus { role, ..published });
            return role != published.role;
        }
        let Ok(Some(cursor)) = journal.open_reader(ticket, self.port.partition, None) else {
            // Busy, or the partition is not created yet: look again later.
            return false;
        };
        let end = cursor.committed_end();
        let installed = journal
            .installed_end(term.scope.view, self.port.partition)
            .map_or(end, |installed| installed.min(end));
        self.term = Some(term);
        self.port.publish(LogStatus {
            view: term.scope.view,
            role,
            committed_end: end,
            retained_from: cursor.retained_from(),
            lead: Some(crate::log::Lead {
                start_end: end,
                installed_commit_end: installed,
            }),
        });
        true
    }

    /// Publish a role other than leader. Appends in flight resolve through
    /// the journal.
    fn lose(&mut self, role: LogRole, view: u64) -> bool {
        self.term = None;
        let published = self.port.published();
        let status = LogStatus {
            view,
            role,
            lead: None,
            ..published
        };
        self.port.publish(status);
        status != published
    }

    /// Take queued requests while arenas and the read slot allow.
    fn take_commands(
        &mut self,
        actor: &PartitionActor,
        cx: &mut Context<'_>,
        now: Duration,
    ) -> bool {
        let mut progress = false;
        for _ in 0..TURN_COMMANDS {
            let command = match self.waiting.take() {
                Some(command) => command,
                None => match self.port.poll_command(cx) {
                    Poll::Ready(Some(command)) => command,
                    Poll::Ready(None) | Poll::Pending => return progress,
                },
            };
            let held = match command {
                Command::Append(append) => self.start_append(append).map(Command::Append),
                Command::Read(read) => self.start_read(read).map(Command::Read),
                Command::Trim(trim) => self.start_trim(actor, trim).map(Command::Trim),
                Command::StepDown(request) => {
                    let (_, local, _) = actor.native_identity();
                    self.start_step_down(request, local, now);
                    None
                }
            };
            if let Some(command) = held {
                self.waiting = Some(command);
                return progress;
            }
            progress = true;
        }
        cx.waker().wake_by_ref();
        progress
    }
}
