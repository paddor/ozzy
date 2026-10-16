//! One receipt cursor serves both live sends and retained-history catch-up.

use crate::replica_actor::require_normal;
use ozzy_replication::PipelineLimits;
use ozzy_replication::flow::{Channel, Repair, Sender};
use ozzy_replication::wire::FetchOps;

use super::{ActorError, DriverError, Duration, NodeId, Prefix, ReplicaActor, SendClass};
use crate::replica_journal::FetchedHistory;

#[derive(Debug, Clone, Copy)]
pub(super) struct Plan {
    channel: Channel,
    after: Prefix,
    repair: Option<Repair>,
    limits: PipelineLimits,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct Blocked {
    channel: Channel,
    generation: ozzy_replication::JournalGeneration,
    after: Prefix,
    required: usize,
}

impl ReplicaActor {
    fn replay_plan(&self, voter: usize) -> Option<Plan> {
        self.bindings[voter]?;
        let peer = self.work.flow.peers[voter].as_ref()?;
        let sender = peer.sender()?;
        if self
            .outbox
            .queued(self.configuration.voters()[voter], SendClass::Data)?
            .0
            > 0
        {
            return None;
        }
        let repair = peer.repair(false);
        if self.work.flow.publication_enabled && repair.is_none() && !peer.needs_catch_up() {
            return None;
        }
        let (after, mut limits) = if let Some(repair) = repair {
            (repair.after, repair_size(sender, repair))
        } else {
            (sender.sent(), sender.available())
        };
        if self.work.flow.publication_enabled && repair.is_none() {
            let behind = peer.catch_up_through()?.0.checked_sub(after.op.0)?;
            limits.max_operations = limits
                .max_operations
                .min(usize::try_from(behind).unwrap_or(usize::MAX));
        }
        limits.max_operations = limits
            .max_operations
            .min(self.config.transfer.max_operations);
        limits.max_body_bytes = limits
            .max_body_bytes
            .min(self.config.transfer.max_body_bytes);
        if limits.max_operations == 0 || limits.max_body_bytes == 0 {
            return None;
        }
        let plan = Plan {
            channel: sender.channel(),
            after,
            repair,
            limits,
        };
        (!self.replay_blocked(voter, &plan)).then_some(plan)
    }

    /// Whether the peer's last read of the same history found a next
    /// operation larger than this plan's body credit.
    fn replay_blocked(&self, voter: usize, plan: &Plan) -> bool {
        self.work.replay_blocked[voter].is_some_and(|blocked| {
            blocked.channel == plan.channel
                && blocked.after == plan.after
                && self.driver.normal().is_some_and(|normal| {
                    normal.snapshot().journal.generation == blocked.generation
                })
                && plan.limits.max_body_bytes < blocked.required
        })
    }

    /// Whether the plan a read was issued for still matches this peer's
    /// current plan, and the fetched history still belongs to this journal.
    fn replay_current(&self, old: &Plan, voter: usize, request: FetchOps) -> bool {
        self.replay_plan(voter).is_some_and(|current| {
            old.channel == current.channel
                && old.after == current.after
                && old.repair == current.repair
                && old.after == request.predecessor
                && old.limits.max_operations <= current.limits.max_operations
                && old.limits.max_body_bytes <= current.limits.max_body_bytes
        }) && self.application_ready()
            && self.driver.scope() == request.scope
            && self.driver.normal().is_some_and(|normal| {
                normal.snapshot().journal.generation == request.source.generation
            })
    }

    /// True starts a read or reserves a durable-policy capture turn. Once submitted,
    /// reader I/O progresses independently of subsequent normal journal commands.
    pub(super) fn schedule_replay(&mut self, _now: Duration) -> Result<bool, ActorError> {
        if self.buffer.is_none() {
            return Ok(false);
        }
        let normal = require_normal(&self.driver)?;
        let snapshot = normal.snapshot();
        let floor = normal
            .start_view()
            .map_err(DriverError::from)?
            .map_or(Prefix::GENESIS, |start| start.accepted);
        for delta in 0..3 {
            let index = (self.work.replay_peer + delta) % 3;
            let Some(plan) = self.replay_plan(index) else {
                continue;
            };
            if plan.after.op < floor.op || plan.after.op >= snapshot.applied.op {
                continue;
            }
            if self.replay_recent(index, plan.after)? {
                self.work.replay_peer = (index + 1) % 3;
                // A cache send needs no journal slot. Slow/full peers must not
                // prevent local proposal admission, sync, or application.
                return Ok(false);
            }
            if self.work.needs_sync {
                return Ok(false);
            }
            if self.pending_sync.is_some() {
                crate::profiling::event(crate::profiling::Event::ReplayPersistenceWait);
                // Finish the already open write group, then reserve a quiescent
                // turn. Continuous overlap must not starve retained-history reads.
                return Ok(true);
            }
            if plan.after.op >= require_normal(&self.driver)?.stored_through() {
                // No completed physical prefix beyond this cursor yet. Keep
                // admitting work; background completion will make it readable.
                crate::profiling::event(crate::profiling::Event::ReplayPersistenceWait);
                continue;
            }
            self.work.replay_peer = (index + 1) % 3;
            let Some(buffer) = self.buffer.take() else {
                return Ok(false);
            };
            self.pending_replay = Some(crate::replica_actor::io::PendingReplay {
                completion: self
                    .journal
                    .fetch_replication(
                        self.driver.begin_validation()?,
                        plan.after,
                        plan.limits,
                        buffer,
                    )
                    .map_err(|rejected| rejected.reason)?,
                to: self.configuration.voters()[index],
            });
            self.work.replay_plan = Some(plan);
            crate::profiling::event(crate::profiling::Event::ReplayDiskFetch);
            return Ok(true);
        }
        Ok(false)
    }

    pub(super) fn complete_replay(
        &mut self,
        fetched: FetchedHistory,
        to: NodeId,
        _now: Duration,
    ) -> Result<(), ActorError> {
        let request = fetched.request();
        let issued = self.work.replay_plan.take();
        let voter = self
            .configuration
            .voters()
            .iter()
            .position(|peer| *peer == to)
            .ok_or_else(super::flow::no_channel)?;
        let Some(plan) = issued.filter(|old| self.replay_current(old, voter, request)) else {
            self.recycle(fetched.into_buffer())?;
            return Ok(());
        };
        if let Some(before) = fetched.retired_predecessor() {
            self.recycle(fetched.into_buffer())?;
            return self.notify_retired(
                to,
                request.scope,
                ozzy_replication::wire::HistoryFence::Receive(plan.channel.epoch),
                before,
            );
        }
        if let Some(required) = fetched.minimum_body_bytes() {
            if required > self.config.transfer.max_body_bytes {
                return Err(ActorError::Limits);
            }
            self.work.replay_blocked[voter] = Some(Blocked {
                channel: plan.channel,
                generation: request.source.generation,
                after: plan.after,
                required,
            });
            self.recycle(fetched.into_buffer())?;
            return Ok(());
        }
        self.work.replay_blocked[voter] = None;
        self.send_replayed(fetched, voter, &plan)
    }

    /// Send fetched history to the voter it was read for and recycle its arena.
    fn send_replayed(
        &mut self,
        mut fetched: FetchedHistory,
        voter: usize,
        plan: &Plan,
    ) -> Result<(), ActorError> {
        let snapshot = require_normal(&self.driver)?.snapshot();
        let end = fetched.end();
        let committed = if end.op <= snapshot.committed.op {
            end
        } else {
            snapshot.committed
        };
        let payload = fetched.shared_bodies();
        let operations: smallvec::SmallVec<[_; crate::replica_actor::MAX_TRANSFER_OPERATIONS]> =
            fetched.wire_operations().collect();
        self.work.flow.scratch.clear();
        self.work
            .flow
            .scratch
            .extend(operations.iter().copied().map(super::flow::operation));
        let scope = fetched.request().scope;
        let mut packets = self.prepare_packets(scope, committed, &operations, &payload)?;
        drop(operations);
        self.recycle(fetched.into_buffer())?;
        let to = self.configuration.voters()[voter];
        let packet = self.bind_packet(
            packets[voter].take().ok_or_else(super::flow::no_channel)?,
            plan.channel.epoch,
            committed,
            self.session(to).ok_or_else(super::flow::no_channel)?,
        )?;
        self.transmit(voter, plan.repair, end, packet)
    }
}

/// Operations and body bytes still outstanding in one repair range.
fn repair_size(sender: &Sender, repair: Repair) -> PipelineLimits {
    sender
        .outstanding()
        .filter(|op| op.prefix.op > repair.after.op && op.prefix.op <= repair.through.op)
        .fold(
            PipelineLimits {
                max_operations: 0,
                max_body_bytes: 0,
            },
            |mut limits, op| {
                limits.max_operations = limits.max_operations.saturating_add(1);
                limits.max_body_bytes =
                    limits.max_body_bytes.saturating_add(op.body_bytes as usize);
                limits
            },
        )
}
