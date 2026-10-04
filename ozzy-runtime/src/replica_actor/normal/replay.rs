//! One receipt cursor serves both live sends and retained-history catch-up.

use ozzy_replication::PipelineLimits;
use ozzy_replication::flow::{Channel, Repair, TransmitError};

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
            let limits = sender
                .outstanding()
                .filter(|op| op.prefix.op > repair.after.op && op.prefix.op <= repair.through.op)
                .fold(
                    PipelineLimits {
                        max_operations: 0,
                        max_body_bytes: 0,
                    },
                    |mut limits, op| {
                        limits.max_operations += 1;
                        limits.max_body_bytes += op.body_bytes as usize;
                        limits
                    },
                );
            (repair.after, limits)
        } else {
            (sender.sent(), sender.available())
        };
        if self.work.flow.publication_enabled && repair.is_none() {
            limits.max_operations = limits
                .max_operations
                .min((peer.catch_up_through()?.0 - after.op.0) as usize);
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
        if self.work.replay_blocked[voter].is_some_and(|blocked| {
            blocked.channel == sender.channel()
                && blocked.after == after
                && self.driver.normal().is_some_and(|normal| {
                    normal.snapshot().journal.generation == blocked.generation
                })
                && limits.max_body_bytes < blocked.required
        }) {
            return None;
        }
        Some(Plan {
            channel: sender.channel(),
            after,
            repair,
            limits,
        })
    }

    /// True starts a read or reserves a durable-policy capture turn. Once submitted,
    /// reader I/O progresses independently of subsequent normal journal commands.
    pub(super) fn schedule_replay(&mut self, _now: Duration) -> Result<bool, ActorError> {
        if self.buffer.is_none() {
            return Ok(false);
        }
        let normal = self.driver.normal().expect("normal primary");
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
            if plan.after.op
                >= self
                    .driver
                    .normal()
                    .expect("normal primary")
                    .stored_through()
            {
                // No completed physical prefix beyond this cursor yet. Keep
                // admitting work; background completion will make it readable.
                crate::profiling::event(crate::profiling::Event::ReplayPersistenceWait);
                continue;
            }
            self.work.replay_peer = (index + 1) % 3;
            let buffer = self.buffer.take().expect("checked arena");
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
        mut fetched: FetchedHistory,
        to: NodeId,
        _now: Duration,
    ) -> Result<(), ActorError> {
        let request = fetched.request();
        let plan = self.work.replay_plan.take();
        let voter = self
            .configuration
            .voters()
            .iter()
            .position(|peer| *peer == to)
            .expect("peer");
        let current = self.replay_plan(voter);
        if !self.application_ready()
            || self.driver.scope() != request.scope
            || self.driver.normal().is_none_or(|normal| {
                normal.snapshot().journal.generation != request.source.generation
            })
            || plan.zip(current).is_none_or(|(old, current)| {
                old.channel != current.channel
                    || old.after != current.after
                    || old.repair != current.repair
                    || old.after != request.predecessor
                    || old.limits.max_operations > current.limits.max_operations
                    || old.limits.max_body_bytes > current.limits.max_body_bytes
            })
        {
            self.recycle(fetched.into_buffer());
            return Ok(());
        }
        let plan = plan.expect("checked plan");
        if let Some(before) = fetched.retired_predecessor() {
            self.recycle(fetched.into_buffer());
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
            self.recycle(fetched.into_buffer());
            return Ok(());
        }
        self.work.replay_blocked[voter] = None;
        let snapshot = self.driver.normal().expect("normal replay").snapshot();
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
        let mut packets = self.prepare_packets(request.scope, committed, &operations, &payload)?;
        drop(operations);
        self.recycle(fetched.into_buffer());
        let packet = self.bind_packet(
            packets[voter].take().expect("remote packet"),
            plan.channel.epoch,
            committed,
            self.session(to)
                .expect("current replay plan requires a live binding"),
        )?;
        let peer = self.work.flow.peers[voter].as_mut().expect("remote peer");
        if let Some(repair) = plan.repair {
            peer.record_repair(repair, end)
        } else {
            peer.record_send(&self.work.flow.scratch)
        }
        .map_err(TransmitError::from)?;
        crate::profiling::count(
            crate::profiling::Event::ReplicaPayloadBytes,
            packet.part_bytes(2).expect("payload").len() as u64,
        );
        self.outbox
            .try_enqueue(to, SendClass::Data, packet)
            .map_err(|(error, _)| ActorError::Enqueue(error))?;
        Ok(())
    }
}
