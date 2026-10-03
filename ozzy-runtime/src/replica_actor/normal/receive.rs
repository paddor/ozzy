//! One reusable receive window beside the arena currently owned by journal I/O.
//!
//! Staged operations are unadmitted hints. Count/body limits include the core's
//! accepted suffix, read-only validation in flight, and this queued suffix together.

use ozzy_replication::flow::{Operation, Receiver};
use ozzy_replication::wire::PrepareBatch;
use ozzy_replication::{PipelineLimits, ReplicaSnapshot};

use super::{ActorError, AppendBuffer, Prefix, ReplicaActor};

#[derive(Debug, Clone, Copy)]
struct Flight {
    end: Prefix,
    operations: usize,
    bytes: usize,
}

#[derive(Debug)]
pub(in crate::replica_actor) struct Receive {
    pub(in crate::replica_actor) queued: AppendBuffer,
    end: Option<Prefix>,
    flight: Option<Flight>,
    pub(super) ledger: Receiver,
    operations: Vec<Operation>,
    pub(in crate::replica_actor) allocator: Option<crate::memory::Allocator>,
}

impl Receive {
    /// Whether a staged suffix is in journal validation.
    pub(super) fn in_flight(&self) -> bool {
        self.flight.is_some()
    }

    /// Operations staged but not yet taken for validation.
    pub(super) fn queued_operations(&self) -> usize {
        self.queued.len()
    }

    pub(super) fn new(
        queued: AppendBuffer,
        channel: ozzy_replication::flow::Channel,
        limits: PipelineLimits,
    ) -> Result<Self, ActorError> {
        let ledger = Receiver::new(channel, Prefix::GENESIS, limits)
            .map_err(ozzy_replication::flow::TransmitError::from)?;
        Ok(Self {
            queued,
            end: None,
            flight: None,
            ledger,
            operations: Vec::with_capacity(limits.max_operations),
            allocator: None,
        })
    }

    pub(super) fn reset(&mut self) {
        self.queued.clear();
        self.end = None;
        self.flight = None;
    }

    /// The core admitted the suffix in flight and now charges it.
    pub(super) fn admitted(&mut self) {
        self.flight = None;
    }

    /// True consumes the packet (retained or duplicate); false needs repair/space.
    pub(super) fn stage(
        &mut self,
        batch: PrepareBatch<'_>,
        snapshot: &ReplicaSnapshot,
        limits: PipelineLimits,
    ) -> Result<bool, ActorError> {
        use crate::profiling::{Event, event};
        event(Event::ReplicaReceive);
        let packet_bytes = if crate::profiling::enabled() {
            batch
                .operations()
                .map(|op| op.canonical().body.len() as u64)
                .sum()
        } else {
            0
        };
        crate::profiling::count(Event::ReceiveBytes, packet_bytes);
        let predecessor = self
            .end
            .or(self.flight.map(|flight| flight.end))
            .unwrap_or(snapshot.accepted);
        // Duplicates do not consume capacity. Check every retained boundary that
        // coincides with the packet's end before ignoring an already seen packet.
        for known in [
            Some(snapshot.accepted),
            self.end,
            self.flight.map(|flight| flight.end),
        ]
        .into_iter()
        .flatten()
        {
            if batch.end().op == known.op && batch.end() != known {
                return Err(ActorError::History);
            }
        }
        if batch.end().op <= predecessor.op {
            event(Event::ReplicaReceiveDuplicate);
            crate::profiling::count(Event::DuplicateBytes, packet_bytes);
            return Ok(true);
        }
        if batch.predecessor().op > predecessor.op {
            event(Event::ReplicaReceiveGap);
            return Ok(false);
        }
        let first = batch
            .operations()
            .find(|operation| operation.prefix().op > predecessor.op)
            .ok_or(ActorError::History)?;
        if first.canonical().previous_digest != predecessor.digest {
            return Err(ActorError::History);
        }
        let (count, bytes) = batch
            .operations()
            .filter(|op| op.prefix().op > predecessor.op)
            .fold((0, 0), |(count, bytes), op| {
                (count + 1, bytes + op.canonical().body.len())
            });
        // Once admitted, the core charges the flight. Before that, charge it here.
        let validating = self
            .flight
            .filter(|flight| flight.end.op > snapshot.accepted.op);
        let used_ops = snapshot.pending_operations
            + self.queued.len()
            + validating.map_or(0, |flight| flight.operations);
        let used_bytes = snapshot.pending_body_bytes
            + self.queued.body_bytes()
            + validating.map_or(0, |flight| flight.bytes);
        let local = self.ledger.available();
        if count > local.max_operations
            || bytes > local.max_body_bytes
            || count > limits.max_operations.saturating_sub(used_ops)
            || bytes > limits.max_body_bytes.saturating_sub(used_bytes)
            || count > self.queued.limits().max_operations - self.queued.len()
            || bytes > self.queued.limits().max_body_bytes - self.queued.body_bytes()
        {
            // The total backlog may exceed this reusable command arena. Leave
            // receipt unchanged; the sender retries after this queued suffix drains.
            event(Event::ReplicaReceiveCapacity);
            return Ok(false);
        }
        if !self.queued.reserve_incoming(bytes)? {
            event(Event::ReplicaReceiveCapacity);
            return Ok(false);
        }
        for operation in batch
            .verified_operations()
            .filter(|op| op.prefix().op > predecessor.op)
        {
            self.queued.push_primary_verified(operation)?;
        }
        self.operations.clear();
        self.operations.extend(
            batch
                .operations()
                .filter(|op| op.prefix().op > predecessor.op)
                .map(super::flow::operation),
        );
        self.ledger
            .retain(self.ledger.report().channel, &self.operations)
            .map_err(ozzy_replication::flow::TransmitError::from)?;
        crate::profiling::count(Event::RetainedBytes, bytes as u64);
        self.end = Some(batch.end());
        Ok(true)
    }
}

impl ReplicaActor {
    pub(super) fn schedule_received(&mut self) -> Result<bool, ActorError> {
        if self.work.receive.queued.is_empty() || !self.received_has_capacity() {
            return Ok(false);
        }
        if let Some(validate) = self.take_received()? {
            self.submit_turn(crate::replica_journal::Turn {
                validate: Some(validate),
                ..Default::default()
            })?;
        }
        // A coalescing wait also keeps the slot for this suffix.
        Ok(true)
    }

    /// Take the staged suffix for validation, unless it should keep coalescing
    /// behind a running barrier or no arena can replace it.
    pub(super) fn take_received(
        &mut self,
    ) -> Result<Option<(ozzy_replication::driver::ValidationTicket, AppendBuffer)>, ActorError>
    {
        if self.work.receive.queued.is_empty() || !self.received_has_capacity() {
            return Ok(None);
        }
        assert!(self.work.receive.flight.is_none());
        if self.coalescing_received() {
            return Ok(None);
        }
        let ticket = self.driver.begin_validation()?;
        Ok(self.swap_received().map(|buffer| (ticket, buffer)))
    }

    /// The next barrier cannot start until the previous one finishes. Coalesce
    /// the queued suffix up to the existing count/byte target, or dispatch the
    /// partial group as soon as that barrier completes. Staging keeps its
    /// local receive capacity; it supplies no durable evidence.
    fn coalescing_received(&self) -> bool {
        let queued = &self.work.receive.queued;
        let target = self.config.sync_batch_target;
        self.pending_sync.is_some()
            && queued.len()
                < target
                    .operations
                    .saturating_sub(self.work.sync_batch_operations)
            && queued.body_bytes()
                < target
                    .body_bytes
                    .saturating_sub(self.work.sync_batch_body_bytes)
    }

    fn received_has_capacity(&self) -> bool {
        self.driver.normal().is_some_and(|normal| {
            self.journal
                .validation_has_capacity(&self.work.receive.queued, normal.snapshot().accepted.op)
        })
    }

    /// Move the staged suffix into flight, replacing its arena. Changes
    /// nothing when no arena is free.
    pub(super) fn swap_received(&mut self) -> Option<AppendBuffer> {
        let mut spare = self
            .buffer
            .take()
            .or_else(|| self.spare.take())
            .or_else(|| self.journal.lease_append_buffer().ok())?;
        assert!(spare.is_empty());
        let receive = &mut self.work.receive;
        if let Some(allocator) = &receive.allocator {
            spare
                .bind_allocator(allocator)
                .expect("empty same-shard receive arena");
        }
        let buffer = std::mem::replace(&mut receive.queued, spare);
        receive.flight = Some(Flight {
            end: receive.end.take().expect("nonempty queued suffix"),
            operations: buffer.len(),
            bytes: buffer.body_bytes(),
        });
        Some(buffer)
    }
}
