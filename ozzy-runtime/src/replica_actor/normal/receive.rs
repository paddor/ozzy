//! One reusable receive window beside the arena currently owned by journal I/O.
//!
//! Staged operations are unadmitted hints. Count/body limits include the core's
//! accepted suffix, read-only validation in flight, and this queued suffix together.

use crate::profiling::{Event, event};
use crate::replica_actor::HistoryReason;
use crate::replica_journal::JournalError;
use ozzy_replication::flow::{Operation, Receiver};
use ozzy_replication::wire::PrepareBatch;
use ozzy_replication::{PipelineLimits, ReplicaSnapshot};

use super::{ActorError, AppendBuffer, Prefix, ReplicaActor};

/// End, operation count, and body bytes of one received suffix.
#[derive(Debug, Clone, Copy)]
struct Suffix {
    end: Prefix,
    operations: usize,
    bytes: usize,
}

#[derive(Debug)]
pub(in crate::replica_actor) struct Receive {
    pub(in crate::replica_actor) queued: AppendBuffer,
    end: Option<Prefix>,
    flight: Option<Suffix>,
    retry: Option<AppendBuffer>,
    pub(in crate::replica_actor) ledger: Receiver,
    operations: Vec<Operation>,
    pub(in crate::replica_actor) allocator: Option<crate::memory::Allocator>,
}

impl Receive {
    #[cfg(test)]
    pub(in crate::replica_actor) fn retry_pending(&self) -> bool {
        self.retry.is_some()
    }

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
        // The ledger counts bodies from this replica's released prefix, which
        // trails the leader's commit announcement, while the leader fills a
        // whole live window beyond that. Like the core, it holds a backup's
        // retained capacity so a full pipeline is not refused and repaired.
        let ledger = Receiver::new(channel, Prefix::GENESIS, limits.backup())
            .map_err(ozzy_replication::flow::TransmitError::from)?;
        Ok(Self {
            queued,
            end: None,
            flight: None,
            retry: None,
            ledger,
            operations: Vec::with_capacity(limits.max_operations),
            allocator: None,
        })
    }

    /// Drop the staged suffix. Returns the arena a deferred suffix held for
    /// its retry, which the caller must recycle.
    pub(super) fn reset(&mut self) -> Option<AppendBuffer> {
        self.queued.clear();
        self.end = None;
        self.flight = None;
        self.retry.take()
    }

    pub(super) fn defer(&mut self, buffer: AppendBuffer) {
        assert!(self.flight.is_some());
        assert!(self.retry.replace(buffer).is_none());
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
        event(Event::ReplicaReceive);
        let packet_bytes = batch
            .operations()
            .map(|op| op.canonical().body.len() as u64)
            .sum();
        crate::profiling::count(Event::ReceiveBytes, packet_bytes);
        let predecessor = self
            .end
            .or(self.flight.map(|flight| flight.end))
            .unwrap_or(snapshot.accepted);
        // Duplicates do not consume capacity. Check every retained boundary that
        // coincides with the packet's end before ignoring an already seen packet.
        self.check_known_end(batch.end(), snapshot)?;
        if batch.end().op <= predecessor.op {
            event(Event::ReplicaReceiveDuplicate);
            crate::profiling::count(Event::DuplicateBytes, packet_bytes);
            return Ok(true);
        }
        if batch.predecessor().op > predecessor.op {
            event(Event::ReplicaReceiveGap);
            return Ok(false);
        }
        let suffix = suffix_after(batch, predecessor)?;
        if !self.has_room(suffix, snapshot, limits)? {
            return Ok(false);
        }
        self.retain(batch, predecessor)?;
        crate::profiling::count(Event::RetainedBytes, suffix.bytes as u64);
        self.end = Some(batch.end());
        Ok(true)
    }

    fn check_known_end(&self, end: Prefix, snapshot: &ReplicaSnapshot) -> Result<(), ActorError> {
        for known in [
            Some(snapshot.accepted),
            self.end,
            self.flight.map(|flight| flight.end),
        ]
        .into_iter()
        .flatten()
        {
            if end.op == known.op && end != known {
                return Err(ActorError::history(HistoryReason::ReceiveWindow));
            }
        }
        Ok(())
    }

    /// Whether the suffix fits the receipt ledger, the core's retained
    /// window, and the receive arena. Reserves the arena bytes when it does.
    fn has_room(
        &mut self,
        suffix: Suffix,
        snapshot: &ReplicaSnapshot,
        limits: PipelineLimits,
    ) -> Result<bool, ActorError> {
        let Suffix {
            end,
            operations: count,
            bytes,
        } = suffix;
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
        // Three limits refuse a suffix; each counts separately so a stall names
        // its cause. Receipt stays unchanged; the sender retries after a drain.
        let local = self.ledger.available();
        if count > local.max_operations || bytes > local.max_body_bytes {
            event(Event::ReplicaReceiveLedgerFull);
            return Ok(false);
        }
        // The core retains two live windows here and admits only what the
        // leader keeps in flight beyond the commit it announced. Staged
        // suffixes count now so the core never refuses a validated one.
        let retained = snapshot.retained_limits;
        if count > retained.max_operations.saturating_sub(used_ops)
            || bytes > retained.max_body_bytes.saturating_sub(used_bytes)
            || end.op.0.saturating_sub(snapshot.live_base.0) > limits.max_operations as u64
        {
            event(Event::ReplicaReceiveWindowFull);
            return Ok(false);
        }
        // The total backlog may exceed this reusable command arena.
        if count > self.queued.limits().max_operations - self.queued.len()
            || bytes > self.queued.limits().max_body_bytes - self.queued.body_bytes()
            || !self.queued.reserve_incoming(bytes)?
        {
            event(Event::ReplicaReceiveArenaFull);
            return Ok(false);
        }
        Ok(true)
    }

    /// Queue the verified operations after `predecessor` and retain them in
    /// the receipt ledger.
    fn retain(&mut self, batch: PrepareBatch<'_>, predecessor: Prefix) -> Result<(), ActorError> {
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
        Ok(())
    }
}

/// The part of `batch` after `predecessor`, which must chain onto it.
fn suffix_after(batch: PrepareBatch<'_>, predecessor: Prefix) -> Result<Suffix, ActorError> {
    let first = batch
        .operations()
        .find(|operation| operation.prefix().op > predecessor.op)
        .ok_or_else(|| ActorError::history(HistoryReason::ReceiveWindow))?;
    if first.canonical().previous_digest != predecessor.digest {
        return Err(ActorError::history(HistoryReason::ReceiveWindow));
    }
    let (operations, bytes) = batch
        .operations()
        .filter(|op| op.prefix().op > predecessor.op)
        .fold((0, 0), |(count, bytes), op| {
            (count + 1, bytes + op.canonical().body.len())
        });
    Ok(Suffix {
        end: batch.end(),
        operations,
        bytes,
    })
}

impl ReplicaActor {
    pub(super) fn schedule_received(&mut self) -> Result<bool, ActorError> {
        if self.work.receive.retry.is_none() && self.work.receive.queued.is_empty()
            || !self.received_has_capacity()
        {
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
        if self.work.receive.retry.is_none() && self.work.receive.queued.is_empty()
            || !self.received_has_capacity()
        {
            return Ok(None);
        }
        if let Some(buffer) = self.work.receive.retry.take() {
            return Ok(Some((self.driver.begin_validation()?, buffer)));
        }
        assert!(self.work.receive.flight.is_none());
        if self.coalescing_received() {
            return Ok(None);
        }
        let ticket = self.driver.begin_validation()?;
        Ok(self.swap_received()?.map(|buffer| (ticket, buffer)))
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
            self.journal.validation_has_capacity(
                self.work
                    .receive
                    .retry
                    .as_ref()
                    .unwrap_or(&self.work.receive.queued),
                normal.snapshot().accepted.op,
            )
        })
    }

    /// Move the staged suffix into flight, replacing its arena. Changes
    /// nothing when no arena is free.
    pub(super) fn swap_received(&mut self) -> Result<Option<AppendBuffer>, ActorError> {
        let Some(mut spare) = self
            .buffer
            .take()
            .or_else(|| self.spare.take())
            .or_else(|| self.journal.lease_append_buffer().ok())
        else {
            return Ok(None);
        };
        assert!(spare.is_empty());
        let receive = &mut self.work.receive;
        if let Some(allocator) = &receive.allocator {
            spare.bind_allocator(allocator)?;
        }
        let end = receive.end.take().ok_or(JournalError::AppendMismatch)?;
        let buffer = std::mem::replace(&mut receive.queued, spare);
        receive.flight = Some(Suffix {
            end,
            operations: buffer.len(),
            bytes: buffer.body_bytes(),
        });
        Ok(Some(buffer))
    }
}
