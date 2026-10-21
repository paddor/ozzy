//! Source-anchored streaming: bounded selection cache, one transfer, one arena.

mod bridge;

use crate::replica_actor::HistoryReason;
use ozzy_replication::wire::{FetchOps, OpsBatch};
use ozzy_replication::{Digest, OpNumber};

use super::deadline::Earliest;
use super::io::FetchPurpose;
use super::{
    ActorError, AppendBuffer, Bytes, Duration, LogSource, Message, NodeId, PendingIo, Prefix,
    ReplicaActor, Scope, SendClass, wire,
};
use crate::replica_journal::FetchedHistory;

#[derive(Debug, Default)]
pub(super) struct Lookup {
    scope: Option<Scope>,
    entries: [Option<(LogSource, Prefix)>; 6],
    unavailable: Option<(LogSource, OpNumber)>,
    bridge: Option<bridge::Bridge>,
    compatible: Option<(LogSource, LogSource)>,
}

impl Lookup {
    pub(super) fn reset_scope(&mut self, scope: Scope) {
        if self.scope != Some(scope) {
            self.scope = Some(scope);
            self.entries.fill(None);
            self.unavailable = None;
            self.bridge = None;
            self.compatible = None;
        }
    }

    pub(super) fn get(&self, source: LogSource, op: OpNumber) -> Option<Digest> {
        self.entries
            .iter()
            .flatten()
            .find_map(|&(key, prefix)| (key == source && prefix.op == op).then_some(prefix.digest))
    }

    pub(super) fn unavailable(&self, source: LogSource, op: OpNumber) -> bool {
        self.unavailable == Some((source, op))
    }

    pub(super) fn retire(&mut self, source: LogSource, op: OpNumber) {
        self.unavailable = Some((source, op));
    }

    pub(super) fn insert(&mut self, source: LogSource, prefix: Prefix) -> Result<(), ActorError> {
        // A newly arrived report may select a different donor. Each selection
        // needs at most six prefixes, all from one exact source, not six per donor.
        if self.entries.iter().flatten().any(|(key, _)| *key != source) {
            self.entries.fill(None);
        }
        if let Some(digest) = self.get(source, prefix.op) {
            return if digest == prefix.digest {
                Ok(())
            } else {
                Err(ActorError::history(HistoryReason::Lookup))
            };
        }
        let slot = self
            .entries
            .iter_mut()
            .find(|entry| entry.is_none())
            .ok_or_else(|| ActorError::history(HistoryReason::Lookup))?;
        *slot = Some((source, prefix));
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) enum TransferPurpose {
    // A candidate digest is not usable until this scan reaches the source tail.
    Lookup { op: OpNumber, found: Option<Prefix> },
    Install,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct Transfer {
    pub request: FetchOps,
    pub purpose: TransferPurpose,
    retry_at: Duration,
}

impl Transfer {
    /// Add its next retry.
    pub(super) fn deadlines(&self, next: Earliest) -> Earliest {
        next.at(self.retry_at)
    }
}

impl ReplicaActor {
    pub(super) fn complete_position(
        &mut self,
        position: &crate::replica_journal::HistoryPosition,
    ) -> Result<(), ActorError> {
        if let Some(prefix) = position.position {
            self.lookup.verified(position.source, prefix)?;
        } else if position.op < position.retained_predecessor.op {
            self.bridge_or_retire(position.source, position.op, position.retained_predecessor);
        } else {
            return Err(ActorError::history(HistoryReason::Lookup));
        }
        Ok(())
    }

    /// Look up the digest of `op` in `source`: from the journal when it is the
    /// pinned local source, otherwise through a lookup transfer. `reason`
    /// names the failure when a local source is not pinned.
    ///
    /// A remote lookup starts at `expected`, the prefix the caller checks. A
    /// source that holds it proves it by a short chain to its tail; one that
    /// does not answers with its retained boundary, and the scan restarts
    /// there. Without `expected` the scan starts at genesis.
    pub(super) fn lookup_history(
        &mut self,
        source: LogSource,
        op: OpNumber,
        expected: Option<Prefix>,
        reason: HistoryReason,
    ) -> Result<(), ActorError> {
        if source.voter != self.local {
            let start = expected.filter(|start| start.op == op && op < source.accepted.op);
            let purpose = TransferPurpose::Lookup { op, found: start };
            let predecessor = start.unwrap_or(Prefix::GENESIS);
            return self.start_transfer(self.driver.scope(), source, predecessor, purpose);
        }
        if self.pinned != Some(source) {
            return Err(ActorError::history(reason));
        }
        self.pending = Some(PendingIo::Position(
            self.journal.history_position(source, op)?,
        ));
        Ok(())
    }

    /// Fetch from `source` after `predecessor`; the first request goes out
    /// on the next retry turn.
    pub(super) fn start_transfer(
        &mut self,
        scope: Scope,
        source: LogSource,
        predecessor: Prefix,
        purpose: TransferPurpose,
    ) -> Result<(), ActorError> {
        self.transfer = Some(Transfer {
            request: FetchOps {
                scope,
                request_id: self.ids.request()?,
                source,
                predecessor,
                max_operations: u32::try_from(self.config.transfer.max_operations)
                    .map_err(|_| ActorError::Limits)?,
                max_body_bytes: u32::try_from(self.config.transfer.max_body_bytes)
                    .map_err(|_| ActorError::Limits)?,
            },
            purpose,
            retry_at: Duration::ZERO,
        });
        Ok(())
    }

    pub(super) fn retry_transfer(&mut self, now: Duration) -> Result<(), ActorError> {
        let Some(transfer) = &mut self.transfer else {
            return Ok(());
        };
        if now < transfer.retry_at {
            return Ok(());
        }
        let request = transfer.request;
        // Selection-only lookup from an abandoned view supplies no new evidence.
        // Staging abort is scheduled separately, after admitted disk work settles.
        if matches!(transfer.purpose, TransferPurpose::Lookup { .. })
            && request.scope != self.driver.scope()
        {
            self.transfer = None;
            return Ok(());
        }
        if request.source.voter == self.local {
            if self.pending.is_none() {
                let buffer = self
                    .buffer
                    .take()
                    .ok_or_else(|| ActorError::history(HistoryReason::BufferUnavailable))?;
                self.pending = Some(PendingIo::Fetch(
                    self.journal
                        .fetch_history(request, buffer)
                        .map_err(|rejected| rejected.reason)?,
                    FetchPurpose::Install,
                ));
            }
        } else {
            transfer.retry_at = now.saturating_add(self.config.timing.retransmit);
            let Some(session) = self.session(request.source.voter) else {
                return Ok(());
            };
            let encoded = wire::encode_fetch(
                self.local,
                session,
                request,
                &mut self.metadata,
                self.wire_limits,
            )?;
            let message = super::control_message(encoded, &self.metadata);
            self.enqueue(request.source.voter, SendClass::Data, message)?;
        }
        Ok(())
    }

    pub(super) fn serve(&mut self, from: NodeId, request: FetchOps) -> Result<(), ActorError> {
        if self.queue_recovery_fetch(from, request) {
            return Ok(());
        }
        // Reader/lookup work can own the journal without an actor pending action.
        // The requester retains this addressed request and retries after pressure.
        if self.journal.available_command_slots() == 0 {
            return Ok(());
        }
        if self.pinned.is_none()
            && self.application_ready()
            && self.configuration.primary(self.driver.scope().view) == self.local
            && request.source.voter == self.local
            && request.scope == self.driver.scope()
            && self.pending.is_none()
            && self.pending_sync.is_none()
            && self.pending_persistence.is_empty()
        {
            self.pending = Some(PendingIo::RetiredLookup(
                self.journal.replication_positions(
                    self.driver.begin_validation()?,
                    [request.predecessor.op; 2],
                )?,
                from,
                request,
            ));
            return Ok(());
        }
        if self.pending.is_some()
            || self.promise.is_some()
            || self.pinned != Some(request.source)
            || request.scope != self.journal_scope
            || request.max_operations as usize > self.config.transfer.max_operations
            || request.max_body_bytes as usize > self.config.transfer.max_body_bytes
        {
            return Ok(()); // No negative vote. Requester retains and retries its request.
        }
        let Some(buffer) = self.buffer.take() else {
            return Ok(());
        };
        self.pending = Some(PendingIo::Fetch(
            self.journal
                .fetch_history(request, buffer)
                .map_err(|rejected| rejected.reason)?,
            FetchPurpose::Serve(from),
        ));
        Ok(())
    }

    pub(super) fn send_ops(
        &mut self,
        to: NodeId,
        fetched: FetchedHistory,
    ) -> Result<(), ActorError> {
        if let Some(before) = fetched.retired_predecessor() {
            let request = fetched.request();
            self.recycle(fetched.into_buffer())?;
            return self.notify_retired(
                to,
                request.scope,
                wire::HistoryFence::Fetch(request.request_id),
                before,
            );
        }
        let Some(session) = self.session(to) else {
            self.recycle(fetched.into_buffer())?;
            return Ok(()); // Requester will retry after its new link is ready.
        };
        let operations: smallvec::SmallVec<[_; super::MAX_TRANSFER_OPERATIONS]> =
            fetched.wire_operations().collect();
        let encoded = wire::encode_ops(
            wire::EncodeContext {
                sender: self.local,
                session,
                limits: self.wire_limits,
            },
            fetched.request(),
            &operations,
            &mut self.metadata,
            &mut self.payload,
        )?;
        let message = Message::multipart([
            Bytes::copy_from_slice(&encoded.header),
            Bytes::copy_from_slice(&self.metadata[..encoded.metadata_bytes]),
            Bytes::copy_from_slice(&self.payload[..encoded.payload_bytes]),
        ]);
        drop(operations);
        self.recycle(fetched.into_buffer())?;
        self.enqueue(to, SendClass::Data, message)
    }

    pub(super) fn receive_ops(&mut self, batch: &OpsBatch<'_>) -> Result<(), ActorError> {
        let Some(mut transfer) = self.transfer else {
            return Ok(());
        };
        if batch.validate_response(transfer.request).is_err() {
            return Ok(());
        }
        match &mut transfer.purpose {
            TransferPurpose::Lookup { op, found } => {
                if batch.scope() != self.driver.scope() {
                    return Ok(());
                }
                for operation in batch.operations() {
                    if operation.prefix().op == *op {
                        *found = Some(operation.prefix());
                    }
                }
                if batch.end() == transfer.request.source.accepted {
                    self.lookup.verified(
                        batch.source(),
                        found.ok_or_else(|| ActorError::history(HistoryReason::Lookup))?,
                    )?;
                    self.transfer = None;
                } else {
                    transfer.request.predecessor = batch.end();
                    transfer.request.request_id = self.ids.request()?;
                    transfer.retry_at = Duration::ZERO;
                    self.transfer = Some(transfer);
                }
            }
            TransferPurpose::Install => {
                if self.pending.is_some() {
                    return Ok(());
                }
                let ticket = self
                    .driver
                    .installation_ticket()
                    .ok_or_else(|| ActorError::history(HistoryReason::Lookup))?;
                let bytes = batch.operations().map(|op| op.canonical().body.len()).sum();
                // Without room, keep the transfer request for retry after memory returns.
                let Some(mut buffer) = self.stage_arena(bytes)? else {
                    return Ok(());
                };
                for operation in batch.operations() {
                    buffer.push(operation.canonical())?;
                }
                self.transfer = None;
                self.pending = Some(PendingIo::Chunk(
                    self.journal
                        .install_chunk(ticket, buffer)
                        .map_err(|rejected| rejected.reason)?,
                ));
            }
        }
        Ok(())
    }

    pub(super) fn recycle(&mut self, mut buffer: AppendBuffer) -> Result<(), ActorError> {
        buffer.clear();
        buffer.restore_allocator(self.history_allocator.as_ref())?;
        if self.buffer.is_none() {
            self.buffer = Some(buffer);
        } else if self.spare.is_none() {
            self.spare = Some(buffer);
        }
        // Otherwise a split received suffix leased one arena more than the
        // idle slots hold; dropping it returns that lease to the pool.
        Ok(())
    }

    /// Take the arena once it can stage `bytes`; otherwise leave it bound to
    /// its usual allocator for a retry.
    fn stage_arena(&mut self, bytes: usize) -> Result<Option<AppendBuffer>, ActorError> {
        let Some(buffer) = self.buffer.as_mut() else {
            return Ok(None);
        };
        if let Some(allocator) = &self.history_receive_allocator {
            buffer.bind_allocator(allocator)?;
        }
        if !buffer.reserve_incoming(bytes)? {
            buffer.restore_allocator(self.history_allocator.as_ref())?;
            return Ok(None);
        }
        Ok(self.buffer.take())
    }
}

impl ReplicaActor {
    pub(super) fn complete_retired_lookup(
        &mut self,
        positions: &crate::replica_journal::ReplicationPositions,
        to: NodeId,
        request: FetchOps,
    ) -> Result<(), ActorError> {
        if !self.application_ready()
            || positions.ticket.scope() != self.driver.scope()
            || request.scope != self.driver.scope()
            || self.configuration.primary(request.scope.view) != self.local
        {
            return Ok(());
        }
        let retired = request.predecessor.op < positions.retained_predecessor.op;
        match positions.positions[0] {
            // A position not read is no evidence either way.
            None if !retired => return Ok(()),
            Some(held) if !retired && held == request.predecessor => {}
            // A retired predecessor, or another prefix at its op: name the
            // boundary to rescan from.
            _ => {
                return self.notify_retired(
                    to,
                    request.scope,
                    wire::HistoryFence::Fetch(request.request_id),
                    positions.retained_predecessor,
                );
            }
        }
        let Some(buffer) = self.buffer.take() else {
            return Ok(());
        };
        self.pending = Some(PendingIo::Fetch(
            self.journal
                .fetch_history(request, buffer)
                .map_err(|rejected| rejected.reason)?,
            FetchPurpose::Serve(to),
        ));
        Ok(())
    }
}

impl ReplicaActor {
    pub(super) fn notify_retired(
        &mut self,
        to: NodeId,
        scope: Scope,
        fence: wire::HistoryFence,
        before: Prefix,
    ) -> Result<(), ActorError> {
        let Some(session) = self.session(to) else {
            return Ok(());
        };
        let encoded = wire::encode_history_retired(
            self.local,
            session,
            wire::HistoryRetired {
                scope,
                fence,
                before,
            },
            &mut self.metadata,
            self.wire_limits,
        )?;
        self.enqueue(
            to,
            SendClass::Control,
            super::control_message(encoded, &self.metadata),
        )
    }

    pub(super) fn receive_retired(
        &mut self,
        from: NodeId,
        notice: wire::HistoryRetired,
    ) -> Result<(), ActorError> {
        if notice.scope != self.driver.scope() {
            return Ok(());
        }
        let behind = match notice.fence {
            wire::HistoryFence::Receive(epoch) => {
                from == self.configuration.primary(notice.scope.view)
                    && self.work.receive.ledger.report().channel.epoch == epoch
                    && self
                        .driver
                        .normal()
                        .is_some_and(|normal| normal.snapshot().accepted.op < notice.before.op)
            }
            wire::HistoryFence::Fetch(id) => self.retired_transfer(from, id, notice)?,
        };
        if behind {
            self.recovery_required = true;
            self.ingress.close();
        }
        Ok(())
    }

    /// The donor retired history that transfer `id` asked for. A lookup scans
    /// on from the boundary or bridges it; `true` when this copy fell behind.
    fn retired_transfer(
        &mut self,
        from: NodeId,
        id: ozzy_proto::RequestId,
        notice: wire::HistoryRetired,
    ) -> Result<bool, ActorError> {
        let Some(transfer) = self.transfer.filter(|transfer| {
            transfer.request.request_id == id
                && transfer.request.scope == notice.scope
                && transfer.request.source.voter == from
                && notice.before.op <= transfer.request.source.accepted.op
        }) else {
            return Ok(false);
        };
        if notice.before.op <= transfer.request.predecessor.op {
            // The source does not hold the expected prefix a lookup started
            // from. Rescan from its boundary; the digest found there is
            // evidence once the scan reaches the tail.
            if notice.before != transfer.request.predecessor
                && let TransferPurpose::Lookup { op, .. } = transfer.purpose
            {
                self.start_transfer(
                    notice.scope,
                    transfer.request.source,
                    notice.before,
                    TransferPurpose::Lookup {
                        op,
                        found: (op == notice.before.op).then_some(notice.before),
                    },
                )?;
            }
            return Ok(false);
        }
        if let TransferPurpose::Lookup { op, .. } = transfer.purpose
            && op >= notice.before.op
        {
            if notice.before.op == transfer.request.source.accepted.op {
                if notice.before == transfer.request.source.accepted {
                    self.lookup
                        .verified(transfer.request.source, notice.before)?;
                    self.transfer = None;
                }
                return Ok(false);
            }
            // The boundary is only a scan starting point. No digest
            // enters election evidence until the exact tail verifies.
            self.start_transfer(
                notice.scope,
                transfer.request.source,
                notice.before,
                TransferPurpose::Lookup {
                    op,
                    found: (op == notice.before.op).then_some(notice.before),
                },
            )?;
        } else if let TransferPurpose::Lookup { op, .. } = transfer.purpose
            && self.pinned.is_some_and(|local| {
                local.voter == self.local && local.accepted.op >= notice.before.op
            })
        {
            self.bridge_or_retire(transfer.request.source, op, notice.before);
            self.transfer = None;
        } else {
            return Ok(true);
        }
        Ok(false)
    }
}
