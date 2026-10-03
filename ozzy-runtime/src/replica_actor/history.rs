//! Source-anchored streaming: bounded selection cache, one transfer, one arena.

use ozzy_replication::wire::{FetchOps, OpsBatch};
use ozzy_replication::{Digest, OpNumber};

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
}

impl Lookup {
    pub(super) fn reset_scope(&mut self, scope: Scope) {
        if self.scope != Some(scope) {
            self.scope = Some(scope);
            self.entries.fill(None);
        }
    }

    pub(super) fn get(&self, source: LogSource, op: OpNumber) -> Option<Digest> {
        self.entries
            .iter()
            .flatten()
            .find_map(|&(key, prefix)| (key == source && prefix.op == op).then_some(prefix.digest))
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
                Err(ActorError::History)
            };
        }
        let slot = self
            .entries
            .iter_mut()
            .find(|entry| entry.is_none())
            .ok_or(ActorError::History)?;
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

impl<E: crate::replica_journal::JournalExecution> ReplicaActor<E> {
    pub(super) fn start_transfer(
        &mut self,
        scope: Scope,
        source: LogSource,
        predecessor: Prefix,
        purpose: TransferPurpose,
        now: Duration,
    ) -> Result<(), ActorError> {
        self.transfer = Some(Transfer {
            request: FetchOps {
                scope,
                request_id: self.ids.request()?,
                source,
                predecessor,
                max_operations: u32::try_from(self.config.transfer.max_operations)
                    .expect("validated bound"),
                max_body_bytes: u32::try_from(self.config.transfer.max_body_bytes)
                    .expect("validated bound"),
            },
            purpose,
            retry_at: now,
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
                let buffer = self.buffer.take().ok_or(ActorError::History)?;
                self.pending = Some(PendingIo::Fetch(
                    self.journal
                        .fetch_history(request, buffer)
                        .map_err(|rejected| rejected.reason)?,
                    FetchPurpose::Install,
                ));
            }
        } else {
            transfer.retry_at = now + self.config.timing.retransmit;
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
            let message = Message::multipart([
                Bytes::copy_from_slice(&encoded.header),
                Bytes::copy_from_slice(&self.metadata[..encoded.metadata_bytes]),
                Bytes::new(),
            ]);
            self.enqueue(request.source.voter, SendClass::Data, message)?;
        }
        Ok(())
    }

    pub(super) fn serve(&mut self, from: NodeId, request: FetchOps) -> Result<(), ActorError> {
        if self.queue_recovery_fetch(from, request) {
            return Ok(());
        }
        if self.pending.is_some()
            || self.buffer.is_none()
            || self.promise.is_some()
            || self.pinned != Some(request.source)
            || request.scope != self.journal_scope
            || request.max_operations as usize > self.config.transfer.max_operations
            || request.max_body_bytes as usize > self.config.transfer.max_body_bytes
        {
            return Ok(()); // No negative vote. Requester retains and retries its request.
        }
        let buffer = self.buffer.take().expect("checked arena");
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
        let Some(session) = self.session(to) else {
            self.recycle(fetched.into_buffer());
            return Ok(()); // Requester will retry after its new link is ready.
        };
        let operations: smallvec::SmallVec<[_; super::MAX_TRANSFER_OPERATIONS]> =
            fetched.wire_operations().collect();
        let encoded = wire::encode_ops(
            self.local,
            session,
            fetched.request(),
            &operations,
            &mut self.metadata,
            &mut self.payload,
            self.wire_limits,
        )?;
        let message = Message::multipart([
            Bytes::copy_from_slice(&encoded.header),
            Bytes::copy_from_slice(&self.metadata[..encoded.metadata_bytes]),
            Bytes::copy_from_slice(&self.payload[..encoded.payload_bytes]),
        ]);
        drop(operations);
        self.recycle(fetched.into_buffer());
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
                    self.lookup
                        .insert(batch.source(), found.ok_or(ActorError::History)?)?;
                    self.transfer = None;
                } else {
                    transfer.request.predecessor = batch.end();
                    transfer.request.request_id = self.ids.request()?;
                    transfer.retry_at = Duration::ZERO;
                    self.transfer = Some(transfer);
                }
            }
            TransferPurpose::Install => {
                if self.pending.is_some() || self.buffer.is_none() {
                    return Ok(());
                }
                let ticket = self
                    .driver
                    .installation_ticket()
                    .ok_or(ActorError::History)?;
                let bytes = batch.operations().map(|op| op.canonical().body.len()).sum();
                if let Some(allocator) = &self.history_receive_allocator {
                    self.buffer
                        .as_mut()
                        .expect("checked arena")
                        .bind_allocator(allocator)?;
                }
                if !self
                    .buffer
                    .as_mut()
                    .expect("checked arena")
                    .reserve_incoming(bytes)?
                {
                    self.buffer
                        .as_mut()
                        .expect("checked arena")
                        .restore_allocator(self.history_allocator.as_ref())?;
                    return Ok(()); // Keep the transfer request for retry after memory returns.
                }
                let mut buffer = self.buffer.take().expect("checked arena");
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

    pub(super) fn recycle(&mut self, mut buffer: AppendBuffer) {
        buffer.clear();
        buffer
            .restore_allocator(self.history_allocator.as_ref())
            .expect("cleared history arena restores its original allocation source");
        if self.buffer.is_none() {
            self.buffer = Some(buffer);
        } else {
            assert!(self.spare.is_none());
            self.spare = Some(buffer);
        }
    }
}
