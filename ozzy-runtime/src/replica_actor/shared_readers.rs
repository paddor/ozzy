//! Bounded subscriptions on established shared broker links. No sockets or tasks.

mod output;
mod publication;
mod receive;

use super::{ActorError, NativeReceive, PartitionActor, reader_cursor::Cursor};
use crate::{
    frontend::{Kind, Link, Links},
    reader_service::{Delivery, Failure, reader_frame},
    replica_journal::{PartitionReadBuffer, PartitionReadLimits},
    replicated::payload::Payload,
    signal::DataSignal,
};
use bytes::Bytes;
use omq_tokio::{Message, TrySendError};
use ozzy_proto::{
    Envelope, NodeId, Opcode, PartitionIncarnation, data::DataLimits, decode_packet,
    nack::AuthorityHint, reader,
};
use ozzy_replication::{JournalGeneration, PipelineLimits, Prefix, Scope};
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

/// Fixed partition-local reader bounds. Physical broker links remain shared.
#[derive(Clone, Copy, Debug)]
pub struct SharedReaderConfig {
    /// Checked partition incarnation on this actor.
    pub partition: PartitionIncarnation,
    /// Maximum one delivery; credit is independently negotiated per link.
    pub limits: DataLimits,
    /// Aggregate subscriptions on this partition, including idle readers.
    pub subscriptions: usize,
}

#[derive(Debug)]
struct Slot {
    peer: NodeId,
    payload: Payload,
    delivery: Delivery<Cursor>,
    pending: Option<Message>,
}

pub(super) struct SharedReaders {
    local: NodeId,
    config: SharedReaderConfig,
    links: Links,
    slots: Vec<Option<Slot>>,
    pool: PartitionReadBuffer,
    metadata: Vec<u8>,
    rejection: Option<Message>,
    next: usize,
    observed: Option<(Scope, JournalGeneration, Prefix)>,
    work: Arc<DataSignal>,
    readiness: Pin<Box<dyn Future<Output = ()> + Send>>,
    publication: publication::Publication,
}

impl std::fmt::Debug for SharedReaders {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedReaders")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl SharedReaders {
    pub(super) fn new(
        actor: &mut PartitionActor,
        config: SharedReaderConfig,
        links: Links,
    ) -> Result<Self, ActorError> {
        let local = actor.native_identity().1;
        let (_, journal) = actor.read_access().ok_or(ActorError::StartupMismatch)?;
        let operation = journal.operation_limits();
        if !(1..=65536).contains(&config.subscriptions)
            || config.partition.as_bytes() == &[0; 16]
            || config.limits.max_records == 0
            || config.limits.max_records > operation.max_records
            || config.limits.max_parts == 0
            || config.limits.max_parts > operation.max_parts
            || config.limits.envelope.max_payload_bytes == 0
        {
            return Err(ActorError::Limits);
        }
        let buffer = journal.lease_append_buffer_with_limits(PipelineLimits {
            max_operations: 1,
            max_body_bytes: config.limits.envelope.max_payload_bytes,
        })?;
        let work = Arc::new(DataSignal::default());
        let pool = PartitionReadBuffer::new(
            buffer,
            PartitionReadLimits {
                max_records: config.limits.max_records,
                max_parts: config.limits.max_parts,
                max_payload_bytes: config.limits.envelope.max_payload_bytes,
            },
            work.clone(),
        )?;
        let publication = publication::Publication::new(journal, config, work.clone())?;
        Ok(Self {
            local,
            config,
            links,
            slots: (0..config.subscriptions).map(|_| None).collect(),
            pool,
            metadata: Vec::with_capacity(config.limits.envelope.max_metadata_bytes),
            rejection: None,
            next: 0,
            observed: None,
            readiness: wait(work.clone()),
            work,
            publication,
        })
    }

    pub(super) fn supports(message: &Message) -> bool {
        envelope(message).is_some_and(|e| {
            matches!(
                e.opcode,
                Opcode::Subscribe | Opcode::Credit | Opcode::Ack | Opcode::Unsubscribe
            )
        })
    }

    fn current(&self, peer: NodeId, envelope: Envelope) -> Option<Link> {
        self.links.get(peer).filter(|link| {
            link.binding.kind == Kind::Client
                && envelope.session == Some(link.binding.session)
                && envelope.sender == peer
                && link.remote.roles & ozzy_proto::handshake::CONSUMER != 0
        })
    }

    fn discard_stale(&mut self) {
        for slot in &mut self.slots {
            if slot.as_ref().is_some_and(|slot| {
                self.links
                    .get(slot.peer)
                    .is_none_or(|link| Some(link.binding.session) != slot.delivery.request.session)
            }) {
                *slot = None;
            }
        }
        if self.rejection.as_ref().is_some_and(|message| {
            let Some(peer) = message
                .part_slice(0)
                .and_then(|bytes| bytes.try_into().ok())
                .map(NodeId::from_bytes)
            else {
                return true;
            };
            let envelope = envelope(message);
            envelope.is_none_or(|envelope| {
                self.links
                    .get(peer)
                    .is_none_or(|link| Some(link.binding.session) != envelope.session)
            })
        }) {
            self.rejection = None;
        }
    }
}

fn envelope(message: &Message) -> Option<Envelope> {
    if message.len() != 4 {
        return None;
    }
    let frames: [&[u8]; 3] =
        std::array::from_fn(|index| message.part_slice(index + 1).expect("four frames"));
    decode_packet(&frames, ozzy_proto::EnvelopeLimits::default())
        .ok()
        .map(|packet| packet.envelope)
}

fn wait(work: Arc<DataSignal>) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    Box::pin(async move { work.ready().await })
}
