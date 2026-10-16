//! Bounded subscriptions on established shared broker links. No sockets or tasks.

mod output;
mod publication;
mod receive;

use super::{ActorError, NativeReceive, PartitionActor, reader_cursor::Cursor};
use crate::transport::Frames;
use crate::{
    frontend::{Kind, Link, Links},
    reader_service::{Delivery, Failure, reader_frame},
    replica_journal::{PartitionReadBuffer, PartitionReadLimits, ReplicaJournal},
    replicated::payload::Payload,
};
use bytes::Bytes;
use futures::task::AtomicWaker;
use omq_tokio::{Message, TrySendError};
use ozzy_proto::{
    Envelope, NodeId, Opcode, PartitionIncarnation, data::DataLimits, decode_packet,
    nack::AuthorityHint, reader,
};
use ozzy_replication::driver::ValidationTicket;
use ozzy_replication::{JournalGeneration, PipelineLimits, Prefix, Scope};
use std::{
    sync::Arc,
    task::{Context, Poll},
};

/// Fixed partition-local reader bounds. Physical broker links remain shared.
#[derive(Clone, Copy, Debug)]
pub struct SharedReaderConfig {
    /// Checked partition incarnation on this actor.
    pub partition: PartitionIncarnation,
    /// Maximum one delivery, narrowed by the link's codec limits.
    pub limits: DataLimits,
    /// Aggregate subscriptions on this partition, including idle readers.
    pub subscriptions: usize,
}

/// What one poll reads with: this broker's identity and the journal view
/// current for the poll.
struct Visit<'a> {
    local: NodeId,
    ticket: Option<ValidationTicket>,
    journal: &'a mut ReplicaJournal,
}

/// A consumer and its current link, which bounds and carries replies to it.
#[derive(Clone, Copy, Debug)]
struct Client {
    peer: NodeId,
    link: Link,
}

#[derive(Debug)]
struct Slot {
    peer: NodeId,
    payload: Payload,
    delivery: Delivery<Cursor>,
    pending: Option<Message>,
    resolved_offset: Option<u64>,
    reply_due: bool,
}

pub(super) struct SharedReaders {
    local: NodeId,
    config: SharedReaderConfig,
    links: Links,
    /// Installed subscriptions, at most `config.subscriptions`, preallocated.
    slots: Vec<Slot>,
    pool: PartitionReadBuffer,
    metadata: Vec<u8>,
    rejection: Option<Message>,
    next: usize,
    observed: Option<(Scope, JournalGeneration, Prefix)>,
    /// Registered by each visit before it checks reader state. Returned read
    /// arenas and payloads, also from transport threads, wake it.
    wake: Arc<AtomicWaker>,
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
        let wake = Arc::new(AtomicWaker::new());
        let pool = PartitionReadBuffer::new(
            buffer,
            PartitionReadLimits {
                max_records: config.limits.max_records,
                max_parts: config.limits.max_parts,
                max_payload_bytes: config.limits.envelope.max_payload_bytes,
            },
            wake.clone(),
        )?;
        let publication = publication::Publication::new(journal, config, wake.clone())?;
        Ok(Self {
            local,
            config,
            links,
            slots: Vec::with_capacity(config.subscriptions),
            pool,
            metadata: Vec::with_capacity(config.limits.envelope.max_metadata_bytes),
            rejection: None,
            next: 0,
            observed: None,
            wake,
            publication,
        })
    }

    /// Whether a framed publication still waits for the dispatcher.
    pub(super) fn publishing(&self) -> bool {
        self.publication.waiting()
    }

    pub(super) fn supports(message: &Message) -> bool {
        envelope(message).is_some_and(|e| {
            matches!(
                e.opcode,
                Opcode::Subscribe | Opcode::Ack | Opcode::Unsubscribe
            )
        })
    }

    fn current(&self, peer: NodeId, envelope: Envelope) -> Option<Client> {
        self.links
            .get(peer)
            .filter(|link| {
                link.binding.kind == Kind::Client
                    && envelope.session == Some(link.binding.session)
                    && envelope.sender == peer
                    && link.remote.roles & ozzy_proto::handshake::CONSUMER != 0
            })
            .map(|link| Client { peer, link })
    }

    /// Whether this client's refusal or acknowledgment still waits in the
    /// shared reply slot. Its next command must wait for that reply.
    pub(super) fn client_work(&self, node: NodeId) -> usize {
        usize::from(
            self.rejection
                .as_ref()
                .is_some_and(|message| message.part_slice(0) == Some(node.as_bytes().as_slice())),
        )
    }

    fn discard_stale(&mut self) {
        let links = &self.links;
        self.slots.retain(|slot| {
            links
                .get(slot.peer)
                .is_some_and(|link| Some(link.binding.session) == slot.delivery.request.session)
        });
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
    let (_, frames) = message.routed_packet()?;
    decode_packet(&frames, ozzy_proto::EnvelopeLimits::default())
        .ok()
        .map(|packet| packet.envelope)
}
