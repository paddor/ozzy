//! Shared native SDK links for topic writers and readers.
//!
//! Writers submit individual records. Partition selection precedes transparent
//! batching and adaptive LZ4. PEER carries APPEND, confirmation, repair, and
//! control; SUB receives live publications from broker PUB. Each partition
//! retains independent retry identity and authority over shared broker links.

mod broker_links;
mod retry;
pub use broker_links::{
    AppendLinkLimits, BrokerAddress, BrokerLinkError, BrokerLinks, BrokerLinksConfig, ClockError,
    ReaderLinkLimits, RouteGeneration, SdkClock, TopicRoutes,
};
pub(crate) mod payload;
mod topic_reader;
mod topic_writer;
pub use topic_reader::{
    ReaderError, TopicCheckpoint, TopicReader, TopicReaderConfig, TopicReaderError,
    TopicReaderStats, TopicRecord,
};
mod writer;

pub use ozzy_proto::append::{Append, AppendKey, Authority, DataLimits, Policy, Record};
pub use ozzy_proto::nack::AuthorityHint;
use ozzy_proto::nack::RetryClass;
pub use ozzy_proto::reader::{Source, Target};
use ozzy_proto::{NodeId, ProducerId};
pub use ozzy_replication::Configuration;
pub use retry::RetryPolicy;
pub use topic_writer::{
    SharedTopicPendingRecord, SharedTopicReceipt, SharedTopicWriter, SharedTopicWriterConfig,
    TopicWriterError,
};
pub use writer::{
    MAX_APPEND_RECORDS, MAX_SEQUENCE, PAYLOAD_COMPRESSION_THRESHOLD, PartitionTarget,
    PendingRecord, RecordInput, RecordReceipt, SharedWriterReservation, Writer, WriterConfig,
    WriterError, WriterRuntime, WriterStats,
};

/// Explicit application access for one trusted or independently authenticated node.
#[derive(Debug, Clone, Copy)]
pub struct ClientAccess {
    /// Expected PEER routing and envelope identity. Does not authenticate a peer.
    pub node: NodeId,
    /// Only this producer identity may append through this node's client session.
    pub producer: ProducerId,
}

/// Startup-bounded record intake on an owner's existing endpoint.
#[derive(Debug, Clone)]
pub struct StreamingConfig {
    /// Trusted or independently authenticated writer identities; at most 32.
    pub peers: Vec<ClientAccess>,
    /// Per-request receive bounds, including SDK-generated record batches.
    pub limits: DataLimits,
    /// Per-writer queued plus unconfirmed record budget, 1 through 65,536.
    pub inflight_records: usize,
    /// Per-writer queued plus unconfirmed payload-byte budget.
    pub inflight_bytes: usize,
    /// Group proposals in flight per writer, at least one. Each leases one
    /// journal append arena. Local owners ignore it.
    pub proposals: usize,
}

/// Startup-bounded native ingress on the same endpoint as voter commands.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// At most 32 configured clients, each with one outstanding request/arena.
    /// Multiple clients share the actor's existing global proposal budget.
    pub peers: Vec<ClientAccess>,
    /// Advertised directional receive limits. Every permitted append must fit a
    /// canonical body buffer including descriptors. No silent fragmentation.
    pub limits: DataLimits,
}

impl ClientConfig {
    /// Conservative size of one canonical append under the advertised bounds.
    /// RAM and disk frontends must account for the same fixed fields, record
    /// descriptors, and part lengths in addition to the wire payload.
    pub(crate) fn canonical_body_bytes(&self) -> Option<usize> {
        self.limits
            .max_records
            .checked_mul(20)?
            .checked_add(self.limits.max_parts.checked_mul(4)?)?
            .checked_add(80)?
            .checked_add(self.limits.envelope.max_payload_bytes)
    }

    /// Disk groups store codec metadata before the exact raw or LZ4 payload.
    pub(crate) fn prepared_canonical_body_bytes(&self) -> Option<usize> {
        self.canonical_body_bytes()?.checked_add(9)
    }
}

/// Native connection error. Cancellation, transport loss, and protocol errors
/// after sending leave the append outcome unknown. Preserve its stable identity.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Invalid local bounds or identity configuration.
    #[error("invalid native connection configuration")]
    Configuration,
    /// Socket failed locally; no inference about a previously admitted append.
    #[error(transparent)]
    Transport(#[from] omq_tokio::Error),
    /// Shared broker control or routing failed independently of confirmation.
    #[error(transparent)]
    SharedLink(#[from] BrokerLinkError),
    /// Invalid negotiation, not permission to fall back to weaker guarantees.
    #[error(transparent)]
    Handshake(#[from] ozzy_proto::handshake::HandshakeError),
    /// Local encode or remote receipt schema validation failed.
    #[error(transparent)]
    Append(#[from] ozzy_proto::append::CodecError),
    /// SDK group compression failed before transport admission.
    #[error("APPEND compression failed")]
    AppendCompression,
    /// Remote response does not match the live request/session/policy/record IDs.
    #[error("native response does not match the request")]
    Response,
    /// Explicit rejection of this attempt; previous attempts may still have committed.
    #[error("native append rejected: code {code}, retry {retry:?}")]
    Rejected {
        /// Protocol error code. Unknown values are preserved.
        code: u16,
        /// Machine-readable retry classification.
        retry: RetryClass,
        /// Optional routing hint, not permission to write or claim commit.
        hint: Option<AuthorityHint>,
    },
}
