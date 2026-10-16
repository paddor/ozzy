#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable,
        clippy::too_many_lines,
        clippy::too_many_arguments,
        reason = "product code is linted without cfg(test)"
    )
)]

mod completion;
pub mod dispatch;
pub mod frontend;
pub mod memory;
mod native_frames;
mod peer_sessions;
pub use ozzy_telemetry as profiling;
mod reader_service;
mod returning;
// Kept as a crate path so the Loom tests can substitute their model.
use ozzy_io::signal;
mod turn;

pub mod replica_actor;
pub mod replica_journal;
pub mod replica_transport;
pub mod replicated;
pub mod topic_metadata;
pub mod transport;

/// Native link failure.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// OMQ transport or socket failure.
    #[error(transparent)]
    Omq(#[from] omq_tokio::Error),
    /// Malformed Ozzy packet.
    #[error(transparent)]
    Protocol(#[from] ozzy_proto::data::CodecError),
    /// Invalid shared envelope.
    #[error(transparent)]
    Envelope(#[from] ozzy_proto::EnvelopeError),
    /// Invalid peer negotiation.
    #[error(transparent)]
    Handshake(#[from] ozzy_proto::handshake::HandshakeError),
    /// Invalid negative response.
    #[error(transparent)]
    Nack(#[from] ozzy_proto::nack::NackError),
    /// No established native session for this peer.
    #[error("peer session is not established")]
    NotConnected,
    /// Correlated request table reached its configured bound.
    #[error("too many requests in flight")]
    TooManyPendingRequests,
}

/// Native link result.
pub type Result<T, E = Error> = std::result::Result<T, E>;
