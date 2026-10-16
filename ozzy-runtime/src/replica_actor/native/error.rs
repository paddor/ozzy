//! Terminal native intake errors.

use ozzy_proto::nack;

/// Terminal adapter error. No error creates confirmation evidence.
#[derive(Debug, thiserror::Error)]
pub enum NativeIntakeError {
    /// Invalid startup identities, admission limits, or arena count/capacity.
    #[error("invalid native partition intake configuration")]
    Configuration,
    /// Actor stopped with an unresolved proposal.
    #[error(transparent)]
    Stopped(#[from] crate::replica_actor::ProposalStopped),
    /// Completed arena does not describe the admitted request.
    #[error("native proposal result does not match its request")]
    History,
    /// A slot lacks the arena, reply or update its phase holds.
    #[error("native slot state does not match its phase")]
    Slot,
    /// Bounded wire result cannot be encoded.
    #[error(transparent)]
    Codec(#[from] ozzy_proto::data::CodecError),
    /// Bounded rejection cannot be encoded.
    #[error(transparent)]
    Nack(#[from] nack::NackError),
    /// Shared outgoing transport failed.
    #[error(transparent)]
    Transport(#[from] omq_tokio::Error),
}
