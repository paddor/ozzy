//! Bounded local retention and receipt accounting, independent of consensus and durability.
//!
//! Adapters authenticate peers, fence epochs on every packet, retain payloads,
//! verify canonical history, and supply release events only after application.
//! This module performs no I/O, clock reads, hashing, voting, or implicit recovery.
//! Probe deadlines advance only from explicit adapter-supplied monotonic time.
//! See `doc/REPLICATION.md` for the surrounding probe/repair integration contract.

mod probe;
mod receiver;
mod sender;
mod transmit;

pub use probe::{Probe, ProbeError, ProbeScheduler, ProbeTiming};
pub use receiver::Receiver;
pub use sender::Sender;
pub use transmit::{OpenRequest, Repair, StatusOutcome, TransmitError, Transmitter};

use std::collections::VecDeque;

use crate::{Digest, PipelineLimits, Prefix, Scope};

/// Receiver-issued incarnation of volatile retention state.
///
/// Supply a fresh unpredictable value on restart, scope change, or retraction.
/// Never reuse an epoch for reset state. It is not a durable journal generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReceiveEpoch(u128);

impl ReceiveEpoch {
    /// Validate a nonzero adapter-supplied identity; does not generate randomness.
    pub const fn new(value: u128) -> Result<Self, FlowError> {
        if value == 0 {
            Err(FlowError::Invalid)
        } else {
            Ok(Self(value))
        }
    }

    /// Exact identity for wire encoding and equality checks, never chronological order.
    pub const fn get(self) -> u128 {
        self.0
    }
}

/// Complete scope and receive incarnation for one independently bound peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Channel {
    /// Group, immutable configuration, and current normal view.
    pub scope: Scope,
    /// Receiver-owned volatile incarnation.
    pub epoch: ReceiveEpoch,
}

/// Compact metadata for one hash-validated canonical body retained by the adapter.
///
/// This is neither payload ownership nor application-validation evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Operation {
    /// Exact contiguous prefix ending at this operation.
    pub prefix: Prefix,
    /// Digest of the immediately preceding canonical operation.
    pub previous_digest: Digest,
    /// Canonical body bytes; not compressed, framed, or per-record bytes.
    pub body_bytes: u64,
}

/// Exact cumulative receipt in one receive epoch.
///
/// No field is quorum, durable, or application evidence. The adapter authenticates
/// and correlates reports before opening an epoch; same-epoch updates are monotonic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Report {
    /// Scope/incarnation fence, in addition to the external peer/session binding.
    pub channel: Channel,
    /// Strictly increasing on retention changes, unchanged on retransmission.
    pub revision: u64,
    /// Fixed applied prefix at epoch initialization; cumulative counters start here.
    pub base: Prefix,
    /// Contiguous prefix whose exact bodies are retained in this epoch.
    pub received: Prefix,
    /// Cumulative canonical body bytes after `base` through `received`.
    pub received_bytes: u64,
}

/// Rejected accounting transition. Every failure leaves existing state unchanged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum FlowError {
    /// Zero/invalid identity, prefix, limits, revision, or operation size.
    #[error("invalid replica flow metadata")]
    Invalid,
    /// Scope or receiver incarnation differs; a fresh correlated open is required.
    #[error("replica flow channel mismatch")]
    Channel,
    /// Local count or body capacity is exhausted; retain/backpressure, never over-admit.
    #[error("replica flow capacity exhausted")]
    Capacity,
    /// Prefix/digest/byte accounting does not match the retained contiguous history.
    #[error("replica flow history mismatch")]
    History,
    /// Same-epoch report equivocates or retracts previously advertised state.
    #[error("replica flow report is inconsistent")]
    Report,
    /// A cumulative counter cannot advance; fence and establish a new epoch.
    #[error("replica flow counter exhausted")]
    Exhausted,
    /// Startup could not reserve the configured metadata capacity.
    #[error("replica flow metadata allocation failed")]
    Allocation,
}

fn capacities(limits: PipelineLimits) -> Result<(u64, u64), FlowError> {
    if limits.max_operations == 0 || limits.max_body_bytes == 0 {
        return Err(FlowError::Invalid);
    }
    Ok((
        u64::try_from(limits.max_operations).map_err(|_| FlowError::Invalid)?,
        u64::try_from(limits.max_body_bytes).map_err(|_| FlowError::Invalid)?,
    ))
}

fn ledger(limits: PipelineLimits) -> Result<VecDeque<Operation>, FlowError> {
    capacities(limits)?;
    let mut entries = VecDeque::new();
    entries
        .try_reserve_exact(limits.max_operations)
        .map_err(|_| FlowError::Allocation)?;
    Ok(entries)
}

fn prefix_valid(prefix: Prefix) -> bool {
    (prefix.op.0 == 0) == (prefix.digest == Digest::ZERO)
}

fn batch(operations: &[Operation], previous: Prefix) -> Result<(Prefix, u64), FlowError> {
    if operations.is_empty() {
        return Err(FlowError::Invalid);
    }
    let mut end = previous;
    let mut bytes = 0u64;
    for operation in operations {
        if operation.body_bytes == 0 || !prefix_valid(operation.prefix) {
            return Err(FlowError::Invalid);
        }
        if operation.prefix.op.0 != end.op.0.checked_add(1).ok_or(FlowError::Exhausted)?
            || operation.previous_digest != end.digest
        {
            return Err(FlowError::History);
        }
        bytes = bytes
            .checked_add(operation.body_bytes)
            .ok_or(FlowError::Exhausted)?;
        end = operation.prefix;
    }
    Ok((end, bytes))
}

impl Report {
    /// Validate receipt shape and the caller's local retention configuration.
    /// Does not authenticate or verify history; heterogeneous peers may differ.
    pub fn validate(self, limits: PipelineLimits) -> Result<(), FlowError> {
        capacities(limits)?;
        self.validate_shape()
    }

    /// Validate fixed metadata without applying a negotiated receive-window bound.
    ///
    /// A local retention window can exceed one wire frame. Codecs use this allocation-free
    /// check. Senders separately enforce their own outstanding metadata/body
    /// bounds on every reservation.
    pub fn validate_shape(self) -> Result<(), FlowError> {
        if self.channel.scope.group_id.as_bytes() == &[0; 16]
            || self.channel.scope.configuration_digest == Digest::ZERO
            || self.revision == 0
            || !prefix_valid(self.base)
            || !prefix_valid(self.received)
        {
            return Err(FlowError::Invalid);
        }
        let received = self
            .received
            .op
            .0
            .checked_sub(self.base.op.0)
            .ok_or(FlowError::History)?;
        if (received == 0 && self.received != self.base)
            || (received == 0) != (self.received_bytes == 0)
            || self.received_bytes < received
        {
            return Err(FlowError::Report);
        }
        Ok(())
    }
}
