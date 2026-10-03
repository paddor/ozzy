//! Destination-owned dispatch admission, independent of transport delivery.
//!
//! Only an application shard creates grants. Dispatch consumes the reserved
//! capacity, including while frames are in transport. Data and control have
//! disjoint budgets. These reservations do not establish record confirmation.

mod credit;
mod hot;
mod lane;
mod returns;
mod state;
#[cfg(test)]
mod tests;

pub use credit::{Admission, Client, Grant, GrantKey, Owner, Retention};
pub use lane::{Received, Receiver, SendError, SendFailure, Sender, channel};

/// Independent admission classes. Data cannot borrow control capacity.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Class {
    /// APPEND or replication data.
    Data,
    /// Elections, confirmations, session traffic, and credit.
    Control,
}

impl Class {
    const fn index(self) -> usize {
        match self {
            Self::Data => 0,
            Self::Control => 1,
        }
    }
}

/// Capacity for one admission class, including unused grants.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Budget {
    /// Reserved or occupied dispatcher-to-shard queue slots.
    pub queue_slots: usize,
    /// Reserved messages or admitted messages with live payload references.
    pub retained_messages: usize,
    /// Full retained backing allocations and descriptor storage, not slice size.
    pub bytes: usize,
}

/// Per-destination or per-client limits for both admission classes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Budgets {
    /// Data traffic allowance.
    pub data: Budget,
    /// Reserved control allowance.
    pub control: Budget,
}

impl Budgets {
    fn array(self) -> [Budget; 2] {
        [self.data, self.control]
    }
}

/// Fixed metadata and payload bounds for one destination shard.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Limits {
    /// Shared by every partition, client, and dispatcher producer for this shard.
    pub capacity: Budgets,
    /// Includes disconnected clients that still own grants or retained payloads.
    pub clients: usize,
    /// Maximum grant tokens per class, including revoked or spent tokens until
    /// the dispatcher drops them. Data cannot exhaust control token storage.
    /// Per-client tokens per class also cannot exceed its retained-message limit.
    pub grants: usize,
}

/// A reservation issued before messages enter a destination queue.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Quota {
    /// Queue slots and retained-message slots reserved together.
    pub messages: usize,
    /// Full retained bytes allowed across those messages.
    pub bytes: usize,
}

/// Admission failed without changing ownership or accepting an operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum Error {
    /// Limits, session identity, or requested quota are invalid.
    #[error("invalid dispatch limits, session, or quota")]
    Invalid,
    /// The requested capacity is already reserved or retained.
    #[error("dispatch capacity exhausted")]
    Full,
    /// The shard owner has stopped granting and consuming capacity.
    #[error("dispatch owner closed")]
    Closed,
    /// Session replacement or explicit revocation fenced this grant.
    #[error("dispatch grant revoked")]
    Revoked,
    /// The frame carries another session's identity.
    #[error("dispatch session mismatch")]
    Session,
    /// The client or grant belongs to another destination shard.
    #[error("dispatch destination mismatch")]
    Destination,
}
