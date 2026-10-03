//! Local queue classes and memory bounds. Transport backpressure owns flow.

/// Independent admission classes. Data cannot borrow control capacity.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Class {
    /// APPEND or replication data.
    Data,
    /// Elections, confirmations, and session traffic.
    Control,
}

/// Owner-local count and retained-memory capacity for one admission class.
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

/// Local queue refusal, never a confirmation boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum Error {
    /// Configuration exceeds the queue bounds.
    #[error("invalid queue limits")]
    Invalid,
    /// The owner has no free capacity.
    #[error("queue full")]
    Full,
    /// The destination owner has stopped.
    #[error("queue closed")]
    Closed,
}

/// A bounded local send failed before transferring ownership.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SendFailure {
    /// Local capacity refusal.
    Admission(Error),
    /// Destination ended.
    Closed,
    /// Queue disagrees with its owner-local count.
    CapacityInvariant,
}
