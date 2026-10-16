//! Individual processing submissions, with cumulative broker confirmations.

pub(crate) mod driver;
pub(crate) mod progress;
pub(crate) mod window;

use ozzy_proto::{ConsumerGroupId, ConsumerMemberId, Offset, PartitionIncarnation, append::Policy};
use progress::Observer;

/// Exact group ownership and durability boundary of one processing window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessingIdentity {
    /// Persistent consumer-group incarnation.
    pub group: ConsumerGroupId,
    /// Persistent source partition incarnation.
    pub partition: PartitionIncarnation,
    /// Assigned processing member.
    pub member: ConsumerMemberId,
    /// Source partition's assignment epoch.
    pub assignment_epoch: u64,
    /// Required broker confirmation policy.
    pub policy: Policy,
}

/// Processing failure. No error silently advances an application's cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ProgressError {
    /// Invalid initial identity, cursor, or resource configuration.
    #[error("invalid processing configuration")]
    Configuration,
    /// Processing owner stopped; already confirmed observations remain successful.
    #[error("processing owner stopped")]
    Closed,
    /// Group assignment no longer belongs to this member and epoch.
    #[error("processing assignment is fenced")]
    Fenced,
    /// Token belongs to another processing window or was never delivered here.
    #[error("invalid processing token")]
    Token,
    /// The bounded processing window or its metadata budget is full.
    #[error("processing capacity exhausted")]
    Capacity,
    /// Delivery or confirmation would skip records or contradict known progress.
    #[error("processing response contradicts the tracked cursor")]
    Response,
    /// Persisted processing progress no longer has retained source history.
    #[error("processing cursor {cursor:?} precedes retained history {retained_from:?}")]
    HistoryExpired {
        /// Exclusive persisted resume cursor.
        cursor: Offset,
        /// Earliest still-retained source record.
        retained_from: Offset,
    },
}

/// One delivered record's processing identity. Dropping it acknowledges nothing.
#[derive(Debug)]
pub struct ProcessingToken {
    progress: Observer,
    cursor: Offset,
}

impl ProcessingToken {
    /// Ownership under which this record was delivered.
    pub fn identity(&self) -> ProcessingIdentity {
        self.progress.identity()
    }

    /// Original source record offset, independent of delivery batching.
    pub fn offset(&self) -> Offset {
        Offset::new(self.cursor.get() - 1)
    }
}

/// Broker confirmation covering an individual processing acknowledgment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProgressReceipt {
    /// Exact processing ownership and required confirmation policy.
    pub identity: ProcessingIdentity,
    /// Exclusive cursor covered by the confirmed cumulative broker progress.
    pub cursor: Offset,
}

/// Read-only confirmation handle, separate from acknowledgment submission.
/// Dropping it does not undo the application's submitted acknowledgment.
#[derive(Debug)]
pub struct PendingProgress {
    progress: Observer,
    cursor: Offset,
}

impl PendingProgress {
    /// Exclusive cursor required for this individual acknowledgment.
    pub const fn cursor(&self) -> Offset {
        self.cursor
    }

    /// Wait for broker confirmation, without spawning a per-record task.
    /// Cancellation preserves the submission and any subsequently confirmed result.
    pub async fn confirmed(&self) -> Result<ProgressReceipt, ProgressError> {
        self.progress.wait(self.cursor.get()).await?;
        Ok(self.receipt())
    }

    /// Observe confirmation or terminal failure without waiting.
    pub fn try_confirmed(&self) -> Option<Result<ProgressReceipt, ProgressError>> {
        self.progress
            .observe(self.cursor.get())
            .map(|result| result.map(|()| self.receipt()))
    }

    fn receipt(&self) -> ProgressReceipt {
        ProgressReceipt {
            identity: self.progress.identity(),
            cursor: self.cursor,
        }
    }
}

#[cfg(all(test, not(ozzy_loom)))]
mod tests;
