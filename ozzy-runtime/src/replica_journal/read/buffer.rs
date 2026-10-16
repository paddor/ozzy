//! One preallocated read arena, returned even when a queued completion is canceled.

use std::ops::{Deref, DerefMut};
use std::{cell::RefCell, rc::Rc, sync::Arc};

use super::{PartitionReadLimits, ReadRecord};
use crate::replica_journal::{AppendBuffer, JournalError};
use crate::returning::{Lent, ReturnSlot, Returning};
use futures::task::AtomicWaker;

#[derive(Debug)]
struct Arena {
    buffer: AppendBuffer,
    records: Vec<ReadRecord>,
    parts: Vec<std::ops::Range<usize>>,
    bounds: Option<(usize, usize)>,
}

impl Arena {
    fn new(buffer: AppendBuffer) -> Self {
        Self {
            buffer,
            records: Vec::new(),
            parts: Vec::new(),
            bounds: None,
        }
    }

    fn clear(&mut self) {
        self.buffer.clear();
        self.records.clear();
        self.parts.clear();
    }

    fn reserve(&mut self, limits: PartitionReadLimits) -> Result<(), JournalError> {
        if self.bounds.is_some_and(|(records, parts)| {
            limits.max_records > records || limits.max_parts > parts
        }) {
            return Err(super::PartitionReadError::Limits.into());
        }
        self.records
            .try_reserve_exact(limits.max_records.saturating_sub(self.records.len()))
            .map_err(|_| JournalError::AppendCapacity)?;
        self.parts
            .try_reserve_exact(limits.max_parts.saturating_sub(self.parts.len()))
            .map_err(|_| JournalError::AppendCapacity)?;
        Ok(())
    }
}

/// The pool a leased arena returns to. A standalone arena has none.
type Pooled = Option<ReturnSlot<Arena, AtomicWaker>>;

/// Shared handle to exactly one startup-reserved read arena. Cloning the handle
/// does not create another arena or permit concurrent use of its mutable bytes.
#[derive(Debug, Clone)]
pub(crate) struct PartitionReadBuffer(Rc<RefCell<Lent<Arena, AtomicWaker>>>);

impl PartitionReadBuffer {
    pub(crate) fn new(
        buffer: AppendBuffer,
        limits: PartitionReadLimits,
        wake: Arc<AtomicWaker>,
    ) -> Result<Self, JournalError> {
        let mut arena = Arena::new(buffer);
        arena.reserve(limits)?;
        arena.bounds = Some((limits.max_records, limits.max_parts));
        Ok(Self(Rc::new(RefCell::new(Lent::new(arena, wake)))))
    }

    /// Take the existing arena when no disk command or delivery still owns it.
    /// The lease reserves its return slot now, so dropping it cannot fail.
    pub(crate) fn try_lease(&self) -> Option<PartitionReadLease> {
        let (mut arena, slot) = self.0.borrow_mut().try_lend()?;
        arena.clear();
        Some(PartitionReadLease(Returning::new(arena, Some(slot))))
    }
}

/// Exclusive arena ownership through command, completion and delivery. Dropping
/// a pooled lease returns the same allocation and wakes its owner, who clears it
/// before lending it again.
/// Native subscriptions reserve payload and flat descriptors at startup. A
/// standalone lease converted from an `AppendBuffer` reserves descriptors on its
/// first read and retains them across subsequent `ReadPartition::into_buffer` calls.
#[derive(Debug)]
pub struct PartitionReadLease(Returning<Arena, Pooled>);

impl From<AppendBuffer> for PartitionReadLease {
    fn from(buffer: AppendBuffer) -> Self {
        Self(Returning::new(Arena::new(buffer), None))
    }
}

impl Deref for PartitionReadLease {
    type Target = AppendBuffer;
    fn deref(&self) -> &Self::Target {
        &self.0.buffer
    }
}

impl DerefMut for PartitionReadLease {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0.buffer
    }
}

impl PartitionReadLease {
    /// Clear payload and descriptors, retaining their existing allocations.
    pub fn clear(&mut self) {
        self.0.clear();
    }

    pub(in crate::replica_journal) fn records(&self) -> &[ReadRecord] {
        &self.0.records
    }

    pub(super) fn parts(&self) -> &[std::ops::Range<usize>] {
        &self.0.parts
    }

    pub(in crate::replica_journal) fn prepare(
        &mut self,
        limits: PartitionReadLimits,
    ) -> Result<(), JournalError> {
        self.0.reserve(limits)
    }

    /// Reserve payload for the records about to be copied from one stored group.
    pub(in crate::replica_journal) fn reserve_payload(
        &mut self,
        bytes: usize,
    ) -> Result<(), JournalError> {
        self.0.buffer.reserve_read_parts(bytes)
    }

    pub(in crate::replica_journal) fn push_view(
        &mut self,
        record: &ozzy_journal_segment::RecordView<'_>,
    ) -> Result<(), JournalError> {
        self.push_record(record.message_id(), record.encoding(), record.parts())
    }

    pub(in crate::replica_journal) fn push_record<'a>(
        &mut self,
        id: ozzy_proto::MessageId,
        encoding: ozzy_proto::data::Encoding,
        parts: impl Iterator<Item = &'a [u8]>,
    ) -> Result<(), JournalError> {
        let arena = &mut *self.0;
        let first = arena.parts.len();
        for part in parts {
            arena.parts.push(arena.buffer.push_read_part(part)?);
        }
        arena.records.push(ReadRecord {
            encoding,
            id,
            parts: first..arena.parts.len(),
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ozzy_replication::{JournalGeneration, PipelineLimits};

    #[tokio::test]
    async fn canceled_completion_returns_original_payload_and_descriptor_allocations() {
        let permit = Arc::new(tokio::sync::Semaphore::new(1))
            .acquire_owned()
            .await
            .unwrap();
        let buffer = AppendBuffer::new(
            JournalGeneration(1),
            PipelineLimits {
                max_operations: 1,
                max_body_bytes: 4096,
            },
            permit,
        );
        let limits = PartitionReadLimits {
            max_records: 4,
            max_parts: 8,
            max_payload_bytes: 4096,
        };
        let pool = PartitionReadBuffer::new(buffer, limits, Arc::new(AtomicWaker::new())).unwrap();
        let mut lease = pool.try_lease().unwrap();
        lease.push_read_part(b"pending bytes").unwrap();
        let payload = lease.read_bytes().as_ptr();
        let records = lease.0.records.as_ptr();
        let parts = lease.0.parts.as_ptr();
        assert!(pool.try_lease().is_none());
        let (done, completion) = crate::completion::channel();
        drop(completion);
        drop(done.send(lease));
        let mut returned = pool.try_lease().unwrap();
        assert_eq!(returned.body_bytes(), 0);
        assert_eq!(returned.read_bytes().as_ptr(), payload);
        assert_eq!(returned.0.records.as_ptr(), records);
        assert_eq!(returned.0.parts.as_ptr(), parts);
        returned.prepare(limits).unwrap();
        assert_eq!(returned.0.records.as_ptr(), records);
        assert_eq!(returned.0.parts.as_ptr(), parts);
        assert!(
            returned
                .prepare(PartitionReadLimits {
                    max_records: 5,
                    ..limits
                })
                .is_err()
        );
    }
}
