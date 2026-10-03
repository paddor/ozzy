//! One preallocated read arena, returned even when a queued completion is canceled.

use std::ops::{Deref, DerefMut};
use std::{cell::RefCell, rc::Rc, sync::Arc};
use tokio::sync::mpsc;

use super::{PartitionReadLimits, ReadRecord};
use crate::replica_journal::{AppendBuffer, JournalError};
use crate::signal::DataSignal;

#[derive(Debug)]
struct Slot {
    buffer: Option<Arena>,
    returns: mpsc::Receiver<Arena>,
    return_to: mpsc::Sender<Arena>,
    work: Arc<DataSignal>,
}

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

/// Shared handle to exactly one startup-reserved read arena. Cloning the handle
/// does not create another arena or permit concurrent use of its mutable bytes.
#[derive(Debug, Clone)]
pub(crate) struct PartitionReadBuffer(Rc<RefCell<Slot>>);

impl PartitionReadBuffer {
    pub(crate) fn new(
        buffer: AppendBuffer,
        limits: PartitionReadLimits,
        work: Arc<DataSignal>,
    ) -> Result<Self, JournalError> {
        let mut arena = Arena::new(buffer);
        arena.reserve(limits)?;
        arena.bounds = Some((limits.max_records, limits.max_parts));
        let (return_to, returns) = mpsc::channel(1);
        Ok(Self(Rc::new(RefCell::new(Slot {
            buffer: Some(arena),
            returns,
            return_to,
            work,
        }))))
    }

    /// Take the existing arena when no disk command or delivery still owns it.
    pub(crate) fn try_lease(&self) -> Option<PartitionReadLease> {
        let mut slot = self.0.borrow_mut();
        slot.buffer
            .take()
            .or_else(|| slot.returns.try_recv().ok())
            .map(|buffer| PartitionReadLease {
                buffer: Some(buffer),
                owner: Some((slot.return_to.clone(), slot.work.clone())),
            })
    }
}

/// Exclusive arena ownership through command, completion and delivery. Dropping
/// a pooled lease clears and returns the same allocation, then signals readiness.
/// Native subscriptions reserve payload and flat descriptors at startup. A
/// standalone lease converted from an `AppendBuffer` reserves descriptors on its
/// first read and retains them across subsequent `ReadPartition::into_buffer` calls.
#[derive(Debug)]
pub struct PartitionReadLease {
    buffer: Option<Arena>,
    owner: Option<(mpsc::Sender<Arena>, Arc<DataSignal>)>,
}

impl From<AppendBuffer> for PartitionReadLease {
    fn from(buffer: AppendBuffer) -> Self {
        Self {
            buffer: Some(Arena::new(buffer)),
            owner: None,
        }
    }
}

impl Deref for PartitionReadLease {
    type Target = AppendBuffer;
    fn deref(&self) -> &Self::Target {
        &self.buffer.as_ref().expect("live read arena").buffer
    }
}

impl DerefMut for PartitionReadLease {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buffer.as_mut().expect("live read arena").buffer
    }
}

impl PartitionReadLease {
    /// Clear payload and descriptors, retaining their existing allocations.
    pub fn clear(&mut self) {
        self.buffer.as_mut().expect("live read arena").clear();
    }

    pub(in crate::replica_journal) fn records(&self) -> &[ReadRecord] {
        &self.buffer.as_ref().expect("live read arena").records
    }

    pub(super) fn parts(&self) -> &[std::ops::Range<usize>] {
        &self.buffer.as_ref().expect("live read arena").parts
    }

    pub(in crate::replica_journal) fn prepare(
        &mut self,
        limits: PartitionReadLimits,
    ) -> Result<(), JournalError> {
        self.buffer
            .as_mut()
            .expect("live read arena")
            .reserve(limits)
    }

    /// Reserve payload for the records about to be copied from one stored group.
    pub(in crate::replica_journal) fn reserve_payload(
        &mut self,
        bytes: usize,
    ) -> Result<(), JournalError> {
        self.buffer
            .as_mut()
            .expect("live read arena")
            .buffer
            .reserve_read_parts(bytes)
    }

    pub(in crate::replica_journal) fn push_view(
        &mut self,
        record: &ozzy_journal_segment::RecordView<'_>,
    ) {
        self.push_record(record.message_id(), record.encoding(), record.parts());
    }

    pub(in crate::replica_journal) fn push_record<'a>(
        &mut self,
        id: ozzy_proto::MessageId,
        encoding: ozzy_proto::data::Encoding,
        parts: impl Iterator<Item = &'a [u8]>,
    ) {
        let arena = self.buffer.as_mut().expect("live read arena");
        let first = arena.parts.len();
        for part in parts {
            arena.parts.push(
                arena
                    .buffer
                    .push_read_part(part)
                    .expect("checked payload read bound"),
            );
        }
        arena.records.push(ReadRecord {
            encoding,
            id,
            parts: first..arena.parts.len(),
        });
    }
}

impl Drop for PartitionReadLease {
    fn drop(&mut self) {
        if let Some((return_to, work)) = &self.owner {
            let mut buffer = self.buffer.take().expect("live read arena");
            buffer.clear();
            match return_to.try_send(buffer) {
                Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    panic!("single read arena return slot overflowed")
                }
            }
            work.mark();
        }
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
        let pool =
            PartitionReadBuffer::new(buffer, limits, Arc::new(DataSignal::default())).unwrap();
        let mut lease = pool.try_lease().unwrap();
        lease.push_read_part(b"pending bytes").unwrap();
        let payload = lease.read_bytes().as_ptr();
        let records = lease.buffer.as_ref().unwrap().records.as_ptr();
        let parts = lease.buffer.as_ref().unwrap().parts.as_ptr();
        assert!(pool.try_lease().is_none());
        let (done, completion) = crate::completion::channel();
        drop(completion);
        drop(done.send(lease));
        let mut returned = pool.try_lease().unwrap();
        assert_eq!(returned.body_bytes(), 0);
        assert_eq!(returned.read_bytes().as_ptr(), payload);
        assert_eq!(returned.buffer.as_ref().unwrap().records.as_ptr(), records);
        assert_eq!(returned.buffer.as_ref().unwrap().parts.as_ptr(), parts);
        returned.prepare(limits).unwrap();
        assert_eq!(returned.buffer.as_ref().unwrap().records.as_ptr(), records);
        assert_eq!(returned.buffer.as_ref().unwrap().parts.as_ptr(), parts);
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
