//! Exact receipt data published by the writer's confirmed prefix. The owner
//! stores offsets before releasing that prefix; observers acquire it first.

#[cfg(all(test, ozzy_loom))]
use loom::sync::atomic::{AtomicU64, Ordering};
use ozzy_proto::MessageId;
#[cfg(not(all(test, ozzy_loom)))]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

/// One record's stable receipt, retained by its queue entry and pending handles.
/// No payload or admission state. Only the driver writes its offset, once.
#[derive(Debug)]
struct RecordCompletion {
    message_id: MessageId,
    offset: AtomicU64,
}

const PAGE_RECORDS: usize = 16;

#[derive(Debug)]
struct Page([OnceLock<RecordCompletion>; PAGE_RECORDS]);

// The existing per-record metadata reservation covers even a page whose last
// retained receipt is its only live slot. Pages retain neither payload nor I/O.
const _: () = assert!(size_of::<Page>() + 2 * size_of::<usize>() <= 1024);

/// Immutable identity and one offset cell, shared by intake and pending receipt.
#[derive(Debug, Clone)]
pub(super) struct Completion {
    page: Arc<Page>,
    slot: usize,
}

impl Completion {
    fn record(&self) -> &RecordCompletion {
        self.page.0[self.slot]
            .get()
            .expect("published receipt identity")
    }

    pub(super) fn message_id(&self) -> MessageId {
        self.record().message_id
    }

    /// The driver must publish its confirmed prefix after storing this offset.
    pub(super) fn publish(&self, offset: u64) {
        self.record().offset.store(offset, Ordering::Relaxed);
    }

    /// The caller must first acquire a confirmed prefix covering this record.
    pub(super) fn offset(&self) -> u64 {
        self.record().offset.load(Ordering::Relaxed)
    }

    #[cfg(all(test, not(ozzy_loom)))]
    pub(super) fn new(message_id: MessageId) -> Self {
        Pool::default().allocate(message_id)
    }
}

/// Caller-owned cursor. Cloned producer handles start independent empty pools.
#[derive(Debug, Default)]
pub(super) struct Pool {
    page: Option<Arc<Page>>,
    next: usize,
}

impl Pool {
    pub(super) fn allocate(&mut self, message_id: MessageId) -> Completion {
        if self.page.is_none() || self.next == PAGE_RECORDS {
            self.page = Some(Arc::new(Page(std::array::from_fn(|_| OnceLock::new()))));
            self.next = 0;
        }
        let page = self.page.as_ref().expect("receipt page allocated");
        let slot = self.next;
        page.0[slot]
            .set(RecordCompletion {
                message_id,
                offset: AtomicU64::new(0),
            })
            .expect("one caller owns each receipt slot");
        self.next += 1;
        Completion {
            page: page.clone(),
            slot,
        }
    }
}

#[cfg(all(test, not(ozzy_loom)))]
mod tests {
    use super::*;

    #[test]
    fn retained_receipts_survive_rollover_and_pool_drop_without_identity_or_offset_changes() {
        let mut pool = Pool::default();
        let first = pool.allocate(MessageId::from_bytes([1; 16]));
        first.publish(u64::MAX);
        let alias = first.clone();
        let receipts: Vec<_> = (2..100)
            .map(|id| pool.allocate(MessageId::from_bytes([id; 16])))
            .collect();
        for (index, receipt) in receipts.iter().enumerate().rev() {
            receipt.publish(index as u64 * 7);
        }
        drop(pool);
        drop(first);
        assert_eq!(alias.message_id(), MessageId::from_bytes([1; 16]));
        assert_eq!(alias.offset(), u64::MAX);
        for (index, receipt) in receipts.iter().enumerate() {
            assert_eq!(
                receipt.message_id(),
                MessageId::from_bytes([index as u8 + 2; 16])
            );
            assert_eq!(receipt.offset(), index as u64 * 7);
        }
    }
}
