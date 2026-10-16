//! Exact receipt data published by the writer's confirmed prefix. The owner
//! stores offsets before releasing that prefix; observers acquire it first.

#[cfg(all(test, ozzy_loom))]
use loom::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use ozzy_proto::{MessageId, append::AppendDisposition};
use std::sync::Arc;
#[cfg(not(all(test, ozzy_loom)))]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// One record's receipt cell. No payload or admission state. The allocating
/// caller stores the identity before sharing the receipt; only the driver
/// writes the outcome, once.
#[derive(Debug)]
struct Cell {
    message_id: [AtomicU64; 2],
    offset: AtomicU64,
    duplicate: AtomicBool,
}

const PAGE_RECORDS: usize = 16;

#[derive(Debug)]
struct Page([Cell; PAGE_RECORDS]);

impl Page {
    fn new() -> Self {
        Self(std::array::from_fn(|_| Cell {
            message_id: [AtomicU64::new(0), AtomicU64::new(0)],
            offset: AtomicU64::new(0),
            duplicate: AtomicBool::new(false),
        }))
    }
}

// The existing per-record metadata reservation covers even a page whose last
// retained receipt is its only live slot. Pages retain neither payload nor I/O.
const _: () = assert!(size_of::<Page>() + 2 * size_of::<usize>() <= 1024);

/// Immutable identity and one receipt cell, shared by intake and pending receipt.
#[derive(Debug, Clone)]
pub(super) struct Completion {
    page: Arc<Page>,
    slot: usize,
}

impl Completion {
    fn cell(&self) -> &Cell {
        &self.page.0[self.slot]
    }

    pub(super) fn message_id(&self) -> MessageId {
        let [high, low] = &self.cell().message_id;
        let high = u128::from(high.load(Ordering::Relaxed));
        let low = u128::from(low.load(Ordering::Relaxed));
        MessageId::from_bytes((high << 64 | low).to_be_bytes())
    }

    /// The driver must publish its confirmed prefix after storing this offset.
    pub(super) fn publish(&self, offset: u64, disposition: AppendDisposition) {
        self.cell().offset.store(offset, Ordering::Relaxed);
        self.cell().duplicate.store(
            matches!(disposition, AppendDisposition::Duplicate),
            Ordering::Relaxed,
        );
    }

    /// The caller must first acquire a confirmed prefix covering this record.
    pub(super) fn offset(&self) -> u64 {
        self.cell().offset.load(Ordering::Relaxed)
    }

    /// The caller must first acquire the same confirmed prefix as for the offset.
    pub(super) fn disposition(&self) -> AppendDisposition {
        if self.cell().duplicate.load(Ordering::Relaxed) {
            AppendDisposition::Duplicate
        } else {
            AppendDisposition::Stored
        }
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
        let page = match &self.page {
            Some(page) if self.next < PAGE_RECORDS => page.clone(),
            _ => {
                self.next = 0;
                self.page.insert(Arc::new(Page::new())).clone()
            }
        };
        let slot = self.next;
        self.next += 1;
        let completion = Completion { page, slot };
        let id = u128::from_be_bytes(*message_id.as_bytes());
        let [high, low] = &completion.cell().message_id;
        high.store((id >> 64) as u64, Ordering::Relaxed);
        low.store(id as u64, Ordering::Relaxed);
        completion
    }
}

#[cfg(all(test, not(ozzy_loom)))]
mod tests {
    use super::*;

    #[test]
    fn retained_receipts_survive_rollover_and_pool_drop_without_identity_or_offset_changes() {
        let mut pool = Pool::default();
        let first = pool.allocate(MessageId::from_bytes([1; 16]));
        first.publish(u64::MAX, AppendDisposition::Duplicate);
        let alias = first.clone();
        let receipts: Vec<_> = (2..100)
            .map(|id| pool.allocate(MessageId::from_bytes([id; 16])))
            .collect();
        for (index, receipt) in receipts.iter().enumerate().rev() {
            receipt.publish(index as u64 * 7, AppendDisposition::Stored);
        }
        drop(pool);
        drop(first);
        assert_eq!(alias.message_id(), MessageId::from_bytes([1; 16]));
        assert_eq!(alias.offset(), u64::MAX);
        assert_eq!(alias.disposition(), AppendDisposition::Duplicate);
        for (index, receipt) in receipts.iter().enumerate() {
            assert_eq!(
                receipt.message_id(),
                MessageId::from_bytes([index as u8 + 2; 16])
            );
            assert_eq!(receipt.offset(), index as u64 * 7);
        }
    }
}
