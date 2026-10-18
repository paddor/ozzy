//! Exact receipt data published by the writer's confirmed prefix. The owner
//! stores offsets before releasing that prefix; observers acquire it first.
//! One observer per record may claim a targeted waker, so publishing a prefix
//! wakes only its own observers instead of every waiting record.

#[cfg(all(test, ozzy_loom))]
use loom::future::AtomicWaker;
#[cfg(all(test, ozzy_loom))]
use loom::sync::atomic::{AtomicBool, AtomicU64, Ordering, fence};
use ozzy_proto::{MessageId, append::AppendDisposition};
use std::sync::Arc;
#[cfg(not(all(test, ozzy_loom)))]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering, fence};
use std::task::Waker;

#[cfg(not(all(test, ozzy_loom)))]
use futures::task::AtomicWaker;

/// One record's receipt cell. No payload or admission state. The allocating
/// caller stores the identity before sharing the receipt; only the driver
/// writes the outcome, once.
#[derive(Debug)]
struct Cell {
    message_id: [AtomicU64; 2],
    offset: AtomicU64,
    duplicate: AtomicBool,
    /// Set while one observer owns `waker`; other observers use the broadcast.
    watched: AtomicBool,
    waker: AtomicWaker,
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
            watched: AtomicBool::new(false),
            waker: AtomicWaker::new(),
        }))
    }
}

// The existing per-record metadata reservation covers even a page whose last
// retained receipt is its only live slot. Pages retain neither payload nor I/O.
#[cfg(not(all(test, ozzy_loom)))]
const _: () = assert!(size_of::<Page>() + 2 * size_of::<usize>() <= 1024);

/// Order the driver's confirmed-prefix store before its `watched` loads. Pairs
/// with the fence in [`Completion::watch`]: either the driver sees the claim
/// and wakes, or the observer sees the prefix after claiming.
pub(super) fn published() {
    fence(Ordering::SeqCst);
}

/// Exclusive targeted wakeup for one observer of one record.
#[derive(Debug)]
pub(super) struct Watch<'a>(&'a Cell);

impl Watch<'_> {
    /// Register before checking the confirmed prefix.
    pub(super) fn register(&self, waker: &Waker) {
        #[cfg(all(test, ozzy_loom))]
        self.0.waker.register_by_ref(waker);
        #[cfg(not(all(test, ozzy_loom)))]
        self.0.waker.register(waker);
    }
}

impl Drop for Watch<'_> {
    fn drop(&mut self) {
        // Release the caller's waker before another observer may claim.
        #[cfg(all(test, ozzy_loom))]
        drop(self.0.waker.take_waker());
        #[cfg(not(all(test, ozzy_loom)))]
        drop(self.0.waker.take());
        self.0.watched.store(false, Ordering::Release);
    }
}

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

    /// Claim this record's targeted waker, or `None` while another observer
    /// holds it. Check the confirmed prefix after claiming.
    pub(super) fn watch(&self) -> Option<Watch<'_>> {
        let cell = self.cell();
        if cell.watched.swap(true, Ordering::Acquire) {
            return None;
        }
        fence(Ordering::SeqCst);
        Some(Watch(cell))
    }

    /// Wake a claimed observer. Call after [`published`] follows the prefix store.
    pub(super) fn wake_watched(&self) {
        let cell = self.cell();
        if cell.watched.load(Ordering::Relaxed) {
            cell.waker.wake();
        }
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
