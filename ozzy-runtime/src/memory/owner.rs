use super::{Limits, Reservation};
use crate::returning::{GiveBack, Returning};
use crate::signal::StateSignal;
use bytes::Bytes;
use std::{
    cell::RefCell,
    fmt, io,
    ops::{Deref, DerefMut},
    rc::{Rc, Weak as LocalWeak},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread::{self, ThreadId},
};
use tokio::sync::mpsc;

/// Allocation and cache owner, confined to its application shard. Clones share
/// capacity; additional partitions never create additional memory budgets.
#[derive(Clone, Debug)]
pub struct Owner(Rc<RefCell<Local>>);

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Capacity {
    bytes: usize,
    buffers: usize,
}

impl Capacity {
    pub(crate) const EMPTY: Self = Self {
        bytes: 0,
        buffers: 0,
    };
}

/// Owner-local reservation for bounded maintenance scratch outside payload arenas.
#[derive(Debug)]
pub(crate) struct Charge {
    owner: Owner,
    bytes: usize,
}

impl Drop for Charge {
    fn drop(&mut self) {
        let local = self.owner.0.borrow();
        local
            .shared
            .usage
            .bytes
            .fetch_sub(self.bytes, Ordering::AcqRel);
        local.shared.usage.changed.notify_changed();
    }
}

/// Movable journal arenas may retain this weak capability. Allocation still
/// requires the original owner thread and a live owner; backend observers may
/// only retain or release the resulting bytes.
#[derive(Clone, Debug)]
pub(crate) struct Allocator {
    local: LocalWeak<RefCell<Local>>,
    thread: ThreadId,
    maximum: usize,
}

#[derive(Debug)]
struct Local {
    returns: mpsc::Receiver<Block>,
    shared: Arc<Return>,
    cache: Vec<Block>,
    cache_bytes: usize,
    limits: Limits,
    pressure: Option<(Capacity, Capacity)>,
}

#[derive(Debug)]
struct Return {
    sender: mpsc::Sender<Block>,
    usage: Arc<Usage>,
}

#[derive(Debug)]
struct Usage {
    _reservation: Reservation,
    bytes: AtomicUsize,
    buffers: AtomicUsize,
    changed: StateSignal,
}

#[derive(Debug)]
struct Block {
    bytes: Vec<u8>,
    capacity: usize,
    usage: Arc<Usage>,
}

/// Fixed-capacity mutable payload. Freezing transfers this allocation into
/// shared bytes. Its last reference returns the allocation to the owner cache.
pub struct Buffer {
    block: Returning<Block, Arc<Return>>,
    length: usize,
}

impl fmt::Debug for Buffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Buffer")
            .field("length", &self.length)
            .field("capacity", &self.capacity())
            .finish_non_exhaustive()
    }
}

impl Owner {
    pub(crate) fn begin_working_set(&self) -> Capacity {
        let mut local = self.0.borrow_mut();
        local.collect();
        local.pressure = None;
        local.live_capacity()
    }

    pub(crate) fn failed_working_set(&self, baseline: Capacity) -> io::Result<Capacity> {
        let local = self.0.borrow();
        let (live, requested) = local.pressure.ok_or(io::ErrorKind::WouldBlock)?;
        let capacity = Capacity {
            bytes: requested
                .bytes
                .checked_add(live.bytes.saturating_sub(baseline.bytes))
                .ok_or(io::ErrorKind::InvalidInput)?,
            buffers: requested
                .buffers
                .checked_add(live.buffers.saturating_sub(baseline.buffers))
                .ok_or(io::ErrorKind::InvalidInput)?,
        };
        if capacity.bytes > local.limits.bytes || capacity.buffers > local.limits.buffers {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        Ok(capacity)
    }

    pub(super) fn new(reservation: Reservation, limits: Limits) -> Self {
        let (sender, returns) = mpsc::channel(limits.buffers);
        Self(Rc::new(RefCell::new(Local {
            returns,
            shared: Arc::new(Return {
                sender,
                usage: Arc::new(Usage {
                    _reservation: reservation,
                    bytes: AtomicUsize::new(0),
                    buffers: AtomicUsize::new(0),
                    changed: StateSignal::default(),
                }),
            }),
            cache: Vec::with_capacity(limits.buffers),
            cache_bytes: 0,
            limits,
            pressure: None,
        })))
    }

    /// Reuse or allocate a zeroed payload on this owner. Exhaustion never spills
    /// into an uncharged allocation. At most `limits.buffers` returns are drained.
    pub fn try_lease(&self, length: usize) -> io::Result<Buffer> {
        self.0.borrow_mut().try_lease(length, usize::MAX)
    }

    /// Reserve external scratch against the same aggregate owner budget. This
    /// creates no payload allocation or return-queue entry and never waits.
    pub(crate) fn try_charge(&self, bytes: usize) -> io::Result<Charge> {
        let mut local = self.0.borrow_mut();
        if bytes == 0 || bytes > local.limits.bytes {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        local.collect();
        while local.shared.usage.bytes.load(Ordering::Acquire) > local.limits.bytes - bytes {
            let Some(block) = local.cache.pop() else {
                local.record_pressure(Capacity { bytes, buffers: 0 });
                return Err(io::ErrorKind::WouldBlock.into());
            };
            local.cache_bytes -= block.capacity;
            drop(block);
        }
        local.shared.usage.bytes.fetch_add(bytes, Ordering::AcqRel);
        Ok(Charge {
            owner: self.clone(),
            bytes,
        })
    }

    pub(crate) fn has_working_capacity(&self, capacity: Capacity) -> bool {
        let mut local = self.0.borrow_mut();
        if capacity.bytes > local.limits.bytes || capacity.buffers > local.limits.buffers {
            return false;
        }
        local.collect();
        while local.shared.usage.bytes.load(Ordering::Acquire) > local.limits.bytes - capacity.bytes
            || local.shared.usage.buffers.load(Ordering::Acquire)
                > local.limits.buffers - capacity.buffers
        {
            let Some(block) = local.cache.pop() else {
                return false;
            };
            local.cache_bytes -= block.capacity;
            drop(block);
        }
        true
    }

    pub(crate) fn allocator(&self) -> Allocator {
        Allocator {
            local: Rc::downgrade(&self.0),
            thread: thread::current().id(),
            maximum: usize::MAX,
        }
    }

    /// Wait for returned capacity. Canceling before a lease completes retains
    /// no buffer. Existing shared payloads keep their charges independently.
    pub async fn lease(&self, length: usize) -> io::Result<Buffer> {
        let shared = self.0.borrow().shared.clone();
        loop {
            let seen = shared.usage.changed.generation();
            match self.try_lease(length) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    shared.usage.changed.changed_after(seen).await;
                }
                result => return result,
            }
        }
    }

    /// Retained payload bytes, including caches, queued returns, shared buffers,
    /// and foreign transport allocations bound to this owner.
    pub fn allocated_bytes(&self) -> usize {
        self.0.borrow().shared.usage.bytes.load(Ordering::Acquire)
    }

    /// Capture before checking allocation or intake capacity.
    pub fn generation(&self) -> u64 {
        self.0.borrow().shared.usage.changed.generation()
    }

    /// Cancel-safe observation of returned allocation or foreign backing.
    pub fn changed_after(&self, generation: u64) -> impl Future<Output = ()> + use<> {
        let usage = self.0.borrow().shared.usage.clone();
        async move { usage.changed.changed_after(generation).await }
    }

    /// Drop cached and already-returned allocations on the owner thread. Live
    /// transport/storage references remain valid and keep their memory charged.
    pub fn trim_cache(&self) {
        let mut local = self.0.borrow_mut();
        local.collect();
        local.cache.clear();
        local.cache_bytes = 0;
    }
}

impl Local {
    fn live_capacity(&self) -> Capacity {
        Capacity {
            bytes: self.shared.usage.bytes.load(Ordering::Acquire) - self.cache_bytes,
            buffers: self.shared.usage.buffers.load(Ordering::Acquire) - self.cache.len(),
        }
    }

    fn record_pressure(&mut self, requested: Capacity) {
        self.pressure = Some((self.live_capacity(), requested));
    }

    fn try_lease(&mut self, length: usize, maximum: usize) -> io::Result<Buffer> {
        let mut block = self.try_block(length, maximum)?;
        block.bytes.resize(block.capacity, 0);
        block.bytes[..length].fill(0);
        Ok(Buffer {
            block: Returning::new(block, self.shared.clone()),
            length,
        })
    }

    /// Smallest fitting cached block or a new one, with unspecified contents.
    fn try_block(&mut self, length: usize, maximum: usize) -> io::Result<Block> {
        let local = self;
        if length == 0 || length > local.limits.bytes || length > maximum {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        local.collect();
        let cached = local
            .cache
            .iter()
            .enumerate()
            .filter(|(_, block)| (length..=maximum).contains(&block.capacity))
            .min_by_key(|(_, block)| block.capacity)
            .map(|(index, _)| index);
        if let Some(index) = cached {
            let block = local.cache.swap_remove(index);
            local.cache_bytes -= block.capacity;
            Ok(block)
        } else {
            // Cached smaller blocks may occupy the needed byte or count budget.
            // Eviction happens here on their allocation owner, never on a sender.
            while !local.can_allocate(length) {
                let Some(block) = local.cache.pop() else {
                    local.record_pressure(Capacity {
                        bytes: length,
                        buffers: 1,
                    });
                    return Err(io::ErrorKind::WouldBlock.into());
                };
                local.cache_bytes -= block.capacity;
                drop(block);
            }
            Block::new(local.shared.usage.clone(), length)
        }
    }

    fn collect(&mut self) {
        for _ in 0..self.limits.buffers {
            let Ok(block) = self.returns.try_recv() else {
                break;
            };
            if block.capacity <= self.limits.cache_bytes - self.cache_bytes {
                self.cache_bytes += block.capacity;
                self.cache.push(block);
            }
        }
    }
    fn can_allocate(&self, bytes: usize) -> bool {
        bytes <= self.limits.bytes
            && self.shared.usage.bytes.load(Ordering::Acquire) <= self.limits.bytes - bytes
            && self.shared.usage.buffers.load(Ordering::Acquire) < self.limits.buffers
    }
}

impl Allocator {
    pub(crate) fn with_limit(mut self, maximum: usize) -> Self {
        self.maximum = self.maximum.min(maximum);
        self
    }

    pub(crate) fn try_arena(&self, capacity: usize) -> io::Result<Arena> {
        if thread::current().id() != self.thread {
            return Err(io::Error::other("journal allocation left its owner thread"));
        }
        let local = self.local.upgrade().ok_or(io::ErrorKind::BrokenPipe)?;
        let mut local = local.borrow_mut();
        // An arena is written by appending, so skip zeroing the leased bytes.
        let mut block = local.try_block(capacity, self.maximum)?;
        block.bytes.clear();
        Ok(Arena(Buffer {
            block: Returning::new(block, local.shared.clone()),
            length: capacity,
        }))
    }

    /// Wait on returned capacity without retaining the owner's mutable state.
    pub(crate) async fn arena(&self, capacity: usize) -> io::Result<Arena> {
        let shared = self
            .local
            .upgrade()
            .ok_or(io::ErrorKind::BrokenPipe)?
            .borrow()
            .shared
            .clone();
        loop {
            let seen = shared.usage.changed.generation();
            match self.try_arena(capacity) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    shared.usage.changed.changed_after(seen).await;
                }
                result => return result,
            }
        }
    }
}

/// A fixed allocation exposed to the existing Vec-based canonical encoders.
/// Callers must reserve their complete encoded length before borrowing the Vec.
#[derive(Debug)]
pub(crate) struct Arena(Buffer);

impl Arena {
    pub(crate) fn bytes_mut(&mut self) -> &mut Vec<u8> {
        &mut self.0.block.bytes
    }

    pub(crate) fn capacity(&self) -> usize {
        self.0.capacity()
    }

    pub(crate) fn share(arena: &Arc<Self>) -> Bytes {
        // The block releases its charged capacity regardless; growth is a bug.
        debug_assert_eq!(
            arena.0.block.bytes.capacity(),
            arena.capacity(),
            "canonical encoder grew an uncharged allocation"
        );
        Bytes::from_owner(SharedArena(arena.clone()))
    }
}

struct SharedArena(Arc<Arena>);

impl AsRef<[u8]> for SharedArena {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref().as_ref()
    }
}

impl AsRef<[u8]> for Arena {
    fn as_ref(&self) -> &[u8] {
        &self.0.block.bytes
    }
}

impl Drop for Arena {
    fn drop(&mut self) {
        debug_assert_eq!(
            self.0.block.bytes.capacity(),
            self.capacity(),
            "canonical encoder grew an uncharged allocation"
        );
    }
}

impl Block {
    fn new(usage: Arc<Usage>, capacity: usize) -> io::Result<Self> {
        usage.bytes.fetch_add(capacity, Ordering::AcqRel);
        usage.buffers.fetch_add(1, Ordering::AcqRel);
        let mut block = Self {
            bytes: Vec::new(),
            capacity,
            usage,
        };
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(capacity)
            .map_err(io::Error::other)?;
        bytes.resize(capacity, 0);
        // Normalize reported capacity before handing the Vec to fixed encoders.
        block.bytes = bytes.into_boxed_slice().into_vec();
        Ok(block)
    }
}

impl Drop for Block {
    fn drop(&mut self) {
        // Free before publishing budget release. The separately bounded queue
        // retains only metadata after this point.
        drop(std::mem::take(&mut self.bytes));
        self.usage.bytes.fetch_sub(self.capacity, Ordering::AcqRel);
        self.usage.buffers.fetch_sub(1, Ordering::AcqRel);
        self.usage.changed.notify_changed();
    }
}

impl Buffer {
    /// Allocation size charged even when only a shorter slice is published.
    pub fn capacity(&self) -> usize {
        self.block.capacity
    }

    /// Change visible size without growing the allocation. New bytes are zeroed.
    pub fn resize(&mut self, length: usize) -> io::Result<()> {
        if length > self.capacity() {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        if length > self.length {
            self.block.bytes[self.length..length].fill(0);
        }
        self.length = length;
        Ok(())
    }

    /// Immutable aliases retain the full allocation until their final drop.
    pub fn freeze(self) -> Bytes {
        Bytes::from_owner(self)
    }
}

impl Deref for Buffer {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.block.bytes[..self.length]
    }
}
impl DerefMut for Buffer {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.block.bytes[..self.length]
    }
}
impl AsRef<[u8]> for Buffer {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl GiveBack<Block> for Arc<Return> {
    fn give_back(&mut self, block: Block) {
        // Owner shutdown may have disconnected the receiver. In that case the
        // final observer reclaims bytes, with no unbounded detached return list.
        drop(self.sender.try_send(block));
        self.usage.changed.notify_changed();
    }
}
