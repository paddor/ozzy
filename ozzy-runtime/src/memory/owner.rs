use super::{Limits, Reservation};
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
    block: Option<Block>,
    length: usize,
    owner: Arc<Return>,
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
        })))
    }

    /// Reuse or allocate a zeroed payload on this owner. Exhaustion never spills
    /// into an uncharged allocation. At most `limits.buffers` returns are drained.
    pub fn try_lease(&self, length: usize) -> io::Result<Buffer> {
        self.0.borrow_mut().try_lease(length, usize::MAX)
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
    fn try_lease(&mut self, length: usize, maximum: usize) -> io::Result<Buffer> {
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
        let mut block = if let Some(index) = cached {
            let block = local.cache.swap_remove(index);
            local.cache_bytes -= block.capacity;
            block
        } else {
            // Cached smaller blocks may occupy the needed byte or count budget.
            // Eviction happens here on their allocation owner, never on a sender.
            while !local.can_allocate(length) {
                let Some(block) = local.cache.pop() else {
                    return Err(io::ErrorKind::WouldBlock.into());
                };
                local.cache_bytes -= block.capacity;
                drop(block);
            }
            Block::new(local.shared.usage.clone(), length)?
        };
        block.bytes.resize(block.capacity, 0);
        block.bytes[..length].fill(0);
        Ok(Buffer {
            block: Some(block),
            length,
            owner: local.shared.clone(),
        })
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
        let mut buffer = local.borrow_mut().try_lease(capacity, self.maximum)?;
        buffer.block.as_mut().expect("live buffer").bytes.clear();
        Ok(Arena(buffer))
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
        &mut self.0.block.as_mut().expect("live arena").bytes
    }

    pub(crate) fn capacity(&self) -> usize {
        self.0.capacity()
    }

    pub(crate) fn share(arena: &Arc<Self>) -> Bytes {
        assert_eq!(
            arena.0.block.as_ref().expect("live arena").bytes.capacity(),
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
        &self.0.block.as_ref().expect("live arena").bytes
    }
}

impl Drop for Arena {
    fn drop(&mut self) {
        assert_eq!(
            self.0.block.as_ref().expect("live arena").bytes.capacity(),
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
        self.block.as_ref().expect("live buffer").capacity
    }

    /// Change visible size without growing the allocation. New bytes are zeroed.
    pub fn resize(&mut self, length: usize) -> io::Result<()> {
        if length > self.capacity() {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        if length > self.length {
            self.block.as_mut().expect("live buffer").bytes[self.length..length].fill(0);
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
        &self.block.as_ref().expect("live buffer").bytes[..self.length]
    }
}
impl DerefMut for Buffer {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.block.as_mut().expect("live buffer").bytes[..self.length]
    }
}
impl AsRef<[u8]> for Buffer {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        let block = self.block.take().expect("one buffer release");
        let result = self.owner.sender.try_send(block);
        // Owner shutdown may have disconnected the receiver. In that case the
        // final observer reclaims bytes, with no unbounded detached return list.
        drop(result);
        self.owner.usage.changed.notify_changed();
    }
}
