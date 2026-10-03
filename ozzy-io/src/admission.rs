use std::{
    cell::RefCell,
    collections::HashMap,
    io,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
};

use fanring::{mpsc, teardown::Coordinated};
use tokio::sync::Notify;

/// Separate capacity for barriers/recovery. Ordinary work cannot consume it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Class {
    /// Ordinary file work, including segment writes and reads.
    Data,
    /// Reserved barriers, recovery, and other work needed to release capacity.
    Progress,
}

impl Class {
    const fn index(self) -> usize {
        match self {
            Self::Data => 0,
            Self::Progress => 1,
        }
    }
}

/// Operation count and retained-byte count for one traffic class.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Quota {
    /// Operation count, including retained completions.
    pub operations: usize,
    /// Retained backing bytes, including results and backend scratch.
    pub bytes: usize,
}

/// Aggregate device budget. Each shard gets a fixed, fair share; creating more
/// partitions or submission lanes does not multiply it. Idle shares are not lent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// Application shards sharing this fixed device budget.
    pub shards: usize,
    /// Aggregate allowance for ordinary work.
    pub data: Quota,
    /// Aggregate allowance reserved for progress work.
    pub progress: Quota,
}

impl Limits {
    /// Check nonzero per-shard shares and aggregate overflow.
    pub fn validate(self) -> io::Result<Self> {
        if self.shards == 0
            || [self.data, self.progress]
                .iter()
                .any(|q| q.operations < self.shards || q.bytes < self.shards)
            || self
                .data
                .operations
                .checked_add(self.progress.operations)
                .is_none()
            || self.data.bytes.checked_add(self.progress.bytes).is_none()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "I/O budgets need a nonzero share for every shard",
            ));
        }
        Ok(self)
    }

    /// Fixed count/byte share for a configured shard; panics for a foreign shard.
    pub fn share(self, shard: usize, class: Class) -> Quota {
        assert!(shard < self.shards);
        let total = match class {
            Class::Data => self.data,
            Class::Progress => self.progress,
        };
        let split = |n| n / self.shards + usize::from(shard < n % self.shards);
        Quota {
            operations: split(total.operations),
            bytes: split(total.bytes),
        }
    }
}

/// Shared capacity observations; mutable ledgers remain on their shard owners.
#[derive(Clone, Debug)]
pub struct Admission(Arc<Shared>);

#[derive(Debug)]
struct Shared {
    limits: Limits,
    closed: AtomicBool,
    claimed: Vec<AtomicBool>,
    observed: Vec<[Observed; 2]>,
    changed: Notify,
}

#[derive(Debug, Default)]
struct Observed {
    operations: AtomicUsize,
    bytes: AtomicUsize,
}

/// One shard owns this ledger. Completion drops send bounded returns to it.
#[derive(Debug)]
pub struct Lane {
    admission: Admission,
    shard: usize,
    used: [Quota; 2],
    returned: mpsc::Receiver<Return, Coordinated>,
    sender: Arc<ReturnSender>,
}

#[derive(Clone, Copy, Debug)]
struct Return {
    class: Class,
    bytes: usize,
}

type Sender = mpsc::Sender<Return, Coordinated>;

#[derive(Debug)]
struct ReturnSender {
    id: u64,
    registration: Mutex<Sender>,
}

static NEXT_RETURN_ID: AtomicU64 = AtomicU64::new(1);

thread_local! {
    static RETURN_SENDERS: RefCell<HashMap<u64, (Weak<ReturnSender>, Sender)>> =
        RefCell::new(HashMap::new());
}

impl ReturnSender {
    fn send(self: &Arc<Self>, value: Return) {
        RETURN_SENDERS.with(|local| {
            let mut local = local.borrow_mut();
            if !local.contains_key(&self.id) {
                local.retain(|_, (owner, _)| owner.strong_count() != 0);
                let Some(sender) = self
                    .registration
                    .lock()
                    .expect("return lane registration poisoned")
                    .try_clone()
                else {
                    return;
                };
                local.insert(self.id, (Arc::downgrade(self), sender));
            }
            let sender = &mut local.get_mut(&self.id).expect("registered return lane").1;
            match sender.try_send(value) {
                Ok(()) | Err(mpsc::TrySendError::Disconnected(_)) => {}
                Err(mpsc::TrySendError::Full(_)) => {
                    panic!("bounded disk admission return lane overflowed")
                }
            }
        });
    }
}

/// Held through physical completion and retention of an unobserved result.
#[derive(Debug)]
pub struct Charge {
    admission: Admission,
    shard: usize,
    class: Class,
    bytes: usize,
    sender: Arc<ReturnSender>,
}

impl Admission {
    /// Create admission for validated fixed device limits.
    pub fn new(limits: Limits) -> io::Result<Self> {
        let limits = limits.validate()?;
        Ok(Self(Arc::new(Shared {
            limits,
            closed: AtomicBool::new(false),
            claimed: (0..limits.shards).map(|_| AtomicBool::new(false)).collect(),
            observed: (0..limits.shards)
                .map(|_| [Observed::default(), Observed::default()])
                .collect(),
            changed: Notify::new(),
        })))
    }

    /// Configured aggregate device bounds.
    pub fn limits(&self) -> Limits {
        self.0.limits
    }

    /// Claim the sole submission lane for one application shard.
    pub fn lane(&self, shard: usize) -> io::Result<Lane> {
        let claimed = self
            .0
            .claimed
            .get(shard)
            .ok_or(io::ErrorKind::InvalidInput)?;
        let capacity = self
            .0
            .limits
            .share(shard, Class::Data)
            .operations
            .checked_add(self.0.limits.share(shard, Class::Progress).operations)
            .ok_or(io::ErrorKind::InvalidInput)?;
        let (sender, returned) = mpsc::try_channel_with_policy(capacity)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))?;
        claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| io::ErrorKind::AlreadyExists)?;
        Ok(Lane {
            admission: self.clone(),
            shard,
            used: [Quota::default(); 2],
            returned,
            sender: Arc::new(ReturnSender {
                id: NEXT_RETURN_ID.fetch_add(1, Ordering::Relaxed),
                registration: Mutex::new(sender),
            }),
        })
    }

    fn check(&self, shard: usize, class: Class, bytes: usize) -> io::Result<()> {
        if self.0.closed.load(Ordering::Acquire) {
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        if shard >= self.0.limits.shards {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let limit = self.0.limits.share(shard, class);
        if bytes > limit.bytes {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let used = self.used(shard, class);
        if used.operations >= limit.operations || bytes > limit.bytes.saturating_sub(used.bytes) {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        Ok(())
    }

    /// Wait for capacity without retaining a request or reserving a permit.
    /// The caller retries admission. Its own bounded state owns pending bytes.
    pub async fn ready(&self, shard: usize, class: Class, bytes: usize) -> io::Result<()> {
        loop {
            let notified = self.0.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let check = self.check(shard, class, bytes);
            match check {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => notified.await,
                result => return result,
            }
        }
    }

    /// Observed retained count/bytes for one shard and traffic class.
    pub fn used(&self, shard: usize, class: Class) -> Quota {
        let observed = &self.0.observed[shard][class.index()];
        Quota {
            operations: observed.operations.load(Ordering::Acquire),
            bytes: observed.bytes.load(Ordering::Acquire),
        }
    }

    /// Reject later reservations and wake all capacity waiters.
    pub fn close(&self) {
        self.0.closed.store(true, Ordering::Release);
        self.0.changed.notify_waiters();
    }
}

impl Lane {
    fn collect(&mut self) {
        while let Ok(returned) = self.returned.try_recv() {
            let used = &mut self.used[returned.class.index()];
            used.operations = used
                .operations
                .checked_sub(1)
                .expect("disk operation charge");
            used.bytes = used
                .bytes
                .checked_sub(returned.bytes)
                .expect("disk byte charge");
        }
    }

    /// Reserve one operation and retained bytes; return `WouldBlock` on pressure.
    pub fn try_charge(&mut self, class: Class, bytes: usize) -> io::Result<Charge> {
        self.collect();
        let shard = self.shard;
        let admission = &self.admission;
        if admission.0.closed.load(Ordering::Acquire) {
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        let limit = admission.0.limits.share(shard, class);
        if bytes > limit.bytes {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let used = &mut self.used[class.index()];
        if used.operations == limit.operations || bytes > limit.bytes - used.bytes {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        used.operations += 1;
        used.bytes += bytes;
        let observed = &admission.0.observed[shard][class.index()];
        observed.operations.fetch_add(1, Ordering::AcqRel);
        observed.bytes.fetch_add(bytes, Ordering::AcqRel);
        Ok(Charge {
            admission: admission.clone(),
            shard,
            class,
            bytes,
            sender: self.sender.clone(),
        })
    }
}

impl Drop for Charge {
    fn drop(&mut self) {
        self.sender.send(Return {
            class: self.class,
            bytes: self.bytes,
        });
        let observed = &self.admission.0.observed[self.shard][self.class.index()];
        let previous = observed.operations.fetch_sub(1, Ordering::AcqRel);
        assert!(previous > 0, "disk operation charge underflow");
        let previous = observed.bytes.fetch_sub(self.bytes, Ordering::AcqRel);
        assert!(previous >= self.bytes, "disk byte charge underflow");
        self.admission.0.changed.notify_waiters();
    }
}
