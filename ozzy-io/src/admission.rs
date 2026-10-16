use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use crate::signal::StateSignal;

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

    /// Fixed count/byte share for a configured shard. A foreign shard is
    /// `InvalidInput`.
    pub fn share(self, shard: usize, class: Class) -> io::Result<Quota> {
        if shard >= self.shards {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let total = match class {
            Class::Data => self.data,
            Class::Progress => self.progress,
        };
        let split = |n| n / self.shards + usize::from(shard < n % self.shards);
        Ok(Quota {
            operations: split(total.operations),
            bytes: split(total.bytes),
        })
    }
}

/// Shared per-shard capacity counters. Only a shard's lane adds to them; charge
/// drops on any thread subtract.
#[derive(Clone, Debug)]
pub struct Admission(Arc<Shared>);

#[derive(Debug)]
struct Shared {
    limits: Limits,
    closed: AtomicBool,
    claimed: Vec<AtomicBool>,
    observed: Vec<[Observed; 2]>,
    changed: StateSignal,
}

#[derive(Debug, Default)]
struct Observed {
    operations: AtomicUsize,
    bytes: AtomicUsize,
}

/// The sole submission side of one shard's counters. A reading that races a
/// charge drop can only overstate use, so check-then-add never overshoots.
#[derive(Debug)]
pub struct Lane {
    admission: Admission,
    shard: usize,
}

/// Held through physical completion and retention of an unobserved result.
#[derive(Debug)]
pub struct Charge {
    admission: Admission,
    shard: usize,
    class: Class,
    bytes: usize,
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
            changed: StateSignal::default(),
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
        claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| io::ErrorKind::AlreadyExists)?;
        Ok(Lane {
            admission: self.clone(),
            shard,
        })
    }

    fn check(&self, shard: usize, class: Class, bytes: usize) -> io::Result<()> {
        if self.0.closed.load(Ordering::Acquire) {
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        let limit = self.0.limits.share(shard, class)?;
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
        self.0
            .changed
            .wait_for(|| match self.check(shard, class, bytes) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => None,
                result => Some(result),
            })
            .await
    }

    /// Observed retained count/bytes for one shard and traffic class. A
    /// foreign shard retains nothing.
    pub fn used(&self, shard: usize, class: Class) -> Quota {
        let Some(observed) = self.0.observed.get(shard) else {
            return Quota::default();
        };
        let observed = &observed[class.index()];
        Quota {
            operations: observed.operations.load(Ordering::Acquire),
            bytes: observed.bytes.load(Ordering::Acquire),
        }
    }

    /// Reject later reservations and wake all capacity waiters.
    pub fn close(&self) {
        self.0.closed.store(true, Ordering::Release);
        self.0.changed.notify_changed();
    }
}

impl Lane {
    /// Reserve one operation and retained bytes; return `WouldBlock` on pressure.
    pub fn try_charge(&mut self, class: Class, bytes: usize) -> io::Result<Charge> {
        let shard = self.shard;
        let admission = &self.admission;
        admission.check(shard, class, bytes)?;
        let observed = &admission.0.observed[shard][class.index()];
        observed.operations.fetch_add(1, Ordering::AcqRel);
        observed.bytes.fetch_add(bytes, Ordering::AcqRel);
        Ok(Charge {
            admission: admission.clone(),
            shard,
            class,
            bytes,
        })
    }
}

impl Drop for Charge {
    fn drop(&mut self) {
        let observed = &self.admission.0.observed[self.shard][self.class.index()];
        let previous = observed.operations.fetch_sub(1, Ordering::AcqRel);
        assert!(previous > 0, "disk operation charge underflow");
        let previous = observed.bytes.fetch_sub(self.bytes, Ordering::AcqRel);
        assert!(previous >= self.bytes, "disk byte charge underflow");
        self.admission.0.changed.notify_changed();
    }
}
