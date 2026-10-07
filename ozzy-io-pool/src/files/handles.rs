use std::{
    fs::File,
    io,
    ops::Deref,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

#[derive(Debug)]
pub(super) struct Budget {
    limit: usize,
    used: Vec<AtomicUsize>,
}

impl Budget {
    pub(super) fn new(limit: usize, shards: usize) -> Arc<Self> {
        Arc::new(Self {
            limit,
            used: (0..shards).map(|_| AtomicUsize::new(0)).collect(),
        })
    }

    #[allow(
        deprecated,
        reason = "Atomic::try_update requires Rust 1.95; MSRV is 1.93"
    )]
    pub(super) fn reserve(self: &Arc<Self>, shard: usize) -> io::Result<Reservation> {
        let limit =
            self.limit / self.used.len() + usize::from(shard < self.limit % self.used.len());
        self.used[shard]
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                (used < limit).then_some(used + 1)
            })
            .map_err(|_| io::ErrorKind::WouldBlock)?;
        Ok(Reservation {
            budget: self.clone(),
            shard,
        })
    }
}

#[derive(Debug)]
pub(super) struct Reservation {
    budget: Arc<Budget>,
    shard: usize,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        let previous = self.budget.used[self.shard].fetch_sub(1, Ordering::AcqRel);
        assert!(previous > 0, "handle budget underflow");
    }
}

/// Field order matters: physical close precedes capacity release. Clones held
/// by running workers keep both alive even after explicit handle invalidation.
#[derive(Debug)]
pub struct OwnedFile {
    file: File,
    _reservation: Reservation,
}

impl OwnedFile {
    pub(super) fn new(file: File, reservation: Reservation) -> Self {
        Self {
            file,
            _reservation: reservation,
        }
    }
}

impl Deref for OwnedFile {
    type Target = File;
    fn deref(&self) -> &File {
        &self.file
    }
}

#[cfg(unix)]
impl std::os::fd::AsFd for OwnedFile {
    fn as_fd(&self) -> std::os::fd::BorrowedFd<'_> {
        std::os::fd::AsFd::as_fd(&self.file)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execution_reference_keeps_descriptor_capacity_after_registry_removal() {
        let budget = Budget::new(1, 1);
        let reservation = budget.reserve(0).unwrap();
        let file = Arc::new(OwnedFile::new(tempfile::tempfile().unwrap(), reservation));
        let executing = file.clone();
        drop(file);
        assert_eq!(
            budget.reserve(0).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        drop(executing);
        assert!(budget.reserve(0).is_ok());
    }

    #[test]
    fn concurrent_handles_cannot_overbook_one_shard() {
        let budget = Budget::new(2, 1);
        let ready = Arc::new(std::sync::Barrier::new(3));
        let release = Arc::new(std::sync::Barrier::new(3));
        let workers = (0..2)
            .map(|_| {
                let budget = budget.clone();
                let ready = ready.clone();
                let release = release.clone();
                std::thread::spawn(move || {
                    let reservation = budget.reserve(0).unwrap();
                    ready.wait();
                    release.wait();
                    drop(reservation);
                })
            })
            .collect::<Vec<_>>();
        ready.wait();
        assert_eq!(
            budget.reserve(0).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        release.wait();
        for worker in workers {
            worker.join().unwrap();
        }
        assert!(budget.reserve(0).is_ok());
    }
}
