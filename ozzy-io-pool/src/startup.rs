use std::{io, sync::Arc, sync::mpsc};

/// Execution role, independent of partition count.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Worker {
    /// Ordinary execution worker with its fixed pool index.
    Data(usize),
    /// Worker for reserved barriers, recovery, and release work.
    Progress,
    /// Worker owning the direct-write driver and its kernel state.
    Direct,
}

/// Runs on the new worker before its file execution or kernel setup. Shared
/// submission queues are startup allocations, not worker-local memory pools.
pub type Initializer = Arc<dyn Fn(Worker) -> io::Result<()> + Send + Sync>;

pub(crate) fn initialize(
    initialize: &Initializer,
    role: Worker,
    started: &mpsc::SyncSender<io::Result<()>>,
) -> bool {
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| initialize(role)))
        .unwrap_or_else(|_| Err(io::Error::other("I/O worker initializer panicked")));
    let success = result.is_ok();
    started.send(result).is_ok() && success
}

pub(crate) fn ready(receiver: &mpsc::Receiver<io::Result<()>>) -> io::Result<()> {
    receiver
        .recv()
        .map_err(|_| io::Error::other("I/O worker exited during initialization"))?
}
