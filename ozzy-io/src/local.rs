//! One submission lane shared by partition tasks on the same application shard.

use crate::{Admission, Backend, Class, Completed, Completion, Operation, Rejected};
use std::{
    cell::{Cell, RefCell},
    io,
    rc::Rc,
};

/// Clones share one lane and its budgets. Intentionally not Send or Sync: move
/// the backend onto its shard before constructing this value. No mutable lane
/// borrow survives a submission, capacity wait or physical-completion wait.
#[derive(Clone, Debug)]
pub struct Local {
    backend: Rc<RefCell<Box<dyn Backend>>>,
    admission: Admission,
    shard: usize,
    completed: Rc<Cell<u64>>,
}

impl Local {
    /// Install one backend submission lane on its owning application shard.
    pub fn new(backend: impl Backend + 'static) -> Self {
        Self {
            admission: backend.admission().clone(),
            shard: backend.shard(),
            backend: Rc::new(RefCell::new(Box::new(backend))),
            completed: Rc::default(),
        }
    }

    /// Try admission without waiting; rejection returns the original operation.
    pub fn submit(&self, class: Class, operation: Operation) -> Result<Completion, Rejected> {
        self.backend.borrow_mut().submit(class, operation)
    }

    /// Capacity observations shared by this lane and its backend.
    pub fn admission(&self) -> &Admission {
        &self.admission
    }
    /// Configured shard whose fixed share backs this lane.
    pub const fn shard(&self) -> usize {
        self.shard
    }

    /// Jobs this lane awaited to physical completion, over all clones. A
    /// count that stays put while work is pending means the device completes
    /// nothing.
    pub fn completed(&self) -> u64 {
        self.completed.get()
    }

    /// Wait for admission, then physical completion. The caller must account
    /// for pending bytes in its own bounded state until admission succeeds.
    /// Canceling after submission does not cancel physical execution.
    pub async fn execute(&self, class: Class, mut operation: Operation) -> io::Result<Completed> {
        let bytes = operation.retained_bytes()?;
        loop {
            self.admission.ready(self.shard, class, bytes).await?;
            match self.submit(class, operation) {
                Ok(completion) => {
                    let result = completion.await;
                    self.completed.set(self.completed.get().wrapping_add(1));
                    return result;
                }
                Err(rejected) if rejected.error.kind() == io::ErrorKind::WouldBlock => {
                    operation = *rejected.operation;
                    // Backend queue availability can lag its admission counter.
                    // Yield once rather than spinning on an immediately-ready
                    // capacity future and monopolizing this shard.
                    crate::signal::yield_once().await;
                }
                Err(rejected) => return Err(rejected.error),
            }
        }
    }
}
