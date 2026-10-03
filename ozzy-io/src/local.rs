//! One submission lane shared by partition tasks on the same application shard.

use crate::{Admission, Backend, Class, Completed, Completion, Operation, Rejected};
use std::{cell::RefCell, io, rc::Rc};

/// Clones share one lane and its budgets. Intentionally not Send or Sync: move
/// the backend onto its shard before constructing this value. No mutable lane
/// borrow survives a submission, capacity wait or physical-completion wait.
#[derive(Clone, Debug)]
pub struct Local {
    backend: Rc<RefCell<Box<dyn Backend>>>,
    admission: Admission,
    shard: usize,
}

impl Local {
    pub fn new(backend: impl Backend + 'static) -> Self {
        Self {
            admission: backend.admission().clone(),
            shard: backend.shard(),
            backend: Rc::new(RefCell::new(Box::new(backend))),
        }
    }

    pub fn submit(&self, class: Class, operation: Operation) -> Result<Completion, Rejected> {
        self.backend.borrow_mut().submit(class, operation)
    }

    pub fn admission(&self) -> &Admission {
        &self.admission
    }
    pub const fn shard(&self) -> usize {
        self.shard
    }

    /// Wait for admission, then physical completion. The caller must account
    /// for pending bytes in its own bounded state until admission succeeds.
    /// Canceling after submission does not cancel physical execution.
    pub async fn execute(&self, class: Class, mut operation: Operation) -> io::Result<Completed> {
        let bytes = operation.retained_bytes()?;
        loop {
            self.admission.ready(self.shard, class, bytes).await?;
            match self.submit(class, operation) {
                Ok(completion) => return completion.await,
                Err(rejected) if rejected.error.kind() == io::ErrorKind::WouldBlock => {
                    operation = *rejected.operation;
                    // Backend queue availability can lag its admission counter.
                    // Yield once rather than spinning on an immediately-ready
                    // capacity future and monopolizing this shard.
                    let mut yielded = false;
                    std::future::poll_fn(|cx| {
                        if yielded {
                            std::task::Poll::Ready(())
                        } else {
                            yielded = true;
                            cx.waker().wake_by_ref();
                            std::task::Poll::Pending
                        }
                    })
                    .await;
                }
                Err(rejected) => return Err(rejected.error),
            }
        }
    }
}
