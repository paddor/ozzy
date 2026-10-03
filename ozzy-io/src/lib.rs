//! Backend-neutral file operations. No file descriptors or filesystem calls live
//! here. Admission is nonblocking; an admitted operation survives observer drop.
//!
//! Operations may finish out of order. Await dependent operations explicitly,
//! especially writes before barriers, publication, or close. Only the caller
//! decides which completed bytes belong to its journal's durable prefix.
#![forbid(unsafe_code)]

mod admission;
mod completion;
mod handle;
mod local;
mod operation;
#[cfg(feature = "simulation")]
pub mod simulation;
#[cfg(test)]
mod tests;

pub use admission::{Admission, Charge, Class, Lane, Limits, Quota};
pub use completion::{Completed, Completion, Reply, completion};
pub use handle::{Handle, HandleOwner, HandleToken};
pub use local::Local;
pub use operation::{
    Entry, FileKind, Metadata, OpenMode, Operation, Outcome, ReadBuffer, SyncMode, WriteBuffer,
};

use std::{fmt, io};

/// One shard's submission lane. Cloning partition actors must not create a new
/// device budget or another physical execution pool.
pub trait Backend: fmt::Debug + Send {
    /// On rejection, return the unmodified operation. No physical work started.
    fn submit(&mut self, class: Class, operation: Operation) -> Result<Completion, Rejected>;

    fn admission(&self) -> &Admission;
    fn shard(&self) -> usize;
}

#[derive(Debug)]
pub struct Rejected {
    pub error: io::Error,
    pub operation: Box<Operation>,
}

impl Rejected {
    pub fn new(error: io::Error, operation: Operation) -> Self {
        Self {
            error,
            operation: Box::new(operation),
        }
    }
}

impl fmt::Display for Rejected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(f)
    }
}

impl std::error::Error for Rejected {}
