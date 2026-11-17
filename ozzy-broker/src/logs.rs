//! Logs an embedder creates and deletes while the broker runs. The embedder
//! orders creation and deletion across brokers; each broker keeps only its
//! own durable record of every log.

pub(crate) mod registry;
mod setup;
pub(crate) mod stops;

pub use ozzy_config::LogState;
pub(crate) use setup::LogSetup;

use ozzy_proto::{GroupId, NodeId, PartitionIncarnation};
use ozzy_runtime::log::Log;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, PoisonError},
};
use tokio::sync::oneshot;

/// A log to create. Every member broker is given the same spec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogSpec {
    /// The log's ID and replication group. Never reused, on any broker.
    pub id: GroupId,
    /// Record namespace of the log. Never reused.
    pub incarnation: PartitionIncarnation,
    /// One broker for a local log, or all three for a replicated one. The
    /// first leads the first view.
    pub members: Vec<NodeId>,
    /// Bytes per segment file, or the class's `segment_bytes`. An idle log
    /// holds one segment on disk and a log at most 256, so small segments
    /// suit many small logs and large ones a busy or large log. Fixed at
    /// creation.
    pub segment_bytes: Option<u64>,
}

/// Why a broker did not create a log.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum CreateError {
    /// The deployment configures no runtime log class.
    #[error("no runtime log class is configured")]
    Disabled,
    /// The spec names no valid member set for this broker.
    #[error("invalid log spec: {0}")]
    Invalid(&'static str),
    /// The ID belongs to a log with another incarnation, member list or
    /// class.
    #[error("the log exists with another spec")]
    Exists,
    /// The ID was deleted here. It is never formatted again.
    #[error("the log was deleted")]
    Deleted,
    /// The broker holds its configured number of logs.
    #[error("the broker holds its configured number of logs")]
    Full,
    /// The log is being created or the shard's queue is full. Try again.
    #[error("the log or its shard is busy")]
    Busy,
    /// Storage refused. The log is not usable; a restart cleans it up.
    #[error("log creation failed: {0}")]
    Failed(String),
    /// The broker stopped.
    #[error("the broker stopped")]
    Stopped,
}

/// Why a broker did not delete a log.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum DeleteError {
    /// This broker never held the log.
    #[error("unknown log")]
    Unknown,
    /// The log is being created or deleted, or the shard's queue is full.
    #[error("the log or its shard is busy")]
    Busy,
    /// Storage refused. The log stays deleted; a restart finishes removal.
    #[error("log deletion failed: {0}")]
    Failed(String),
    /// The broker stopped.
    #[error("the broker stopped")]
    Stopped,
}

/// One request from the broker handle to the shard that owns the log.
#[derive(Debug)]
pub(crate) enum Command {
    Create {
        spec: LogSpec,
        reply: oneshot::Sender<Result<(), CreateError>>,
    },
    Delete {
        id: GroupId,
        reply: oneshot::Sender<Result<(), DeleteError>>,
    },
}

/// Commands one shard may have queued.
pub(crate) const COMMANDS: usize = 64;

#[derive(Debug)]
struct Row {
    state: LogState,
    log: Option<Log>,
}

/// Every runtime log of this broker and its state. Shards write their own
/// logs' rows; the broker handle reads. Holds are a map operation long.
#[derive(Debug, Clone, Default)]
pub(crate) struct LogTable {
    rows: Arc<Mutex<BTreeMap<GroupId, Row>>>,
}

impl LogTable {
    fn with<T>(&self, run: impl FnOnce(&mut BTreeMap<GroupId, Row>) -> T) -> T {
        run(&mut self.rows.lock().unwrap_or_else(PoisonError::into_inner))
    }

    /// Record the state of `id`, and its handle while it serves.
    pub(crate) fn set(&self, id: GroupId, state: LogState, log: Option<Log>) {
        self.with(|rows| rows.insert(id, Row { state, log }));
    }

    /// Forget a log that never served.
    pub(crate) fn forget(&self, id: GroupId) {
        self.with(|rows| rows.remove(&id));
    }

    /// The handle of a ready log.
    pub(crate) fn log(&self, id: GroupId) -> Option<Log> {
        self.with(|rows| rows.get(&id).and_then(|row| row.log.clone()))
    }

    /// Logs that hold or will hold a store: all but the deleted.
    pub(crate) fn live(&self) -> usize {
        self.with(|rows| {
            rows.values()
                .filter(|row| row.state != LogState::Deleted)
                .count()
        })
    }

    pub(crate) fn states(&self) -> Vec<(GroupId, LogState)> {
        self.with(|rows| rows.iter().map(|(id, row)| (*id, row.state)).collect())
    }
}
