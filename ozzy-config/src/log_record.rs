use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::ConfigError;

/// The directory below a device root that holds the records and the stores
/// of runtime logs.
pub const LOG_DIRECTORY: &str = "logs";

/// The name of log `id` on disk: its store directory below
/// [`LOG_DIRECTORY`], and the stem of the files beside it.
pub fn log_label(id: Uuid) -> String {
    id.simple().to_string()
}

/// One broker's durable record of a log created at run time. It is written
/// before the store is formatted and kept after the log is deleted, so a
/// log ID is formatted at most once on a broker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogRecord {
    /// Persistent cluster namespace.
    pub cluster: Uuid,
    /// Persistent local broker ID.
    pub broker: Uuid,
    /// The log's ID, which is its replication group ID.
    pub log: Uuid,
    /// Record namespace of the log's one partition.
    pub incarnation: Uuid,
    /// One or three member brokers. The first leads the first view.
    pub members: Vec<Uuid>,
    /// Persistent containing volume ID.
    pub volume: Uuid,
    /// Persistent local journal-store ID.
    pub store: Uuid,
    /// Store incarnation fenced against obsolete physical work.
    pub generation: u64,
    /// Segment size of the class the log was created in.
    pub segment_bytes: u64,
    /// Append bound of the class the log was created in.
    pub max_append_bytes: u64,
    /// How far creation or deletion got.
    pub state: LogState,
}

/// Lifecycle of a runtime log on one broker. It only moves forward.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum LogState {
    /// Recorded; the store may be missing or partial. It never served.
    Creating,
    /// Formatted. The broker opens it at every start.
    Ready,
    /// Being removed. It is never opened again.
    Deleting,
    /// Removed. The record stays, so the ID is never formatted again.
    Deleted,
}

impl LogRecord {
    /// Whether `other` records the same log with the same members and class,
    /// whatever its store and state.
    pub fn same_log(&self, other: &Self) -> bool {
        (self.cluster, self.broker, self.log, self.incarnation)
            == (other.cluster, other.broker, other.log, other.incarnation)
            && self.members == other.members
            && (self.segment_bytes, self.max_append_bytes)
                == (other.segment_bytes, other.max_append_bytes)
    }

    /// This record in `state`.
    #[must_use]
    pub fn with_state(&self, state: LogState) -> Self {
        Self {
            state,
            ..self.clone()
        }
    }

    /// Encode a checksummed record.
    pub fn encode(&self) -> Result<String, ConfigError> {
        crate::record::encode("log", self)
    }

    /// Check integrity and parse a record.
    pub fn decode(input: &str) -> Result<Self, ConfigError> {
        crate::record::decode("log", input)
    }
}
