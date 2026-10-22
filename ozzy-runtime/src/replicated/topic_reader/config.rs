//! Reader selection: initial positions, partition filters, and checkpoints.

use super::TopicReaderError;
use ozzy_proto::{MessageId, Offset, TopicId};
use std::{collections::BTreeSet, time::Duration};

/// Volatile receive positions. Persist them only after the application has
/// completed its own processing; this is not a consumer-group progress service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicCheckpoint {
    /// Exact topic incarnation. Recreating a topic invalidates its checkpoints.
    pub topic: TopicId,
    /// First undelivered offset of each subscribed numeric partition.
    pub positions: Vec<(u32, Offset)>,
}

/// Initial history selection. Reconnects use delivered offsets, never repeat a seek.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub enum ReaderStart {
    /// First retained record in each selected partition.
    #[default]
    Earliest,
    /// Current confirmed ends, followed by new records.
    Latest,
    /// Application-saved per-partition next offsets.
    Checkpoint(TopicCheckpoint),
    /// First retained offset whose broker append time is at least Unix milliseconds.
    Timestamp(u64),
    /// Select exactly one partition by application record ID.
    RecordId {
        /// Numeric topic partition.
        partition: u32,
        /// Application record identity, independent of producer retry identity.
        id: MessageId,
        /// Explicit duplicate selection; [`Self::record_id`] requires uniqueness.
        policy: ozzy_proto::reader::IdPolicy,
    },
}

impl ReaderStart {
    /// Select one partition by ID, rejecting multiple retained matches by default.
    pub const fn record_id(partition: u32, id: MessageId) -> Self {
        Self::RecordId {
            partition,
            id,
            policy: ozzy_proto::reader::IdPolicy::RequireUnique,
        }
    }

    /// The wire start of partition `number`. A checkpoint must name it.
    pub(super) fn partition_start(
        &self,
        number: u32,
    ) -> Result<ozzy_proto::reader::Start, TopicReaderError> {
        Ok(match self {
            Self::Earliest => ozzy_proto::reader::Start::Earliest,
            Self::Latest => ozzy_proto::reader::Start::Latest,
            Self::Timestamp(timestamp) => ozzy_proto::reader::Start::Timestamp(*timestamp),
            Self::RecordId { id, policy, .. } => ozzy_proto::reader::Start::RecordId {
                id: *id,
                policy: *policy,
            },
            Self::Checkpoint(checkpoint) => ozzy_proto::reader::Start::Offset(
                checkpoint
                    .positions
                    .iter()
                    .find(|&&(n, _)| n == number)
                    .ok_or(TopicReaderError::Configuration)?
                    .1
                    .get(),
            ),
        })
    }
}

/// Topic reader configuration. Default starts at each partition's retained floor.
#[derive(Debug, Clone)]
pub struct TopicReaderConfig {
    /// Initial selector. Checkpoints require every selected partition.
    pub start: ReaderStart,
    /// Optional numeric partition filter. Routing remains automatic.
    pub partitions: Option<Vec<u32>>,
    /// Refresh after silence or an unavailable leader. Uses the SDK clock.
    pub refresh: Duration,
}

impl Default for TopicReaderConfig {
    fn default() -> Self {
        Self {
            start: ReaderStart::Earliest,
            partitions: None,
            refresh: Duration::from_millis(100),
        }
    }
}

impl TopicReaderConfig {
    /// Distinct existing partitions to read: the filter, else the record-ID
    /// partition, else all. A record-ID start reads only its partition, and a
    /// checkpoint must cover exactly these partitions of this topic.
    pub(super) fn selected(
        &self,
        metadata: &crate::topic_metadata::TopicMetadata,
    ) -> Result<Vec<u32>, TopicReaderError> {
        let selected = self.partitions.clone().unwrap_or_else(|| match self.start {
            ReaderStart::RecordId { partition, .. } => vec![partition],
            _ => (0..metadata.partition_count() as u32).collect(),
        });
        if let ReaderStart::RecordId { partition, .. } = self.start
            && selected != [partition]
        {
            return Err(TopicReaderError::Configuration);
        }
        let unique: BTreeSet<_> = selected.iter().copied().collect();
        if selected.is_empty()
            || unique.len() != selected.len()
            || selected.iter().any(|&n| metadata.partition(n).is_none())
        {
            return Err(TopicReaderError::Configuration);
        }
        if let ReaderStart::Checkpoint(checkpoint) = &self.start
            && (checkpoint.topic != metadata.id()
                || checkpoint.positions.len() != selected.len()
                || checkpoint
                    .positions
                    .iter()
                    .map(|&(n, _)| n)
                    .collect::<BTreeSet<_>>()
                    != unique)
        {
            return Err(TopicReaderError::Configuration);
        }
        Ok(selected)
    }
}
