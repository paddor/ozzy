//! One topic writer over shared broker links and partition-local SDK state.

use super::{
    PendingRecord, RecordInput, RecordReceipt, RetryPolicy, Writer, WriterConfig, WriterError,
};
use ozzy_proto::PartitionIncarnation;

mod identity;
mod shared;
pub use identity::ProducerIdentity;
pub use shared::{
    SharedTopicPendingRecord, SharedTopicReceipt, SharedTopicWriter, SharedTopicWriterConfig,
};
mod state;
use state::Partitions;

#[derive(Debug, Clone, Copy)]
enum TopicSelection<'a> {
    Key(&'a [u8]),
    Keyless,
}

/// Topic routing or underlying partition-writer failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TopicWriterError {
    /// Invalid topic metadata or writer bounds.
    #[error("invalid topic writer configuration")]
    Configuration,
    /// Shared broker metadata or control failure.
    #[error(transparent)]
    Broker(#[from] super::BrokerLinkError),
    /// Failure from the chosen partition writer. Its outcome may be unknown.
    #[error(transparent)]
    Writer(#[from] WriterError),
    /// A send failed after selection; retry against this frozen destination.
    #[error("topic partition {partition:?}: {source}")]
    Send {
        /// Partition selected before admission.
        partition: PartitionIncarnation,
        /// Underlying writer failure; outcome may be unknown.
        #[source]
        source: WriterError,
    },
}

struct Partition {
    target: PartitionIncarnation,
    writer: Writer,
}

fn keyed_index(key: &[u8], seed: u64, partitions: usize) -> usize {
    (xxhash_rust::xxh3::xxh3_64_with_seed(key, seed) % partitions as u64) as usize
}
