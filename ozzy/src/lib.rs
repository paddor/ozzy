#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub use omq_tokio::{Context, Endpoint};
pub use ozzy_proto::{
    EnvelopeLimits, MessageId, NodeId, Offset, PartitionId, PartitionIncarnation, ProducerId,
    SubscriptionId, Topic, TopicId, handshake,
};
pub use ozzy_runtime::replicated;
pub use ozzy_runtime::replicated::{
    AppendLinkLimits, BrokerAddress, BrokerLinkError, BrokerLinks, BrokerLinksConfig, ClockError,
    DataLimits, IdPolicy, Policy, ProducerIdentity, ReaderLinkLimits, ReaderStart, RecordInput,
    RecordReceipt, RetryPolicy, SdkClock, SharedTopicPendingRecord, SharedTopicReceipt,
    SharedTopicWriter, SharedTopicWriterConfig, SharedWriterReservation, TopicCheckpoint,
    TopicReader, TopicReaderConfig, TopicReaderError, TopicReaderStats, TopicRecord,
    TopicWriterError, WriterError, WriterRuntime,
};
pub use ozzy_runtime::{Error, Result};
