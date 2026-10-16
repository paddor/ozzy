#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable,
        clippy::too_many_lines,
        clippy::too_many_arguments,
        reason = "product code is linted without cfg(test)"
    )
)]

pub use omq_tokio::{Context, Endpoint};
pub use ozzy_proto::{
    ConsumerGroupId, ConsumerMemberId, EnvelopeLimits, MessageId, NodeId, Offset, PartitionId,
    PartitionIncarnation, ProducerId, SubscriptionId, Topic, TopicId, handshake, progress,
};
pub use ozzy_runtime::replicated;
pub use ozzy_runtime::replicated::{
    AppendDisposition, AppendLinkLimits, BrokerAddress, BrokerLinkError, BrokerLinks,
    BrokerLinksConfig, ClockError, DataLimits, GroupInspection, GroupReader, GroupReaderConfig,
    GroupReaderError, GroupRecord, GroupStart, IdPolicy, PendingProgress, Policy,
    ProcessingIdentity, ProcessingToken, ProducerIdentity, ProgressError, ProgressReceipt,
    ReaderLinkLimits, ReaderStart, RecordInput, RecordPayload, RecordReceipt, RetryPolicy,
    SdkClock, SharedTopicPendingRecord, SharedTopicReceipt, SharedTopicWriter,
    SharedTopicWriterConfig, SharedWriterReservation, TopicCheckpoint, TopicReader,
    TopicReaderConfig, TopicReaderError, TopicReaderStats, TopicRecord, TopicWriterError,
    WriterError, WriterRuntime,
};
pub use ozzy_runtime::{Error, Result};
