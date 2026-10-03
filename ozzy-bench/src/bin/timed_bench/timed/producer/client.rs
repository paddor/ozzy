//! Timed writer adapter for the production topic SDK.

use super::{Config, Result, error, lanes};
use bytes::Bytes;
use ozzy_proto::TopicId;
use ozzy_runtime::replicated::{
    BrokerLinks, PartitionTarget, RecordInput, RecordReceipt, RetryPolicy,
    SharedTopicPendingRecord, SharedTopicWriter, WriterRuntime, WriterStats,
};
use serde_json::Value;
use std::ops::Range;

use super::super::native::Setup;
#[cfg(test)]
mod tests;

pub(super) struct Writer {
    sdk: SharedTopicWriter,
    key: Bytes,
    number: u32,
}

pub(super) struct PendingRecord {
    pending: SharedTopicPendingRecord,
    topic: TopicId,
    number: u32,
}

pub(super) struct ReceiptCheck {
    pub(super) partition: PartitionTarget,
    first: Option<u64>,
    previous: Option<u64>,
}

impl ReceiptCheck {
    pub(super) fn offset(&mut self, offset: u64, sequence: u64) -> bool {
        let valid = offset >= sequence && self.previous.is_none_or(|previous| offset > previous);
        if valid {
            self.first.get_or_insert(offset);
            self.previous = Some(offset);
        }
        valid
    }

    pub(super) fn offset_range(&self) -> Option<[u64; 2]> {
        Some([self.first?, self.previous?])
    }
}

impl Writer {
    pub(super) fn shared(writer: SharedTopicWriter, key: Bytes) -> Self {
        let number = writer.metadata().keyed_partition(&key).number;
        Self {
            sdk: writer,
            key,
            number,
        }
    }

    pub(super) fn receipt_check(&self) -> ReceiptCheck {
        ReceiptCheck {
            partition: PartitionTarget::Group(
                self.sdk
                    .metadata()
                    .partition(self.number)
                    .expect("checked numeric partition")
                    .incarnation,
            ),
            first: None,
            previous: None,
        }
    }

    pub(super) fn number(&self) -> u32 {
        self.number
    }

    pub(super) async fn send(&mut self, input: RecordInput) -> Result<PendingRecord> {
        let topic = self.sdk.metadata().id();
        let pending = self.sdk.send(input, Some(&self.key)).await?;
        if pending.partition() != self.number || pending.topic() != topic {
            return Err(error("native topic admission changed destination"));
        }
        Ok(PendingRecord {
            pending,
            topic,
            number: self.number,
        })
    }

    pub(super) fn stats(&self) -> WriterStats {
        self.sdk
            .partition_stats(self.number)
            .expect("checked numeric partition")
    }

    pub(super) async fn close(self) -> Result<()> {
        self.sdk.close().await?;
        Ok(())
    }
}

impl PendingRecord {
    pub(super) async fn confirmed(&self) -> Result<RecordReceipt> {
        Self::record(self.pending.confirmed().await?, self.topic, self.number)
    }

    pub(super) fn try_confirmed(&self) -> Option<Result<RecordReceipt>> {
        self.pending
            .try_confirmed()
            .map(|result| Self::record(result?, self.topic, self.number))
    }

    fn record(
        receipt: ozzy_runtime::replicated::SharedTopicReceipt,
        topic: TopicId,
        number: u32,
    ) -> Result<RecordReceipt> {
        if receipt.topic != topic || receipt.partition != number {
            return Err(error("native topic confirmation changed destination"));
        }
        Ok(receipt.record)
    }
}

pub(super) async fn connect_shared(
    runtime: &WriterRuntime,
    config: &Config,
    setup: &Value,
    lanes: Range<usize>,
) -> Result<(Vec<Writer>, BrokerLinks)> {
    if config.args.writer_linger_us != 0 {
        return Err(error(
            "shared topic writers currently require --writer-linger-us 0",
        ));
    }
    let settings = setup;
    let setup = Setup::parse(config, settings)?;
    let links = setup
        .writer_links(runtime, config, &settings["append"])
        .await?;
    let mut clients = Vec::new();
    for lane in lanes {
        let settings = super::super::native::writer_settings(config);
        let writer = SharedTopicWriter::open_with_producer(
            &links,
            &setup.topic,
            lanes::producer(lane),
            settings,
            RetryPolicy::default(),
        )
        .await?;
        if writer.metadata().policy() != config.args.system.policy()
            || writer.metadata().partition_count() != setup.partitions
        {
            return Err(error("native topic policy or partition count mismatch"));
        }
        let desired = lane % setup.partitions;
        let key = (0_u64..1_000_000)
            .map(u64::to_be_bytes)
            .find(|key| writer.metadata().keyed_partition(key).number as usize == desired)
            .ok_or_else(|| error("cannot construct bounded balanced native keys"))?;
        clients.push(Writer::shared(writer, Bytes::copy_from_slice(&key)));
    }
    Ok((clients, links))
}
