# ozzy

Native writer and reader SDK for Ozzy brokers over OMQ.

`SharedTopicWriter` opens a named topic through an existing
`BrokerLinks` owner. Records take an optional key. Confirmation includes the
topic identity, numeric partition, offset, and configured storage policy.

```rust,no_run
use ozzy::{
    BrokerLinks, DataLimits, MessageId, RecordInput, RetryPolicy,
    SharedTopicWriter, SharedTopicWriterConfig,
};

async fn write(links: &BrokerLinks, limits: DataLimits) -> Result<(), Box<dyn std::error::Error>> {
    let mut writer = SharedTopicWriter::open(
        links, "orders", SharedTopicWriterConfig::new(limits), RetryPolicy::default(),
    ).await?;
    let pending = writer.send(
        RecordInput::copy_from_slice(MessageId::new(), b"order-42"),
        Some(b"customer-17"),
    ).await?;
    let receipt = pending.confirmed().await?;
    println!("partition={} offset={}", receipt.partition, receipt.record.offset);
    writer.close().await?;
    Ok(())
}
```

See [runtime contracts](../doc/RUNTIME.md#sdk-protocol-batching) for admission,
resource bounds, and cancellation.

`TopicReader` opens every partition of a named topic and returns
individual records. Its checkpoint names the next received offset per partition.
Save it after application processing, then pass it when reopening the reader.

```rust,no_run
use ozzy::{BrokerLinks, TopicReader, TopicReaderConfig};

async fn read(links: BrokerLinks) -> Result<(), Box<dyn std::error::Error>> {
    let mut reader = TopicReader::open(links, "orders", TopicReaderConfig::default()).await?;
    let record = reader.next().await?;
    println!("partition={} offset={}", record.partition, record.offset.get());
    let checkpoint = reader.checkpoint();
    reader.close().await?;
    // Persist checkpoint only after processing record successfully.
    let _ = checkpoint;
    Ok(())
}
```

Create a `WriterRuntime`, then connect `BrokerLinks` with broker endpoints,
negotiation parameters, and bounded writer or reader capacity. The link is
shared across topics and partitions. `handshake`, `EnvelopeLimits`, `DataLimits`,
`AppendLinkLimits`, and `ReaderLinkLimits` are available from this crate for
that configuration.

`send` admits a record locally. Only `confirmed()` establishes the configured
broker storage policy. Reader checkpoints are volatile receive positions; save
them only after application processing. A checkpoint does not make application
side effects transactional. See [runtime contracts](../doc/RUNTIME.md) for
capacity and cancellation behavior and [design](../DESIGN.md) for deployment
boundaries.
