# ozzy

Add the SDK to an application's Cargo dependencies:

```toml
[dependencies]
ozzy = "0.1.0"
```

Requires Rust 1.93 or newer. Run a provisioned Ozzy broker separately; install it
with `cargo install ozzy-broker --version 0.1.0 --locked --bin ozy_broker`.

Producer SDK and consumer SDK for Ozzy brokers over OMQ. They share session
code while retaining separate role state. `BrokerLinks` owns one data PEER and
one control PEER connected to all configured brokers. Live consumers add one
SUB per broker; partition count does not add PEER sockets.

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

Save `writer.identity().to_bytes()` once in application config. Resume needs
only this 32-byte topic/producer token; brokers supply each partition's current
epoch and next sequence. Use `SharedTopicWriter::resume` after the old process
has stopped, or `takeover` to fence it explicitly. Both resolve every partition
before returning. A failed takeover may already have fenced some partitions.
The SDK stores no durable outbox. A crash may lose unconfirmed records; application
resubmission may repeat records. Record IDs do not deduplicate application work.

```rust,no_run
use ozzy::{BrokerLinks, DataLimits, ProducerIdentity, RetryPolicy,
    SharedTopicWriter, SharedTopicWriterConfig};

async fn resume(links: &BrokerLinks, limits: DataLimits, saved: [u8; 32])
    -> Result<SharedTopicWriter, Box<dyn std::error::Error>> {
    Ok(SharedTopicWriter::resume(links, "orders", ProducerIdentity::from_bytes(saved)?,
        SharedTopicWriterConfig::new(limits), RetryPolicy::default()).await?)
}
```

See [runtime contracts](../doc/RUNTIME.md#sdk-protocol-batching) for admission,
resource bounds, and cancellation. The default payload target is 64 KiB with
one outstanding APPEND. Collection is bounded by bytes, negotiated limits, and
a hard 2,048-record ceiling; sparse sends have no artificial collection wait.

`TopicReader` opens every partition of a named topic and returns
individual records. Its checkpoint names the next received offset per partition.
Save it after application processing, then reopen with
`TopicReaderConfig { start: ReaderStart::Checkpoint(saved), ..Default::default() }`.
Default start is earliest retained. `ReaderStart::Latest` starts at the current end;
`Timestamp(unix_millis)` seeks by broker append time. `ReaderStart::record_id(partition,
id)` requires one unique retained match. Choose `IdPolicy::FirstRetained` or
`LastRetained` explicitly when duplicate IDs are expected. Seeks use confirmed
history; reconnect continues from the next delivered offset.

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
