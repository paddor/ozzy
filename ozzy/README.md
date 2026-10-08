# Ozzy SDK

Ozzy is a Rust message streaming system for durable event logs, running as a
single broker or a replicated cluster over OMQ. This crate provides its native
Rust producer and consumer APIs.

## Setup

Requires Rust 1.93+. Run a broker using the
[getting started guide](https://github.com/paddor/ozzy/blob/main/GETTING_STARTED.md).
Create a `WriterRuntime`, then connect `BrokerLinks` to your broker endpoints.
Share the link across topics; the examples below use an existing link.

## Produce

`SharedTopicWriter` routes records by optional key. `send` admits locally;
`confirmed()` returns the partition, offset, and achieved storage policy.

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

Keep several records in flight for throughput; waiting for each confirmation
before sending the next record serializes writes.

## Resume a producer

Save `writer.identity().to_bytes()` once: a 32-byte topic/producer token.
Restore it with `ProducerIdentity::from_bytes(saved)`.

| API | Use |
| --- | --- |
| `SharedTopicWriter::resume` | Continue after the old producer has stopped |
| `SharedTopicWriter::takeover` | Fence the old producer explicitly |

Both resolve every partition before returning. A failed takeover may already
have fenced some partitions. The SDK has no durable outbox; a crash can lose
unconfirmed input, and application resubmission can repeat records.

## Consume

`TopicReader` reads individual records from every partition, starting at the
earliest retained offset by default. It repairs live-stream gaps automatically.

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

Save checkpoints after successful application processing. Reopen with
`TopicReaderConfig { start: ReaderStart::Checkpoint(saved), ..Default::default() }`.
Other start positions include latest, broker timestamp, and retained record ID.
Checkpoints do not make application side effects transactional.

## Further reading

- [Overview](https://github.com/paddor/ozzy/blob/main/doc/OVERVIEW.md): record flow and broker modes.
- [Runtime contracts](https://github.com/paddor/ozzy/blob/main/doc/RUNTIME.md): admission, cancellation, batching, and resource bounds.
- Rust API docs: `BrokerLinksConfig`, `SharedTopicWriterConfig`, `ReaderStart`, and `TopicReaderConfig`.
