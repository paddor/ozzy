# ozzy

Rust SDK for [Ozzy](https://github.com/paddor/ozzy), a broker-owned,
segment-only event log over OMQ. Brokers own storage and confirmation; the SDK
owns no journal and binds no broker socket.

## Entry points

| Type | Use |
| --- | --- |
| `WriterRuntime` | Owned SDK and OMQ threads; any caller executor works |
| `BrokerLinks` | Shared connections to one or three brokers |
| `SharedTopicWriter` | Send records by optional key; await confirmation |
| `TopicReader` | Read every partition from a chosen start |
| `GroupReader` | Divide a topic's partitions among group members |

## Connect

Requires Rust 1.93+ and a running broker; see
[getting started](https://github.com/paddor/ozzy/blob/main/GETTING_STARTED.md).

1. Create a `WriterRuntime::new()?`.
2. Fill a `BrokerLinksConfig`. Each `BrokerAddress` takes a broker ID from the
   shared identity file and the broker's configured `peer` and `data_peer`.
3. `BrokerLinks::connect(&runtime, config).await?`. Clones share its sockets.
4. `links.shutdown().await?` before exit.

Pick `parameters` by use and add `handshake::OWNER_ROUTING` to `capabilities`:

- Writers and readers: `Parameters::streaming(limits, PRODUCER | CONSUMER)`
  plus `OWNER_READ`, with `append` and `reader` limits.
- Readers: `Parameters::reader(limits, CONSUMER)`, with `reader` limits.
- Consumer groups: `Parameters::group(limits, CONSUMER)`, with `reader` limits.

Size `control_bytes` with `control_reservation_bytes()` and `reader.bytes` with
`ReaderLinkLimits::reservation_bytes`; `validate()` opens no sockets. Feature
`lz4-transport` adds `lz4+tcp://` endpoints.

## Produce

`send` picks a partition by optional key and returns after local admission.
`confirmed()` waits for the topic's confirmation policy, never transport
receipt.

```rust,no_run
use ozzy::{
    BrokerLinks, DataLimits, MessageId, RecordInput, RetryPolicy, SharedTopicWriter,
    SharedTopicWriterConfig,
};

async fn write(links: &BrokerLinks, limits: DataLimits) -> Result<(), Box<dyn std::error::Error>> {
    let config = SharedTopicWriterConfig::new(limits);
    let mut writer = SharedTopicWriter::open(links, "orders", config, RetryPolicy::default()).await?;
    let record = RecordInput::copy_from_slice(MessageId::new(), b"order-42");
    let pending = writer.send(record, Some(b"customer-17")).await?;
    let receipt = pending.confirmed().await?;
    println!("partition={} offset={}", receipt.partition, receipt.record.offset);
    writer.close().await?;
    Ok(())
}
```

- Keep many records in flight; awaiting each confirmation serializes writes.
- Dropping a `confirmed()` future does not withdraw the record.
- An error after `send` leaves the outcome unknown.

## Resume a producer

Save `writer.identity().to_bytes()` once (32 bytes). Restore it with
`ProducerIdentity::from_bytes`.

| API | Use |
| --- | --- |
| `SharedTopicWriter::resume` | Continue after the old process has stopped |
| `SharedTopicWriter::takeover` | Fence the old producer explicitly |

Both resolve every partition before returning. A failed takeover may already
have fenced some partitions; retrying it is safe. The SDK has no durable
outbox: a crash can lose unconfirmed input, and resubmission can repeat records.

## Deduplication

A topic with `[topics.NAME.deduplication]` stores only the first record per
`MessageId` within its window. Every sender of that ID gets the original
offset; `disposition` says whether this attempt stored it.

```rust,no_run
use ozzy::{AppendDisposition, SharedTopicPendingRecord};

async fn confirm(pending: SharedTopicPendingRecord) -> Result<(), Box<dyn std::error::Error>> {
    let receipt = pending.confirmed().await?;
    if receipt.record.disposition == AppendDisposition::Duplicate {
        println!("already stored at offset {}", receipt.record.offset);
    }
    Ok(())
}
```

Message IDs must be nonzero and unique per event. After the window, a repeat is
stored again.

## Consume

`TopicReader` reads every partition from the earliest retained offset by
default and repairs live-stream gaps itself. Order holds per partition only.

```rust,no_run
use ozzy::{BrokerLinks, ReaderStart, TopicCheckpoint, TopicReader, TopicReaderConfig};

async fn read(
    links: BrokerLinks,
    saved: Option<TopicCheckpoint>,
) -> Result<TopicCheckpoint, Box<dyn std::error::Error>> {
    let start = saved.map_or(ReaderStart::Earliest, ReaderStart::Checkpoint);
    let config = TopicReaderConfig { start, ..Default::default() };
    let mut reader = TopicReader::open(links, "orders", config).await?;
    let record = reader.next().await?;
    println!("partition={} offset={}", record.partition, record.offset.get());
    // Process the record, then persist the checkpoint.
    let checkpoint = reader.checkpoint();
    reader.close().await?;
    Ok(checkpoint)
}
```

Save a checkpoint only after processing succeeds; it does not make side effects
transactional. Other starts: `Latest`, `Timestamp` (broker append time, Unix
ms), and `ReaderStart::record_id`.

## Consumer groups

`GroupReader` divides a topic's partitions among a named group's members. Each
partition resumes at the group's confirmed cursor.

```rust,no_run
use ozzy::{BrokerLinks, ConsumerMemberId, GroupReader, GroupReaderConfig};

async fn work(links: BrokerLinks, member: ConsumerMemberId) -> Result<(), Box<dyn std::error::Error>> {
    let config = GroupReaderConfig::new(member);
    let mut reader = GroupReader::open(links, "orders", "billing", config).await?;
    let delivered = reader.next().await?;
    // Process delivered.record, then acknowledge it.
    let pending = reader.acknowledge(delivered.token)?;
    pending.confirmed().await?;
    reader.close().await?;
    Ok(())
}
```

- Keep `member` stable across restarts of one worker.
- Unacknowledged records go to the partition's next owner: at-least-once.
- `BrokerLinks::inspect_group` shows members and partition ownership.
- `BrokerLinks::delete_group` fences members, drops cursors, and frees the
  name. Its readers stop with `GroupReaderError::Deleted`.

## Docs

- [Overview](https://github.com/paddor/ozzy/blob/main/doc/OVERVIEW.md): terms,
  record flow, broker modes.
- [Runtime](https://github.com/paddor/ozzy/blob/main/doc/RUNTIME.md#sdk-protocol-batching):
  SDK batching, admission, cancellation, bounds.
- Rustdoc of each `*Config` type for every bound.
