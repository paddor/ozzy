# Ozzy Python SDK

Blocking and asyncio producers and consumers for Python 3.12+. The native
extension calls Ozzy's Rust SDK for every log operation. There is no Python
protocol, retry, routing, batching, or transport implementation and no pyomq
dependency. Supported broker modes are single-broker durable and disk quorum.

Classes, methods, configuration fields, and native record properties have API
docstrings available through Python's `help()`, for example `help(Producer.send)`.

Build from this checkout with Rust 1.93+ and maturin:

```sh
export OZZY_ARTIFACT_ROOT=/path/to/artifacts
source scripts/ozzy_tools.sh
maturin build --manifest-path bindings/python/Cargo.toml --release \
  --out "$OZZY_ARTIFACT_ROOT/wheels"
python -m pip install "$OZZY_ARTIFACT_ROOT"/wheels/ozzy-*.whl
```

Use a broker's persistent UUID from its identity file and its configured `peer`
and `data_peer` endpoints. Supply exactly one broker or all three quorum brokers.
Provision topics in the broker configuration before opening a producer or consumer.

```python
from ozzy import BrokerAddress, Client, ClientConfig

config = ClientConfig((BrokerAddress(
    "11111111-1111-1111-1111-111111111111",
    "tcp://127.0.0.1:7000",
    "tcp://127.0.0.1:7001",
),))

with Client(config) as client:
    with client.producer("orders") as producer:
        pending = producer.send(b"order-42", partition=0)
        receipt = pending.confirmed()
        print(receipt.partition, receipt.offset, receipt.policy)

    with client.consumer("orders") as consumer:
        record = next(consumer)
        print(record.message_id, record.parts)
        # Process successfully before persisting this checkpoint.
        saved = consumer.checkpoint()

    with client.consumer("orders", checkpoint=saved) as consumer:
        ready = consumer.try_next()  # Record or None; never waits.
        stats = consumer.stats()  # live_records, replayed_records, live_gaps
```

## Produce

`send(payload, *, key=None, partition=None, id=None)` accepts bytes-like input
or a nonempty sequence of byte parts. Empty parts are preserved. Mutable inputs
are copied before submission. `record.parts` is always a tuple of bytes.
Single-buffer sends copy directly into Rust SDK storage. Receipt UUID strings
are formatted on access. Read `record.parts` once to avoid copying payloads again.
`max_records` and `max_parts` bound producer batches. Reader receive bounds
`reader_max_records` and `reader_max_parts` default to 2,048 each, independent
of producer batching, because raw broker publications combine multiple APPENDs.

Specify `key=b"customer-17"` for Rust SDK key routing, `partition=0` for an exact
numeric partition, or neither for automatic routing. A key and partition are
mutually exclusive. IDs accept a UUID or UUID string; omission generates a
Rust SDK UUIDv7. IDs must be nonzero and identify one application event.

`ClientConfig(compression="lz4", ...)` uses the Rust SDK's adaptive whole-APPEND
LZ4 packing, enabled by default. It keeps raw bytes when compression does not
save space. `compression="raw"` disables payload packing. Consumers decode both
automatically, including mixed history from different producers. Limits and
reported payload sizes count uncompressed bytes.

Wheels also support OMQ `lz4+tcp://` endpoints. Build the broker with its
`lz4-transport` feature and configure matching endpoints on the broker and all
broker addresses. Use `compression="raw"` with compressed
transport to avoid applying compression at both layers.

`send` returns after local admission. Keep multiple pending records in flight
and confirm them later; waiting after each send serializes throughput. Pending
records expose `topic_id`, `partition`, and `sequence`. `confirmed()` waits for
the broker's durability policy. `try_confirmed()` returns a receipt or `None`.
Receipt fields include the partition-global `offset`, `message_id`, producer
identity/epoch/sequence, `disposition` (`stored` or `duplicate`), and `policy`
(`local_durable` or `quorum_durable`). All exposed UUIDs are strings.

`flush()` waits for the current admitted prefix. `close()` seals the producer
and drains admitted records. Save `producer.identity` (32 bytes) once to resume
with `client.producer(topic, identity=saved)` after the previous process has
stopped. `takeover=True` explicitly fences that producer. A failed takeover may
already have fenced some partitions. The SDK has no durable outbox.

An error after admission leaves the record's outcome unknown. Cancelling or
dropping a confirmation does not withdraw its record. A topic configured for
message-ID deduplication reports duplicates using the original offset. This
does not establish exactly-once application processing.

## Consume

Consumers merge partitions, preserving order within each partition. The Rust
reader repairs live-stream gaps. `start="earliest"` is the default;
`start="latest"` selects confirmed ends. `partitions=[0, 2]` filters numeric
partitions. `next()` waits indefinitely; consumers also support iteration.
Subscriptions, including latest-end selection, resolve lazily when polled.
Records retain bounded Rust receive-frame capacity until dropped. Release them
as you process them; keep `record.parts` copies when retaining bytes for later.
`reader_queue_messages=32` in `ClientConfig` sets the default OMQ reader queue
window. Values from 1 through 64 trade burst capacity for reserved memory;
the Rust SDK includes queued and retained frames in its byte reservation.

`Checkpoint(topic_id, positions)` contains the first undelivered offset for
each selected partition. Persist `dataclasses.asdict(checkpoint)` as JSON and
restore it with the same fields (convert positions back to tuples). Opening
with a checkpoint requires the same topic incarnation and partition selection.
It represents delivered progress, not a transaction with application effects.

## Asyncio

```python
import asyncio
from ozzy import BrokerAddress, ClientConfig
from ozzy.asyncio import Client

async def main(config: ClientConfig) -> None:
    async with Client(config) as client:
        async with await client.producer("orders") as producer:
            pending = await producer.send((b"header", b"body"), key=b"customer-17")
            receipt = await pending.confirmed()
        async with await client.consumer("orders") as consumer:
            async with asyncio.timeout(5):
                record = await consumer.next()

# asyncio.run(main(config))
```

`await Client.connect(config)` also opens a client. Async methods use the same
Rust futures as blocking methods. Ready operations complete inline; pending
operations yield to asyncio. Lifecycle polls and blocking waits release the GIL.
Cancelling `next()` preserves reader delivery state; it does not consume a
future record in a background Python worker. Cancelling a send before admission
assigns no sequence. An admitted record remains active independently of its
observer. An interrupted close leaves the handle sealed.

Handles can move between Python threads; blocking waits release the GIL. One
producer send or consumer receive may be active per handle; overlapping operations
are rejected (`BusyError` for a leased async handle). Pending confirmations
support simultaneous observers. Cancel and
await an outstanding operation before closing its handle. Create new clients
after fork. Close producers and consumers before closing their client; client
close shuts down its shared broker links. Close is idempotent.

## Limits and errors

`ClientConfig` declares finite per-client capacity: `max_partitions` (16),
`max_producers` (1), `max_consumers` (1), `max_packet_bytes` (1 MiB),
`max_record_bytes` (64 KiB), `max_records` per packet (64), `max_parts` per
packet (256), and `request_timeout` in seconds (5). Producer and consumer counts
reserve the configured partition ceiling per handle; smaller topics can share
unused partition capacity. Increase them for more simultaneously open handles.
The binding uses the Rust SDK's reservation helpers
and validation, including unused queue capacity. Broker directional limits may
be smaller. Admission waits under pressure instead of growing Python queues.

Invalid Python inputs raise `TypeError`, `ValueError`, or `OverflowError`.
SDK failures raise `OzzyError`, with `ClosedError` and `BusyError` for binding
lifecycle errors. The API includes type information (`py.typed`).

## Development

Run `scripts/check-python.sh` from the repository root. Rust workspace tests
execute the shipped Python facade against the real broker over OMQ inproc and
memory storage in both modes. They cover ordering, multipart data, routing,
resume, checkpoints, cancellation, bounded intake, and GIL release. The Python
check also builds a wheel, checks its native exports against the type stub,
and exercises it in separate Python processes over TCP.
