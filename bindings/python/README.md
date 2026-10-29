# Ozzy Python SDK

Blocking and asyncio producers and consumers for [Ozzy](../../README.md).
Python 3.12+ on Linux, as a thin layer over the Rust SDK.

## Install

Requires Rust 1.93+. From the repository root:

```sh
python3 -m venv tmp/venv
tmp/venv/bin/pip install ./bindings/python
```

## Try it

Start a local broker with topic `orders`
([getting started](../../GETTING_STARTED.md#try-it)):

```sh
cargo run --release -p ozzy-broker -- quickstart tmp/try
```

Save as `quickstart.py` and run `tmp/venv/bin/python quickstart.py` from the
repository root:

```python
import tomllib
from pathlib import Path

from ozzy import BrokerAddress, Client, ClientConfig

# Broker ID and endpoints written by `quickstart tmp/try`.
deployment = tomllib.loads(Path("tmp/try/deployment.toml").read_text())
identity = tomllib.loads(Path("tmp/try/shared.identity").read_text())
endpoints = deployment["brokers"]["local"]["endpoints"]
broker = BrokerAddress(
    identity["brokers"]["local"], endpoints["peer"], endpoints["data_peer"]
)
config = ClientConfig([broker])

with Client(config) as client:
    with client.producer("orders") as producer:
        pending = [producer.send(b"order-%d" % n, key=b"customer-17") for n in range(3)]
        for record in pending:
            receipt = record.confirmed()  # durable on the broker
            print("stored", receipt.partition, receipt.offset)

    with client.consumer("orders") as consumer:
        for _ in range(3):
            record = consumer.next(timeout=5)
            print("read", record.partition, record.offset, record.parts)
        checkpoint = consumer.checkpoint()  # persist after processing
```

`ozzy.asyncio` has the same types, with coroutines. Same `config`:

```python
import asyncio

from ozzy.asyncio import Client


async def main() -> None:
    async with Client(config) as client:
        async with await client.producer("orders") as producer:
            pending = [await producer.send(b"order-%d" % n, partition=0) for n in range(3)]
            receipts = await asyncio.gather(*(p.confirmed() for p in pending))
        async with await client.consumer("orders", partitions=[0]) as consumer:
            async for record in consumer:
                print("read", record.offset, record.parts)
                if record.offset == receipts[-1].offset:
                    break


asyncio.run(main())
```

## How it works

- The `ozzy._native` extension (PyO3, abi3) calls the Rust SDK for every log
  operation. Routing, batching, retries, LZ4, and OMQ transport stay in Rust.
  Python only checks arguments.
- Each `Client` runs a Rust SDK thread and OMQ I/O threads. Brokers own the
  log; the client owns no journal and binds no socket.
- Ready calls finish inline on the caller's thread: a send with free capacity,
  a buffered record, a confirmed receipt. Only waits involve other threads.
- Blocking waits release the GIL. Asyncio tasks wake through one native
  notifier per event loop; SDK threads never take the GIL.
- `ClientConfig` bounds memory before connecting. Under pressure, `send`
  waits or yields; no Python queue grows.
- Delivered records hold Python `bytes`, not Rust receive buffers.

## Essentials

| Type | Use |
| --- | --- |
| `ClientConfig(brokers, ...)` | Brokers plus finite limits, e.g. `max_record_bytes`, `max_producers` |
| `BrokerAddress(node_id, peer, data_peer)` | Broker UUID and its two endpoints |
| `Client(config)` | `producer(topic)`, `consumer(topic, start=, checkpoint=, partitions=)`, `close()` |
| `Producer` | `send(payload, key=/partition=, id=, due_millis=)` returns `PendingRecord`; `flush()`; `identity` |
| `PendingRecord` | `confirmed()` returns `Receipt`; `try_confirmed()` |
| `Receipt` | `partition`, `offset`, `message_id`, `disposition`, `policy` |
| `Consumer` | `next()`, iteration, `try_next()`, `checkpoint()`, `stats()` |
| `Record` | `partition`, `offset`, `message_id`, `parts` |
| `OzzyError` | SDK failure; `ClosedError` and `BusyError` are subclasses |

Blocking waits accept `timeout=` in seconds and raise `TimeoutError`. Every
API has docstrings: `help(ozzy.Producer.send)`.

## Good to know

- `send` returns once the record is admitted locally. `confirmed()` waits for
  durability: the local disk in single-broker mode, two of three copies in disk
  quorum. Keep many records in flight; confirm or `flush()` later.
- A key always routes to the same partition. Order holds per partition.
- On a topic with delay, `due_millis=` (absolute Unix milliseconds) holds a
  record until then. Its receipt says `disposition == "scheduled"`, and its
  `offset` is the schedule sequence; readers see the record at a new offset
  once it is due. A due time past the topic's maximum delay fails.
- Save `producer.identity` and pass it back as `identity=` to resume after a
  restart. `takeover=True` fences an old process still using it.
- Persist a checkpoint only after processing succeeds, then reopen with
  `consumer(topic, checkpoint=saved)`. It tracks delivery, not application
  effects. Ozzy gives no exactly-once processing.
- One send or receive per handle at a time; an overlapping call raises
  `BusyError`. Handles may move between threads, and opens may overlap.
- `max_producers` and `max_consumers` default to 1. Raise them for more open
  handles.
- Close producers and consumers before their client. Create a new client after
  `fork()`.
- `compression="lz4"` (the default) packs APPENDs adaptively. With
  `lz4+tcp://` endpoints, use `compression="raw"`.

## Development

`scripts/check-python.sh` lints, type-checks, and tests the SDK against the
real broker over OMQ inproc and memory storage, then tests the built wheel in
separate processes over TCP.
