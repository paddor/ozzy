# Architecture

This page maps Ozzy's components, what each one owns, and the rules between
them. It is for contributors. Terms are defined in the
[overview](doc/OVERVIEW.md#terms); the [workspace table](README.md#workspace)
describes each crate.

Ozzy is a broker-owned, segment-only log over OMQ. SDKs own client state;
brokers own journals, replication, and confirmation.

## Components

| Component | Owns | Crates |
| --- | --- | --- |
| OMQ | Network queues, routing, framing, reconnects, fairness, transport pressure | `omq-tokio` |
| Wire protocol | Sans-I/O codecs and message types | `ozzy-proto` |
| Core state | Record order, retry identity, dedup index, group plans, confirmation state | `ozzy-core` |
| Replication | Fixed-three authority, voting, elections, replica recovery | `ozzy-replication` |
| Dispatcher | Public sockets, sessions, routing to shards, source admission | `ozzy-runtime`, `ozzy-broker` |
| Shard | One thread that hosts many partition actors | `ozzy-runtime`, `ozzy-broker` |
| Partition actor | One partition's journal, replication state, readers, and dedup index | `ozzy-runtime` |
| Group coordinator | Membership and plans of the consumer groups whose names map to its partition | `ozzy-core`, `ozzy-runtime` |
| Journal | Canonical history, segment encoding, reads, recovery | `ozzy-journal`, `ozzy-journal-segment` |
| Storage backend | File handles, physical jobs, buffers, fixed workers per device | `ozzy-io`, `ozzy-io-pool`, `ozzy-io-aio` |
| Telemetry | Always-on thread-owned counters; opt-in stage timing and PUB snapshots; never part of confirmation or admission | `ozzy-telemetry` |
| Admin owner | Admin endpoint, status queries to shards, counter snapshots; read-only | `ozzy-broker`, `ozzy-proto` |
| Admin console | Console state and screens without I/O; terminal frontend and admin client | `ozzy-ui` |
| Producer and consumer SDKs | Separate role state over shared session and transport code | `ozzy`, `ozzy-runtime` |
| Python SDK | Thin PyO3 bindings to the Rust SDK; Python values and scheduling only | `bindings/python` |

## Rules

| Area | Rule |
| --- | --- |
| Threads | Each thread owns its mutable state. No cross-thread locks on hot state; immutable `Arc` and `Bytes` may cross. |
| Messaging | OMQ inproc connects the dispatcher and shards. Calls within one thread are direct. |
| Local rings | SDK caller lanes and storage job rings are the only raw rings; see [runtime](doc/RUNTIME.md#local-ring-exceptions). |
| Shards | Shards are symmetric; shard 0 has no special duties. |
| Sans-I/O | Codecs and replication state do no I/O. |
| Modes | All modes share one intake, journal, reader, and storage pipeline. Only authority and confirmation policy differ. |
| Failover | A failed three-broker group never becomes a single broker. |
| Batching | Ready work batches within count and byte bounds, with no collection delay. |
| Capacity | Control traffic has capacity separate from bulk data. |
| Memory | Dequeue frees a queue slot; payload memory is freed when its last alias drops. |
| Cancellation | An accepted job keeps its buffers and handles until its physical completion. |
| Storage | Shards make no filesystem calls. No `io_uring`. |
| Telemetry | Each emitting thread owns its PUB socket; an external SUB collector aggregates. |
| Tests | Tests embed the real broker over OMQ inproc and memory storage, assembled by the `ozzy-sim` `broker` feature. Product crates use it only in tests. |

## Reference map

Each contract has one owning document. This page adds no wire, storage, or
replication specification.

| Document | Owns |
| --- | --- |
| [Overview](doc/OVERVIEW.md) | Terms, modes, partition placement, record flow |
| [Protocol](doc/PROTOCOL.md) | Socket planes, exact wire bytes, commands, sessions, errors |
| [Runtime](doc/RUNTIME.md) | Thread ownership, batching, queues, pressure, cancellation, deployment config |
| [Storage](doc/STORAGE.md) | On-disk formats, segments, barriers, retention, recovery, failure model |
| [Replication](doc/REPLICATION.md) | Fixed-three authority, voting, elections, repair |
| [Getting started](GETTING_STARTED.md) | Build, provision, run, containers, recovery commands |
| [Development](DEVELOPMENT.md) | Contributor checks, test layers, soaks |
| [Benchmark tools](ozzy-bench/README.md) | Benchmark commands and measurement rules |
