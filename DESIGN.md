# Architecture

Ozzy is a broker-owned, segment-only log over OMQ. SDKs own client state;
brokers own journals, replication, and confirmation. The
[overview](doc/OVERVIEW.md) explains record flow; the
[workspace table](README.md#workspace) lists crate boundaries.

## Boundaries

| Component | Owns |
| --- | --- |
| OMQ | Network queues, routing, framing, reconnects, fairness, transport pressure |
| Ozzy protocol and core | Record identity, partition offsets, retry results, confirmation |
| Partition actor | Its journal, replication state, and reader state on one shard |
| Transport owner | Shared public sockets, sessions, routing, source admission |
| Storage backend | File handles, physical jobs, buffers, fixed workers per device |
| Producer / consumer SDK | Separate role state using common session/transport code |

Each shard owns its mutable state on one thread. OMQ inproc connects transport
owners and shards; same-thread journal calls are direct. Codecs and replication
state are sans-I/O. SDK caller lanes and storage job rings are local exceptions
described in [runtime](doc/RUNTIME.md#local-ring-exceptions).

All modes share intake, journal, reader, and storage pipelines. Their authority
and confirmation policies remain explicit. Every replicated broker stores every
partition; local shard placement changes execution, not record identity or
membership. A failed three-broker group never becomes a single-broker deployment.

Ready work batches within count/byte bounds without a collection delay. Control
has separate capacity from bulk data. Dequeue releases slots; final aliases
release backing memory. Cancellation retains accepted jobs and their buffers
until physical completion. Shards perform no filesystem calls; no io_uring.

## Reference map

Each contract has one owning document; this file adds no separate wire, storage,
or replication specification.

| Reference | Contract |
| --- | --- |
| [Overview](doc/OVERVIEW.md) | Terms, partition placement, record flow, mode boundaries |
| [Protocol](doc/PROTOCOL.md) | Socket planes, exact wire bytes, commands, sessions |
| [Runtime](doc/RUNTIME.md) | Thread ownership, batching, queues, pressure, cancellation |
| [Storage](doc/STORAGE.md) | Canonical formats, segments, barriers, retention, recovery |
| [Replication](doc/REPLICATION.md) | Fixed-three authority, voting, elections, repair |

Tests embed the real broker with controlled OMQ inproc and memory storage.
The optional simulator `broker` feature assembles that harness; product libraries
do not depend on it outside tests. [Development](DEVELOPMENT.md) owns test and
soak guidance. [Benchmark tools](ozzy-bench/README.md) own measurement commands.
