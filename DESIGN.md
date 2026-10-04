# Ozzy design

Ozzy is a broker-owned, segment-only record log above OMQ. Producer and consumer
SDKs connect to brokers; they neither own journals nor bind broker sockets.
Embedded tests use the real broker runtime with injected transport and file I/O.
[Overview](doc/OVERVIEW.md) defines the terms.

## Documentation

| Reference | Owns |
| --- | --- |
| [Protocol](doc/PROTOCOL.md) | Socket planes, wire bytes, commands, sessions |
| [Runtime](doc/RUNTIME.md) | Owners, queues, batching, pressure, readers |
| [Storage](doc/STORAGE.md) | Canonical bytes, segments, barriers, caches, recovery |
| [Replication](doc/REPLICATION.md) | Fixed-three authority, confirmation, election, repair |
| [Development](DEVELOPMENT.md) | Contributor checks, test layers, simulation, and soaks |

Codecs and independent fixtures own exact layouts. READMEs describe usage.
[BENCHMARKS.md](BENCHMARKS.md) owns measured charts; raw results stay outside
tracked docs. Reserved commands do not imply an implemented public service.

## Architectural rules

1. OMQ owns network queues, identity routing, reconnects, framing, fairness,
   wakeups, and transport pressure. Ozzy commands are ordinary OMQ messages.
2. Ozzy owns record identity, offset order, retries, gap repair, persistence,
   and confirmation. Transport receipt or OMQ QoS proves none of them.
3. One partition actor owns replication and journal state on one shard.
   Each broker independently places actors; actors share threads, never history.
4. Use one intake, journal, reader, and storage pipeline across modes.
   Keep real authority/persistence policies explicit; share common algorithms.
5. Producer and consumer roles keep separate state and reuse `BrokerLinks`.
   Route before batching. One APPEND contains one producer/epoch/partition;
   retry identity survives regrouping and reconnect.
6. Batch ready work within count/byte bounds. Sparse traffic sends promptly;
   loaded owners form larger batches without waiting for them to fill.
7. Use OMQ inproc between broker transport and shards. Call same-thread owners
   directly. SDK caller and file-job rings remain local, explicit exceptions.
8. Keep data and control capacity separate. Never await a full destination and
   stall unrelated sources. Full PEER intake pauses that connection; PUB drops
   and repairs over PEER. No wire capacity grants remain.
9. Backends own file handles, physical jobs, and fixed workers per device.
   Shards own format/state, perform no file syscalls, and install only ordered,
   generation-matched completions. No io_uring.
10. Cancellation stops observation, not accepted work or buffer ownership.
    Queue dequeue releases a slot; final aliases release retained bytes.
11. Integrity, stale-work fences, protected history, and fail-closed recovery
    remain mandatory. Decoding alone establishes no authority or durability.
12. Replay can repeat application effects. Never claim exactly-once processing.

## Crate boundaries

| Crate | Responsibility |
| --- | --- |
| `ozzy` | Public producer/consumer SDK facade |
| `ozzy-broker` | Provisioning, startup, shards, shared public socket owners |
| `ozzy-config` | Deployment TOML, persistent identities, local placement validation |
| `ozzy-runtime` | OMQ orchestration, partition actors, SDKs, reader services |
| `ozzy-proto` | Sans-I/O application codecs and shared types |
| `ozzy-core` | Canonical state, offsets, retry results, confirmation |
| `ozzy-replication` | Sans-I/O fixed-three elections, evidence, recovery |
| `ozzy-journal` | Canonical operations, journal/read contracts, integrity |
| `ozzy-journal-segment` | Segment state, encoding, async reads/writes, recovery |
| `ozzy-io` | Owned file jobs, opaque handles, admission, completion, file simulator |
| `ozzy-io-pool` | Bounded blocking execution and shared handle registry |
| `ozzy-io-aio` | One Linux AIO owner per device, with fixed blocking helpers |
| `ozzy-sim` | Controlled workloads, protocol schedules, shared real-broker memory/inproc harness and independent SDK record oracle |
| `ozzy-bench` | Serial workloads, profiling, append-only result ledger, SVG charts |

The simulator's base library depends only on the canonical core and journal.
Its optional `broker` feature assembles the full product harness and `ozy_sim`
runner. Product libraries never depend on this harness outside dev dependencies.

## Integration boundaries

| Mode | Authority | Confirmation |
| --- | --- | --- |
| Single-broker durable | Local driver; no election or failover | Local durable prefix and application |
| Disk quorum | Fixed three-broker group | Matching durable history on leader and one follower, then application |
| Replicated-persisting | Same fixed group | Matching retained history on leader and one follower, then application; background disk writes |

The native broker supports all three through the same journal owner, frontend,
and reader pipeline. Single-broker mode is explicit; a failed replicated group
never changes membership or weakens its policy. RP unclean restart requires
nonvoting recovery when clean-stop evidence cannot be proven.

Known broker endpoints are provisioned. Topic metadata lookup, route watches,
producer open/fence, APPEND confirmation, reader replay/live delivery, leader
change, and explicit partition recovery are implemented. Broker-stored consumer
offsets, consumer groups, online topic creation, discovery, partition movement, and online membership are not.

## Partition placement and leadership

A topic binds an immutable partition set and partitioner. Each partition has
its own group, journal, offsets, view, and recovery. All three brokers store
every partition. A partition belongs to the topic, not a producer.

| Unit | Owns |
| --- | --- |
| Topic | Name/incarnation, partition count/order, fixed XXH3-64 seed |
| Partition/group | Shared offsets, operation order, leader/view, journal, policy |
| Producer within partition | Epoch, sequence, bounded exact retry results |
| Application shard | Exclusive execution of assigned partition actors |
| Broker link | Sessions and multiplexed traffic; no partition ownership |

Initial persisted membership order distributes leaders. Elections choose
`brokers[view % 3]`; hints never grant authority. Restart does not reorder members.
Shard counts/numbers may differ across brokers. More partitions allow more
parallelism but cost journals, timers, files, and smaller per-partition batches.

The SDK chooses the partition before admission. A wrong leader returns scoped
information instead of forwarding payloads. Retry preserves the destination.
Adding CPUs changes local placement, not partition identities or key routing.

## Writer API and batching

Callers submit individual records and receive per-record pending handles.
`send` means local admission; `confirmed` establishes the configured broker
policy. Waiting after every send serializes that caller's writes.

The producer owner restores per-partition sequence order, collects bounded
APPENDs, packs/compresses, retries, and applies confirmation ranges. A full batch
may pipeline behind outstanding APPENDs; a partial successor waits. Intake
reserves one permitted lookahead record so byte-limited batches can become ready.
The SDK caps one APPEND at 2,048 records in addition to negotiated limits.

APPEND collection, replication publication, physical write groups, and OMQ
coalescing are different batches. They add no application transaction. Adaptive
APPEND LZ4 preserves canonical bytes through replication/storage; transport and
segment compression are independent. Full-group reader delivery can reuse that
block, while partial reads decode lazily.

[Runtime](doc/RUNTIME.md#sdk-protocol-batching) owns defaults and memory lifetimes.
[Protocol](doc/PROTOCOL.md#streaming-writer-profile) owns exact confirmation ranges.

## Execution ownership

Each application shard has one OS thread and Tokio current-thread runtime.
The broker defaults to one separate dispatcher and one OMQ I/O worker.
A partition actor is a task, not a thread. SDK owner work also uses a separate
current-thread runtime; callers may use any executor.

Each broker binds control PEER, data PEER, reader PUB, and optional follower PUB
once. Two SDK PEER sockets connect all brokers. Normal follower traffic uses
PUB/SUB; PEER control carries votes/elections and PEER data repairs gaps.
Consumers use PUB/SUB live traffic plus PEER replay and independent control.

Each shard has four OMQ inproc inputs and separate outgoing data/control ports.
Shards visit broker control, client control, broker data, then producer data in
bounded turns. OMQ handles connection fairness inside sockets. Pressure is per
connection, not per partition multiplexed on it.

The actor calls its journal directly. One AIO owner per device handles direct
writes; fixed helpers handle blocking file operations. Pool backends use fixed
workers through the same owned-job contract. File handles survive operations;
physical completion, including canceled work, determines buffer release.
The remaining shared backend handle registry is an explicit implementation
seam, not replicated state or another journal owner.

Use `<broker-root>/topics/<topic>/partitions/<partition>/` for each journal.
Device queues and budgets are shared; logs and durability evidence are not.
See [runtime](doc/RUNTIME.md) for queue layout and [storage](doc/STORAGE.md) for
file publication and restart order.
