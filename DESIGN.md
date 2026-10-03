# Ozzy design

Ozzy is an application protocol above ordinary OMQ messages. It adds record
identity, confirmation, retry, replay, reader progress, and persistent storage.
The selected product is broker-only and segment-only. SDK applications connect
to brokers, never own segment journals or bind broker sockets. Keep the broker
runtime embeddable for test harnesses, not as a second producer-owned-log product.

Development uses one broker container with the same protocol and segment engine.
Production currently targets three brokers. Single-broker mode is explicit and
locally durable, with no replication or failover. A group losing members never
downgrades itself to this mode. The broker executable and Linux container use the same runtime
and explicitly initialized stores as embedded deployments.

## Documentation

Read the reference relevant to the change. These describe contracts and how
the system works, not a chronological implementation or benchmark diary.

| Reference | Owns |
| --- | --- |
| [Overview](doc/OVERVIEW.md) | Plain-language concepts and failure boundaries |
| [Protocol](doc/PROTOCOL.md) | Wire bytes, sessions, commands, capability negotiation |
| [Storage](doc/STORAGE.md) | Canonical state, integrity, persistence, recovery, retention |
| [Replication](doc/REPLICATION.md) | Fixed-three authority, votes, leader change, restart and recovery |
| [Runtime](doc/RUNTIME.md) | Delivery/retry, buffer ownership, scheduling, placement |
| [Validation](doc/VALIDATION.md) | Checks, invariants, fault fixtures, measurement discipline |

Codecs and golden fixtures own exact implemented layouts. Reserved commands,
proposed semantics, and available storage primitives do not imply a finished
public API. Plans stay separate from reference documentation. Benchmark charts
and their setup are in [BENCHMARKS.md](BENCHMARKS.md). Runner commands belong in
[ozzy-bench](ozzy-bench/README.md); raw results stay in ignored artifacts.

## Architectural rules

1. ZMTP stays unchanged. Every Ozzy command is an OMQ application message.
2. Correctness does not depend on optional OMQ QoS. Transport receipt is not
   record confirmation, durability, or processing.
3. Replicated groups persist. A group confirms a writer after durability on the
   quorum (disk quorum) or after quorum replication with background persistence
   (replicated-persisting). The policy is explicit and fixed; requests cannot
   silently weaken it.
4. Owner append, reader delivery, and retention are independent contracts.
   Local durability and group replication cover different failures, not ordered
   levels.
5. Each mutable core has one owner on one application shard. A shard may own
   several partition cores. Network, timers, storage completions, and faults
   arrive as explicit events. Count/byte bounds cover all resources. Product
   threads do not share mutable routing, credit, session, storage, or lifecycle
   state behind a `Mutex` or `RwLock`. Bounded queues carry work and capacity
   returns between owners; rare flags and counters may use atomics.
6. Each partition has its own group and journal. Current replicated groups use
   the same three brokers, and leader plus one follower confirms. Development
   uses an explicit single-broker configuration with a local-durable boundary.
   Leadership and recovery are per partition, not per topic or application
   shard. Record offsets and group operation numbers serve different purposes.
7. Leader changes and recovery must preserve confirmed history even when a
   commit announcement or client-success marker is missing. Empty restart is
   nonvoting recovery, never fresh bootstrap.
8. SDK writers use PEER for batched APPENDs, control, and confirmations. No SDK
   PUSH sockets or separate writer-data endpoints. Live reading retains broker
   PUB and SDK SUB; reader replay and control stay on PEER. OMQ owns transport
   fairness and coalescing. See the [protocol implementation status](doc/PROTOCOL.md).
9. Application payload meaning is opaque to brokers. APPEND payload encoding is
   adaptive and internal: the SDK attempts one LZ4 block at 2 KiB, and the leader
   partition actor applies the same fallback to a fresh raw suffix after retry
   reconciliation but before canonical hashing. Packing wins only when the whole
   representation shrinks. Brokers validate LZ4 syntax, exact decoded length, and
   descriptor totals without materializing the payload. Producer-packed blocks
   remain byte-exact through replication, storage, and full-group delivery.
   Partial reads decode lazily. Transport and physical segment compression remain
   independent. Writers behind a compressing transport can turn APPEND packing
   off; payloads then stay plain end to end.
10. At-least-once replay can repeat application effects. Never claim exactly-once
    arbitrary processing.

## Crate boundaries

| Crate | Responsibility |
| --- | --- |
| `ozzy` | Native public API facade |
| `ozzy-broker` | Deployment provisioning and shared broker runtime |
| `ozzy-config` | Typed deployment TOML, persistent identity and broker-local placement validation |
| `ozzy-io` | Owned file operations, opaque handles, asynchronous results and admission contracts |
| `ozzy-io-pool` | Explicit shared file workers, bounded handles and asynchronous drain |
| `ozzy-io-aio` | Backend-owned Linux AIO direct writes with shared pool helpers |
| `ozzy-runtime` | OMQ/async orchestration, partition actors, readers, storage integration |
| `ozzy-proto` | Sans-I/O application protocol and shared message types |
| `ozzy-core` | Deterministic owner, confirmation, retry, canonical state |
| `ozzy-replication` | Sans-I/O fixed-three core, elections and recovery |
| `ozzy-journal` | Synchronous journal/read contracts and canonical operations |
| `ozzy-journal-segment` | Canonical append-only segment engine |
| `ozzy-sim` | Deterministic workloads and fault composition |
| `ozzy-bench` | Runtime, journal, transport, and comparison runners |

Retain canonical journal contracts and fault-injection boundaries while removing
the remaining blocking segment execution. Preserve useful retry/recovery checks against
the segment engine or an independent state oracle. The selected path sends file
work to shared device backends, never application or OMQ I/O threads. One actor
owns each mutable core; transport adapters must not introduce independently
advancing copies of its authority.

## Integration boundaries

Development uses the same broker runtime and native SDK path as the replicated
deployment, with one explicitly local-durable broker.
Writer, reader, and broker traffic share one native protocol and wire version.
Replicated writers use preprovisioned producer sessions. All SDK writers collect
ready records into APPEND requests over PEER, with adaptive LZ4 and no intentional
delay by default. Sparse traffic produces single-record APPENDs; there is no
separate unbatched SDK mode. Brokers collect ready requests and return
independent per-writer confirmation ranges. Retries preserve record identity,
not group boundaries.

One topic-writer handle can route individual records across an immutable,
preprovisioned partition set. Keyed placement is deterministic, keyless traffic
is sticky within an SDK group, and explicit placement stays available. The
chosen partition and its producer sequence stay fixed through retries.
Readers use session-fenced offset subscriptions and explicit count/byte credit.
One reader service owns delivery, cancellation, and transport-buffer reuse;
local and group adapters supply confirmed indexed history. Policy expiration
reports retention gaps instead of blocking writers. Disk-quorum and
replicated-persisting confirmations use distinct evidence and restart rules.
Sharing a frontend does not unify their failure guarantees.

Journal-backed actors can publish live operations to both followers over PUB/SUB.
Reader publication likewise keeps broker PUB and SDK SUB sockets. PEER retains
confirmations, elections, replay, and bounded gap repair. Canonical payload backing
is shared with transport and background persistence.

The journal-backed actor also supports RAM confirmation with bounded background
persistence and explicit nonvoting recovery after an unclean restart. See
[Replication](doc/REPLICATION.md) for loss and restart limits.

Durable inbox/progress, checkpoint transfer, discovery,
consumer groups, online membership, and partition movement require explicit
integration. A reserved opcode, benchmark adapter, or local storage primitive
is not proof that the corresponding public service exists.

For every change, identify its authority, ownership, completion boundary,
resource limits, cancellation behavior, and failure/recovery tests. Optimize
only a measured path without weakening these contracts.

The native writer/reader path, broker sharding, and per-partition groups are
implemented. The dedicated threaded journal has been removed. Remaining work
centers on blocking segment execution, sustained disk behavior, and fault
coverage. Durable
reader state, discovery, groups, reconfiguration, and compatibility facades
need their own contracts and tests.

## Partition placement and leadership

The selected deployment has three brokers. All three store every topic and
partition; a follower may temporarily lag. Each partition has its own group ID,
configuration, leader/view, ordered operation log, and recovery state. No group
spans partitions. Topic metadata binds its immutable partition set, partitioner,
and each partition's group configuration. A topic has no common leader or order.

This describes the replicated deployment. One-broker development keeps the same
partition and journal boundaries without claiming a second copy. Future
six-member groups must remain possible without coupling broker identity to
thread count or SDK connection layout. Quorum and recovery changes require their
own design and fault qualification. See [Replication](doc/REPLICATION.md#future-six-broker-groups).

At partition creation, distribute initial leaders across brokers by choosing each
group's persisted broker order. All members receive the same configuration.
Normal elections determine later leaders; placement hints never grant authority.
This distributes initial leader work, not storage or automatic ongoing load
balancing. Never reorder an existing group on restart to rebalance it.

| Unit | Owns |
| --- | --- |
| Topic | Named partition set and stable SDK partitioning parameters |
| Partition/group | One leader, operation order, journal, confirmation policy, election and recovery; shared record offsets across writers |
| Writer within a partition | Producer epoch, sequence, retry results and bounded credit |
| Application shard | Exclusive execution of assigned partition cores; shared capacity accounting |
| Broker connection | Multiplexed topic/partition sessions, not partition ownership |

A partition belongs to its topic, not to a producer. Many writers can target
the same partition. Their sequences are independent; the leader interleaves
accepted records into partition offsets. Same-key placement gives one partition
order, not a preexisting total order among concurrent independent writers.

Each partition has one application actor per broker, placed independently on
that broker's shards. Shard counts and numbering need not match across brokers.
A shard can lead some partitions and follow others, including within one topic.
Actors share fixed thread pools; partition count does not create threads.
Independent state, timers, active files, and replication windows still cost
resources and need explicit bounds and fair scheduling.

SDKs select partitions and batch/compress per partition, then send each APPEND
to that partition's current leader. Refresh leader authority without changing
the admitted partition or record retry identity. A nonleader returns scoped
authority information; it does not forward writer payloads to another broker.
No mixed-partition APPEND or broker-side key hashing is needed.

This is the selected contract, not completed integration. Static SDK partitioning,
individual group actors, and shared canonical partitions with independent writer
state exist. Streaming replies preserve interleaved retry results. Partition/group binding,
a multiplexed broker frontend, shared SDK connections, and metadata
discovery also remain necessary. See [runtime](doc/RUNTIME.md#placement-and-transport-bounds).

## Writer API and batching

The public writer API accepts individual records and returns per-record
confirmation handles. Bounded typed fanring lanes connect callers to one SDK
owner. That owner restores sequence order, selects and compresses groups, owns
request/retry state, and applies confirmations. OMQ has separate owned I/O threads.
Callers do not construct batches. Admission and confirmation are separate:
awaiting confirmation before submitting another record serializes that caller's
writes.

Transparent SDK protocol batching runs in the writer's
application-side driver, grouping queued records per partition into bounded
APPEND requests sent over PEER. Built-in adaptive LZ4 may compress the concatenated
group payload once on the SDK owner. OMQ continues to own transport framing
and write coalescing; it does not combine Ozzy commands. Broker
storage/replication groups remain independent of SDK batches. The leader may
fuse consecutive ready operations into one replica publication without changing
their operation or SDK-request boundaries. None of these groups defines record
retry identity or adds an application transaction.

For SDK APPEND batching, use zero intentional delay, optional bounded linger, and
an explicit outstanding APPEND request limit and finite producer inboxes.
At most 2,048 records enter one request. Hard packet limits and broker receive
credit still apply.
Partial confirmations do not release an APPEND slot. Never require a full
batch or deliberately couple independent partitions' confirmations. Exact bounds,
ownership, and implementation gates belong in
[Runtime](doc/RUNTIME.md#sdk-protocol-batching) and
[Protocol](doc/PROTOCOL.md#sdk-protocol-batching).

## Execution ownership

`ShardedNode` creates one OS thread and Tokio `current_thread` runtime per
application shard. Replicated benchmarks use the same runtime flavor.
`ScheduledReplica` receives dispatcher-delivered OMQ messages and has no socket.
`WriterRuntime` owns one current-thread SDK runtime for batching, compression,
request state, and confirmations. Producer
futures work with any executor. An actor is a task owning state, not another thread.

Bind the broker PEER endpoint once and each PUB endpoint once. OMQ control
accepts connections and distributes them over owned I/O workers. Start with one
I/O worker and one separate Ozzy dispatcher thread per broker. Add I/O workers
when measured load warrants them. OMQ NUMA placement and dispatch colocated with
each I/O worker are deferred until supported by OMQ. Application actors never
run on transport or dispatch threads; all
application shards are symmetric and shard 0 has no extra transport duties.
SDK OMQ remains separate from SDK compression.

Each partition actor owns its replication and journal state on the same shard.
It handles election, recovery/repair decisions, writer retries, validation,
offsets, storage grouping, and segment administration. Keep these as separate
modules without a separate journal-owner thread. CPU work runs in bounded turns.

All file access uses backend-neutral asynchronous jobs. The segment engine
defines layout and durability ordering, not the kernel I/O mechanism. A Linux
AIO backend crate owns submission, completions, handles, and bounded blocking
helpers. A pool implementation and a future opt-in io_uring crate use the same
contract. Application shards neither call file syscalls nor drive AIO eventfds.
Pending I/O suspends dependent work, never the shard's other actors and timers.
Backend workers hold no replication authority or independently advancing journal
state. Only the partition actor installs generation-matched results.

Use `<broker-root>/topics/<topic>/partitions/<partition>/` for segments and
partition-local evidence.
Share backend execution and admission budgets by device, not per partition.
Separate journals do not imply separate disks or one sync covering every file.
Buffers stay owned until physical work settles, including canceled waits.
Shutdown drains asynchronously. See [async storage](doc/RUNTIME.md#async-storage-target).

Partition actors use the asynchronous journal and shared device backends.
Blocking segment execution remains for offline work and older segment tests.

The dispatcher consumes ordinary PEER messages and forwards them through one
typed fanring per destination shard, initially with one producer. Connections
can target any partition; accept-time placement need not predict destinations.
Route directly to the owning shard, including across NUMA groups. Replies and
buffer reclamation have bounded return paths. No application-shard receive
lanes or new OMQ placement hooks are required. Future per-I/O dispatch adds
producer lanes without changing partition ownership or multiplying credit.

Dispatch never awaits one full shard queue. Shards grant lane-backed count/byte
credit; dispatch enforces it. Nonblocking admission and reserved control capacity
preserve isolation. Additional I/O workers do not multiply a shard's budget.
The replacement credit path must keep capacity on the shard and spent grant
balances on the dispatcher. A bounded shard-to-dispatcher fanring carries
installation, batched replenishment, and revocation. Queue-slot capacity returns
after dequeue; retained-byte capacity returns after the last payload reference
is released. Revocation acknowledges unused credit before the shard reuses it.
No cross-thread shared mutable credit table or mutex belongs on this path.
The separate lanes are Ozzy's dispatcher-to-shard queues, not an OMQ PEER API.
Qualify the replacement frontend under saturation and failure before using it;
this is an implementation gate, not a requirement to retain OMQ receive lanes.
See [dispatch ownership](doc/RUNTIME.md#placement-and-transport-bounds).

Local partition state stays on its owner; network buffers may cross NUMA groups.
Queue batching reduces coordination, not remote-memory cost. Measure worker
saturation, cross-NUMA bytes, and partition fairness before claiming scaling.

See [thread map and scenario diagrams](doc/RUNTIME.md#omq-ownership-and-scheduling)
and [worker bounds](doc/RUNTIME.md#disk-workers).
