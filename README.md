<p align="center">
  <img src="doc/ozzy-logo.webp" alt="Ozzy the ocelot" width="256" />
</p>

# Ozzy

Ozzy is a message streaming system in Rust for durable event logs. Brokers own
the log; [OMQ](https://github.com/paddor/omq.rs) carries the messages. Run one
broker or a three-broker cluster.

Requires Linux and Rust 1.93. [Try it](GETTING_STARTED.md#try-it) in three
commands: a broker, the admin console, and a demo writer and reader. Then
connect applications with the [Rust SDK](ozzy/README.md) or
[Python SDK](bindings/python/README.md).

## Goals

- Fast, durable streaming with very low, steady latency across workloads.
- OMQ handles the networking.
- Natural batching: no artificial delay; larger batches under load.
- Backpressure through every queue; no credits or grants.
- One broker, or a three-broker cluster.
- Runs in ordinary Linux containers with the default security profile.
- Tests run the real broker in a simulator over OMQ inproc and memory storage.

## Broker modes

| Mode | A write is confirmed when | Brokers |
| --- | --- | --- |
| Single durable | It is persisted locally | One; no failover |
| Disk quorum | It is persisted on the leader and one follower | Three; two eligible copies required |

The [overview](doc/OVERVIEW.md#modes) defines each boundary and its config
value.

## Features

- Partitioned topics shared by many producers. A retried record returns its
  original offset.
- Producer resume and takeover across restarts.
- Optional per-topic deduplication: within a configured window, the first record
  with a message ID is stored, and repeats confirm with its original offset.
- Consumer groups with broker-stored progress and automatic partition
  assignment.
- Readers start at the earliest or latest record, an offset, a broker timestamp,
  or a record ID, and repair live-stream gaps on their own.
- Optional per-topic retention by age or size, in whole segments.
- Optional aggregate ingress and egress network byte limits per broker.
- Optional `lz4+tcp://` and `zstd+tcp://` endpoints (Cargo features
  `lz4-transport` and `zstd-transport`).

## Workspace

| Crate | Responsibility |
| --- | --- |
| [`ozzy`](ozzy/) | Public producer and consumer SDKs |
| [`ozzy-python`](bindings/python/) | Blocking and asyncio Python bindings to the Rust SDK |
| [`ozzy-broker`](ozzy-broker/) | Server, provisioning, and embedded broker API |
| [`ozzy-config`](ozzy-config/) | Deployment configuration, identities, and placement |
| [`ozzy-runtime`](ozzy-runtime/) | Broker actors, SDK owners, OMQ sessions, and readers |
| [`ozzy-proto`](ozzy-proto/) | Sans-I/O wire codecs and protocol types |
| [`ozzy-core`](ozzy-core/) | Record order, retry identity, and confirmation state |
| [`ozzy-replication`](ozzy-replication/) | Elections, voting, and replica recovery |
| [`ozzy-journal`](ozzy-journal/) | Journal, read, and integrity contracts |
| [`ozzy-journal-segment`](ozzy-journal-segment/) | Segment encoding, reads, writes, and recovery |
| [`ozzy-io`](ozzy-io/) | File jobs, handles, completion, and controlled memory storage |
| [`ozzy-io-pool`](ozzy-io-pool/) | Bounded blocking storage workers |
| [`ozzy-io-aio`](ozzy-io-aio/) | Linux AIO storage backend |
| [`ozzy-telemetry`](ozzy-telemetry/) | Optional thread-owned IPC telemetry and bounded diagnostic logs |
| [`ozzy-ui`](ozzy-ui/) | Terminal admin console of one broker |
| [`ozzy-sim`](ozzy-sim/) | Controlled simulation and full-product workload/fault runs |
| [`ozzy-bench`](ozzy-bench/) | Benchmarks, comparisons, profiles, and SVG charts |
| [`ozzy-test-alloc`](ozzy-test-alloc/) | Allocation counting for allocation budget tests |

The simulator, benchmark, and allocation test crates are for development only
and are not published.

## Further reading

| Document | Covers |
| --- | --- |
| [Getting started](GETTING_STARTED.md) | Build, provision, run, containers, recovery |
| [Rust SDK](ozzy/README.md) | Producing, resume, deduplication, consuming, checkpoints, consumer groups |
| [Overview](doc/OVERVIEW.md) | Terms, modes, placement, record flow |
| [Architecture](DESIGN.md) | Component boundaries and reference map |
| [Protocol](doc/PROTOCOL.md) | Wire messages, sessions, commands, errors |
| [Runtime](doc/RUNTIME.md) | Owners, scheduling, batching, backpressure, deployment config |
| [Storage](doc/STORAGE.md) | Segments, integrity, retention, restart |
| [Replication](doc/REPLICATION.md) | Authority, confirmation, elections, repair |
| [Development](DEVELOPMENT.md), [contributing](CONTRIBUTING.md) | Checks and test layers |
| [Benchmarks](BENCHMARKS.md) | The four charts and their measurement conditions |

## License

[ISC](LICENSE).
