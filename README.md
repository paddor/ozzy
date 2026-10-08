<p align="center">
  <img src="doc/ozzy-logo.webp" alt="Ozzy the ocelot" width="256" />
</p>

# Ozzy

Ozzy is a message streaming system written in Rust, built for high throughput
and durable event logs. It runs as a single broker or a replicated cluster,
with [OMQ](https://github.com/paddor/omq.rs) handling message passing.

Run a broker with the [getting started guide](GETTING_STARTED.md), then connect
applications using the [Rust SDK](ozzy/README.md). Requires Linux and Rust 1.93.

## Goals

- Keep message streaming fast and durable.
- Aim for very low latency, and keep it steady across workloads.
- Let OMQ handle the networking.
- Natural batching: no artificial delays; better efficiency under load.
- Backpressure all the way through queues; no credits or grants.
- Run on one broker or as a three-broker cluster.
- Run in ordinary Linux containers with the default security profile.
- Use OMQ's inproc transport to run the real broker sans I/O in a simulator.

## Broker modes

| Mode | Write confirmation | Brokers |
| --- | --- | --- |
| Single durable | Persisted locally | One; no failover |
| Disk quorum | Persisted on the leader and one follower | Three; two eligible copies required |
| Replicated-persisting | Retained on the leader and one follower; persisted in the background | Three; two eligible copies required |

## Workspace

| Crate | Responsibility |
| --- | --- |
| [`ozzy`](ozzy/) | Public producer and consumer SDKs |
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
| [`ozzy-sim`](ozzy-sim/) | Controlled simulation and full-product workload/fault runs |
| [`ozzy-bench`](ozzy-bench/) | Benchmarks, comparisons, profiles, and SVG charts |

The simulator and benchmark tools are development crates, not published packages.

## Further reading

- [Getting started](GETTING_STARTED.md): build, provision, run, containers, recovery.
- [Rust SDK](ozzy/README.md): producing, resume, consuming, checkpoints.
- [Overview](doc/OVERVIEW.md): terms and producer/consumer sequence diagrams.
- [Architecture](DESIGN.md): component boundaries and reference map.
- [Protocol](doc/PROTOCOL.md): wire messages, sessions, commands, errors.
- [Runtime](doc/RUNTIME.md): owners, scheduling, batching, backpressure.
- [Storage](doc/STORAGE.md): segments, integrity, retention, restart.
- [Replication](doc/REPLICATION.md): authority, confirmation, elections, repair.
- [Development](DEVELOPMENT.md) and [contributing](CONTRIBUTING.md): checks and test layers.
- [Benchmarks](BENCHMARKS.md): six charts and their measurement conditions.

## License

[ISC](LICENSE).
