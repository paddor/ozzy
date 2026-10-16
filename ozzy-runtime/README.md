# ozzy-runtime

Broker actors and SDK internals of [Ozzy](https://github.com/paddor/ozzy) over
OMQ. Applications use the
[`ozzy`](https://github.com/paddor/ozzy/blob/main/ozzy/README.md) SDK, which
reexports `replicated`.

## Owns

- Partition actors: replication, journal adapters, and reader service.
- The broker transport owner: shared PEER sessions, routing, and separate
  bounded data and control dispatch.
- SDK owners: shared broker links, batching, retries, and live PUB/SUB
  delivery with PEER gap repair.

Partition actors own their journals; storage backends own physical I/O. The
replication cores and canonical journal state live in `ozzy-replication` and
`ozzy-journal`.

## Entry points

| Module | Use |
| --- | --- |
| `replicated` | SDK writers, readers, groups, and broker links |
| `frontend` | Broker transport owner, sessions, and routing |
| `replica_actor` | Fixed-voter partition actor over PEER |
| `replica_journal` | Partition journal admission and execution adapters |
| `replica_transport` | Bounded outbound scheduling for one replica actor |
| `dispatch`, `transport` | Local queue classes; OMQ socket limits |
| `memory` | Bounded payload allocation domains |
| `topic_metadata` | Checked topic identity from broker lookups |
| `profiling` | Reexport of `ozzy-telemetry` |

Features: `lz4-transport` and `zstd-transport` (compressed endpoints),
`lz4-storage` (LZ4 segment bodies).

## Docs

- [Runtime](https://github.com/paddor/ozzy/blob/main/doc/RUNTIME.md): owners,
  scheduling, batching, and resource limits.
- [Protocol](https://github.com/paddor/ozzy/blob/main/doc/PROTOCOL.md): native
  messages.
- [Design](https://github.com/paddor/ozzy/blob/main/DESIGN.md): ownership and
  confirmation boundaries.
