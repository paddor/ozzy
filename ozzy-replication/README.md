# ozzy-replication

Sans-I/O replication and recovery cores for
[Ozzy](https://github.com/paddor/ozzy) partitions. Runtime, transport, and file
execution live outside this crate. Applications use the
[`ozzy`](https://github.com/paddor/ozzy/blob/main/ozzy/README.md) SDK.

## Owns

- Fixed three-broker groups for disk quorum and replicated-persisting:
  ordered operations, digest chains, votes, commit, and application.
- Elections: contact deadlines, durable promises, and view installation.
- Bounded retention, volatile receipts, probes, and gap repair.
- Recovery of a broker that lost its store, from two other brokers.
- Single durable authority with no quorum or election.
- Native replica message codecs.

## Entry points

| Item | Use |
| --- | --- |
| `NormalReplica` | Normal-operation core of one group |
| `driver::ReplicaDriver` | Normal and election scheduling |
| `flow::{Sender, Receiver, Transmitter, ProbeScheduler}` | Retention, receipts, probes, and repair |
| `recovery::Recovery` | Lost-store recovery admission |
| `local::Driver` | Single durable authority |
| `wire` | Replica message codecs |
| `Configuration`, `QuorumPolicy` | Group membership and confirmation policy |

The broker adapter in `ozzy-runtime` (`replica_actor`) owns the sockets and
partition journals.

## Docs

- [Replication](https://github.com/paddor/ozzy/blob/main/doc/REPLICATION.md):
  authority, voting, elections, recovery, and limits.
- [Protocol](https://github.com/paddor/ozzy/blob/main/doc/PROTOCOL.md): replica
  messages.
