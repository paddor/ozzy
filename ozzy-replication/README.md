# ozzy-replication

Sans-I/O replication and recovery cores. Fixed three-broker groups support disk
quorum and replicated-persisting; `local` supplies explicit single-broker durable
authority. Runtime, transport and physical file execution live outside this crate.

| Module | Owns |
| --- | --- |
| Normal core | Ordered canonical operations, digest chains, eligible votes, commit and application |
| `driver` | Normal/election scheduling, contact deadlines, durable promises and view installation |
| `flow` | Bounded local retention, volatile receipts, correlated probes and gap repair |
| `recovery` | Fresh two-peer authority, frozen donor prefixes and nonvoting lost-store recovery |
| `local` | Local written/durable/applied frontiers with no quorum or election |
| `wire` | Bounded native replica codecs and borrowed operation bodies |

Disk quorum requires exact durable copies on two brokers. Replicated-persisting
requires validated retained copies on two brokers while bounded persistence drains.
Receipts/probes schedule repair; they are neither votes nor remote grants. A full
local backlog creates pressure without changing the confirmation policy.

The broker adapter owns PUB/SUB replication and PEER control/repair sockets.
It composes the cores with partition-owned async journals and checkpoint
transfer. Membership changes and online partition movement are not implemented.
See [replication](https://github.com/paddor/ozzy/blob/main/doc/REPLICATION.md) for authority, election, and recovery
contracts, and [protocol](https://github.com/paddor/ozzy/blob/main/doc/PROTOCOL.md) for messages.
