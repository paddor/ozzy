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

`ConfigurationRecord` binds ordered broker identities/principals, confirmation
policy, protocol/schema and integrity profile. Journal adapters persist its exact
bytes. Transport identity binding must precede decoding and authority checks.

Disk quorum requires exact durable copies on two brokers. Replicated-persisting
requires validated retained copies on two brokers while bounded persistence drains.
Receipts/probes schedule repair; they are neither votes nor remote grants. A full
local backlog creates pressure without changing the confirmation policy.

Normal operation uses PUB/SUB in the broker adapter. Votes, elections and probes
use control PEER; targeted history uses data PEER. The core does not own sockets.
Generation-scoped completions and captured validation tickets reject stale work.

A new view requires durable promises, distinct election evidence, protected
history selection, complete publication and rebuilt application state. A routing
hint never grants authority. Intact restart fences the old view; lost-store
recovery stays nonvoting until fresh authority, bounded transfer, durable
publication and the existing election handoff complete.

The selected broker composes these cores with partition-owned async journals.
Legacy `Node` replication, checkpoint transfer, membership changes and partition
movement remain separate work. See [replication](../doc/REPLICATION.md),
[protocol](../doc/PROTOCOL.md) and [test layers](../DEVELOPMENT.md#test-layers).

```sh
scripts/test.sh core
scripts/test.sh replication
scripts/test.sh simulation
```
