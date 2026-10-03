# ozzy-runtime

Embedded broker and native SDK runtime over OMQ. Applications use the
`ozzy` facade to open topic writers and readers through shared broker links.
Applications do not own journals or bind broker sockets.

The runtime holds partition actors, shared PEER sessions, bounded dispatch,
replication orchestration, SDK batching and retries, and reader PUB/SUB
delivery. The broker provisions those actors and connects them to shared
asynchronous storage backends. The fixed-three replication core and canonical
journal state live in separate crates.

Broker integration uses `ReplicaJournal<ShardJournal>`, shared topic SDKs,
and the shared frontend. The old journal worker and actor-owned client
services have been removed. Blocking segment execution remains for older
segment tests and offline operations.

See [design](../DESIGN.md) for ownership and confirmation boundaries,
[runtime](../doc/RUNTIME.md) for scheduling and resource limits, and
[protocol](../doc/PROTOCOL.md) for native messages.
