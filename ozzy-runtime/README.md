# ozzy-runtime

Embedded broker and native SDK runtime over OMQ. Applications use the
`ozzy` facade to open topic writers and readers through shared broker links.
Applications do not own journals or bind broker sockets.

The runtime holds partition actors, shared PEER sessions, bounded dispatch,
replication orchestration, SDK batching and retries, and reader PUB/SUB
delivery. The broker provisions those actors and connects them to shared
asynchronous storage backends. The fixed-three replication core and canonical
journal state live in separate crates.

Broker dispatch uses separate bounded data/control OMQ inproc lanes.
Partition actors own journals directly; storage backends own physical I/O.

See [design](https://github.com/paddor/ozzy/blob/main/DESIGN.md) for ownership and confirmation boundaries,
[runtime](https://github.com/paddor/ozzy/blob/main/doc/RUNTIME.md) for scheduling and resource limits, and
[protocol](https://github.com/paddor/ozzy/blob/main/doc/PROTOCOL.md) for native messages.
