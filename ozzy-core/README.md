# ozzy-core

Deterministic canonical state, reader progress, and live visibility. No OMQ,
Tokio, or file I/O is required. `ozzy-replication` owns three-broker authority;
`ozzy-journal` owns canonical operation and durability evidence.

The same state transitions serve speculative and confirmed images. Writer
identity and retry results are scoped within each partition. See
[design](../DESIGN.md) and [storage](../doc/STORAGE.md).
