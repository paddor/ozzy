# ozzy-journal-segment

Canonical append-only segment engine for Ozzy.

Broker partition actors use `AsyncGroupJournal`, `AsyncRecoveryDirectory`,
and the asynchronous partition reader. File operations run through shared
device backends. The actor owns journal state and installs completed results;
the backend owns handles, buffers, and physical I/O. See
[storage](../doc/STORAGE.md) and [runtime](../doc/RUNTIME.md).

The segment engine checks physical framing, canonical operations, chain
digests, manifests, checkpoints, and recovery evidence. Active writes use
explicit durable or buffered policy. A writer reply follows the configured
confirmation boundary, never a transport receipt.

`GroupDirectory` and `SegmentWriter` remain for offline volume and
relocation work and older segment tests. Broker startup uses async backends. They are
not a second broker execution path. The old blocking store wrapper, detached
sealed-read cache, and packed-output buffer have been removed.
