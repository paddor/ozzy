# ozzy-journal

Canonical operation codecs, progress and integrity contracts, read limits,
and bounded work helpers. The code owns no file handles or execution threads.
The segment engine implements storage; application and replication cores use
the same canonical operation format and durability evidence.

See [storage](https://github.com/paddor/ozzy/blob/main/doc/STORAGE.md) for segment layout and failure boundaries.
