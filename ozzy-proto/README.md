# ozzy-proto

Sans-I/O Ozzy protocol. One 64-byte envelope, three application frames, wire
version 1. Routine REPLICA_RECEIPT has a compact single-frame profile. No legacy
decoder or version-named module tree.

`handshake` owns negotiation; `append` and `reader` own command schemas.
`data` shares bounded multipart encoding and borrowed/owned record iterators.
`nack` preserves typed retry classes and unknown error details.
Shared identifiers and framing are exported at the crate root.

Fixed-byte, malformed-input, capacity, and session tests protect these contracts.
Decoding never establishes authentication, ownership, persistence, or group
confirmation. See [protocol](https://github.com/paddor/ozzy/blob/main/doc/PROTOCOL.md).
