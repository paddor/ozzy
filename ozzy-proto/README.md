# ozzy-proto

Sans-I/O wire codecs and shared message types for
[Ozzy](https://github.com/paddor/ozzy). Applications use the
[`ozzy`](https://github.com/paddor/ozzy/blob/main/ozzy/README.md) SDK.

## Owns

- The 64-byte envelope (magic `OZY\0`, wire version 1) and the three-frame
  packet: envelope, metadata, payload.
- HELLO/WELCOME negotiation of roles, capabilities, and receive limits.
- Command schemas for appends, producer sessions, readers, processing
  progress, consumer groups, and routing interests.
- Negative responses with typed retry classes. Unknown codes and details stay
  readable.
- Shared identifiers (`NodeId`, `TopicId`, `MessageId`, ...) and `Offset`.

Decoding checks structure only. It never establishes authentication,
ownership, persistence, or confirmation.

## Entry points

| Item | Use |
| --- | --- |
| `Envelope`, `decode_packet`, `ENVELOPE_BYTES`, `VERSION` | Envelope and packet framing |
| `handshake` | Negotiation profiles and capability bits |
| `append`, `producer` | Producer requests, receipts, and sessions |
| `reader`, `progress`, `group` | Subscriptions, processing cursors, consumer groups |
| `directory` | Routing interests and endpoint schemes |
| `data` | Bounded multipart records shared by data commands |
| `nack` | Negative responses and retry classes |
| `layout` | Bounds-checked big-endian fields |

Replica message codecs live in `ozzy-replication` (`wire`).

## Docs

- [Protocol](https://github.com/paddor/ozzy/blob/main/doc/PROTOCOL.md): exact
  bytes, commands, sessions, and errors.
