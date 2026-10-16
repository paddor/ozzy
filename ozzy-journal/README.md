# ozzy-journal

Journal contracts for [Ozzy](https://github.com/paddor/ozzy): canonical
operation codecs, durability progress, integrity hashes, and read limits. No
file handles or threads. Applications use the
[`ozzy`](https://github.com/paddor/ozzy/blob/main/ozzy/README.md) SDK.

## Owns

- The canonical operation format shared by storage, replication, and state.
- Write and sync progress per journal generation. Tickets record local
  actions, not quorum evidence.
- The integrity profile: XXH3-128 in 32-byte digest slots. It detects errors;
  it does not authenticate.
- Bounded metadata work helpers.

## Entry points

| Item | Use |
| --- | --- |
| `operation` | `CanonicalOperation`, append encoders, record views, digests |
| `progress` | `JournalProgress`, `WriteTicket`, `SyncTicket` |
| `integrity` | `hash`, `IntegrityHasher`, `PROFILE` |
| `work` | Bounded metadata computation |
| `ReadLimits` | Record and byte bounds for one read |

`ozzy-journal-segment` implements storage on these contracts.

## Docs

- [Storage](https://github.com/paddor/ozzy/blob/main/doc/STORAGE.md): record
  identity, segment layout, and failure boundaries.
