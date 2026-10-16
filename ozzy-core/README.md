# ozzy-core

Deterministic partition state for [Ozzy](https://github.com/paddor/ozzy):
record order, retry identity, reader progress, and live visibility. No OMQ,
Tokio, or file I/O. Applications use the
[`ozzy`](https://github.com/paddor/ozzy/blob/main/ozzy/README.md) SDK.

## Owns

- Canonical state transitions, shared by speculative and confirmed images.
- Producer sessions and retry results, scoped per partition.
- The deduplication index: first-wins message IDs within the window.
- Consumer-group coordinator state and assignment targets.
- Reader scheduling, history seek, and live-stream repair pacing.
- Whole-segment retention plans and state checkpoint images.

## Entry points

| Module | Use |
| --- | --- |
| `state` | `CanonicalState`, `CanonicalImages`: canonical operation transitions |
| `dedup` | `Index`: partition-local event admission |
| `reader` | `ReadScheduler` and `seek`: one subscription's reads |
| `live` | `LiveProgress`, `LiveCursor`: live stream beside ordered replay |
| `retention` | `Plan`: whole-segment retention decisions |
| `checkpoint` | Checkpoint image layout and `Decoder` |

Three-broker authority lives in `ozzy-replication`. Canonical operation codecs
and durability evidence live in `ozzy-journal`.

## Docs

- [Storage](https://github.com/paddor/ozzy/blob/main/doc/STORAGE.md): record
  identity, retention, and recovery.
- [Design](https://github.com/paddor/ozzy/blob/main/DESIGN.md): component
  boundaries.
