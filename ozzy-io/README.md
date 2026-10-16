# ozzy-io

Backend-neutral async file operations for
[Ozzy](https://github.com/paddor/ozzy) storage. No file descriptors or
filesystem calls live here. Applications use the
[`ozzy`](https://github.com/paddor/ozzy/blob/main/ozzy/README.md) SDK.

## Owns

- The `Backend` contract: a shard's nonblocking submission lane to a device.
- Device admission: fixed shard shares of operations and bytes, plus reserved
  `Progress` capacity for barriers and recovery.
- Owned operations: an admitted job keeps its buffers and handles until it
  completes, even after its observer is dropped.
- The workspace's async wakeup primitives in `signal`. Other crates use them
  instead of raw Tokio notifications.
- Controlled memory storage for tests (feature `simulation`).

Operations may finish out of order. The journal owner decides which completed
bytes form its durable prefix.

## Entry points

| Item | Use |
| --- | --- |
| `Backend`, `Local` | Submission lane; lane shared by one shard's partitions |
| `Operation`, `Completion`, `Outcome` | Owned job, its observer, and its result |
| `Admission`, `Limits`, `Quota`, `Class` | Device budget and traffic classes |
| `Handle`, `HandleOwner` | Opaque backend-owned file handles |
| `signal::{DataSignal, StateSignal, CloseSignal}` | Coalesced readiness, state changes, closure |
| `simulation::Controller` | Byte-backed backend with explicit scheduling |

## Docs

- [Runtime](https://github.com/paddor/ozzy/blob/main/doc/RUNTIME.md): storage
  owners, queues, and cancellation.
- [Storage](https://github.com/paddor/ozzy/blob/main/doc/STORAGE.md#supported-failure-model):
  supported failure model.
