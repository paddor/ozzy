# ozzy-io-aio

Linux native AIO storage backend for [Ozzy](https://github.com/paddor/ozzy):
kernel AIO for data writes, fixed blocking helpers for other file operations.
Builds only on Linux. Applications use the
[`ozzy`](https://github.com/paddor/ozzy/blob/main/ozzy/README.md) SDK.

## Owns

- Kernel AIO contexts, eventfds, descriptors, and completions, all on backend
  threads.
- Direct writes up to a configured depth, plus one reserved slot for
  progress writes.
- Device admission and handle bounds, shared with its `ozzy-io-pool` helpers.

Setup failure fences admission and fails queued work. The backend never falls
back to the pool on its own.

## Entry points

| Item | Use |
| --- | --- |
| `Aio::new(config)` | Start the backend; returns one `Client` lane per shard |
| `Aio::with_initializer` | Apply thread placement before setup |
| `Aio::shutdown`, `Aio::join` | Drain, destroy kernel contexts, close, then wait for the threads |
| `Config` | Helper pool bounds (`PoolConfig`) and direct-write `depth` |
| `kernel` | Owning-thread AIO context primitive |

## Docs

- [Runtime](https://github.com/paddor/ozzy/blob/main/doc/RUNTIME.md): storage
  owners and cancellation.
- [Operator tuning](https://github.com/paddor/ozzy/blob/main/doc/STORAGE.md#operator-tuning):
  device depth.
