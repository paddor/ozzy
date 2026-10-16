# ozzy-io-pool

Fixed blocking file workers for [Ozzy](https://github.com/paddor/ozzy) storage
devices, implementing the `ozzy-io` backend contract. Applications use the
[`ozzy`](https://github.com/paddor/ozzy/blob/main/ozzy/README.md) SDK.

## Owns

- Worker threads, file handles, and physical jobs for one device or
  controller.
- One bounded submission lane per application shard.
- Reserved capacity and workers for progress jobs.
- Shutdown: finish admitted work and close every handle on the workers.

## Entry points

| Item | Use |
| --- | --- |
| `Pool::new(config)` | Start workers; returns one `Client` lane per shard |
| `Pool::with_initializer` | Apply thread placement before workers run jobs |
| `Pool::shutdown`, `Pool::join` | Drain and close, then wait for the threads |
| `Config` | Thread, in-flight job, handle, and admission bounds |
| `direct` | Extension point for a direct-write driver, used by `ozzy-io-aio` |

## Docs

- [Runtime](https://github.com/paddor/ozzy/blob/main/doc/RUNTIME.md): storage
  owners and local ring exceptions.
- [Operator tuning](https://github.com/paddor/ozzy/blob/main/doc/STORAGE.md#operator-tuning):
  device bounds.
