# ozzy-config

Parses and validates the shared deployment TOML of
[Ozzy](https://github.com/paddor/ozzy): brokers, storage, topics, and
confirmation policies. It opens no files and starts no brokers.

## Owns

- The TOML schema. Unknown fields fail parsing.
- Deployment-wide validation: membership, names, endpoints, budgets.
- One broker's local placement against its host CPUs and NUMA nodes.
- Shared and broker-local identity records: created once, checked on restart.

## Entry points

| Item | Use |
| --- | --- |
| `Deployment::parse(text)?.validate()?` | Parse and check a deployment |
| `ValidatedDeployment::broker_plan` | Resolve one broker's placement on its host |
| `initialize` / `check_identity` | Create or verify the shared `DeploymentIdentity` |
| `initialize_broker_identity` / `check_broker_identity` | Create or verify one broker's `BrokerIdentity` |
| `encode` / `decode` on both identities | Checksummed TOML identity records |

Generate identities once, keep them with the data, and give every broker the
same shared identity. `ozzy --config FILE validate --broker NAME` runs the
checks from the shell.

## Examples

| File | Use |
| --- | --- |
| [`single.toml`](https://github.com/paddor/ozzy/blob/main/ozzy-config/examples/single.toml) | One durable broker |
| [`cluster.toml`](https://github.com/paddor/ozzy/blob/main/ozzy-config/examples/cluster.toml) | Three brokers on separate hosts |
| [`three.toml`](https://github.com/paddor/ozzy/blob/main/ozzy-broker/three.toml) | Three processes on loopback |

Every broker reads the same file and selects its entry with `--broker NAME`.

## Names

Table keys like `my_server`, `my_ssd`, `my_controller`, and `orders` are your
labels. Field names and enum values are fixed.

- Broker, device, controller, and topic names: 1-128 bytes of `a-z`, `0-9`,
  `-`, `_`, `.`, starting with a letter or digit.
- Endpoints are real addresses: the broker binds them and clients connect.
- `root` is the real local data directory: absolute, normalized, and not
  overlapping another device root.
- `controller` names a broker-local group of storage workers and budgets. It
  discovers no hardware. Devices sharing it must have identical `workers`.
- Optional `id` keys pin the UUIDs that initialization would generate. Omit
  them.
- CPU and NUMA values are OS IDs.

## Keys

| Table | Keys (default) |
| --- | --- |
| `[cluster]` | `mode`: `"single"` or `"three"` |
| `[limits]` | `max_topics` (1024), `max_partitions` (65536), `max_clients` (128, at most 4096) |
| `[brokers.NAME.endpoints]` | `peer` (control), `data_peer` (APPEND, replay), `reader_pub` (live records); optional `follower_pub`, `admin` |
| `[brokers.NAME.devices.DEVICE]` | `root`, `controller` |
| `[brokers.NAME.devices.DEVICE.workers]` | `backend` (`"aio"` on Linux, else `"pool"`), worker, CPU, job, byte, and handle bounds |
| `[brokers.NAME.topology]` | `omq.io_threads` (1), `dispatcher`, `shards`, `memory_pools`, `partitions` |
| `[topics.NAME]` | `partitions` (16), `partitioner_seed` (0), `segment_bytes` (256 MiB), `max_append_bytes` (8 MiB) |
| `[topics.NAME.retention]` | `max_age_secs`, `max_bytes` (unset: keep all) |
| `[topics.NAME.deduplication]` | `window_millis` (300000), `max_entries`, `max_bytes` |

The rustdoc of each field in `src/schema.rs` lists the rest, including
`workers` and shard `budget` defaults.

## Confirmation

`cluster.mode` fixes confirmation for every topic; a send cannot choose
another policy. `single` needs exactly one broker and confirms local durable.
`three` needs exactly three brokers and confirms by disk quorum. The
[overview](https://github.com/paddor/ozzy/blob/main/doc/OVERVIEW.md#modes)
says what each mode confirms.

## Validation rules

- Every endpoint is unique in the deployment. TCP endpoints need a fixed
  nonzero port and a connectable address.
- `admin` must be `ipc://` or `inproc://`. It has no authentication: anyone
  who can connect is an admin. An `ipc://@name` endpoint is in the Linux
  abstract namespace, open to every local user; a path uses file permissions.
- One device gets one shard by default. Several devices need explicit `shards`.
- `segment_bytes` is at least 1 MiB, a multiple of 4096, and at least twice
  `max_append_bytes`. `max_append_bytes` is at most 8 MiB.
- Shard and device byte budgets must cover the largest `max_append_bytes`.
- Restart rejects changed mode, brokers, topics, partition counts,
  `partitioner_seed`, or deduplication window. Local shards,
  CPUs, and worker budgets may change.

## Retention

```toml
[topics.orders.retention]
max_age_secs = 86400
max_bytes = 1073741824
```

- Either limit expires the oldest sealed segments; both can be omitted.
- `max_bytes` counts segment capacities per partition per broker, including the
  active segment. It must fit at least one segment. A retired file kept for
  the next new segment adds one segment on disk.
- Active work and captured reads can delay deletion.

## Deduplication

```toml
[topics.orders.deduplication]
window_millis = 300000
max_entries = 1000000
max_bytes = 268435456
```

- Off unless the topic sets it. Whether it is on, and its window, are fixed
  at initialization.
- `max_entries` and `max_bytes` bound each partition's identity index;
  `max_bytes` must allow 32 bytes per entry.
- A full index makes producers retry later. It never evicts a live identity.

## Docs

- [Getting started](https://github.com/paddor/ozzy/blob/main/GETTING_STARTED.md):
  provisioning and startup commands.
- [Deployment configuration](https://github.com/paddor/ozzy/blob/main/doc/RUNTIME.md#deployment-configuration):
  endpoint schemes, compression, and `max_clients`.
- [Operator tuning](https://github.com/paddor/ozzy/blob/main/doc/STORAGE.md#operator-tuning):
  storage worker bounds.
- [Retention](https://github.com/paddor/ozzy/blob/main/doc/STORAGE.md#indexes-checkpoints-and-retention):
  when segments are deleted.
