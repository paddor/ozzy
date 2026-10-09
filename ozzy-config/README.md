# ozzy-config

Ozzy is a message streaming system built on OMQ. This crate parses and validates
the shared TOML deployment: brokers, storage, topics, and confirmation policies.
It does not open files or start brokers.

## Examples

| Configuration | Use |
| --- | --- |
| [single.toml](examples/single.toml) | One durable broker on your machine |
| [cluster.toml](examples/cluster.toml) | Three brokers on separate hosts |
| [Local cluster](https://github.com/paddor/ozzy/blob/main/ozzy-broker/three.toml) | Three processes on loopback |

Edit addresses and storage roots before provisioning. Every broker reads the
same deployment and selects its entry with `--broker my_server` (or
`my_server_1` in the cluster example). See
[Getting started](https://github.com/paddor/ozzy/blob/main/GETTING_STARTED.md)
for initialization and startup.

## Names and values

| Setting | Meaning |
| --- | --- |
| `brokers.my_server` | Your broker label, not a hostname lookup |
| `devices.my_ssd` | Your storage label; shard `device` references this name |
| `controller = "my_controller"` | Your broker-local storage worker/budget group |
| `topics.orders` | Your topic name, used by SDKs |
| `root` | Actual local directory holding the broker's data |
| `peer`, `data_peer`, `reader_pub` | Actual bind/connect addresses for control, record data, and live consumer traffic |

Choose broker, device, controller, and topic names before provisioning: 1-128
lowercase ASCII characters, starting with a letter or digit; `-`, `_`, and `.`
are allowed.
The TOML field names and enum values are fixed. CPU/NUMA affinity values are
actual OS IDs. Optional `id` fields are UUID constraints, not these labels;
omit them to generate identities during initialization.

`controller` does not discover hardware or select `/dev/nvme0`. `root` determines
where data lives. Devices with the same controller label within one broker share
a backend, workers, and I/O budgets, and must use identical worker settings.
Use the same label for roots sharing a physical controller; different labels
create separate pools. Labels on different brokers do not share resources.

## Confirmation

The policy is fixed per topic, not chosen per send.

| `confirmation` | `cluster.mode` | When the producer receives confirmation |
| --- | --- | --- |
| `local-durable` | `single` | The sole broker has synchronized the record to disk |
| `disk-quorum` | `three` | The leader and one follower have synchronized matching records to disk |
| `replicated-persisting` | `three` | The leader and one follower retain the records; disk persistence continues in the background |

`single` requires one broker; `three` requires exactly three. Replicated-persisting
can lose a confirmed, unpersisted tail if every volatile copy is lost. Changing
confirmation policy on an existing deployment is rejected at restart.

## Defaults and retention

- 16 partitions per topic, 256 MiB segments, maximum APPEND body 8 MiB.
- One shard on the sole device; multiple devices require explicit shard mappings.
- Linux AIO storage on Linux; `backend = "pool"` selects blocking workers.
- Unlimited retention unless an age or byte target is set:

```toml
[topics.orders.retention]
max_age_secs = 86400
max_bytes = 1073741824
```

Either limit can be omitted. Bytes count segment capacities per partition per
broker, including the active segment; the target must fit at least one segment.
Limits expire oldest sealed segments. Active work and captured reads can delay
deletion. See [storage](https://github.com/paddor/ozzy/blob/main/doc/STORAGE.md#indexes-checkpoints-and-retention).

## Rust API

- `Deployment::parse(text)?.validate()?`: check fields, membership, and budgets.
- `broker_plan(name, host)`: resolve local placement against host resources.
- `initialize(...)` / `check_identity(...)`: provision or verify shared identity.
- `initialize_broker_identity(...)` / `check_broker_identity(...)`: local stores.

Unknown fields fail validation. Generate identities once, keep them with the
data, and distribute the shared identity unchanged. See the
[API reference](https://docs.rs/ozzy-config) for all fields and defaults.
