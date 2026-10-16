# ozzy-broker

The `ozzy` broker binary and its embedded Rust API. Part of
[Ozzy](https://github.com/paddor/ozzy), a broker-owned, segment-only event log
over OMQ. Install with `cargo install ozzy-broker`.

## Owns

- Provisioning: shared and local identities, volume markers, journal formats.
- Startup checks of deployment, identities, host placement, and storage.
- Serving: the transport owner, application shards with their partition
  actors, and storage device pools.
- Explicit recovery of replicated partitions.

## Commands

Every command takes `--config FILE`. `ozzy --help` lists all flags.

| Command | Does |
| --- | --- |
| `validate` | Check the deployment and this host's placement; writes nothing |
| `init` | Create the shared deployment identity once |
| `init-broker` | Create this broker's local store bindings once |
| `init-volumes` | Bind existing device directories to this broker's volumes |
| `format` | Initialize absent segment journals |
| `check` | Check existing identity and local topology; creates nothing |
| `serve` | Serve established journals; requires `--trusted-transport` |
| `recover` | Serve with explicit nonvoting recovery of selected partitions |

Features `lz4-transport` and `zstd-transport` serve `lz4+tcp://` and
`zstd+tcp://` endpoints.

## Entry points

| Item | Use |
| --- | --- |
| `load_deployment`, `check_config` | Read the TOML; check it against identity and host |
| `initialize_identity`, `initialize_broker_identity` | Provision identities once |
| `initialize_volumes`, `format_partition_journals` | Prepare device roots and journals |
| `load_broker_identity` | Load this broker's store bindings |
| `Broker::start_trusted` | Serve established stores |
| `Broker::start_trusted_with_context` | Share an OMQ context with inproc SDKs |
| `Broker::start_recovering_trusted` | Serve with `RecoverySelection`s |
| `Broker::ready`, `shutdown`, `closed` | Wait for serving, drain, or observe failure |

## Docs

- [Getting started](https://github.com/paddor/ozzy/blob/main/GETTING_STARTED.md):
  build, provision, run, containers, and recovery.
- [Project README](https://github.com/paddor/ozzy/blob/main/README.md): broker
  modes and features.
- [Configuration](https://github.com/paddor/ozzy/blob/main/ozzy-config/README.md):
  deployment keys.
- [Design](https://github.com/paddor/ozzy/blob/main/DESIGN.md): ownership and
  durability boundaries.
