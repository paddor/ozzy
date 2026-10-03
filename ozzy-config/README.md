# ozzy-config

Typed deployment TOML and pure validation. No filesystem or runtime dependency.
See [runtime configuration](../doc/RUNTIME.md#deployment-configuration).

- `Deployment::parse(...).validate()` checks names, membership, policies,
  roots, topology and resource budgets. Unknown fields fail, including
  unsupported OMQ affinity settings.
- `broker_plan(name, host)` checks effective CPU/memory restrictions and resolves
  local partition placement. Host information can be a simulator fixture.
- `initialize(identity_source)` creates shared provisioning metadata with
  rotated initial leaders and independent broker principal bindings.
  Generate once, persist, distribute unchanged.
- `check_identity(...)` rejects changed persistent contracts at restart.
  Changing local shard counts does not change partition identities.
- `initialize_broker_identity(...)` provisions local volume/store IDs once.
  `check_broker_identity(...)` validates their shared-group and device bindings.

Topics default to 16 numeric partitions. Keyed placement uses
`XXH3-64(key, topic_seed) % partition_count`. Every broker stores every partition.
Default single-broker fixture: [single.toml](tests/fixtures/single.toml).

This crate does not initialize segment journals or start a broker. The
[`ozzy-broker` startup commands](../ozzy-broker/README.md) own file publication
and effective host discovery. Identity records have an integrity checksum;
decoding alone is not sufficient deployment validation.
