# Broker startup

Provisioning commands initialize persistent identity, volumes, and segment
journals explicitly. `serve` opens established stores and binds configured
endpoints.

For three brokers on one development host, use [three.toml](three.toml). It
configures six partitions, two application shards per broker, one dispatcher
per broker, and distinct PEER/PUB ports. Edit its absolute storage roots and
ports before initialization if needed. Pass the same TOML and shared identity
to all three processes; select each process with `--broker broker-0`,
`broker-1`, or `broker-2`. Initialize once, then run `init-broker`,
`init-volumes`, and `format` for each broker before `serve`.

With the sample paths, provision from the repository root:

```sh
bin=/mnt/ssd/tmp/cargo-target/debug/ozy_broker
config=ozzy-broker/three.toml
root=/mnt/ssd/tmp/ozzy-three
mkdir -p "$root"/broker-{0,1,2}
"$bin" --config "$config" init --identity "$root/shared.identity"
for n in 0 1 2; do
  name="broker-$n"
  for step in init-broker init-volumes format; do
    "$bin" --config "$config" "$step" --broker "$name" \
      --identity "$root/shared.identity" --local-identity "$root/$name.identity"
  done
done
```

Start one `serve` process per broker with the same `--config` and shared
`--identity`, its own `--broker` and `--local-identity`, and
`--trusted-transport` in an explicitly trusted environment.

```sh
cargo run -p ozzy-broker -- --config deployment.toml validate --broker laptop
cargo run -p ozzy-broker -- --config deployment.toml init --identity deployment.identity.toml
cargo run -p ozzy-broker -- --config deployment.toml init-broker --broker laptop \
  --identity deployment.identity.toml --local-identity laptop.identity.toml
cargo run -p ozzy-broker -- --config deployment.toml init-volumes --broker laptop \
  --identity deployment.identity.toml --local-identity laptop.identity.toml
cargo run -p ozzy-broker -- --config deployment.toml format --broker laptop \
  --identity deployment.identity.toml --local-identity laptop.identity.toml
cargo run -p ozzy-broker -- --config deployment.toml check --broker laptop --identity deployment.identity.toml
cargo run -p ozzy-broker -- --config deployment.toml serve --broker laptop \
  --identity deployment.identity.toml --local-identity laptop.identity.toml \
  --trusted-transport
```

Generate identity once, distribute the exact file to every broker. Never run
independent initialization on each broker. `init` refuses existing files, even
damaged ones. Missing or incompatible identity makes `check` fail. Identity
publication synchronizes file and directory; checksums detect accidental damage.

`init-broker` creates local volume/store bindings once per broker. Keep this file
with that broker's persistent deployment state. Pass `--local-identity` to
`check` to validate its bindings too. These commands never format segment stores.

CPU checks use the calling thread's effective Linux affinity. Memory checks use
its allowed NUMA nodes. Unknown locality cannot satisfy explicit NUMA placement.
Identity and volume commands run before workers start. `format` starts configured
shards and shared device workers, initializes their assigned stores, then drains
both. Device roots must exist. Existing or partial partition stores are refused
before formatting. Topic directory creation and journal work use the async backend.

`DevicePools::start(&plan)` starts shared controller pools and returns one I/O
lane per application shard. Move each lane onto its shard before constructing
`ozzy_io::Local`. Call `shutdown().await` after journals drain. See
[runtime configuration](../doc/RUNTIME.md#deployment-configuration) for limits
and worker placement.

`ApplicationShards::start(&plan, lanes, factory).await` constructs application
state on each assigned thread. The factory receives `ShardContext`, supports
non-Send local actor tasks, and calls `ready()` after initialization. Observe its
`shutdown` signal during initialization and serving. Drain journals before
returning, then await shard shutdown before device shutdown.

Successful `check` is not evidence that partition files exist or are recoverable.

`serve` requires existing shared and local identity, volume markers, and segment
history. Missing or damaged state fails startup. `--trusted-transport` explicitly
selects a transport domain that already establishes trust. Native client IDs
are bounded admission identities, not authentication. SIGINT and SIGTERM drain
journals, sockets, and shared device workers. Unexpected worker failure drains
the other owners and exits with failure.

`Broker::start_trusted(checked, local).await` opens established journals, starts
configured application shards and shared device pools, then binds the shared
frontend. It never formats missing history. Use this entry point only in an
explicitly trusted transport domain. `start_trusted_with_context` shares the OMQ
inproc registry with SDKs in embedded tests.

`Broker::shutdown()` requests drain immediately. Await its result before dropping
deployment resources. Canceling that observation still drains journals and
sockets before device workers. Completion means the shard, dispatcher, and
device worker threads have exited. Dropping the broker also requests drain;
`closed()` observes completion or an unexpected worker failure.

`initialize_volumes(&checked, &local_identity)` publishes device markers in
existing roots. `check_volumes` verifies them without writing. Run volume checks
before starting workers. Neither function creates partition journals. See
[storage identity](../doc/STORAGE.md#files-and-exclusive-ownership).

`JournalPlan::new(&checked, &local_identity, &principals)` maps partition stores
to native journal settings. Supply the transport adapter's established broker
principal fingerprints. Move each `PartitionJournal` onto its assigned shard
and call `open(io, generation).await` for restart. Explicit `format` requires an
absent partition directory under an existing topic directory. Both use the
shared shard I/O lane. Replicated restart returns election-fenced authority.

For an explicitly trusted broker transport domain,
`JournalPlan::from_trusted_deployment(&checked, &local_identity)` uses the
independent principal bindings persisted by `init`. Restart never generates
these bindings. Connection trust remains the transport adapter's responsibility.

## Explicit partition recovery

`recover` serves the broker with selected replicated partitions kept nonvoting
until complete recovery publication and election. Unselected partitions open
normally. Existing shared/local identity and volume bindings remain required.

```sh
ozy_broker --config deployment.toml recover --broker broker-0 \
  --identity deployment.identity.toml --local-identity broker-0.identity.toml \
  --trusted-transport --replace orders/0 --resume orders/1
```

| Flag | Required store state | Intent |
| --- | --- | --- |
| `--replace TOPIC/PARTITION` | Absent partition directory | Create a nonvoting replacement |
| `--quarantine TOPIC/PARTITION` | Established identity and authority metadata | Preserve files and remove voting eligibility |
| `--resume TOPIC/PARTITION` | Exact unfinished marker | Continue recovery, allowing sealed-file repair |
| `--resume-full TOPIC/PARTITION` | Exact unfinished marker | Continue using full history transfer |

Repeat flags to select multiple partitions. The complete selection is checked
before any selected mutation. Unknown, duplicate, single-broker, or incompatible
store selections fail. `serve` never changes missing history into recovery.
Use `serve` after publication; resume refuses completed stores. See
[recovery boundaries](../doc/REPLICATION.md).

After an unclean stop, `replicated-persisting` partitions also require explicit
recovery when drained voting history cannot be proven. Select every such
partition; ordinary serving refuses them even if payload files look intact.

Embedded callers use `Broker::start_recovering_trusted(checked, local, selections)`.
The context variant shares OMQ with SDKs in tests. Recovery uses the existing
shards, frontend, and device pools. It adds no per-partition sockets or workers.

## Linux container

Build a native Linux binary for the Debian 13 image. Keep its build context
outside the source tree. Podman host networking preserves the explicit endpoints
in [container.toml](container.toml). `--userns keep-id` preserves ownership of the
persistent bind mount.

```sh
cargo build --release -p ozzy-broker --bin ozy_broker
mkdir -p /mnt/ssd/tmp/ozzy-image /mnt/ssd/tmp/ozzy-dev/data
cp /mnt/ssd/tmp/cargo-target/release/ozy_broker /mnt/ssd/tmp/ozzy-image/
container() {
  podman --root /mnt/ssd/tmp/ozzy-container-storage \
    --runroot "${XDG_RUNTIME_DIR}/ozzy-container-dev" "$@"
}
container build -f ozzy-broker/Containerfile -t localhost/ozzy:dev /mnt/ssd/tmp/ozzy-image
cp ozzy-broker/container.toml /mnt/ssd/tmp/ozzy-dev/deployment.toml
ozy() {
  container run --rm --network host --userns keep-id \
    -v /mnt/ssd/tmp/ozzy-dev:/ozzy localhost/ozzy:dev \
    --config /ozzy/deployment.toml "$@"
}
ozy init --identity /ozzy/shared.identity
for command in init-broker init-volumes format; do
  ozy "$command" --broker laptop --identity /ozzy/shared.identity \
    --local-identity /ozzy/local.identity
done
ozy serve --broker laptop --identity /ozzy/shared.identity \
  --local-identity /ozzy/local.identity --trusted-transport
```

Initialize once. Restart with `serve` using the same bind mount. The binary runs
as the container's main process. SIGINT and SIGTERM request journal and worker
drain. The example endpoints are reachable only on the host's loopback interface.
