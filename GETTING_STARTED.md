# Getting started

This page builds the broker, provisions storage, and runs one broker, a
container, or three brokers. Then connect applications with the
[Rust SDK](ozzy/README.md).

Requires Linux, Rust 1.93 or newer, and bash. Run every command from the
repository root.

## Build

```bash
source scripts/ozzy_tools.sh
scripts/ozzy_cargo build --locked --release -p ozzy-broker --bin ozzy
export PATH="$CARGO_TARGET_DIR/release:$PATH"
```

- `scripts/ozzy_tools.sh` puts build output under `OZZY_ARTIFACT_ROOT`.
- To serve compressed endpoints, add `--features lz4-transport` or
  `--features zstd-transport`.

## Start a single durable broker

The sample [`container.toml`](ozzy-broker/container.toml) defines broker
`laptop` on loopback ports and topic `orders`. This stores data under
`tmp/ozzy-single`.

```bash
ozzy_dir="$PWD/tmp/ozzy-single"
mkdir -p "$ozzy_dir/data"
sed "s|/ozzy/data|$ozzy_dir/data|" ozzy-broker/container.toml > "$ozzy_dir/deployment.toml"
ozzy() { command ozzy --config "$ozzy_dir/deployment.toml" "$@"; }

ozzy validate --broker laptop
ozzy init --identity "$ozzy_dir/shared.identity"
for step in init-broker init-volumes format; do
  ozzy "$step" --broker laptop --identity "$ozzy_dir/shared.identity" \
    --local-identity "$ozzy_dir/local.identity"
done
ozzy serve --broker laptop --identity "$ozzy_dir/shared.identity" \
  --local-identity "$ozzy_dir/local.identity" --trusted-transport
```

- Provision once. To restart, run only `serve` with the same configuration and
  identity files.
- Ctrl-C (SIGINT) or SIGTERM drains journals, sockets, and storage workers,
  then exits.
- `format` preallocates one segment (`segment_bytes`) per partition, so the
  samples reserve several GiB.

## Commands

`--config FILE` goes before the subcommand.

| Command | What it does |
| --- | --- |
| `validate` | Checks the configuration and this host's resources; writes nothing |
| `init` | Creates the shared deployment identity; copy the same file to every broker |
| `init-broker` | Creates this broker's local identity (store bindings) |
| `init-volumes` | Binds existing device root directories to this broker |
| `format` | Creates absent partition journals on this broker's shards |
| `check` | Checks identities and placement; opens no stores |
| `serve` | Opens existing stores and serves |
| `recover` | Serves with selected partitions in nonvoting recovery |

| Flags | Commands |
| --- | --- |
| `--broker NAME` | All but `init` |
| `--identity FILE` | All but `validate` |
| `--local-identity FILE` | `init-broker`, `init-volumes`, `format`, `serve`, `recover`; optional for `check` |
| `--trusted-transport` | Required for `serve` and `recover` |

- Initialization refuses existing state.
- `serve` refuses missing or damaged stores and never creates them.
- Keep the shared and local identity files with the persistent data.
- `--trusted-transport` states that the network is already trusted. Client IDs
  are admission identities; they do not authenticate clients.

## Build and run a container

Requires Podman. Build the binary first, with a toolchain compatible with
Debian 13 (the image base). The image holds only the server; data lives in a
bind mount.

```bash
ozzy_image="$PWD/tmp/ozzy-image"
ozzy_container_dir="$PWD/tmp/ozzy-container"
mkdir -p "$ozzy_image" "$ozzy_container_dir/data"
cp "$CARGO_TARGET_DIR/release/ozzy" "$ozzy_image/"
cp ozzy-broker/container.toml "$ozzy_container_dir/deployment.toml"
podman build -f ozzy-broker/Containerfile -t localhost/ozzy:dev "$ozzy_image"
ozzy_container() {
  podman run --rm --network host --userns keep-id \
    -v "$ozzy_container_dir:/ozzy" localhost/ozzy:dev \
    --config /ozzy/deployment.toml "$@"
}

ozzy_container init --identity /ozzy/shared.identity
for step in init-broker init-volumes format; do
  ozzy_container "$step" --broker laptop --identity /ozzy/shared.identity \
    --local-identity /ozzy/local.identity
done
ozzy_container serve --broker laptop --identity /ozzy/shared.identity \
  --local-identity /ozzy/local.identity --trusted-transport
```

- To restart, run only the last command.
- `--network host` keeps the sample's loopback endpoints.
- `--userns keep-id` keeps bind-mount file ownership.

## Run three brokers

The sample [`three.toml`](ozzy-broker/three.toml) defines `broker-0`,
`broker-1`, and `broker-2` on loopback, and topic `orders` with six partitions
in disk quorum mode. Provision once:

```bash
ozzy_dir="$PWD/tmp/ozzy-three"
mkdir -p "$ozzy_dir"/broker-{0,1,2}
sed "s|/var/tmp/ozzy-three|$ozzy_dir|g" ozzy-broker/three.toml > "$ozzy_dir/deployment.toml"
ozzy_three() { command ozzy --config "$ozzy_dir/deployment.toml" "$@"; }
ozzy_three init --identity "$ozzy_dir/shared.identity"
for n in 0 1 2; do
  for step in init-broker init-volumes format; do
    ozzy_three "$step" --broker "broker-$n" --identity "$ozzy_dir/shared.identity" \
      --local-identity "$ozzy_dir/broker-$n.identity"
  done
done
```

Run one process per broker. Repeat this for `broker-1` and `broker-2`:

```bash
ozzy_three serve --broker broker-0 --identity "$ozzy_dir/shared.identity" \
  --local-identity "$ozzy_dir/broker-0.identity" --trusted-transport
```

- Every broker needs its own `peer`, `data_peer`, `reader_pub`, and
  `follower_pub` endpoint. For separate hosts, edit the endpoints and device
  roots.
- All brokers share one `shared.identity`. Each keeps its own local identity
  and stores.
- Writes confirm while two of the three brokers are up.
- For background persistence, set `confirmation = "replicated-persisting"` on
  the topic.

## Recover a partition

In three-broker mode, start a broker with `recover` instead of `serve` when one
of its partition stores is lost or must stop voting. An unclean stop of a
replicated-persisting partition needs this even when its files look intact.

```bash
ozzy_three recover --broker broker-0 --identity "$ozzy_dir/shared.identity" \
  --local-identity "$ozzy_dir/broker-0.identity" --trusted-transport \
  --quarantine orders/0
```

| Flag | Use when |
| --- | --- |
| `--replace TOPIC/PARTITION` | The partition directory is absent, for example after a lost disk |
| `--quarantine TOPIC/PARTITION` | The store exists but must lose its vote, for example after an unclean replicated-persisting stop |
| `--resume TOPIC/PARTITION` | An earlier recovery did not finish; allows sealed-file repair |
| `--resume-full TOPIC/PARTITION` | An earlier recovery did not finish; forces a full history transfer |

- Repeat flags to select several partitions, each at most once.
- Recovery needs both other brokers running.
- Selected partitions stay nonvoting until recovery completes.
- After recovery completes, restart with `serve`.
- Single-broker partitions cannot recover.

## Next

- [Configuration keys](ozzy-config/README.md): every key, its default, and the
  validation rules.
- [Deployment configuration](doc/RUNTIME.md#deployment-configuration): what
  endpoints, limits, shards, and devices do at runtime.
- [Storage](doc/STORAGE.md) and [replication](doc/REPLICATION.md): what
  survives a crash and how recovery works.
