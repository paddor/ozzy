# Getting started

Ozzy streams records through partitioned topics. Run a broker, then connect
producers and consumers with the [Rust SDK](ozzy/README.md). Brokers own the
retained log; OMQ handles message passing.

## Build

Requires Linux and Rust 1.93+. Run these commands from the repository root:

```sh
source scripts/ozzy_tools.sh
scripts/ozzy_cargo build --locked --release -p ozzy-broker --bin ozzy
export PATH="$CARGO_TARGET_DIR/release:$PATH"
```

## Start a single durable broker

The sample binds loopback ports and stores data under `tmp/ozzy-single`.

```sh
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

Provision once. Restart with `serve` and the same configuration and identities.
Ctrl-C requests a drain of journals, sockets, and storage workers.
`--trusted-transport` requires a trusted transport domain; client IDs do not
authenticate clients.

| Command | Creates or checks |
| --- | --- |
| `validate` | Configuration and local resource placement |
| `init` | Shared deployment identity; distribute the same file to every broker |
| `init-broker` | This broker's local store bindings |
| `init-volumes` | Volume markers in existing device roots |
| `format` | New partition journals |
| `serve` | Opens existing stores and starts serving |

Initialization refuses existing state. `serve` refuses missing or damaged
stores. Keep shared/local identities with the persistent data.

## Build and run a container

Requires Podman. Build above with a Linux toolchain compatible with Debian 13.
The image contains only the server; data lives in a persistent bind mount.

```sh
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

Restart with the last command. Host networking keeps the sample's loopback
endpoints; `--userns keep-id` preserves bind-mount ownership.

## Run three brokers

[three.toml](ozzy-broker/three.toml) defines six partitions in disk-quorum mode,
with distinct loopback endpoints. Edit endpoints and device roots for separate
hosts. Use `replicated-persisting` confirmation for background persistence.

Provision this local example once:

```sh
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

Run one process per broker, selecting `broker-0`, `broker-1`, or `broker-2`:

```sh
ozzy_three serve --broker broker-0 --identity "$ozzy_dir/shared.identity" \
  --local-identity "$ozzy_dir/broker-0.identity" --trusted-transport
```

Each broker keeps its own local identity and stores. Two of three eligible
copies are required for confirmation. Single durable has no failover.

## Recover a replicated partition

An unclean replicated-persisting restart may require explicit nonvoting recovery.
Select each affected partition:

```sh
ozzy_three recover --broker broker-0 --identity "$ozzy_dir/shared.identity" \
  --local-identity "$ozzy_dir/broker-0.identity" --trusted-transport \
  --quarantine orders/0
```

| Flag | Required state |
| --- | --- |
| `--replace TOPIC/PARTITION` | Absent partition directory |
| `--quarantine TOPIC/PARTITION` | Established identity and authority metadata |
| `--resume TOPIC/PARTITION` | Unfinished recovery marker; permits sealed-file repair |
| `--resume-full TOPIC/PARTITION` | Unfinished recovery marker; forces full history transfer |

Repeat flags for multiple partitions. Selected partitions remain nonvoting
until recovery completes. Restart completed stores with `serve`.

See [runtime configuration](doc/RUNTIME.md#deployment-configuration),
[storage](doc/STORAGE.md), and [replication](doc/REPLICATION.md) for the contracts.
