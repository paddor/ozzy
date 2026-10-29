# Getting started

This page tries Ozzy in three commands, then builds the broker, provisions
storage, and runs one broker, a container, or three brokers. Then connect
applications with the [Rust SDK](ozzy/README.md).

Requires Linux, Rust 1.93 or newer, and bash. Run every command from the
repository root.

## Try it

Run each command in its own terminal:

```bash
cargo run --release -p ozzy-broker -- quickstart tmp/try
cargo run --release -p ozzy-ui -- --config tmp/try/deployment.toml --broker local
cargo run --release -p ozzy-broker --example ozzy_demo -- \
  --config tmp/try/deployment.toml --broker local --identity tmp/try/shared.identity
```

1. `quickstart` provisions broker `local` in `tmp/try` with the steps below,
   then serves topics `orders` and `delayed` (4 partitions each) on loopback
   TCP ports it picks. Formatting writes 128 MiB; retention keeps at most
   256 MiB per partition.
2. `ozzy-tui` shows the broker. Keys and screens: [ozzy-ui](ozzy-ui/README.md).
3. `ozzy_demo` writes 100 records a second (`--rate`) and reads them as a
   member of group `workers`. It stops on Ctrl-C or when you delete the group
   on the console's Groups screen.

- Ctrl-C stops the broker. Run the same `quickstart` command to restart it;
  it serves the existing stores and never formats again.
- A directory with files but no `deployment.toml` is refused. Remove
  `tmp/try` to start over.
- Like `serve --trusted-transport`, it does not authenticate clients: any
  local user can connect.

## Try delayed delivery

Topic `delayed` takes records with a due time. With the quickstart broker
running:

```bash
cargo run --release -p ozzy-broker --example ozzy_demo -- \
  --config tmp/try/deployment.toml --broker local --identity tmp/try/shared.identity \
  --topic delayed --due-secs 10
```

- Each record is due 10 seconds after it is written. The broker confirms it
  at once, and readers get it at its due time.
- Each second, the demo prints how late records arrived after their due
  time: p50, p99, and max.
- The topic allows delays up to an hour and holds up to 1,000,000 records per
  partition. Past that, producers back off until records are released.
- A `tmp/try` provisioned before topic `delayed` existed does not have it.
  Remove `tmp/try` and run `quickstart` again.
- For your own topics, see [delay keys](ozzy-config/README.md#delay).

## Build

```bash
cargo install --locked --path ozzy-broker
cargo install --locked --path ozzy-ui
```

- This installs `ozzy` and `ozzy-tui` into `~/.cargo/bin`.
- To serve compressed endpoints, add `--features lz4-transport` or
  `--features zstd-transport` to the broker install.

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
- `format` writes zeros over one segment file (`segment_bytes`) per
  partition, so the samples write several GiB once.

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

Requires Podman. Install the broker first ([Build](#build)), with a toolchain compatible with
Debian 13 (the image base). The image holds only the server; data lives in a
bind mount.

```bash
ozzy_image="$PWD/tmp/ozzy-image"
ozzy_container_dir="$PWD/tmp/ozzy-container"
mkdir -p "$ozzy_image" "$ozzy_container_dir/data"
cp "$(command -v ozzy)" "$ozzy_image/"
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

## Watch a broker

`ozzy-tui` shows one broker's status and counters in the terminal. The samples
give each broker an `admin` endpoint in the local IPC namespace, so run it on
the broker's host with the same config and broker name:

```bash
ozzy-tui --config "$ozzy_dir/deployment.toml" --broker laptop    # single broker
ozzy-tui --config "$ozzy_dir/deployment.toml" --broker broker-0  # three brokers
```

Keys and screens: [ozzy-ui](ozzy-ui/README.md).

## Recover a partition

In three-broker mode, start a broker with `recover` instead of `serve` when one
of its partition stores is lost or must stop voting.

```bash
ozzy_three recover --broker broker-0 --identity "$ozzy_dir/shared.identity" \
  --local-identity "$ozzy_dir/broker-0.identity" --trusted-transport \
  --quarantine orders/0
```

| Flag | Use when |
| --- | --- |
| `--replace TOPIC/PARTITION` | The partition directory is absent, for example after a lost disk |
| `--quarantine TOPIC/PARTITION` | The store exists but must lose its vote, for example after storage damage |
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
