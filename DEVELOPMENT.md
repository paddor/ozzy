# Development

How to check, test, and soak Ozzy changes. For contributors. Release steps live
in [RELEASING.md](RELEASING.md); benchmark rules live in
[BENCHMARKS.md](BENCHMARKS.md) and [ozzy-bench](ozzy-bench/README.md).

## Requirements

- Rust 1.93 or newer, edition 2024, ASCII-only source.
- [`cargo-nextest`](https://nexte.st/).
- CPython 3.12+ and its development library for the Python binding workspace
  member. Select the interpreter with `PYO3_PYTHON` when needed.
- Optional, for [verification](#verification): Kani, ripgrep, and the
  TLA+ tools jar.
- JDK 22+ for the Java binding workspace member; Maven for packaged JAR checks.

## Local checks

```sh
./scripts/test-all.sh          # full gate: every check below
./scripts/test-all.sh lint     # format, file shape, Clippy
./scripts/test-all.sh tests    # Nextest, doctests, API docs

# The same checks one at a time:
source scripts/ozzy_tools.sh   # artifact paths, see below
cargo fmt --all -- --check
scripts/check-shape.sh
cargo build --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo nextest run --workspace --test-threads 8
cargo test --workspace --doc
RUSTDOCFLAGS='-D warnings' cargo doc -p ozzy -p ozzy-runtime -p ozzy-proto --no-deps
```

- `scripts/ozzy_cargo <args>` runs Cargo with the same artifact paths.
- `ozzy-bench` is not a default member. A plain `cargo build` skips it; use
  `cargo build -p ozzy-bench` or `--workspace`.

`scripts/check-python.sh` uses uv to select Python, keeps its virtual environment
and wheels under `OZZY_ARTIFACT_ROOT`, and runs Python lint/type checks, inproc
memory-broker tests, and installed-wheel TCP process tests in both broker modes.
Set `OZZY_PYTHON_VERSION=3.14` to test another supported interpreter. TCP/process
tests are opt-in through `wheel-tests` and run in the extended Nextest profile.

`scripts/check-java.sh` compiles Java with warnings as errors, generates Javadoc,
builds native platform JARs, and runs the Java inproc matrix and packaged-JAR
TCP/process tests. Set `OZZY_JAVA_RELEASE=1` for release native libraries.
Cargo compiles Java test classes under its artifact directory; Nextest starts
a JVM per case over the same real-broker memory harness. `package-tests` enables
the separate-JVM layer in the extended profile. Test Java 22 and a newer JDK.

### Artifact paths

`scripts/ozzy_tools.sh` sets these. An explicit value always wins.

| Variable | Default |
| --- | --- |
| `OZZY_ARTIFACT_ROOT` | `$TMPDIR`, else `/tmp`; a relative path resolves from the repo root |
| `CARGO_TARGET_DIR` | `$OZZY_ARTIFACT_ROOT/cargo-target/checkouts/<checkout hash>` |
| `TMPDIR` | `$OZZY_ARTIFACT_ROOT` |

### Code limits

Clippy and `scripts/check-shape.sh` enforce these:

- Product code has no `unwrap`, `expect`, `panic!`, `unreachable!`, `todo!`,
  or `unimplemented!`. Test builds allow them.
- A function has at most 60 code lines and 5 arguments besides `self`
  (`clippy.toml`).
- A Rust file has at most 600 lines; tests and simulation support have 1000.
- `scripts/check-shape.allow` lists exempt files. Remove an entry once its file
  fits, or the check fails.
- Test targets and tools use the `ozzy_` prefix. The broker command is `ozzy`.

## CI

| Workflow | Runs on | Jobs |
| --- | --- | --- |
| `ci.yml` | Pull requests, pushes to `main`, manual | `test-all.sh lint`, then build plus `test-all.sh tests`, MSRV `cargo check` on Rust 1.93, and `cargo publish --dry-run` |
| `extended.yml` | Sundays 03:00 UTC, manual | `test-all.sh lint`, then the `extended` profile and `scripts/test.sh loom` |
| `release-plz.yml` | After a green `main` CI run | Release PRs and publishing, see [RELEASING.md](RELEASING.md) |

CI builds with `-C target-cpu=x86-64`. Local builds use `target-cpu=native`
from `.cargo/config.toml`.
Release builds use ThinLTO, which increases link time.

## Nextest profiles

`.config/nextest.toml` owns the split and the timeouts.

| Profile | Runs | When |
| --- | --- | --- |
| `default` | Fast suite, a few minutes: unit, core, simulation, inproc broker/SDK over memory storage | Every PR and push |
| `extended` | System tests: TCP, processes, physical disk, `stress` modules, exhaustive crash cuts, timed bench cohorts | Weekly CI; locally when a change touches TCP, processes, physical storage, or startup |

```sh
cargo nextest run --workspace --test-threads 8 --profile extended
```

- An integration case in the default profile finishes in about ten seconds.
- Put TCP/process, physical-disk, long, and exhaustive cases in a `stress`
  module or the `extended` filter.
- Command-line filters intersect with the profile filter. To select a system
  test by name, add `--profile extended` or `--ignore-default-filter`.

### Focused scopes

`scripts/test.sh <scope> [filters]` runs one slice, such as `core`, `storage`,
or `replication`. `scripts/test.sh --help` lists the scopes, and the script
shows what each selects. A scope ignores the default filter, so it includes the
system tests in that slice.

## Test layers

| Layer | Proves | Lives in | Profile |
| --- | --- | --- | --- |
| Unit and core | Codecs, state transitions, bounds, authority fences | `src/` and `tests/` of `ozzy-proto`, `ozzy-core`, `ozzy-journal`, `ozzy-journal-segment`, `ozzy-replication`, `ozzy-config` | `default` |
| Core simulation | Replayable protocol fault schedules with gated time, messages, and file jobs | `ozzy-sim/tests/` | `default`; `stress` in `extended` |
| Seeded actor cluster | Production partition shards and journals of three brokers on one thread from one seed: link, disk, and client faults, scripted leader loss, power loss and restart, per-tick history checks, convergence, missed wakeups | `ozzy-sim/tests/actor_cluster/` | `default`; seed campaign in `extended` |
| Inproc integration | Real SDKs and brokers over OMQ inproc and memory file jobs; owner lifecycle | `ozzy-broker/tests/journals/serving/`, `ozzy-sim/tests/broker/` | `default`; `stress` in `extended` |
| Full-product simulator | Sustained workload and fault churn over real owners, SDKs, and brokers | `ozzy-sim` `ozzy_sim` binary | Short gate in `default`; long runs manual |
| Backend conformance | The same owned file operations on the pool and AIO backends | `ozzy-io-pool/tests/`, `ozzy-io-aio/tests/` | `default` |
| File-model power cuts | Dirty and durable bytes, file lengths, namespace barriers | `ozzy-io` `simulation` backend; crash-cut tests in `ozzy-journal-segment` and `ozzy-sim` | `default`; exhaustive cuts in `extended` |
| TCP and process | Framing, pressure, reconnect, SIGKILL and restart | `ozzy-broker/tests/process/` | `extended` |
| Disk lifecycle | Broker startup, format, and recovery over real device pools | `ozzy-broker/tests/journals/actors.rs`, `startup.rs` | `extended` |
| Loom | Shared counters and wakeup protocols | `tests/loom.rs` in `ozzy-io`, `ozzy-runtime`, `ozzy-broker` | None; `scripts/test.sh loom` |
| Soaks | Hours of churn over processes or memory-only brokers | Ignored tests, see [Soaks](#soaks-and-simulator-runs) | None; manual |

What a layer does not prove:

- Unit and core: only finite state and format cases.
- Core simulation: only within model bounds and injected faults.
- Seeded actor cluster: modeled links with OMQ semantics, no sockets, broker
  frontend, or SDKs.
- Inproc: no ZMTP wire and no physical disk. Threaded scheduling stays
  nondeterministic.
- Simulator: seed replay reproduces the fault prefix, not the transport
  schedule.
- Backend conformance: kernel and filesystem evidence, not hardware power loss.
- File model: a file-level model, not a complete filesystem.
- TCP and process: SIGKILL keeps the OS page cache, so this is not power loss.

### Test rules

- Integration tests embed the real broker over OMQ inproc and the memory file
  backend. No other product mode.
- Control execution and completion as separate steps. Aliased or canceled jobs
  keep their physical charges.
- Check with independent oracles: message IDs, multipart boundaries, payload
  bytes, sequence numbers, and offsets.
- Poll for conditions. Never sleep a fixed time. Drive long timers with manual
  broker time.
- Test under bounded load first.
- Run builds, heavy checks, benchmarks, and profiling serially.

## Loom

```sh
scripts/test.sh loom [filters]
```

The scope builds with `--cfg ozzy_loom` and runs the `tests/loom.rs` models of
`ozzy-io`, `ozzy-runtime`, and `ozzy-broker`. Models compile the production
atomics and cover Ozzy-owned shared counters and wakeup protocols: storage
signals, SDK capacity, receipts, replica wakeups, and broker shutdown. Each
model file lists its cases.

Outside Loom: Tokio `Notify` internals, Tokio wakes, OMQ internals, physical
I/O, and complete replication.

## Required coverage

A change keeps or adds cases for each area it touches:

- **Identity and order:** exact bytes, parts, and message IDs; shared offsets;
  independent producer epochs; unchanged retries.
- **Confirmation:** local durability; two durable disk quorum copies;
  reply-before-COMMIT failover.
- **Authority:** configuration, view, session, incarnation, and receive-epoch
  fencing; stale work after replacement.
- **Pressure:** full-backing charges through aliases and cancellation;
  separate control capacity; slow readers and followers.
- **Repair:** lost, duplicate, and reordered PUB; quiet final loss; bounded
  PEER repair; unavailable quorum.
- **Storage:** short and torn writes; reversed results; failed barriers; roll
  and checkpoint cuts; captured-source pins.
- **Recovery:** nonvoting damage; frozen donors; private replay; bounded
  transfer; interrupted publication.
- **SDK lifecycle:** resume and takeover; control-slot reuse; idle readers;
  detached close; lost replies.
- **Retention:** confirmed retry and record floors; repeated retirement;
  checkpoints; cold replay; ancient replicas.

## Verification

```sh
scripts/verify-rust.sh     # Kani proofs
scripts/verify-models.sh   # TLC models and negative controls
```

Tool versions, setup, and claim limits:
[verification/README.md](verification/README.md). The scripts download
nothing. Do not run them alongside benchmarks.

## Soaks and simulator runs

Long runs are ignored tests or the `ozzy_sim` binary. Neither profile runs
them. Write results as JSONL under `~/.cache/ozzy/`. Run one at a time on idle
hosts. `OZZY_SOAK_TRACE=1` prints producer reopen steps in any soak.

### Full-product simulator

```sh
scripts/ozzy_cargo run -p ozzy-sim --features broker --bin ozzy_sim -- \
  --mode disk-quorum --seed 42 --duration 28800 \
  --artifacts tmp/ozzy-sim-dq-42
```

Flags, actions, artifacts, and replay: [ozzy-sim/README.md](ozzy-sim/README.md).

### Seeded actor cluster

A seed campaign over every profile (`none`, `light`, `heavy`, `swarm`, and
`slow` disks) and scenario: `random` faults only; `failover`, which also cuts
the leader off and requires a new one before it rejoins; `full`, which fills
a journal of six 32 KiB segments until it refuses proposals, then loses the
leader; `sparse-followers`, where both followers write one
operation per group while proposals carry four, so they converge only by
packing; and `sparse-rejoin`, where such a follower misses about 1300
operations, then rejoins as the leader is lost, so the view change installs
a suffix that outlasts any election timeout and passes the segment limit;
`crash`, where a follower and then the leader lose power and restart
over what reached their disks, with fresh writer generations and link
sessions; `power-loss`, where all three lose power at once mid-traffic;
`recovery-crash`, where power fails again at a seeded tick of that recovery;
`torn-write`, where every disk stops completing jobs and power then fails
with the queued writes in flight; `disk-stall`, where the leader's disk
completes nothing for 15 s while its links stay up, and another broker must
take over; `pause`, where the leader's process runs nothing for 15 s, then
resumes into a newer view; and `one-way`, where the leader first hears
nothing, then a later leader can send nothing. A power loss puts some queued
writes in the cache unreported, even `O_DSYNC` ones, then lets a seeded share
of cached 512-byte sectors reach media, so unsynced writes tear and land out
of order. The power-loss fast test also bounds the outage, which grows when a
restart refetches or replays all history. The three sparse or full scenarios
skip `heavy` and `swarm`, which make too little progress to fill a journal.
A failing seed prints its parameters, schedule hash, and this command with
`OZZY_ACTOR_SIM_SEED`, `OZZY_ACTOR_SIM_PROFILE`, and
`OZZY_ACTOR_SIM_SCENARIO` set. A run whose faults left too little progress
to prove much is vacuous: the campaign lists it and fails only above one in
twenty. Runs spread over one thread per core; `OZZY_ACTOR_SIM_THREADS` sets
the count. The fast suite also loses the leader after a long history and
repeatedly; `seeded::stress` adds a very long history.
`OZZY_ACTOR_SIM_TRACE=1` prints each broker's view and positions as they
change.

```sh
OZZY_ACTOR_SIM_SEEDS=1000 cargo nextest run -p ozzy-sim \
  --test ozzy_actor_cluster --profile extended seeded::stress::campaign
```

### Memory-only broker churn

Real inproc brokers and SDKs over the memory backend.
`OZZY_SOAK_POLICY` is `single` or `dq`.

```sh
OZZY_SOAK_POLICY=dq OZZY_SOAK_SECONDS=3600 \
OZZY_SOAK_RESULTS=$HOME/.cache/ozzy/churn-dq.jsonl \
  scripts/ozzy_cargo test -p ozzy-broker --test ozzy_broker_config \
  journals::serving::simulated::churn::memory_only_varied_churn_soak \
  -- --ignored --exact --nocapture
```

### Core fault churn

Seeded storage and recovery schedules over the core simulation.

```sh
OZZY_CHURN_SECONDS=3600 OZZY_CHURN_SEED=1 \
OZZY_CHURN_RESULTS=$HOME/.cache/ozzy/core-churn.jsonl \
  scripts/ozzy_cargo test -p ozzy-sim --test ozzy_lifecycle \
  churn::long_running_accelerated_fault_churn -- --ignored --exact --nocapture
```

### Single-broker disk soak

One durable broker process on physical storage. Single durable has no
failover; the soak restarts its only broker. `OZZY_SOAK_BROKER_BINARY` pins
the `ozzy` binary; the default is the one Cargo builds.

```sh
OZZY_SOAK_SECONDS=3600 OZZY_SOAK_RESULTS=$HOME/.cache/ozzy/single.jsonl \
  scripts/ozzy_cargo test -p ozzy-broker --test ozzy_broker_config \
  process::soak::single::single_broker_varied_io_soak \
  -- --ignored --exact --nocapture
```

### Cross-host crash soak

Broker processes on three hosts (disk quorum) or one
host (single durable), with SIGKILL and orderly restarts. These are
process-crash tests, not power-loss tests.

1. Create a fresh directory per broker with the same `ozzy` binary,
   `deployment.toml`, and `shared.identity`, plus that broker's
   `local.identity`. Provision as in [GETTING_STARTED.md](GETTING_STARTED.md).
2. Use topic `orders` with four partitions and bounded retention.
3. Give the controller `ssh` key login to every host. It starts each broker
   with `taskset`.
4. Run a short gate first, then the full run.

```sh
source scripts/ozzy_tools.sh
taskset -c 4,5 cargo test -p ozzy-broker --test ozzy_broker_config \
  process::soak::three_host_crash_soak -- --ignored --exact --nocapture
```

Use `process::soak::cross_host_single_crash_soak` for single durable.

| Variable | Meaning |
| --- | --- |
| `OZZY_SOAK_CONFIG` | Controller's deployment file |
| `OZZY_SOAK_DIRS` | Comma-separated broker directories, in host order |
| `OZZY_SOAK_HOSTS` | Host order; default `si-dev,wu-dev,er-dev` |
| `OZZY_SOAK_LOGS` | Broker log directory |
| `OZZY_SOAK_RESULTS` | JSONL results file |
| `OZZY_SOAK_SECONDS` | Duration; default 3600 |
| `OZZY_SOAK_IDENTITY` | Controller's `shared.identity` when the first broker is remote |
| `OZZY_SOAK_MEMORY` | `1` runs memory-only brokers, see below |

- On separate hosts, brokers use core 0 and the SDK uses cores 4-5.
- For three local processes, set all hosts to `si-dev` and use distinct
  directories and endpoints. Brokers then use cores 0, 1, and 2.
- Single durable restarts its only broker before it waits for progress.
- Process failure, or 30 seconds without progress, fails the run.

With `OZZY_SOAK_MEMORY=1`, copy the `ozzy_broker_config` test executable as
`memory-broker` into every directory. Each broker then runs over TCP with
simulated files. A restart loses that broker's store, so recovery needs both
other copies, also for disk quorum. This is a test harness, not a storage mode
or a durability claim.
