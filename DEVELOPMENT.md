# Development

Ozzy requires Rust 1.93 or newer.

CI checks out published OMQ and fanring revisions as sibling directories and
uses the same workspace check scripts. It checks MSRV separately and builds the
registry crates through a publishing dry run. CI uses generic x86_64 codegen.

## Local check

```sh
./scripts/test-all.sh
```

The script checks formatting, runs Clippy with warnings denied, runs workspace
tests through Nextest with eight threads, runs doctests separately, and builds
strict public API documentation.

Executables use the `ozy_` prefix (15 characters fit the kernel process name);
integration tests use `ozzy_`.
The suite also names its Cargo, compiler, and rustdoc processes. Use
`scripts/ozzy_cargo build --workspace` for the same naming and SSD build
directory on individual commands. Clippy retains its own compiler wrapper.

Individual commands:

```sh
source scripts/ozzy_tools.sh
cargo build --workspace
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo nextest run --workspace --test-threads 8
cargo test --workspace --doc
RUSTDOCFLAGS='-D warnings' cargo doc -p ozzy -p ozzy-runtime -p ozzy-proto \
  --no-deps
```

`ozzy-bench` is excluded from the default workspace members. Build it
explicitly when working on benchmarks:

```sh
cargo build -p ozzy-bench
```

For fast inside-out checks, use `scripts/test.sh core`, `writer`, `inproc`,
`simulation`, or `loom` before the full gate. The test layers below define
their evidence and limits.

## Verification

```sh
cargo nextest run -p ozzy-journal -p ozzy-core -p ozzy-sim --test-threads 8
bash scripts/verify-rust.sh
bash scripts/verify-models.sh
```

The optional verification scripts require Kani and the TLA+ tools jar;
see [verification/README.md](verification/README.md) for versions, model bounds,
and limits of each claim. Set `OZZY_KANI_BIN` if `cargo-kani` is not on PATH,
and `OZZY_TLC_JAR` for a nondefault jar location. No tools are downloaded by
the check scripts. Do not run heavy verification alongside benchmarks.
## Test layers

| Layer | Evidence | Limits |
| --- | --- | --- |
| Unit/core | Production codecs, transitions, bounds, authority fences | Finite state/format cases |
| Inproc integration | Real SDKs/brokers, OMQ, memory-only file jobs, owner lifecycle | No ZMTP or physical disk; threaded scheduling remains nondeterministic |
| Core simulation | Gated time/messages/file jobs; replayable protocol fault schedules | Model bounds and injected faults |
| Full-product simulator | Real owners/SDKs/brokers over OMQ inproc; sustained workload/fault churn | Seed replay reproduces fault prefixes, not exact transport scheduling |
| Pool/AIO conformance | Same owned operations on physical backends | Kernel/filesystem evidence, not hardware power-loss proof |
| TCP/process | Framing, pressure, reconnect, SIGKILL/restart | SIGKILL retains OS page cache |
| File-model power cuts | Dirty/durable bytes, file lengths, namespace barriers | File-level model, not a complete filesystem |

Use independent IDs, multipart boundaries, payload bytes, and sequence/offset
oracles. Inproc cases embed the production broker and memory backend, with distinct
broker stores in one shared OMQ context registry. Execution and completion are
controlled separately; aliases/canceled jobs retain their physical charges.

| `scripts/test.sh` scope | Selection |
| --- | --- |
| `unit` / `core` | Workspace library / protocol, owner, journal, replication |
| `writer` / `local` | SDK / configuration and native integration |
| `inproc` | Real broker simulation subtree and full-product simulator gate |
| `storage` / `io` | Segment/index/recovery / backend contract and conformance |
| `replication` / `simulation` | Runtime/socket/process / core and full-product schedules |
| `loom` | Production shared counters, readiness, receipt publication, shutdown |

Nextest uses eight threads except Loom. Process-global/lifecycle cases have
separate isolation groups. Run builds, heavy checks, benchmarks, and profiling
serially. Benchmark commands/boundaries live in [ozzy-bench](ozzy-bench/README.md)
and [BENCHMARKS.md](BENCHMARKS.md).

### Loom

Run `scripts/test.sh loom`. Models compile production atomics and cover capacity
admission/cancellation/release, readiness versus drain, receipt publication, and
broker shutdown versus queue closure. Admission uses a two-preemption bound;
receipt offsets use relaxed stores followed by release/acquire prefix publication.
Shutdown models retain both owners' completion state and include a negative control.
OMQ/Tokio internals, physical I/O, and complete replication are outside Loom.

### Required coverage

| Area | Cases |
| --- | --- |
| Identity/order | Exact bytes/parts/IDs, shared offsets, independent producer epochs, unchanged retries |
| Confirmation | Local durability, two durable DQ copies, two retained RP copies, reply-before-COMMIT failover |
| Authority | Configuration/view/session/incarnation/receive-epoch fencing; stale work after replacement |
| Pressure | Full-backing charges through aliases/cancellation; separate control capacity; slow readers/followers |
| Repair | Lost/duplicate/reordered PUB, quiet final loss, bounded PEER repair, unavailable quorum |
| Storage | Short/torn writes, reversed results, failed barriers, roll/checkpoint cuts, captured-source pins |
| Recovery | Nonvoting damage, frozen donors, private replay, bounded transfer, interrupted publication |
| SDK lifecycle | Resume/takeover, control-slot reuse, idle readers, detached close, lost replies |
| Retention | Confirmed retry/record floors, repeated retirement, checkpoints, cold replay, ancient replicas |

## Soaks and simulator runs

The simulator command/resource reference is [ozzy-sim/README.md](ozzy-sim/README.md).
The shared `ozzy-sim` broker feature supplies storage ownership, deployment
placement, and the independent SDK record oracle to short integration tests and
sustained product runs. Virtual and wall-clock durations are separate.

Product workloads cover concurrent producers, resume/takeover, consumer checkpoint
reopen/pause, reconnect, retention, quorum loss, broker restart, short writes,
and held completions. Fault logs stream to SSD; bounded recent events and dirty/
durable images support failure inspection. Process cuts retain dirty bytes;
modeled device cuts restore only durable bytes/namespace effects. Seed replay
reproduces the recorded fault prefix, not the complete threaded schedule.

### Cross-host crash soak

Use `process::soak::three_host_crash_soak` in broker integration tests for DQ/RP,
or `process::soak::cross_host_single_crash_soak` for single durable. Provision
fresh isolated directories with identical broker binary, `deployment.toml`,
`shared.identity`, and each broker's `local.identity`. Initialize volumes/stores
through the CLI. Use topic `orders`, four partitions, bounded retention.

| Variable | Value/default |
| --- | --- |
| `OZZY_SOAK_CONFIG` | Controller deployment file |
| `OZZY_SOAK_HOSTS` | Host order; default `si-dev,wu-dev,er-dev` |
| `OZZY_SOAK_DIRS` | Comma-separated directories in host order |
| `OZZY_SOAK_LOGS` | SSD artifact directory |
| `OZZY_SOAK_RESULTS` | JSONL under `~/.cache/ozzy/` |
| `OZZY_SOAK_SECONDS` | Default 3600; use a short gate first |
| `OZZY_SOAK_IDENTITY` | Controller's shared identity when first broker is remote |
| `OZZY_SOAK_BROKER_BINARY` | Copied CLI binary to keep restart provenance fixed |

On separate hosts, broker/storage use core 0 and the SDK uses cores 4-5.
For three local processes, use distinct directories/endpoints and broker cores
0/1/2; set all three hosts to `si-dev`. Keep hosts idle. Single durable restarts
its only broker before awaiting progress; it has no failover.

```sh
source scripts/ozzy_tools.sh
taskset -c 4,5 cargo test -p ozzy-broker --test ozzy_broker_config \
  process::soak::three_host_crash_soak -- --ignored --exact --nocapture
```

The workload verifies every confirmed record/offset, releases verified oracle
payloads, churns saved producer/consumer identities, repairs slow-reader gaps,
and checks checkpoint replay. Sparse, burst, hot-partition, mixed-size, and empty
multipart traffic rotate with SIGKILL/orderly restart. Unclean RP copies are
quarantined for recovery. Process failure or 30 seconds without completion fails
the run. These are process-crash tests, not power-loss tests.

| Additional ignored case | Configuration |
| --- | --- |
| `process::soak::single::single_broker_varied_io_soak` | Single durable; same seconds/results/binary controls |
| `journals::serving::simulated::churn::memory_only_varied_churn_soak` | Real inproc brokers/SDKs; `OZZY_SOAK_POLICY=single/dq/rp` |
| `churn::long_running_accelerated_fault_churn` | Core schedules; `OZZY_CHURN_SECONDS` (3600), `OZZY_CHURN_SEED`, `OZZY_CHURN_RESULTS` |

Cross-host memory-only runs use `OZZY_SOAK_MEMORY=1` and copy the same broker
integration test executable as `memory-broker` into every run directory. Its
ignored `journals::serving::simulated::churn::host::memory_only_broker_process`
embeds the real broker over TCP with simulated files. Restart loses the store and
requires both remaining copies for recovery, including DQ. This is a test harness,
not a product storage mode or durability claim.
