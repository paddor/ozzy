# Ozzy benchmarks

Commands for verified Ozzy workloads, Iggy/Redpanda comparisons, profiling, and
SVG charts. [Benchmarks](../BENCHMARKS.md) shows the published charts. Run
commands from the repository root; each binary's `--help` lists every option.

## Setup

Requires Linux, Rust, and a block-backed benchmark disk. Set the artifact root
before building; it defaults to the system temporary directory.

```sh
export OZZY_ARTIFACT_ROOT=/path/to/benchmark-disk/ozzy
source scripts/ozzy_tools.sh
scripts/ozzy_cargo build --release -p ozzy-bench --features comparisons --bins
export PATH="$CARGO_TARGET_DIR/release:$PATH"
ozzy_compare --impl ozzy --check-only
```

- `--check-only` runs formatting, Clippy, the bench tests, and the worker build.
- `--checks focused` keeps only formatting and the build; run affected tests
  yourself.
- `--no-build` reuses the checked worker only while its inputs and binary hash
  still match.

## Measurement rules

- [ ] Run builds, benchmarks, and profilers serially on an idle machine. Invoke
  built binaries directly.
- [ ] Pin brokers and clients to disjoint physical CPU sets.
- [ ] Keep disk, CPU placement, compiler, dependencies, payloads, and controls
  fixed. Change one variable at a time.
- [ ] Use at least two repetitions. Overlapping ranges need more measurements.
- [ ] Compare the current tree with compatible cached results.
- [ ] Discard the run on any Ozzy/OMQ/runner warning, timeout, competing
  process, source change, or record verification failure.
- [ ] Clean obsolete scratch and trim an idle SSD before sustained work.
- [ ] Never commit result JSONL.

Comparisons and profiles flush before warmup. Comparisons also wait, for at
most 300 s, until the disk shows 5 s without writes and three 1 MiB `O_DSYNC`
probes take at most 60 ms each.

| Path | Contents |
| --- | --- |
| `~/.cache/ozzy/IMPL.jsonl` | Append-only result rows; failed rows supply no measurements |
| `$OZZY_ARTIFACT_ROOT/ozzy-artifacts/runs/RUN_ID/` | Commands, hashes, settings, isolation evidence, logs |
| `$OZZY_ARTIFACT_ROOT/ozzy-profiles/` | Profiler output |
| `$OZZY_ARTIFACT_ROOT/ozzy-chart-inputs/` | Validated chart inputs |

Worker builds are isolated per checkout. Do not share Cargo targets across
worktrees by hand.

## Saturation comparisons

Single-broker durable:

```sh
ozzy_compare --impl ozzy --modes durable --sizes 128,1024,8192 \
  --partitions 8 --segment-mib 1024 --broker-cpus 0,1 --client-cpus 2,3,4,5 \
  --aio-depth 8 --writer-inflight-appends 3 --request-records 2048 \
  --writer-batch-target-kib 2048 --warmup 2 --duration 8 \
  --repetitions 2 --no-build
```

Disk quorum: replace the mode and CPU arguments with

```sh
--modes disk-quorum --broker-cpus 0/1/2 --client-cpus 3,4,5
```

- `--broker-cpus`: `/` separates per-broker sets; a comma-only list is one
  shared pool. A single broker uses all of them.
- Each writer and reader process runs on one `--client-cpus` core, assigned
  in turn with readers after writers, so its application and SDK threads
  share caches.
- `--impl all|iggy|redpanda` adds external systems. Run `--check-only` with
  that selection first; it prepares the external servers.
- Ozzy APPENDs carry at most 2,048 records, even with a larger
  `--request-records`.
- `--patterns random` and `--payload-compression off` are compression controls.
- A short run without a segment roll says nothing about rollover performance.

## Fixed-load latency

These ramps match the chart ceilings: 1M records/s for 128 B, 100k/s for
1 KiB, 10k/s for 8 KiB. The rate totals all writers and ignores confirmations.

```sh
ozzy_compare --impl ozzy --modes durable --sizes 128 \
  --partitions 8 --segment-mib 1024 --broker-cpus 0,1 --client-cpus 2,3,4,5 \
  --aio-depth 8 --writer-inflight-appends 3 --request-records 2048 \
  --writer-batch-target-kib 2048 --warmup 2 --duration 62 \
  --ramp 100:30,1000:8,10000:8,100000:8,1000000:8 --repetitions 2 --no-build
ozzy_compare --impl ozzy --modes durable --sizes 1024 \
  --partitions 8 --segment-mib 1024 --broker-cpus 0,1 --client-cpus 2,3,4,5 \
  --aio-depth 8 --writer-inflight-appends 3 --request-records 2048 \
  --writer-batch-target-kib 2048 --warmup 2 --duration 54 \
  --ramp 100:30,1000:8,10000:8,100000:8 --repetitions 2 --no-build
ozzy_compare --impl ozzy --modes durable --sizes 8192 \
  --partitions 8 --segment-mib 1024 --broker-cpus 0,1 --client-cpus 2,3,4,5 \
  --aio-depth 8 --writer-inflight-appends 3 --request-records 2048 \
  --writer-batch-target-kib 2048 --warmup 2 --duration 46 \
  --ramp 100:30,1000:8,10000:8 --repetitions 2 --no-build
```

- Use the three-broker arguments above for disk quorum.
- `--duration` must equal the ramp total.
- One continuous case yields one result row per stage, with no warmup between
  stages.
- An arrival unadmitted for 1 s ends the ramp. That stage and all later stages
  are failed loads.
- Latency starts at the scheduled arrival and includes admission delay and
  final drain. Late records keep their original timestamp.
- A writer lane timer wakes at most every 250 us. Arrivals due sooner are
  admitted together at the next wakeup.
- Never combine percentiles arithmetically. At 100/s, 30 s gives only 3,000
  samples, so p99 needs repeated runs.

## Inside-out layers

Qualify a workload inside out and change one layer at a time.

| Layer | Flags |
| --- | --- |
| Threads over OMQ inproc, memory storage | `--memory-storage --inproc` |
| Processes over TCP, memory storage | `--memory-storage` |
| Processes over TCP, disk | none |
| Hosts | `--placements` (see [cross-host placement](#cross-host-placement)) |

```sh
ozzy_compare --impl ozzy --modes disk-quorum --sizes 200 --partitions 8 \
  --memory-storage --inproc --memory-mib 512 --segment-mib 64 ... --no-build
```

- `--inproc` and `--memory-mib` require `--memory-storage`.
- Memory storage keeps journals in a bounded image per broker behind the
  production file-job contract. `--memory-mib` counts dirty and durable copies,
  and one segment must fit, so use small segments.
- History only grows without retention. For runs longer than the image holds,
  use `ozzy_timed_bench --retention-mib N`; `ozzy_compare` has no retention flag.
- The layer is part of the recorded configuration, so layer rows never match
  disk baselines.
- Direct native `ozzy_timed_bench` runs need `--processes --network-ingress
  --streaming`; `ozzy_compare` adds them. Disk runs there also need an explicit
  `--storage-dir`; `TMPDIR` is not enough.

## Consumer groups

```sh
ozzy_compare --impl ozzy --modes disk-quorum --sizes 200 --partitions 8 \
  --groups 1 --group-members 4 --ack-window 1024 \
  --broker-cpus 0/1/2 --client-cpus 3,4,5 --aio-depth 8 \
  --writer-inflight-appends 3 --request-records 2048 \
  --writer-batch-target-kib 2048 --warmup 2 --duration 8 \
  --repetitions 2 --no-build
```

- Each group verifies the whole topic. Its members share one verifier on the
  group's reader process.
- Every record is acknowledged individually.
- Group rows add processing-confirmation p50, p99, and p99.9, measured from
  delivery.
- A run completes only after full-topic writer counts and confirmed processing.
- Handoff redeliveries are counted apart and must match retained unconfirmed
  identities and bytes.

## Deduplication

A balanced workload: two producers per feed, 80,000 unique events/s, and a
30-second identity window.

```sh
ozzy_compare --impl ozzy --modes disk-quorum --sizes 200 --partitions 8 \
  --redundant-producers 2 --records-per-second 160000 \
  --dedup-window-secs 30 --dedup-max-entries 330000 \
  --dedup-max-bytes 402653184 --broker-cpus 0/1/2 --client-cpus 3,4,5 \
  --aio-depth 8 --writer-inflight-appends 3 --request-records 256 \
  --reader-records 4096 --reader-payload-mib 8 \
  --writer-batch-target-kib 256 --segment-mib 1024 --warmup 2 --duration 30 \
  --repetitions 2 --no-build
```

For a partially redundant workload at 40,000 unique events/s, change these
flags: `--redundant-feeds 4 --records-per-second 60000
--dedup-max-entries 165000 --dedup-max-bytes 201326592`.

- The dedup flags install the production per-partition dedup policy.
- Redundant producers send the same ordered event IDs, bytes, routing key, and
  event clock.
- Redundant and skewed feeds require `--impl ozzy`, a dedup window, and a fixed
  `--records-per-second` (no `--ramp`).
- `--records-per-second` counts all attempts. At 80,000 unique events/s, use
  160,000 for two copies (50% duplicates) or 240,000 for three (66.7%).
- `--redundant-feeds K` copies only the first K feeds. With 8 partitions,
  `--redundant-producers 2 --redundant-feeds 4` runs 12 writers, and only
  partitions 0-3 see duplicates.
- `--hot-partition-percent P` routes P% of the feeds to partition 0 and must
  name an exact number of feeds.
- Warmup and measurement windows must hold complete physical-writer rounds.
- Readers verify each unique ID and its bytes once. SDK stored and duplicate
  counts must match the reader checks.
- With `--groups`, each group verifies the same unique records, so one case
  measures dedup and group progress together.

## What is measured

| Mode | Ozzy confirmation | External configuration |
| --- | --- | --- |
| Durable | Local durable prefix | Iggy persisted; Redpanda one replica, write caching off |
| Disk quorum | Matching durable prefix on leader and one follower | Three replicas; Iggy persisted, Redpanda write caching off |

- All adapters get identical record bytes and partition counts. Readers verify
  identities, partition order, sizes, and full-byte digests.
- Throughput counts completions inside the measurement window. Latency follows
  its submission cohort through drain. Warmup is excluded.
- Ozzy submits individual records; the SDK groups ready records without linger
  and uses adaptive LZ4.
- Iggy batches ready records with one outstanding request per connection.
- Redpanda uses librdkafka with zero linger, no compression, and `acks=all`.
- External versions are pinned in `src/automation/server.rs` and
  `src/automation/server/redpanda/build.rs`. Each case starts fresh processes
  and checks effective settings.
- External errors, timeouts, and failed requests abort the run; warnings stay
  in the logs.
- Host network counters include loopback. Snapshots from different brokers
  overlap; never sum them.

## Regression checks

Compare matched baseline and candidate runs with at least two repetitions per
cell. This writes a report and no charts:

```sh
ozzy_chart --run-id CANDIDATE --baseline-run-id BASELINE --modes durable \
  --regression-output "$OZZY_ARTIFACT_ROOT/regression-durable.json"
ozzy_chart --fixed-load --run-id CANDIDATE_SMALL --run-id CANDIDATE_LARGE \
  --baseline-run-id BASELINE_SMALL --baseline-run-id BASELINE_LARGE \
  --modes durable --max-rate 128:100000 --max-rate 1024:100000 \
  --max-rate 8192:10000 \
  --regression-output "$OZZY_ARTIFACT_ROOT/regression-durable-fixed.json"
```

- Repeat for `disk-quorum`.
- A cell fails when its median throughput drops at least 5%, or its median
  confirmation or delivery latency (p50, p99, p99.9) rises at least 5%.
- The gate accepts only the chart workloads.
- Missing metrics cannot pass. Changed measurement controls are incomparable.
- Investigate overlapping ranges; never average cells.

## Charts

Generate charts only when asked. Keep existing comparison series by reusing
compatible cached runs; incomplete or incompatible inputs are rejected.

1. Run the Ozzy workloads: saturation, and fixed load for each record size,
   each for durable and for disk quorum.
2. Render each run group with its cached external references:

   ```sh
   ozzy_chart --run-id OZZY_SATURATION --external-reference-run-id CACHED_COMPARISONS
   ozzy_chart --fixed-load --run-id OZZY_128 --run-id OZZY_1024 --run-id OZZY_8192 \
     --external-reference-run-id CACHED_FIXED_LOAD --ozzy-only-rate 1000000 \
     --max-rate 128:1000000 --max-rate 1024:100000 --max-rate 8192:10000
   ```

3. Check the output: `doc/charts/single/durable.svg` and
   `doc/charts/cluster/disk-quorum.svg`, each with a `-fixed-load.svg` sibling.

- `--ozzy-only-rate` marks an offered load the references do not measure.
  Iggy and Redpanda stop at 100k/s.
- Saturation charts take exactly one external reference run.
  `--iggy-reference-run-id` and `--redpanda-reference-run-id` attach Iggy and
  Redpanda from separate runs.
- Buffered and diagnostic variants never go in `doc/charts/`. Send them
  elsewhere with `--output-dir`.
- Failed loads are annotations, not samples.

## Profiling

Replay a verified native case apart from comparison timings:

```sh
ozzy_profile --case-dir "$OZZY_ARTIFACT_ROOT/ozzy-artifacts/runs/RUN/CASE" --kind cpu
```

| Kind | Output |
| --- | --- |
| `cpu` / `cpu-kernel` | Userspace / userspace and kernel stacks, window reports, raw `perf.data` |
| `syscall` | Segment write and kernel AIO syscalls, `writes.json`, raw strace |
| `stages` | Thread-owned counters and stage timing over OMQ PUB/SUB |

- Profile timings never enter comparison ledgers.
- `--current-build` replays with the current verified build and records both
  hashes.
- `--counter-trace` keeps the received counter snapshots.
- `--no-stacks` samples CPU instruction pointers only, at up to 4999 Hz.
  Stack samples stop at 999 Hz.
- Counter and stage names, with their meanings, live in
  `ozzy-telemetry/src/names.rs`.
- Refusal and wait counters include repeated handling of a held packet. They
  show pressure, not unique RPCs.

To collect telemetry without a profiler:

1. Bind an OMQ SUB socket to an IPC endpoint and subscribe to everything.
2. Start the broker with `OZZY_TELEMETRY_ENDPOINT=ipc://@ozzy-telemetry`.
3. Detect missed snapshots by sequence gaps; delivery is best effort.

[Runtime](../doc/RUNTIME.md) owns the snapshot contents and limits.

## Disk calibration

`ozzy_disk_probe` overwrites an explicitly named scratch file. It measures disk
writes, not broker throughput, replication, metadata publication, or
confirmation.

```sh
scratch="$OZZY_ARTIFACT_ROOT/disk-scratch"
touch "$scratch"
ozzy_disk_probe --path "$scratch" --overwrite --mode direct-dsync \
  --write-kib 4096 --segment-mib 256 --file-mib 8192
```

- Compare `buffered`, `dsync`, and `direct-dsync` with equal bytes, repeated in
  alternating order.
- Each mode syncs at segment boundaries and verifies sampled bytes.
- Report sustained throughput, sample counts, write latency, and sync stalls.
  Short runs and SSD cache bursts prove no sustainable capacity.
- Qualify candidate settings through real Ozzy at fixed load and saturation.
  See [storage tuning](../doc/STORAGE.md#operator-tuning).

## Cross-host placement

Pass `--placements placement.json --control-bind tcp://CONTROLLER:0` with three
broker entries. Clients stay on the controller, using one monotonic clock.

| JSON field | Meaning |
| --- | --- |
| `bind` | Reachable broker address |
| `storage_dir` | Absolute block-backed storage parent on that host |
| `cpus` | Optional broker CPU mask, verified for every thread |
| `ssh`, `executable` | Remote launch host and matching worker binary |

- `ozzy_compare` accepts all-local brokers, or two local and one remote, in
  `disk-quorum`. `--impl redpanda` cannot use placements.
- Cross-host comparisons need three distinct XFS devices and one CPU per
  broker. The remote process auditor uses CPU 5.
- Remote hosts need SSH keys, `timeout`, and `taskset`.
- Use a trusted network. SSH launches workers but does not authenticate OMQ
  links.
- Stop and reap all local and remote workers before the next run.

## Python SDK

For concurrent throughput, run separate producer, broker, and consumer processes:

```sh
scripts/ozzy_cargo build --release -p ozzy-python --example ozzy_python_pipeline
"$CARGO_TARGET_DIR/release/examples/ozzy_python_pipeline" --python /path/to/venv/bin/python \
  --broker-cpus 0,1 --producer-cpus 2,3 --consumer-cpu 4 \
  --codec lz4 --size 1024 --warmup 2 --duration 8 --repetitions 2
```

Each producer submits individual records. `flush()` confirms every stage;
`--confirmation each` additionally materializes each record's receipt. The
consumer verifies every byte, producer stream order, and partition-global
offsets. It returns benchmark credit every `--window` records per producer
(8,192 by default), bounding application backlog while all roles run concurrently.
Each partition receives an end marker from every producer before a stage ends.
Rates include final confirmation and consumer drain. Results also record CPU
time for each SDK process, including its native threads.

This uses TCP and the shared real-broker memory-storage harness, with a 1 GiB
image budget per broker and bounded retention. It does not measure SSD writes.
Segments default to 16 MiB and retention to 64 MiB per partition; both are
explicit controls (`--segment-mib`, `--retention-mib`) recorded in each result.
CPU sets must be disjoint. Repeat with `--api asyncio`, `--codec raw`,
`--transport lz4+tcp`, or `--mode quorum`. Quorum needs, for example, broker
CPUs `0,1,2`, producer CPUs `3,4`, and consumer CPU `5`.
The three quorum brokers share one child process; this runner measures no
process isolation between those brokers. `--reader-queue-messages 64` gives
the consumer more room for publication bursts; keep it fixed when comparing.
`--profile /path/to/profile --repetitions 1` saves cProfile reports per SDK
process outside the ledger. Use `--no-ledger` when running native profilers.
Successful rows use kind `python-pipeline`; compare matching controls and
workload hashes in `~/.cache/ozzy/python-sdk.jsonl`.

For a continuous 20-minute SDK soak, keep the same processes alive and omit
the benchmark ledger. The runner reports process RSS, peak RSS, and thread
counts every 30 seconds, and aborts after 45 seconds without protocol progress.
Use a 4 GiB image budget to leave room for concurrent retention cleanup:

```sh
"$CARGO_TARGET_DIR/release/examples/ozzy_python_pipeline" --python /path/to/venv/bin/python \
  --api asyncio --codec raw --size 1024 --reader-queue-messages 64 \
  --memory-mib 4096 --warmup 2 --duration 1200 --repetitions 1 --no-ledger
```

Build a release wheel, install it into a Python 3.12+ environment, and build
the Rust runner. Keep `PYO3_PYTHON` set to that interpreter while building.
The older combined-client runner measures sequential send/confirm/read windows:

```sh
scripts/ozzy_cargo build --release -p ozzy-python --example ozzy_python_bench
"$CARGO_TARGET_DIR/release/examples/ozzy_python_bench" --python /path/to/venv/bin/python \
  --api blocking --codec raw --size 1024 --broker-cpus 0,1 --client-cpu 2 \
  --warmup 2 --duration 8 --repetitions 2 --label baseline
```

Repeat with `--api asyncio`, `--codec lz4`, sizes `128, 1024, 8192` as separate
runs, and `--mode quorum`. Use `--memory-mib 1024` for 8 KiB cases, including
their baselines. The default image budget is 256 MiB per broker, counting dirty
and durable bytes. `--transport lz4+tcp` measures OMQ compression.
Profile separately with `--profile "$OZZY_ARTIFACT_ROOT/python.prof"` and
`--repetitions 1`; cProfile data and a text report stay outside the timing ledger.

This boundary uses TCP and the shared real-broker memory-storage harness.
One Python client sends 256 individual records, confirms them, reads all 256,
and independently verifies IDs, partitions, offsets, and every payload byte.
Four partitions use rolling retention. Warmup and measurement finish complete
windows; measured time includes submission, confirmation, reading, and checking.
This measures client overhead, not physical-disk throughput. CPU sets must be
disjoint. Run alone under the same isolation rules as other benchmarks.

Successful unprofiled rows append to `~/.cache/ozzy/python-sdk.jsonl`, including
interpreter version, extension/Python source hashes, source diff, and controls.
Compare matching cached baseline/candidate cells; do not combine these rows with
the eight-producer published chart workloads. Python stderr, broker warnings,
broker errors, or source changes invalidate a run. The runner polls broker
diagnostics and stops the child when a warning or error appears.
`--reader-queue-messages` defaults to 32. Compare it with one while keeping
other controls fixed to measure the effect of the Python SDK's burst window.

## Other tools

| Tool | Boundary |
| --- | --- |
| `ozzy_timed_bench` | Native timed broker/SDK workloads; `--window` counts writers, `--partitions` counts partitions |
| `ozzy_workloads` | Serial native mode/worker/size sweep; `--dry-run` shows cases |
| `ozzy_host` | Untimed host inspection (`inspect`) and remote isolation watch (`watch`) |
| `ozzy_segment_verify_bench` | In-memory segment-group decode and integrity; no disk or transport |
| `ozzy_lz4_dict_bench` | Codec CPU/dictionary experiment; no broker or persistence costs |

For a quick correctness and throughput gate, run alone on six physical CPUs:

```sh
scripts/ozzy_cargo test -p ozzy-broker --test ozzy_broker_config \
  inproc_tmpfs_writer_broker_reader_throughput_gate -- --nocapture
```

[Development](../DEVELOPMENT.md#test-layers) owns test boundaries.
