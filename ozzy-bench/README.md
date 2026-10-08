# Ozzy benchmarks

Rust tools for verified Ozzy workloads, Iggy/Redpanda comparisons, profiling,
and Plotters SVG charts. See [the six charts](../BENCHMARKS.md) for published
workloads and measurement conditions. Run commands from the repository root;
each binary's `--help` lists all options.

## Setup

Requires Linux, Rust, and a block-backed benchmark disk. Set the artifact root
before building; its default is the system temporary directory.

```sh
export OZZY_ARTIFACT_ROOT=/path/to/benchmark-disk/ozzy
source scripts/ozzy_tools.sh
scripts/ozzy_cargo build --release -p ozzy-bench --features comparisons --bins
export PATH="$CARGO_TARGET_DIR/release:$PATH"
ozzy_compare --impl ozzy --check-only
```

`--check-only` runs formatting, Clippy, tests, and the worker build.
`--checks focused` limits it to formatting and the build; run affected tests
separately. `--no-build` requires the exact verified worker and matching inputs.

## Measurement rules

- Run builds, benchmarks, and profilers serially on an idle machine. Invoke built
  binaries directly. Pin clients and brokers to disjoint physical CPU sets.
- Keep disk, CPU placement, compiler, dependencies, payloads, and controls fixed.
  Compare the current tree with compatible cached results. Use at least two
  repetitions; overlapping ranges need more measurements.
- Ozzy/OMQ/runner warnings, timeouts, competing processes, source changes, or
  record verification failures invalidate a run. Native live readers must incur
  zero persisted-operation loads.
- Persistent runs require observable block backing. Comparisons/profiles flush
  before warmup; comparisons also wait for 5 s without device writes and require
  three 1 MiB `O_DSYNC` probes taking at most 60 ms each. The settle deadline is 300 s.
- Clean obsolete scratch and trim an idle SSD before sustained benchmark work.
  Keep append-only result JSONL under `~/.cache/ozzy/`; never commit it.

Artifacts live under `$OZZY_ARTIFACT_ROOT/ozzy-artifacts/runs/RUN_ID/`.
Worker build directories are isolated per checkout. Sources stay frozen during
a run; do not manually share Cargo targets across worktrees.

## Saturation comparisons

Single-broker durable:

```sh
ozzy_compare --impl ozzy --modes durable --sizes 128,1024,8192 \
  --partitions 8 --segment-mib 1024 --broker-cpus 0,1 --client-cpus 2,3,4,5 \
  --aio-depth 8 --writer-inflight-appends 3 --request-records 2048 \
  --writer-batch-target-kib 2048 --warmup 2 --duration 8 \
  --repetitions 2 --no-build
```

For both three-broker modes, replace the mode and CPU arguments with:

```sh
--modes disk-quorum,replicated-persisting --broker-cpus 0/1/2 --client-cpus 3,4,5
```

`/` separates broker CPU sets; a comma-only list is one shared pool. A single
broker uses their union. `--impl all`, `iggy`, or `redpanda` selects external
comparisons; run `--check-only` for that selection first.

| Control | Meaning |
| --- | --- |
| `--partitions` | Topic partitions; default 8, with one verified reader each |
| `--request-records` | APPEND/request record ceiling; Ozzy hard cap 2,048 |
| `--writer-batch-target-kib` | Ozzy APPEND payload target; independent of record cap |
| `--writer-inflight-appends` | Ozzy outstanding requests until fully confirmed |
| `--shards` / `--broker-io-threads` | Application shards / separate OMQ I/O workers |
| `--io-backend aio\|pool` | Linux AIO / bounded blocking workers |
| `--aio-depth` | Data writes in flight per broker device controller |
| `--patterns events\|random` | Varying event corpus / incompressible control |
| `--payload-compression off` | Disable Ozzy SDK/leader LZ4 for a control run |

Change one variable at a time. A short run without a segment roll does not
establish rollover performance.

## Fixed-load latency

Arrival rate totals all writers and is independent of confirmations. These ramps
match the chart ceilings: 100k records/s for 128 B and 1 KiB, 10k/s for 8 KiB.

```sh
ozzy_compare --impl ozzy --modes durable --sizes 128,1024 \
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

Use the cluster mode/CPU arguments above for each replicated mode. `--duration`
must equal the ramp total. One continuous case produces a result row per stage;
there is no new warmup between stages. An arrival unadmitted for 1 s stops the
ramp and marks that stage and later stages as failed loads.

Scheduled-arrival latency includes admission delay and final drain. Late records
keep their original timestamp. Never combine percentiles arithmetically.
At 100/s, 30 s gives only 3,000 samples; p99 needs repeated measurements.

## What is measured

| Mode | Ozzy confirmation | External configuration |
| --- | --- | --- |
| Durable | Local durable prefix | Iggy persisted; Redpanda one replica, write caching off |
| Disk quorum | Matching durable prefix on leader and one follower | Three replicas; Iggy persisted, Redpanda write caching off |
| Replicated-persisting | Matching retained bytes on leader and one follower | Three replicas; Iggy replicated, Redpanda write caching on |

All adapters get identical record bytes and partition counts. Readers verify
identities, partition order, sizes, and full-byte digests. Throughput counts
completions inside the measurement window; latency follows its submission cohort
through drain. Warmup is excluded. Saturation latency compares achieved loads.
Device counters cover the whole worker lifetime and are separate diagnostics.

Ozzy submits individual records; the SDK groups ready records without linger and
uses adaptive LZ4. Iggy explicitly batches ready records with one outstanding
request per connection. Redpanda uses librdkafka with zero linger, no compression,
and `acks=all`. Results retain each system's actual batching and request window.
Event payloads vary within each codec window; `random` is the compression control.

Iggy is pinned to unmodified server 0.9.0 with its matching SDK. Redpanda uses
the checksum-verified 26.2.2 native distribution. Each case provisions fresh
processes and checks effective settings. External warnings remain in logs;
errors, timeouts, and failed requests abort. RP drains persistence before final
success; that drain is outside confirmation latency and proves no crash recovery.

## Results and regression checks

Completed rows in `~/.cache/ozzy/` are append-only. Failed/interrupted rows remain
but do not supply measurements. Saved commands, hashes, settings, isolation
evidence, and logs accompany each run.

Compare matched baseline and candidate runs with at least two repetitions per
cell. This writes a report without generating charts:

```sh
ozzy_chart --run-id CANDIDATE --baseline-run-id BASELINE --modes durable \
  --regression-output "$OZZY_ARTIFACT_ROOT/regression-durable.json"
ozzy_chart --fixed-load --run-id CANDIDATE_SMALL --run-id CANDIDATE_LARGE \
  --baseline-run-id BASELINE_SMALL --baseline-run-id BASELINE_LARGE \
  --modes durable --max-rate 128:100000 --max-rate 1024:100000 \
  --max-rate 8192:10000 \
  --regression-output "$OZZY_ARTIFACT_ROOT/regression-durable-fixed.json"
```

Repeat for both replicated modes. Any cell losing at least 5% throughput or gaining
at least 5% confirmation/delivery latency fails. Missing metrics cannot pass;
changed measurement controls are incomparable. Reports retain medians, ranges,
and provenance. Investigate overlapping ranges instead of averaging cells.

## Charts

Generate charts only when requested. Preserve existing comparison series using
compatible cached runs; incomplete or incompatible inputs are rejected.

```sh
ozzy_chart --run-id OZZY_SATURATION --external-reference-run-id CACHED_COMPARISONS
ozzy_chart --fixed-load --run-id OZZY_SMALL --run-id OZZY_LARGE \
  --external-reference-run-id CACHED_FIXED_LOAD \
  --max-rate 128:100000 --max-rate 1024:100000 --max-rate 8192:10000
```

Each mode has a saturation SVG and a `-fixed-load.svg`: durable in
`doc/charts/single/`, disk-quorum and replicated-persisting in `doc/charts/cluster/`.
No buffered or diagnostic variants belong there. Chart inputs retain each
implementation's workload, batch ceiling, dependencies, and settings under
`$OZZY_ARTIFACT_ROOT/ozzy-chart-inputs/`. Failed loads are annotations, not samples.

## Profiling

Replay a verified native case separately from comparison timings:

```sh
ozzy_profile --case-dir "$OZZY_ARTIFACT_ROOT/ozzy-artifacts/runs/RUN/CASE" --kind cpu
```

| Kind | Output |
| --- | --- |
| `cpu` / `cpu-kernel` | Userspace / userspace and kernel stacks, measurement-window reports, raw `perf.data` |
| `syscall` | Segment `pwritev` sizes/durations, `writes.json`, raw strace |
| `stages` | Thread-owned counters over OMQ PUB/SUB; `--counter-trace` retains snapshots |

Defaults are 2 s warmup, 5 s measurement, 99 Hz sampling. Outputs go under
`$OZZY_ARTIFACT_ROOT/ozzy-profiles/`. `--current-build` replays with a newly verified
worker and records both hashes. Profile timings never enter comparison ledgers.

## Disk calibration

`ozzy_disk_probe` overwrites an explicitly named scratch file. It measures disk
writes, not broker throughput, replication, metadata publication, or confirmation.

```sh
scratch="$OZZY_ARTIFACT_ROOT/disk-scratch"
touch "$scratch"
ozzy_disk_probe --path "$scratch" --overwrite --mode direct-dsync \
  --write-kib 4096 --segment-mib 256 --file-mib 8192
```

Compare `buffered`, `dsync`, and `direct-dsync` with equal bytes and repeated,
alternating order. Each mode includes segment-boundary syncs and verifies sampled
bytes. Report sustained throughput, sample counts, write latency, and sync stalls;
short runs and SSD cache bursts cannot establish sustainable capacity. Qualify
candidate settings through real Ozzy at fixed load and saturation. See
[storage tuning](../doc/STORAGE.md#operator-tuning).

## Cross-host placement

Use `--placements placement.json --control-bind tcp://CONTROLLER:0` with three
broker entries. Clients remain on the controller, using one monotonic clock.

| JSON field | Meaning |
| --- | --- |
| `bind` | Reachable broker address |
| `storage_dir` | Absolute block-backed storage parent on that host |
| `cpus` | Optional broker CPU mask, verified for every thread |
| `ssh`, `executable` | Remote launch host and matching worker binary |

Native timed runs accept local/remote placements. Cross-host comparisons require
three distinct XFS devices and one CPU per broker; the remote process auditor
uses CPU 5. Remote hosts need SSH keys, `timeout`, and `taskset` for CPU masks.
Use a trusted network; SSH launches workers but does not authenticate OMQ links.
Saved placement and audit evidence distinguish these runs from local results.
Stop and reap all local/remote workers before starting another run.

## Other tools

| Tool | Boundary |
| --- | --- |
| `ozzy_timed_bench` | Native timed broker/SDK workloads; `--window` counts writers, `--partitions` counts partitions |
| `ozzy_workloads` | Serial native mode/worker/size sweep; `--dry-run` shows cases |
| `ozzy_segment_verify_bench` | In-memory production group decode/integrity; no disk or transport |
| `ozzy_lz4_dict_bench` | Codec CPU/dictionary experiment; no broker or persistence costs |

Direct disk workloads require `--storage-dir`; `TMPDIR` alone is insufficient.
For a quick correctness/throughput gate, run alone on six physical CPUs:

```sh
scripts/ozzy_cargo test -p ozzy-broker --test ozzy_broker_config \
  inproc_tmpfs_writer_broker_reader_throughput_gate -- --nocapture
```

See [Development](../DEVELOPMENT.md#test-layers) for test boundaries. Run/worker
control uses OMQ with bounded deadlines; stdout/stderr carry diagnostics and
completed results.
