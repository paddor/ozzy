# Ozzy benchmarks

Rust runners and Plotters charts. Use `ozzy_compare` for Ozzy/Iggy/Redpanda
comparisons; other binaries isolate particular paths.

## Setup and rules

```bash
source scripts/ozzy_tools.sh
export PATH="$CARGO_TARGET_DIR/release:$PATH"
scripts/ozzy_cargo build --release -p ozzy-bench --features comparisons --bins
ozzy_compare --impl ozzy --check-only
ozzy_compare --impl ozzy --modes durable,replicated-persisting \
  --sizes 128,1024,8192 --partitions 8 --segment-mib 1024 \
  --broker-cpus 0/1/2 --client-cpus 3,4,5 \
  --aio-depth 8 --writer-inflight-appends 3 --request-records 2048 \
  --writer-batch-target-kib 2048 \
  --warmup 2 --duration 8 --repetitions 3
```

- Run builds, benchmarks and profilers **serially** on an otherwise idle machine.
  Pin brokers and clients to disjoint CPU sets. Keep host scheduling and device
  settings fixed; repeat the baseline after changes. Clean obsolete artifacts
  and trim the idle SSD during daily sustained benchmark work.
- Use the mounted SSD for storage. Native disk commands need
  `--storage-dir /mnt/ssd/tmp/ozzy-bench`; `TMPDIR` alone is insufficient.
- Comparisons and profiles check the mount and run `sync -f /mnt/ssd/tmp`
  before warmup. Flush failures stop the run.
- Before each comparison case, `ozzy_compare` waits for 5 s without completed
  device writes, then requires three 1 MiB `O_DSYNC` probe writes of at most
  60 ms each. A device that does not settle within 300 s
  stops the run. Rows record the wait and probes as `device_settle`.
- Ozzy/runner warnings, timeouts, competing processes, source changes, lost records and
  persisted-operation loads by live readers invalidate measurements.
- Results append under `~/.cache/ozzy/`. Only SVGs belong in `doc/charts/`.
  Never commit result JSON.
- Invoke built binaries directly. A resident `cargo run` parent invalidates
  audited comparison measurements.

### Fast inproc gate

```sh
scripts/ozzy_cargo test -p ozzy-broker --test ozzy_broker_config \
  inproc_tmpfs_writer_broker_reader_throughput_gate -- --nocapture
```

Run this test alone on an idle six-core machine before process comparisons.
It pins the broker shard, dispatcher, two disk workers, writer/SDK/OMQ, and
reader to separate physical CPUs. It uses OMQ inproc and 1 MiB segments under
`/tmp` tmpfs. The test verifies all received record IDs and bytes at 128 B,
1 KiB, and 8 KiB, then checks conservative throughput floors. A shared VM can
still interfere with a wall-clock floor; rerun an isolated failure before
attributing it to code.

## Repeatable storage comparisons

```sh
ozzy_compare --impl all --broker-cpus 0,1 --client-cpus 2,3,4,5 --repetitions 2
ozzy_compare --impl ozzy --broker-cpus 0,1 --client-cpus 2,3,4,5 --repetitions 2
ozzy_compare --impl redpanda --broker-cpus 0,1 --client-cpus 2,3,4,5 --repetitions 2
ozzy_compare --impl ozzy --check-only
```

`--check-only` runs formatting, lint, tests and the release build.
For tiny-record experiments, run focused correctness tests, then:

```sh
ozzy_compare --impl ozzy --modes buffered --sizes 16 \
  --broker-cpus 0,1 --client-cpus 2,3,4,5 --request-records 1024 \
  --warmup 1 --duration 3 --checks focused
```

`--checks focused` runs formatting and the worker build. Run affected unit tests
before measuring; run Clippy and the full test suite before committing.
Isolation, source fingerprints, warnings and reader verification stay enforced.
Short runs screen experiments; repeat promising changes before keeping them.
Broker sockets use dedicated OMQ I/O threads. `--broker-io-threads` sets their
count; application shards stay separate.

`--no-build` requires matching worker inputs and the exact verified binary.
Local dependencies, including dirty and untracked files, are fingerprinted.
Fixture orchestration and charts have separate source provenance, so editing
them does not force new measurements. All sources stay frozen during each run.
Workers build in separate directories per checkout under
`/mnt/ssd/tmp/cargo-target/checkouts/`. Comparison, profiling, and workload
runners select that checkout's worker. Never share a Cargo target directory
between source worktrees when manually building benchmark binaries.

`all` includes Redpanda for single-broker and three-broker modes. The runner
downloads the pinned 26.2.2 native distribution, verifies its archive checksum,
invokes its bundled
loader from the SSD, and records executable/library hashes. Each case gets
a fresh deployment, 4 GiB memory per broker, and the assigned broker CPUs.
No container or JVM.
Private setup uses `curl` and `tar`; no system installation.
External broker and SDK warnings stay in the logs. Errors, failed requests,
timeouts, and verification failures abort. Ozzy, OMQ, and runner warnings
remain fatal. Nonempty client logs
are kept under `~/.cache/ozzy/client-logs/` and linked from each result.

Redpanda clients use Rust `rdkafka` with compiled C `librdkafka`, `acks=all`,
one copy per broker, no compression, and zero linger. Disk confirmation topics
disable `write.caching`; replicated-persisting topics enable it. Developer mode
and `unsafe_bypass_fsync` stay disabled. Effective topic settings include the
requested segment size. All three adapters support three-broker comparisons.

### Workload and controls

Comparisons create one logical topic in each system. Iggy contains that topic
in one stream namespace. Every adapter receives the same explicit partition
count and reports its effective topology. Direct `ozzy_timed_bench` runs use
`--window` for writer count and `--partitions` for partition count (default 16).
Several writers may share a partition. Offsets remain global within that
partition; writer order and full-byte digests are checked independently.

| Control | Default / meaning |
| --- | --- |
| `--sizes` | `128,1024,8192` bytes |
| `--modes` | `durable,replicated-persisting`; also `buffered,disk-quorum`. The default covers single-broker durable and three-broker replicated-persisting comparisons |
| `--warmup`, `--duration` | 5 s warmup, 10 s measurement |
| `--repetitions` | 1; use at least 2 when judging changes |
| `--partitions` | 8, with one reader connection per partition |
| Writer processes | Four processes for eight partitions. Each owns two partition-targeted SDK writer lanes. |
| `--live-readers` | Off. Ozzy disk groups only: the leader publishes confirmed records once per partition and readers take live records from that publication; history and gaps still use their subscription. Each reader is verified exactly as before |
| `--readers-per-partition` | 1. Ozzy only: each additional reader is its own process and verifies every record. One group admits at most 32 readers. Delivery rate is the mean per reader; delivery latency covers all readers |
| `--shards` | Native local shards; default minimum of partitions and broker CPU count |
| `--request-records` | Native APPEND record ceiling and queue capacity per slot (queue minimum 1024); Iggy's records per request. Native charts use 2048. |
| APPEND packing | SDK and leader use adaptive LZ4 at/above 2 KiB |
| `--payload-compression off` | Ozzy only: no SDK LZ4 and no leader packing; plain payload bytes on the wire, in replication and on disk |
| SDK grouping | Comparisons collect available records into SDK APPENDs, no linger |
| Broker proposals | Each SDK APPEND becomes one partition-local proposal. The frontend does not combine APPENDs from several writers |
| `--writer-inflight-appends` | 1 by default; charts use 3. SDK request cap until full confirmation. |
| `--writer-batch-target-kib` | 832 by default; native SDK APPEND payload target. Record, segment, and broker's 8 MiB message bounds clamp the effective cap. Results verify the effective bytes. |
| `--reader-records` | 16384 records per native delivery / Iggy poll |
| `--reader-payload-mib` | 128 MiB reply cap; record count is reduced to fit |
| `--segment-mib` | Physical segment limit: 1 GiB for records of at least 1 KiB, else 256 MiB |
| `--disk-history-mib` | External cases only; production brokers use their own history bounds |
| `--direct-io` | `true`; replicated groups write segment data through a separate `O_DIRECT` descriptor from 4 KiB-aligned staging (`O_DSYNC` too in disk quorum). Headers, zeroing and reads stay buffered |
| `--io-backend pool\|aio` | `aio`: backend-owned workers submit segment data writes through Linux kernel AIO. Needs `--direct-io true`. `pool` hands them to the writer threads |
| `--aio-depth` | `1`; kernel AIO data writes in flight per broker device controller, 1 to 64. Shards share this bound. Power-loss recovery discards a hole and later writes only after the durable position |

Native writers use transparent APPEND grouping over
[separate data/control PEER sockets](../doc/PROTOCOL.md). Live readers retain broker PUB and SDK SUB.
Saturation and fixed-load callers bound ready work to 256 records or 2 MiB per
turn. Ready confirmations drain in batches; every receipt and latency sample
is still checked individually.
Local SDK request/byte bounds and OMQ backpressure bound outstanding records. Sparse
traffic sends single-record APPENDs without waiting for more arrivals.
`--request-records` sets the APPEND record ceiling and queue capacity per slot
(at least 1,024 queue entries); grouping has no linger. One APPEND
has at most 2,048 records. Charts use 2,048 records and a 2 MiB byte target.
`--writer-inflight-appends` bounds those requests
until fully confirmed.
At 8 KiB, the default byte target permits about 104 records per APPEND.
Increase `--writer-batch-target-kib` independently of the record queue to measure
larger batches. Sparse traffic still sends promptly, without waiting to fill them.
The live reader queue keeps at most 64 frames and a 52 MiB payload window;
larger frames reduce its slot count. Results record that effective queue size.
Results record and verify the selected protocol against actual send counters.
Keep each result's measured transport label; a protocol decision does not relabel
older measurements or charts.
Iggy permits one outstanding request per connection; Ozzy defaults to one.
The per-request record ceiling matches, but total in-flight windows differ.
Results and charts retain each implementation's actual ceiling.
For the full size sweep, use
`--sizes 16,32,64,128,256,512,1024,2048,4096,8192,16384`.

Replicated-persisting confirms after replication while background persistence
drains within broker-owned bounds. Roll and clean shutdown synchronize files;
ordinary write completion is not a durability claim. Profile actual syscall
sizes separately from comparison timings.

Results include SSD request, byte, flush, and wait-time counters over each whole
worker lifetime. These diagnostics include startup, warmup, and drain; they are
separate from the measured throughput window.

Each writer reserves at least 1024 preparation records per APPEND slot;
otherwise capacity follows the request record ceiling, capped at 2048 for Ozzy.
Bounded queues feed SDK preparation and I/O. Local shards collect received
requests independently. Hard record/protocol bounds remain separate.

Partial confirmations release confirmed records and their retained-byte budget, but retain
the APPEND slot until its final record. Producer inboxes remain finite. Iggy's adapter permits one outstanding request per
connection. Reader limits are independent of writer batching.

Iggy's adapter builds a vector of up to `--request-records` messages, calls
`TcpClient::send_messages()`, and waits for confirmation before building the
next vector. This is explicit batching in benchmark client code. Fixed-load
runs collect only records already due, without waiting to fill the vector.
Iggy also offers automatic background batching, but those sends return after
queueing without broker confirmations; they cannot supply this benchmark's
writer-confirmation timings.

Redpanda receives individual SDK submissions through rust-rdkafka. librdkafka
batches transparently with zero linger, up to `--request-records` records or
4 MiB; the adapter bounds unconfirmed records by
`--request-records`. Fixed-load runs observe deliveries while awaiting the next
scheduled arrival, preserve late arrivals, and include admission delay in latency.

**Payloads:** `--patterns events` uses a 16 B binary event and JSON events at
larger sizes. Binary fields: 8 B monotonic submission clock, four single-byte
fields, and a varying 4 B integer; integers are big-endian. JSON is the event
stream of OMQ's compression benchmark: five newline-separated event kinds after
the 8 B clock, cut at the exact record size, with no filler text. Each writer
lane cycles through a pool of distinct bodies built once: 8 MiB of bodies, and
at least one request's records. No codec window or request sees a body twice.
The clock is read every 64 records. Bodies are built before measurement;
record assembly, clock reads, and full-byte verification are timed. The final event may be cut mid-field to keep the exact requested size;
consumers verify the original bytes rather than parsing them as JSON.

The maximum-APPEND corpus check finds no duplicate bodies or complete events.
With zero clocks, LZ4 retains 25.5%, 26.7%, and 26.3% of raw bytes at
128 B, 1 KiB, and 8 KiB. Tests require 20%-50% retention at all three sizes.
This checks the event workload, not a universal application compression ratio.
Use `--patterns random` for the incompressible control.

`--patterns random` supplies an incompressible control; `structured` retains the
older repetitive corpus for diagnostics. Explicit `json` requires sizes above
16 B. Corpus identity is checked before accepting results.

### CPU allocation

This VM has six vCPUs pinned to separate physical host cores. The default
`--broker-cpus 0/1/2 --client-cpus 3,4,5` gives each of three brokers one
core, and clients the other three. `/` separates broker CPU lists. A plain list
such as `0,1,2,3` is one pool that all brokers share. A single broker uses the union.
Iggy shards and Redpanda reactors per broker follow the CPU count of the
broker's list, divided among brokers that share it. All brokers use one SSD.

For a single broker on two cores, use:

```sh
--broker-cpus 0,1 --client-cpus 2,3,4,5
```

That gives two application/journal shards, four partitions/writers/readers and
two simultaneous writes on separate physical cores. Refresh all implementations
when changing allocation. Host activity can still cause interference.
The comparison command starts native processes in the selected CPU pool and
records their observed placement.

### Confirmation and measurement boundaries

| Series | Writer confirmation |
| --- | --- |
| Ozzy buffered | Local buffered append; no stable-storage barrier |
| Ozzy durable | Local O_DSYNC write completed |
| Ozzy disk quorum | Recoverable durable copies on two of three brokers |
| Ozzy replicated-persisting | Two validated RAM copies; bounded background O_DSYNC writes |
| Iggy replicated | Replicated and applied; storage still writes in background |
| Iggy persisted | Recoverable durable copies; one locally, two in a three-broker group |
| Redpanda buffered | `acks=all`, one broker, topic write caching enabled |
| Redpanda durable | `acks=all`, one broker, topic write caching disabled; fsync before reply |

Native readers use `Reader::into_records()` for raw fallbacks and packed APPENDs.
Disk groups retain each exact producer block through replication and storage;
full-group reader delivery forwards it for SDK decoding. Canonical storage keeps
no raw payload duplicate; partial historical reads decode lazily. Results record
the adaptive policy separately from physical segment encoding. Main charts contain
one Ozzy series. Forced physical-codec experiments retain results in artifacts.

Readers check identities, offsets and sizes per record; writer and reader
digests over every payload byte must match. Throughput counts
completions inside the measurement window; latency follows submissions inside
that window through final drain. Warmup is excluded. Reader backlog and drain
remain visible. Saturation p99 compares systems at their own achieved loads.

Subscribed readers must stay within each broker's resident read budget. Even
one persisted-operation load invalidates a native comparison. Segment, index,
history and host-memory budgets are checked before timing.

Disk-group reader replies remain bounded by the journal's per-operation record
ceiling.

`replicated-persisting` uses the regular warmup/duration and disk history limits.
Broker rows report sampled confirmation-to-persistence lag and bounded pending
bytes/operations. Sampling can miss peaks. Final snapshots require all accepted
operations persisted; that drain remains outside writer latency measurement.
See [restart limits](../doc/REPLICATION.md#receipt-and-repair).

### Iggy and isolation

Server: unmodified `server-0.9.0`, revision
`71f29618ba04917fded0ef5cd085c206fef0ee49`, matching SDK, release/mimalloc build,
Web UI disabled. Checkout, build outputs and private dependencies stay on SSD.
Ozzy's MSRV is unchanged by Iggy's newer toolchain.

Each case gets fresh processes and verified topic settings. Iggy client
registration and final logout run serially; measured writers/readers run
concurrently. After reader verification, an untimed filesystem flush drains
pending writes before logout. Broker logs are audited, then disposable servers
are killed and reaped. Graceful server shutdown/recovery is a separate test.

The runner holds a process lock and audits competing processes every 100 ms.
Paused servers also count as competition. Every case records isolation evidence
and an empty process check after shutdown. Cluster cases require all three
brokers ready within the shared CPU budget.

### Results

| Path under `~/.cache/ozzy/` | Contents |
| --- | --- |
| `ozzy.jsonl`, `iggy.jsonl` | Append-only measurements and run status |

Commands, identities, diagnostics and raw output go under
`/mnt/ssd/tmp/ozzy-artifacts/runs/RUN_ID/`.

Only completed runs count. Failed/interrupted rows remain stored but are
excluded. Summaries retain medians and repetition ranges. Ozzy-only runs leave
external ledgers untouched. Reuse a baseline only when workload, adapter,
dependencies, environment and measurement settings match.

### Fixed-load latency

All three implementations use the same scheduled-arrival clock and record checks.
Arrival rate is independent of confirmations and totals all four writers.

```sh
ozzy_compare --impl all --check-only
ozzy_compare --impl all --modes durable --sizes 128,1024 \
  --partitions 8 --segment-mib 1024 --broker-cpus 0,1 \
  --client-cpus 2,3,4,5 --warmup 2 --duration 54 \
  --ramp 100:30,1000:8,10000:8,100000:8 --repetitions 1 --no-build
ozzy_compare --impl all --modes durable --sizes 8192 \
  --partitions 8 --segment-mib 1024 --broker-cpus 0,1 \
  --client-cpus 2,3,4,5 --warmup 2 --duration 46 \
  --ramp 100:30,1000:8,10000:8 --repetitions 1 --no-build
```

For cluster runs, use `--modes replicated-persisting` or
`--modes disk-quorum` and `--broker-cpus 0/1/2 --client-cpus 3,4,5`.
Redpanda group modes
run three brokers on the broker CPUs, one reactor each, with replication factor
3 and `acks=all`: `write.caching=false` for disk quorum, `true` for
replicated-persisting.

The ramp is one continuous offered load per case; `--duration` must equal its
total. It does not warm up at each new rate, so stage-transition tails differ
from standalone fixed-rate runs. Every stage becomes its own result row. When a
writer leaves an arrival unadmitted for 1 s,
the ramp stops: that stage and later ones become chart annotations,
while earlier stages keep complete scheduled cohorts.

To extend existing charts, run only missing rates, then pass both the existing
and new completed run IDs to `ozzy_chart --fixed-load`. Do not repeat covered
cells or replace historical result rows.

Use `--impl ozzy`, `--impl iggy`, or `--impl redpanda` for one implementation. Iggy sends available
due records without waiting to fill a request; its SDK allows one pending request
per connection. Iggy reader latency includes offset polling and the existing
1 ms timer after empty polls. Ozzy readers receive pushed records; Redpanda uses
librdkafka's asynchronous fetch loop.

| Summary field | Boundary |
| --- | --- |
| `scheduled_ack_p50_us`, `scheduled_ack_p99_us` | Scheduled arrival to confirmation |
| `scheduled_delivery_p50_us`, `scheduled_delivery_p99_us` | Scheduled arrival to verified delivery |
| `scheduling_lag_*` | Scheduled arrival to actual generation |
| `ack_*`, `delivery_*` | Actual generation to confirmation/delivery |
| `offered_s`, `submitted_s`, `confirmed_s`, `delivered_s` | Demand versus completed work |
| `scheduled_samples` | Every planned measured arrival, including final drain |

Pacing uses absolute monotonic `timerfd` deadlines, without millisecond rounding
or busy waiting. Late admission never resets timestamps. An arrival still
unadmitted 1 s after its due time fails the run. Never add or subtract percentiles. At 100 records/s,
30 seconds supplies 3000 samples; longer repeated runs are needed for stable p99.
Partial reruns use `--replace-run-id ID` alongside the full `--run-id`.
Only existing Ozzy cases from an identical worker binary and matching controls
can replace measurements; other cases and both runs' provenance remain.
Charts refuse to drop existing systems, record sizes, or offered rates unless
an explicit per-size `--max-rate SIZE:RATE` excludes those rates. The six release
charts stop at 100k/s for 128 B and 1 KiB, and 10k/s for 8 KiB; higher loads belong
in separate saturation experiments. Raw ledger measurements remain intact.

Keep fixed-load results separate from saturation curves.

### Size and concurrency sweeps

Change one variable at a time. Keep CPU allocation, readers, payload encoding,
segment sizes and confirmation policy fixed. Use the same sustainable offered
load as well as saturation. This example varies application shards across
three broker CPUs and reserves three other CPUs for clients. Check host
physical-core placement first.

```sh
set -e
storage=(--impl ozzy --modes durable --sizes 8192
  --broker-cpus 0,1,2 --client-cpus 3,4,5
  --warmup 5 --duration 10 --repetitions 2)
ozzy_compare "${storage[@]}" --check-only
for shards in 1 2 3 4; do
  ozzy_compare "${storage[@]}" --shards "$shards" --no-build
done
```

Application shards spread partitions across threads. The shared storage backend
bounds device jobs. Keep SDK request and confirmation limits fixed. Syscall
profiles show actual write sizes.

### Disk calibration

`ozzy_disk_probe` compares buffered, `O_DSYNC`, and `O_DIRECT|O_DSYNC` writes on
Linux. It overwrites an explicitly named existing scratch file. It is a disk
probe, not a broker benchmark: no record construction, replication, metadata
roll, or confirmation latency. Results append to `~/.cache/ozzy/disk-probe.jsonl`.

```sh
cargo build --release -p ozzy-bench --features comparisons --bin ozzy_disk_probe
scratch=/mnt/ssd/tmp/disk-scratch
touch "$scratch"
# Run built binary directly, with the VM otherwise idle.
ozzy_disk_probe --path "$scratch" --overwrite --mode direct-dsync \
  --write-kib 4096 --segment-mib 256 --file-mib 8192
```

Each case warms up, then truncates/preallocates fresh unwritten extents before
measurement. EOF stays fixed. One synchronous writer drains a prepared nonzero
payload buffer; `sync_data()` runs at each simulated segment boundary in all
modes. Boundary samples are verified after timing. Filesystem alignment and
effective open flags are recorded. `--staging-copy` includes a payload copy into
the aligned buffer; preparation of the deterministic corpus remains untimed.

1. Sweep write size with segment size fixed. Compare `--mode dsync` and
   `--mode direct-dsync` in alternating order, then reverse size and mode order.
   Use equal bytes and at least two repetitions. Increase run length when SSD
   cache/thermal effects or host activity change results. Zero-filled `dd`
   results and this nonzero-payload probe are separate workloads.
   Record throughput drift; do not average fast and slower regimes into a
   claimed sustainable rate. Establish repeatable capacity under sustained
   writes before sizing production backlog or selecting defaults.
2. For background persistence, include `--mode buffered`. Its throughput includes
   segment data barriers, so dirty RAM alone cannot inflate the final result.
   Sweep segment size separately; the probe does not reproduce Ozzy's metadata
   publication or overlapped maintenance. Never compare buffered write p99 alone
   with synchronous write p99; include segment-sync stalls.
3. Report sample counts, write/batch p50/p99/max, segment-sync times, and sustained
   MiB/s. Large writes yield few samples; short runs cannot establish stable p99.
   A useful throughput candidate is the smallest write reaching 90% of the best
   repeated rate, subject to a separately stated latency budget. It is not a
   universal optimum or proof of the device's absolute ceiling.
4. Qualify candidates through real Ozzy at fixed load and saturation. Preserve
   record size, SDK windows, encoding, shard/device concurrency, and RAM bounds.
   Check confirmation p99, persistence lag, peak memory, rolls, repair traffic,
   and zero persisted-operation loads for live readers. Measure leader and each
   follower separately, including promotion and a permanently slow disk.

The probe's direct mode does not enable direct I/O in Ozzy. See
[operator tuning](../doc/STORAGE.md#operator-tuning) and
[Linux direct-I/O alignment](https://man7.org/linux/man-pages/man2/open.2.html).

### Profiling

Replay a verified native case, separately from comparison timings.

```sh
ozzy_profile --case-dir /mnt/ssd/tmp/ozzy-artifacts/runs/RUN/CASE --kind syscall
ozzy_profile --case-dir /mnt/ssd/tmp/ozzy-artifacts/runs/RUN/CASE --kind cpu
ozzy_profile --case-dir /mnt/ssd/tmp/ozzy-artifacts/runs/RUN/CASE --kind cpu-kernel
ozzy_profile --case-dir /mnt/ssd/tmp/ozzy-artifacts/runs/RUN/CASE --kind stages
```

Defaults: 2 s warmup, 5 s measurement; override with `--warmup` and `--duration`.
Profiles go under `/mnt/ssd/tmp/ozzy-profiles/`. CPU sampling defaults to 99 Hz;
use `--frequency 499` for more samples. Profile timings are not comparisons.
Comparison builds report each broker's resident/historical delivery counts and
shared/copied reader bytes under `usage.record_deliveries`. These counters cover
the process lifetime, including warmup and drain.
Stage profiles publish thread-owned counters over local OMQ PUB/SUB.
The profiler binds abstract IPC, merges thread snapshots, and prints live
repair and refusal alerts. Shards publish directly without a shared counter table.
Add `--counter-trace` to a stage profile to retain timestamped SUB snapshots
under `/mnt/ssd/tmp/ozzy-profiles/` for correlation after the run. Only the
collector writes the artifact, after all workers stop.
CPU profiles disable ASLR to avoid stale parent mappings in `perf`'s unwinder.
Profiles place the controller on saved client CPUs. Production broker placement
comes from the case command; the old worker-affinity environment is not replayed.
The same verification, source-stamp, and isolation gates apply. Profiles verify
broker CPU masks and the production PUB/SUB reader path. Cold gap repair is allowed.
After rebuilding with `ozzy_compare --impl ozzy --check-only --checks focused`,
use `--current-build` to profile the saved workload with the new verified worker.
The profile records both executable hashes and deploys the worker to remote hosts.
CPU reports select the exact measurement window and separate brokers, writers
and readers. Startup allocation, warmup and final drain are excluded from those
sampled CPU reports; raw `perf.data` retains the complete process lifetime.

| Kind | Meaning |
| --- | --- |
| `syscall` | Successful segment `pwritev` sizes/durations, `writes.json` and raw strace |
| `cpu` | 99 Hz userspace stacks, combined/per-broker reports and `perf.data` |
| `cpu-kernel` | Same sampling, including kernel CPU work; requires kernel profiling permission |

Profiling rows never enter comparison ledgers.

### SVG charts

```sh
ozzy_chart --run-id LOCAL_RUN --run-id CLUSTER_RUN
ozzy_chart --run-id NEW_OZZY_RUN --iggy-run-id COMPATIBLE_BASELINE_RUN
ozzy_chart --run-id NEW_OZZY_RUN --iggy-reference-run-id CACHED_IGGY_RUN
ozzy_chart --run-id NEW_OZZY_RUN --iggy-reference-run-id CACHED_IGGY_RUN \
  --redpanda-reference-run-id CACHED_REDPANDA_RUN
ozzy_chart --run-id BOTH_MODES_RUN --modes durable \
  --iggy-reference-run-id CACHED_SINGLE_BROKER_RUN
ozzy_chart --run-id NEW_OZZY_RUN --external-reference-run-id CACHED_COMPARISON_RUN
ozzy_chart --fixed-load --run-id RATE_100_RUN --run-id RATE_1000_RUN \
  --run-id RATE_10000_RUN --run-id RATE_100000_RUN
ozzy_chart --fixed-load --run-id NEW_OZZY_RUN \
  --external-reference-run-id CACHED_RATE_100_RUN \
  --external-reference-run-id CACHED_OTHER_RATES_RUN
ozzy_chart --fixed-load --run-id EXISTING_RUN \
  --max-rate 128:100000 --max-rate 1024:100000 --max-rate 8192:10000
```

Finish benchmark work by refreshing the affected SVGs before committing.
Refreshing an existing SVG must preserve its measured series. The renderer
rejects missing comparison lines before overwriting any chart. Include compatible
cached baseline runs. Keep diagnostic output in SSD artifacts.
An explicit `--iggy-reference-run-id`, `--redpanda-reference-run-id`, or
`--external-reference-run-id` keeps an independently validated cached
reference when the workload revision or SDK request ceiling differs. Host,
compiler, payload pattern, sizes, timing and other controls must still match.
The chart shows each implementation's batch ceiling. This is not an
identical-workload baseline; both provenances remain in
the cached chart input. External references retain their original dependency
provenance; current Ozzy-only OMQ/fanring changes do not invalidate them.
References may span revisions across runs. Each run must remain internally
consistent; chart inputs keep its workload fingerprint and controls separately.
Prefer reuse over rerunning unchanged external systems.
Native APPEND windows, disk settings, and live-reader delivery may differ from
cached external rows. Their adapters do not use those controls. Captions and
provenance retain the native settings. Host, workload shape, timing, and
durability checks still apply.
Use `--modes durable` or `--modes replicated-persisting` to select part of a
completed run. References must cover every selected case. Mode filtering keeps
overload annotations and original run provenance.

- Exactly six charts: `doc/charts/single/durable.svg`,
  `doc/charts/cluster/disk-quorum.svg`, and
  `doc/charts/cluster/replicated-persisting.svg`, plus each mode's
  `-fixed-load.svg` form. No buffered or suffixed variants in `doc/charts/`.
- OMQ style: black background, shared fonts/palette/grids, lines and measured dots.
- Titles include `at saturation` or `at fixed load`. Hardware subtitles use the
  recorded CPU model. Optional repo-root `.chart_hw` uses OMQ's `prefix` and
  `postfix` keys, joined around that model. This file is local and gitignored:

  ```text
  prefix=Linux VM on a 2018 Mac Mini
  postfix=6 cores, performance governor, turbo off
  ```

- Writer confirmation left, verified reader right. Throughput above latency.
- Each throughput panel overlays dashed records/s (left axis) and solid decimal
  MB/s (right axis), across every measured size. Higher is better.
- Latency uses a 0-400 ms linear scale. Solid P99 lines and
  thin P50-P99.9 whiskers use repetition medians. Triangles mark values above
  400 ms. Missing P99.9 leaves only the P50-P99 lower whisker. Backlog-limit
  labels sit on the right of each fixed-load panel.
- Throughput whiskers retain the min/max across repetitions.
- Footer: series, writer count and explicit request ceiling. No journal-byte panel.
- Generate charts only when explicitly requested. Refresh these six paths.
  Chart-input artifacts go under `/mnt/ssd/tmp/ozzy-chart-inputs/`.

Selection rejects incomplete repetitions, mixed Ozzy revisions, incompatible
settings and missing audit evidence. No smoothing or extrapolation. Timing
windows remain in the saved chart inputs and benchmark description.

Fixed-load charts use `single/` or `cluster/`, named `MODE-fixed-load.svg`: one
panel per record size, top to bottom; offered records/s on X, scheduled-arrival
latency on Y. Each system's writer confirmation uses its color and its verified
reader a lighter shade; P99 lines and P50-P99.9 whiskers use the same style as
saturation latency panels. Select one Ozzy build;
include compatible Iggy and Redpanda runs with the reference flags above.
Durations may differ between rates, but must match between
sizes and modes at each rate. Saturation files remain separate.
Use `--failed-run-id ID` for an explicitly selected single-case scheduled-backlog
failure. It adds an annotation and a gap when no completed attempt exists.
A separate failed repeat keeps the completed attempt plotted and labels the
repeat. Failures supply no latency samples. Build, settings, broker version,
and failure artifacts must match the selected results.

For a release regression check, select completed Ozzy baseline and candidate
runs with at least two repetitions per cell. The same tool writes a JSON report
instead of charts and exits unsuccessfully if any cell loses at least 5% of
throughput or gains at least 5% of confirmation/delivery latency:

```sh
ozzy_chart --run-id CANDIDATE_RUN --baseline-run-id BASELINE_RUN \
  --modes durable --regression-output /mnt/ssd/tmp/ozzy-regression-durable.json
ozzy_chart --fixed-load --run-id CANDIDATE_SMALL_RUN --run-id CANDIDATE_LARGE_RUN \
  --baseline-run-id BASELINE_SMALL_RUN --baseline-run-id BASELINE_LARGE_RUN \
  --modes durable --max-rate 128:100000 --max-rate 1024:100000 \
  --max-rate 8192:10000 --regression-output /mnt/ssd/tmp/ozzy-regression-durable-fixed.json
```

Repeat for both cluster modes. The report keeps percentile medians, repetition
ranges, overlapping-range flags, and both validated provenances. Missing cells,
metrics, or repetitions cannot pass. Changed workload, dependency, compiler,
hardware, or runtime controls produce an incomparable report. Product source and
binary changes are expected. Repeat matched measurements when ranges overlap;
never average a regressed cell into improvements elsewhere.
Timed reports retain merged histogram bins for later percentile extraction.

### Iggy logout diagnostic

```sh
ozzy_compare --impl iggy --check-only
cargo build --locked --release -p ozzy-bench --features comparisons \
  --example ozzy_iggy_rc_logout
"$CARGO_TARGET_DIR/release/examples/ozzy_iggy_rc_logout" \
  --processes --logout-on-eof --debug --poll \
  --writers 4 --writes 128 --records 1024 --record-bytes 8192
```

Repeat with `--sync-before-logout` to isolate pending disk writes. A 30 s deadline
bounds logout. Logs/outcome go to `~/.cache/ozzy/diagnostics/`; retries here never
contribute comparison timings.

## Payload correctness checks

Tiny payload allocation check (run alone; process-wide allocator counters):

```sh
cargo test -p ozzy-bench --features allocation-counting --bin ozzy_timed_bench \
  tiny_record_payload_and_message_allocate_nothing -- --ignored --test-threads=1 --nocapture
```

Checks SDK input construction, 53-byte OMQ messages, cloning, borrowed decoding
and inline payload retention. Transport batch buffers and broker operation
construction are outside this allocation bracket.

## Other runners

### LZ4 dictionary experiment

```sh
cargo build --locked --release -p ozzy-bench --features comparisons --bin ozzy_lz4_dict_bench
ozzy_lz4_dict_bench --cpu 0 --controller-cpu 2
```

Canonical Append bodies, held-out comparison JSON and random payloads, no/2 KiB/8 KiB
dictionaries. Targets span 256 B-16 KiB plus a 64 KiB control; whole records must fit.
Training and setup are untimed and reported separately. Reused buffers measure
codec CPU only; every body roundtrips before/after timing. The padded-byte model
uses current entry/seal framing and raw fallback, checked against production.
Dictionary persistence/repair costs are excluded; this is not broker throughput.
The supervised worker runs alone on one CPU. Results append to
`~/.cache/ozzy/experiments/ozzy.jsonl`; full provenance stays beside each run.

### Runner boundaries

Use each binary's `--help` for the complete option list. Their boundaries differ:

| Binary | Measures |
| --- | --- |
| `ozzy_segment_verify_bench` | In-memory production group decode/integrity, optionally LZ4 |
| `ozzy_workloads` | Serial persistent native policy/worker/size matrix |
| `ozzy_timed_bench` | Native timed workloads; persistent policies use the production broker |

```sh
ozzy_segment_verify_bench --codec lz4 --record-bytes 1024 \
  --batch 1000 --corpus-batches 64 --seconds 5
ozzy_workloads --sizes 128 --workers 1,4 --connections 4 --request-records 1024
```

Segment verification includes headers, digests, chain and group seal. Corpus
construction and byte-for-byte preflight are untimed; no disk, network or writer
confirmation is measured. Never present this as broker throughput.

## Repeatable native workloads

```sh
ozzy_workloads --dry-run
ozzy_workloads --profiles single-durable,replicated-persisting \
  --workers 1 --sizes 1024 --segment-mib 64,256,1024
```

The default workload sweeper covers local durable and replicated-persisting
policies using individual records and verified readers. Disk-quorum remains an
explicit diagnostic profile.
Exhaustion or retention gaps invalidate the run.
A short run without a segment roll cannot establish rollover performance.

## Native timed runner

```sh
ozzy_timed_bench --system replicated-persisting --processes --network-ingress --streaming \
  --record-bytes 1024 --duration 3 --warmup 0.25 \
  --window 4 --partitions 16 --request-records 1024 \
  --producer-workers 4 --reader-workers 4 --storage-dir /mnt/ssd/tmp/ozzy-bench
```

`--window` counts logical writers. `--partitions` counts broker partitions
(default 16). Worker counts divide client handles without increasing each
SDK process's APPEND cap. Durable modes use the production `Broker`,
`SharedTopicWriter`, and `TopicReader` APIs. OMQ coalescing, SDK requests and
broker groups are independent.
`--records-per-second` selects native fixed-rate pacing. `--system` defaults to
`replicated-persisting`.

Broker application work and OMQ I/O stay on separate threads. Results record
`broker_omq_mode` and the broker I/O thread count.

## Cross-host placement

Native `ozzy_timed_bench` uses `--placements` with three entries and a reachable
TCP control endpoint.
Native example with separate storage devices:

```json
[
  {"bind": "192.168.11.155", "cpus": [0],
   "storage_dir": "/mnt/ssd/tmp/ozzy-bench"},
  {"bind": "192.168.11.155", "cpus": [1],
   "storage_dir": "/mnt/bench/tmp/ozzy-bench"},
  {"bind": "192.168.11.100", "ssh": "er-dev",
   "executable": "/mnt/bench/tmp/ozzy-bin/ozzy_timed_bench",
   "cpus": [0], "storage_dir": "/mnt/bench/tmp/ozzy-bench"}
]
```

Use `--control-bind tcp://<reachable-controller-address>:0`. Copy identical
executables beforehand; reported hashes must match. SSH launches workers;
OMQ control/data connects directly. Use a trusted isolated network: SSH launch
does not authenticate payload links. Remote Linux needs `timeout`, SSH keys and
a disk-backed storage parent.

Each worker prepares its run directory and reserves endpoints on its own host.
The coordinator distributes identical configuration and identity records over
control. Workers check those records against their resources before formatting.

Native `storage_dir` overrides the global parent per broker. Optional `cpus`
sets each broker's mask before its threads start; readiness and final results
verify every thread's observed mask. Results include the storage device and
filesystem type. Remote hosts also need `taskset` when CPU masks are supplied.
`ozzy_compare --placements <file> --control-bind tcp://<controller>:0` supports
replicated-persisting and disk-quorum comparisons with three local brokers or
two local brokers and one remote broker. Cross-host runs require three distinct
XFS devices and one CPU per broker. The same placement
applies to Ozzy and Iggy. Remote CPU 5 runs the process audit; keep it outside
the broker mask. The runner deploys matching binaries, checks effective Iggy
configuration, monitors both hosts for competing processes, and cleans each case.
Results retain placement and remote audit evidence; they cannot mix with local runs.

For single-host comparisons and Ozzy CPU profiling, placements may contain
three local brokers with disjoint CPU sets or the same `--broker-cpus` pool
in all three entries. Every set lies inside `--broker-cpus`. A set can hold both
threads of one physical core, such as `[0, 6]`; Ozzy-only runs then accept a
shared `--broker-cpus` pool with gaps, such as `0,1,2,6,7,8`.
For example, `"cpus": [0, 1, 2]` lets existing broker threads share three CPUs;
keep clients on a disjoint mask such as `--client-cpus 3,4,5`.
Iggy divides a shared local CPU pool across its three brokers when choosing
shard counts. Both implementations retain the same process CPU masks.
Explicit storage parents are required. Shared XFS devices are allowed and
reported; results carry `deployment_kind: all-local`. Take a fresh baseline
when changing placement. The two-host 1 Gbit/s link limits cross-host tests:
use 16 B / 128 B fixed loads first, accounting for both follower streams and
client traffic. A leader plus local follower can confirm without crossing it.

Writers/readers stay on the coordinator host, so latency uses one monotonic clock.
Never subtract broker clocks. Stop all local/remote workers before another run;
forced termination can leave scratch. Multiple brokers sharing a host/disk still
share a failure domain. See [fault tests](../DEVELOPMENT.md#test-layers).

## Process control

Bounded OMQ control uses abstract IPC locally (`ipc://@...`) and explicit TCP
remotely. No pipe-reader coordination threads. Run/worker identities, sequence
checks, chunking, readiness, shutdown and worker failures have explicit deadlines.
stdout/stderr are diagnostics or the completed result, never the control protocol.
