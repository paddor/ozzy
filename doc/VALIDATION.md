# Validation

Test the production cores and adapters. A simplified second implementation or
matching encoder/decoder bugs cannot establish correctness. Keep deterministic
fault tests separate from filesystem/process tests and performance measurements.

## Local checks

Use `scripts/test.sh` for development. It reports elapsed time and runs only
the selected subsystem; it does not replace the full gate.

| Scope | Coverage |
| --- | --- |
| `unit` (default) | Workspace library tests |
| `core` | Protocol, owner, journal contracts, replication cores |
| `writer` | SDK admission, batching, confirmation, retry tests |
| `local` | Native writer/reader integration, raw and LZ4 storage |
| `storage` | Segment engine, including LZ4 |
| `io` | Async file contracts, controlled storage, and backend conformance |
| `replication` | Runtime groups, sockets, process recovery |
| `simulation` | Deterministic workload and fault scenarios |

Arguments after the scope select tests. Every scope runs through Nextest with
eight threads. Keep process-global-counter and expensive qualification tests
isolated. `scripts/test-all.sh` also runs formatting, lint, doctests, and docs.

```sh
cargo build --workspace
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo nextest run --workspace --test-threads 8
cargo test --workspace --doc
RUSTDOCFLAGS='-D warnings' cargo doc -p ozzy -p ozzy-runtime -p ozzy-proto \
  -p ozzy-replication --no-deps
```

Run expensive checks serially. Formal models and the Kani
harness have separate instructions in [verification](../verification/README.md).

## Test placement

Use one process with three isolated logical brokers and native SDKs over OMQ
`inproc://` for the fast integration path. Each broker keeps its own identity,
partition actors, journals, and restart state. Inject endpoints and transport
context without replacing broker behavior. Unique inproc names replace ports.
The pinned OMQ resolves inproc names within a context registry, so participating
context-created sockets must share that registry. Separate contexts cannot
reach each other merely by using the same endpoint string.

| Layer | Purpose |
| --- | --- |
| Deterministic simulation, one process | Production cores/codecs with controlled time, message delivery, disk bytes, and completions |
| Native inproc integration, one process | Real SDK sessions, PEER/PUB/SUB, routing, credit, confirmation, replay, and broker lifecycle |
| Small TCP and multi-process suite | ZMTP framing/handshake, OS socket backpressure, disconnects, process death, and real restart |
| Controlled storage faults | Torn writes, missing syncs, directory publication, and device power loss |

Inproc bypasses the ZMTP byte codec and synthesizes its transport handshake.
It exercises the Ozzy wire commands, not every production TCP/OMQ I/O path.
It also does not make Tokio scheduling deterministic. Keep the existing
virtual-time/event scheduler and independent confirmation oracle for replayable
faults. To combine those controls with sockets, expose explicit message and
completion gates rather than relying on wall-clock sleeps.

One-broker container smoke tests cover the developer setup. Three logical
brokers still cover replication and failover even when hosted in one process.
Dropping a broker actor is not an OS process crash, and neither simulates a
power cut without controlling which disk bytes survived.

### Controlled async file I/O

`ozzy-io`'s `simulation` feature implements the same owned operations and
completions as the pool backend. The controller independently executes jobs,
delivers results, persists selected ranges/lengths, and crashes a process or
device. File bytes and directory entries have separate durability barriers.
Handles reference inodes across rename/unlink and are fenced across restart.

Bounded typed traces reproduce seeded schedules. Short writes, failed barriers,
torn persistence, missing earlier writes, cancellation, and delayed results are
explicit events. A shared ordinary file workload checks pool/AIO/model conformance.
Direct-write checks cover cancellation, shutdown, alignment and descriptor limits.
This is a file-level model, not an extent allocator or full filesystem emulator.
Segment metadata tests use the shared publication algorithm through this
interface. Crash cuts around execution and result delivery check complete
CURRENT selections, canceled-owner lock lifetime and intact DURABLE copies
after partial writes. Full journal/actor integration is still required for
broker power-loss qualification.

Asynchronous segment checks exchange files with the existing writer through
pool and AIO backends. Controlled schedules cover reversed partition completion,
canceled writes/barriers, short reads/writes and failed recovery barriers. Holes
before protected history fail without modification; damaged tails beyond it are
zeroed before recovery reports durability.

Async journal tests exchange stores with existing recovery on pool and AIO
backends. Execution/observation cuts during rolls preserve selected history.
Checkpoint and sealed-file corruption are refused before active-tail repair.
Admission rejects groups that exceed configured recovery limits before writing.
Checkpoint build/selection cuts recover either the previous selection or the
complete checkpoint. Exact artifacts are reusable; conflicting contents and
checkpoint-name/identity mismatches are refused.
Retention/deletion crash cuts preserve checkpoint-covered history. Captured
generations remain readable after retirement and block physical deletion.
Failed index-deletion barriers never permit removing their source payloads.
Checkpoint/metadata cleanup cuts retain the selected checkpoint and its source.
Live build results and readers protect unselected artifacts across publication
changes and journal destruction.
Async multi-pass index builds match existing encoded indexes byte-for-byte.
Publication crash cuts permit rebuilding or reusing derived indexes without
changing journal history. Indexed raw/LZ4 reads validate selectors and corrupted
entry bytes, including reads protected across retention.
Async snapshots preserve committed/accepted boundaries and ignore later active
tails while validating their captured prefix. Concurrent cold catalog reads,
cancellation, lookup failures, bounded range reads and exact identity handoff
are covered. Synchronous identity checks never submit file jobs or treat an
unresolved key as absent.
Async canonical recovery covers checkpoint-plus-tail restart, interleaved writer
results, historical control duplicates, separate committed/speculative views,
canceled replay and private selected tails larger than the live plan budget.
Activation rejects unpublished confirmation, a changed view or a different tail.
Async history tests cover frozen tails, bounded chunks, raw/LZ4 bodies, valid but
conflicting physical replacements, cache reuse and deletion protection. Pool and
AIO stores support the same history reads and fresh validation. Byte budgets,
short/canceled reads and failed validation steps cannot produce partial evidence.
Recovery crash cuts preserve damaged files and isolate replacement generations
behind a nonvoting configuration marker. Metadata and checkpoints remain strict.
Configuration publication checks exact authority and canonical replay without
promoting accepted-only history. Failed publication fences its owner even when
the configuration rename physically completed.
Sealed repair covers raw/LZ4 fragments, wrong or incomplete donor history, bounds,
device errors and canceled transfers. Execution/observation crash cuts preserve
all original files and select only complete replacements. Pool/AIO repairs remain
readable by strict existing recovery without promoting accepted-only history.
Async selected-history checks preserve a commit floor inside a physical group,
older sealed files and captured readers. Whole-chunk rejection, bounded splitting,
decoded-byte rolls, failed/canceled staging and cleanup keep old history selected.
Crash cuts select either the old generation or the complete new history. The
same installation runs through pool/AIO and strict existing restart.
Index-only orphan cleanup respects selected/captured sources and fences after
failed barriers. Closing a journal preserves reader access and its group lock
without adding durability or confirmation evidence.
Async pipeline checks cover shared/raw/LZ4 output, device-budget coalescing,
reversed physical completion, foreign or lost results and ordered installation.
Canceled waits retain payload ownership through physical completion. Buffered
results and partial evidence exclude later reservations. Power-loss recovery
discards later completed groups above an unprotected hole. Shared writes also
run through pool and AIO backends before strict existing restart.

## Invariants and fault coverage

### Topic foundation integration gates

These qualify the selected design; they are not claims of existing coverage.

- Single-broker container: native SDK write, local-durable confirmation, live
  PUB/SUB, PEER replay, and segment restart. It never advertises replicated
  durability. A failed three-member group never falls back to this mode.
- Two independent writers, same key/partition: one offset order, independent
  sequences/epochs, exact retry results after interleaving, restart, and failover.
  Fencing one writer does not fence the other.
- One group and journal per partition on the same three brokers; reject group
  reuse across partitions and inconsistent metadata. Different leaders within
  one topic; one partition's election/repair never changes another's authority,
  confirmed history, selected segments, or durability evidence. Exercise
  different shard counts and placements on the three brokers.
- Shared PEER connection across topics/partitions: correct destination,
  per-writer order, correlation, credit, and reconnect fencing. Full compressed
  payload blocks survive dispatch, replication, and storage byte-exact.
- Stall one partition's journal and fill its shard queue while healthy shards keep
  writing. Exercise both shared and separate connections. Prove bounded memory,
  no dispatcher wait on the full queue, healthy-partition progress, and reserved
  election/control progress. Include a client that floods or stops reading replies.
- Shard actors grant to several clients sharing one dispatcher producer
  lane. Prove aggregate and per-lane slot/byte reservations cover unused grants,
  in-flight work, retained buffers, and reconnect. Delay fanring slot publication
  and byte release independently; neither can cause double grants or overflow.
- Bind each PEER/PUB endpoint once with multiple I/O workers and a separate
  dispatcher. Connections carry destinations across shards without an intermediate
  application shard. Measure dispatcher capacity. Check generation-safe replies, per-writer order,
  bounded publication fanout and buffer returns, and continued I/O under load.
  Qualify per-I/O NUMA placement and additional producer lanes when implemented.
- Many partition journals share fixed worker pools and device admission. Thread
  counts must not grow per partition. Stress rolls, sync evidence, catch-up,
  and shutdown together; healthy progress assumes a functioning shared device.
- Enable the replacement Ozzy frontend only after those isolation checks pass.
  This is not a requirement to retain OMQ's optional receive-lane API. Include
  SDK scheduling past a credit-starved writer on a shared broker connection;
  average throughput or low message rate is not a substitute.

### Core invariants

- Wire: fixed bytes in both directions, reserved flags, size/overflow bounds,
  multipart empties, corrupt envelopes/bodies, exact session/request correlation.
- Owner: atomic batch admission, epoch fences, exact retry equality, original
  confirmed receipts, uncertain cancellation, bounded reader queues and replay.
- Replication: ordered two-copy confirmation, no transport-only votes, preserved
  unannounced confirmed history, minority refusal, protected leader selection,
  stale-view fencing, intact restart, and nonvoting lost-state recovery.
- Resources: full count/byte budgets, independent slow followers, cloned/sliced
  owners, cross-thread release, canceled waiters, stale completion reclamation,
  no capacity growth or permit loss during retries and view changes.
- Storage: short/interrupted/partial writes, ambiguous sync/publication failure,
  truncated crash tails versus selected-history corruption, stale generations,
  roll/metadata order, bounded staging, index rebuild, and checkpoint/trim pins.
- Recovery: dropped, duplicated, reordered, delayed and corrupt messages;
  one-way cuts and partitions; donor/receiver loss during transfer; changed
  authority before installation; repeated rejoin near exact retention limits.

Simulation must schedule network, time, storage completions, and faults
independently with recorded seeds/traces. Assert both safety and bounded progress
under explicit healthy-resource assumptions. Leader agreement or matching final
digests alone does not prove that every issued writer receipt survived.

### Byte-backed faults

Segment publication tests run the production immutable-file, `CURRENT`, and
two-copy `DURABLE` algorithms over bounded bytes. File and directory syncs
advance separate durable state. Ranges and file length may reach stable storage
before a completed barrier.

- Seeded short writes, torn tails, post-sync damage, and damaged metadata use
  production codecs. Raw and LZ4 tests persist header, body, and seal regions
  out of order. Protected history survives or recovery refuses without changing
  damaged segment bytes.
- A test-only missing directory barrier must fail the independent confirmation
  oracle. A persisted byte range alone is neither directory publication nor a
  completed barrier.
- Repair failures carry replayable event words. Reduction must reproduce the
  same complete assertion within a bounded replay budget.

The byte adapter covers one active segment and full-history replacement, without
checkpoints. Directory and process tests cover the remaining runtime boundaries.

### Lifecycle and repair

The byte-backed lifecycle runner combines the production replication driver
and codecs in one schedule. It checks each decoded disk result against the
action contract; writer receipts remain outside broker state. Append,
confirmation, restart refusal, leader change, nonvoting repair, and fresh writes
share that schedule.

- RAM-confirming cases use production restart markers and raw/LZ4 writers. They
  separate retained votes from durable prefixes, cut drained-marker publication,
  repair torn writes, and require progress despite a stalled disk or callback.
- Quarantine tests cut marker publication, preserve damaged originals, and
  require fresh authority from both other brokers before transfer. Corrupt
  authority cannot authorize quarantine; loss of both required payload copies
  refuses recovery.
- Repair tests cut multi-chunk staging and final publication, change donor
  authority, and require fresh confirmation by the rebuilt broker. Controlled
  actors check stale transfers, late publication, and unchanged damaged files.
- Sealed repair tests reuse intact entries and fetch missing raw/LZ4 operations
  from differently segmented donors. They check exact boundaries, bounded
  staging, healthy files, cleanup, and every publication cut.

Validation tests cover byte bounds, compressed groups spanning reads, short
maintenance turns, resumed cycles, later corruption, and stale tickets. They do
not qualify automatic repair scheduling, reconstruction of authority metadata,
or valid rollback of an authoritative record.

Process fixtures verify actual child death, full replay, new writes, and a
returning broker's contribution to confirmation. SIGKILL preserves OS page
cache; device power-loss qualification needs controlled block or VM faults.
Use dedicated test stores, not the workspace disk.

The selected broker tests exercise shared SDK batching, raw/LZ4 delivery,
reader credit, multi-partition routing, two replicated confirmation policies,
leader loss, and intact restart. The process layer covers TCP and actual broker
termination. Run the broker target in isolation when testing process faults:

```sh
cargo nextest run -p ozzy-broker --test ozzy_broker_config \
  --test-threads 8 cli_tcp_three_brokers_restart_and_confirm
```

The deterministic lifecycle and controlled-storage suites cover torn writes,
missing syncs, delayed completions, damaged-history refusal, and recovery.
A process kill retains the OS page cache; controlled storage tests establish
the separate power-loss boundary.

## Benchmark discipline

Commands and timing boundaries belong in [ozzy-bench](../ozzy-bench/README.md).
Keep run tables, profiles, experiment patches, and raw output in ignored
artifacts. A short result may go in an explicitly requested commit message.
Do not create per-run documentation or append a measurement diary here.

Before measuring: clean build, formatting, Clippy, relevant correctness tests,
then an idle machine. Never overlap benchmarks/profilers with builds, tests,
or another measurement. Stop immediately on warnings/timeouts; retain failed
diagnostics but exclude their rates. Profile before optimizing.

For a comparison, preserve binaries/source identity and hold payload, batching,
in-flight work, topology, compression, durability, and completion fixed. Repeat
and alternate order. Report medians, ranges, sample counts, and batch versus
record latency; don't select only favorable runs. Profiled/allocation-counted
timings are separate from ordinary measurements.

Count exact validated records at the stated boundary. Writer confirmation,
reader delivery, and application echo are different metrics. Warmup, setup,
drain, replay, and process CPU brackets must be explicit. Never subtract clocks
across hosts or count SSH CPU as broker CPU. Local loopback and finite retained
cohorts establish neither independent failure domains nor sustained capacity.
