# Validation

Test production code from the inside out. Keep independent record/format evidence;
a second broker implementation or matching encoder/decoder bug proves little.
Benchmarks measure performance after correctness checks.

## Local checks

`scripts/test.sh` selects a subsystem; extra arguments select tests. Nextest uses
eight threads except for Loom. Expensive/process-global cases run separately.
The shared full-product simulator target joins the broker lifecycle isolation
group; its real owner and transport threads do not run beside other test cohorts.

| Scope | Coverage |
| --- | --- |
| `unit` | Workspace library tests |
| `core` | Protocol, owner, journal contracts, replication cores |
| `writer` | SDK admission, batching, receipts, cancellation and retry |
| `local` | Broker configuration and native writer/reader integration |
| `inproc` | Real broker and SDKs over OMQ inproc, memory-only disk, load and faults |
| `storage` | Segment format, recovery, indexes and LZ4 |
| `io` | Owned I/O contract, controlled file model, pool/AIO conformance |
| `replication` | Runtime groups, sockets and process recovery |
| `simulation` | Deterministic workload and fault schedules |
| `loom` | SDK counters, persistent readiness and exact receipt publication |

```sh
scripts/test.sh inproc
scripts/test.sh writer
scripts/test.sh simulation
scripts/test.sh loom
scripts/test-all.sh
```

The full gate checks formatting, workspace build, Clippy, Nextest, doctests and
strict public API docs. Formal tools have separate instructions in
[verification](../verification/README.md). Run expensive checks serially.

## Test layers

| Layer | Executes | Establishes |
| --- | --- | --- |
| Unit/core tests | Production codecs and state transitions | Exact formats, bounds, stale-work fences and policy decisions |
| Memory-only inproc integration | Real broker, SDKs and OMQ PEER/PUB/SUB | Routing, sessions, pressure, batching, repair and owner lifecycle |
| Deterministic simulation | Production cores with gated time, messages and file jobs | Replayable message/disk order and crash schedules |
| Pool/AIO conformance | Same owned file operations on real backends | Alignment, cancellation, handle lifetime and physical execution |
| Small TCP/process suite | Native processes and persisted stores | ZMTP, socket pressure, reconnect, process death and restart |
| Controlled power loss | File-model persistence and namespace barriers | Which bytes/entries can survive each modeled crash cut |

Inproc integration performs no physical disk I/O and creates no OS files. Three
logical brokers have distinct identities, journals and restart state. All sockets
must share the OMQ context registry; matching endpoint strings across separate
registries do not connect.

Inproc bypasses ZMTP bytes and synthesizes the transport handshake. Tokio scheduling
remains nondeterministic. Simulations use explicit message/completion gates and an
independent history oracle. Dropping an actor is not process death; SIGKILL retains
the OS page cache and does not model a power cut.

### Controlled file backend

`ozzy-io`'s `simulation` feature implements the same owned operation/completion
contract as pool and AIO. The controller independently:

- Executes submitted jobs, including short or failed operations.
- Delivers results, including delayed and reversed completions.
- Persists chosen byte ranges, file lengths and directory entries.
- Crashes a process or device and reopens the surviving image.

Handles reference inodes across rename/unlink. File data and directory publication
have separate barriers. Restart fences old handles; canceled jobs retain buffers
and leases until physical completion. Typed traces reproduce seeded schedules.
This is a file-level model, not a complete filesystem or extent allocator.

### Loom

`scripts/test.sh loom` compiles production counters and receipt slots with Loom
atomics. Models cover admission/cancellation/release, capacity observation,
persistent readiness racing a drain, and offset publication before confirmed-prefix
observation. Admission uses a two-preemption exploration bound.

Broker models compile the production lifecycle atomics and owner setup. They
explore queue closure racing both shutdown request orders and either owner's
failure, preserving separate completion tracking. A negative control with the old
independent stop signals must find a queue closure before the receiving owner
observes shutdown. Queue closure is modeled as release/acquire publication, not
as OMQ queue internals.

The receipt model checks relaxed offset stores followed by release publication
and acquire observation. Ordinary tests cover 16-cell page reuse, alias lifetime,
notification registration, cancellation and wakes. Loom does not control OMQ,
Tokio internals, filesystem operations or the full replication state machine.

## Required invariants

| Area | Invariant and exercised faults |
| --- | --- |
| Record identity | Exact IDs, multipart boundaries and every payload byte; independent oracle; retries retain original offsets |
| Partition state | One global offset order; independent producer sequences/epochs; fencing one producer leaves others usable |
| Confirmation | Single-broker durable evidence, DQ two durable copies, RP two validated retained copies; transport receipts never vote |
| Authority | Configuration/view/session/incarnation/receive-epoch fences; delayed work cannot revive old authority |
| Failover | Preserve quorum-held history even when the old leader replied before announcing COMMIT |
| Pressure | Count and full-backing byte charges survive dequeue, aliases and cancellation; control capacity remains separate |
| Replication | Lost/duplicate/reordered PUB traffic, quiet final loss, bounded PEER repair, one slow follower and unavailable quorum |
| Readers | Confirmed applied records only; replay/live overlap, source changes, paused consumers, explicit retention gaps |
| Storage | Short/torn writes, reversed completion, failed barriers, roll/checkpoint publication cuts, protected read sources |
| Recovery | Nonvoting damaged stores, frozen donor tails, bounded transfer, private replay and fenced election handoff |

Memory-only broker/SDK load cases cover all three modes with FIFO and reversed
file-job completions. Many producers share partitions; a paused consumer cannot
retain producer arenas indefinitely. The twenty-producer case resumes the reader
and verifies gap repair after every producer finishes. PUB output keeps a separate
encoded backing reservation while writer capacity is filled.

Storage suites compare model/pool/AIO behavior, reject corrupt protected history,
and install only complete selected generations. Captured reads protect their exact
files through roll, replacement and retention. Failed deletion barriers cannot
permit removal of required payloads. Buffered completion and partial durable
publication never cover later reservations.

Recovery suites include interleaved retry state, accepted-only suffixes, histories
larger than live admission windows, stale validation, canceled transfers, wrong
donors, conflicting physical replacements and configuration publication failure.
Checksums establish integrity, not authority. Two other normal brokers authorize
lost-store recovery; damaged peers alone do not reconstruct quorum evidence.

Virtual-time SDK tests deliver ready input before advancing retry clocks. Process
and real-backend cases add evidence the inproc/file models cannot supply. Coverage
is finite: it does not prove complete consensus safety or arbitrary host-loss
availability.

## Benchmark discipline

Commands and boundaries belong in [ozzy-bench](../ozzy-bench/README.md) and
[benchmark charts](../BENCHMARKS.md). Keep raw results and profiles in ignored
artifacts, not a documentation diary.

Before measuring: build, format, lint, relevant correctness tests, then an idle
machine. Benchmarks, builds, tests and profilers run serially. Warnings/timeouts
invalidate rates. Profile before optimization.

Hold payload corpus, topology, CPU allocation, compression, batching, in-flight
bounds and confirmation policy fixed. Preserve source/binary identities. Repeat
comparisons and report medians, ranges, sample counts and measured boundaries.
Writer confirmation and verified reader delivery are separate metrics. Warmup,
scheduled arrival, setup, drain and persistence lag remain explicit.

## SDK identity and history starts

Producer tests use real SDK owners, OMQ inproc, real brokers and memory-only file
jobs in all three modes. They cover fresh identity, resume, takeover, stale queued
frames, and abrupt owner loss without an SDK outbox. Core snapshot fixtures check
that producer transitions and retry coordinates survive a checkpoint.

Directory tests run the production session/catalog owner and SDK over OMQ inproc.
They withhold one broker's topic response while another responds, deliver late
duplicate replies, and repeat discovery past the control-slot bound. With every
response lost, the manual clock expires one shared deadline; later lookups regain
admission. Replacing the directory session requires a fresh reply under the same
deadline. A four-partition broker case repeats resume/takeover forty times with
a live reader and verifies every payload again through checkpoint replay.

A controlled SDK case admits four subscriptions on one broker, cancels the
next-record observers, and leaves both two-partition readers idle. Producer
attachment and confirmation must finish without polling those readers or
advancing the clock; resuming the readers then verifies the original payload.
The case repeats the pause with four unsubscribe completions and requires
producer resume and a subsequent confirmation to finish under the same bounds.

Reader-close cases disconnect the real OMQ control socket with either an
unanswered opening or a completed subscription. Detached cleanup preserves the
checkpoint and settles without advancing the manual clock. Four unread producer
responses hold all control slots through disconnect or session replacement;
closing the old reader still settles, and the surviving writer confirms another
record after replacement. A lost cancellation reply on the original live session
remains an explicit timeout.

Retention cleanup runs even below the byte limit. A controlled journal case
checks the eight-object removal bound, unchanged selected bytes, unknown-file
preservation, and unchanged record floors. Real four-partition DQ/RP brokers
repeat recovery, reclaim the prior segments, and replay the selected payloads
after every restart. Existing cleanup crash cuts and capture-pin cases still
protect selected checkpoint sources and retained backing.

Controlled retirement cases remove successive sealed prefixes without an
intervening append, preserving the exact selected checkpoint in all three modes.
The normal retention command path stays healthy across repeated passes. Reopen
preserves retained multipart payloads and producer retry floors; later operations
permit a new checkpoint. A failed read of the reused checkpoint fences retirement
before another segment is unreferenced.

Controlled journal and live actor cases fetch an older durable source while a
later RP write remains unsynchronized. Repeated correlated requests return only
the original operations and preserve the durable watermark. An unsynchronized
source remains unavailable. A four-partition inproc case replaces two voters
with fresh memory stores, resumes producer identities, and verifies reader
payloads while the original donor continues serving.

Consumer tests resolve time/ID starts through the same memory-only brokers.
Pure selector tests cover duplicate IDs, confirmed/retained bounds and backward
clock changes. Wire fixtures cover selector tags and resolved offset replies.
Retention file-model tests commit floors before deleting segments, restart from
the selected checkpoint, and continue producer sequences and partition offsets.
Interleaved producers retain independent retry floors. Retained payloads survive
repeated follower recovery and full-cluster restart; time starts continue from
their resolved offsets across reconnect, and ID/checkpoint starts read cold
retained segments. Restart elections include an ancient replica below both
healthy copies' retained boundaries. A checkpoint at the accepted tail needs
only state bytes.

Controlled checkpoint exchanges reject future ranges, repeated chunks and old
link sessions without advancing the receive cursor or adding memory charges.
Pending checkpoint bytes use the same bounded owner memory as transfer state;
exhaustion leaves the correlated request available for retry. The small TCP
suite resumes and takes over saved producer identities after SIGKILL in DQ/RP.
An unclean RP copy uses explicit nonvoting recovery before another voter stops.

### Cross-host crash soak

The ignored `process::soak::three_host_crash_soak` case uses si-dev, wu-dev and
er-dev. Provision fresh isolated directories with the same broker binary,
`deployment.toml` and `shared.identity`, plus each broker's `local.identity`.
Use the CLI's ordinary volume initialization and format steps. The topic is
`orders` with four partitions and bounded retention. Broker processes and their
storage workers use core 0; run the SDK test on cores 4-5. Keep each host idle.

Set `OZZY_SOAK_CONFIG` to the controller's deployment file, `OZZY_SOAK_DIRS` to
the three comma-separated directories in host order, and `OZZY_SOAK_LOGS` to an
SSD artifact directory. `OZZY_SOAK_RESULTS` names a JSONL file under
`~/.cache/ozzy/`. `OZZY_SOAK_SECONDS` defaults to 3600; use a short gate before
requiring a full hour of clean churn for each real-I/O placement and policy.
`OZZY_SOAK_HOSTS` defaults to `si-dev,wu-dev,er-dev`. Set it to
`si-dev,si-dev,si-dev` for three processes on one host, with distinct directories
and configured endpoints. Repeated hosts use separate broker cores 0, 1 and 2;
the SDK remains on cores 4-5.

The ignored `process::soak::cross_host_single_crash_soak` case shares this workload
for single durable. Provision one `si-dev` broker, set one host and directory,
and run the SDK controller on another host. Set `OZZY_SOAK_IDENTITY` to the
controller's copy of `shared.identity` when the first broker's directory is
remote. The default identity path remains the first broker directory. The one
broker restarts before awaiting SDK progress; single durable has no failover.

The test admits individual records, checks every confirmed offset, ID and
payload, then releases the verified oracle entries. It resumes or takes over
the saved producer identity, keeps a reader connected across leader changes,
and repeats sparse traffic, bounded bursts, hot-partition traffic, mixed sizes
and multipart payloads with empty parts. Slow readers exercise repair while
periodic checkpoint replay independently checks retained payloads. It rotates
SIGKILL and orderly shutdown across brokers, and explicitly
quarantines an unclean RP copy for quorum recovery. A failed process or thirty
seconds without completion fails the run. These process kills preserve the OS
page cache and establish no power-loss claim.

For cross-host memory-only runs, copy the same built broker integration test
binary as `memory-broker` into every run directory and set `OZZY_SOAK_MEMORY=1`.
The ignored `journals::serving::simulated::churn::host::memory_only_broker_process`
case embeds the real broker with the controlled file backend and configured TCP
endpoints. Every process restart loses its memory store and starts nonvoting
recovery, including DQ. Both remaining copies must supply recovery authority.
This is a test harness, with no production storage mode or durability claim.

The ignored `process::soak::single::single_broker_varied_io_soak` case uses the
same varied SDK oracle on one durable broker. It retains its SSD artifacts,
rotates SIGKILL and orderly restart, and checks saved producer and live reader
reconnects. Set `OZZY_SOAK_SECONDS` and `OZZY_SOAK_RESULTS` and run on cores 4-5.
Set `OZZY_SOAK_BROKER_BINARY` to a copied CLI binary to keep all restarts on the
validated build. Otherwise the CLI process tests use Cargo's broker executable.

The ignored `journals::serving::simulated::churn::memory_only_varied_churn_soak`
case runs real brokers and SDKs over inproc with no filesystem journal I/O.
Set `OZZY_SOAK_POLICY` to `single`, `dq` or `rp`, plus seconds and results.
Replicated cases inject short segment writes, observe broker fencing, confirm
with the surviving quorum, and explicitly recover the failed copy. The regular
gate covers all policies before long runs; single-broker cases delay storage
and restart without claiming failover.

The reusable storage owner, deployment placement, and SDK record oracle live in
`ozzy-sim` under its `broker` feature. Short broker integration tests and full
product simulation use those same implementations. The simulator smoke gate
runs every confirmation mode through production SDKs, brokers, OMQ inproc, and
memory-only physical storage. `scripts/test.sh inproc` selects the entire real
broker simulation subtree, including resume, retention, and churn, plus that
simulator gate. `scripts/test.sh simulation` also includes the smaller protocol
models. Those models provide controlled protocol schedules; full product runs
provide owner, transport, admission, and storage lifecycle coverage.
The shared harness accepts `ServingContext::simulated` with the existing manual
SDK clock. Broker protocol deadlines and append timestamps follow that clock;
storage execution and completion delivery remain independently held. Ordinary
serving uses elapsed and Unix time. A full-stack case advances time through an
age-retention deadline and checks timestamp seeks in each confirmation mode.
The `ozzy-sim` binary accepts separate virtual-duration and wall-clock generation
limits, bounded admission/retention settings, and a weighted fault mix. It runs
a sustained seeded cluster with independent execution
and completion holds, concurrent producers sharing partition offsets,
producer resume and takeover with a live old producer, consumer checkpoint reopen,
new SDK physical connections, rolling retention, broker restart and short-write
recovery. Paused consumers have independent SDK owners, exercise PEER repair,
and retain payload aliases across close. Quorum-loss schedules stop two copies,
require unconfirmed admissions, cancel observations and restore exact clean images.
Retention-lag schedules verify foreground cohorts, advance past an old saved
checkpoint, and require its reopen to return an explicit retained-floor gap.
Live device cuts discard unobserved physical completions. Process cuts retain
dirty bytes; modeled power cuts restore only durable bytes and namespace effects.
All three modes exercise cuts with held completions and retry the same records
through production restart and recovery. This file-model evidence does not
establish kernel or hardware power-loss safety.
The runner can sustain one cluster or start successive fresh seeded scenarios.
Each wave releases verified oracle payloads. The fault schedule streams
to disk with clock observations, while only 128 recent workload events and
4096 physical events per device remain resident. Failure artifacts include independently submitted and
confirmed records, configuration, dirty/durable memory images and recent physical
order. A stopped storage pump retains its final image for failure inspection.
`--replay` and `--waves` reproduce a recorded fault prefix from fresh state.
They do not reproduce an exact threaded transport schedule or replace the core
simulator's virtual-time schedule. Reported PEER/PUB counts come from the actual
reader delivery paths. Runner commands and current resource bounds belong in
the [simulator README](../ozzy-sim/README.md).

Interrupted recovery restarts select full resume for exact nonvoting markers
and quarantine for established configurations. A controlled case holds donor
barriers across a second restart, then verifies records after release. Selection
is per partition; production membership and marker checks remain strict.
Fault rotation waits for configuration publication; the following fault's
confirmation requires the recovered copy to supply quorum. A separate controlled
DQ/RP case holds an independent history lookup across duplicate `FETCH_OPS`
requests, then checks the retried response's correlation, chain, and exact bodies.

The ignored simulator `churn::long_running_accelerated_fault_churn` repeats
bounded seed schedules until `OZZY_CHURN_SECONDS` expires (default one hour).
`OZZY_CHURN_SEED` chooses the first seed; `OZZY_CHURN_RESULTS` records progress.
DQ schedules retain replayable event words and reduction on failure. RP schedules
combine failed/torn background writes, compressed/raw operations, permanent
corruption, repeated repair, dropped/duplicated/reordered messages, and a fresh
quorum supplied by recovered copies. Virtual clock jumps up to thirty seconds
exercise protocol timeouts without wall-clock waits. Simulator byte faults are
controlled crash-image evidence; host SIGKILL is a separate process-crash layer.

```sh
source scripts/ozzy_tools.sh
taskset -c 4,5 cargo test -p ozzy-broker --test ozzy_broker_config \
  process::soak::three_host_crash_soak -- --ignored --exact --nocapture
```
