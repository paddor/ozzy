# Validation

Test production code from the inside out. Keep independent record/format evidence;
a second broker implementation or matching encoder/decoder bug proves little.
Benchmarks measure performance after correctness checks.

## Local checks

`scripts/test.sh` selects a subsystem; extra arguments select tests. Nextest uses
eight threads except for Loom. Expensive/process-global cases run separately.

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
