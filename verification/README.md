# Verification

These are small executable contract models, not a verified VSR implementation.
No model proves the complete runtime, storage codec, OS, or hardware behavior.

## TLC

Install the official [TLA+ tools 1.7.2](https://github.com/tlaplus/tlaplus/releases/tag/v1.7.2)
jar outside tracked source. Default location:
`target/verification/tla2tools-1.7.2.jar`. Alternatively set `OZZY_TLC_JAR` to
an absolute path. The upstream published SHA-1 is
`7f21faa2cdae3189e7d5fadb4488f0dfcc658407`. No binaries are committed.

Run `bash scripts/verify-models.sh`. Requires Java and ripgrep. Logs/state files
stay under ignored `target/verification/`. Each negative control must fail for
its named invariant/deadlock, not a parse error or missing dependency.

| Model | Bound and claim | Deliberately broken switch |
| --- | --- | --- |
| LocalCommit | Three ops, two generations; local durable commit/apply and observed success remain within stable data | Commit from write without sync |
| PrimaryRestart | One operation slot, two values, three views; no conflicting slot meanings after primary restart | Permit same-view restart after send-before-sync |
| PrepareReplacement | One uncommitted local prepare and a supplied authorized empty selected suffix; installation remains possible | Treat local prepare ACK as immutable commit |
| ExitView | Two backups, a lost primary, promises stalled behind the disk; both leave view 0 on EXIT_VIEW alone, and never without their own timeout | Ignore a request to leave a view already left |
| WriteLane | One shard's writer state, three commands and a stop; every command is written and stop ends with the state home | Skip the owner's check for commands sent while the state was returning |

`LocalCommit` corresponds to `JournalProgress`/`LocalProgress` actions and the
lost-sync, stale-generation, and crash scenarios in `ozzy-sim`. It abstracts
operation data into prefixes; Rust simulation retains values. A TLC `Write`
is one complete group, not a partial byte write. Its success history is an
external oracle retained across simulated power cuts.

`PrimaryRestart` and `PrepareReplacement` retain small design-review
counterexamples. The production core now exists, with restart and selected-history
regressions in Rust. These models do not elect a primary or prove quorum selection.
The restart model
abstracts successful new-view establishment; the replacement model explicitly
assumes the selected suffix is authorized and the prepare was not committed.

`ExitView` and `WriteLane` check liveness under weak fairness. `ExitView`
mirrors `ReplicaDriver::poll` and `receive_exit`; its Rust regression is
`a_voter_past_a_view_supports_a_lagging_peer_leaving_it`. `WriteLane` mirrors
`WriteQueue::send`, `kick`, `poll` and `run_turns`, abstracting command bodies
and turn capacity.

The first three models check safety plus the replacement model's deadlock
check. They do not establish general liveness. Idle transitions prevent artificial
deadlocks at finite bounds. Future VSR models must add message/quorum state,
fairness and eventual-healing conditions, and code-to-model trace mapping.

## Kani

Run `bash scripts/verify-rust.sh` using Kani 0.67.0. Set `OZZY_KANI_BIN` if
`cargo-kani` is not on PATH. The equivalent command is
`cargo kani -p ozzy-journal -p ozzy-core --lib --output-format terse`.
Harnesses live next to production code under `cfg(kani)`.

Admission checks all `u64` initial prefixes and counts, including overflow.
The inductive journal transition check assumes only ordered initial frontiers;
event fields are arbitrary. Other harnesses check captured sync scope, local
durable commit, fault fencing, and bounded application of committed state.
These are selected-function properties, not a proof of journal contents or I/O.

Kani may report unsupported constructs in unreachable dependency code. Default
reachability/undefined-function checks remain enabled; a reachable unsupported
construct must fail verification. Do not suppress checks to obtain success.

## Loom

Run `scripts/test.sh loom`. Production SDK counters and receipt slots use Loom
atomics to explore admission/cancellation/release, persistent readiness, capacity
observation and exact offset publication. Tokio/OMQ internals and disk/message
orders remain outside these models; see [Loom scope](../DEVELOPMENT.md#loom).

## Rust simulation

Run `cargo test -p ozzy-sim`. This uses real journal/owner completion code with
a bounded simulated disk. It is not a performance benchmark or a byte-level
segment-recovery test. Real OMQ tests remain part of the workspace sweep.
