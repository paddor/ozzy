# Verification

Small TLA+ models and Kani proofs for selected Ozzy contracts. For
contributors who change journal progress, view changes, or the shard write
lane.

- These are executable contract models, not a verified VSR implementation.
- No model proves the complete runtime, storage codec, OS, or hardware
  behavior.
- The scripts download nothing. Do not run them alongside benchmarks.

## TLC models

Setup:

1. Install Java and ripgrep.
2. Download the official
   [TLA+ tools 1.7.2](https://github.com/tlaplus/tlaplus/releases/tag/v1.7.2)
   jar. Its published SHA-1 is `7f21faa2cdae3189e7d5fadb4488f0dfcc658407`.
3. Put it at `target/verification/tla2tools-1.7.2.jar`, or set `OZZY_TLC_JAR`
   to its absolute path. Do not commit it.

```sh
scripts/verify-models.sh
```

Logs and state files go to ignored `target/verification/run-*/`. Each model
has a `.cfg` that must pass and a `.broken.cfg` negative control that must
fail with the named error, not a parse error or a missing dependency.

| Model | Bound and claim | Broken switch | Broken config fails with |
| --- | --- | --- | --- |
| `LocalCommit` | Three ops, two generations; local durable commit, apply, and observed success stay within stable data | Commit from write without sync | Invariant `PrefixOrder` |
| `PrimaryRestart` | One slot, two values, three views; no conflicting slot meanings after a primary restart | Same-view restart after send-before-sync | Invariant `SlotIdentity` |
| `PrepareReplacement` | One uncommitted local prepare and an authorized empty selected suffix; installation stays possible | Treat a local prepare ACK as an immutable commit | Deadlock |
| `ExitView` | Two backups, a lost primary, promises stalled behind the disk; both leave view 0 on `EXIT_VIEW` alone, never without their own timeout | Ignore a request to leave a view already left | Temporal properties |
| `WriteLane` | One shard's writer state, three commands, and a stop; every command is written and stop ends with the state home | Skip the owner's check for commands sent while the state was returning | Deadlock |

Limits:

- `LocalCommit`, `PrimaryRestart`, and `PrepareReplacement` check safety.
  `PrepareReplacement` also checks deadlock.
- `ExitView` and `WriteLane` check liveness under weak fairness.
- No model elects a primary or proves quorum selection.
  `PrepareReplacement` assumes the selected suffix is authorized.
- A `LocalCommit` write is one complete group, not a partial byte write.

## Kani proofs

Requires Kani 0.67.0. Set `OZZY_KANI_BIN` if `cargo-kani` is not on `PATH`.

```sh
scripts/verify-rust.sh
# same as:
cargo kani -p ozzy-journal -p ozzy-core --lib --output-format terse
```

Proofs live under `cfg(kani)` in `ozzy-journal/src/progress/proofs.rs`:

- Journal progress events keep `durable <= written <= accepted`, never move a
  frontier back, and leave the state unchanged on error.
- Admission is atomic and cannot wrap `u64`.
- A sync acknowledges only writes completed before it began.

These are properties of selected functions, not a proof of journal contents
or I/O. Default reachability and undefined-function checks stay on. A
reachable unsupported construct must fail verification; do not suppress
checks to get a pass.

## Related checks

- Loom models: [DEVELOPMENT.md](../DEVELOPMENT.md#loom).
- Simulation suites: [ozzy-sim](../ozzy-sim/README.md) and
  [test layers](../DEVELOPMENT.md#test-layers).
