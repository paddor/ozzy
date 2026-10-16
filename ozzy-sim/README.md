# ozzy-sim

Deterministic workload and fault tests around Ozzy production code, plus the
`ozzy_sim` full-product runner. An unpublished test crate for contributors.

## What it covers

- Core suites in `tests/` control message order, virtual time, storage
  effects, and completion observation. They cover canonical state,
  three-broker replication, leader change, recovery, and persisted history.
- The `broker` feature is the shared real-broker harness: production SDKs and
  broker owners over OMQ inproc and the controlled `ozzy-io` memory backend.
  `ozzy-broker` integration tests and `ozzy_sim` both use it.
- An independent record oracle checks every submitted, confirmed, and
  delivered record.
- Limits: it does not replace TCP/process-crash or real-disk checks. A seed
  alone does not reproduce a threaded OMQ run. See
  [test layers](../DEVELOPMENT.md#test-layers).

## Run the tests

```sh
scripts/test.sh simulation   # core suites and the full-product gate
scripts/test.sh inproc       # inproc broker and SDK gates
```

## Run the full-product simulator

```sh
scripts/ozzy_cargo run -p ozzy-sim --features broker --bin ozzy_sim -- \
  --mode replicated-persisting --seed 42 --duration 28800 \
  --interval-ms 10 --progress-timeout 30 \
  --artifacts tmp/ozzy-sim-rp-42
```

Pass `--help` to `ozzy_sim` for every flag and its default.

| Flag | Meaning |
| --- | --- |
| `--mode` | `durable`, `disk-quorum`, or `replicated-persisting` |
| `--seed` | Workload and fault seed |
| `--scenarios N` | Run N fresh clusters with successive seeds, each in `scenario-<i>-seed-<seed>/` |
| `--artifacts` | Output directory (required) |
| `--duration` | Wall-clock seconds of work generation |
| `--waves` | Maximum waves; bounds CI runs and replays |
| `--interval-ms` | Pause between bounded waves |
| `--progress-timeout` | Seconds without independently verified records before the run fails |
| `--simulated-duration` | Use a simulated clock and also stop at this simulated time, in seconds; without it the clock is elapsed time |
| `--clock-tick-ms`, `--clock-step-ms` | With a simulated clock: real milliseconds between ticks, simulated milliseconds per tick |
| `--partitions` | Partitions, 1 to 8 |
| `--resident-mib`, `--physical-jobs`, `--physical-mib` | Shard and device admission limits |
| `--retained-mib`, `--retained-age` | Retention size (1 to 8 MiB) and age in seconds |
| `--actions` | Comma-separated fault mix; a repeated action runs more often |
| `--replay` | Replay a recorded `schedule.jsonl` |

Rules:

- Disk activity alone never extends the progress deadline.
- SDK retries, broker deadlines, and append timestamps share one clock, also
  across broker restarts. Storage holds and threaded transport
  delivery do not.
- Single durable delays writes; it has no quorum failover.

### Actions

The default mix is every action except `fail-write`.

| Action | Effect |
| --- | --- |
| `traffic` | Live traffic and reader verification |
| `shared-producers` | A fresh producer shares the live producer's partitions, then leaves |
| `slow-consumer` | Pause a consumer through a burst, repair gaps, hold returned payloads |
| `retention-lag` | A paused checkpoint falls behind retention and reopens with an explicit gap |
| `quorum-loss` | Stop two voters; admitted records stay unconfirmed; restore exact clean images |
| `consumer` | Close and reopen a reader at its verified checkpoint |
| `resume` | Resume a closed producer with its saved identity |
| `takeover` | Fence the live old producer and start a new epoch |
| `reconnect` | Replace OMQ links and resume with a fresh SDK identity |
| `completions` | Run physical work while holding its completions |
| `restart` | Restart a broker with its retained memory image |
| `process-crash` | Cut a live memory device and keep dirty cached bytes |
| `power-loss` | Cut a live memory device and keep only modeled durable effects |
| `torn-write` | Inject a short write |
| `fail-write` | Fail a physical write and keep the fail-closed broker evidence |

### Artifacts

| File | Content |
| --- | --- |
| `config.json` | Selected controls |
| `schedule.jsonl` | Full injected fault schedule, with clock observations |
| `report.json` | Terminal report; `complete: true` after a clean shutdown |
| `failure.json` | First-failure evidence: recent events, outstanding records, configuration, physical order |
| `image-<broker>.json` | Dirty and durable memory images at the first failure |

Progress prints every minute.

### Replay

```sh
scripts/ozzy_cargo run -p ozzy-sim --features broker --bin ozzy_sim -- \
  --replay tmp/ozzy-sim-rp-42/schedule.jsonl --waves 100 \
  --artifacts tmp/ozzy-sim-rp-42-replay
```

- Use a new artifact directory.
- Replay controls the recorded workload and fault boundaries, not the threaded
  OMQ or kernel schedule.
- With `--simulated-duration`, replay waits for a boundary reached early. It
  cannot rewind a boundary reached late.
- An inproc pass still needs TCP/process-crash qualification.

## In the code

- `src/soak.rs`: sustained runner, actions, report.
- `src/broker.rs`, `src/broker/`: shared real-broker harness.
- `src/client.rs`: SDK client and record oracle.
- `src/bin/ozzy_sim.rs`: command line.
