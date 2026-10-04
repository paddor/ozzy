# ozzy-sim

Deterministic workload and fault tests around production cores. They control
message order, virtual time, physical storage effects, and completion
observation. An independent history checks submitted, confirmed, and
delivered records.

The fast suites exercise canonical state, three-broker replication, leader
change, recovery, and persisted history. Actor suites use production runtime
actors. These tests do not replace TCP/process-crash or real-disk checks.
The `broker` feature provides the shared real-broker OMQ inproc harness and SDK
record oracle. Broker integration tests and full product simulation use the same
production SDKs, broker owners, and controlled `ozzy-io` backend. Execution and
result delivery are separate events, with file/directory persistence modeled
without journal filesystem I/O. The smaller core suites control protocol
schedules; threaded OMQ runs do not claim exact deterministic execution from a
seed alone. See [test layers](../DEVELOPMENT.md#test-layers).

Run `scripts/test.sh simulation` for both layers, or `scripts/test.sh inproc`
for the full broker and SDK gates.

Run sustained workloads with the same production harness:

```sh
scripts/ozzy_cargo run -p ozzy-sim --features broker --bin ozy_sim -- \
  --mode replicated-persisting --seed 42 --duration 28800 \
  --interval-ms 10 --progress-timeout 30 \
  --artifacts /mnt/ssd/tmp/ozzy-sim-rp-42
```

Modes are `durable`, `disk-quorum`, and `replicated-persisting`. `--waves` limits
CI runs. `--scenarios N` runs N fresh clusters with successive seeds and separate
artifact subdirectories; the default sustains one cluster. The seed chooses sparse, burst, hot-partition and multipart traffic,
concurrent independent producers sharing partitions, saved-identity resume,
takeover while the old producer is still alive, consumer checkpoint reopen, fresh SDK transport
identities, independently delayed completions, broker restarts and short writes.
Paused consumers use separate SDK owners, repair missed PUB traffic through PEER,
consume slowly and retain returned payload aliases across close. Quorum-loss
actions stop two replicated brokers, require admitted records to remain
unconfirmed, drop confirmation futures and restore their exact clean images.
Retention-lag actions roll bounded 64-record cohorts while the foreground reader
verifies them, then reopen an expired checkpoint and require an explicit gap.
Process-crash actions cut the live memory device without draining physical work
and retain dirty bytes. Power-loss actions restore only modeled durable effects.
Both reopen the surviving image through production recovery selection; a held
completion regression verifies retries keep their original record identities.
Single durable delays writes without claiming quorum failover. Retention rolls
1 MiB segments and bounds selected history to 2 MiB per partition.

`--duration` bounds wall time for generating work. `--simulated-duration` also
bounds protocol time; `--clock-tick-ms` and `--clock-step-ms` control its pace
(defaults: 2 ms real time per 10 ms simulated time). SDK retries, broker deadlines
and append timestamps share that clock, including across broker restart. Storage
holds and threaded transport delivery remain independent. Each schedule boundary
has its own wall-clock progress deadline; finishing it and shutdown can extend
the generation limit.

`--interval-ms` controls load between bounded waves. `--actions` accepts a
comma-separated fault mix, for example `traffic,shared-producers,consumer,reconnect`;
repeating an action increases its frequency. Workload shape is drawn separately.
`--partitions` accepts 1 through 8. `--resident-mib`, `--physical-jobs` and
`--physical-mib` set production shard and device admission limits. Retention uses
`--retained-mib` (1 through 8) and `--retained-age` in seconds. The memory backend additionally
bounds each file to 16 MiB, media to 256 MiB per device, nodes to 4096 and directory
entries to 16384. SDK limits remain 128 admitted records and eight subscriptions;
each wave retains at most 32 records per partition or 64 in one hot partition.

Each wave verifies confirmation identity, partition-global offsets, payloads and
consumer checkpoints. Verified payload evidence is discarded between waves.
Storage keeps 4096 recent physical events per device and bounded media images;
the report counts completed churn, recovery faults and actual PUB/PEER delivery.
The selected controls are saved in `config.json`. The entire injected fault
schedule, including clock observations, streams to `schedule.jsonl`. The first failure
saves outstanding record evidence, configuration, recent physical order and
dirty/durable memory images before stopping. Orderly stopped owners retain their
final images until restoration, including when a fault boundary is interrupted.

Replay a fault prefix with `--replay /path/to/schedule.jsonl --waves N` and a new
artifact directory. This controls the recorded workload/fault boundaries, not
the exact threaded OMQ or kernel schedule. With manual time, replay waits for a
recorded boundary's observation when execution reaches it early; it cannot rewind
time if threaded execution reaches it late. The default runner uses elapsed time.
The shared harness also accepts `ServingContext::simulated` with a manual
`SdkClock` and a timestamp epoch, independently of physical execution and
completion delivery. Inproc runs still require TCP/process-crash qualification.
