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
seed alone. See [validation](../doc/VALIDATION.md).

Run `scripts/test.sh simulation` for both layers, or `scripts/test.sh inproc`
for the full broker and SDK gates.

Run sustained workloads with the same production harness:

```sh
scripts/ozzy_cargo run -p ozzy-sim --features broker --bin ozzy-sim -- \
  --mode replicated-persisting --seed 42 --duration 28800 \
  --interval-ms 10 --progress-timeout 30 \
  --artifacts /mnt/ssd/tmp/ozzy-sim-rp-42
```

Modes are `durable`, `disk-quorum`, and `replicated-persisting`. `--waves` limits
CI runs. The seed chooses sparse, burst, hot-partition and multipart traffic,
producer resume/takeover, consumer checkpoint reopen, fresh SDK transport
identities, independently delayed completions, broker restarts and short writes.
Single durable delays writes without claiming quorum failover. Retention rolls
1 MiB segments and bounds selected history to 2 MiB per partition.

Each wave verifies confirmation identity, partition-global offsets, payloads and
consumer checkpoints. Verified payload evidence is discarded between waves.
Storage keeps 4096 recent physical events per device and bounded media images;
the report counts completed churn, recovery faults and actual PUB/PEER delivery.
The entire injected fault schedule streams to `schedule.jsonl`. The first failure
saves outstanding record evidence, configuration, recent physical order and
dirty/durable memory images before stopping.

Replay a fault prefix with `--replay /path/to/schedule.jsonl --waves N` and a new
artifact directory. This controls the recorded workload/fault boundaries, not
the exact threaded OMQ or kernel schedule. Broker timers currently use elapsed
runtime time. The core simulator remains the layer for controlled virtual-time
protocol schedules; inproc runs still require TCP/process-crash qualification.
