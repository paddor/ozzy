# Benchmarks

These charts compare Ozzy, Iggy, and Redpanda with eight partitions, four
writer processes, one verified reader per partition, and 128 B, 1 KiB, or
8 KiB JSON event records. Segments are 1 GiB. All systems use the same SSD.
Ozzy allows one outstanding APPEND per writer.

Saturation runs warm up for 2 s and measure for 8 s, with two repetitions.
Fixed-load runs warm up for 2 s, then offer 100/s for 30 s and 1k/s, 10k/s,
and 100k/s for 8 s each, with one repetition. The stages form a continuous
upward ramp without a fresh warmup. Transition effects are part of each stage's
latency. A separate 1M/s test covers 128 B single-broker writes. The
fixed-load clock starts at scheduled arrival.

This VM has six physical cores and no hyperthreads. Single-broker runs give
the broker CPUs 0, 1 and clients CPUs 2-5. Three-broker runs give each broker
one core (0, 1, 2) and clients CPUs 3-5. Pin every benchmark thread to its
assigned CPU set. Do not let a client thread migrate onto a broker core.
The current charts use host VM process nice -20 and KVM `halt_poll_ns=2000000`.
The inproc gate pins the shard, dispatcher, data worker, progress worker,
writer/SDK/OMQ, and reader to CPUs 0-5 respectively. Inproc transport alone
does not isolate CPU work. If thread placement is unavailable, use separate
processes with `taskset` and a process transport. Run performance checks
serially on an otherwise idle VM. The [benchmark runner](ozzy-bench/README.md)
has commands and confirmation boundaries.
Record the host VM process priority and KVM `halt_poll_ns` setting with each
run. Repeat the baseline after either setting changes before comparing results.

The fast inproc gate exercises the production writer, broker, and reader over
OMQ inproc, with real 1 MiB segments under `/tmp` tmpfs. It checks verified
throughput at all three record sizes. It detects large software regressions
without measuring physical storage. Run it alone; competing tests make a
wall-clock performance gate unreliable.

## Single broker: durable

![Single-broker durable saturation](doc/charts/single/durable.svg)

![Single-broker durable fixed load](doc/charts/single/durable-fixed-load.svg)

## Three brokers: disk quorum

![Three-broker disk-quorum saturation](doc/charts/cluster/disk-quorum.svg)

![Three-broker disk-quorum fixed load](doc/charts/cluster/disk-quorum-fixed-load.svg)

## Three brokers: replicated-persisting

![Three-broker replicated-persisting saturation](doc/charts/cluster/replicated-persisting.svg)

![Three-broker replicated-persisting fixed load](doc/charts/cluster/replicated-persisting-fixed-load.svg)
