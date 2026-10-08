# ozzy-broker

Ozzy is a message streaming system built on OMQ. Producers append records to
partitioned topics; consumers read live streams or replay retained history.
Ozzy adds persistence, replication, retention, and producer resume to OMQ's
message passing.

This crate provides the `ozzy_broker` server and its embedded Rust API. Brokers
own the segment journals. Each partition runs on one owner thread, with shared
transport and storage workers.

| Mode | Confirmation |
| --- | --- |
| Single durable | Persisted on one broker |
| Disk quorum | Persisted on two of three brokers |
| Replicated-persisting | Retained on two of three brokers; persisted in the background |

See [Getting started](https://github.com/paddor/ozzy/blob/main/GETTING_STARTED.md) for building, provisioning,
containers, and recovery. Applications use the [Rust SDK](https://github.com/paddor/ozzy/blob/main/ozzy/README.md).
The [design](https://github.com/paddor/ozzy/blob/main/DESIGN.md) describes ownership and durability boundaries.
