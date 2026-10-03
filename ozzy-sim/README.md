# ozzy-sim

Deterministic workload and fault tests around production cores. They control
message order, virtual time, physical storage effects, and completion
observation. An independent history checks submitted, confirmed, and
delivered records.

The fast suites exercise canonical state, three-broker replication, leader
change, recovery, and persisted history. Actor suites use production runtime
actors. These tests do not replace TCP/process-crash or real-disk checks.
The controlled `ozzy-io` file backend adds owned jobs, separate execution/result
delivery and file/directory persistence without real disk I/O. Real-broker OMQ
inproc tests live in `ozzy-broker`, sharing this backend rather than substituting
a second broker implementation. See [validation](../doc/VALIDATION.md).
