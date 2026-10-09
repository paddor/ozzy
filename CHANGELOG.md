# Changelog

## [Unreleased]

## [0.1.1] - 2026-10-09

### Breaking

- Rename the broker executable from `ozy_broker` to `ozzy`.

### Fixed

- Keep retention running after broker restart and repeated segment retirement.
- Recover replicas safely when retention has removed different history prefixes.
- Prevent incomplete or stale donor state from being used during recovery.
- Report the first broker failure through shutdown instead of losing its cause.

## 0.1.0 - 2026-10-07

Initial release of the Ozzy broker and native Rust producer and consumer SDKs.

- Single-broker durable, disk-quorum, and replicated-persisting modes.
- Producer identity resume and explicit takeover, with broker-owned retry identity.
- Live consumption, checkpoint replay, and bounded retained history.
- OMQ transport and a shared inproc simulator with workload churn and faults.
