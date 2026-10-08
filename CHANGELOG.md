# Changelog

## Unreleased

- Fix retention after broker restart and recovery across retired history.
- Fence stale recovery donors and preserve the first broker failure on shutdown.
- Remove the producer driver's shared reply-receiver lock; retain synchronized
  confirmation and failure publication.
- Extend the inproc simulator with churn, storage faults, and verified-progress
  monitoring; allow bounded storage pauses throughout a workload boundary.
- Automate release PRs and crates.io trusted publishing; shorten reference docs.

## 0.1.0 - 2026-10-07

Initial release of the Ozzy broker and native Rust producer and consumer SDKs.

- Single-broker durable, disk-quorum, and replicated-persisting modes.
- Producer identity resume and explicit takeover, with broker-owned retry identity.
- Live consumption, checkpoint replay, and bounded retained history.
- OMQ transport and a shared inproc simulator with workload churn and faults.
