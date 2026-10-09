# Releasing

Release-plz runs after successful main CI. It opens or updates a version PR;
merging that PR publishes through crates.io trusted publishing and creates
per-crate tags and GitHub releases. Failed or superseded main revisions do not
publish. Bot-created release PRs dispatch the same required CI checks.

1. Review the proposed versions and dependency updates.
2. Add a dated version section below `Unreleased` in
   [CHANGELOG.md](CHANGELOG.md), with categorized changes. Preserve existing
   version sections.
3. Merge with all required checks green. Check the `Release-plz` workflow and
   crates.io results. Its manual dispatch can retry a failed release after
   successful main CI.

## Published crates

The allowlist in [release-plz.toml](release-plz.toml) contains the broker, SDK,
and their supporting crates. `ozzy-bench` and `ozzy-sim` remain unpublished.

| Crate | Trusted publisher settings |
| --- | --- |
| `ozzy` | [crates.io](https://crates.io/crates/ozzy/settings) |
| `ozzy-broker` | [crates.io](https://crates.io/crates/ozzy-broker/settings) |
| `ozzy-config` | [crates.io](https://crates.io/crates/ozzy-config/settings) |
| `ozzy-core` | [crates.io](https://crates.io/crates/ozzy-core/settings) |
| `ozzy-io` | [crates.io](https://crates.io/crates/ozzy-io/settings) |
| `ozzy-io-aio` | [crates.io](https://crates.io/crates/ozzy-io-aio/settings) |
| `ozzy-io-pool` | [crates.io](https://crates.io/crates/ozzy-io-pool/settings) |
| `ozzy-journal` | [crates.io](https://crates.io/crates/ozzy-journal/settings) |
| `ozzy-journal-segment` | [crates.io](https://crates.io/crates/ozzy-journal-segment/settings) |
| `ozzy-proto` | [crates.io](https://crates.io/crates/ozzy-proto/settings) |
| `ozzy-replication` | [crates.io](https://crates.io/crates/ozzy-replication/settings) |
| `ozzy-runtime` | [crates.io](https://crates.io/crates/ozzy-runtime/settings) |

For each crate, configure GitHub owner `paddor`, repository `ozzy`, workflow
`release-plz.yml`, and no environment. The workflow requests an OIDC token;
it needs no long-lived registry secret. Registry configuration requires a crate
owner's account or a token with the trusted-publishing scope.

Correctness and soak commands live in [DEVELOPMENT.md](DEVELOPMENT.md);
performance isolation lives in [BENCHMARKS.md](BENCHMARKS.md).
