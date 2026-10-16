# Releasing

How Ozzy crates reach crates.io. For maintainers.

## How it works

- After a green CI run on a push to `main`, the `Release-plz` workflow
  (`release-plz.yml`) publishes merged version bumps and opens or updates a
  release PR.
- Only the tested, latest `main` revision publishes. Failed or superseded
  revisions do not.
- Bot-created release PRs dispatch the same required CI checks.
- Publishing uses crates.io trusted publishing over OIDC. No long-lived
  registry secret exists.
- Each published crate gets a `<crate>-v<version>` tag and a GitHub release
  that links to [CHANGELOG.md](CHANGELOG.md).
- Release-plz does not edit `CHANGELOG.md`.

## Release steps

1. Review the release PR's proposed versions and dependency updates.
2. In [CHANGELOG.md](CHANGELOG.md), add `## [<version>] - <YYYY-MM-DD>`
   directly below `## [Unreleased]`, with categorized subsections. Never edit
   existing version sections.
3. Merge with all required checks green.
4. Check the `Release-plz` run and the crates.io pages.
5. To retry a failed release, dispatch `Release-plz` manually on `main` after
   a green CI run.

## Published crates

- [release-plz.toml](release-plz.toml) lists the crates that release
  (`release = true`).
- `ozzy-bench`, `ozzy-sim`, and `ozzy-test-alloc` are `publish = false`.
- CI's `publish-check` job dry-runs `cargo publish` for every other workspace
  crate.

## Trusted publisher setup

For each released crate, open `https://crates.io/crates/<crate>/settings` and
add a GitHub trusted publisher:

| Field | Value |
| --- | --- |
| Owner | `paddor` |
| Repository | `ozzy` |
| Workflow | `release-plz.yml` |
| Environment | None |

This needs a crate owner's account or a token with the trusted-publishing
scope.

Correctness checks and soaks: [DEVELOPMENT.md](DEVELOPMENT.md). Performance
isolation: [BENCHMARKS.md](BENCHMARKS.md).
