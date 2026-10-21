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
- `ozzy-bench`, `ozzy-sim`, `ozzy-test-alloc`, `ozzy-python`, `ozzy-java`, and
  `ozzy-bindings` are
  `publish = false`.
- CI's `publish-check` job dry-runs `cargo publish` for every other workspace
  crate.

## Python package

`bindings/python` builds the `ozzy` Python package with maturin. Its version
comes from the Rust workspace. Refresh `bindings/python/uv.lock` after a version
bump and run `scripts/check-python.sh` for Python 3.12 and 3.14 before preparing
wheels. Build commands and supported APIs live in the
[Python SDK README](bindings/python/README.md). Python packages are not uploaded
by the crates.io release workflow.

## Java package

`bindings/java/pom.xml` versions `io.github.paddor:ozzy-java`; keep it aligned
with the Rust workspace. `scripts/check-java.sh` builds a Java-only JAR and a
current-platform classifier JAR containing the native library. Test Java 22 and
a newer JDK before preparing packages. There is no automatic Maven publication.
Usage and native-loading options: [Java SDK README](bindings/java/README.md).

## Trusted publisher setup

A new crate's first version is published by hand: an owner runs
`cargo publish -p <crate>`, after any unpublished workspace crate it depends
on. Trusted publishing covers later versions.

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
