# Development

Ozzy requires Rust 1.93 or newer.

## Local check

```sh
./scripts/test-all.sh
```

The script checks formatting, runs Clippy with warnings denied, runs workspace
tests through Nextest with eight threads, runs doctests separately, and builds
strict public API documentation.

Executables use the `ozy_` prefix (15 characters fit the kernel process name);
integration tests use `ozzy_`.
The suite also names its Cargo, compiler, and rustdoc processes. Use
`scripts/ozzy_cargo build --workspace` for the same naming and SSD build
directory on individual commands. Clippy retains its own compiler wrapper.

Individual commands:

```sh
source scripts/ozzy_tools.sh
cargo build --workspace
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo nextest run --workspace --test-threads 8
cargo test --workspace --doc
RUSTDOCFLAGS='-D warnings' cargo doc -p ozzy -p ozzy-runtime -p ozzy-proto \
  --no-deps
```

`ozzy-bench` is excluded from the default workspace members. Build it
explicitly when working on benchmarks:

```sh
cargo build -p ozzy-bench
```

For fast inside-out checks, use `scripts/test.sh core`, `writer`, `inproc`,
`simulation`, or `loom` before the full gate. [Validation](doc/VALIDATION.md)
explains each layer and its limits.

## Verification

```sh
cargo nextest run -p ozzy-journal -p ozzy-core -p ozzy-sim --test-threads 8
bash scripts/verify-rust.sh
bash scripts/verify-models.sh
```

The optional verification scripts require Kani and the TLA+ tools jar;
see [verification/README.md](verification/README.md) for versions, model bounds,
and limits of each claim. Set `OZZY_KANI_BIN` if `cargo-kani` is not on PATH,
and `OZZY_TLC_JAR` for a nondefault jar location. No tools are downloaded by
the check scripts. Do not run heavy verification alongside benchmarks.
