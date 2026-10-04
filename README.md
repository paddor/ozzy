# Ozzy

Messaging and replicated logs over OMQ.

## Install

Install the broker with Rust 1.93 or newer:

```sh
cargo install ozzy-broker --version 0.1.0 --locked --bin ozy_broker
```

Provide a deployment TOML and explicitly initialize its identity, volumes, and
segment stores before serving. See [broker startup](ozzy-broker/README.md).

Applications use the native producer and consumer SDK:

```toml
[dependencies]
ozzy = "0.1.0"
```

## Start here

- [Rust API and example](ozzy/README.md)
- [Plain-language overview](doc/OVERVIEW.md)
- [Producer record flow](doc/OVERVIEW.md#producer-to-broker)
- [Consumer record flow](doc/OVERVIEW.md#broker-to-consumer)
- [Architecture and reference docs](DESIGN.md)
- [Build, test, and contribute](DEVELOPMENT.md) (Rust 1.93+)
- [Benchmark commands and measurement boundaries](ozzy-bench/README.md)
- [Benchmark charts](BENCHMARKS.md)
