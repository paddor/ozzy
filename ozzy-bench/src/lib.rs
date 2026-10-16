#![warn(missing_docs)]
//! Shared workloads, run identity, serial orchestration, and validated charts.
#![forbid(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable,
        clippy::too_many_lines,
        clippy::too_many_arguments,
        reason = "product code is linted without cfg(test)"
    )
)]

pub mod automation;
pub mod budget;
pub mod control;
pub mod dedup;
pub mod native;
pub mod placement;
pub mod provenance;
pub mod schedule;
/// Deterministic payload corpora and delivery verification.
pub mod workload;

/// Benchmark failures propagated across asynchronous workers and control tasks.
pub type BenchResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn bench_error(message: impl Into<String>) -> Box<dyn std::error::Error + Send + Sync> {
    std::io::Error::other(message.into()).into()
}
