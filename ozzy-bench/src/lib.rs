//! Shared workloads, run identity, serial orchestration, and validated charts.
#![forbid(unsafe_code)]

pub mod automation;
pub mod control;
pub mod native;
pub mod placement;
pub mod provenance;
pub mod schedule;
pub mod workload;

pub type BenchResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn bench_error(message: impl Into<String>) -> Box<dyn std::error::Error + Send + Sync> {
    std::io::Error::other(message.into()).into()
}
