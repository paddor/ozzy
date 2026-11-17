//! Throughput and confirmation latency of one log through the in-process
//! log API: one writer, several owner appends in flight.
#![forbid(unsafe_code)]

mod log_bench;

fn main() -> log_bench::Result<()> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(log_bench::run())
}
