//! Timed production Ozzy workloads and external comparisons.
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

#[cfg(target_os = "linux")]
#[path = "timed_bench/mod.rs"]
mod bench;

#[cfg(target_os = "linux")]
fn main() -> bench::Result<()> {
    if std::env::var("OZZY_BENCH_PROFILE").as_deref() == Ok("stages") {
        ozzy_runtime::profiling::enable();
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(bench::run())
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("ozzy_timed_bench requires Linux CPU and peak-RSS accounting");
    std::process::exit(1);
}
