//! Timed production Ozzy workloads and external comparisons.

#[cfg(target_os = "linux")]
#[path = "timed_bench/mod.rs"]
mod bench;

#[cfg(target_os = "linux")]
#[tokio::main(flavor = "current_thread")]
async fn main() -> bench::Result<()> {
    if std::env::var("OZZY_BENCH_PROFILE").as_deref() == Ok("stages") {
        ozzy_runtime::profiling::enable();
    }
    bench::run().await
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("ozy_timed_bench requires Linux CPU and peak-RSS accounting");
    std::process::exit(1);
}
