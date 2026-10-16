//! Steps of `ozzy_profile`: the replayed workload command, live counter
//! collection, verification of the profiled run, and the profiler reports.
pub(crate) mod counters;
pub(crate) mod report;
pub(crate) mod verify;
pub(crate) mod workload;

/// This host's name, as profile rows record it.
fn local_host() -> ozzy_bench::automation::Result<String> {
    Ok(std::fs::read_to_string("/proc/sys/kernel/hostname")?
        .trim()
        .to_owned())
}
