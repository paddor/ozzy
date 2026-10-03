use std::io;

/// Called inside a Ozzy-owned thread, before its local runtime or buffers exist.
#[cfg(target_os = "linux")]
pub(crate) fn pin(cpu: Option<u32>) -> io::Result<()> {
    use rustix::thread::{CpuSet, sched_getaffinity, sched_setaffinity};
    let Some(cpu) = cpu else {
        return Ok(());
    };
    let cpu = usize::try_from(cpu).map_err(|_| io::ErrorKind::InvalidInput)?;
    if cpu >= CpuSet::MAX_CPU || !sched_getaffinity(None)?.is_set(cpu) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "worker CPU is outside inherited process restrictions",
        ));
    }
    let mut mask = CpuSet::new();
    mask.set(cpu);
    sched_setaffinity(None, &mask)?;
    if sched_getaffinity(None)? != mask {
        return Err(io::Error::other("worker CPU placement was not applied"));
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn pin(cpu: Option<u32>) -> io::Result<()> {
    if cpu.is_some() {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "CPU placement requires Linux",
        ))
    } else {
        Ok(())
    }
}
