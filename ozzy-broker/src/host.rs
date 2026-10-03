use std::collections::BTreeSet;

use ozzy_config::HostResources;

use crate::StartupError;

/// Discover the calling thread's effective CPU affinity and allowed memory nodes.
/// This is startup validation, not a topology optimizer or an IRQ tuning tool.
#[cfg(target_os = "linux")]
pub fn host_resources() -> Result<HostResources, StartupError> {
    use rustix::thread::{CpuSet, sched_getaffinity};
    use std::collections::BTreeMap;
    use std::fs;

    let affinity =
        sched_getaffinity(None).map_err(|error| StartupError::Host(error.to_string()))?;
    let status = fs::read_to_string("/proc/thread-self/status")
        .map_err(|error| StartupError::Host(error.to_string()))?;
    let memory = status
        .lines()
        .find_map(|line| line.strip_prefix("Mems_allowed_list:"))
        .ok_or_else(|| StartupError::Host("missing allowed memory nodes".to_owned()))?;
    let memory_nodes = parse_id_list(memory.trim())?;
    let mut cpus = BTreeMap::new();
    for cpu in 0..CpuSet::MAX_CPU {
        if !affinity.is_set(cpu) {
            continue;
        }
        let mut node = None;
        let directory = format!("/sys/devices/system/cpu/cpu{cpu}");
        match fs::read_dir(&directory) {
            Ok(entries) => {
                for entry in entries {
                    let entry = entry.map_err(|error| StartupError::Host(error.to_string()))?;
                    if let Some(value) = entry.file_name().to_string_lossy().strip_prefix("node") {
                        let value = value
                            .parse::<u32>()
                            .map_err(|error| StartupError::Host(error.to_string()))?;
                        if node.replace(value).is_some() {
                            return Err(StartupError::Host("ambiguous CPU NUMA node".to_owned()));
                        }
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(StartupError::Host(error.to_string())),
        }
        cpus.insert(cpu as u32, node);
    }
    Ok(HostResources {
        cpus,
        memory_nodes,
        linux_aio: true,
    })
}

#[cfg(not(target_os = "linux"))]
pub fn host_resources() -> Result<HostResources, StartupError> {
    Err(StartupError::Host(
        "native host discovery currently requires Linux".to_owned(),
    ))
}

// Bound expansions even if a damaged fixture contains a huge range. Sparse OS
// identifiers are valid; they are not indexes into an allocation-sized CPU array.
fn parse_id_list(input: &str) -> Result<BTreeSet<u32>, StartupError> {
    const MAX_IDS: usize = 65_536;
    let bad = || StartupError::Host("invalid or excessive kernel ID list".to_owned());
    let mut ids = BTreeSet::new();
    for part in input.split(',') {
        let (first, last) = part.split_once('-').unwrap_or((part, part));
        let first = first.parse::<u32>().map_err(|_| bad())?;
        let last = last.parse::<u32>().map_err(|_| bad())?;
        if first > last || u64::from(last) - u64::from(first) + 1 > MAX_IDS as u64 {
            return Err(bad());
        }
        for id in first..=last {
            if !ids.insert(id) || ids.len() > MAX_IDS {
                return Err(bad());
            }
        }
    }
    Ok(ids)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sparse_kernel_ids_are_os_identifiers() {
        assert_eq!(
            parse_id_list("0-2,6,20-21").unwrap(),
            [0, 1, 2, 6, 20, 21].into()
        );
        assert_eq!(parse_id_list("1000000").unwrap(), [1_000_000].into());
    }

    #[test]
    fn malformed_duplicate_and_excessive_ranges_are_rejected() {
        for input in [
            "",
            "2-1",
            "1,,3",
            "1-",
            "-1",
            "0-4294967295",
            "1,1",
            "1-3,3-4",
            "x",
        ] {
            assert!(parse_id_list(input).is_err(), "{input}");
        }
    }
}
