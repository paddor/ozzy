//! Per-broker checks: devices, OMQ topology, shards, and NUMA pools.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use super::{require, safe_name};
use crate::{Broker, ConfigError, QueueBudget, Shard, invalid};

pub(super) fn validate_broker(broker: &Broker, path: &str) -> Result<Vec<Shard>, ConfigError> {
    validate_devices(broker, path)?;
    for (direction, limit) in [
        ("ingress", broker.topology.omq.ingress),
        ("egress", broker.topology.omq.egress),
    ] {
        if let Some(limit) = limit {
            require(
                limit.bytes_per_second > 0 && limit.burst_bytes > 0,
                &format!("{path}.topology.omq.{direction}"),
                "network bytes_per_second and burst_bytes must be positive",
            )?;
        }
    }
    require(
        (1..=256).contains(&broker.topology.omq.io_threads),
        path,
        "OMQ io_threads must be between 1 and 256",
    )?;
    let mut shards = broker.topology.shards.clone();
    if shards.is_empty() {
        let mut devices = broker.devices.keys();
        let (Some(device), None) = (devices.next(), devices.next()) else {
            return Err(invalid(
                path,
                "multiple devices require explicit shard-to-device assignments",
            ));
        };
        shards.push(Shard {
            id: 0,
            device: device.clone(),
            affinity: crate::Affinity::default(),
            budget: QueueBudget::default(),
        });
    }
    let mut ids = BTreeSet::new();
    for shard in &shards {
        require(
            ids.insert(shard.id) && broker.devices.contains_key(&shard.device),
            path,
            "duplicate shard ID or unknown shard device",
        )?;
        validate_shard(shard, path)?;
    }
    shards.sort_by_key(|shard| shard.id);
    check_numa_pools(broker, &shards, path)?;
    Ok(shards)
}

fn validate_devices(broker: &Broker, path: &str) -> Result<(), ConfigError> {
    require(
        !broker.devices.is_empty(),
        path,
        "at least one device required",
    )?;
    let mut roots: Vec<&Path> = Vec::new();
    let mut controllers = BTreeMap::new();
    for (name, device) in &broker.devices {
        safe_name(name, path)?;
        safe_name(&device.controller, path)?;
        let device_path = format!("{path}.devices.{name}");
        safe_root(&device.root, &device_path)?;
        require(
            roots
                .iter()
                .all(|root| !root.starts_with(&device.root) && !device.root.starts_with(root)),
            &device_path,
            "device roots overlap",
        )?;
        roots.push(&device.root);
        let workers = &device.workers;
        require(
            (1..=64).contains(&workers.write_threads)
                && (workers.cpus.is_empty() || workers.cpus.len() == workers.write_threads)
                && workers.cpus.iter().collect::<BTreeSet<_>>().len() == workers.cpus.len()
                && (workers.backend == crate::IoBackend::Aio || workers.aio_cpu.is_none())
                && (1..=64).contains(&workers.aio_depth)
                && workers.max_inflight >= workers.aio_depth
                && workers.max_inflight <= workers.queued_jobs
                && workers.queued_jobs > 0
                && workers.queued_bytes > 0
                && workers.progress_jobs > 0
                && workers.progress_bytes > 0
                && workers.open_handles > 0
                && workers
                    .queued_jobs
                    .checked_add(workers.progress_jobs)
                    .is_some()
                && workers
                    .queued_bytes
                    .checked_add(workers.progress_bytes)
                    .is_some(),
            &device_path,
            "invalid storage worker count, affinity or admission budget",
        )?;
        if let Some(previous) = controllers.insert(&device.controller, workers) {
            require(
                previous == workers,
                &device_path,
                "devices sharing a controller must share worker settings",
            )?;
        }
    }
    Ok(())
}

fn validate_shard(shard: &Shard, path: &str) -> Result<(), ConfigError> {
    let budget = &shard.budget;
    require(
        budget.append_slots > 0
            && budget.control_slots > 0
            && budget.resident_bytes > 0
            && budget.control_bytes > 0
            && budget
                .append_slots
                .checked_add(budget.control_slots)
                .is_some()
            && budget
                .resident_bytes
                .checked_add(budget.control_bytes)
                .is_some(),
        path,
        "invalid shard data/control budget",
    )?;
    require(
        shard.affinity.numa_node.is_none() || shard.affinity.cpu.is_some(),
        path,
        "NUMA placement requires an explicit CPU",
    )
}

/// NUMA pools are unique and nonempty and cover the shards placed on them.
fn check_numa_pools(broker: &Broker, shards: &[Shard], path: &str) -> Result<(), ConfigError> {
    let mut pools = BTreeMap::new();
    for pool in &broker.topology.memory_pools {
        require(
            pool.bytes > 0 && pools.insert(pool.numa_node, pool.bytes).is_none(),
            path,
            "duplicate or empty NUMA memory pool",
        )?;
    }
    let mut reserved = BTreeMap::<u32, u64>::new();
    for shard in shards {
        if let Some(node) = shard.affinity.numa_node {
            let total = reserved.entry(node).or_default();
            *total = total
                .checked_add(shard.budget.resident_bytes + shard.budget.control_bytes)
                .ok_or_else(|| invalid(path, "NUMA memory reservation overflow"))?;
            require(
                pools.get(&node).is_some_and(|bytes| bytes >= total),
                path,
                "NUMA memory pool does not cover shard reservations",
            )?;
        }
    }
    Ok(())
}

fn safe_root(root: &Path, path: &str) -> Result<(), ConfigError> {
    let value = root
        .to_str()
        .ok_or_else(|| invalid(path, "storage root must be UTF-8"))?;
    require(
        root.is_absolute()
            && value != "/"
            && !value
                .bytes()
                .any(|byte| byte == 0 || byte.is_ascii_control())
            && value[1..]
                .split('/')
                .all(|part| !part.is_empty() && part != "." && part != ".."),
        path,
        "storage root must be an absolute normalized path below filesystem root",
    )
}
