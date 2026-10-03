use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use crate::validate::require;
use crate::{
    Affinity, ConfigError, IoBackend, QueueBudget, StorageWorkers, ValidatedDeployment, invalid,
};

/// Effective resources available to this process, not the machine's full CPU set.
/// The caller supplies OS discovery or a deterministic simulator fixture.
#[derive(Debug, Clone)]
pub struct HostResources {
    /// Allowed OS CPU IDs and each CPU's NUMA node, if known.
    pub cpus: BTreeMap<u32, Option<u32>>,
    pub memory_nodes: BTreeSet<u32>,
    pub linux_aio: bool,
}

/// Validated local placement. Constructed before starting runtime workers.
#[derive(Debug, Clone)]
pub struct BrokerPlan {
    pub name: String,
    pub omq_io_threads: usize,
    pub dispatcher: Affinity,
    pub shards: Vec<ShardPlan>,
    pub controllers: Vec<ControllerPlan>,
    pub memory_pools: Vec<crate::MemoryPool>,
    pub partitions: Vec<PartitionPlacement>,
}

/// One execution pool per used controller. Lanes follow the listed shard order;
/// sparse application shard IDs are not backend lane indexes.
#[derive(Debug, Clone)]
pub struct ControllerPlan {
    pub name: String,
    pub shards: Vec<u32>,
    pub workers: StorageWorkers,
}

#[derive(Debug, Clone)]
pub struct ShardPlan {
    pub id: u32,
    pub affinity: Affinity,
    pub device: String,
    pub budget: QueueBudget,
}

/// Local directory and execution owner; never advertised as SDK routing metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionPlacement {
    pub topic: String,
    pub partition: u32,
    pub shard: u32,
    pub device: String,
    pub directory: PathBuf,
}

impl ValidatedDeployment {
    /// Resolve a selected broker against its current effective host restrictions.
    /// Filesystem aliases/symlinks must additionally be checked when opening roots.
    pub fn broker_plan(&self, name: &str, host: &HostResources) -> Result<BrokerPlan, ConfigError> {
        let broker = self
            .deployment
            .brokers
            .get(name)
            .ok_or_else(|| invalid("broker", "unknown selected broker"))?;
        require(!host.cpus.is_empty(), name, "process has no usable CPUs")?;
        let mut pinned = BTreeSet::new();
        check_affinity(&broker.topology.dispatcher, host, &mut pinned, name)?;
        for pool in &broker.topology.memory_pools {
            require(
                host.memory_nodes.contains(&pool.numa_node),
                name,
                "memory pool NUMA node is outside process restrictions",
            )?;
        }
        let mut controllers = BTreeSet::new();
        for device in broker.devices.values() {
            require(
                device.workers.backend != IoBackend::Aio || host.linux_aio,
                name,
                "Linux AIO requested but unsupported on this host",
            )?;
            if controllers.insert(&device.controller) {
                for cpu in device
                    .workers
                    .cpus
                    .iter()
                    .chain(device.workers.progress_cpu.iter())
                    .chain(device.workers.aio_cpu.iter())
                {
                    check_affinity(
                        &Affinity {
                            cpu: Some(*cpu),
                            numa_node: None,
                        },
                        host,
                        &mut pinned,
                        name,
                    )?;
                }
            }
        }
        let shards = &self.shards[name];
        for shard in shards {
            check_affinity(&shard.affinity, host, &mut pinned, name)?;
        }
        Ok(BrokerPlan {
            name: name.to_owned(),
            omq_io_threads: broker.topology.omq.io_threads,
            dispatcher: broker.topology.dispatcher.clone(),
            controllers: self.controller_plans(name)?,
            memory_pools: broker.topology.memory_pools.clone(),
            shards: shards
                .iter()
                .map(|shard| ShardPlan {
                    id: shard.id,
                    device: shard.device.clone(),
                    affinity: shard.affinity.clone(),
                    budget: shard.budget.clone(),
                })
                .collect(),
            partitions: self.partition_placements(name),
        })
    }

    pub(crate) fn partition_placements(&self, name: &str) -> Vec<PartitionPlacement> {
        let broker = &self.deployment.brokers[name];
        let shards = &self.shards[name];
        let overrides: BTreeMap<_, _> = broker
            .topology
            .partitions
            .iter()
            .map(|entry| ((entry.topic.as_str(), entry.partition), entry.shard))
            .collect();
        let mut placements = Vec::new();
        let mut ordinal = 0;
        for (topic_name, topic) in &self.deployment.topics {
            for partition in 0..topic.partitions {
                let shard = match overrides.get(&(topic_name.as_str(), partition)) {
                    Some(id) => shards.iter().find(|shard| shard.id == *id).unwrap(),
                    None => &shards[ordinal % shards.len()],
                };
                placements.push(PartitionPlacement {
                    topic: topic_name.clone(),
                    partition,
                    shard: shard.id,
                    device: shard.device.clone(),
                    directory: broker.devices[&shard.device]
                        .root
                        .join("topics")
                        .join(topic_name)
                        .join("partitions")
                        .join(partition.to_string()),
                });
                ordinal += 1;
            }
        }
        placements
    }

    fn controller_plans(&self, name: &str) -> Result<Vec<ControllerPlan>, ConfigError> {
        let broker = &self.deployment.brokers[name];
        let shards = &self.shards[name];
        let mut pools = BTreeMap::<String, ControllerPlan>::new();
        for shard in shards {
            let device = &broker.devices[&shard.device];
            pools
                .entry(device.controller.clone())
                .or_insert_with(|| ControllerPlan {
                    name: device.controller.clone(),
                    shards: Vec::new(),
                    workers: device.workers.clone(),
                })
                .shards
                .push(shard.id);
        }
        for pool in pools.values() {
            let count = pool.shards.len();
            let w = &pool.workers;
            require(
                w.queued_jobs >= count && w.progress_jobs >= count && w.open_handles >= count,
                name,
                "controller must reserve jobs and handles for every assigned shard",
            )?;
            let max_append = self
                .deployment
                .topics
                .values()
                .map(|t| t.max_append_bytes)
                .max()
                .unwrap();
            require(
                w.queued_bytes / count as u64 >= max_append && w.progress_bytes / count as u64 > 0,
                name,
                "controller byte shares must hold work for every assigned shard",
            )?;
            require(
                usize::try_from(w.queued_bytes).is_ok()
                    && usize::try_from(w.progress_bytes).is_ok(),
                name,
                "controller byte limits exceed native address space",
            )?;
        }
        Ok(pools.into_values().collect())
    }
}

fn check_affinity(
    affinity: &Affinity,
    host: &HostResources,
    pinned: &mut BTreeSet<u32>,
    path: &str,
) -> Result<(), ConfigError> {
    if let Some(cpu) = affinity.cpu {
        let node = host
            .cpus
            .get(&cpu)
            .ok_or_else(|| invalid(path, format!("CPU {cpu} is outside process restrictions")))?;
        require(
            pinned.insert(cpu),
            path,
            "explicit thread CPU assignments overlap",
        )?;
        if let Some(expected) = affinity.numa_node {
            require(
                *node == Some(expected) && host.memory_nodes.contains(&expected),
                path,
                "CPU locality is unknown, mismatched or outside memory restrictions",
            )?;
        }
    } else {
        require(
            affinity.numa_node.is_none(),
            path,
            "NUMA placement requires an explicit CPU",
        )?;
    }
    Ok(())
}
