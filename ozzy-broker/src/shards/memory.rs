use crate::StartupError;
use ozzy_config::{BrokerPlan, ShardPlan};
use ozzy_runtime::memory::{Domain, Limits, Owner};
use std::{collections::BTreeMap, io};

/// Local payload caches with independent data and control reservations.
/// Cloned owners share these budgets across every partition on the shard.
#[derive(Debug)]
pub struct ShardMemory {
    /// Writer canonical work. Shares the resident budget with follower progress.
    pub data: Owner,
    /// Follower canonical work, independently reserved in replicated serving.
    pub replica: Owner,
    /// Independent retained control-message allocation owner.
    pub control: Owner,
}

pub(super) struct Domains(BTreeMap<Option<u32>, Domain>);

pub(super) struct Plan {
    domain: Domain,
    data: Limits,
    control: Limits,
    follower_progress: bool,
}

impl Domains {
    pub(super) fn new(plan: &BrokerPlan) -> Result<Self, StartupError> {
        let error = |reason: &str| StartupError::Runtime(reason.into());
        let mut required = BTreeMap::<Option<u32>, u64>::new();
        for shard in &plan.shards {
            let total = required.entry(shard.affinity.numa_node).or_default();
            *total = total
                .checked_add(shard.budget.resident_bytes)
                .and_then(|value| value.checked_add(shard.budget.control_bytes))
                .ok_or_else(|| error("application memory budget overflow"))?;
        }
        let mut configured = BTreeMap::new();
        for pool in &plan.memory_pools {
            if configured
                .insert(Some(pool.numa_node), pool.bytes)
                .is_some()
            {
                return Err(error("duplicate application NUMA pool"));
            }
        }
        if let Some(&unplaced) = required.get(&None) {
            configured.insert(None, unplaced);
        }
        for (node, required) in required {
            if configured.get(&node).is_none_or(|bytes| *bytes < required) {
                return Err(error(
                    "NUMA pool cannot cover its application shard reservations",
                ));
            }
        }
        let mut domains = BTreeMap::new();
        for (node, bytes) in configured {
            let bytes =
                usize::try_from(bytes).map_err(|_| error("memory pool exceeds address space"))?;
            domains.insert(
                node,
                Domain::new(node, bytes).map_err(|source| error(&source.to_string()))?,
            );
        }
        Ok(Self(domains))
    }

    pub(super) fn plan(
        &self,
        shard: &ShardPlan,
        follower_progress: bool,
    ) -> Result<Plan, StartupError> {
        let limits = |bytes, buffers| -> Result<Limits, StartupError> {
            let bytes = usize::try_from(bytes).map_err(|_| StartupError::Shard {
                shard: shard.id,
                reason: "memory reservation exceeds address space".into(),
            })?;
            Ok(Limits {
                bytes,
                buffers,
                cache_bytes: bytes,
            })
        };
        Ok(Plan {
            domain: self.0[&shard.affinity.numa_node].clone(),
            data: limits(shard.budget.resident_bytes, shard.budget.append_slots)?,
            control: limits(shard.budget.control_bytes, shard.budget.control_slots)?,
            follower_progress,
        })
    }
}

impl Plan {
    // Called only after affinity is set on the destination thread.
    pub(super) fn allocate(self) -> io::Result<ShardMemory> {
        if !self.follower_progress {
            let data = self.domain.owner(self.data)?;
            return Ok(ShardMemory {
                replica: data.clone(),
                data,
                control: self.domain.owner(self.control)?,
            });
        }
        let replica_bytes = self.data.bytes / 2;
        let replica_buffers = self.data.buffers / 2;
        let replica = Limits {
            bytes: replica_bytes,
            buffers: replica_buffers,
            cache_bytes: replica_bytes,
        };
        let data = Limits {
            bytes: self.data.bytes - replica_bytes,
            buffers: self.data.buffers - replica_buffers,
            cache_bytes: self.data.bytes - replica_bytes,
        };
        Ok(ShardMemory {
            data: self.domain.owner(data)?,
            replica: self.domain.owner(replica)?,
            control: self.domain.owner(self.control)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn full_writer_memory_cannot_consume_follower_progress() {
        let domain = Domain::new(None, 10_240).unwrap();
        let memory = Plan {
            domain: domain.clone(),
            data: Limits {
                bytes: 8192,
                buffers: 8,
                cache_bytes: 8192,
            },
            control: Limits {
                bytes: 2048,
                buffers: 2,
                cache_bytes: 2048,
            },
            follower_progress: true,
        }
        .allocate()
        .unwrap();
        assert_eq!(domain.reserved_bytes(), 10_240);
        let writer = memory.data.lease(4096).await.unwrap();
        let follower = memory.replica.lease(4096).await.unwrap();
        let control = memory.control.lease(2048).await.unwrap();
        assert_eq!(memory.data.allocated_bytes(), 4096);
        assert_eq!(memory.replica.allocated_bytes(), 4096);
        drop((writer, follower, control, memory));
        assert_eq!(domain.reserved_bytes(), 0);
    }
}
