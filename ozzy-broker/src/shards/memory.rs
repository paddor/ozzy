use crate::StartupError;
use ozzy_config::{BrokerPlan, ShardPlan};
use ozzy_runtime::memory::{Domain, Limits, Owner};
use std::{collections::BTreeMap, io};

/// Local payload caches with independent data and control reservations.
/// Cloned owners share these budgets across every partition on the shard.
#[derive(Debug)]
pub struct ShardMemory {
    pub data: Owner,
    pub control: Owner,
}

pub(super) struct Domains(BTreeMap<Option<u32>, Domain>);

pub(super) struct Plan {
    domain: Domain,
    data: Limits,
    control: Limits,
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

    pub(super) fn plan(&self, shard: &ShardPlan) -> Result<Plan, StartupError> {
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
        })
    }
}

impl Plan {
    // Called only after affinity is set on the destination thread.
    pub(super) fn allocate(self) -> io::Result<ShardMemory> {
        Ok(ShardMemory {
            data: self.domain.owner(self.data)?,
            control: self.domain.owner(self.control)?,
        })
    }
}
