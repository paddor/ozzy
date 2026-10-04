//! Finite admission limits applied through the production deployment contract.
use serde::Serialize;

/// Resource and retention bounds for generated full-product runs.
#[derive(Clone, Debug, Serialize)]
pub struct Resources {
    /// Shared partitions; each generated wave retains at most 32 records per partition.
    pub partitions: u32,
    /// Payload backing allowance per shard, independent of control capacity.
    pub resident_bytes: u64,
    /// Retained ordinary physical jobs per device, including unread completions.
    pub physical_jobs: usize,
    /// Byte allowance for those physical jobs.
    pub physical_bytes: u64,
    /// Selected journal byte target per partition, in whole segments.
    pub retained_bytes: u64,
    /// Age deadline for sealed groups.
    pub retained_age_secs: u64,
}

impl Default for Resources {
    fn default() -> Self {
        Self {
            partitions: 4,
            resident_bytes: 256 * 1024 * 1024,
            physical_jobs: 256,
            physical_bytes: 128 * 1024 * 1024,
            retained_bytes: 2 * 1024 * 1024,
            retained_age_secs: 120,
        }
    }
}

impl Resources {
    pub(super) fn validate(&self) -> Result<(), String> {
        if !(1..=8).contains(&self.partitions)
            || self.resident_bytes < 4 * 1024 * 1024
            || self.physical_jobs < 8
            || self.physical_bytes < 1024 * 1024
            || !(1024 * 1024..=8 * 1024 * 1024).contains(&self.retained_bytes)
            || self.retained_age_secs == 0
        {
            return Err("partitions must be 1..8, resident memory >=4 MiB, physical jobs >=8, physical bytes >=1 MiB, and retention 1..8 MiB with positive age".into());
        }
        Ok(())
    }

    pub(super) fn configure(&self, deployment: &mut ozzy_config::Deployment) {
        let topic = deployment.topics.get_mut("orders").unwrap();
        topic.partitions = self.partitions;
        topic.retention.max_bytes = Some(self.retained_bytes);
        topic.retention.max_age_secs = Some(self.retained_age_secs);
        for broker in deployment.brokers.values_mut() {
            for shard in &mut broker.topology.shards {
                shard.budget.resident_bytes = self.resident_bytes;
            }
            for device in broker.devices.values_mut() {
                device.workers.queued_jobs = self.physical_jobs;
                device.workers.max_inflight = device.workers.max_inflight.min(self.physical_jobs);
                device.workers.queued_bytes = self.physical_bytes;
            }
        }
    }
}
