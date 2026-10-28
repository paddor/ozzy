//! Comparison argument validation: cases, bounds, placements, and CPUs.
use super::{Args, Result};
use crate::placement::Placement;

impl Args {
    /// Text patterns need room after the 8 B submission clock.
    fn patterns_valid(&self) -> bool {
        let known = ["events", "json", "random", "structured"];
        !self.patterns.is_empty()
            && self.patterns.iter().all(|p| known.contains(&p.as_str()))
            && !(self.sizes.contains(&16) && self.patterns.iter().any(|p| p == "json"))
    }

    fn validate_readers(&self) -> Result<()> {
        self.feeds
            .validate(self.feeds.writers(usize::try_from(self.partitions)?))?;
        if self.feeds.enabled()
            && (self.implementation != "ozzy"
                || self.dedup_window_secs.is_none()
                || self.records_per_second.is_none()
                || self.ramp.is_some())
        {
            return Err(
                "redundant/skew feeds require native dedup and a fixed offered rate".into(),
            );
        }
        if self.groups != 0 || self.dedup_window_secs.is_some() {
            if self.implementation != "ozzy" || self.readers_per_partition != 1 {
                return Err("native group/dedup experiments require --impl ozzy and the default reader copies".into());
            }
        } else if self.group_members != 1 || self.ack_window != 1024 {
            return Err("group members and acknowledgment windows require --groups".into());
        }
        if self.delay_millis.is_some()
            && (self.implementation != "ozzy"
                || self.groups != 0
                || self.dedup_window_secs.is_some())
        {
            return Err("--delay-millis requires --impl ozzy without groups or dedup".into());
        }
        // One broker group admits at most 32 preprovisioned readers.
        if !(1..=32).contains(&self.readers())
            || (self.readers_per_partition != 1 && self.implementation != "ozzy")
        {
            return Err("several readers per partition require Ozzy and at most 32 readers".into());
        }
        if self
            .broker_io_threads
            .is_some_and(|threads| !(1..=32).contains(&threads) || self.implementation != "ozzy")
        {
            return Err("broker I/O threads require Ozzy and 1..=32".into());
        }
        if self.payload_compression != "adaptive" && self.implementation != "ozzy" {
            return Err("payload compression control requires Ozzy".into());
        }
        if self.live_readers
            && (self.implementation != "ozzy" || self.modes.iter().any(|m| m != "disk-quorum"))
        {
            return Err("live readers require Ozzy disk-quorum".into());
        }
        Ok(())
    }

    fn validate_shards(&self) -> Result<()> {
        if self.shards.is_some_and(|shards| {
            shards == 0
                || shards > self.partitions
                || self.implementation != "ozzy"
                || self.modes.iter().any(|mode| mode != "durable")
        }) {
            return Err("shard override requires native durable mode and 1..=partitions".into());
        }
        Ok(())
    }

    fn validate_placements(&self) -> Result<()> {
        let Some(path) = &self.placements else {
            if self.control_bind.is_some() {
                return Err("control bind needs placements".into());
            }
            return Ok(());
        };
        let placements = Placement::load(Some(path))?;
        if !self.placements_valid(&placements)
            || self.modes.iter().any(|m| m != "disk-quorum")
            || self.implementation == "redpanda"
            || self
                .control_bind
                .as_ref()
                .is_none_or(|s| !s.starts_with("tcp://"))
        {
            return Err("placements require all-local or two local/one remote brokers, disjoint local broker CPU sets (one CPU each across hosts) or one all-local broker CPU pool, explicit storage, disk-quorum, and TCP control".into());
        }
        Ok(())
    }

    /// Whether the brokers are all local, or two local and one remote, each
    /// with explicit storage and CPUs. Local CPU sets stay apart from each
    /// other, unless all share the broker pool, and from the clients.
    fn placements_valid(&self, placements: &[Placement; 3]) -> bool {
        let pool = self.broker_cpus.pool();
        let all_local = placements.iter().all(|p| p.remote.is_none());
        let shared_pool = all_local && placements.iter().all(|p| p.cpus.as_ref() == Some(&pool));
        let Some(cpus) = placements
            .iter()
            .map(|p| p.storage_dir.as_ref().and(p.cpus.as_deref()))
            .collect::<Option<Vec<&[usize]>>>()
        else {
            return false;
        };
        let local = if all_local { &cpus[..] } else { &cpus[..2] };
        // Cross-host brokers keep one CPU. A local broker may own several,
        // such as both threads of one physical core.
        let sets_fit = shared_pool
            || cpus.iter().all(|set| {
                !set.is_empty()
                    && (all_local || set.len() == 1)
                    && (!all_local || set.iter().all(|cpu| pool.contains(cpu)))
            });
        let local_disjoint = shared_pool
            || local.iter().enumerate().all(|(i, set)| {
                local[..i]
                    .iter()
                    .all(|taken| !set.iter().any(|cpu| taken.contains(cpu)))
            });
        placements[..2].iter().all(|p| p.remote.is_none())
            && (all_local || placements[2].remote.is_some())
            && sets_fit
            && (all_local || !cpus[2].contains(&5))
            && local_disjoint
            && local
                .iter()
                .all(|set| !set.iter().any(|cpu| self.client_cpus.contains(cpu)))
    }

    /// Reject unsupported cases, conflicting options, and invalid placement or
    /// capacity against the caller's allowed CPU inventory.
    pub fn validate(&self, allowed: &[usize]) -> Result<()> {
        self.validate_shards()?;
        self.validate_placements()?;
        self.validate_layers()?;
        if !self.selection_valid() {
            return Err("invalid case selection".into());
        }
        if !self.bounds_valid() {
            return Err("invalid bounded workload".into());
        }
        self.validate_readers()?;
        if !self.cpus_valid(allowed) {
            return Err("invalid CPU placement".into());
        }
        self.validate_load()?;
        if [
            "OZZY_BENCH_PROFILE",
            "OZZY_BENCH_HEAPTRACK",
            "OZZY_TELEMETRY_ENDPOINT",
        ]
        .iter()
        .any(|k| std::env::var_os(k).is_some())
        {
            return Err("profiling must run separately from comparisons".into());
        }
        Ok(())
    }

    /// Memory storage, inproc workers, and compressed transports are Ozzy
    /// layers with their own placement rules.
    fn validate_layers(&self) -> Result<()> {
        if self.memory_storage && self.implementation != "ozzy" {
            return Err("memory-storage and inproc layers are Ozzy-only".into());
        }
        if self.transport != "tcp" && (self.implementation != "ozzy" || self.inproc) {
            return Err("compressed transports need Ozzy brokers in separate processes".into());
        }
        if self.inproc && self.control_bind.is_some() {
            return Err("inproc workers share one local process".into());
        }
        Ok(())
    }

    /// Supported record sizes, modes, and payload patterns.
    fn selection_valid(&self) -> bool {
        !self.sizes.is_empty()
            && self
                .sizes
                .iter()
                .all(|s| *s == 16 || (32..=16384).contains(s))
            && !self.modes.is_empty()
            && self
                .modes
                .iter()
                .all(|m| matches!(m.as_str(), "buffered" | "durable" | "disk-quorum"))
            && self.patterns_valid()
    }

    /// Repetitions, windows, and every request, reader, and storage bound.
    fn bounds_valid(&self) -> bool {
        (1..=30).contains(&self.repetitions)
            && (1..=32).contains(&self.partitions)
            && (1..=65536).contains(&self.request_records)
            && (1..=65536).contains(&self.reader_records)
            && (1..=128).contains(&self.reader_payload_mib)
            && (1..=65536).contains(&self.writer_inflight_appends)
            && self.shard_resident_mib.is_none_or(|mib| {
                (1..=16384).contains(&mib)
                    && !matches!(self.implementation.as_str(), "iggy" | "redpanda")
            })
            && (0.05..=120.0).contains(&self.duration)
            && (0.0..=10.0).contains(&self.warmup)
            && (4..=65536).contains(&self.disk_history_mib)
            && (4..=1024).contains(&self.segment_mib.unwrap_or(4))
    }

    /// Broker and client CPUs are disjoint and inside `allowed`. A shared pool
    /// is one CPU range; per-broker lists may pair hyperthread siblings such
    /// as 0 and 6.
    fn cpus_valid(&self, allowed: &[usize]) -> bool {
        let pool = self.broker_cpus.pool();
        !self.client_cpus.is_empty()
            && (self.broker_cpus.per_broker()
                || pool.windows(2).all(|w| w[1] == w[0] + 1)
                || (self.placements.is_some() && self.implementation == "ozzy"))
            && !pool.iter().any(|c| self.client_cpus.contains(c))
            && pool
                .iter()
                .chain(&self.client_cpus)
                .all(|c| allowed.contains(c))
    }

    /// A positive offered load, a ramp matching the window, and some case.
    fn validate_load(&self) -> Result<()> {
        if self.records_per_second == Some(0) {
            return Err("offered load must be positive".into());
        }
        if let Some(ramp) = &self.ramp
            && ((self.duration * 1e9).round() as u64 != ramp.duration_ns() || self.check_only)
        {
            return Err("--duration must equal the ramp total".into());
        }
        if self.cases().is_empty() {
            return Err("selected implementation has no cases in these modes".into());
        }
        Ok(())
    }
}
