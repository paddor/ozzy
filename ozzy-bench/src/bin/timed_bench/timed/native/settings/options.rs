//! Native deployment options and the worker flags that reproduce them.

use super::scaled;
use crate::bench::{Args, IoBackend, Result, error};
use ozzy_config::{QueueBudget, StorageWorkers};

/// Separate topic size, shard admission, and shared device admission.
#[derive(Debug, Clone, PartialEq, Eq, clap::Args)]
pub(in crate::bench) struct Options {
    #[command(flatten)]
    pub(in crate::bench::timed) feeds: ozzy_bench::dedup::Feeds,
    /// Independent full-topic consumer groups. Zero uses ordinary `TopicReader`.
    #[arg(long, default_value_t = 0, value_parser = clap::value_parser!(u32).range(0..=32))]
    pub(in crate::bench::timed) groups: u32,
    /// Members per group, hosted together on each group's reader worker.
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u32).range(1..=32))]
    pub(in crate::bench::timed) group_members: u32,
    /// Outstanding individual acknowledgments per source; one is serial.
    #[arg(long, default_value_t = 1024, value_parser = clap::value_parser!(u32).range(1..=65536))]
    pub(in crate::bench::timed) ack_window: u32,
    /// Per-partition first-wins identity window in seconds; omitted disables dedup.
    #[arg(long, requires_all = ["dedup_max_entries", "dedup_max_bytes"])]
    pub(in crate::bench::timed) dedup_window_secs: Option<u64>,
    /// Strict live plus pending identity capacity per partition.
    #[arg(long, requires = "dedup_window_secs")]
    pub(in crate::bench::timed) dedup_max_entries: Option<u64>,
    /// Strict per-partition identity-index allocation ceiling, in bytes.
    #[arg(long, requires = "dedup_window_secs")]
    pub(in crate::bench::timed) dedup_max_bytes: Option<u64>,
    /// Per-partition retention byte target in MiB; omitted keeps all history.
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    pub(in crate::bench::timed) retention_mib: Option<u64>,
    /// Shared topic partitions, independent of writer count. Default: 16.
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..=65536))]
    pub(in crate::bench::timed) partitions: Option<u32>,
    /// Ordinary blocking workers shared by every partition on the device.
    #[arg(long)]
    backend_write_threads: Option<usize>,
    /// Ordinary physical jobs running across the shared backend.
    #[arg(long)]
    backend_max_inflight: Option<usize>,
    /// Ordinary jobs retained by the shared backend, including unread results.
    #[arg(long)]
    backend_queued_jobs: Option<usize>,
    /// Ordinary retained byte budget per shared device, in MiB.
    #[arg(long)]
    backend_queued_mib: Option<u64>,
    /// Reserved progress jobs per shared device.
    #[arg(long)]
    backend_progress_jobs: Option<usize>,
    /// Reserved progress byte budget per shared device, in MiB.
    #[arg(long)]
    backend_progress_mib: Option<u64>,
    /// Open file handles per shared device.
    #[arg(long)]
    backend_open_handles: Option<usize>,
    /// APPEND queue slots per application shard.
    #[arg(long)]
    shard_append_slots: Option<usize>,
    /// Retained APPEND byte budget per application shard, in MiB.
    #[arg(long)]
    shard_resident_mib: Option<u64>,
    /// Separate control queue slots per application shard.
    #[arg(long)]
    shard_control_slots: Option<usize>,
    /// Separate control byte budget per application shard, in KiB.
    #[arg(long)]
    shard_control_kib: Option<u64>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            feeds: ozzy_bench::dedup::Feeds::default(),
            groups: 0,
            group_members: 1,
            ack_window: 1024,
            retention_mib: None,
            partitions: None,
            backend_write_threads: None,
            backend_max_inflight: None,
            backend_queued_jobs: None,
            backend_queued_mib: None,
            backend_progress_jobs: None,
            backend_progress_mib: None,
            backend_open_handles: None,
            shard_append_slots: None,
            shard_resident_mib: None,
            shard_control_slots: None,
            shard_control_kib: None,
            dedup_window_secs: None,
            dedup_max_entries: None,
            dedup_max_bytes: None,
        }
    }
}

impl Options {
    pub(super) fn retention_bytes(&self) -> Result<Option<u64>> {
        self.retention_mib
            .map(|mib| scaled(mib, 1024 * 1024))
            .transpose()
    }

    pub(super) fn deduplication(&self) -> Result<Option<ozzy_config::TopicDeduplication>> {
        self.dedup_window_secs
            .map(|seconds| {
                Ok(ozzy_config::TopicDeduplication {
                    window_millis: seconds
                        .checked_mul(1000)
                        .ok_or_else(|| error("dedup window overflow"))?,
                    max_entries: self
                        .dedup_max_entries
                        .ok_or_else(|| error("dedup entry limit required"))?,
                    max_bytes: self
                        .dedup_max_bytes
                        .ok_or_else(|| error("dedup byte limit required"))?,
                })
            })
            .transpose()
    }

    /// Command-line flags that reproduce these options in a worker process.
    pub(in crate::bench) fn append(&self, arguments: &mut Vec<String>) {
        self.feeds.append(arguments);
        let groups = (self.groups != 0).then_some(self.groups);
        let members = (self.group_members != 1).then_some(self.group_members);
        let ack_window = (self.ack_window != 1024).then_some(self.ack_window);
        push_flag(arguments, "--groups", groups);
        push_flag(arguments, "--group-members", members);
        push_flag(arguments, "--ack-window", ack_window);
        push_flag(arguments, "--dedup-window-secs", self.dedup_window_secs);
        push_flag(arguments, "--dedup-max-entries", self.dedup_max_entries);
        push_flag(arguments, "--dedup-max-bytes", self.dedup_max_bytes);
        push_flag(arguments, "--retention-mib", self.retention_mib);
        push_flag(arguments, "--partitions", self.partitions);
        push_flag(
            arguments,
            "--backend-write-threads",
            self.backend_write_threads,
        );
        push_flag(
            arguments,
            "--backend-max-inflight",
            self.backend_max_inflight,
        );
        push_flag(arguments, "--backend-queued-jobs", self.backend_queued_jobs);
        push_flag(arguments, "--backend-queued-mib", self.backend_queued_mib);
        push_flag(
            arguments,
            "--backend-progress-jobs",
            self.backend_progress_jobs,
        );
        push_flag(
            arguments,
            "--backend-progress-mib",
            self.backend_progress_mib,
        );
        push_flag(
            arguments,
            "--backend-open-handles",
            self.backend_open_handles,
        );
        push_flag(arguments, "--shard-append-slots", self.shard_append_slots);
        push_flag(arguments, "--shard-resident-mib", self.shard_resident_mib);
        push_flag(arguments, "--shard-control-slots", self.shard_control_slots);
        push_flag(arguments, "--shard-control-kib", self.shard_control_kib);
    }

    pub(in crate::bench::timed) fn is_default(&self) -> bool {
        !self.feeds.enabled()
            && self.groups == 0
            && self.group_members == 1
            && self.ack_window == 1024
            && self.dedup_window_secs.is_none()
            && self.dedup_max_entries.is_none()
            && self.dedup_max_bytes.is_none()
            && self.partitions.is_none()
            && self.backend_write_threads.is_none()
            && self.backend_max_inflight.is_none()
            && self.backend_queued_jobs.is_none()
            && self.backend_queued_mib.is_none()
            && self.backend_progress_jobs.is_none()
            && self.backend_progress_mib.is_none()
            && self.backend_open_handles.is_none()
            && self.shard_append_slots.is_none()
            && self.shard_resident_mib.is_none()
            && self.shard_control_slots.is_none()
            && self.shard_control_kib.is_none()
    }

    /// Shared device workers: defaults with any backend overrides.
    pub(super) fn storage_workers(&self, args: &Args) -> Result<StorageWorkers> {
        let defaults = StorageWorkers::default();
        Ok(StorageWorkers {
            backend: match args.io_backend {
                IoBackend::Aio => ozzy_config::IoBackend::Aio,
                IoBackend::Pool => ozzy_config::IoBackend::Pool,
            },
            aio_depth: args.aio_depth,
            write_threads: self.backend_write_threads.unwrap_or(defaults.write_threads),
            max_inflight: self.backend_max_inflight.unwrap_or(defaults.max_inflight),
            queued_jobs: self.backend_queued_jobs.unwrap_or(defaults.queued_jobs),
            queued_bytes: scale_override(
                self.backend_queued_mib,
                1024 * 1024,
                defaults.queued_bytes,
            )?,
            progress_jobs: self.backend_progress_jobs.unwrap_or(defaults.progress_jobs),
            progress_bytes: scale_override(
                self.backend_progress_mib,
                1024 * 1024,
                defaults.progress_bytes,
            )?,
            open_handles: self.backend_open_handles.unwrap_or(defaults.open_handles),
            ..defaults
        })
    }

    /// Per-shard queue budget: defaults with any shard overrides.
    pub(super) fn queue_budget(&self) -> Result<QueueBudget> {
        let budget = QueueBudget::default();
        Ok(QueueBudget {
            append_slots: self.shard_append_slots.unwrap_or(budget.append_slots),
            resident_bytes: scale_override(
                self.shard_resident_mib,
                1024 * 1024,
                budget.resident_bytes,
            )?,
            control_slots: self.shard_control_slots.unwrap_or(budget.control_slots),
            control_bytes: scale_override(self.shard_control_kib, 1024, budget.control_bytes)?,
        })
    }
}

/// Append `flag value` when the option is set.
fn push_flag(arguments: &mut Vec<String>, flag: &str, value: Option<impl ToString>) {
    if let Some(value) = value {
        arguments.extend([flag.into(), value.to_string()]);
    }
}

fn scale_override(value: Option<u64>, unit: u64, default: u64) -> Result<u64> {
    value.map_or(Ok(default), |value| scaled(value, unit))
}
