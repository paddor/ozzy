//! Benchmark inputs translated to the production deployment schema.

use crate::bench::{Args, IoBackend, Result, error};
use ozzy_config::{DeploymentMode, Endpoints, QueueBudget, StorageWorkers};
use serde_json::{Value, json};
use std::path::PathBuf;

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
    fn retention_bytes(&self) -> Result<Option<u64>> {
        self.retention_mib
            .map(|mib| scaled(mib, 1024 * 1024))
            .transpose()
    }

    fn deduplication(&self) -> Result<Option<ozzy_config::TopicDeduplication>> {
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
    fn storage_workers(&self, args: &Args) -> Result<StorageWorkers> {
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
    fn queue_budget(&self) -> Result<QueueBudget> {
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

#[derive(Debug, Clone)]
pub(in crate::bench::timed) struct Settings {
    deduplication: Option<ozzy_config::TopicDeduplication>,
    retention_bytes: Option<u64>,
    pub(in crate::bench::timed) partitions: usize,
    mode: DeploymentMode,
    segment_bytes: u64,
    decoded_segment_bytes: u64,
    append_bytes: u64,
    /// Record payload bytes one PEER-framed APPEND of `append_bytes` carries.
    append_payload_bytes: usize,
    io_threads: usize,
    shards: usize,
    workers: StorageWorkers,
    budget: QueueBudget,
    balanced: bool,
    pub(in crate::bench::timed) clients: Option<super::Clients>,
    pub(in crate::bench::timed) reservation_bytes: u64,
}

/// Fixed, connectable addresses and an absent root chosen before initialization.
pub(super) struct Broker {
    pub(super) name: String,
    pub(super) endpoints: Endpoints,
    pub(super) root: PathBuf,
}

impl Settings {
    pub(in crate::bench::timed) fn check_overrides(args: &Args) -> Result<()> {
        let unsupported = [
            (args.disk_workers != 0, "--disk-workers"),
            (args.disk_owner_threads != 1, "--disk-owner-threads"),
            (args.segment_decoded_mib != 64, "--segment-decoded-mib"),
            (args.read_depth != 8, "--read-depth"),
            (args.broker_hwm != 8192, "--broker-hwm"),
            (
                args.storage_lane_capacity != 1024,
                "--storage-lane-capacity",
            ),
            (
                args.storage_group_records != 4096,
                "--storage-group-records",
            ),
            (args.operation_target_kib != 4096, "--operation-target-kib"),
            (
                args.write_group_target_kib != 4096,
                "--write-group-target-kib",
            ),
            (args.resident_read_mib.is_some(), "--resident-read-mib"),
            (args.packing_block_kib != 64, "--packing-block-kib"),
            (args.storage_group_kib.is_some(), "--storage-group-kib"),
            (args.history_operations != 131_072, "--history-operations"),
            (args.history_mib != 512, "--history-mib"),
            (args.writer_linger_us != 0, "--writer-linger-us"),
            (
                args.io_threads != 1 && args.worker_index.is_none(),
                "--io-threads",
            ),
        ];
        if let Some((_, flag)) = unsupported.into_iter().find(|(changed, _)| *changed) {
            return Err(error(format!(
                "production benchmark does not implement {flag}; override refused before initialization"
            )));
        }
        if args.direct_io != (args.io_backend == IoBackend::Aio) {
            return Err(error(
                "production AIO uses direct writes; pool uses buffered writes; --direct-io must match --io-backend",
            ));
        }
        if args.io_backend == IoBackend::Pool && args.aio_depth != 1 {
            return Err(error("--aio-depth requires the AIO backend"));
        }
        Ok(())
    }

    pub(in crate::bench::timed) fn new(
        args: &Args,
        operation: ozzy_journal::operation::OperationLimits,
    ) -> Result<Self> {
        let options = &args.native;
        let segment_bytes = scaled(args.segment_mib as u64, 1024 * 1024)?;
        let decoded_segment_bytes = scaled(args.segment_decoded_mib as u64, 1024 * 1024)?;
        let deduplication = options.deduplication()?;
        let append_bytes = append_bytes(options, operation, segment_bytes)?;
        // Bounded by `MAX_APPEND_BYTES`, so it fits every target `usize`.
        let append_payload_bytes = ozzy_bench::native::peer_payload_capacity(
            append_bytes as usize,
            deduplication.is_some(),
        )?;
        let settings = Self {
            deduplication,
            retention_bytes: options.retention_bytes()?,
            clients: None,
            reservation_bytes: 0,
            partitions: options.partitions.unwrap_or(16) as usize,
            mode: if args.system.brokers() == 1 {
                DeploymentMode::Single
            } else {
                DeploymentMode::Three
            },
            segment_bytes,
            decoded_segment_bytes,
            append_bytes,
            append_payload_bytes,
            io_threads: args
                .broker_io_threads
                .map_or(args.io_threads, |count| count as usize),
            shards: args.app_threads,
            workers: options.storage_workers(args)?,
            budget: options.queue_budget()?,
            balanced: args.balanced_partitions,
        };
        // Validate the same schema before choosing endpoints or touching files.
        settings.document(&preflight_brokers(args.system.brokers()))?;
        Ok(settings)
    }

    pub(in crate::bench::timed) fn install_clients(
        &mut self,
        clients: super::Clients,
        payload_pools: u64,
    ) -> Result<()> {
        let overflow = || error("native workload memory reservation overflow");
        let shards = (self.shards as u64)
            .checked_mul(
                self.budget
                    .resident_bytes
                    .checked_add(self.budget.control_bytes)
                    .ok_or_else(overflow)?,
            )
            .ok_or_else(overflow)?;
        let backend = self
            .workers
            .queued_bytes
            .checked_add(self.workers.progress_bytes)
            .ok_or_else(overflow)?;
        // Physical segment capacity consumes SSD space. Decoded segment state is
        // the bounded RAM allocation, independent of file capacity.
        let partitions = (self.partitions as u64)
            .checked_mul(
                self.decoded_segment_bytes
                    .checked_mul(2)
                    .and_then(|bytes| bytes.checked_add(self.append_bytes.checked_mul(16)?))
                    .ok_or_else(overflow)?,
            )
            .ok_or_else(overflow)?;
        let dedup = self.deduplication.map_or(Ok(0), |policy| {
            policy
                .max_bytes
                .checked_mul(self.partitions as u64)
                .ok_or_else(overflow)
        })?;
        let broker = shards
            .checked_add(dedup)
            .ok_or_else(overflow)?
            .checked_add(backend)
            .and_then(|bytes| bytes.checked_add(partitions))
            .ok_or_else(overflow)?;
        let brokers = if self.mode == DeploymentMode::Single {
            1
        } else {
            3
        };
        self.reservation_bytes = broker
            .checked_mul(brokers)
            .and_then(|bytes| bytes.checked_add(clients.bytes))
            .and_then(|bytes| bytes.checked_add(payload_pools))
            .and_then(|bytes| bytes.checked_add(1024 * 1024 * 1024))
            .ok_or_else(overflow)?;
        self.clients = Some(clients);
        Ok(())
    }

    pub(in crate::bench::timed) fn append_payload_bytes(&self) -> usize {
        self.append_payload_bytes
    }

    pub(super) fn document(&self, brokers: &[Broker]) -> Result<String> {
        let mut deployment = json!({
            "cluster": {"mode": self.mode},
            "limits": {"max_topics": 1, "max_partitions": self.partitions},
            "topics": {"benchmark": {
                "partitions": self.partitions,
                "partitioner_seed": 0, "segment_bytes": self.segment_bytes,
                "max_append_bytes": self.append_bytes,
            }},
            "brokers": {},
        });
        if let Some(bytes) = self.retention_bytes {
            deployment["topics"]["benchmark"]["retention"] = json!({"max_bytes": bytes});
        }
        if let Some(policy) = self.deduplication {
            deployment["topics"]["benchmark"]["deduplication"] = json!({
                "window_millis": policy.window_millis, "max_entries": policy.max_entries,
                "max_bytes": policy.max_bytes,
            });
        }
        for broker in brokers {
            if deployment["brokers"].get(&broker.name).is_some() {
                return Err(error("duplicate native benchmark broker name"));
            }
            let mut endpoints = json!({"peer": broker.endpoints.peer, "data_peer": broker.endpoints.data_peer, "reader_pub": broker.endpoints.reader_pub});
            if let Some(follower) = &broker.endpoints.follower_pub {
                endpoints["follower_pub"] = json!(follower);
            }
            let shards = (0..self.shards)
                .map(|id| json!({"id": id, "device": "storage", "budget": {
                    "append_slots": self.budget.append_slots, "resident_bytes": self.budget.resident_bytes,
                    "control_slots": self.budget.control_slots, "control_bytes": self.budget.control_bytes,
                }}))
                .collect::<Vec<_>>();
            let mut topology = json!({"omq": {"io_threads": self.io_threads}, "shards": shards});
            if self.balanced {
                topology["partitions"] = json!((0..self.partitions)
                    .map(|partition| json!({"topic": "benchmark", "partition": partition, "shard": partition % self.shards}))
                    .collect::<Vec<_>>());
            }
            deployment["brokers"][&broker.name] = json!({
                "endpoints": endpoints,
                "devices": {"storage": {"root": broker.root, "controller": "storage", "workers": self.worker_settings()}},
                "topology": topology,
            });
        }
        let source = toml::to_string_pretty(&deployment)?;
        let checked = ozzy_config::Deployment::parse(&source)?.validate()?;
        // Temporary identities validate the real journal/actor construction
        // bounds. They are never persisted or passed to startup.
        let identity = checked.initialize(uuid::Uuid::now_v7)?;
        let host = ozzy_broker::host_resources()?;
        for broker in brokers {
            let local =
                checked.initialize_broker_identity(&identity, &broker.name, uuid::Uuid::now_v7)?;
            let config = ozzy_broker::CheckedConfig {
                deployment: checked.clone(),
                identity: identity.clone(),
                plan: checked.broker_plan(&broker.name, &host)?,
            };
            ozzy_broker::JournalPlan::from_trusted_deployment(&config, &local)?;
        }
        Ok(source)
    }

    fn worker_settings(&self) -> Value {
        let workers = &self.workers;
        json!({
            "backend": match workers.backend {ozzy_config::IoBackend::Aio => "aio", ozzy_config::IoBackend::Pool => "pool"},
            "write_threads": workers.write_threads, "aio_depth": workers.aio_depth,
            "max_inflight": workers.max_inflight, "queued_jobs": workers.queued_jobs,
            "queued_bytes": workers.queued_bytes, "progress_jobs": workers.progress_jobs,
            "progress_bytes": workers.progress_bytes, "open_handles": workers.open_handles,
        })
    }
}

/// The topic APPEND bound. The production HELLO reserves metadata for its
/// maximum record count; a small SDK group still needs its full payload to fit.
fn append_bytes(
    options: &Options,
    operation: ozzy_journal::operation::OperationLimits,
    segment_bytes: u64,
) -> Result<u64> {
    let metadata = ozzy_journal::operation::append_metadata_bound(
        1,
        ozzy_runtime::replicated::MAX_APPEND_RECORDS,
        options.dedup_window_secs.is_some(),
    )
    .ok_or("APPEND metadata overflow")?;
    let body = operation
        .max_body_bytes
        .max(metadata + operation.max_payload_bytes)
        .max(1024);
    Ok(u64::try_from(body)?
        .min(ozzy_config::MAX_APPEND_BYTES)
        .min(segment_bytes / 2))
}

/// Placeholder addresses and roots that only validate the schema; startup
/// never uses them.
fn preflight_brokers(count: usize) -> Vec<Broker> {
    (0..count)
        .map(|index| Broker {
            name: format!("broker-{index}"),
            endpoints: Endpoints {
                peer: format!("tcp://127.0.0.1:{}", 20000 + index * 3),
                reader_pub: format!("tcp://127.0.0.1:{}", 20001 + index * 3),
                data_peer: format!("tcp://127.0.0.1:{}", 20002 + index * 3),
                follower_pub: None,
                admin: None,
            },
            root: PathBuf::from(format!("/native-preflight/broker-{index}")),
        })
        .collect()
}

/// Append `flag value` when the option is set.
fn push_flag(arguments: &mut Vec<String>, flag: &str, value: Option<impl ToString>) {
    if let Some(value) = value {
        arguments.extend([flag.into(), value.to_string()]);
    }
}

fn scaled(value: u64, unit: u64) -> Result<u64> {
    value
        .checked_mul(unit)
        .ok_or_else(|| error("native deployment byte budget overflow"))
}

fn scale_override(value: Option<u64>, unit: u64, default: u64) -> Result<u64> {
    value.map_or(Ok(default), |value| scaled(value, unit))
}

#[cfg(test)]
mod tests;
