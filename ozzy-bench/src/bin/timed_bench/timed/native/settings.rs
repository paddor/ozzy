//! Benchmark inputs translated to the production deployment schema.

use crate::bench::{Args, IoBackend, Result, System, error};
use ozzy_config::{Confirmation, Endpoints, QueueBudget, StorageWorkers};
use serde_json::{Value, json};
use std::path::PathBuf;

/// Separate topic size, shard admission, and shared device admission.
#[derive(Debug, Clone, Default, PartialEq, Eq, clap::Args)]
pub(in crate::bench) struct Options {
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

impl Options {
    pub(in crate::bench) fn append(&self, arguments: &mut Vec<String>) {
        let values = [
            (
                "--partitions",
                self.partitions.map(|value| value.to_string()),
            ),
            (
                "--backend-write-threads",
                self.backend_write_threads.map(|value| value.to_string()),
            ),
            (
                "--backend-max-inflight",
                self.backend_max_inflight.map(|value| value.to_string()),
            ),
            (
                "--backend-queued-jobs",
                self.backend_queued_jobs.map(|value| value.to_string()),
            ),
            (
                "--backend-queued-mib",
                self.backend_queued_mib.map(|value| value.to_string()),
            ),
            (
                "--backend-progress-jobs",
                self.backend_progress_jobs.map(|value| value.to_string()),
            ),
            (
                "--backend-progress-mib",
                self.backend_progress_mib.map(|value| value.to_string()),
            ),
            (
                "--backend-open-handles",
                self.backend_open_handles.map(|value| value.to_string()),
            ),
            (
                "--shard-append-slots",
                self.shard_append_slots.map(|value| value.to_string()),
            ),
            (
                "--shard-resident-mib",
                self.shard_resident_mib.map(|value| value.to_string()),
            ),
            (
                "--shard-control-slots",
                self.shard_control_slots.map(|value| value.to_string()),
            ),
            (
                "--shard-control-kib",
                self.shard_control_kib.map(|value| value.to_string()),
            ),
        ];
        for (flag, value) in values {
            if let Some(value) = value {
                arguments.extend([flag.into(), value]);
            }
        }
    }

    pub(in crate::bench::timed) fn is_default(&self) -> bool {
        self.partitions.is_none()
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
}

#[derive(Debug, Clone)]
pub(in crate::bench::timed) struct Settings {
    pub(in crate::bench::timed) partitions: usize,
    confirmation: Confirmation,
    segment_bytes: u64,
    decoded_segment_bytes: u64,
    append_bytes: u64,
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
            (args.write_call_kib.is_some(), "--write-call-kib"),
            (
                args.persistence_backlog_mib.is_some(),
                "--persistence-backlog-mib",
            ),
            (
                args.replication_cache_mib.is_some(),
                "--replication-cache-mib",
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
        let confirmation = match args.system {
            System::SingleDurable => Confirmation::LocalDurable,
            System::DiskQuorum => Confirmation::DiskQuorum,
            System::ReplicatedPersisting => Confirmation::ReplicatedPersisting,
        };
        let options = &args.native;
        let defaults = StorageWorkers::default();
        let budget = QueueBudget::default();
        let segment_bytes = scaled(args.segment_mib as u64, 1024 * 1024)?;
        let decoded_segment_bytes = scaled(args.segment_decoded_mib as u64, 1024 * 1024)?;
        let settings = Self {
            clients: None,
            reservation_bytes: 0,
            partitions: options.partitions.unwrap_or(16) as usize,
            confirmation,
            segment_bytes,
            decoded_segment_bytes,
            // The production HELLO reserves metadata for its maximum record
            // count. A small SDK group still needs its full payload to fit.
            append_bytes: u64::try_from(
                operation
                    .max_body_bytes
                    .max(
                        89 + 24 * ozzy_runtime::replicated::MAX_APPEND_RECORDS
                            + operation.max_payload_bytes,
                    )
                    .max(1024),
            )?
            .min(ozzy_config::MAX_APPEND_BYTES)
            .min(segment_bytes / 2),
            io_threads: args
                .broker_io_threads
                .map_or(args.io_threads, |count| count as usize),
            shards: args.app_threads,
            workers: StorageWorkers {
                backend: match args.io_backend {
                    IoBackend::Aio => ozzy_config::IoBackend::Aio,
                    IoBackend::Pool => ozzy_config::IoBackend::Pool,
                },
                aio_depth: args.aio_depth,
                write_threads: options
                    .backend_write_threads
                    .unwrap_or(defaults.write_threads),
                max_inflight: options
                    .backend_max_inflight
                    .unwrap_or(defaults.max_inflight),
                queued_jobs: options.backend_queued_jobs.unwrap_or(defaults.queued_jobs),
                queued_bytes: scale_override(
                    options.backend_queued_mib,
                    1024 * 1024,
                    defaults.queued_bytes,
                )?,
                progress_jobs: options
                    .backend_progress_jobs
                    .unwrap_or(defaults.progress_jobs),
                progress_bytes: scale_override(
                    options.backend_progress_mib,
                    1024 * 1024,
                    defaults.progress_bytes,
                )?,
                open_handles: options
                    .backend_open_handles
                    .unwrap_or(defaults.open_handles),
                ..defaults
            },
            budget: QueueBudget {
                append_slots: options.shard_append_slots.unwrap_or(budget.append_slots),
                resident_bytes: scale_override(
                    options.shard_resident_mib,
                    1024 * 1024,
                    budget.resident_bytes,
                )?,
                control_slots: options.shard_control_slots.unwrap_or(budget.control_slots),
                control_bytes: scale_override(
                    options.shard_control_kib,
                    1024,
                    budget.control_bytes,
                )?,
            },
            balanced: args.balanced_partitions,
        };
        // Validate the same schema before choosing endpoints or touching files.
        // The placeholder addresses and roots are never used for startup.
        let count = args.system.brokers();
        let brokers = (0..count)
            .map(|index| Broker {
                name: format!("broker-{index}"),
                endpoints: Endpoints {
                    peer: format!("tcp://127.0.0.1:{}", 20000 + index * 3),
                    reader_pub: format!("tcp://127.0.0.1:{}", 20001 + index * 3),
                    data_peer: format!("tcp://127.0.0.1:{}", 20002 + index * 3),
                    follower_pub: None,
                },
                root: PathBuf::from(format!("/native-preflight/broker-{index}")),
            })
            .collect::<Vec<_>>();
        settings.document(&brokers)?;
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
        let broker = shards
            .checked_add(backend)
            .and_then(|bytes| bytes.checked_add(partitions))
            .ok_or_else(overflow)?;
        let brokers = if self.confirmation == Confirmation::LocalDurable {
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
        let body = usize::try_from(self.append_bytes).expect("validated native APPEND bound");
        ozzy_bench::native::peer_payload_capacity(body)
    }

    pub(super) fn document(&self, brokers: &[Broker]) -> Result<String> {
        let mut deployment = json!({
            "cluster": {"mode": if self.confirmation == Confirmation::LocalDurable {"single"} else {"three"}},
            "limits": {"max_topics": 1, "max_partitions": self.partitions},
            "topics": {"benchmark": {
                "partitions": self.partitions, "confirmation": self.confirmation,
                "partitioner_seed": 0, "segment_bytes": self.segment_bytes,
                "max_append_bytes": self.append_bytes,
            }},
            "brokers": {},
        });
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
