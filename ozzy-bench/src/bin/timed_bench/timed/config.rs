use ozzy_journal::operation::OperationLimits;
use ozzy_proto::{EnvelopeLimits, GroupId, MessageId, append::DataLimits};

use super::super::{Args, Result, System, error};
use ozzy_runtime::replicated::Policy;

// Fixed across SDK cap experiments. Broker receive credit must not serialize
// full APPENDs or change with the writer setting being measured.
pub(super) const SDK_BATCH_TARGET_BYTES: usize = 832 * 1024;

pub(super) fn sdk_batch_target(record_bytes: usize) -> usize {
    SDK_BATCH_TARGET_BYTES.max(record_bytes)
}
// A disk-quorum follower overlaps its next write with the previous barrier only
// while the live window holds many operations. One storage group holds two
// full SDK batches, which serializes every window behind its own barriers.
pub(super) const DISK_QUORUM_WINDOW_BYTES: usize = 32 * 1024 * 1024;

fn native_disk(args: &Args) -> bool {
    !external(args)
}

fn external(args: &Args) -> bool {
    #[cfg(feature = "comparisons")]
    {
        args.external_system.is_some()
    }
    #[cfg(not(feature = "comparisons"))]
    {
        let _ = args;
        false
    }
}

fn partition_count(args: &Args, production: bool) -> Result<usize> {
    if production || external(args) {
        if external(args) {
            let mut overrides = args.native.clone();
            overrides.partitions = None;
            if !overrides.is_default() {
                return Err(error(
                    "external comparisons cannot configure native resources",
                ));
            }
        }
        Ok(args.native.partitions.unwrap_or(16) as usize)
    } else if args.native.is_default() {
        Ok(args.window.unwrap_or(4))
    } else {
        Err(error(
            "production deployment settings require the production coordinator",
        ))
    }
}

/// Bound byte-targeted SDK groups so raw plus strictly smaller LZ4 fits one
/// indivisible canonical operation and an empty disk segment.
fn prepared_operation_bounds(
    args: &Args,
    native_disk: bool,
    writers: usize,
    records: usize,
) -> Result<(usize, usize)> {
    let requested = (records * args.record_bytes).min(sdk_batch_target(args.record_bytes));
    let fixed = 4 + 76 * writers + records * 24 + 9;
    let body_limit = if native_disk {
        let capacity = args.segment_mib * 1024 * 1024;
        let usable = capacity.saturating_sub(ozzy_journal_segment::SEGMENT_HEADER_BYTES);
        let aligned = usable / ozzy_journal_segment::WRITE_GROUP_ALIGNMENT
            * ozzy_journal_segment::WRITE_GROUP_ALIGNMENT;
        aligned
            .saturating_sub(
                ozzy_journal_segment::ENTRY_HEADER_BYTES + ozzy_journal_segment::GROUP_SEAL_BYTES,
            )
            .min(args.segment_decoded_mib * 1024 * 1024)
    } else {
        usize::MAX
    };
    let payload = requested.min(body_limit.saturating_sub(fixed) / 2);
    if payload < args.record_bytes {
        return Err(error("segment cannot hold one prepared record operation"));
    }
    Ok((fixed, payload))
}

impl System {
    pub(in super::super) fn name(self) -> &'static str {
        match self {
            Self::SingleDurable => "single-durable",
            Self::DiskQuorum => "disk-quorum",
            Self::ReplicatedPersisting => "replicated-persisting",
        }
    }
    pub(in super::super) fn brokers(self) -> usize {
        if matches!(self, Self::DiskQuorum | Self::ReplicatedPersisting) {
            3
        } else {
            1
        }
    }
    pub(in super::super) fn policy(self) -> Policy {
        match self {
            Self::SingleDurable => Policy::LocalDurable,
            Self::DiskQuorum => Policy::QuorumDurable,
            Self::ReplicatedPersisting => Policy::QuorumReplicatedPersisting,
        }
    }
}

#[derive(Clone)]
pub(super) struct Config {
    pub args: Args,
    pub writers: usize,
    pub workers: usize,
    pub history: History,
    pub native: Option<super::native::Settings>,
}

/// Retained history budget per broker. Exhaustion invalidates a run; it never
/// ends or throttles one.
#[derive(Debug, Clone, Copy)]
pub(super) struct History {
    pub max_operations: usize,
    pub operation: OperationLimits,
}

impl History {
    fn new(args: &Args, native_disk: bool, writers: usize) -> Result<Self> {
        let (fixed_body_bytes, payload_bytes) =
            prepared_operation_bounds(args, native_disk, writers, args.request_records)?;
        // Prepared LZ4 retains its exact block beside the raw canonical view.
        Ok(Self {
            max_operations: args.history_operations,
            operation: OperationLimits {
                max_body_bytes: fixed_body_bytes + 2 * payload_bytes,
                max_append_batches: writers,
                max_records: args.request_records,
                max_parts: args.request_records,
                max_payload_bytes: payload_bytes,
                ..OperationLimits::default()
            },
        })
    }
}

fn storage_targets_valid(args: &Args) -> bool {
    let default = ozzy_runtime::replica_journal::WritePipelineConfig::default()
        .backlog
        .max_body_bytes
        >> 20;
    if args.replication_cache_mib.unwrap_or(default)
        < args.persistence_backlog_mib.unwrap_or(default)
    {
        return false;
    }
    if [args.persistence_backlog_mib, args.replication_cache_mib]
        .into_iter()
        .flatten()
        .any(|mib| {
            !(64..=1024).contains(&mib)
                || args.system != System::ReplicatedPersisting
                || !native_disk(args)
        })
    {
        return false;
    }
    if args
        .resident_read_mib
        .is_some_and(|mib| !(16..=4096).contains(&mib) || !native_disk(args))
    {
        return false;
    }
    args.write_call_kib.is_none_or(|kib| {
        (16..=4096).contains(&kib)
            && args.system == System::ReplicatedPersisting
            && native_disk(args)
    }) && [args.operation_target_kib, args.write_group_target_kib]
        .into_iter()
        .all(|kib| {
            (64..=8192).contains(&kib)
                && (kib == 4096 || (native_disk(args) && args.system.brokers() == 1))
        })
}

impl Config {
    #[cfg(any(feature = "comparisons", test))]
    pub(super) fn new(mut args: Args) -> Result<Self> {
        #[cfg(feature = "comparisons")]
        if args.external_system.is_some() {
            args.system = args
                .external_policy
                .ok_or_else(|| error("missing external confirmation policy"))?
                .configuration_system();
        }
        #[cfg(not(feature = "comparisons"))]
        let _ = &mut args;
        Self::build(args, false)
    }

    pub(super) fn production(args: Args) -> Result<Self> {
        Self::build(args, true)
    }

    fn build(mut args: Args, production: bool) -> Result<Self> {
        let native_disk = native_disk(&args);
        let external = external(&args);
        // Owners, one reader per shard, the writer pool and maintenance.
        let dedicated = args
            .app_threads
            .checked_add(args.disk_owner_threads)
            .and_then(|workers| workers.checked_add(2 + 1))
            .ok_or_else(|| error("disk thread budget overflow"))?;
        if !production && args.disk_workers == 0 {
            args.disk_workers = if native_disk { dedicated } else { 1 };
        }
        let writers = args.window.unwrap_or(4);
        let workers = args.producer_workers.unwrap_or(1);
        let partitions = partition_count(&args, production)?;
        let reader_slots = partitions
            .checked_mul(args.readers_per_partition)
            .ok_or_else(|| error("reader subscription count overflow"))?;
        if production || external {
            args.reader_workers.get_or_insert(reader_slots.min(4));
        }
        if production {
            super::native::Settings::check_overrides(&args)?;
        }
        let disk_workers_valid = args.disk_workers == if native_disk { dedicated } else { 1 };
        if !args.network_ingress
            || args
                .duration
                .is_none_or(|d| !d.is_finite() || !(0.05..=120.0).contains(&d))
            || !args.warmup.is_finite()
            || !(0.0..=10.0).contains(&args.warmup)
            || !(1..=32).contains(&writers)
            || !(1..=writers).contains(&workers)
            || !(1..=32).contains(&args.readers_per_partition)
            || (!production && args.live_readers && args.system.brokers() != 3)
            // One broker group admits at most 32 preprovisioned readers.
            || (!production && reader_slots > 32)
            || !(1..=reader_slots.min(if production { 32 } else { usize::MAX })).contains(&args.reader_workers())
            || !(1..=if production { 32 } else { writers }).contains(&args.app_threads)
            || (!production && args.system.brokers() == 3 && args.app_threads != 1)
            || !args.streaming
            || !(1..=65536).contains(&args.request_records)
            || !(1..=65536).contains(&args.reader_records)
            || !(1..=128).contains(&args.reader_payload_mib)
            || !(1..=65536).contains(&args.writer_inflight_appends)
            || !(args.record_bytes == 16 || (32..=16384).contains(&args.record_bytes))
            || (args.binary_payload && args.record_bytes != 16)
            || (args.json_payload && args.record_bytes < 32)
            || !(4..=65536).contains(&args.history_mib)
            || !(4..=4096).contains(&args.segment_mib)
            || !(8..=1024).contains(&args.segment_decoded_mib)
            || (!production && !storage_targets_valid(&args))
            || !(1..=args.app_threads).contains(&args.disk_owner_threads)
            || (!native_disk && args.disk_owner_threads != 1)
            || (!production && !disk_workers_valid)
            || !(1..=1024).contains(&args.read_depth)
            || !(1..=65536).contains(&args.broker_hwm)
            || !(1..=65536).contains(&args.storage_lane_capacity)
            || (args.storage_lane_capacity != 1024 && args.system.brokers() != 1)
            || !(1..=65536).contains(&args.storage_group_records)
            || !(4..=1024).contains(&args.packing_block_kib)
            || !args.packing_block_kib.is_power_of_two()
            || args.storage_group_kib.is_some_and(|kib| {
                kib > 64 * 1024
                    || kib.saturating_mul(1024) < args.request_records * args.record_bytes
            })
            || (!production && args.history_mib / args.app_threads < args.segment_mib)
            || !(2 * writers + 1..=1_000_000).contains(&args.history_operations)
            || !(1..=120).contains(&args.drain_timeout_secs)
        {
            return Err(error(
                "invalid timed native workload: one-part records and bounded time/history required",
            ));
        }
        let history = History::new(&args, native_disk, writers)?;
        if production && !sdk_owner_counts_valid(&args, writers, workers, partitions, reader_slots)
        {
            return Err(error(
                "native SDK owner count/request bounds exceeded before initialization",
            ));
        }
        Self::assemble(args, writers, workers, history, production)
    }

    fn assemble(
        args: Args,
        writers: usize,
        workers: usize,
        history: History,
        production: bool,
    ) -> Result<Self> {
        let native = production
            .then(|| super::native::Settings::new(&args, history.operation))
            .transpose()?;
        let mut config = Self {
            args,
            writers,
            workers,
            history,
            native,
        };
        if production {
            let clients = super::native::Clients::new(&config)?;
            let payload_pools = super::producer::payload_pool_bytes(&config)
                .checked_mul(config.writers as u64)
                .ok_or_else(|| error("native workload payload pool overflow"))?;
            config
                .native
                .as_mut()
                .expect("production settings")
                .install_clients(clients, payload_pools)?;
        }
        Ok(config)
    }

    pub(super) fn preflight_memory(&self) -> Result<()> {
        let mem = std::fs::read_to_string("/proc/meminfo")?;
        let available: u64 = mem
            .lines()
            .find_map(|line| line.strip_prefix("MemAvailable:"))
            .and_then(|line| line.split_whitespace().next())
            .ok_or_else(|| error("missing available memory"))?
            .parse()?;
        let required = self.memory_reservation_bytes();
        if required > available * 1024 {
            return Err(error(format!(
                "timed native workload reserves {required} bytes including bounded history, client windows and safety reserve; only {} available",
                available * 1024
            )));
        }
        Ok(())
    }

    fn memory_reservation_bytes(&self) -> u64 {
        if let Some(native) = &self.native {
            return native.reservation_bytes;
        }
        // In-flight requests plus one preparation slot per APPEND slot.
        let request_copies = 2 * self.args.writer_inflight_appends as u64;
        #[cfg(feature = "comparisons")]
        let request_copies = if self.args.external_system.is_some() {
            1
        } else {
            request_copies
        };
        let clients = self.writers as u64
            * self.args.request_records.max(1024) as u64
            * self.args.record_bytes as u64
            * 8
            * request_copies
            + self.reader_slots() as u64
                * self.reader_records() as u64
                * self.args.record_bytes as u64
                * 8
            // Every writer and reader lane builds its own body pool.
            + (self.writers + self.reader_slots()) as u64 * super::producer::payload_pool_bytes(self)
            + 1024 * 1024 * 1024;
        #[cfg(feature = "comparisons")]
        if self.args.external_system.is_some() {
            // The external broker already exists when MemAvailable is sampled.
            return clients;
        }
        let broker = {
            let owners = if self.args.system.brokers() == 3 {
                3
            } else {
                self.args.app_threads as u64
            };
            // Disk history is a file-count allowance, not a touched RAM arena.
            // Reserve active/predecessor payloads plus operation backing held by
            // admitted reads. Charge conservative descriptor/state headroom for
            // this benchmark's known one-part, fixed-size records.
            let payloads = owners
                * (2 * self.decode_limits().max_segment_decoded_body_bytes as u64
                    + self.args.read_depth as u64 * self.history.operation.max_body_bytes as u64);
            // Every replicated disk broker retains its live window in RAM.
            let backlog = if self.args.system.brokers() == 3 {
                let pipeline = self.disk_pipeline();
                owners * (3 * pipeline.max_body_bytes as u64 + pipeline.max_operations as u64 * 512)
            } else {
                0
            };
            let replay = if self.args.system.brokers() == 3 {
                let cache = self.disk_replay_cache();
                owners * (2 * cache.max_body_bytes as u64 + cache.max_operations as u64 * 1024)
            } else {
                0
            };
            // Each additional reader owns one disk and one transport arena per
            // broker. So does each published partition.
            let lanes = if self.args.live_readers {
                self.writers
            } else {
                0
            };
            let readers = owners
                * (self.reader_slots() - self.writers + lanes) as u64
                * 2
                * self.reader_limits().envelope.max_payload_bytes as u64;
            // Each broker reserves one APPEND proposal arena per writer slot.
            let proposals = if self.args.system.brokers() == 3 {
                owners
                    * (self.writers * self.writer_proposals()) as u64
                    * self.history.operation.max_body_bytes as u64
            } else {
                0
            };
            payloads
                + backlog
                + replay
                + proposals
                + readers
                + self.resident_descriptor_reservation(payloads)
                + owners * self.history.max_operations as u64 * 512
        };
        clients + broker
    }

    fn resident_descriptor_reservation(&self, bodies: u64) -> u64 {
        let payload = self.args.record_bytes as u64;
        if payload > 255 {
            return bodies / payload * 512;
        }
        // These fixed-size, single-part records use the canonical packed layout:
        // 16-byte ID + 1-byte length per record, 76 bytes per nonempty batch.
        // Resident selectors are 24 bytes/record; double for allocation headroom.
        // Charge 768 per batch for its cached operation, batch summary, shared
        // ownership, hash buckets and growing index runs. Count even one-record
        // batches; transparent SDK batching is not a minimum-size guarantee.
        let record = payload + 17;
        bodies.div_ceil(record) * 48 + bodies.div_ceil(record + 76) * 768
    }

    pub(super) fn decode_limits(&self) -> ozzy_journal_segment::DecodeLimits {
        ozzy_journal_segment::DecodeLimits {
            max_decoded_body_bytes: self.history.operation.max_body_bytes,
            max_group_decoded_body_bytes: (8 * 1024 * 1024)
                .max(self.history.operation.max_body_bytes + 1024)
                .max(self.storage_group_bytes() + 100 * self.args.storage_group_records + 1024),
            max_segment_decoded_body_bytes: self.args.segment_decoded_mib * 1024 * 1024,
            ..ozzy_journal_segment::DecodeLimits::default()
        }
    }

    pub(super) fn segment_bytes(&self) -> usize {
        self.args.segment_mib * 1024 * 1024
    }

    pub(super) fn disk_pipeline(&self) -> ozzy_replication::PipelineLimits {
        ozzy_replication::PipelineLimits {
            max_operations: if self.args.system == System::ReplicatedPersisting {
                ozzy_runtime::replica_journal::WritePipelineConfig::default()
                    .backlog
                    .max_operations
            } else {
                ozzy_runtime::replica_journal::MAX_APPEND_OPERATIONS
            },
            // Backlog is independent of one encoded group's decode limit.
            max_body_bytes: if self.args.system == System::ReplicatedPersisting {
                self.args
                    .persistence_backlog_mib
                    .map_or_else(
                        || {
                            ozzy_runtime::replica_journal::WritePipelineConfig::default()
                                .backlog
                                .max_body_bytes
                        },
                        |mib| mib * 1024 * 1024,
                    )
                    .max(self.decode_limits().max_group_decoded_body_bytes)
            } else if self.args.system == System::DiskQuorum {
                DISK_QUORUM_WINDOW_BYTES.max(self.decode_limits().max_group_decoded_body_bytes)
            } else {
                self.decode_limits().max_group_decoded_body_bytes
            },
        }
    }

    pub(super) fn disk_replay_cache(&self) -> ozzy_replication::PipelineLimits {
        let mut cache = self.disk_pipeline();
        if self.args.system == System::ReplicatedPersisting {
            // Changing the persistence backlog must not also change repair retention.
            cache.max_body_bytes = self
                .args
                .replication_cache_mib
                .map_or_else(
                    || {
                        ozzy_runtime::replica_journal::WritePipelineConfig::default()
                            .backlog
                            .max_body_bytes
                    },
                    |mib| mib * 1024 * 1024,
                )
                .max(self.decode_limits().max_group_decoded_body_bytes);
        }
        cache
    }

    pub(super) fn storage_group_bytes(&self) -> usize {
        // Leave room for descriptors within one decoded segment.
        self.args.storage_group_kib.map_or(
            (self.args.storage_group_records * self.args.record_bytes).min(
                (self.args.segment_decoded_mib * 1024 * 1024)
                    .saturating_sub(100 * self.args.storage_group_records + 1024),
            ),
            |kib| kib * 1024,
        )
    }

    /// Group proposals in flight per writer at the leader. One per in-flight
    /// writer APPEND, so no admitted APPEND waits for a proposal buffer.
    pub(super) fn writer_proposals(&self) -> usize {
        self.args
            .writer_inflight_appends
            .min(ozzy_runtime::replica_journal::MAX_APPEND_OPERATIONS / self.writers.max(1))
            .max(1)
    }

    /// Publications queued per reader before newer ones are lost and replayed.
    pub(super) const LIVE_QUEUE_MESSAGES: u32 = 64;

    /// Independent reader subscriptions per partition, shared over broker links
    /// in the production adapter.
    pub(super) fn reader_slots(&self) -> usize {
        self.partitions() * self.args.readers_per_partition
    }

    pub(super) fn partitions(&self) -> usize {
        self.native.as_ref().map_or_else(
            || {
                if external(&self.args) {
                    self.args.native.partitions.unwrap_or(16) as usize
                } else {
                    self.writers
                }
            },
            |settings| settings.partitions,
        )
    }

    pub(super) fn reader_records(&self) -> usize {
        let records = self
            .args
            .reader_records
            .min(self.args.reader_payload_mib * 1024 * 1024 / self.args.record_bytes);
        if let Some(native) = &self.native {
            return records
                .min(2048)
                .min(self.history.operation.max_records)
                .min(self.history.operation.max_payload_bytes / self.args.record_bytes)
                .min(native.append_payload_bytes() / self.args.record_bytes);
        }
        // Disk-group read arenas currently share canonical operation bounds.
        // The external comparison adapter uses this same effective poll size.
        if self.args.system.brokers() == 3 {
            records
                .min(self.history.operation.max_records)
                .min(self.disk_pipeline().max_body_bytes / self.args.record_bytes)
        } else {
            records
        }
    }

    pub(super) fn reader_limits(&self) -> DataLimits {
        self.limits(self.reader_records())
    }

    pub(super) fn writer_limits(&self) -> DataLimits {
        let mut limits = self.limits(
            (if self.args.writer_batch_records == 0 {
                self.args.request_records
            } else {
                self.args.writer_batch_records as usize
            })
            .min(self.args.request_records)
            .min(ozzy_runtime::replicated::MAX_APPEND_RECORDS)
            .min(self.history.operation.max_records),
        );
        limits.envelope.max_payload_bytes = limits
            .envelope
            .max_payload_bytes
            .min(sdk_batch_target(self.args.record_bytes))
            .min(self.history.operation.max_payload_bytes);
        limits
    }

    pub(super) fn limits(&self, records: usize) -> DataLimits {
        DataLimits {
            max_record_bytes: if self.native.is_some() {
                self.args.record_bytes
            } else {
                1024 * 1024
            },
            envelope: EnvelopeLimits {
                max_metadata_bytes: 512 + 28 * records,
                max_payload_bytes: (self.args.record_bytes * records).min(
                    self.native
                        .as_ref()
                        .map_or(usize::MAX, super::native::Settings::append_payload_bytes),
                ),
            },
            max_records: records,
            max_parts: records,
        }
    }
}

fn sdk_owner_counts_valid(
    args: &Args,
    writers: usize,
    workers: usize,
    partitions: usize,
    reader_slots: usize,
) -> bool {
    let per_owner = writers.div_ceil(workers);
    per_owner
        .checked_mul(partitions)
        .is_some_and(|count| count <= 65536)
        && per_owner
            .checked_mul(args.writer_inflight_appends)
            .is_some_and(|count| count <= 65536)
        && reader_slots.div_ceil(args.reader_workers()) <= 65536
}

pub(super) fn group(args: &Args) -> Result<GroupId> {
    Ok(GroupId::from_bytes(
        *args
            .group
            .ok_or_else(|| error("missing timed group"))?
            .as_bytes(),
    ))
}

pub(super) fn message_id(group: GroupId, lane: usize, sequence: u64) -> MessageId {
    let mut id = *group.as_bytes();
    id[7] ^= lane as u8;
    for (byte, sequence) in id[8..].iter_mut().zip(sequence.to_be_bytes()) {
        *byte ^= sequence;
    }
    MessageId::from_bytes(id)
}

pub(super) fn record_number(lane: usize, sequence: u64) -> Result<u64> {
    if sequence >= (1_u64 << 56) {
        return Err(error("benchmark sequence overflow"));
    }
    Ok((lane as u64) << 56 | sequence)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[cfg(feature = "comparisons")]
    #[test]
    fn comparison_native_commands_use_production_settings() {
        use ozzy_bench::automation::compare;

        for mode in ["durable", "disk-quorum", "replicated-persisting"] {
            let options = compare::Args::parse_from([
                "compare", "--impl", "ozzy", "--modes", mode, "--sizes", "128",
            ]);
            let case = &options.cases()[0];
            let command = options
                .command(std::path::Path::new("bench"), case, None)
                .unwrap();
            let args = Args::try_parse_from(command.into_iter().skip(3)).unwrap();
            Config::production(args).unwrap();
        }
    }

    #[cfg(feature = "comparisons")]
    #[test]
    fn native_and_external_partitions_are_independent_of_writer_count() {
        let args = Args::parse_from([
            "bench",
            "--system",
            "single-durable",
            "--processes",
            "--network-ingress",
            "--streaming",
            "--duration",
            "1",
            "--window",
            "4",
            "--history-mib",
            "512",
            "--partitions",
            "2",
        ]);
        let native = Config::production(args.clone()).unwrap();
        assert_eq!(
            (native.writers, native.partitions(), native.reader_slots()),
            (4, 2, 2)
        );
        for system in [
            super::super::external::System::Iggy,
            super::super::external::System::Redpanda,
        ] {
            let mut args = args.clone();
            args.external_system = Some(system);
            args.external_policy = Some(super::super::external::Policy::Durable);
            args.external_endpoint = Some("127.0.0.1:18090".into());
            let config = Config::new(args).unwrap();
            assert_eq!(
                (config.writers, config.partitions(), config.reader_slots()),
                (4, 2, 2)
            );
            assert_eq!(config.args.reader_workers(), 2);
            let mut default = config.args.clone();
            default.native.partitions = None;
            assert_eq!(Config::new(default).unwrap().partitions(), 16);
        }
    }

    #[test]
    fn binary_layout_requires_16_bytes_and_json_requires_room_for_its_schema() {
        for (size, flag, valid) in [
            ("16", "--binary-payload", true),
            ("128", "--json-payload", true),
            ("16", "--json-payload", false),
            ("128", "--binary-payload", false),
            ("15", "--binary-payload", false),
        ] {
            let args = Args::parse_from([
                "bench",
                "--processes",
                "--network-ingress",
                "--streaming",
                "--duration",
                "10",
                "--record-bytes",
                size,
                flag,
            ]);
            assert_eq!(Config::new(args).is_ok(), valid, "{size} {flag}");
        }
    }

    #[test]
    fn memory_reservations_separate_disk_history_from_resident_records() {
        let config = |system, history, decoded, segment| {
            Config::new(Args::parse_from([
                "bench",
                "--network-ingress",
                "--processes",
                "--streaming",
                "--duration",
                "3",
                "--system",
                system,
                "--history-mib",
                history,
                "--segment-decoded-mib",
                decoded,
                "--segment-mib",
                segment,
            ]))
            .unwrap()
        };
        for system in ["single-durable", "disk-quorum", "replicated-persisting"] {
            let small = config(system, "8192", "64", "64");
            let large = config(system, "32768", "64", "64");
            assert_eq!(
                small.memory_reservation_bytes(),
                large.memory_reservation_bytes()
            );
            assert!(
                config(system, "32768", "128", "64").memory_reservation_bytes()
                    > large.memory_reservation_bytes()
            );
            assert_eq!(
                large.memory_reservation_bytes(),
                config(system, "32768", "64", "1024").memory_reservation_bytes()
            );
        }
        #[cfg(feature = "comparisons")]
        {
            let mut external = config("single-durable", "32768", "64", "64");
            external.args.external_system = Some(super::super::external::System::Iggy);
            external.args.external_policy = Some(super::super::external::Policy::Durable);
            let before = external.memory_reservation_bytes();
            external.args.segment_decoded_mib = 1024;
            assert_eq!(external.memory_reservation_bytes(), before);
        }
    }

    #[test]
    fn packed_record_reservation_covers_sparse_batches_and_larger_segments() {
        for bytes in [16, 32, 64, 128] {
            let mut args = Args::parse_from([
                "bench",
                "--system",
                "replicated-persisting",
                "--processes",
                "--network-ingress",
                "--streaming",
                "--duration",
                "1",
                "--segment-mib",
                "256",
                "--segment-decoded-mib",
                "256",
                "--request-records",
                "2048",
            ]);
            args.record_bytes = bytes;
            let mut config = Config::new(args).unwrap();
            let count = 10_000;
            let sparse_bodies = count * (4 + 76 + 17 + bytes as u64);
            assert!(config.resident_descriptor_reservation(sparse_bodies) >= count * (768 + 48));
            assert!(config.memory_reservation_bytes() < 20 * 1024 * 1024 * 1024);
            if bytes == 16 {
                config.args.segment_decoded_mib = 1024;
                assert!(config.memory_reservation_bytes() > 24 * 1024 * 1024 * 1024);
            }
        }
    }

    #[test]
    fn default_reader_processes_are_capped_by_partitions_and_can_be_overridden() {
        for (partitions, expected) in [("1", 1), ("2", 2), ("4", 4), ("16", 4)] {
            let args = [
                "bench",
                "--system",
                "single-durable",
                "--network-ingress",
                "--processes",
                "--streaming",
                "--duration",
                "3",
                "--window",
                partitions,
            ];
            assert_eq!(
                Config::new(Args::parse_from(args))
                    .unwrap()
                    .args
                    .reader_workers(),
                expected
            );
            for (override_count, valid) in [("1", true), ("0", false), ("33", false)] {
                let config = Config::new(Args::parse_from(
                    args.into_iter().chain(["--reader-workers", override_count]),
                ));
                assert_eq!(config.is_ok(), valid);
                if let Ok(config) = config {
                    assert_eq!(config.args.reader_workers(), 1);
                }
            }
        }
    }

    #[test]
    fn record_ids_keep_run_entropy_instead_of_discarding_uuid_suffix() {
        let mut bytes = [42; 16];
        let first = GroupId::from_bytes(bytes);
        bytes[12] ^= 0x80;
        let second = GroupId::from_bytes(bytes);
        assert_ne!(message_id(first, 0, 0), message_id(second, 0, 0));
        assert_ne!(message_id(first, 0, 0), message_id(first, 1, 0));
        assert_ne!(message_id(first, 0, 0), message_id(first, 0, 1));
    }

    #[test]
    fn background_backlog_is_independent_of_physical_group_decode_limit() {
        let config = Config::new(Args::parse_from([
            "bench",
            "--system",
            "replicated-persisting",
            "--record-bytes",
            "16",
            "--processes",
            "--network-ingress",
            "--streaming",
            "--duration",
            "1",
        ]))
        .unwrap();
        assert_eq!(config.disk_pipeline().max_body_bytes, 128 * 1024 * 1024);
        assert_eq!(config.disk_pipeline().max_operations, 4096);
        let mut larger = config.clone();
        larger.args.persistence_backlog_mib = Some(256);
        assert!(Config::new(larger.args.clone()).is_err());
        larger.args.replication_cache_mib = Some(256);
        let larger = Config::new(larger.args).unwrap();
        assert_eq!(larger.disk_pipeline().max_body_bytes, 256 * 1024 * 1024);
        assert_eq!(larger.disk_pipeline().max_operations, 4096);
        assert_eq!(larger.disk_replay_cache().max_body_bytes, 256 * 1024 * 1024);
        let mut cached = larger.clone();
        cached.args.replication_cache_mib = Some(512);
        let cached = Config::new(cached.args).unwrap();
        assert_eq!(cached.disk_pipeline().max_body_bytes, 256 * 1024 * 1024);
        assert_eq!(cached.disk_replay_cache().max_body_bytes, 512 * 1024 * 1024);
        for invalid in [0, 63, 1025, usize::MAX] {
            let mut args = config.args.clone();
            args.persistence_backlog_mib = Some(invalid);
            assert!(Config::new(args).is_err());
        }
        assert_eq!(
            config.decode_limits().max_group_decoded_body_bytes,
            8 * 1024 * 1024
        );
    }

    #[test]
    fn disk_quorum_window_holds_many_full_sdk_batches_beyond_one_storage_group() {
        let config = Config::new(Args::parse_from([
            "bench",
            "--system",
            "disk-quorum",
            "--record-bytes",
            "1024",
            "--processes",
            "--network-ingress",
            "--streaming",
            "--duration",
            "1",
        ]))
        .unwrap();
        let window = config.disk_pipeline();
        // A slow follower pays barriers per window. Two batches per window made
        // saturated 1 KiB catch-up miss its deadline on a 47 ms/barrier disk.
        assert!(window.max_body_bytes >= 8 * SDK_BATCH_TARGET_BYTES);
        assert!(window.max_body_bytes > config.decode_limits().max_group_decoded_body_bytes);
        assert_eq!(
            window.max_operations,
            ozzy_runtime::replica_journal::MAX_APPEND_OPERATIONS
        );
        // Storage grouping is unchanged; only the replication window grew.
        assert_eq!(
            config.decode_limits().max_group_decoded_body_bytes,
            8 * 1024 * 1024
        );
    }

    #[test]
    fn large_sdk_queue_keeps_operation_and_reader_bytes_within_storage_bounds() {
        for bytes in [16, 128, 1024, 8192] {
            let mut args = Args::parse_from([
                "bench",
                "--system",
                "replicated-persisting",
                "--processes",
                "--network-ingress",
                "--streaming",
                "--duration",
                "1",
                "--request-records",
                "8192",
            ]);
            args.record_bytes = bytes;
            let config = Config::new(args).unwrap();
            let writer = config.writer_limits();
            assert_eq!(
                writer.max_records,
                ozzy_runtime::replicated::MAX_APPEND_RECORDS
            );
            assert_eq!(
                writer.envelope.max_payload_bytes,
                (ozzy_runtime::replicated::MAX_APPEND_RECORDS * bytes).min(sdk_batch_target(bytes))
            );
            assert!(
                config.history.operation.max_payload_bytes >= writer.envelope.max_payload_bytes
            );
            assert!(config.history.operation.max_payload_bytes <= 4 * 1024 * 1024);
            let decode = config.decode_limits();
            assert!(decode.max_decoded_body_bytes <= decode.max_group_decoded_body_bytes);
            assert!(decode.max_group_decoded_body_bytes <= decode.max_segment_decoded_body_bytes);
            assert!(
                config.reader_limits().envelope.max_payload_bytes
                    <= config.disk_pipeline().max_body_bytes
            );
        }
    }

    #[test]
    fn small_segment_respects_sdk_cap_and_physical_bound() {
        let args = Args::parse_from([
            "bench",
            "--system",
            "replicated-persisting",
            "--processes",
            "--network-ingress",
            "--streaming",
            "--duration",
            "1",
            "--window",
            "2",
            "--record-bytes",
            "8192",
            "--request-records",
            "256",
            "--writer-batch-records",
            "256",
            "--segment-mib",
            "4",
        ]);
        let config = Config::new(args).unwrap();
        let writer = config.writer_limits();
        assert_eq!(writer.envelope.max_payload_bytes, SDK_BATCH_TARGET_BYTES);
        assert!(writer.envelope.max_payload_bytes < 256 * 8192);
        let usable = 4 * 1024 * 1024 - ozzy_journal_segment::SEGMENT_HEADER_BYTES;
        let physical = usable / ozzy_journal_segment::WRITE_GROUP_ALIGNMENT
            * ozzy_journal_segment::WRITE_GROUP_ALIGNMENT
            - ozzy_journal_segment::ENTRY_HEADER_BYTES
            - ozzy_journal_segment::GROUP_SEAL_BYTES;
        assert!(config.history.operation.max_body_bytes <= physical);
    }

    #[test]
    fn reader_credit_fits_record_and_payload_bounds_independently_of_writers() {
        for (bytes, records) in [(16, 16384), (128, 16384), (1024, 16384), (8192, 16384)] {
            let mut args = Args::parse_from([
                "bench",
                "--processes",
                "--network-ingress",
                "--streaming",
                "--duration",
                "1",
            ]);
            args.record_bytes = bytes;
            args.system = System::SingleDurable;
            let config = Config::new(args.clone()).unwrap();
            assert_eq!(config.reader_records(), records);
            assert_eq!(
                config.reader_limits().envelope.max_payload_bytes,
                records * bytes
            );
            assert_eq!(config.writer_limits().max_records, 1024);
            args.reader_records = 7;
            args.reader_payload_mib = 1;
            assert_eq!(Config::new(args).unwrap().reader_records(), 7);
        }
    }
}
