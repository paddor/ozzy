//! Timed workload configuration: validated arguments, the retained history
//! budget, and the run sizes derived from them.

use ozzy_journal::operation::OperationLimits;
use ozzy_proto::{GroupId, MessageId};

use super::super::{Args, Result, System, error};
use ozzy_runtime::replicated::Policy;

mod limits;
mod memory;
#[cfg(test)]
mod tests;
mod validate;

// Default for record-count-only SDK batch experiments.
#[cfg(test)]
pub(super) const SDK_BATCH_TARGET_BYTES: usize = ozzy_bench::native::DEFAULT_SDK_BATCH_TARGET_BYTES;

pub(super) fn sdk_batch_target(args: &Args) -> usize {
    (args.writer_batch_target_kib as usize * 1024).max(args.record_bytes)
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
    let capacity = if native_disk {
        ozzy_bench::native::segment_body_capacity(
            args.segment_mib * 1024 * 1024,
            args.segment_decoded_mib * 1024 * 1024,
        )
    } else {
        usize::MAX
    };
    ozzy_bench::native::AppendBudget {
        writers,
        records,
        record_bytes: args.record_bytes,
        target_bytes: sdk_batch_target(args),
        metadata: ozzy_bench::native::AppendMetadata {
            resolved: args.native.dedup_window_secs.is_some(),
            scheduled: args.native.delay_millis.is_some(),
        },
    }
    .bounds(capacity)
}

impl System {
    pub(in super::super) fn name(self) -> &'static str {
        match self {
            Self::SingleDurable => "single-durable",
            Self::DiskQuorum => "disk-quorum",
        }
    }
    pub(in super::super) fn brokers(self) -> usize {
        if self == Self::DiskQuorum { 3 } else { 1 }
    }
    pub(in super::super) fn policy(self) -> Policy {
        match self {
            Self::SingleDurable => Policy::LocalDurable,
            Self::DiskQuorum => Policy::QuorumDurable,
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
        // Owners, one reader per shard, the writer pool and maintenance.
        let dedicated_disk_workers = args
            .app_threads
            .checked_add(args.disk_owner_threads)
            .and_then(|workers| workers.checked_add(2 + 1))
            .ok_or_else(|| error("disk thread budget overflow"))?;
        if !production && args.disk_workers == 0 {
            args.disk_workers = if native_disk {
                dedicated_disk_workers
            } else {
                1
            };
        }
        let writers = args.window.unwrap_or(4);
        let workers = args.producer_workers.unwrap_or(1);
        let partitions = partition_count(&args, production)?;
        validate::reader_groups(&mut args)?;
        let reader_slots = partitions
            .checked_mul(args.readers_per_partition)
            .ok_or_else(|| error("reader subscription count overflow"))?;
        if production || external(&args) {
            args.reader_workers.get_or_insert(reader_slots.min(4));
        }
        if production {
            super::native::Settings::check_overrides(&args)?;
        }
        let sizing = validate::Sizing {
            production,
            native_disk,
            writers,
            workers,
            partitions,
            reader_slots,
            dedicated_disk_workers,
        };
        if !validate::workload_valid(&args, &sizing) {
            return Err(error(
                "invalid timed native workload: one-part records and bounded time/history required",
            ));
        }
        let history = History::new(&args, native_disk, writers)?;
        validate::feeds(&args, &sizing)?;
        validate::delay(&args, &sizing)?;
        if production && !validate::sdk_owner_counts_valid(&args, &sizing) {
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
                .ok_or_else(|| error("production config lacks native settings"))?
                .install_clients(clients, payload_pools)?;
        }
        Ok(config)
    }

    /// How long brokers hold each record, in nanoseconds; zero stores at once.
    pub(super) fn delay_ns(&self) -> u64 {
        self.args.native.delay_millis.unwrap_or(0) * 1_000_000
    }

    pub(super) fn logical_writers(&self) -> usize {
        self.args.native.feeds.logical_writers(self.writers)
    }

    pub(super) fn logical_lane(&self, writer: usize) -> usize {
        self.args.native.feeds.logical_lane(writer)
    }

    pub(super) fn feed_partition(&self, lane: usize, partitions: usize) -> usize {
        self.args
            .native
            .feeds
            .partition(lane, self.writers, partitions)
    }
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
