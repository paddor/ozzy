//! Workload bounds a timed run must satisfy before any worker starts.

use super::super::super::{Args, Result, error};
use super::{external, native_disk};

/// The run mode and sizes `Config::build` derives before it checks the
/// workload bounds.
pub(super) struct Sizing {
    /// A production-deployment run.
    pub(super) production: bool,
    /// An Ozzy run (not an external system) with Ozzy-managed storage.
    pub(super) native_disk: bool,
    pub(super) writers: usize,
    pub(super) workers: usize,
    pub(super) partitions: usize,
    pub(super) reader_slots: usize,
    /// Disk threads a native disk run dedicates: owners, one reader per
    /// shard, the writer pool and maintenance.
    pub(super) dedicated_disk_workers: usize,
}

/// Apply `--groups`: it fixes the reader copies per partition and bounds the
/// reader workers. Member and ack windows need groups.
pub(super) fn reader_groups(args: &mut Args) -> Result<()> {
    if args.native.groups != 0 {
        if args.readers_per_partition != 1
            && args.readers_per_partition != args.native.groups as usize
        {
            return Err(error("--groups determines full-topic reader copies"));
        }
        args.readers_per_partition = args.native.groups as usize;
        if args
            .reader_workers
            .is_some_and(|n| n > args.native.groups as usize)
        {
            return Err(error("group reader workers cannot exceed groups"));
        }
        args.reader_workers
            .get_or_insert((args.native.groups as usize).min(4));
    } else if args.native.group_members != 1 || args.native.ack_window != 1024 {
        return Err(error("group member and ack windows require --groups"));
    }
    Ok(())
}

/// Whether the workload fits every timed-run bound. Each group checks one
/// concern; a violated bound in any group rejects the run.
pub(super) fn workload_valid(args: &Args, sizing: &Sizing) -> bool {
    run_window_valid(args)
        && clients_valid(args, sizing)
        && records_valid(args)
        && storage_valid(args, sizing)
        && history_valid(args, sizing)
}

/// Networked streaming ingress over a bounded warmup, measurement and drain.
fn run_window_valid(args: &Args) -> bool {
    args.network_ingress
        && args.streaming
        && args
            .duration
            .is_some_and(|d| d.is_finite() && (0.05..=3600.0).contains(&d))
        && args.warmup.is_finite()
        && (0.0..=10.0).contains(&args.warmup)
        && (1..=120).contains(&args.drain_timeout_secs)
}

/// Writer, reader and application thread counts, and client request sizes.
fn clients_valid(args: &Args, sizing: &Sizing) -> bool {
    let Sizing {
        production,
        writers,
        workers,
        reader_slots,
        ..
    } = *sizing;
    (1..=32).contains(&writers)
        && (1..=writers).contains(&workers)
        && (1..=32).contains(&args.readers_per_partition)
        && (production || !args.live_readers || args.system.brokers() == 3)
        // One broker group admits at most 32 preprovisioned readers.
        && (production || reader_slots <= 32)
        && (1..=reader_slots.min(if production { 32 } else { usize::MAX }))
            .contains(&args.reader_workers())
        && (1..=if production { 32 } else { writers }).contains(&args.app_threads)
        && (production || args.system.brokers() != 3 || args.app_threads == 1)
        && (1..=65536).contains(&args.request_records)
        && (1..=65536).contains(&args.reader_records)
        && (1..=128).contains(&args.reader_payload_mib)
        && (1..=65536).contains(&args.writer_inflight_appends)
}

/// One-part records, with room for the selected payload layout.
fn records_valid(args: &Args) -> bool {
    let size = args.record_bytes;
    let sized = if args.binary_payload {
        size == 16
    } else {
        size == 16 || (32..=16384).contains(&size)
    };
    sized && (!args.json_payload || size >= 32)
}

/// Segment sizes, disk threads, queue depths and storage grouping.
fn storage_valid(args: &Args, sizing: &Sizing) -> bool {
    let production = sizing.production;
    let dedicated = if sizing.native_disk {
        sizing.dedicated_disk_workers
    } else {
        1
    };
    (4..=4096).contains(&args.segment_mib)
        && (8..=1024).contains(&args.segment_decoded_mib)
        && (production || storage_targets_valid(args))
        && (1..=args.app_threads).contains(&args.disk_owner_threads)
        && (sizing.native_disk || args.disk_owner_threads == 1)
        && (production || args.disk_workers == dedicated)
        && (1..=1024).contains(&args.read_depth)
        && (1..=65536).contains(&args.broker_hwm)
        && (1..=65536).contains(&args.storage_lane_capacity)
        && (args.storage_lane_capacity == 1024 || args.system.brokers() == 1)
        && (1..=65536).contains(&args.storage_group_records)
        && (4..=1024).contains(&args.packing_block_kib)
        && args.packing_block_kib.is_power_of_two()
        && args.storage_group_kib.is_none_or(|kib| {
            kib <= 64 * 1024
                && kib.saturating_mul(1024)
                    >= args.request_records.saturating_mul(args.record_bytes)
        })
}

/// Retained history: total size, a segment per journal owner, and enough
/// operations for every writer window.
fn history_valid(args: &Args, sizing: &Sizing) -> bool {
    (4..=65536).contains(&args.history_mib)
        && (sizing.production
            || args
                .history_mib
                .checked_div(args.app_threads)
                .is_some_and(|per_owner| per_owner >= args.segment_mib))
        && (2 * sizing.writers + 1..=1_000_000).contains(&args.history_operations)
}

fn storage_targets_valid(args: &Args) -> bool {
    if args
        .resident_read_mib
        .is_some_and(|mib| !(16..=4096).contains(&mib) || !native_disk(args))
    {
        return false;
    }
    [args.operation_target_kib, args.write_group_target_kib]
        .into_iter()
        .all(|kib| {
            (64..=8192).contains(&kib)
                && (kib == 4096 || (native_disk(args) && args.system.brokers() == 1))
        })
}

/// Redundant and skewed feeds need native dedup and a fixed offered rate whose
/// window boundaries hold complete writer rounds.
pub(super) fn feeds(args: &Args, sizing: &Sizing) -> Result<()> {
    args.native.feeds.validate(sizing.writers).map_err(error)?;
    if args.native.feeds.enabled() {
        if !sizing.production
            || external(args)
            || args.native.dedup_window_secs.is_none()
            || args.records_per_second.is_none()
            || args.ramp.is_some()
        {
            return Err(error(
                "redundant/skew feeds require native dedup and a fixed offered rate",
            ));
        }
        let plan = ozzy_bench::schedule::Load {
            records_per_second: args.records_per_second,
            ..Default::default()
        }
        .plan(1)?
        .ok_or_else(|| error("redundant feeds require a fixed offered rate"))?;
        let duration = args
            .duration
            .ok_or_else(|| error("redundant feeds require a duration"))?;
        for seconds in [args.warmup, args.warmup + duration] {
            if !plan
                .count((seconds * 1e9) as u64)?
                .is_multiple_of(sizing.writers)
            {
                return Err(error(
                    "redundant feed window boundaries must contain complete writer rounds",
                ));
            }
        }
    }
    Ok(())
}

/// Held records need native delay, plain readers, and a drain that outlasts
/// the delay, so the last records released still arrive before the deadline.
pub(super) fn delay(args: &Args, sizing: &Sizing) -> Result<()> {
    let Some(millis) = args.native.delay_millis else {
        return Ok(());
    };
    if !sizing.production || external(args) {
        return Err(error("--delay-millis requires the production adapter"));
    }
    if args.native.dedup_window_secs.is_some() || args.native.groups != 0 {
        return Err(error("--delay-millis excludes dedup and --groups"));
    }
    if args.drain_timeout_secs.saturating_mul(1000) <= millis {
        return Err(error("--drain-timeout-secs must exceed --delay-millis"));
    }
    Ok(())
}

pub(super) fn sdk_owner_counts_valid(args: &Args, sizing: &Sizing) -> bool {
    let Sizing {
        writers,
        workers,
        partitions,
        reader_slots,
        ..
    } = *sizing;
    let per_owner = writers.div_ceil(workers);
    per_owner
        .checked_mul(partitions)
        .is_some_and(|count| count <= 65536)
        && per_owner
            .checked_mul(args.writer_inflight_appends)
            .is_some_and(|count| count <= 65536)
        && reader_slots.div_ceil(args.reader_workers()) <= 65536
}
