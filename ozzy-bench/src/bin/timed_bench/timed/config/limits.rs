//! Byte and record limits derived from the run: decode, replication
//! pipeline, storage groups, and writer and reader envelopes.

use super::super::super::System;
use super::{Config, DISK_QUORUM_WINDOW_BYTES, external, sdk_batch_target};
use ozzy_proto::{EnvelopeLimits, append::DataLimits};

impl Config {
    pub(in super::super) fn decode_limits(&self) -> ozzy_journal_segment::DecodeLimits {
        ozzy_journal_segment::DecodeLimits {
            max_decoded_body_bytes: self.history.operation.max_body_bytes,
            max_group_decoded_body_bytes: (8 * 1024 * 1024)
                .max(self.history.operation.max_body_bytes + 1024)
                .max(self.storage_group_bytes() + 100 * self.args.storage_group_records + 1024),
            max_segment_decoded_body_bytes: self.args.segment_decoded_mib * 1024 * 1024,
            ..ozzy_journal_segment::DecodeLimits::default()
        }
    }

    pub(in super::super) fn segment_bytes(&self) -> usize {
        self.args.segment_mib * 1024 * 1024
    }

    pub(in super::super) fn disk_pipeline(&self) -> ozzy_replication::PipelineLimits {
        ozzy_replication::PipelineLimits {
            max_operations: ozzy_runtime::replica_journal::MAX_APPEND_OPERATIONS,
            max_body_bytes: if self.args.system == System::DiskQuorum {
                DISK_QUORUM_WINDOW_BYTES.max(self.decode_limits().max_group_decoded_body_bytes)
            } else {
                self.decode_limits().max_group_decoded_body_bytes
            },
        }
    }

    pub(in super::super) fn storage_group_bytes(&self) -> usize {
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
    pub(in super::super) fn writer_proposals(&self) -> usize {
        self.args
            .writer_inflight_appends
            .min(ozzy_runtime::replica_journal::MAX_APPEND_OPERATIONS / self.writers.max(1))
            .max(1)
    }

    /// Preserve the live reader byte window while APPEND payload sizes vary.
    pub(in super::super) fn live_queue_messages(&self) -> usize {
        ozzy_bench::native::reader_queue_messages(self.reader_limits().envelope.max_payload_bytes)
    }

    /// Independent reader subscriptions per partition, shared over broker links
    /// in the production adapter.
    pub(in super::super) fn reader_slots(&self) -> usize {
        self.partitions() * self.args.readers_per_partition
    }

    pub(in super::super) fn partitions(&self) -> usize {
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

    pub(in super::super) fn reader_records(&self) -> usize {
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

    pub(in super::super) fn reader_limits(&self) -> DataLimits {
        self.limits(self.reader_records())
    }

    pub(in super::super) fn writer_limits(&self) -> DataLimits {
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
            .min(sdk_batch_target(&self.args))
            .min(self.history.operation.max_payload_bytes);
        limits
    }

    pub(in super::super) fn limits(&self, records: usize) -> DataLimits {
        DataLimits {
            max_record_bytes: if self.native.is_some() {
                self.args.record_bytes
            } else {
                1024 * 1024
            },
            envelope: EnvelopeLimits {
                max_metadata_bytes: 512 + 28 * records,
                max_payload_bytes: (self.args.record_bytes * records).min(
                    self.native.as_ref().map_or(
                        usize::MAX,
                        super::super::native::Settings::append_payload_bytes,
                    ),
                ),
            },
            max_records: records,
            max_parts: records,
        }
    }
}
