//! Shared writer storage, independent of partition authority and physical links.

use super::{DataLimits, MAX_APPEND_RECORDS, WriterConfig};

/// Declared shared-link storage for one logical partition writer. Capacity is
/// reserved before opening, including unused queues and packing slots.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SharedWriterReservation {
    /// Owned writer storage and its rounded confirmation queue.
    pub idle_bytes: usize,
    /// Maximum additional backing per correlated APPEND, including reply
    /// aliases. Keep one extra request available while opening an idle writer.
    pub request_bytes: usize,
    pub(super) reply_slots: usize,
    pub(super) writer_bytes: usize,
}

pub(in crate::replicated) struct Bounds {
    pub(in crate::replicated) limits: DataLimits,
    pub(in crate::replicated) batch_target_bytes: usize,
    pub(in crate::replicated) max_producers: usize,
    pub(in crate::replicated) inflight_appends: usize,
}

impl From<&WriterConfig> for Bounds {
    fn from(config: &WriterConfig) -> Self {
        Self {
            limits: config.limits,
            batch_target_bytes: config.batch_target_bytes,
            max_producers: config.max_producers,
            inflight_appends: config.inflight_appends,
        }
    }
}

impl Bounds {
    pub(in crate::replicated) fn reservation(
        &self,
        reply_metadata_bytes: usize,
    ) -> Option<SharedWriterReservation> {
        let wire_sizes = [
            self.limits.max_records,
            self.limits.max_parts,
            self.limits.max_record_bytes,
            self.limits.envelope.max_payload_bytes,
            self.limits.envelope.max_metadata_bytes,
        ];
        if wire_sizes
            .iter()
            .any(|&size| size == 0 || u32::try_from(size).is_err())
            || self.max_producers == 0
            || self.inflight_appends == 0
            || self.batch_target_bytes == 0
        {
            return None;
        }
        let reply_slots = self
            .limits
            .max_records
            .checked_mul(self.inflight_appends)?
            .checked_add(self.inflight_appends)?;
        let writer_bytes = self.storage_bytes()?;
        Some(SharedWriterReservation {
            idle_bytes: super::super::broker_links::append::Connection::writer_bytes(
                reply_slots,
                writer_bytes,
            )?,
            request_bytes:
                super::super::broker_links::append::Connection::request_bytes_with_metadata(
                    reply_metadata_bytes,
                    self.limits.max_records.min(MAX_APPEND_RECORDS),
                    transport_backing_bytes(self.limits),
                )?,
            reply_slots,
            writer_bytes,
        })
    }

    fn storage_bytes(&self) -> Option<usize> {
        let records = self
            .limits
            .max_records
            .max(1024)
            .checked_mul(self.inflight_appends)?
            .checked_mul(self.max_producers)?;
        let queue = records
            .checked_next_power_of_two()?
            .checked_mul(self.max_producers)?
            .checked_mul(2 * super::state::QUEUED_SLOT_BYTES)?;
        // Intake charges payloads and part tables to one byte window. Prepared
        // requests retain at most one negotiated part table each until ACK.
        let table = self.limits.max_parts.checked_mul(size_of::<u32>())?;
        let intake = intake_bytes(self.limits, self.batch_target_bytes)?;
        let tables = table
            .checked_mul(self.inflight_appends)?
            .checked_add(intake)?
            .checked_mul(self.max_producers)?
            .checked_add(records.checked_mul(1024)?)?;
        let bodies = self
            .batch_target_bytes
            .max(self.limits.max_record_bytes)
            .checked_mul(self.inflight_appends)?
            .checked_mul(self.max_producers)?
            .checked_mul(2)?;
        let packing = lz4rip::get_maximum_output_size(self.limits.envelope.max_payload_bytes)
            .checked_mul(2)?
            .checked_mul(self.inflight_appends.checked_mul(4)?.checked_add(1)?)?;
        queue
            .checked_add(tables)?
            .checked_add(bodies)?
            .checked_add(packing)?
            .checked_add(self.limits.envelope.max_metadata_bytes.checked_mul(2)?)?
            .checked_add(4 * 1024 * 1024)
    }
}

/// One batch plus the next record whose shape can close it. Part tables of
/// both the selected batch and lookahead share the same intake byte window.
pub(super) fn intake_bytes(limits: DataLimits, target: usize) -> Option<usize> {
    target
        .checked_add(limits.max_record_bytes)?
        .checked_add(limits.max_parts.checked_mul(2 * size_of::<u32>())?)
}

pub(super) fn transport_backing_bytes(limits: DataLimits) -> usize {
    lz4rip::get_maximum_output_size(limits.envelope.max_payload_bytes)
        .saturating_mul(2)
        .saturating_add(65536 + 512)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replicated::SharedTopicWriterConfig;

    #[test]
    fn part_table_reservation_scales_with_requests_instead_of_records_squared() {
        let limits = DataLimits {
            max_records: 2048,
            max_parts: 1,
            envelope: ozzy_proto::EnvelopeLimits {
                max_payload_bytes: 4 * 1024 * 1024,
                ..Default::default()
            },
            ..Default::default()
        };
        let mut config = SharedTopicWriterConfig::new(limits);
        config.batch_target_bytes = 4 * 1024 * 1024;
        config.inflight_appends = 3;
        let small = config.link_reservation(64 * 1024).unwrap();
        config.limits.max_parts = 2048;
        let large = config.link_reservation(64 * 1024).unwrap();
        assert_eq!(large.writer_bytes - small.writer_bytes, (2048 - 1) * 4 * 5);
    }

    #[test]
    fn reservation_refuses_empty_windows_and_overflow_without_allocating() {
        let mut config = SharedTopicWriterConfig::new(DataLimits::default());
        assert!(config.link_reservation(64 * 1024).is_some());
        assert!(config.link_reservation(usize::MAX).is_none());
        config.batch_target_bytes = usize::MAX;
        assert!(config.link_reservation(64 * 1024).is_none());
        config.batch_target_bytes = 64 * 1024;
        config.limits.envelope.max_payload_bytes = usize::MAX;
        assert!(config.link_reservation(64 * 1024).is_none());
        config.limits = DataLimits::default();
        config.inflight_appends = 0;
        assert!(config.link_reservation(64 * 1024).is_none());
        config.inflight_appends = usize::MAX;
        assert!(config.link_reservation(64 * 1024).is_none());
    }
}
