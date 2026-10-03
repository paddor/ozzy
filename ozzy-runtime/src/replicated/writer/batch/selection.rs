//! Pure request sizing. No payload ownership, queues, sockets, or clocks.

use super::{DataLimits, Error};
use ozzy_proto::data::Encoding;

#[derive(Debug, PartialEq, Eq)]
pub(super) struct Selection {
    pub(super) records: usize,
    pub(super) bytes: usize,
    pub(super) full: bool,
}

impl Selection {
    /// Shapes are (wire bytes, part count, encoding). A soft target never splits a
    /// record; hard limits and peer credit always apply, including to singletons.
    pub(super) fn collect(
        shapes: impl Iterator<Item = (usize, usize, Encoding)>,
        limits: DataLimits,
        target_bytes: usize,
        cap: usize,
        byte_credit: usize,
        fixed: usize,
    ) -> Result<Self, Error> {
        let mut selected = Self {
            records: 0,
            bytes: 0,
            full: false,
        };
        let mut metadata = fixed;
        let mut parts = 0;
        let mut logical = 0;
        let payload_limit = limits.envelope.max_payload_bytes.min(byte_credit);
        for (bytes, record_parts, encoding) in shapes {
            let original = match encoding {
                Encoding::Raw => bytes,
                Encoding::Lz4 { decoded_bytes } => decoded_bytes as usize,
            };
            let descriptors = record_parts
                .checked_mul(4)
                .and_then(|bytes| bytes.checked_add(20 + encoding.metadata_bytes()))
                .ok_or(Error::Configuration)?;
            if selected.records == 0
                && (!encoding.validate(record_parts, limits.max_parts, limits.max_record_bytes)
                    || record_parts > limits.max_parts
                    || descriptors > limits.envelope.max_metadata_bytes.saturating_sub(fixed)
                    || bytes
                        > limits
                            .envelope
                            .max_payload_bytes
                            .min(limits.max_record_bytes))
            {
                return Err(Error::Configuration);
            }
            if selected.records == cap
                || original > limits.max_record_bytes
                || bytes > limits.max_record_bytes
                || (selected.records != 0
                    && (logical >= target_bytes || original > target_bytes.saturating_sub(logical)))
                || record_parts > limits.max_parts.saturating_sub(parts)
                || descriptors > limits.envelope.max_metadata_bytes.saturating_sub(metadata)
                || bytes > payload_limit.saturating_sub(selected.bytes)
            {
                selected.full = true;
                break;
            }
            selected.records += 1;
            selected.bytes += bytes;
            logical += original;
            metadata += descriptors;
            parts += record_parts;
        }
        selected.full |= selected.records == cap
            || logical >= target_bytes
            || parts == limits.max_parts
            || selected.bytes == payload_limit
            || metadata == limits.envelope.max_metadata_bytes;
        Ok(selected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ozzy_proto::EnvelopeLimits;

    fn limits() -> DataLimits {
        DataLimits {
            max_records: 8,
            max_parts: 8,
            max_record_bytes: 1024,
            envelope: EnvelopeLimits {
                max_metadata_bytes: 1024,
                max_payload_bytes: 4096,
            },
        }
    }

    #[test]
    fn target_preserves_large_singleton_but_credit_still_bounds_it() {
        let shapes = [(512, 1, Encoding::Raw), (16, 1, Encoding::Raw)];
        let selected = Selection::collect(shapes.into_iter(), limits(), 128, 8, 4096, 100).unwrap();
        assert_eq!(
            selected,
            Selection {
                records: 1,
                bytes: 512,
                full: true
            }
        );
        let selected = Selection::collect(shapes.into_iter(), limits(), 128, 8, 511, 100).unwrap();
        assert_eq!(selected.records, 0);
    }

    #[test]
    fn empty_parts_charge_descriptors_and_part_credit() {
        let selected = Selection::collect(
            [(0, 4, Encoding::Raw), (0, 5, Encoding::Raw)].into_iter(),
            limits(),
            128,
            8,
            4096,
            100,
        )
        .unwrap();
        assert_eq!(
            selected,
            Selection {
                records: 1,
                bytes: 0,
                full: true
            }
        );
        let mut bounds = limits();
        bounds.envelope.max_metadata_bytes = 135;
        assert!(
            Selection::collect(
                [(0, 4, Encoding::Raw)].into_iter(),
                bounds,
                128,
                8,
                4096,
                100
            )
            .is_err()
        );
    }

    #[test]
    fn sparse_batch_is_ready_and_renegotiated_oversize_is_rejected_when_first() {
        let selected = Selection::collect(
            [(16, 1, Encoding::Raw)].into_iter(),
            limits(),
            128,
            8,
            4096,
            100,
        )
        .unwrap();
        assert_eq!(
            selected,
            Selection {
                records: 1,
                bytes: 16,
                full: false
            }
        );
        let selected = Selection::collect(
            [(16, 1, Encoding::Raw), (1025, 1, Encoding::Raw)].into_iter(),
            limits(),
            4096,
            8,
            4096,
            100,
        )
        .unwrap();
        assert_eq!(selected.records, 1);
        assert!(
            Selection::collect(
                [(1025, 1, Encoding::Raw)].into_iter(),
                limits(),
                4096,
                8,
                4096,
                100
            )
            .is_err()
        );
    }
}
