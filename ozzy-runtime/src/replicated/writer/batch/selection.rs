//! Pure request sizing. No payload ownership, queues, sockets, or clocks.

use super::{DataLimits, Error};
use ozzy_proto::data::Encoding;

/// Records sized for one request, with running totals so a partial selection
/// resumes with records queued later instead of sizing its records again.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct Selection {
    pub(super) records: usize,
    pub(super) bytes: usize,
    pub(super) full: bool,
    /// Whether the selected records are scheduled; one request never mixes.
    pub(super) scheduled: Option<bool>,
    metadata: usize,
    parts: usize,
    logical: usize,
}

impl Selection {
    /// An empty selection whose request metadata starts at `fixed` bytes.
    pub(super) const fn new(fixed: usize) -> Self {
        Self {
            records: 0,
            bytes: 0,
            full: false,
            scheduled: None,
            metadata: fixed,
            parts: 0,
            logical: 0,
        }
    }

    /// Add the shapes that follow the selected records until one does not fit.
    /// Shapes are (wire bytes, part count, encoding, scheduled). A soft target
    /// never splits a record; hard packet limits also apply to singletons. A
    /// change between scheduled and immediate records ends the request.
    pub(super) fn extend(
        &mut self,
        shapes: impl Iterator<Item = (usize, usize, Encoding, bool)>,
        limits: DataLimits,
        target_bytes: usize,
        cap: usize,
    ) -> Result<(), Error> {
        let payload_limit = limits.envelope.max_payload_bytes;
        let metadata_limit = limits.envelope.max_metadata_bytes;
        for (bytes, record_parts, encoding, scheduled) in shapes {
            if self.scheduled.is_some_and(|kind| kind != scheduled) {
                self.full = true;
                break;
            }
            let original = match encoding {
                Encoding::Raw => bytes,
                Encoding::Lz4 { decoded_bytes } => decoded_bytes as usize,
            };
            // A scheduled record also carries its due time.
            let descriptors = record_parts
                .checked_mul(4)
                .and_then(|bytes| bytes.checked_add(encoding.head_bytes()))
                .and_then(|bytes| bytes.checked_add(if scheduled { 8 } else { 0 }))
                .ok_or(Error::Configuration)?;
            let metadata_room = metadata_limit.saturating_sub(self.metadata);
            if self.records == 0
                && (!encoding.validate(record_parts, limits.max_parts, limits.max_record_bytes)
                    || record_parts > limits.max_parts
                    || descriptors > metadata_room
                    || bytes > payload_limit.min(limits.max_record_bytes))
            {
                return Err(Error::Configuration);
            }
            if self.records == cap
                || original > limits.max_record_bytes
                || bytes > limits.max_record_bytes
                || (self.records != 0
                    && (self.logical >= target_bytes
                        || original > target_bytes.saturating_sub(self.logical)))
                || record_parts > limits.max_parts.saturating_sub(self.parts)
                || descriptors > metadata_room
                || bytes > payload_limit.saturating_sub(self.bytes)
            {
                self.full = true;
                break;
            }
            self.scheduled = Some(scheduled);
            self.records += 1;
            self.bytes += bytes;
            self.logical += original;
            self.metadata += descriptors;
            self.parts += record_parts;
        }
        self.full |= self.records == cap
            || self.logical >= target_bytes
            || self.parts == limits.max_parts
            || self.bytes == payload_limit
            || self.metadata == metadata_limit;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ozzy_proto::EnvelopeLimits;

    /// (records, bytes, full) of one walk over immediate `shapes`.
    fn collect(
        shapes: impl Iterator<Item = (usize, usize, Encoding)>,
        limits: DataLimits,
        target_bytes: usize,
        cap: usize,
        fixed: usize,
    ) -> Result<(usize, usize, bool), Error> {
        let mut selection = Selection::new(fixed);
        selection.extend(immediate(shapes), limits, target_bytes, cap)?;
        Ok((selection.records, selection.bytes, selection.full))
    }

    fn immediate(
        shapes: impl Iterator<Item = (usize, usize, Encoding)>,
    ) -> impl Iterator<Item = (usize, usize, Encoding, bool)> {
        shapes.map(|(bytes, parts, encoding)| (bytes, parts, encoding, false))
    }

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
    fn target_preserves_large_singleton_but_hard_limits_still_bound_it() {
        let shapes = [(512, 1, Encoding::Raw), (16, 1, Encoding::Raw)];
        let selected = collect(shapes.into_iter(), limits(), 128, 8, 100).unwrap();
        assert_eq!(selected, (1, 512, true));
        let mut hard = limits();
        hard.max_record_bytes = 511;
        assert!(collect(shapes.into_iter(), hard, 128, 8, 100).is_err());
    }

    #[test]
    fn empty_parts_charge_descriptors_and_part_limits() {
        let selected = collect(
            [(0, 4, Encoding::Raw), (0, 5, Encoding::Raw)].into_iter(),
            limits(),
            128,
            8,
            100,
        )
        .unwrap();
        assert_eq!(selected, (1, 0, true));
        let mut bounds = limits();
        bounds.envelope.max_metadata_bytes = 135;
        assert!(collect([(0, 4, Encoding::Raw)].into_iter(), bounds, 128, 8, 100).is_err());
    }

    #[test]
    fn sparse_batch_is_ready_and_renegotiated_oversize_is_rejected_when_first() {
        let selected =
            collect([(16, 1, Encoding::Raw)].into_iter(), limits(), 128, 8, 100).unwrap();
        assert_eq!(selected, (1, 16, false));
        let selected = collect(
            [(16, 1, Encoding::Raw), (1025, 1, Encoding::Raw)].into_iter(),
            limits(),
            4096,
            8,
            100,
        )
        .unwrap();
        assert_eq!(selected.0, 1);
        assert!(
            collect(
                [(1025, 1, Encoding::Raw)].into_iter(),
                limits(),
                4096,
                8,
                100
            )
            .is_err()
        );
    }

    #[test]
    fn resumed_selection_matches_one_walk_over_the_same_records() {
        let shapes = [
            (100, 1, Encoding::Raw),
            (40, 2, Encoding::Lz4 { decoded_bytes: 90 }),
            (0, 3, Encoding::Raw),
            (200, 1, Encoding::Raw),
            (16, 1, Encoding::Raw),
        ];
        for target in [64, 300, 4096] {
            let whole = collect(shapes.into_iter(), limits(), target, 8, 100).unwrap();
            // Records arrive one at a time; each pass sizes only new ones.
            let mut resumed = Selection::new(100);
            for arrived in 1..=shapes.len() {
                if resumed.full {
                    break;
                }
                let new = shapes[resumed.records..arrived].iter().copied();
                resumed.extend(immediate(new), limits(), target, 8).unwrap();
            }
            assert_eq!((resumed.records, resumed.bytes, resumed.full), whole);
        }
    }

    #[test]
    fn scheduled_records_never_share_a_request_with_immediate_ones() {
        let raw = Encoding::Raw;
        let shapes = [(16, 1, raw, true), (16, 1, raw, true), (16, 1, raw, false)];
        let mut selection = Selection::new(100);
        selection
            .extend(shapes.into_iter(), limits(), 4096, 8)
            .unwrap();
        assert_eq!(
            (selection.records, selection.full, selection.scheduled),
            (2, true, Some(true))
        );
        // Each due time takes metadata room: room for one 24-byte
        // descriptor and its due time.
        let mut bounds = limits();
        bounds.envelope.max_metadata_bytes = 100 + 24 + 8 + 31;
        let mut selection = Selection::new(100);
        selection
            .extend(shapes.into_iter(), bounds, 4096, 8)
            .unwrap();
        assert_eq!(selection.records, 1);
    }
}
