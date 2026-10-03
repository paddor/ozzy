//! Memory-only authority snapshot shared by synchronous and asynchronous installers.

use crate::{
    CurrentReference, DecodeLimits, Manifest, OpenGroupJournal, OperationLimits, SegmentHeader,
    WriterPosition,
};

#[derive(Clone, Copy)]
pub(crate) struct Source<'a> {
    pub(crate) manifest: &'a Manifest,
    pub(crate) current: CurrentReference,
    pub(crate) header: &'a SegmentHeader,
    pub(crate) written: WriterPosition,
    pub(crate) durable: WriterPosition,
    pub(crate) healthy: bool,
    pub(crate) decode: DecodeLimits,
    pub(crate) operations: OperationLimits,
    pub(crate) max_segments: usize,
}

impl<'a> Source<'a> {
    pub(crate) fn new(journal: &'a OpenGroupJournal) -> Self {
        Self {
            manifest: &journal.directory.manifest,
            current: journal.directory.current,
            header: journal.writer.header(),
            written: journal.writer.written_position(),
            durable: journal.writer.durable_position(),
            healthy: !journal.buffered_roll_pending() && !journal.writer.is_faulted(),
            decode: journal.decode_limits,
            operations: journal.operation_limits,
            max_segments: journal.directory.limits.max_segments,
        }
    }
}
