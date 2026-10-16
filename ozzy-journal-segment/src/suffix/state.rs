//! Memory-only authority snapshot shared by synchronous and asynchronous installers.

use crate::{
    CurrentReference, DecodeLimits, Manifest, OperationLimits, SegmentHeader, WriterPosition,
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
