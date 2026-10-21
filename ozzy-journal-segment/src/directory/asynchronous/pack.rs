//! Packing: rewrite a run of sealed segments as one segment of large groups.
//!
//! A replica groups operations on its own timing, so one that wrote many
//! small groups needs more segments than its leader for the same history.
//! A job beside writes first measures the run's packed layout, then writes
//! the packed segment, reading and writing at most [`PACK_CHUNK_BYTES`] at a
//! time. An owner step between writes selects it in one manifest
//! publication. The packed segment keeps the run's last segment ID, so the
//! next segment's header still names its predecessor. It numbers its groups
//! from the run's first; they end before the next segment's first, now with
//! a gap.

use super::{
    DirectoryError, Journal, RetirementProof,
    groups::{GroupScope, Groups},
    segment_reference_name,
};
use crate::directory::sealed_source;
use crate::retention::PreparedSegmentLease;
use crate::{
    BodyEncoding, CanonicalOperation, CodecError, DecodeLimits, DecodedOperation, Digest,
    ENTRY_HEADER_BYTES, GROUP_SEAL_BYTES, OperationKind, SEGMENT_HEADER_BYTES, SealedSegment,
    SegmentHeader, SegmentOptions, SegmentReference, SegmentStart, SegmentWriter,
    WRITE_GROUP_ALIGNMENT, WriterError,
    async_files::Access,
    codec::{aligned_group_bytes, encoded_entry_len},
};
use ozzy_journal::progress::JournalGeneration;
use ozzy_proto::GroupId;
use std::{io, ops::Range, path::PathBuf};

/// The largest read, write, and group of a pack job.
pub const PACK_CHUNK_BYTES: usize = 8 * 1024 * 1024;

/// Bounds for one pack job.
#[derive(Debug, Clone, Copy)]
pub struct PackLimits {
    /// Largest physical group, at most [`PACK_CHUNK_BYTES`]. It must fit
    /// what one segment write may submit.
    pub max_group_bytes: usize,
    /// File generations tried past leftovers of earlier attempts.
    pub max_probes: usize,
}

/// One sealed segment's operations in packed groups.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Layout {
    groups: u64,
    bytes: u64,
    decoded: u64,
}

impl Layout {
    /// A segment with an operation too large for one packed group.
    const UNPACKABLE: Self = Self {
        groups: u64::MAX,
        bytes: u64::MAX,
        decoded: u64::MAX,
    };

    /// Count a group `Grouping` closed.
    fn close(&mut self, bytes: Option<usize>) {
        if let Some(bytes) = bytes {
            self.groups += 1;
            self.bytes += bytes as u64;
        }
    }
}

/// Greedy grouping shared by measurement and writing. Measured alone, each
/// segment closes its own last group, so a measured run bounds the written
/// one.
struct Grouping {
    max_group_bytes: usize,
    max_entries: usize,
    max_body_bytes: usize,
    entries: usize,
    count: usize,
    body: usize,
}

impl Grouping {
    fn new(limits: PackLimits, decode: DecodeLimits) -> Self {
        Self {
            max_group_bytes: limits.max_group_bytes.min(PACK_CHUNK_BYTES),
            max_entries: decode.max_entries,
            max_body_bytes: decode.max_group_decoded_body_bytes,
            entries: 0,
            count: 0,
            body: 0,
        }
    }

    /// Add one operation with a `body`-byte body. Returns the physical bytes
    /// of the group it closed first, if any; `None` from `Err` means the
    /// operation fits no group.
    fn push(&mut self, body: usize) -> Result<Option<usize>, Option<CodecError>> {
        let entry = encoded_entry_len(body).map_err(Some)?;
        if aligned_group_bytes(entry).map_err(Some)? > self.max_group_bytes
            || body > self.max_body_bytes
        {
            return Err(None);
        }
        let closed = if self.count != 0
            && (self.count == self.max_entries
                || body > self.max_body_bytes - self.body
                || aligned_group_bytes(self.entries + entry).map_err(Some)? > self.max_group_bytes)
        {
            self.close().map_err(Some)?
        } else {
            None
        };
        self.entries += entry;
        self.count += 1;
        self.body += body;
        Ok(closed)
    }

    /// Close the open group; its physical bytes, if it held anything.
    fn close(&mut self) -> Result<Option<usize>, CodecError> {
        if self.count == 0 {
            return Ok(None);
        }
        let bytes = aligned_group_bytes(self.entries)?;
        self.entries = 0;
        self.count = 0;
        self.body = 0;
        Ok(Some(bytes))
    }
}

impl Journal {
    /// The packed size planning assumes for a sealed segment: its measured
    /// layout, or else a floor. Packing never shrinks entries, and removes
    /// at most each group's padding and seal.
    fn pack_estimate(&self, reference: &SegmentReference, successor: &SegmentReference) -> u64 {
        if let Some(layout) = self.pack_layouts.borrow().get(&layout_key(reference)) {
            return layout.bytes;
        }
        let Some(sealed) = reference.sealed else {
            return u64::MAX;
        };
        let operations = successor
            .first_chain
            .next_op_number()
            .saturating_sub(reference.first_chain.next_op_number());
        let groups = successor
            .first_group_number
            .saturating_sub(reference.first_group_number);
        let used = sealed
            .valid_bytes
            .saturating_sub(SEGMENT_HEADER_BYTES as u64);
        let overhead = (GROUP_SEAL_BYTES + WRITE_GROUP_ALIGNMENT - 1) as u64;
        used.saturating_sub(groups.saturating_mul(overhead))
            .max(operations.saturating_mul(ENTRY_HEADER_BYTES as u64))
    }

    /// Capture the sealed run whose packing frees the most segments per
    /// byte rewritten, or `None` when no run of two or more fits one
    /// segment and frees at least what it rewrites. Reads no files.
    pub fn prepare_pack(&self, limits: PackLimits) -> Result<Option<PackJob>, DirectoryError> {
        self.healthy()?;
        if limits.max_group_bytes < WRITE_GROUP_ALIGNMENT || limits.max_probes == 0 {
            return Err(DirectoryError::PackLimits);
        }
        let segments = &self.manifest.segments;
        let capacity = segments
            .last()
            .ok_or(DirectoryError::MissingActiveSegment)?
            .capacity;
        let estimates: Vec<u64> = segments
            .windows(2)
            .map(|pair| self.pack_estimate(&pair[0], &pair[1]))
            .collect();
        let Some(run) = best_run(&estimates, capacity) else {
            return Ok(None);
        };
        let mut scopes = Vec::with_capacity(run.len());
        let mut leases = Vec::with_capacity(run.len());
        let mut known = Vec::with_capacity(run.len());
        for reference in &segments[run.clone()] {
            scopes.push(self.group_scope(*reference)?);
            leases.push(self.pins.protect_prepared_segment(reference.segment_id)?);
            known.push(
                self.pack_layouts
                    .borrow()
                    .get(&layout_key(reference))
                    .copied(),
            );
        }
        Ok(Some(PackJob {
            scopes,
            known,
            successor: segments[run.end],
            target: Target {
                access: self.access.clone(),
                root: self.root().to_path_buf(),
                options: self.limits.io,
                decode: self.limits.decode,
                group_id: self.manifest.identity.group_id,
                generation: self.writer.written_position().generation(),
                capacity,
                limits,
            },
            leases,
        }))
    }

    /// Memory one pack job holds: its read-ahead window past one source
    /// group, the open packed group's bodies, and that group's encoding.
    pub fn pack_scratch_bytes(&self, limits: PackLimits) -> Result<usize, DirectoryError> {
        let group = limits.max_group_bytes.min(PACK_CHUNK_BYTES);
        self.group_scan_bytes()?
            .checked_add(PACK_CHUNK_BYTES)
            .and_then(|bytes| bytes.checked_add(group.checked_mul(2)?))
            .ok_or(DirectoryError::PackLimits)
    }

    /// Remember the layouts a pack job measured, for later planning.
    pub fn record_pack_layouts(&self, outcome: &PackOutcome) {
        let mut layouts = self.pack_layouts.borrow_mut();
        for (reference, layout) in &outcome.measured {
            layouts.insert(layout_key(reference), *layout);
        }
    }

    /// Whether `packed` still replaces selected segments exactly as captured.
    /// The run's successor may have been sealed since.
    pub fn pack_selected(&self, packed: &PackedSegments) -> bool {
        self.pack_position(packed).is_some()
    }

    fn pack_position(&self, packed: &PackedSegments) -> Option<usize> {
        let segments = &self.manifest.segments;
        let first = packed.replaced.first()?;
        let at = segments.iter().position(|reference| reference == first)?;
        let end = at.checked_add(packed.replaced.len())?;
        let successor = segments.get(end)?;
        let same = |a: &SegmentReference, b: &SegmentReference| {
            (
                a.segment_id,
                a.file_generation,
                a.first_group_number,
                a.first_chain,
            ) == (
                b.segment_id,
                b.file_generation,
                b.first_group_number,
                b.first_chain,
            )
        };
        (segments.get(at..end)? == &packed.replaced[..] && same(successor, &packed.successor))
            .then_some(at)
    }

    /// Select the packed segment in place of the run it replaces, in one
    /// manifest publication. The run must still be selected as captured.
    pub async fn install_pack(&mut self, packed: PackedSegments) -> Result<(), DirectoryError> {
        self.healthy()?;
        let at = self
            .pack_position(&packed)
            .ok_or(DirectoryError::PackMismatch)?;
        let mut next = self.next_manifest()?;
        next.segments
            .splice(at..at + packed.replaced.len(), [packed.merged]);
        self.install_selected(next).await?;
        let mut layouts = self.pack_layouts.borrow_mut();
        let selected = &self.manifest.segments;
        layouts.retain(|key, _| {
            selected
                .iter()
                .any(|reference| layout_key(reference) == *key)
        });
        layouts.insert(layout_key(&packed.merged), packed.layout);
        Ok(())
    }
}

/// The best run by segments freed per estimated byte rewritten. A run frees
/// one segment fewer than it holds and must free at least what it rewrites.
fn best_run(estimates: &[u64], capacity: u64) -> Option<Range<usize>> {
    let room = capacity.checked_sub(SEGMENT_HEADER_BYTES as u64)?;
    let mut best: Option<(Range<usize>, u64)> = None;
    for start in 0..estimates.len() {
        let mut bytes = 0_u64;
        for end in start + 1..=estimates.len() {
            bytes = bytes.saturating_add(estimates[end - 1]);
            if bytes > room {
                break;
            }
            let freed = (end - start - 1) as u64;
            if freed == 0 || freed.saturating_mul(capacity) < bytes {
                continue;
            }
            let rewritten = bytes.max(1);
            let better = best.as_ref().is_none_or(|(run, best_bytes)| {
                u128::from(freed) * u128::from(*best_bytes)
                    > (run.len() as u128 - 1) * u128::from(rewritten)
            });
            if better {
                best = Some((start..end, rewritten));
            }
        }
    }
    best.map(|(run, _)| run)
}

fn layout_key(reference: &SegmentReference) -> (u64, u64) {
    (reference.segment_id, reference.file_generation)
}

/// Where and how a pack job writes, captured from the journal so the job
/// borrows none.
#[derive(Debug)]
struct Target {
    access: Access,
    root: PathBuf,
    options: SegmentOptions,
    decode: DecodeLimits,
    group_id: GroupId,
    generation: JournalGeneration,
    capacity: u64,
    limits: PackLimits,
}

impl Target {
    fn grouping(&self) -> Grouping {
        Grouping::new(self.limits, self.decode)
    }
}

/// A captured run of sealed segments to pack, protected from deletion.
#[derive(Debug)]
pub struct PackJob {
    scopes: Vec<GroupScope>,
    known: Vec<Option<Layout>>,
    successor: SegmentReference,
    target: Target,
    leases: Vec<PreparedSegmentLease>,
}

/// What a pack job measured, and the packed segment when a run fit.
#[derive(Debug)]
pub struct PackOutcome {
    measured: Vec<(SegmentReference, Layout)>,
    packed: Option<PackedSegments>,
}

impl PackOutcome {
    /// The packed segment to install, if a run of two or more fit.
    pub fn take_packed(&mut self) -> Option<PackedSegments> {
        self.packed.take()
    }
}

/// A written, synchronized packed segment awaiting selection. Its run stays
/// protected from deletion until it is installed or dropped.
#[derive(Debug)]
pub struct PackedSegments {
    group_id: GroupId,
    replaced: Vec<SegmentReference>,
    successor: SegmentReference,
    merged: SegmentReference,
    layout: Layout,
    _leases: Vec<PreparedSegmentLease>,
}

impl PackedSegments {
    /// The sealed segments the packed one replaces, oldest first.
    pub fn replaced(&self) -> &[SegmentReference] {
        &self.replaced
    }

    /// The packed segment's reference.
    pub const fn merged(&self) -> SegmentReference {
        self.merged
    }

    /// The packed segment's retirement proof, from `proofs` of every
    /// segment it replaced, or `None` when one is missing.
    pub fn retirement_proof(&self, proofs: &[RetirementProof]) -> Option<RetirementProof> {
        let mut ends = Vec::new();
        for (index, reference) in self.replaced.iter().enumerate() {
            let next = self.replaced.get(index + 1).unwrap_or(&self.successor);
            let source = sealed_source(self.group_id, reference, next).ok()?;
            let proof = proofs.iter().find(|proof| proof.source == source)?;
            ends.extend_from_slice(&proof.ends);
        }
        ends.sort_unstable_by_key(|(partition, _, _)| *partition);
        ends.dedup_by(|later, kept| {
            let same = later.0 == kept.0;
            if same {
                kept.1 = kept.1.max(later.1);
                kept.2 = kept.2.max(later.2);
            }
            same
        });
        Some(RetirementProof {
            source: sealed_source(self.group_id, &self.merged, &self.successor).ok()?,
            capacity: self.merged.capacity,
            ends,
        })
    }
}

impl PackJob {
    /// Measure the run, pick the best part of it that fits, and write that
    /// part as one synchronized segment. Borrows no journal; a failure
    /// leaves at most an unselected file for cleanup.
    pub async fn run(mut self) -> Result<PackOutcome, DirectoryError> {
        let (measured, layouts) = self.measure().await?;
        let Some(run) = self.choose(&layouts) else {
            return Ok(PackOutcome {
                measured,
                packed: None,
            });
        };
        let successor = self.next(run.end);
        let replaced: Vec<_> = self.scopes[run.clone()]
            .iter()
            .map(GroupScope::reference)
            .collect();
        let decoded = layouts[run.clone()]
            .iter()
            .map(|layout| layout.decoded)
            .sum::<u64>();
        let scopes: Vec<_> = self.scopes.drain(run.clone()).collect();
        let leases: Vec<_> = self.leases.drain(run).collect();
        let (merged, groups) = write(&self.target, scopes, successor).await?;
        let layout = Layout {
            groups,
            bytes: merged.sealed.map_or(0, |sealed| sealed.valid_bytes)
                - SEGMENT_HEADER_BYTES as u64,
            decoded,
        };
        Ok(PackOutcome {
            measured,
            packed: Some(PackedSegments {
                group_id: self.target.group_id,
                replaced,
                successor,
                merged,
                layout,
                _leases: leases,
            }),
        })
    }

    /// Each captured segment's layout, measuring those not known yet.
    async fn measure(
        &self,
    ) -> Result<(Vec<(SegmentReference, Layout)>, Vec<Layout>), DirectoryError> {
        let mut measured = Vec::new();
        let mut layouts = Vec::with_capacity(self.scopes.len());
        for (scope, known) in self.scopes.iter().zip(&self.known) {
            let layout = if let Some(layout) = known {
                *layout
            } else {
                let layout = measure(scope.clone(), self.target.grouping()).await?;
                measured.push((scope.reference(), layout));
                layout
            };
            layouts.push(layout);
        }
        Ok((measured, layouts))
    }

    /// The segment after the first `end` captured ones.
    fn next(&self, end: usize) -> SegmentReference {
        self.scopes
            .get(end)
            .map_or(self.successor, GroupScope::reference)
    }

    /// The best part of the run by measured layouts, if one fits.
    fn choose(&self, layouts: &[Layout]) -> Option<Range<usize>> {
        let estimates: Vec<u64> = layouts.iter().map(|layout| layout.bytes).collect();
        best_run(&estimates, self.target.capacity).filter(|run| {
            let parts = &layouts[run.clone()];
            let groups = parts.iter().map(|layout| layout.groups).sum::<u64>();
            let bytes = parts.iter().map(|layout| layout.bytes).sum::<u64>();
            let decoded = parts.iter().map(|layout| layout.decoded).sum::<u64>();
            let first = self.scopes[run.start].reference().first_group_number;
            let decode = self.target.decode;
            parts.iter().all(|layout| *layout != Layout::UNPACKABLE)
                && groups <= self.next(run.end).first_group_number - first
                && groups <= decode.max_groups as u64
                && decoded <= decode.max_segment_decoded_body_bytes as u64
                && bytes <= self.target.capacity - SEGMENT_HEADER_BYTES as u64
        })
    }
}

/// Read one sealed segment and size its operations in packed groups.
async fn measure(scope: GroupScope, mut grouping: Grouping) -> Result<Layout, DirectoryError> {
    let mut groups = scope.open_reading_ahead(None, PACK_CHUNK_BYTES).await?;
    let mut layout = Layout {
        groups: 0,
        bytes: 0,
        decoded: 0,
    };
    let mut unpackable = false;
    while let Some(group) = groups.next().await? {
        for operation in &group.operations {
            match grouping.push(operation.body.len()) {
                Ok(closed) => layout.close(closed),
                // Read on to the end, which checks the seal and closes the file.
                Err(None) => unpackable = true,
                Err(Some(error)) => return Err(error.into()),
            }
            layout.decoded += operation.body.len() as u64;
        }
    }
    groups.finish().await?;
    layout.close(grouping.close()?);
    Ok(if unpackable {
        Layout::UNPACKABLE
    } else {
        layout
    })
}

/// Operations of the open packed group, with owned bodies.
#[derive(Default)]
struct Pending {
    operations: Vec<PendingOperation>,
    digests: Vec<Digest>,
    bodies: Vec<u8>,
}

struct PendingOperation {
    configuration_epoch: u64,
    original_view: u64,
    op_number: u64,
    previous_digest: Digest,
    kind: OperationKind,
    body: Range<usize>,
}

impl Pending {
    fn push(&mut self, operation: &DecodedOperation<'_>) {
        let start = self.bodies.len();
        self.bodies.extend_from_slice(&operation.body);
        self.digests.push(operation.body_digest);
        self.operations.push(PendingOperation {
            configuration_epoch: operation.configuration_epoch,
            original_view: operation.original_view,
            op_number: operation.op_number,
            previous_digest: operation.previous_digest,
            kind: operation.kind,
            body: start..self.bodies.len(),
        });
    }

    async fn flush(
        &mut self,
        writer: &mut SegmentWriter,
        target: &Target,
    ) -> Result<(), WriterError> {
        if self.operations.is_empty() {
            return Ok(());
        }
        let operations: Vec<_> = self
            .operations
            .iter()
            .map(|operation| CanonicalOperation {
                group_id: target.group_id,
                configuration_epoch: operation.configuration_epoch,
                original_view: operation.original_view,
                op_number: operation.op_number,
                previous_digest: operation.previous_digest,
                kind: operation.kind,
                body: &self.bodies[operation.body.clone()],
            })
            .collect();
        writer
            .append_with_limits_and_digests(
                &operations,
                &self.digests,
                BodyEncoding::Raw,
                Some(target.decode),
            )
            .await?;
        self.operations.clear();
        self.digests.clear();
        self.bodies.clear();
        Ok(())
    }
}

/// Write the run's operations into a new generation of its last segment,
/// numbered from the run's first group. Returns the packed segment and its
/// group count.
async fn write(
    target: &Target,
    scopes: Vec<GroupScope>,
    successor: SegmentReference,
) -> Result<(SegmentReference, u64), DirectoryError> {
    let mismatch = DirectoryError::PackMismatch;
    let (Some(first), Some(last)) = (scopes.first(), scopes.last()) else {
        return Err(mismatch);
    };
    let (first, last) = (first.reference(), last.reference());
    let mut sources = scopes.into_iter();
    let groups = sources
        .next()
        .ok_or(mismatch)?
        .open_reading_ahead(None, PACK_CHUNK_BYTES)
        .await?;
    let source = groups.header();
    let header = SegmentHeader::new(
        target.group_id,
        last.segment_id,
        source.predecessor_segment_id(),
        source.predecessor_digest(),
        target.capacity,
    )?;
    let (mut merged, mut writer) = create(target, header, last, first).await?;
    copy(target, groups, sources, &mut writer).await?;
    let written = writer.written_position();
    if written.next_chain() != successor.first_chain
        || written.group_number() >= successor.first_group_number
    {
        return Err(DirectoryError::PackMismatch);
    }
    writer.sync_through(written).await?;
    merged.sealed = Some(SealedSegment {
        valid_bytes: written.end_offset(),
        digest: writer.state().structural_digest(),
    });
    writer.close().await?;
    // The new file's entry must survive a crash once a manifest selects it.
    target
        .access
        .sync_directory(target.root.join("segments"))
        .await?;
    Ok((
        merged,
        written.group_number() + 1 - first.first_group_number,
    ))
}

/// Copy every operation of the sources in packed groups. Groups span source
/// segments, so small ones pack densely too, and there are at most as many
/// as measured.
async fn copy(
    target: &Target,
    mut groups: Box<Groups>,
    mut sources: std::vec::IntoIter<GroupScope>,
    writer: &mut SegmentWriter,
) -> Result<(), DirectoryError> {
    let mut grouping = target.grouping();
    let mut pending = Pending::default();
    loop {
        while let Some(group) = groups.next().await? {
            for operation in &group.operations {
                let closed = grouping
                    .push(operation.body.len())
                    .map_err(|error| error.map_or(DirectoryError::PackMismatch, Into::into))?;
                if closed.is_some() {
                    pending.flush(writer, target).await?;
                }
                pending.push(operation);
            }
        }
        groups.finish().await?;
        let Some(scope) = sources.next() else {
            break;
        };
        groups = scope.open_reading_ahead(None, PACK_CHUNK_BYTES).await?;
    }
    Ok(pending.flush(writer, target).await?)
}

/// Create the next free file generation of `last`'s segment ID, starting at
/// `first`'s group number and chain.
async fn create(
    target: &Target,
    header: SegmentHeader,
    last: SegmentReference,
    first: SegmentReference,
) -> Result<(SegmentReference, SegmentWriter), DirectoryError> {
    let mut reference = SegmentReference {
        segment_id: last.segment_id,
        file_generation: last.file_generation,
        first_group_number: first.first_group_number,
        first_chain: first.first_chain,
        capacity: header.capacity(),
        sealed: None,
    };
    for _ in 0..target.limits.max_probes {
        reference.file_generation = reference
            .file_generation
            .checked_add(1)
            .ok_or(DirectoryError::SegmentIdExhausted)?;
        let writer = SegmentWriter::create(
            target
                .access
                .segment_file(target.root.join(segment_reference_name(reference))),
            header
                .clone()
                .with_file_generation(reference.file_generation),
            SegmentStart {
                generation: target.generation,
                first_group_number: first.first_group_number,
                initial_chain: first.first_chain,
            },
            target.options,
        )
        .await;
        match writer {
            Ok(writer) => return Ok((reference, writer)),
            Err(WriterError::Io(error)) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error.into()),
        }
    }
    Err(DirectoryError::PackProbeLimit)
}
