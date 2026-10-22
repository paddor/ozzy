//! Writing a packed segment: a run's operations in packed groups, in a new
//! file generation of its last segment.

use super::super::groups::{GroupScope, Groups};
use super::{DirectoryError, Target};
use crate::directory::segment_reference_name;
use crate::{
    BodyEncoding, CanonicalOperation, DecodedOperation, Digest, OperationKind, SealedSegment,
    SegmentHeader, SegmentReference, SegmentStart, SegmentWriter, WriterError,
};
use std::{io, ops::Range};

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
pub(super) async fn write(
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
        .open_reading_ahead(None, target.limits.read_ahead_bytes)
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
        groups = scope
            .open_reading_ahead(None, target.limits.read_ahead_bytes)
            .await?;
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
