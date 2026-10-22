//! Running a pack job: measure the captured run, choose the best part that
//! fits, and write that part.

use super::super::groups::GroupScope;
use super::{
    DirectoryError, Layout, PackJob, PackOutcome, PackedSegments, Target, best_run, packed,
};
use crate::{SEGMENT_HEADER_BYTES, SegmentReference};
use std::ops::Range;

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
        let (merged, groups) = packed::write(&self.target, scopes, successor).await?;
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
                let layout = measure(scope.clone(), &self.target).await?;
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
async fn measure(scope: GroupScope, target: &Target) -> Result<Layout, DirectoryError> {
    let mut grouping = target.grouping();
    let mut groups = scope
        .open_reading_ahead(None, target.limits.read_ahead_bytes)
        .await?;
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
