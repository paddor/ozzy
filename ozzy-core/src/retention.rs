//! Pure whole-segment retention decisions. Selection is a target, never deletion
//! authority: the owner first confirms retry floors/trim and selects a checkpoint.

use ozzy_journal::operation::RetentionPolicy;
use ozzy_proto::Offset;

/// Validated summary of one selected segment, in physical history order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Segment {
    /// Physical segment generation.
    pub id: u64,
    /// Allocated capacity, including unused space and framing.
    pub capacity: u64,
    /// Active segments cannot be retired.
    pub sealed: bool,
    /// Last canonical operation covered by this segment.
    pub last_operation: u64,
    /// Exclusive partition record end through this segment.
    pub record_end: Offset,
    /// Maximum broker append time; clocks need not increase with offsets.
    pub newest_append_millis: Option<u64>,
}

/// One bounded prefix decision, to be rechecked against the owner's generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Plan {
    /// Oldest sealed generations eligible after checkpoint publication.
    pub retire: Vec<u64>,
    /// Exclusive record floor through that prefix; used only when it is nonempty.
    pub record_floor: Offset,
    /// Exclusive operation floor through the proposed sealed prefix.
    pub operation_floor: u64,
    /// Selected capacities after the proposed prefix retirement.
    pub selected_bytes: u64,
    /// Seal an aged, fully committed active segment before a subsequent plan.
    pub roll_active: bool,
    /// The first sealed segment a target would retire but held records keep.
    /// Carrying them forward frees it.
    pub carry: Option<Carry>,
}

/// A segment to free by carrying its held records forward.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Carry {
    /// Exclusive operation end of the segment.
    pub end: u64,
    /// Its age expired; otherwise only the byte target retires it.
    pub expired: bool,
    /// Its allocated capacity.
    pub capacity: u64,
}

/// Operations a plan must keep.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Kept {
    /// Highest committed operation; segments past it stay.
    pub committed_operation: u64,
    /// Oldest operation still holding a scheduled record; its segment and
    /// every later one stay.
    pub held_operation: Option<u64>,
}

/// Invalid summaries, limits, or capacity arithmetic.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("invalid retention segment summaries or budget")]
pub struct PlanError;

impl Plan {
    /// Age and byte targets independently expire the oldest prefix. Newer
    /// timestamps or protected accepted operations stop age-based retirement.
    /// A segment holding the held operation or later stops both targets; when
    /// one would retire it, it is the carry target.
    pub fn select(
        segments: &[Segment],
        policy: RetentionPolicy,
        now: u64,
        kept: Kept,
        maximum_segments: usize,
    ) -> Result<Self, PlanError> {
        let committed_operation = kept.committed_operation;
        let active = active(segments, maximum_segments)?;
        let mut selected_bytes = segments.iter().try_fold(0u64, |total, segment| {
            total.checked_add(segment.capacity).ok_or(PlanError)
        })?;
        let expired = |segment: &Segment| {
            policy.max_age_millis.is_some_and(|age| {
                segment.newest_append_millis.is_none_or(|newest| {
                    now.checked_sub(newest)
                        .is_some_and(|elapsed| elapsed >= age.get())
                })
            })
        };
        let mut retire = Vec::new();
        let mut record_floor = Offset::ZERO;
        let mut operation_floor = 0;
        let mut carry = None;
        for segment in segments.iter().take(maximum_segments) {
            if !segment.sealed || segment.last_operation > committed_operation {
                break;
            }
            let oversized = policy
                .max_bytes
                .is_some_and(|limit| selected_bytes > limit.get());
            let aged = expired(segment);
            if !oversized && !aged {
                break;
            }
            if kept
                .held_operation
                .is_some_and(|operation| segment.last_operation >= operation)
            {
                carry = Some(Carry {
                    end: segment.last_operation.checked_add(1).ok_or(PlanError)?,
                    expired: aged,
                    capacity: segment.capacity,
                });
                break;
            }
            selected_bytes -= segment.capacity;
            record_floor = segment.record_end;
            operation_floor = segment.last_operation.checked_add(1).ok_or(PlanError)?;
            retire.push(segment.id);
        }
        Ok(Self {
            retire,
            record_floor,
            operation_floor,
            selected_bytes,
            roll_active: active.newest_append_millis.is_some()
                && expired(active)
                && active.last_operation <= committed_operation,
            carry,
        })
    }
}

/// The active segment, after `segments` and `maximum_segments` are checked.
fn active(segments: &[Segment], maximum_segments: usize) -> Result<&Segment, PlanError> {
    let active = segments.last().ok_or(PlanError)?;
    if maximum_segments == 0
        || active.sealed
        || segments.iter().any(|segment| segment.capacity == 0)
        || segments.windows(2).any(|pair| {
            matches!(pair, [left, right] if !left.sealed
                || left.id >= right.id
                || left.last_operation > right.last_operation
                || left.record_end > right.record_end)
        })
    {
        return Err(PlanError);
    }
    Ok(active)
}
