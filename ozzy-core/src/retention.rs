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
    /// Selected capacities after the proposed prefix retirement.
    pub selected_bytes: u64,
    /// Seal an aged, fully committed active segment before a subsequent plan.
    pub roll_active: bool,
}

/// Invalid summaries, limits, or capacity arithmetic.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("invalid retention segment summaries or budget")]
pub struct PlanError;

impl Plan {
    /// Age and byte targets independently expire the oldest prefix. Newer
    /// timestamps or protected accepted operations stop age-based retirement.
    pub fn select(
        segments: &[Segment],
        policy: RetentionPolicy,
        now: u64,
        committed_operation: u64,
        maximum_segments: usize,
    ) -> Result<Self, PlanError> {
        let active = segments.last().ok_or(PlanError)?;
        if maximum_segments == 0
            || active.sealed
            || segments.iter().any(|segment| segment.capacity == 0)
            || segments.windows(2).any(|pair| {
                !pair[0].sealed
                    || pair[0].id >= pair[1].id
                    || pair[0].last_operation > pair[1].last_operation
                    || pair[0].record_end > pair[1].record_end
            })
        {
            return Err(PlanError);
        }
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
        for segment in segments.iter().take(maximum_segments) {
            if !segment.sealed || segment.last_operation > committed_operation {
                break;
            }
            let oversized = policy
                .max_bytes
                .is_some_and(|limit| selected_bytes > limit.get());
            if !oversized && !expired(segment) {
                break;
            }
            selected_bytes -= segment.capacity;
            record_floor = segment.record_end;
            retire.push(segment.id);
        }
        Ok(Self {
            retire,
            record_floor,
            selected_bytes,
            roll_active: active.newest_append_millis.is_some()
                && expired(active)
                && active.last_operation <= committed_operation,
        })
    }
}
