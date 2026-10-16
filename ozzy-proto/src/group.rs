//! Consumer-group membership, coordinator plans, and source activation on the
//! addressed control plane. Membership replies describe committed intent;
//! sources confirm activation separately.

use crate::{
    ConsumerGroupId, ConsumerMemberId, ENVELOPE_BYTES, Envelope, EnvelopeLimits, Offset, Opcode,
    OperationId, Packet, PartitionIncarnation,
    append::{Authority, Policy},
    data::CodecError,
    layout::Reader,
    wire::{check, command, decode_authority, encode_authority, nonzero, reserve},
};

/// Absolute member bound of one group.
pub const MAX_MEMBERS: usize = 256;
/// Absolute source bound of one group. Every reply that lists one member's
/// sources, and every owned-source report, must fit one bounded control frame.
pub const MAX_PARTITIONS: usize = 1024;
/// Maximum topic-scoped group name bytes.
pub const MAX_NAME_BYTES: usize = 255;
/// Fixed join metadata before the name and owned-source entries.
pub const JOIN_FIXED_BYTES: usize = 112;
/// Bytes per owned-source entry in a join request.
pub const OWNED_BYTES: usize = 32;
/// Fixed membership reply metadata before the target entries.
pub const JOINED_FIXED_BYTES: usize = 78;
/// Bytes per target entry in a membership reply.
pub const TARGET_BYTES: usize = 45;
/// Fixed activation request metadata.
pub const ASSIGN_BYTES: usize = 99;
/// Fixed activation reply metadata.
pub const ASSIGNED_BYTES: usize = 122;
const SOURCE_TAG: u8 = 1;

/// First position of a group that has no persisted cursor for a source yet.
/// Existing cursors always win; this selector never resets progress.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Start {
    /// Earliest retained record of each source when it is first assigned.
    #[default]
    Earliest,
    /// Confirmed end of each source when it is first assigned.
    Latest,
    /// Exact offset applied to every source; it must be retained and confirmed.
    Offset(Offset),
}

impl Start {
    /// Fixed nine-byte encoding: tag, then the offset or zero.
    pub const BYTES: usize = 9;

    /// The wire tag and value: the offset for `Offset`, zero otherwise.
    pub const fn to_wire(self) -> (u8, u64) {
        match self {
            Self::Earliest => (0, 0),
            Self::Latest => (1, 0),
            Self::Offset(offset) => (2, offset.get()),
        }
    }

    /// The selector a wire tag and value name, if they are valid.
    pub const fn from_wire(tag: u8, value: u64) -> Option<Self> {
        match (tag, value) {
            (0, 0) => Some(Self::Earliest),
            (1, 0) => Some(Self::Latest),
            (2, value) => Some(Self::Offset(Offset::new(value))),
            _ => None,
        }
    }

    fn encode(self, output: &mut Vec<u8>) {
        let (tag, value) = self.to_wire();
        output.push(tag);
        output.extend_from_slice(&value.to_be_bytes());
    }

    fn decode(cursor: &mut Reader<'_>) -> Result<Self, CodecError> {
        let tag = cursor.u8()?;
        let value = cursor.u64()?;
        Self::from_wire(tag, value).ok_or(CodecError::Profile)
    }
}

/// Membership intent carried by one request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Mode {
    /// Keep an existing membership alive and report owned sources.
    Heartbeat = 0,
    /// Join with a fresh join identity, creating the group if absent.
    Join = 1,
    /// Leave cooperatively; owned sources move in the next plan.
    Leave = 2,
}

/// One source this instance currently processes, as activated by its owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Owned {
    /// Source partition incarnation.
    pub partition: PartitionIncarnation,
    /// Plan revision that activated this ownership.
    pub plan_revision: u64,
    /// Epoch allocated by the source.
    pub epoch: u64,
}

/// Join, heartbeat, or leave, addressed to the group's coordinator leader.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JoinRequest {
    /// Expected coordinator leader generation.
    pub authority: Authority,
    /// Deterministic coordinator partition of this topic and group name.
    pub coordinator: PartitionIncarnation,
    /// Topic-scoped group name.
    pub name: String,
    /// Application member identity, stable across restarts.
    pub member: ConsumerMemberId,
    /// Fresh identity of this processing instance.
    pub join: OperationId,
    /// Incarnation this instance belongs to, once known. A request naming a
    /// retired incarnation is told so instead of creating a new one.
    pub group: Option<ConsumerGroupId>,
    /// Membership intent.
    pub mode: Mode,
    /// Creation selector, used only when this request creates the group.
    pub initial: Start,
    /// Sources this instance currently processes or drains.
    pub owned: Vec<Owned>,
}

/// Membership status in a reply.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Status {
    /// The committed plan contains this join; targets follow.
    Member = 0,
    /// The group or this join is not committed yet; keep heartbeating.
    Joining = 1,
    /// No such group; a join request creates it.
    Unknown = 2,
    /// The requested incarnation was deleted; this instance must stop.
    Deleted = 3,
}

/// Committed intent for one of the requesting member's sources.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Target {
    /// Source partition incarnation.
    pub partition: PartitionIncarnation,
    /// Numeric topic partition.
    pub number: u32,
    /// Plan revision to relay to the source.
    pub plan_revision: u64,
    /// The prior owner drained, left, or exceeded its deadline.
    pub activatable: bool,
    /// Instance intended by the previous plan, if any.
    pub prior_member: Option<ConsumerMemberId>,
}

/// Committed membership view for one instance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JoinReply {
    /// Coordinator leader generation that served this reply.
    pub authority: Authority,
    /// Coordinator partition incarnation.
    pub coordinator: PartitionIncarnation,
    /// Group incarnation; zero only while the group is still being created.
    pub group: ConsumerGroupId,
    /// Committed coordinator revision; zero without a committed group.
    pub revision: u64,
    /// Membership status of the requesting join.
    pub status: Status,
    /// Sources intended for this instance, in numeric partition order.
    pub targets: Vec<Target>,
}

/// Relay one committed intent to its source leader.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AssignRequest {
    /// Expected source leader generation.
    pub authority: Authority,
    /// Source partition incarnation.
    pub partition: PartitionIncarnation,
    /// Group incarnation.
    pub group: ConsumerGroupId,
    /// Activating member, or none to revoke the source. Deletion revokes at
    /// the maximum revision, fencing every plan of the incarnation.
    pub member: Option<ConsumerMemberId>,
    /// Committed plan revision.
    pub plan_revision: u64,
    /// Group creation selector, resolved only on a source without progress.
    pub initial: Start,
    /// Required immutable source confirmation boundary.
    pub policy: Policy,
}

/// Activation coordinates after the source's confirmation boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AssignReply {
    /// Confirming source leader generation.
    pub authority: Authority,
    /// Source partition incarnation.
    pub partition: PartitionIncarnation,
    /// Group incarnation.
    pub group: ConsumerGroupId,
    /// Activated member, or none after a revocation.
    pub member: Option<ConsumerMemberId>,
    /// Epoch allocated by the source for this ownership.
    pub epoch: u64,
    /// Plan revision confirmed at the source.
    pub plan_revision: u64,
    /// Exclusive cursor captured with the assignment.
    pub cursor: Offset,
    /// Earliest retained record at validation.
    pub retained_from: Offset,
    /// Exclusive confirmed record end at validation.
    pub end: Offset,
    /// Configured confirmation policy.
    pub policy: Policy,
}

/// Native code-7 detail for a creation offset outside confirmed retained history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InitialPosition {
    /// Requested first cursor.
    pub cursor: Offset,
    /// Confirmed earliest retained offset.
    pub retained_from: Offset,
    /// Confirmed exclusive record end.
    pub end: Offset,
}

impl InitialPosition {
    /// Encode cursor, retained floor, and end, in that order.
    pub fn encode(self) -> Result<[u8; 24], CodecError> {
        self.validate()?;
        let mut bytes = [0; 24];
        bytes[..8].copy_from_slice(&self.cursor.get().to_be_bytes());
        bytes[8..16].copy_from_slice(&self.retained_from.get().to_be_bytes());
        bytes[16..].copy_from_slice(&self.end.get().to_be_bytes());
        Ok(bytes)
    }

    /// Decode a strict detail; unrelated NACKs use other details.
    pub fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        if bytes.len() != 24 {
            return Err(CodecError::Length);
        }
        let mut cursor = Reader::new(bytes);
        let detail = Self {
            cursor: Offset::new(cursor.u64()?),
            retained_from: Offset::new(cursor.u64()?),
            end: Offset::new(cursor.u64()?),
        };
        detail.validate()?;
        Ok(detail)
    }

    fn validate(self) -> Result<(), CodecError> {
        if self.retained_from > self.end || (self.retained_from..=self.end).contains(&self.cursor) {
            return Err(CodecError::Profile);
        }
        Ok(())
    }
}

fn member_bytes(member: Option<ConsumerMemberId>) -> [u8; 16] {
    member.map_or([0; 16], |member| *member.as_bytes())
}

fn decode_member(cursor: &mut Reader<'_>) -> Result<Option<ConsumerMemberId>, CodecError> {
    let member = cursor.array::<16>()?;
    Ok((member != [0; 16]).then(|| ConsumerMemberId::from_bytes(member)))
}

fn validate_scope(authority: Authority, partition: PartitionIncarnation) -> Result<(), CodecError> {
    nonzero(authority.group_id.as_bytes())?;
    nonzero(partition.as_bytes())?;
    if authority.config_epoch == 0 {
        return Err(CodecError::Identity);
    }
    Ok(())
}

mod admin;
mod assign;
mod join;
pub use admin::*;
pub use assign::*;
pub use join::*;

#[cfg(test)]
mod tests;
