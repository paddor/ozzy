//! Partition rows of a Partitions reply: replication status, the committed
//! log, and consumer groups with their cursors.

use super::{
    GroupCursor, GroupStatus, PartitionLog, PartitionState, PartitionStatus, count, list,
    read_text, text, values,
};
use crate::{ConsumerGroupId, ConsumerMemberId, GroupId, NodeId, data::CodecError, layout::Reader};
use std::num::NonZeroU64;

const READY: u8 = 1;
const DISK_PENDING: u8 = 2;
const LEADER: u8 = 4;
const LOG: u8 = 8;
const MEMBER: u8 = 1;
const OFFSET: u8 = 2;

pub(super) fn encode_status(out: &mut Vec<u8>, row: &PartitionStatus) -> Result<(), CodecError> {
    let flags = flags(&[
        (row.ready, READY),
        (row.disk_pending, DISK_PENDING),
        (row.leader.is_some(), LEADER),
        (row.log.is_some(), LOG),
    ]);
    out.extend_from_slice(row.group.as_bytes());
    out.push(row.state as u8);
    out.push(flags);
    out.extend_from_slice(&row.view.to_be_bytes());
    out.extend_from_slice(row.leader.unwrap_or(NodeId::from_bytes([0; 16])).as_bytes());
    values(
        out,
        &[
            row.accepted,
            row.committed,
            row.applied,
            row.pending_operations,
            row.pending_body_bytes,
            row.validation_cycles,
            row.reclaimed_bytes,
            row.written,
            row.durable,
            row.leader_commit,
        ],
    );
    count(out, row.followers.len())?;
    for (id, acknowledged) in &row.followers {
        out.extend_from_slice(id.as_bytes());
        out.extend_from_slice(&acknowledged.to_be_bytes());
    }
    if let Some(log) = &row.log {
        encode_log(out, log)?;
    }
    Ok(())
}

fn encode_log(out: &mut Vec<u8>, log: &PartitionLog) -> Result<(), CodecError> {
    out.extend_from_slice(&log.first_offset.to_be_bytes());
    out.extend_from_slice(&log.next_offset.to_be_bytes());
    out.extend_from_slice(&log.producers.to_be_bytes());
    values(
        out,
        &[
            log.retention_age_millis.map_or(0, NonZeroU64::get),
            log.retention_bytes.map_or(0, NonZeroU64::get),
            log.policy_revision,
        ],
    );
    out.extend_from_slice(&log.segments.to_be_bytes());
    values(
        out,
        &[
            log.segment_bytes,
            log.dedup_entries,
            log.dedup_limit.map_or(0, NonZeroU64::get),
        ],
    );
    count(out, log.groups.len())?;
    for group in &log.groups {
        out.extend_from_slice(group.id.as_bytes());
        text(out, &group.name)?;
        out.extend_from_slice(&group.revision.to_be_bytes());
        count(out, group.members.len())?;
        for (member, node) in &group.members {
            out.extend_from_slice(member.as_bytes());
            out.extend_from_slice(node.as_bytes());
        }
    }
    count(out, log.cursors.len())?;
    for cursor in &log.cursors {
        let flags = flags(&[
            (cursor.member.is_some(), MEMBER),
            (cursor.offset.is_some(), OFFSET),
        ]);
        out.extend_from_slice(cursor.group.as_bytes());
        out.push(flags);
        if let Some(member) = cursor.member {
            out.extend_from_slice(member.as_bytes());
        }
        if let Some(offset) = cursor.offset {
            out.extend_from_slice(&offset.to_be_bytes());
        }
    }
    Ok(())
}

pub(super) fn decode_status(reader: &mut Reader<'_>) -> Result<PartitionStatus, CodecError> {
    let group = GroupId::from_bytes(reader.array()?);
    let state = match reader.u8()? {
        0 => PartitionState::Local,
        1 => PartitionState::Normal,
        2 => PartitionState::Electing,
        _ => return Err(CodecError::Profile),
    };
    let flags = reader.u8()?;
    if flags & !(READY | DISK_PENDING | LEADER | LOG) != 0 {
        return Err(CodecError::Profile);
    }
    let view = reader.u64()?;
    let leader = NodeId::from_bytes(reader.array()?);
    Ok(PartitionStatus {
        group,
        state,
        ready: flags & READY != 0,
        disk_pending: flags & DISK_PENDING != 0,
        view,
        leader: (flags & LEADER != 0).then_some(leader),
        accepted: reader.u64()?,
        committed: reader.u64()?,
        applied: reader.u64()?,
        pending_operations: reader.u64()?,
        pending_body_bytes: reader.u64()?,
        validation_cycles: reader.u64()?,
        reclaimed_bytes: reader.u64()?,
        written: reader.u64()?,
        durable: reader.u64()?,
        leader_commit: reader.u64()?,
        followers: list(reader, |reader| {
            Ok((NodeId::from_bytes(reader.array()?), reader.u64()?))
        })?,
        log: if flags & LOG == 0 {
            None
        } else {
            Some(decode_log(reader)?)
        },
    })
}

fn decode_log(reader: &mut Reader<'_>) -> Result<PartitionLog, CodecError> {
    Ok(PartitionLog {
        first_offset: reader.u64()?,
        next_offset: reader.u64()?,
        producers: reader.u32()?,
        retention_age_millis: NonZeroU64::new(reader.u64()?),
        retention_bytes: NonZeroU64::new(reader.u64()?),
        policy_revision: reader.u64()?,
        segments: reader.u32()?,
        segment_bytes: reader.u64()?,
        dedup_entries: reader.u64()?,
        dedup_limit: NonZeroU64::new(reader.u64()?),
        groups: list(reader, |reader| {
            Ok(GroupStatus {
                id: ConsumerGroupId::from_bytes(reader.array()?),
                name: read_text(reader)?,
                revision: reader.u64()?,
                members: list(reader, |reader| {
                    Ok((
                        ConsumerMemberId::from_bytes(reader.array()?),
                        NodeId::from_bytes(reader.array()?),
                    ))
                })?,
            })
        })?,
        cursors: list(reader, |reader| {
            let group = ConsumerGroupId::from_bytes(reader.array()?);
            let flags = reader.u8()?;
            if flags & !(MEMBER | OFFSET) != 0 {
                return Err(CodecError::Profile);
            }
            Ok(GroupCursor {
                group,
                member: if flags & MEMBER == 0 {
                    None
                } else {
                    Some(ConsumerMemberId::from_bytes(reader.array()?))
                },
                offset: if flags & OFFSET == 0 {
                    None
                } else {
                    Some(reader.u64()?)
                },
            })
        })?,
    })
}

/// A flag byte with each `flag` whose condition holds.
fn flags(flags: &[(bool, u8)]) -> u8 {
    flags
        .iter()
        .filter(|(set, _)| *set)
        .fold(0, |flags, (_, flag)| flags | flag)
}
