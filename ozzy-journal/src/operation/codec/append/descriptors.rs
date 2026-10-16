//! Descriptor tables of APPEND batches: tiny-record and general validation.

use super::{
    OperationCodecError, OperationLimits, ReadFields, Reader, RecordPosition, enforce_limit,
};

/// A tiny-record descriptor table: 16-byte IDs, then one length byte per record.
#[derive(Debug, Clone, Copy)]
pub(in crate::operation) struct TinyTable<'a> {
    ids: &'a [u8],
    lengths: &'a [u8],
}

impl<'a> TinyTable<'a> {
    /// Both columns, checked to hold exactly `count` records.
    pub(in crate::operation) fn new(
        ids: &'a [u8],
        lengths: &'a [u8],
        count: usize,
    ) -> Result<Self, OperationCodecError> {
        if ids.len()
            != count
                .checked_mul(16)
                .ok_or(OperationCodecError::LengthOverflow)?
            || lengths.len() != count
        {
            return Err(OperationCodecError::LengthOverflow);
        }
        Ok(Self { ids, lengths })
    }
}

pub(in crate::operation) fn validate_tiny_descriptors(
    table: TinyTable<'_>,
    totals: &mut AppendTotals,
    mut positions: Option<&mut Vec<RecordPosition>>,
) -> Result<(usize, bool), OperationCodecError> {
    let TinyTable { ids, lengths } = table;
    totals.add_parts(lengths.len())?;
    let mut bytes = 0_usize;
    for (index, &len) in lengths.iter().enumerate() {
        if let Some(positions) = positions.as_deref_mut() {
            positions.push(position(index * 16, bytes, 1)?);
        }
        bytes = bytes
            .checked_add(len as usize)
            .ok_or(OperationCodecError::LengthOverflow)?;
    }
    if let Some(positions) = positions {
        positions.push(position(ids.len(), bytes, 0)?);
    }
    totals.add_payload(bytes)?;
    Ok((
        bytes,
        ids.as_chunks::<16>().0.iter().all(|id| id != &[0; 16]),
    ))
}

fn position(
    descriptor: usize,
    payload: usize,
    parts: usize,
) -> Result<RecordPosition, OperationCodecError> {
    Ok(RecordPosition {
        descriptor: u32::try_from(descriptor).map_err(|_| OperationCodecError::LengthOverflow)?,
        payload: u32::try_from(payload).map_err(|_| OperationCodecError::LengthOverflow)?,
        parts: u32::try_from(parts).map_err(|_| OperationCodecError::LengthOverflow)?,
    })
}

/// Identity, payload encoding, and part count opening a general descriptor.
pub(in crate::operation) struct DescriptorHead {
    pub(in crate::operation) id: [u8; 16],
    pub(in crate::operation) encoding: ozzy_proto::data::Encoding,
    pub(in crate::operation) parts: usize,
}

pub(in crate::operation) fn read_descriptor_head(
    decoder: &mut Reader<'_>,
) -> Result<DescriptorHead, OperationCodecError> {
    let id = decoder.id()?;
    let tagged = decoder.u32()?;
    let encoding = match tagged >> 24 {
        0 => ozzy_proto::data::Encoding::Raw,
        1 => ozzy_proto::data::Encoding::Lz4 {
            decoded_bytes: decoder.u32()?,
        },
        _ => return Err(OperationCodecError::EmptyRecordParts),
    };
    Ok(DescriptorHead {
        id,
        encoding,
        parts: (tagged & 0x00ff_ffff) as usize,
    })
}

pub(in crate::operation) fn validate_append_descriptors(
    decoder: &mut Reader<'_>,
    record_count: usize,
    totals: &mut AppendTotals,
    mut positions: Option<&mut Vec<RecordPosition>>,
) -> Result<(usize, bool, bool, Option<usize>), OperationCodecError> {
    let mut batch_payload_bytes = 0_usize;
    let mut nonzero_message_ids = true;
    let mut all_raw = true;
    let mut uniform_part_bytes = None;
    let limits = totals.limits();
    let table_start = decoder.offset();
    for index in 0..record_count {
        let descriptor_start = decoder.offset() - table_start;
        let payload_start = batch_payload_bytes;
        let DescriptorHead {
            id,
            encoding,
            parts: part_count,
        } = read_descriptor_head(decoder)?;
        nonzero_message_ids &= id != [0; 16];
        all_raw &= encoding == ozzy_proto::data::Encoding::Raw;
        if !encoding.validate(part_count, limits.max_parts, limits.max_payload_bytes) {
            return Err(OperationCodecError::EmptyRecordParts);
        }
        if part_count == 0 {
            return Err(OperationCodecError::EmptyRecordParts);
        }
        totals.add_parts(part_count)?;
        for _ in 0..part_count {
            let length = decoder.u32()? as usize;
            totals.add_payload(length)?;
            batch_payload_bytes = batch_payload_bytes
                .checked_add(length)
                .ok_or(OperationCodecError::LengthOverflow)?;
        }
        let single = (part_count == 1 && encoding == ozzy_proto::data::Encoding::Raw)
            .then_some(batch_payload_bytes - payload_start);
        if index == 0 {
            uniform_part_bytes = single;
        } else if uniform_part_bytes != single {
            uniform_part_bytes = None;
        }
        if let Some(positions) = positions.as_deref_mut() {
            positions.push(position(descriptor_start, payload_start, part_count)?);
        }
    }
    if let Some(positions) = positions {
        positions.push(position(
            decoder.offset() - table_start,
            batch_payload_bytes,
            0,
        )?);
    }
    Ok((
        batch_payload_bytes,
        nonzero_message_ids,
        all_raw,
        uniform_part_bytes,
    ))
}

/// Running totals of one APPEND, checked against the limits of its operation.
#[derive(Debug)]
pub(in crate::operation) struct AppendTotals {
    limits: OperationLimits,
    records: usize,
    parts: usize,
    pub(in crate::operation) payload_bytes: usize,
}

impl AppendTotals {
    pub(in crate::operation) const fn new(limits: OperationLimits) -> Self {
        Self {
            limits,
            records: 0,
            parts: 0,
            payload_bytes: 0,
        }
    }

    pub(in crate::operation) const fn limits(&self) -> OperationLimits {
        self.limits
    }

    pub(in crate::operation) fn add_records(
        &mut self,
        count: usize,
    ) -> Result<(), OperationCodecError> {
        self.records = self
            .records
            .checked_add(count)
            .ok_or(OperationCodecError::LengthOverflow)?;
        enforce_limit("append record count", self.records, self.limits.max_records)
    }

    pub(in crate::operation) fn add_parts(
        &mut self,
        count: usize,
    ) -> Result<(), OperationCodecError> {
        self.parts = self
            .parts
            .checked_add(count)
            .ok_or(OperationCodecError::LengthOverflow)?;
        enforce_limit("append part count", self.parts, self.limits.max_parts)
    }

    pub(in crate::operation) fn add_payload(
        &mut self,
        bytes: usize,
    ) -> Result<(), OperationCodecError> {
        self.payload_bytes = self
            .payload_bytes
            .checked_add(bytes)
            .ok_or(OperationCodecError::LengthOverflow)?;
        enforce_limit(
            "append payload bytes",
            self.payload_bytes,
            self.limits.max_payload_bytes,
        )
    }
}
