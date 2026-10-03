//! Bounded byte cursors shared by body encoding and borrowed views.

use crate::operation::{OperationCodecError, OperationLimits, OperationOutput};

pub(in crate::operation) fn validate_position_count(
    first: u64,
    count: usize,
) -> Result<(), OperationCodecError> {
    let delta = u64::try_from(count - 1).map_err(|_| OperationCodecError::LengthOverflow)?;
    first
        .checked_add(delta)
        .ok_or(OperationCodecError::AppendPositionOverflow)?;
    Ok(())
}

pub(in crate::operation) fn validate_name(
    kind: &'static str,
    value: &str,
    max: usize,
) -> Result<(), OperationCodecError> {
    if value.is_empty() {
        return Err(OperationCodecError::EmptyValue(kind));
    }
    enforce_limit(kind, value.len(), max)
}

pub(in crate::operation) fn validate_limits(
    limits: OperationLimits,
) -> Result<(), OperationCodecError> {
    for (kind, value) in [
        ("operation body bytes", limits.max_body_bytes),
        ("name bytes", limits.max_name_bytes),
        ("append batch count", limits.max_append_batches),
        ("append record count", limits.max_records),
        ("append part count", limits.max_parts),
        ("append payload bytes", limits.max_payload_bytes),
    ] {
        if value == 0 {
            return Err(OperationCodecError::LimitExceeded {
                kind,
                actual: 0,
                limit: 0,
            });
        }
    }
    Ok(())
}

pub(in crate::operation) fn enforce_limit(
    kind: &'static str,
    actual: usize,
    limit: usize,
) -> Result<(), OperationCodecError> {
    if actual > limit {
        Err(OperationCodecError::LimitExceeded {
            kind,
            actual,
            limit,
        })
    } else {
        Ok(())
    }
}

#[derive(Debug)]
pub(in crate::operation) struct Encoder<'a, O: OperationOutput> {
    output: &'a mut O,
    pub(in crate::operation) start: usize,
    limit: usize,
}

impl<'a, O: OperationOutput> Encoder<'a, O> {
    pub(in crate::operation) fn new(output: &'a mut O, start: usize, limit: usize) -> Self {
        Self {
            output,
            start,
            limit,
        }
    }

    pub(in crate::operation) fn finish(self) -> usize {
        self.output.len()
    }

    pub(in crate::operation) fn restore_start(&mut self) {
        self.output.truncate(self.start);
    }

    // Metadata fields have compile-time lengths. Keep those lengths visible to
    // the output fast path instead of making an out-of-line memcpy per field.
    #[inline]
    pub(in crate::operation) fn bytes(&mut self, bytes: &[u8]) -> Result<(), OperationCodecError> {
        self.check_length(bytes.len())?;
        self.output.extend(bytes)
    }

    pub(in crate::operation) fn payload(
        &mut self,
        bytes: &[u8],
    ) -> Result<(), OperationCodecError> {
        self.check_length(bytes.len())?;
        self.output.payload(bytes)
    }

    pub(in crate::operation) fn check_length(
        &self,
        additional: usize,
    ) -> Result<(), OperationCodecError> {
        let new_len = self
            .output
            .len()
            .checked_add(additional)
            .ok_or(OperationCodecError::LengthOverflow)?;
        let body_len = new_len
            .checked_sub(self.start)
            .ok_or(OperationCodecError::LengthOverflow)?;
        enforce_limit("operation body bytes", body_len, self.limit)?;
        Ok(())
    }

    pub(in crate::operation) fn u8(&mut self, value: u8) -> Result<(), OperationCodecError> {
        self.bytes(&[value])
    }

    pub(in crate::operation) fn u16(&mut self, value: u16) -> Result<(), OperationCodecError> {
        self.bytes(&value.to_be_bytes())
    }

    pub(in crate::operation) fn u32(&mut self, value: u32) -> Result<(), OperationCodecError> {
        self.bytes(&value.to_be_bytes())
    }

    pub(in crate::operation) fn u64(&mut self, value: u64) -> Result<(), OperationCodecError> {
        self.bytes(&value.to_be_bytes())
    }

    pub(in crate::operation) fn count(&mut self, value: usize) -> Result<(), OperationCodecError> {
        self.u32(u32::try_from(value).map_err(|_| OperationCodecError::LengthOverflow)?)
    }

    pub(in crate::operation) fn length(&mut self, value: usize) -> Result<(), OperationCodecError> {
        self.count(value)
    }

    pub(in crate::operation) fn id(&mut self, value: &[u8; 16]) -> Result<(), OperationCodecError> {
        self.bytes(value)
    }

    pub(in crate::operation) fn text(&mut self, value: &str) -> Result<(), OperationCodecError> {
        self.length(value.len())?;
        self.bytes(value.as_bytes())
    }

    pub(in crate::operation) fn optional_u64(
        &mut self,
        value: Option<u64>,
    ) -> Result<(), OperationCodecError> {
        match value {
            None => self.u8(0),
            Some(value) => {
                self.u8(1)?;
                self.u64(value)
            }
        }
    }

    pub(in crate::operation) fn optional_id(
        &mut self,
        value: Option<[u8; 16]>,
    ) -> Result<(), OperationCodecError> {
        match value {
            None => self.u8(0),
            Some(value) => {
                self.u8(1)?;
                self.id(&value)
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(in crate::operation) struct Decoder<'a> {
    pub(in crate::operation) input: &'a [u8],
    pub(in crate::operation) offset: usize,
}

impl<'a> Decoder<'a> {
    pub(in crate::operation) const fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    pub(in crate::operation) fn finish(self) -> Result<(), OperationCodecError> {
        let remaining = self.input.len() - self.offset;
        if remaining == 0 {
            Ok(())
        } else {
            Err(OperationCodecError::TrailingBytes(remaining))
        }
    }

    pub(in crate::operation) fn take(
        &mut self,
        length: usize,
    ) -> Result<&'a [u8], OperationCodecError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(OperationCodecError::LengthOverflow)?;
        let Some(bytes) = self.input.get(self.offset..end) else {
            return Err(OperationCodecError::Truncated {
                needed: end,
                available: self.input.len(),
            });
        };
        self.offset = end;
        Ok(bytes)
    }

    pub(in crate::operation) fn u8(&mut self) -> Result<u8, OperationCodecError> {
        Ok(self.take(1)?[0])
    }

    pub(in crate::operation) fn u16(&mut self) -> Result<u16, OperationCodecError> {
        let bytes = self.take(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    pub(in crate::operation) fn u32(&mut self) -> Result<u32, OperationCodecError> {
        let bytes = self.take(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    pub(in crate::operation) fn u64(&mut self) -> Result<u64, OperationCodecError> {
        let bytes = self.take(8)?;
        Ok(u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    pub(in crate::operation) fn id(&mut self) -> Result<[u8; 16], OperationCodecError> {
        let mut id = [0; 16];
        id.copy_from_slice(self.take(16)?);
        Ok(id)
    }

    pub(in crate::operation) fn count(
        &mut self,
        kind: &'static str,
        limit: usize,
    ) -> Result<usize, OperationCodecError> {
        let count = self.u32()? as usize;
        enforce_limit(kind, count, limit)?;
        Ok(count)
    }

    pub(in crate::operation) fn text(
        &mut self,
        kind: &'static str,
        limit: usize,
    ) -> Result<&'a str, OperationCodecError> {
        let length = self.count(kind, limit)?;
        let bytes = self.take(length)?;
        let value =
            std::str::from_utf8(bytes).map_err(|_| OperationCodecError::InvalidUtf8(kind))?;
        validate_name(kind, value, limit)?;
        Ok(value)
    }

    pub(in crate::operation) fn optional_u64(
        &mut self,
    ) -> Result<Option<u64>, OperationCodecError> {
        match self.u8()? {
            0 => Ok(None),
            1 => self.u64().map(Some),
            tag => Err(OperationCodecError::InvalidOptionTag(tag)),
        }
    }

    pub(in crate::operation) fn optional_id(
        &mut self,
    ) -> Result<Option<[u8; 16]>, OperationCodecError> {
        match self.u8()? {
            0 => Ok(None),
            1 => self.id().map(Some),
            tag => Err(OperationCodecError::InvalidOptionTag(tag)),
        }
    }
}
