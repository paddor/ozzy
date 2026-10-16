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
        ("group members", limits.max_group_members),
        ("group source partitions", limits.max_group_partitions),
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

/// Encode one body into `output` atomically: an error truncates the output
/// back to its previous length. Returns the encoded range.
pub(in crate::operation) fn encode_atomic<O: OperationOutput>(
    output: &mut O,
    limits: OperationLimits,
    encode: impl FnOnce(&mut Encoder<'_, O>) -> Result<(), OperationCodecError>,
) -> Result<std::ops::Range<usize>, OperationCodecError> {
    validate_limits(limits)?;
    let start = output.len();
    let mut encoder = Encoder::new(output, start, limits.max_body_bytes);
    if let Err(error) = encode(&mut encoder) {
        encoder.restore_start();
        return Err(error);
    }
    Ok(start..encoder.finish())
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

pub(in crate::operation) use ozzy_proto::layout::Reader;

/// Canonical body fields over the shared layout reader.
pub(in crate::operation) trait ReadFields<'a> {
    fn id(&mut self) -> Result<[u8; 16], OperationCodecError>;
    fn count(&mut self, kind: &'static str, limit: usize) -> Result<usize, OperationCodecError>;
    fn text(&mut self, kind: &'static str, limit: usize) -> Result<&'a str, OperationCodecError>;
    fn optional_u64(&mut self) -> Result<Option<u64>, OperationCodecError>;
    fn optional_id(&mut self) -> Result<Option<[u8; 16]>, OperationCodecError>;
}

impl<'a> ReadFields<'a> for Reader<'a> {
    fn id(&mut self) -> Result<[u8; 16], OperationCodecError> {
        Ok(self.array()?)
    }

    fn count(&mut self, kind: &'static str, limit: usize) -> Result<usize, OperationCodecError> {
        let count = self.u32()? as usize;
        enforce_limit(kind, count, limit)?;
        Ok(count)
    }

    fn text(&mut self, kind: &'static str, limit: usize) -> Result<&'a str, OperationCodecError> {
        let length = self.count(kind, limit)?;
        let bytes = self.take(length)?;
        let value =
            std::str::from_utf8(bytes).map_err(|_| OperationCodecError::InvalidUtf8(kind))?;
        validate_name(kind, value, limit)?;
        Ok(value)
    }

    fn optional_u64(&mut self) -> Result<Option<u64>, OperationCodecError> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.u64()?)),
            tag => Err(OperationCodecError::InvalidOptionTag(tag)),
        }
    }

    fn optional_id(&mut self) -> Result<Option<[u8; 16]>, OperationCodecError> {
        match self.u8()? {
            0 => Ok(None),
            1 => self.id().map(Some),
            tag => Err(OperationCodecError::InvalidOptionTag(tag)),
        }
    }
}
