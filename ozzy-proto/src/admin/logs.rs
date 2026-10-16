//! Recent diagnostics of a Logs reply.

use super::{LogEntry, LogLevel, count, list, read_text, text};
use crate::{data::CodecError, layout::Reader};

pub(super) fn encode_logs(out: &mut Vec<u8>, entries: &[LogEntry]) -> Result<(), CodecError> {
    count(out, entries.len())?;
    for entry in entries {
        out.extend_from_slice(&entry.uptime_ms.to_be_bytes());
        out.push(entry.level as u8);
        text(out, &entry.thread)?;
        text(out, &entry.message)?;
    }
    Ok(())
}

pub(super) fn decode_logs(reader: &mut Reader<'_>) -> Result<Vec<LogEntry>, CodecError> {
    list(reader, |reader| {
        Ok(LogEntry {
            uptime_ms: reader.u64()?,
            level: match reader.u8()? {
                0 => LogLevel::Info,
                1 => LogLevel::Warn,
                2 => LogLevel::Error,
                _ => return Err(CodecError::Profile),
            },
            thread: read_text(reader)?,
            message: read_text(reader)?,
        })
    })
}
