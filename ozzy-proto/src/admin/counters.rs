//! Event counter values of a Counters reply.

use super::{Counters, EventKind, count, list, read_text, text, values};
use crate::{data::CodecError, layout::Reader};

pub(super) fn encode_counters(out: &mut Vec<u8>, counters: &Counters) -> Result<(), CodecError> {
    let events = counters.events.len();
    if counters.totals.len() != events
        || counters
            .threads
            .iter()
            .any(|(_, values)| values.len() != events)
    {
        return Err(CodecError::Length);
    }
    out.extend_from_slice(&counters.uptime_ms.to_be_bytes());
    count(out, events)?;
    for (name, kind) in &counters.events {
        text(out, name)?;
        out.push(*kind as u8);
    }
    values(out, &counters.totals);
    count(out, counters.threads.len())?;
    for (name, thread) in &counters.threads {
        text(out, name)?;
        values(out, thread);
    }
    Ok(())
}

pub(super) fn decode_counters(reader: &mut Reader<'_>) -> Result<Counters, CodecError> {
    let uptime_ms = reader.u64()?;
    let events = list(reader, |reader| {
        let name = read_text(reader)?;
        let kind = match reader.u8()? {
            0 => EventKind::Count,
            1 => EventKind::Peak,
            2 => EventKind::Gauge,
            _ => return Err(CodecError::Profile),
        };
        Ok((name, kind))
    })?;
    let read_values = |reader: &mut Reader<'_>| {
        if reader.rest().len() / 8 < events.len() {
            return Err(CodecError::Length);
        }
        (0..events.len())
            .map(|_| Ok(reader.u64()?))
            .collect::<Result<Vec<_>, CodecError>>()
    };
    let totals = read_values(reader)?;
    let threads = list(reader, |reader| {
        Ok((read_text(reader)?, read_values(reader)?))
    })?;
    Ok(Counters {
        uptime_ms,
        events,
        totals,
        threads,
    })
}
