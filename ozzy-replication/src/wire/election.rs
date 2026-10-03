//! Fixed-size election evidence. Full-WAL transfer is a separate exchange.

use super::{Reader, WireError, Writer, validate_election_view, validate_prefix};
use crate::{DoViewChange, FrozenLog, JournalGeneration, Prefix, Scope, StartView};

pub(super) fn validate_report(message: DoViewChange) -> Result<(), WireError> {
    validate_election_view(message.scope)?;
    validate_history(
        message.generation,
        message.log.accepted,
        message.log.committed,
    )?;
    if message.log.last_normal_view >= message.scope.view {
        return Err(WireError::History);
    }
    Ok(())
}

fn validate_history(
    generation: JournalGeneration,
    accepted: Prefix,
    committed: Prefix,
) -> Result<(), WireError> {
    validate_prefix(accepted)?;
    validate_prefix(committed)?;
    if generation.0 == 0
        || committed.op > accepted.op
        || (committed.op == accepted.op && committed != accepted)
    {
        return Err(WireError::History);
    }
    Ok(())
}

pub(super) fn write_report(writer: &mut Writer<'_>, message: DoViewChange) {
    writer.bytes(&message.generation.0.to_be_bytes());
    writer.u64(message.log.last_normal_view);
    writer.prefix(message.log.accepted);
    writer.prefix(message.log.committed);
}

pub(super) fn read_report(
    scope: Scope,
    reader: &mut Reader<'_>,
) -> Result<DoViewChange, WireError> {
    let message = DoViewChange {
        scope,
        generation: JournalGeneration(u128::from_be_bytes(reader.bytes()?)),
        log: FrozenLog {
            last_normal_view: reader.u64()?,
            accepted: reader.prefix()?,
            committed: reader.prefix()?,
        },
    };
    validate_report(message)?;
    Ok(message)
}

pub(super) fn validate_start(message: StartView) -> Result<(), WireError> {
    validate_election_view(message.scope)?;
    validate_history(message.generation, message.accepted, message.committed)
}

pub(super) fn write_start(writer: &mut Writer<'_>, message: StartView) {
    writer.bytes(&message.generation.0.to_be_bytes());
    writer.prefix(message.accepted);
    writer.prefix(message.committed);
}

pub(super) fn read_start(scope: Scope, reader: &mut Reader<'_>) -> Result<StartView, WireError> {
    let message = StartView {
        scope,
        generation: JournalGeneration(u128::from_be_bytes(reader.bytes()?)),
        accepted: reader.prefix()?,
        committed: reader.prefix()?,
    };
    validate_start(message)?;
    Ok(message)
}
