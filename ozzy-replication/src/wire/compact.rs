//! Same-channel receipt counters. Opening still verifies full history.

use ozzy_proto::Opcode;

use crate::{
    OpNumber, Prefix,
    flow::{FlowError, Report},
};

use super::WireError;

/// One single-part message, small enough for OMQ's routed inline storage.
pub const COMPACT_STATE_BYTES: usize = 29;

/// An authenticated channel handle plus cumulative receive counters.
/// Handles bind peer, link session, configuration, view, receive epoch and base
/// at full channel opening. They must never be recycled within a link session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompactState {
    /// Nonzero alias issued at full channel opening.
    pub handle: u32,
    /// Monotonic receiver report revision.
    pub revision: u64,
    /// Last retained operation, never a persistence vote.
    pub received: OpNumber,
    /// Cumulative canonical bytes retained within this channel.
    pub received_bytes: u64,
}

impl CompactState {
    /// Copy counters without changing their confirmation or durability meaning.
    pub fn from_report(handle: u32, report: Report) -> Result<Self, WireError> {
        report.validate_shape()?;
        let state = Self {
            handle,
            revision: report.revision,
            received: report.received.op,
            received_bytes: report.received_bytes,
        };
        state.validate()?;
        Ok(state)
    }

    fn validate(self) -> Result<(), WireError> {
        if self.handle == 0 || self.revision == 0 || self.received.0 == u64::MAX {
            return Err(FlowError::Report.into());
        }
        Ok(())
    }

    /// Exact network order: opcode, u32 handle, five u64 counters.
    pub fn encode(self) -> Result<[u8; COMPACT_STATE_BYTES], WireError> {
        self.validate()?;
        let mut bytes = [0; COMPACT_STATE_BYTES];
        bytes[0] = Opcode::ReplicaReceipt as u8;
        bytes[1..5].copy_from_slice(&self.handle.to_be_bytes());
        for (chunk, value) in bytes[5..].as_chunks_mut::<8>().0.iter_mut().zip([
            self.revision,
            self.received.0,
            self.received_bytes,
        ]) {
            chunk.copy_from_slice(&value.to_be_bytes());
        }
        Ok(bytes)
    }

    /// Reject extensions and truncations before interpreting any counters.
    pub fn decode(bytes: &[u8]) -> Result<Self, WireError> {
        if bytes.len() != COMPACT_STATE_BYTES {
            return Err(WireError::Length);
        }
        if bytes[0] != Opcode::ReplicaReceipt as u8 {
            return Err(WireError::UnsupportedCommand);
        }
        let read = |offset| u64::from_be_bytes(bytes[offset..offset + 8].try_into().unwrap());
        let state = Self {
            handle: u32::from_be_bytes(bytes[1..5].try_into().unwrap()),
            revision: read(5),
            received: OpNumber(read(13)),
            received_bytes: read(21),
        };
        state.validate()?;
        Ok(state)
    }

    /// Restore the fixed opening fields and an independently known prefix.
    /// The caller validates handle/session, then feeds this into the ordinary
    /// monotonic sender ledger. Missing history requires a full report.
    pub fn report(self, bound: Report, received: Prefix) -> Result<Report, WireError> {
        self.validate()?;
        if received.op != self.received {
            return Err(WireError::History);
        }
        let report = Report {
            revision: self.revision,
            received,
            received_bytes: self.received_bytes,

            ..bound
        };
        report.validate_shape()?;
        Ok(report)
    }
}
