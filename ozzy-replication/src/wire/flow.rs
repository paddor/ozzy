//! Exact receipt/probe metadata. These messages never enter the voting core.

use ozzy_proto::{Envelope, Opcode};
use ozzy_proto::{LinkSessionId, NodeId, RequestId};

use super::{
    COMMON_BYTES, ControlEncoding, PrepareBatch, Reader, WireError, WireLimits, Writer,
    validate_prefix, validate_scope,
};
use crate::flow::{Channel, ReceiveEpoch, Report};
use crate::{OpNumber, Scope};

pub use crate::flow::Probe as FlowProbe;

const PROBE_BYTES: usize = COMMON_BYTES + 40 + 8 + 8;
const STATE_BYTES: usize = COMMON_BYTES + 16 + 8 + 40 + 40 + 8 + 8 + 8 + 8 + 4;

fn validate_probe(probe: FlowProbe) -> Result<(), WireError> {
    validate_scope(probe.scope)?;
    validate_prefix(probe.tail)?;
    if probe.available < probe.tail.op
        || probe.available.0 == u64::MAX
        || probe.minimum_body_bytes == 0
    {
        return Err(WireError::Prefix);
    }
    if probe.request_id.as_bytes() == &[0; 16] {
        return Err(WireError::Correlation);
    }
    Ok(())
}

/// Correlated status response or coalesced same-epoch receipt/credit notification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlowState {
    /// Session-bound compact routing handle. Never reused during this process.
    pub handle: u32,
    /// Present for a probe response; absent for an unsolicited same-epoch update.
    pub request_id: Option<RequestId>,
    /// Fixed-size, structurally checked accounting; exact history validation remains.
    pub report: Report,
    /// Repair ceiling: a held PUB predecessor or current receipt during live progress.
    /// None permits normal catch-up through the correlated probe's tail.
    pub repair_limit: Option<OpNumber>,
}

impl FlowState {
    fn validate(self) -> Result<(), WireError> {
        self.report.validate_shape()?;
        if self.handle == 0 {
            return Err(WireError::Correlation);
        }
        if self.repair_limit == Some(OpNumber(u64::MAX)) {
            return Err(WireError::Prefix);
        }
        validate_prefix(self.report.base)?;
        validate_prefix(self.report.received)?;
        if self.request_id.is_some_and(|id| id.as_bytes() == &[0; 16]) {
            return Err(WireError::Correlation);
        }
        Ok(())
    }

    /// Match the currently outstanding probe before considering an epoch change.
    ///
    /// The adapter authenticates peer/session, invalidates retired probes, verifies
    /// reported history, and applies its negotiated credit bounds separately. This
    /// comparison alone never supplies a vote, disk evidence, or recovery authority.
    pub fn validate_response(self, probe: FlowProbe) -> Result<(), WireError> {
        self.validate()?;
        validate_probe(probe)?;
        if self.request_id != Some(probe.request_id) || self.report.channel.scope != probe.scope {
            return Err(WireError::Correlation);
        }
        Ok(())
    }
}

/// Separately typed nonvoting flow family, sharing the ordinary replica envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowMessage<'a> {
    /// Correlated request for receipt/credit state.
    Probe(FlowProbe),
    /// Volatile retention and absolute credits, not durability.
    State(FlowState),
    /// Hash-validated normal data in the locally required receive epoch.
    Prepare {
        /// Receiver incarnation checked before body decoding/hashing.
        epoch: ReceiveEpoch,
        /// Borrowed canonical operations; application validation remains required.
        batch: PrepareBatch<'a>,
    },
}

/// Encode `REPLICA_OPEN` into caller-owned metadata, leaving output unchanged on error.
pub fn encode_flow_probe(
    sender: NodeId,
    session: LinkSessionId,
    probe: FlowProbe,
    output: &mut [u8],
    limits: WireLimits,
) -> Result<ControlEncoding, WireError> {
    validate_probe(probe)?;
    let header = Envelope {
        opcode: Opcode::ReplicaOpen,
        response: false,
        request_id: Some(probe.request_id),
        sender,
        session: Some(session),
    }
    .encode_header(PROBE_BYTES, 0, limits.envelope)?;
    if output.len() < PROBE_BYTES {
        return Err(WireError::Capacity);
    }
    let mut writer = Writer::new(&mut output[..PROBE_BYTES]);
    writer.scope(probe.scope, sender);
    writer.prefix(probe.tail);
    writer.u64(probe.available.0);
    writer.u64(probe.minimum_body_bytes);
    Ok(ControlEncoding {
        header,
        metadata_bytes: PROBE_BYTES,
    })
}

/// Encode a `REPLICA_STATE` response/notification without allocating or granting authority.
/// The advertised credit window is independent of the maximum size of one wire frame.
pub fn encode_flow_state(
    sender: NodeId,
    session: LinkSessionId,
    state: FlowState,
    output: &mut [u8],
    limits: WireLimits,
) -> Result<ControlEncoding, WireError> {
    state.validate()?;
    let header = Envelope {
        opcode: Opcode::ReplicaState,
        response: state.request_id.is_some(),
        request_id: state.request_id,
        sender,
        session: Some(session),
    }
    .encode_header(STATE_BYTES, 0, limits.envelope)?;
    if output.len() < STATE_BYTES {
        return Err(WireError::Capacity);
    }
    let mut writer = Writer::new(&mut output[..STATE_BYTES]);
    let report = state.report;
    writer.scope(report.channel.scope, sender);
    writer.bytes(&report.channel.epoch.get().to_be_bytes());
    writer.u64(report.revision);
    writer.prefix(report.base);
    writer.prefix(report.received);
    writer.u64(report.received_bytes);
    writer.u64(report.operation_limit);
    writer.u64(report.byte_limit);
    writer.u64(state.repair_limit.map_or(u64::MAX, |op| op.0));
    writer.bytes(&state.handle.to_be_bytes());
    Ok(ControlEncoding {
        header,
        metadata_bytes: STATE_BYTES,
    })
}

pub(super) fn decode_probe(
    scope: Scope,
    envelope: Envelope,
    mut reader: Reader<'_>,
    payload: &[u8],
) -> Result<FlowProbe, WireError> {
    if !payload.is_empty() {
        return Err(WireError::Payload);
    }
    if envelope.response {
        return Err(WireError::Correlation);
    }
    let probe = FlowProbe {
        scope,
        request_id: envelope.request_id.ok_or(WireError::Correlation)?,
        tail: reader.prefix()?,
        available: OpNumber(reader.u64()?),
        minimum_body_bytes: reader.u64()?,
    };
    reader.finish()?;
    validate_probe(probe)?;
    Ok(probe)
}

pub(super) fn decode_state(
    scope: Scope,
    envelope: Envelope,
    mut reader: Reader<'_>,
    payload: &[u8],
) -> Result<FlowState, WireError> {
    if !payload.is_empty() {
        return Err(WireError::Payload);
    }
    if envelope.response != envelope.request_id.is_some() {
        return Err(WireError::Correlation);
    }
    let epoch = ReceiveEpoch::new(u128::from_be_bytes(reader.bytes()?))?;
    let state = FlowState {
        handle: 1,
        request_id: envelope.request_id,
        report: Report {
            channel: Channel { scope, epoch },
            revision: reader.u64()?,
            base: reader.prefix()?,
            received: reader.prefix()?,
            received_bytes: reader.u64()?,
            operation_limit: reader.u64()?,
            byte_limit: reader.u64()?,
        },
        repair_limit: match reader.u64()? {
            u64::MAX => None,
            value => Some(OpNumber(value)),
        },
    };
    let state = FlowState {
        handle: reader.u32()?,
        ..state
    };
    reader.finish()?;
    state.validate()?;
    Ok(state)
}

/// Structural full report decoding for bounded compact routing registration.
/// This supplies no membership, receive credit, or replication authority.
pub fn flow_state_route(
    packet: ozzy_proto::Packet<'_>,
    limits: ozzy_proto::EnvelopeLimits,
) -> Result<FlowState, WireError> {
    packet
        .envelope
        .validate_frames(packet.metadata.len(), packet.payload.len(), limits)?;
    if packet.envelope.opcode != Opcode::ReplicaState {
        return Err(WireError::UnsupportedCommand);
    }
    let mut reader = Reader::new(packet.metadata);
    let scope = reader.routing_scope(packet.envelope.sender)?;
    decode_state(scope, packet.envelope, reader, packet.payload)
}
