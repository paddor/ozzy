use ozzy_proto::EnvelopeLimits;
use ozzy_proto::LinkSessionId;
use ozzy_replication::flow::{Probe, ReceiveEpoch};
use ozzy_replication::wire::{
    self, Control, FetchOps, FlowState, PeerBinding, Prepare, RecoveryRequest, RecoveryState,
    ReplicaMessage, WireError, WireLimits,
};

use super::*;

fn session() -> LinkSessionId {
    // Trusted static bindings, as in the actor fixture. Reconnect negotiation
    // and cryptographic authentication are not supplied by this simulator.
    LinkSessionId::from_bytes([9; 16])
}

fn wire_limits() -> WireLimits {
    WireLimits {
        max_operations: PIPELINE,
        envelope: EnvelopeLimits {
            max_metadata_bytes: METADATA_BYTES,
            max_payload_bytes: BODY_BYTES,
        },
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Packet {
    pub from: usize,
    pub to: usize,
    header: [u8; 64],
    metadata: Vec<u8>,
    payload: Vec<u8>,
    // Test/trace inspection only. Replica receive uses its own current epoch,
    // never this sender-captured value, to bind the real decoder.
    epoch: Option<ReceiveEpoch>,
}

impl Packet {
    pub(super) fn recovery(from: usize, to: usize, request: RecoveryRequest) -> Self {
        let mut metadata = [0; METADATA_BYTES];
        let encoded =
            wire::encode_recovery(node(from), session(), request, &mut metadata, wire_limits())
                .unwrap();
        Self {
            from,
            to,
            header: encoded.header,
            metadata: metadata[..encoded.metadata_bytes].to_vec(),
            payload: Vec::new(),
            epoch: None,
        }
    }

    pub(super) fn recovery_state(from: usize, to: usize, state: &RecoveryState) -> Self {
        let mut metadata = [0; METADATA_BYTES];
        let encoded = wire::encode_recovery_state(
            node(from),
            session(),
            *state,
            &mut metadata,
            wire_limits(),
        )
        .unwrap();
        Self {
            from,
            to,
            header: encoded.header,
            metadata: metadata[..encoded.metadata_bytes].to_vec(),
            payload: Vec::new(),
            epoch: None,
        }
    }

    pub(super) fn summary(&self) -> String {
        match self.decode() {
            ReplicaMessage::Checkpoint(_) | ReplicaMessage::HistoryRetired(_) => {
                panic!("retained recovery is exercised by the real broker harness")
            }
            ReplicaMessage::Recovery(message) => format!("recovery {message:?}"),
            ReplicaMessage::Flow(flow) => format!("flow {:?}", std::mem::discriminant(&flow)),
            ReplicaMessage::Control(control) => format!(
                "control {:?} view={}",
                std::mem::discriminant(&control),
                control.scope().view
            ),
            ReplicaMessage::Prepare(batch) => format!(
                "prepare view={} ops={}..={}",
                batch.scope().view,
                batch.predecessor().op.0 + 1,
                batch.end().op.0
            ),
            ReplicaMessage::FetchOps(request) => format!(
                "fetch view={} after={} tail={}",
                request.scope.view, request.predecessor.op.0, request.source.accepted.op.0
            ),
            ReplicaMessage::Ops(batch) => format!(
                "ops view={} ops={}..={}",
                batch.scope().view,
                batch.predecessor().op.0 + 1,
                batch.end().op.0
            ),
        }
    }

    pub(crate) fn decode(&self) -> ReplicaMessage<'_> {
        // Trace inspection has no authority. Actual receive binds the receiver's
        // own configuration and epoch through decode_for, never this fallback.
        [QuorumPolicy::Durable, QuorumPolicy::Replicated]
            .into_iter()
            .find_map(|policy| {
                self.decode_with(
                    configuration_record_with_policy(policy).configuration(),
                    self.epoch,
                )
                .ok()
            })
            .expect("fixture packet decodes under its configured policy")
    }

    pub(super) fn decode_for(
        &self,
        configuration: Configuration,
        epoch: ReceiveEpoch,
    ) -> Result<ReplicaMessage<'_>, WireError> {
        self.decode_with(configuration, Some(epoch))
    }

    fn decode_with(
        &self,
        configuration: Configuration,
        epoch: Option<ReceiveEpoch>,
    ) -> Result<ReplicaMessage<'_>, WireError> {
        let binding = PeerBinding::new(configuration, node(self.from), session()).unwrap();
        wire::decode(
            &[&self.header, &self.metadata, &self.payload],
            epoch.map_or(binding, |epoch| binding.with_receive_epoch(epoch)),
            wire_limits(),
        )
    }

    pub(crate) fn control(from: usize, to: usize, message: Control) -> Self {
        let mut metadata = [0; METADATA_BYTES];
        let encoded = wire::encode_control(node(from), session(), message, &mut metadata).unwrap();
        Self {
            from,
            to,
            header: encoded.header,
            metadata: metadata[..encoded.metadata_bytes].to_vec(),
            payload: Vec::new(),
            epoch: None,
        }
    }

    pub(crate) fn prepare(
        from: usize,
        to: usize,
        scope: Scope,
        epoch: ReceiveEpoch,
        committed: Prefix,
        operations: &[Operation],
    ) -> Self {
        let borrowed: Vec<_> = operations
            .iter()
            .map(|op| {
                wire::Operation::from_verified(op.canonical(), canonical_body_digest(&op.body))
            })
            .collect();
        let mut metadata = [0; METADATA_BYTES];
        let mut payload = [0; BODY_BYTES];
        let encoded = wire::encode_flow_prepare(
            node(from),
            session(),
            epoch,
            Prepare {
                scope,
                committed,
                operations: &borrowed,
            },
            &mut metadata,
            &mut payload,
            wire_limits(),
        )
        .unwrap();
        Self {
            from,
            to,
            header: encoded.header,
            metadata: metadata[..encoded.metadata_bytes].to_vec(),
            payload: payload[..encoded.payload_bytes].to_vec(),
            epoch: Some(epoch),
        }
    }

    pub(super) fn probe(from: usize, to: usize, probe: Probe) -> Self {
        let mut metadata = [0; METADATA_BYTES];
        let encoded =
            wire::encode_flow_probe(node(from), session(), probe, &mut metadata, wire_limits())
                .unwrap();
        Self {
            from,
            to,
            header: encoded.header,
            metadata: metadata[..encoded.metadata_bytes].to_vec(),
            payload: Vec::new(),
            epoch: None,
        }
    }

    pub(super) fn state(from: usize, to: usize, state: FlowState) -> Self {
        let mut metadata = [0; METADATA_BYTES];
        let encoded =
            wire::encode_flow_state(node(from), session(), state, &mut metadata, wire_limits())
                .unwrap();
        Self {
            from,
            to,
            header: encoded.header,
            metadata: metadata[..encoded.metadata_bytes].to_vec(),
            payload: Vec::new(),
            epoch: None,
        }
    }

    pub(crate) fn fetch(from: usize, to: usize, request: FetchOps) -> Self {
        let mut metadata = [0; METADATA_BYTES];
        let encoded =
            wire::encode_fetch(node(from), session(), request, &mut metadata, wire_limits())
                .unwrap();
        Self {
            from,
            to,
            header: encoded.header,
            metadata: metadata[..encoded.metadata_bytes].to_vec(),
            payload: Vec::new(),
            epoch: None,
        }
    }

    pub(crate) fn ops(from: usize, to: usize, request: FetchOps, operations: &[Operation]) -> Self {
        let borrowed: Vec<_> = operations
            .iter()
            .map(|op| {
                wire::Operation::from_verified(op.canonical(), canonical_body_digest(&op.body))
            })
            .collect();
        let mut metadata = [0; METADATA_BYTES];
        let mut payload = [0; BODY_BYTES];
        let encoded = wire::encode_ops(
            node(from),
            session(),
            request,
            &borrowed,
            &mut metadata,
            &mut payload,
            wire_limits(),
        )
        .unwrap();
        Self {
            from,
            to,
            header: encoded.header,
            metadata: metadata[..encoded.metadata_bytes].to_vec(),
            payload: payload[..encoded.payload_bytes].to_vec(),
            epoch: None,
        }
    }
}

pub(super) fn own(operation: wire::Operation<'_>, configuration: Configuration) -> Operation {
    let op = operation.canonical();
    Operation {
        scope: Scope {
            view: op.original_view,
            ..configuration.scope()
        },
        number: op.op_number,
        previous: op.previous_digest,
        kind: op.kind,
        body: op.body.to_vec(),
    }
}
