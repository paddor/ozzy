//! Bounded native replica messages. No I/O or implicit consensus authority.
//!
//! The transport supplies an independently authenticated voter/session binding.
//! Scope decoding checks the immutable configuration; the production cores still
//! decide whether the message's view, history, role, and evidence are admissible.

mod admission;
mod election;
pub use admission::ReceiveFence;
mod flow;
pub use flow::flow_state_route;
mod compact;
pub use compact::{COMPACT_STATE_BYTES, CompactState};
mod prepare;
mod publication;
pub use publication::{
    PUBLICATION_BYTES, PUBLICATION_TOPIC_BYTES, PublicationPart, decode_publication,
    encode_publication, encode_publication_group, publication_topic,
};
mod recovery;
mod transfer;

pub use flow::{FlowMessage, FlowProbe, FlowState, encode_flow_probe, encode_flow_state};
pub use prepare::{
    Operation, Operations, Prepare, PrepareBatch, PrepareEncoding, PrepareMetadata,
    VerifiedOperation, encode_flow_prepare, encode_flow_prepare_metadata, encode_prepare,
    encode_prepare_metadata, encode_prepare_unbound,
};
pub use recovery::{
    RecoveryMessage, RecoveryRequest, RecoveryState, encode_recovery, encode_recovery_state,
};
pub use transfer::{FetchOps, OpsBatch, encode_fetch, encode_ops};

use ozzy_proto::{Envelope, EnvelopeError, EnvelopeLimits, Opcode};
use ozzy_proto::{GroupId, LinkSessionId, NodeId};

use crate::flow::{FlowError, ReceiveEpoch};
use crate::{
    Commit, Configuration, Digest, DoViewChange, OpNumber, Prefix, PrepareOk, QuorumPolicy,
    ReplicationError, RetainedPrepareOk, Scope, StartView, StartViewChange,
};

const COMMON_BYTES: usize = 80;

/// Authenticated peer and established session supplied by the transport adapter.
///
/// Construction checks membership and nonzero IDs, not cryptographic identity.
/// Never construct this binding from untrusted packet fields or routing IDs alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerBinding {
    configuration: Configuration,
    peer: NodeId,
    session: LinkSessionId,
    receive_epoch: Option<ReceiveEpoch>,
}

impl PeerBinding {
    /// Bind one independently authenticated configured voter to its live session.
    pub fn new(
        configuration: Configuration,
        peer: NodeId,
        session: LinkSessionId,
    ) -> Result<Self, WireError> {
        configuration.voter_index(peer)?;
        if session.as_bytes() == &[0; 16] {
            return Err(WireError::Peer);
        }
        Ok(Self {
            configuration,
            peer,
            session,
            receive_epoch: None,
        })
    }

    /// Require credited normal payloads bound to this locally issued receive epoch.
    ///
    /// Set only from the receiver's own current flow state, never incoming fields.
    /// Rejects legacy PREPARE and stale `PREPARE_FLOW` before decoding/hashing bodies.
    /// Control and installation-history messages retain their independent rules.
    #[must_use]
    pub const fn with_receive_epoch(mut self, epoch: ReceiveEpoch) -> Self {
        self.receive_epoch = Some(epoch);
        self
    }
}

/// Directional receive and command work bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireLimits {
    /// Maximum actual metadata and payload frame sizes.
    pub envelope: EnvelopeLimits,
    /// Maximum canonical operations in a prepare/history response, independent of records.
    pub max_operations: usize,
}

impl Default for WireLimits {
    fn default() -> Self {
        Self {
            envelope: EnvelopeLimits::default(),
            max_operations: 64,
        }
    }
}

/// Session-scoped cumulative absolute grant, not a per-message increment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Grant {
    /// Monotonic credit revision, interpreted by the session owner.
    pub revision: u64,
    /// Maximum cumulative records allowed in the current session.
    pub record_limit: u64,
    /// Maximum cumulative canonical body bytes allowed in the current session.
    pub byte_limit: u64,
}

/// Fixed-size replica control metadata. Receipt alone has no authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Control {
    /// Cumulative durable backup evidence plus its current session grant.
    PrepareOk {
        /// Matching contiguous prefix after successful disk synchronization.
        ack: PrepareOk,
        /// Independent flow-control evidence; never counts as a quorum vote.
        grant: Grant,
    },
    /// Cumulative retained-memory evidence with independent background persistence.
    PrepareRetained {
        /// Validated bytes retained until application and persistence complete.
        ack: RetainedPrepareOk,
        /// Independent flow-control evidence; never counts as a quorum vote.
        grant: Grant,
    },
    /// Primary's cumulative commit announcement or idle heartbeat.
    Commit(Commit),
    /// Volatile suspicion about this current view, not a durable promise or vote.
    ExitView(Scope),
    /// First election phase, emitted after the sender's durable view promise.
    StartViewChange(StartViewChange),
    /// Frozen full-WAL descriptor for the candidate's second-phase quorum.
    DoViewChange(DoViewChange),
    /// Primary's durably installed selected lineage, not a backup installation.
    StartView(StartView),
}

impl Control {
    /// Claimed prefix for routing/replay hints. This does not validate its evidence.
    pub const fn acknowledged(self) -> Option<Prefix> {
        match self {
            Self::PrepareOk { ack, .. } => Some(ack.durable),
            Self::PrepareRetained { ack, .. } => Some(ack.retained),
            _ => None,
        }
    }

    /// Exact group, immutable configuration digest, and active/proposed view.
    pub const fn scope(self) -> Scope {
        match self {
            Self::PrepareOk { ack, .. } => ack.scope,
            Self::PrepareRetained { ack, .. } => ack.scope,
            Self::Commit(message) => message.scope,
            Self::ExitView(scope) => scope,
            Self::StartViewChange(message) => message.scope,
            Self::DoViewChange(message) => message.scope,
            Self::StartView(message) => message.scope,
        }
    }

    fn opcode(self) -> Opcode {
        match self {
            Self::PrepareOk { .. } | Self::PrepareRetained { .. } => Opcode::PrepareOk,
            Self::Commit(_) => Opcode::Commit,
            Self::ExitView(_) => Opcode::ExitView,
            Self::StartViewChange(_) => Opcode::StartViewChange,
            Self::DoViewChange(_) => Opcode::DoViewChange,
            Self::StartView(_) => Opcode::StartView,
        }
    }
}

/// Decoded replica command, still subject to role/history checks in the core.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaMessage<'a> {
    /// Fresh lost-state evidence, neither receipt credit nor a normal/election vote.
    Recovery(RecoveryMessage),
    /// Receipt/credit exchange or epoch-bound data; never durable quorum evidence.
    Flow(FlowMessage<'a>),
    /// Fixed-size quorum/control evidence from the bound sender.
    Control(Control),
    /// Hash-validated canonical bytes, still requiring application validation.
    Prepare(PrepareBatch<'a>),
    /// Correlated bounded request for an independently pinned source lineage.
    FetchOps(FetchOps),
    /// Hash-validated response bytes, not a normal prepare or commit announcement.
    Ops(OpsBatch<'a>),
}

/// Envelope and valid prefix length in the caller's reusable metadata buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlEncoding {
    /// First Ozzy frame, with exact lengths for this encoding.
    pub header: [u8; 64],
    /// Second frame length; the third frame must be present and empty.
    pub metadata_bytes: usize,
}

/// Encode control metadata into caller-owned storage without allocating.
///
/// Invalid fields or insufficient capacity leave the output unchanged. Callers
/// may send only messages actually authorized by the current consensus role.
pub fn encode_control(
    sender: NodeId,
    session: LinkSessionId,
    message: Control,
    output: &mut [u8],
) -> Result<ControlEncoding, WireError> {
    let scope = message.scope();
    validate_scope(scope)?;
    let metadata_bytes = match message {
        Control::PrepareOk { ack, .. } => {
            validate_prefix(ack.durable)?;
            COMMON_BYTES + 40 + 1 + 24
        }
        Control::PrepareRetained { ack, .. } => {
            validate_prefix(ack.retained)?;
            COMMON_BYTES + 40 + 1 + 24
        }
        Control::Commit(message) => {
            validate_prefix(message.committed)?;
            COMMON_BYTES + 40
        }
        Control::StartViewChange(message) => {
            validate_election_view(message.scope)?;
            COMMON_BYTES
        }
        Control::ExitView(_) => COMMON_BYTES,
        Control::DoViewChange(message) => {
            election::validate_report(message)?;
            COMMON_BYTES + 16 + 8 + 40 + 40
        }
        Control::StartView(message) => {
            election::validate_start(message)?;
            COMMON_BYTES + 16 + 40 + 40
        }
    };
    if output.len() < metadata_bytes {
        return Err(WireError::Capacity);
    }
    let header = Envelope {
        opcode: message.opcode(),
        response: false,
        request_id: None,
        sender,
        session: Some(session),
    }
    .encode_header(metadata_bytes, 0, EnvelopeLimits::default())?;
    let mut writer = Writer::new(&mut output[..metadata_bytes]);
    writer.scope(scope, sender);
    match message {
        Control::PrepareOk { ack, grant } => {
            writer.prefix(ack.durable);
            writer.bytes(&[2]);
            writer.u64(grant.revision);
            writer.u64(grant.record_limit);
            writer.u64(grant.byte_limit);
        }
        Control::PrepareRetained { ack, grant } => {
            writer.prefix(ack.retained);
            writer.bytes(&[3]);
            writer.u64(grant.revision);
            writer.u64(grant.record_limit);
            writer.u64(grant.byte_limit);
        }
        Control::Commit(message) => writer.prefix(message.committed),
        Control::StartViewChange(_) | Control::ExitView(_) => {}
        Control::DoViewChange(message) => election::write_report(&mut writer, message),
        Control::StartView(message) => election::write_start(&mut writer, message),
    }
    Ok(ControlEncoding {
        header,
        metadata_bytes,
    })
}

/// Read only the fixed group/configuration prefix of an implemented PEER
/// command. This does not authenticate the sender or validate membership, view,
/// operation hashes, or payloads. Dispatch uses it to select an actor, which must
/// still call `decode` with an independently established `PeerBinding`.
pub fn route(packet: ozzy_proto::Packet<'_>, limits: EnvelopeLimits) -> Result<Scope, WireError> {
    packet
        .envelope
        .validate_frames(packet.metadata.len(), packet.payload.len(), limits)?;
    if !matches!(
        packet.envelope.opcode,
        Opcode::ReplicaOpen
            | Opcode::ReplicaState
            | Opcode::Prepare
            | Opcode::PrepareFlow
            | Opcode::PrepareOk
            | Opcode::Commit
            | Opcode::ExitView
            | Opcode::StartViewChange
            | Opcode::DoViewChange
            | Opcode::StartView
            | Opcode::Recovery
            | Opcode::RecoveryState
            | Opcode::FetchOps
            | Opcode::Ops
    ) {
        return Err(WireError::UnsupportedCommand);
    }
    if !matches!(
        packet.envelope.opcode,
        Opcode::Prepare | Opcode::PrepareFlow | Opcode::Ops
    ) && !packet.payload.is_empty()
    {
        return Err(WireError::Payload);
    }
    Reader::new(packet.metadata).routing_scope(packet.envelope.sender)
}

/// Decode a bounded packet and reject mismatched voter/session/configuration.
///
/// This does not validate a claim against local disk state or count a quorum.
/// Old views remain recognizable so the consensus core can apply its fencing rules.
pub fn decode<'a>(
    frames: &[&'a [u8]],
    binding: PeerBinding,
    limits: WireLimits,
) -> Result<ReplicaMessage<'a>, WireError> {
    let packet = ozzy_proto::decode_packet(frames, limits.envelope)?;
    if packet.envelope.sender != binding.peer || packet.envelope.session != Some(binding.session) {
        return Err(WireError::Peer);
    }
    let mut reader = Reader::new(packet.metadata);
    let scope = reader.scope(binding)?;
    match packet.envelope.opcode {
        Opcode::Recovery => {
            return recovery::decode_request(scope, packet.envelope, reader, packet.payload)
                .map(|request| ReplicaMessage::Recovery(RecoveryMessage::Request(request)));
        }
        Opcode::RecoveryState => {
            return recovery::decode_state(scope, packet.envelope, reader, packet.payload, binding)
                .map(|state| ReplicaMessage::Recovery(RecoveryMessage::State(state)));
        }
        Opcode::ReplicaOpen => {
            return flow::decode_probe(scope, packet.envelope, reader, packet.payload)
                .map(|probe| ReplicaMessage::Flow(FlowMessage::Probe(probe)));
        }
        Opcode::ReplicaState => {
            return flow::decode_state(scope, packet.envelope, reader, packet.payload)
                .map(|state| ReplicaMessage::Flow(FlowMessage::State(state)));
        }
        _ => {}
    }
    if packet.envelope.opcode == Opcode::FetchOps {
        return transfer::decode_fetch(scope, packet.envelope, reader, packet.payload, binding)
            .map(ReplicaMessage::FetchOps);
    }
    if packet.envelope.opcode == Opcode::Ops {
        return transfer::decode_ops(scope, packet.envelope, reader, packet.payload, limits)
            .map(ReplicaMessage::Ops);
    }
    if packet.envelope.response || packet.envelope.request_id.is_some() {
        return Err(WireError::Correlation);
    }
    if packet.envelope.opcode == Opcode::PrepareFlow {
        let epoch = ReceiveEpoch::new(u128::from_be_bytes(reader.bytes()?))?;
        if binding.receive_epoch != Some(epoch) {
            return Err(FlowError::Channel.into());
        }
        return prepare::decode_prepare(scope, reader, packet.payload, limits)
            .map(|batch| ReplicaMessage::Flow(FlowMessage::Prepare { epoch, batch }));
    }
    if packet.envelope.opcode == Opcode::Prepare {
        if binding.receive_epoch.is_some() {
            return Err(FlowError::Channel.into());
        }
        return prepare::decode_prepare(scope, reader, packet.payload, limits)
            .map(ReplicaMessage::Prepare);
    }
    if !packet.payload.is_empty() {
        return Err(WireError::Payload);
    }
    let message = match packet.envelope.opcode {
        Opcode::PrepareOk => {
            let prefix = reader.prefix()?;
            let evidence = reader.bytes::<1>()?[0];
            let grant = Grant {
                revision: reader.u64()?,
                record_limit: reader.u64()?,
                byte_limit: reader.u64()?,
            };
            match (binding.configuration.policy(), evidence) {
                (QuorumPolicy::Durable, 2) => Control::PrepareOk {
                    ack: PrepareOk {
                        scope,
                        durable: prefix,
                    },
                    grant,
                },
                (QuorumPolicy::Replicated, 3) => Control::PrepareRetained {
                    ack: RetainedPrepareOk {
                        scope,
                        retained: prefix,
                    },
                    grant,
                },
                _ => return Err(WireError::Evidence),
            }
        }
        Opcode::Commit => Control::Commit(Commit {
            scope,
            committed: reader.prefix()?,
        }),
        Opcode::StartViewChange => {
            validate_election_view(scope)?;
            Control::StartViewChange(StartViewChange { scope })
        }
        Opcode::ExitView => Control::ExitView(scope),
        Opcode::DoViewChange => Control::DoViewChange(election::read_report(scope, &mut reader)?),
        Opcode::StartView => Control::StartView(election::read_start(scope, &mut reader)?),
        _ => return Err(WireError::UnsupportedCommand),
    };
    reader.finish()?;
    Ok(ReplicaMessage::Control(message))
}

fn validate_scope(scope: Scope) -> Result<(), WireError> {
    if scope.group_id.as_bytes() == &[0; 16] || scope.configuration_digest == Digest::ZERO {
        return Err(WireError::Scope);
    }
    Ok(())
}

fn validate_election_view(scope: Scope) -> Result<(), WireError> {
    if scope.view == 0 {
        return Err(WireError::History);
    }
    Ok(())
}

fn validate_prefix(prefix: Prefix) -> Result<(), WireError> {
    if (prefix.op.0 == 0) != (prefix.digest == Digest::ZERO) || prefix.op.0 == u64::MAX {
        return Err(WireError::Prefix);
    }
    Ok(())
}

#[derive(Debug)]
struct Reader<'a> {
    remaining: &'a [u8],
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }
    fn bytes<const N: usize>(&mut self) -> Result<[u8; N], WireError> {
        let (bytes, rest) = self
            .remaining
            .split_at_checked(N)
            .ok_or(WireError::Length)?;
        self.remaining = rest;
        Ok(bytes.try_into().expect("exact field"))
    }
    fn u64(&mut self) -> Result<u64, WireError> {
        Ok(u64::from_be_bytes(self.bytes()?))
    }
    fn u32(&mut self) -> Result<u32, WireError> {
        Ok(u32::from_be_bytes(self.bytes()?))
    }
    fn digest(&mut self) -> Result<Digest, WireError> {
        Ok(Digest::from_bytes(self.bytes()?))
    }
    fn prefix(&mut self) -> Result<Prefix, WireError> {
        let value = Prefix {
            op: OpNumber(self.u64()?),
            digest: self.digest()?,
        };
        validate_prefix(value)?;
        Ok(value)
    }
    fn routing_scope(&mut self, sender: NodeId) -> Result<Scope, WireError> {
        let group_id = GroupId::from_bytes(self.bytes()?);
        let configuration_epoch = self.u64()?;
        let view = self.u64()?;
        let voter = NodeId::from_bytes(self.bytes()?);
        let configuration_digest = self.digest()?;
        if voter != sender {
            return Err(WireError::Peer);
        }
        let scope = Scope {
            group_id,
            configuration_epoch,
            configuration_digest,
            view,
        };
        validate_scope(scope)?;
        if configuration_epoch == 0 {
            return Err(WireError::Scope);
        }
        Ok(scope)
    }

    fn scope(&mut self, binding: PeerBinding) -> Result<Scope, WireError> {
        let scope = self.routing_scope(binding.peer)?;
        if scope
            != (Scope {
                view: scope.view,
                ..binding.configuration.scope()
            })
        {
            return Err(WireError::Scope);
        }
        Ok(scope)
    }
    fn finish(self) -> Result<(), WireError> {
        if self.remaining.is_empty() {
            Ok(())
        } else {
            Err(WireError::Length)
        }
    }
}

#[derive(Debug)]
struct Writer<'a> {
    output: &'a mut [u8],
    offset: usize,
}

impl<'a> Writer<'a> {
    fn new(output: &'a mut [u8]) -> Self {
        Self { output, offset: 0 }
    }
    fn bytes(&mut self, bytes: &[u8]) {
        let end = self.offset + bytes.len();
        self.output[self.offset..end].copy_from_slice(bytes);
        self.offset = end;
    }
    fn u64(&mut self, value: u64) {
        self.bytes(&value.to_be_bytes());
    }
    fn prefix(&mut self, prefix: Prefix) {
        self.u64(prefix.op.0);
        self.bytes(prefix.digest.as_bytes());
    }
    fn scope(&mut self, scope: Scope, voter: NodeId) {
        self.bytes(scope.group_id.as_bytes());
        self.u64(scope.configuration_epoch);
        self.u64(scope.view);
        self.bytes(voter.as_bytes());
        self.bytes(scope.configuration_digest.as_bytes());
    }
}

/// Structural wire failure. No rejection changes replica or journal state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum WireError {
    /// Invalid receipt/credit metadata or mismatched local receive incarnation.
    #[error(transparent)]
    Flow(#[from] FlowError),
    /// Response does not match the live source, request, range, or response bound.
    #[error("replica history response does not match the outstanding request")]
    Transfer,
    /// Invalid election view, frozen generation, or accepted/commit relationship.
    #[error("invalid replica election history descriptor")]
    History,
    /// A batch/request is empty, too large, or exceeds an operation/frame work bound.
    #[error("replica command exceeds operation or byte limits")]
    Limit,
    /// Unsupported canonical operation kind.
    #[error("unsupported replica operation kind {0}")]
    OperationKind(u16),
    /// Canonical envelope does not continue the announced scope/hash chain.
    #[error("replica canonical operation chain mismatch")]
    Chain,
    /// Body or envelope digest does not match the received canonical bytes.
    #[error("replica canonical operation digest mismatch")]
    Digest,
    /// The ACK's evidence kind does not match the selected RAM/disk codec.
    #[error("replica ACK does not carry the configured evidence")]
    Evidence,
    /// Invalid native envelope or directional frame bound.
    #[error(transparent)]
    Envelope(#[from] EnvelopeError),
    /// Invalid configured voter supplied to the binding.
    #[error(transparent)]
    Configuration(#[from] ReplicationError),
    /// Sender, embedded voter, or session does not match authenticated binding.
    #[error("replica peer or session mismatch")]
    Peer,
    /// Group/configuration epoch/digest does not match the configured group.
    #[error("replica configuration scope mismatch")]
    Scope,
    /// Invalid genesis/digest pairing or unrepresentable successor operation.
    #[error("invalid replica log prefix")]
    Prefix,
    /// A control command cannot carry payload bytes.
    #[error("unexpected replica payload")]
    Payload,
    /// Truncated fields or extra metadata.
    #[error("replica metadata length mismatch")]
    Length,
    /// Output storage cannot hold the complete encoded message.
    #[error("replica encoding buffer too small")]
    Capacity,
    /// Command correlation/response flag does not match its exchange family.
    #[error("invalid replica command correlation")]
    Correlation,
    /// Opcode does not have an implemented replica codec.
    #[error("unsupported replica command")]
    UnsupportedCommand,
}
