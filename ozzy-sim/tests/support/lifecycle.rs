//! Deterministic adapter around the production driver, codecs, and application.
//! Disks use either atomic actions or production byte storage with explicit faults.
//! The adapter is not the Tokio actor: actual transport/worker tests remain separate.

#[path = "lifecycle/cluster.rs"]
mod cluster;
#[path = "lifecycle/disk.rs"]
mod disk;
#[path = "lifecycle/flow.rs"]
mod flow;
#[path = "lifecycle/recovery.rs"]
mod recovery;
#[path = "lifecycle/replica.rs"]
mod replica;
#[path = "lifecycle/storage.rs"]
mod storage;
#[path = "lifecycle/wire.rs"]
mod wire;
#[path = "lifecycle/workload.rs"]
mod workload;

use std::time::Duration;

use ozzy_core::state::{CanonicalImages, CanonicalRecovery, StateLimits};
use ozzy_journal::operation::{
    CanonicalOperation, OperationBody, OperationKind, OperationLimits, canonical_body_digest,
    decode_operation_body, encode_operation_body,
};
use ozzy_proto::{GroupId, NodeId};
use ozzy_replication::driver::{DriverError, Timing};
use ozzy_replication::{
    Configuration, Digest, LogSource, OpNumber, PipelineLimits, Prefix, PreparedOperation,
    QuorumPolicy, ReplicationError, Scope, ViewChangeError,
};

pub(crate) use cluster::Cluster;
pub(crate) use disk::DiskAction;
pub(crate) use recovery::RecoveryDisk;
pub(crate) use replica::Replica;
pub(crate) use wire::Packet;

const HISTORY_LIMIT: usize = 128;
const PACKET_LIMIT: usize = 128;
const PIPELINE: usize = 4;
const BODY_BYTES: usize = 8192;
const METADATA_BYTES: usize = 1024;

pub(crate) fn node(index: usize) -> NodeId {
    NodeId::from_bytes([u8::try_from(index).unwrap() + 1; 16])
}

fn index(node_id: NodeId) -> usize {
    (0..3).find(|&n| node(n) == node_id).unwrap()
}

fn configuration_record_with_policy(policy: QuorumPolicy) -> ozzy_replication::ConfigurationRecord {
    ozzy_replication::ConfigurationRecord::with_policy(
        GroupId::from_bytes([7; 16]),
        1,
        std::array::from_fn(|id| ozzy_replication::ConfiguredVoter {
            node_id: node(id),
            principal: Digest::from_bytes([id as u8 + 1; 32]),
        }),
        policy,
    )
    .unwrap()
}

fn limits() -> PipelineLimits {
    PipelineLimits {
        max_operations: PIPELINE,
        max_body_bytes: BODY_BYTES,
    }
}

fn timing() -> Timing {
    Timing {
        heartbeat: Duration::from_millis(10),
        retransmit: Duration::from_millis(10),
        primary_timeout: Duration::from_millis(100),
        election_timeout: Duration::from_millis(200),
        max_election_timeout: Duration::from_millis(800),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Operation {
    scope: Scope,
    number: u64,
    previous: Digest,
    kind: OperationKind,
    body: Vec<u8>,
}

impl Operation {
    fn new(scope: Scope, previous: Prefix, body: &OperationBody<'_>) -> Self {
        Self {
            scope,
            number: previous.op.0 + 1,
            previous: previous.digest,
            kind: body.kind(),
            body: encode_operation_body(body, OperationLimits::default()).unwrap(),
        }
    }

    fn canonical(&self) -> CanonicalOperation<'_> {
        CanonicalOperation {
            group_id: self.scope.group_id,
            configuration_epoch: self.scope.configuration_epoch,
            original_view: self.scope.view,
            op_number: self.number,
            previous_digest: self.previous,
            kind: self.kind,
            body: &self.body,
        }
    }

    fn decoded(&self) -> OperationBody<'_> {
        decode_operation_body(self.kind, &self.body, OperationLimits::default()).unwrap()
    }

    fn metadata(&self) -> PreparedOperation {
        PreparedOperation::from_verified(&self.canonical(), canonical_body_digest(&self.body))
    }

    pub(crate) fn prefix(&self) -> Prefix {
        self.metadata().prefix()
    }
}

fn prefix(operations: &[Operation], through: usize) -> Prefix {
    if through == 0 {
        Prefix::GENESIS
    } else {
        operations[through - 1].prefix()
    }
}

fn tail(operations: &[Operation]) -> Prefix {
    prefix(operations, operations.len())
}

// Full history stays bounded in this model. Replay uses one pending slot, not
// one live transition plan per historical operation. This image stays private
// until the driver authorizes selected-tail activation.
fn private_replay(operations: &[Operation]) -> CanonicalImages {
    let mut replay = CanonicalRecovery::new(StateLimits::default(), HISTORY_LIMIT, PIPELINE);
    let mut previous = Prefix::GENESIS;
    for operation in operations {
        assert_eq!(operation.number, previous.op.0 + 1);
        assert_eq!(operation.previous, previous.digest);
        replay
            .apply(operation.number, &operation.decoded(), true)
            .unwrap();
        previous = operation.prefix();
    }
    replay.finish().unwrap()
}

#[derive(Debug, Clone)]
struct History {
    source: LogSource,
    operations: Vec<Operation>,
}

impl History {
    fn lookup(&self, source: LogSource, number: OpNumber) -> Option<Digest> {
        (self.source == source && number.0 as usize <= self.operations.len())
            .then(|| prefix(&self.operations, number.0 as usize).digest)
    }
}

// Expected retry/reordering rejections only. Conflicting history, invalid
// application transitions, stale storage tickets, and faults must fail the test.
fn retryable(error: DriverError) -> bool {
    match error {
        DriverError::Replication(error)
        | DriverError::ViewChange(ViewChangeError::Replication(error)) => {
            matches!(
                error,
                ReplicationError::WrongRole
                    | ReplicationError::NotNormal
                    | ReplicationError::ScopeMismatch
                    | ReplicationError::HistoryGap
                    | ReplicationError::ActivationPending
                    | ReplicationError::Capacity
            )
        }
        DriverError::ViewChange(error) => matches!(
            error,
            ViewChangeError::PromiseRequired
                | ViewChangeError::StartQuorumMissing
                | ViewChangeError::ReportQuorumMissing
                | ViewChangeError::HistoryMissing
                | ViewChangeError::SelectionFrozen
        ),
        _ => false,
    }
}
