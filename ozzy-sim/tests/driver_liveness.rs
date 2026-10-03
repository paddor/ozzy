//! Virtual time and directed links around production driver transitions.
//! Disk completion below models successful barriers, not filesystem power loss.

use std::collections::VecDeque;
use std::time::Duration;

use ozzy_journal::operation::{
    Barrier, CanonicalOperation, OperationBody, OperationLimits, canonical_body_digest,
    encode_operation_body,
};
use ozzy_proto::{GroupId, LinkSessionId, NodeId, OperationId};
use ozzy_replication::driver::{Action, ReplicaDriver, Timing};
use ozzy_replication::wire::{
    Control, Grant, PeerBinding, ReplicaMessage, WireLimits, decode, encode_control,
};
use ozzy_replication::{
    Admission, Configuration, Digest, JournalGeneration, NormalReplica, PipelineLimits, Prefix,
    PreparedOperation,
};

fn node(index: usize) -> NodeId {
    NodeId::from_bytes([index as u8 + 1; 16])
}

fn configuration() -> Configuration {
    Configuration::new(
        GroupId::from_bytes([7; 16]),
        1,
        Digest::from_bytes([8; 32]),
        std::array::from_fn(node),
    )
    .unwrap()
}

fn timing() -> Timing {
    Timing {
        heartbeat: Duration::from_millis(10),
        primary_timeout: Duration::from_millis(100),
        retransmit: Duration::from_millis(10),
        election_timeout: Duration::from_millis(200),
        max_election_timeout: Duration::from_millis(800),
    }
}

fn wire(from: usize, message: Control) -> Control {
    let session = LinkSessionId::from_bytes([9; 16]);
    let mut metadata = [0; 184];
    let encoded = encode_control(node(from), session, message, &mut metadata).unwrap();
    let ReplicaMessage::Control(decoded) = decode(
        &[&encoded.header, &metadata[..encoded.metadata_bytes], &[]],
        PeerBinding::new(configuration(), node(from), session).unwrap(),
        WireLimits::default(),
    )
    .unwrap() else {
        panic!("control frame");
    };
    decoded
}

fn append(through: Prefix) -> PreparedOperation {
    let body = encode_operation_body(
        &OperationBody::Barrier(Barrier {
            operation_id: OperationId::from_bytes((u128::from(through.op.0) + 1).to_be_bytes()),
        }),
        OperationLimits::default(),
    )
    .unwrap();
    let scope = configuration().scope();
    PreparedOperation::from_verified(
        &CanonicalOperation {
            group_id: scope.group_id,
            configuration_epoch: scope.configuration_epoch,
            original_view: scope.view,
            op_number: through.op.0 + 1,
            previous_digest: through.digest,
            kind: ozzy_journal::operation::OperationKind::Barrier,
            body: &body,
        },
        canonical_body_digest(&body),
    )
}

// Deterministically supplied normal-path work. Election traffic and timeouts
// are driven by the production drivers, not repaired by this workload helper.
fn commit_on_healthy_pair(
    replicas: &mut [ReplicaDriver; 3],
    stable: &mut [Prefix; 3],
    acknowledged: Prefix,
    now: Duration,
) -> Prefix {
    let operation = append(acknowledged);
    for index in 0..2 {
        let Admission::Write { ticket, .. } = replicas[index]
            .prepare(node(0), configuration().scope(), &[operation], now)
            .unwrap()
        else {
            panic!("new operation");
        };
        replicas[index].complete_write(ticket).unwrap();
        let sync = replicas[index].begin_sync().unwrap();
        stable[index] = operation.prefix(); // Physical barrier, before callback.
        replicas[index].complete_sync(sync, now).unwrap();
    }
    let ack = replicas[1].normal().unwrap().acknowledgment().unwrap();
    replicas[0]
        .receive(
            node(1),
            wire(
                1,
                Control::PrepareOk {
                    ack,
                    grant: Grant {
                        revision: 1,
                        record_limit: 8,
                        byte_limit: 4096,
                    },
                },
            ),
            now,
        )
        .unwrap();
    let commit = replicas[0].normal().unwrap().announcement().unwrap();
    assert_eq!(commit.committed, operation.prefix());
    assert!(
        stable
            .iter()
            .filter(|&&copy| copy == commit.committed)
            .count()
            >= 2
    );
    replicas[1]
        .receive(node(0), wire(0, Control::Commit(commit)), now)
        .unwrap();
    for replica in &mut replicas[..2] {
        replica.apply_through(commit.committed).unwrap();
    }
    commit.committed
}

#[test]
fn permanently_deaf_voter_cannot_disrupt_committing_quorum() {
    let mut replicas: [ReplicaDriver; 3] = std::array::from_fn(|index| {
        ReplicaDriver::from_normal(
            NormalReplica::bootstrap(
                configuration(),
                node(index),
                JournalGeneration(index as u128 + 1),
                PipelineLimits {
                    max_operations: 8,
                    max_body_bytes: 4096,
                },
            )
            .unwrap(),
            Duration::ZERO,
            timing(),
        )
        .unwrap()
    });
    let mut stable = [Prefix::GENESIS; 3];
    let mut acknowledged = Prefix::GENESIS;
    let mut network = VecDeque::with_capacity(64);
    let mut outsider_starts = 0;
    for millis in (0..=3000).step_by(10) {
        let now = Duration::from_millis(millis);
        for (from, replica) in replicas.iter_mut().enumerate() {
            // Six actions and <= 12 * 248 control bytes per voter per tick.
            for _ in 0..6 {
                match replica.poll(now).unwrap() {
                    Some(Action::PersistPromise(ticket)) => {
                        assert_eq!(ticket.log().accepted, stable[from]);
                        replica.complete_promise(ticket).unwrap();
                    }
                    Some(Action::Broadcast(message)) => {
                        if from == 2 && matches!(message, Control::ExitView(_)) {
                            outsider_starts += 1;
                        }
                        for to in 0..3 {
                            if to != from {
                                network.push_back((from, to, wire(from, message)));
                            }
                        }
                    }
                    Some(Action::Send { to, message }) => {
                        let to = (0..3).find(|&index| node(index) == to).unwrap();
                        network.push_back((from, to, wire(from, message)));
                    }
                    None => break,
                }
            }
        }
        assert!(network.len() <= 36);
        for _ in 0..36 {
            let Some((from, to, message)) = network.pop_front() else {
                break;
            };
            // Permanent asymmetric partition. Never heal/reboot voter two.
            if to != 2 {
                replicas[to].receive(node(from), message, now).unwrap();
            }
        }
        for replica in &replicas[..2] {
            assert!(
                replica.normal().is_some(),
                "healthy quorum disrupted at {millis}ms by one deaf voter; view={}",
                replica.scope().view
            );
        }
        // Continuous eligible work, not merely an idle cluster that stays in view zero.
        acknowledged = commit_on_healthy_pair(&mut replicas, &mut stable, acknowledged, now);
    }
    assert!(outsider_starts > 100);
    assert_eq!(replicas[2].scope().view, 0); // Timeout suspicion cannot ratchet a view.
    assert_eq!(acknowledged.op.0, 301);
}
