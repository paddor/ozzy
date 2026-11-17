//! Cover and rest: an adapter's own liveness proof stands in for a voter's
//! heartbeats and votes.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test code"
)]

use std::time::Duration;

use ozzy_proto::{GroupId, NodeId};
use ozzy_replication::driver::{Action, ReplicaDriver, Timing};
use ozzy_replication::wire::Control;
use ozzy_replication::{
    BeatSource, Configuration, Digest, JournalGeneration, NormalReplica, PipelineLimits, Scope,
    StartViewChange,
};

fn node(index: u8) -> NodeId {
    NodeId::from_bytes([index + 1; 16])
}

fn configuration() -> Configuration {
    Configuration::new(
        GroupId::from_bytes([7; 16]),
        1,
        Digest::from_bytes([8; 32]),
        [node(0), node(1), node(2)],
    )
    .unwrap()
}

fn driver(index: u8) -> ReplicaDriver {
    let normal = NormalReplica::bootstrap(
        configuration(),
        node(index),
        JournalGeneration(u128::from(index) + 1),
        PipelineLimits {
            max_operations: 8,
            max_body_bytes: 1024,
        },
    )
    .unwrap();
    let timing = Timing {
        heartbeat: Duration::from_millis(10),
        primary_timeout: Duration::from_millis(100),
        retransmit: Duration::from_millis(10),
        election_timeout: Duration::from_millis(200),
        max_election_timeout: Duration::from_secs(2),
    };
    ReplicaDriver::from_normal(normal, Duration::ZERO, timing).unwrap()
}

const SOURCE: BeatSource = BeatSource { shard: 3, epoch: 9 };

fn at(millis: u64) -> Duration {
    Duration::from_millis(millis)
}

fn exits(action: Option<Action>) -> bool {
    matches!(action, Some(Action::Broadcast(Control::ExitView(_))))
}

#[test]
fn a_covered_backup_waits_for_no_contact_until_it_is_uncovered() {
    let mut backup = driver(1);
    assert!(backup.settled());
    // Only the primary's liveness matters to a backup.
    assert!(backup.cover(node(2)).is_err());
    assert!(backup.cover(node(1)).is_err());
    assert!(backup.cover(NodeId::from_bytes([9; 16])).is_err());
    backup.cover(node(0)).unwrap();
    assert!(backup.covered(node(0)) && !backup.covered(node(2)));
    assert_eq!(backup.poll(at(500)).unwrap(), None);
    assert_eq!(backup.next_deadline(), None, "no contact timer runs");
    // The proof last held at 450: contact counts from there.
    backup.uncover(node(0), at(450)).unwrap();
    assert!(!backup.covered(node(0)));
    assert_eq!(backup.next_deadline(), Some(at(550)));
    assert_eq!(backup.poll(at(549)).unwrap(), None);
    assert!(exits(backup.poll(at(550)).unwrap()));
}

#[test]
fn a_proof_that_failed_long_ago_is_suspicion_at_once() {
    let mut backup = driver(1);
    backup.cover(node(0)).unwrap();
    backup.poll(at(500)).unwrap();
    backup.uncover(node(0), at(400)).unwrap();
    assert!(exits(backup.poll(at(500)).unwrap()));
    // Uncovering a voter that is not covered changes nothing.
    let mut backup = driver(1);
    backup.poll(at(50)).unwrap();
    backup.uncover(node(0), at(50)).unwrap();
    assert!(exits(backup.poll(at(100)).unwrap()));
}

#[test]
fn a_rest_offer_is_contact_and_an_acceptance_is_a_vote() {
    let mut primary = driver(0);
    let mut backup = driver(1);
    let commit = primary.normal().unwrap().announcement().unwrap();
    backup
        .receive(
            node(0),
            Control::Rest {
                commit,
                source: SOURCE,
            },
            at(90),
        )
        .unwrap();
    assert_eq!(backup.poll(at(189)).unwrap(), None);
    assert!(exits(backup.poll(at(190)).unwrap()));
    let ack = driver(1).normal().unwrap().acknowledgment().unwrap();
    primary.poll(at(100)).unwrap();
    assert!(primary.primary_doubt());
    let accept = Control::RestOk {
        ack,
        source: SOURCE,
    };
    primary.receive(node(1), accept, at(150)).unwrap();
    assert!(!primary.primary_doubt());
    // Only the primary offers, and only it collects acceptances.
    let offer = Control::Rest {
        commit,
        source: SOURCE,
    };
    assert!(driver(2).receive(node(1), offer, at(1)).is_err());
    assert!(driver(2).receive(node(1), accept, at(1)).is_err());
}

#[test]
fn a_resting_primary_sends_no_heartbeat_and_a_covered_one_has_no_doubt() {
    let mut primary = driver(0);
    assert!(primary.cover(node(0)).is_err());
    primary.cover(node(1)).unwrap();
    primary.poll(at(500)).unwrap();
    assert!(!primary.primary_doubt(), "one covered backup is enough");
    // One backup is still spoken to: the heartbeat runs.
    assert!(!primary.resting());
    while primary.poll(at(500)).unwrap().is_some() {}
    assert_eq!(primary.next_deadline(), Some(at(510)));
    primary.cover(node(2)).unwrap();
    primary.rest(true);
    assert!(primary.resting());
    assert_eq!(primary.poll(at(900)).unwrap(), None);
    assert_eq!(primary.next_deadline(), None);
    // Ending the rest sends the held heartbeat at once.
    primary.rest(false);
    assert!(matches!(
        primary.poll(at(900)).unwrap(),
        Some(Action::Broadcast(Control::Commit(_)))
    ));
    // Doubt counts from when the last proof held.
    primary.uncover(node(1), at(850)).unwrap();
    primary.uncover(node(2), at(880)).unwrap();
    primary.poll(at(979)).unwrap();
    assert!(!primary.primary_doubt());
    primary.poll(at(980)).unwrap();
    assert!(primary.primary_doubt());
}

#[test]
fn a_view_change_ends_every_cover_and_the_rest() {
    let mut primary = driver(0);
    primary.cover(node(1)).unwrap();
    primary.cover(node(2)).unwrap();
    primary.rest(true);
    let scope = Scope {
        view: 1,
        ..configuration().scope()
    };
    primary
        .receive(
            node(1),
            Control::StartViewChange(StartViewChange { scope }),
            at(5),
        )
        .unwrap();
    assert!(primary.normal().is_none());
    assert!(!primary.settled());
    assert!(!primary.resting());
    assert!(!primary.covered(node(1)) && !primary.covered(node(2)));
    assert!(
        primary.cover(node(1)).is_err(),
        "no normal role to cover in"
    );
}
