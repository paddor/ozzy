//! Complete driver lifecycle over bounded deterministic transport and disk adapters.

#[path = "support/lifecycle.rs"]
mod model;

#[path = "support/lifecycle/availability.rs"]
mod availability;

#[path = "support/lifecycle/lost_state.rs"]
mod lost_state;

#[path = "support/lifecycle/storage_faults.rs"]
mod storage_faults;

#[path = "support/lifecycle/storage_schedule.rs"]
mod storage_schedule;

#[path = "support/lifecycle/persisting.rs"]
mod persisting;

#[path = "support/lifecycle/storage_repair.rs"]
mod storage_repair;

#[path = "support/lifecycle/churn.rs"]
mod churn;

use model::{Cluster, DiskAction};
use ozzy_replication::wire::{Control, FlowMessage, ReplicaMessage};
use ozzy_replication::{OpNumber, Prefix};

#[test]
fn erased_store_reopens_nonvoting_and_recovers_before_supplying_a_new_quorum() {
    let mut sim = Cluster::new(101);
    sim.links[0][2] = false;
    sim.links[1][2] = false;
    let first = sim.propose(0, 1).unwrap();
    sim.until(100, |sim| sim.ready(0, 0, first.end()));
    assert!(sim.acknowledge(&first));
    // Erase one of the two actual durable copies. The remaining copy must
    // survive, but requiring two CURRENT copies would be a false oracle failure.
    sim.lose_store(1);
    sim.reopen(1);
    assert!(sim.replicas[1].driver.is_none());
    assert_eq!(sim.replicas[1].stable.operations.len(), 0);
    assert!(!sim.replicas[1].stable.admitted);
    sim.links = [[true; 3]; 3];
    sim.until(5000, |sim| {
        (0..3).all(|replica| sim.ready(replica, 1, first.end()))
    });
    assert_eq!(sim.replicas[1].accepted, sim.acknowledged);
    // Keep the original donor offline. Every new quorum needs the rebuilt voter.
    sim.cut(0);
    sim.until(5000, |sim| {
        sim.primary().is_some_and(|primary| primary != 0)
            && (1..3).all(|replica| sim.ready(replica, 1, first.end()))
    });
    let primary = sim.primary().unwrap();
    let next = sim.propose(primary, 2).unwrap();
    sim.until(1000, |sim| {
        (1..3).all(|replica| sim.ready(replica, 1, next.end()))
    });
    assert!(sim.acknowledge(&next));
}

fn is_prepare(packet: &model::Packet) -> bool {
    matches!(
        packet.decode(),
        ReplicaMessage::Prepare(_) | ReplicaMessage::Flow(FlowMessage::Prepare { .. })
    )
}

// Only transport and protocol work. Physical disk work and callbacks remain
// independently controlled by each scenario, including during channel opening.
fn exchange(sim: &mut Cluster) -> usize {
    for replica in 0..3 {
        sim.pump(replica);
    }
    let mut payloads = 0;
    for _ in 0..128 {
        let Some(packet) = sim.network.front() else {
            break;
        };
        payloads += usize::from(is_prepare(packet));
        sim.deliver(0);
    }
    assert!(sim.network.is_empty(), "bounded exchange exhausted");
    payloads
}

#[test]
fn retained_payload_waits_for_disk_without_retransmission_or_false_commit() {
    let mut sim = Cluster::new(81);
    let request = sim.propose(0, 1).unwrap();
    let initial: usize = (0..4).map(|_| exchange(&mut sim)).sum();
    assert_eq!(initial, 2, "one unique payload per backup");
    for replica in &sim.replicas {
        assert_eq!(replica.accepted.last().unwrap().prefix(), request.end());
    }
    for _ in 0..50 {
        sim.now += std::time::Duration::from_millis(20);
        assert_eq!(exchange(&mut sim), 0, "disk delay is not payload loss");
        assert!(!sim.acknowledge(&request));
        for replica in &sim.replicas {
            assert_eq!(replica.stable.operations.len(), 0);
            if let Some(snapshot) = replica.snapshot() {
                assert_eq!(snapshot.committed, Prefix::GENESIS);
            }
        }
    }
    // Receipts must not hide a disk stalled beyond the independent progress
    // deadline. Election remains fenced on disk until those writes can finish.
    assert!(
        sim.replicas
            .iter()
            .all(|replica| { replica.driver.as_ref().unwrap().scope().view > 0 })
    );
    sim.until(5000, |sim| {
        (0..3).all(|replica| sim.ready(replica, 1, request.end()))
    });
    let primary = sim.primary().unwrap();
    let resumed = sim.propose(primary, 2).unwrap();
    sim.until(300, |sim| sim.ready(primary, 1, resumed.end()));
    assert!(sim.acknowledge(&resumed));
}

#[test]
fn normal_records_pipeline_and_lagging_replica_replay() {
    let mut sim = Cluster::new(1);
    sim.links[0][2] = false;
    sim.links[1][2] = false;
    let request = sim.propose(0, 1).unwrap();
    sim.until(100, |sim| sim.ready(0, 0, request.end()));
    assert!(sim.acknowledge(&request));
    let mut requests = Vec::new();
    for id in 2..=5 {
        requests.push(sim.propose(0, id).unwrap());
    }
    assert!(sim.propose(0, 6).is_none());
    assert_eq!(sim.replicas[0].snapshot().unwrap().pending_operations, 4);
    let end = requests.last().unwrap().end();
    sim.until(150, |sim| sim.ready(0, 0, end));
    for request in &requests {
        assert!(sim.acknowledge(request));
    }
    sim.links = [[true; 3]; 3];
    sim.until(1000, |sim| (0..3).all(|replica| sim.ready(replica, 0, end)));
    assert_eq!(sim.replicas[2].accepted, sim.acknowledged);
}

#[test]
fn acknowledged_before_commit_announcement_survives_election_and_old_primary_rejoin() {
    let mut sim = Cluster::new(2);
    sim.links[0][1] = false;
    let request = sim.propose(0, 42).unwrap();
    // Only backup two receives PREPARE; future primary one has no payload.
    // Replication starts while primary disk work has not started.
    for _ in 0..4 {
        exchange(&mut sim);
    }
    assert_eq!(sim.replicas[0].stable.operations.len(), 0);
    for replica in [0, 2] {
        sim.perform_disk(replica);
        sim.notify_disk(replica);
        sim.pump(replica);
        assert!(matches!(
            sim.replicas[replica].pending_disk(),
            Some(DiskAction::Sync(_))
        ));
        sim.perform_disk(replica);
        sim.notify_disk(replica);
    }
    // Capture the first durable ACK without polling primary to announce COMMIT.
    sim.now += std::time::Duration::from_millis(10);
    sim.pump(2);
    let ack = sim.network.iter().position(|packet| matches!(packet.decode(), ReplicaMessage::Control(Control::PrepareOk { ack, .. }) if ack.durable == request.end())).unwrap();
    sim.deliver(ack);
    sim.replicas[0].apply_normal();
    assert!(sim.acknowledge(&request));
    assert_eq!(sim.replicas[1].stable.operations.len(), 0);
    for backup in 1..3 {
        assert_eq!(
            sim.replicas[backup].snapshot().unwrap().committed,
            Prefix::GENESIS
        );
        assert_eq!(sim.replicas[backup].stable.committed, Prefix::GENESIS);
    }
    sim.cut(0);
    sim.until(3000, |sim| {
        sim.ready(1, 1, request.end()) && sim.ready(2, 1, request.end())
    });
    let next = sim.propose(1, 43).unwrap();
    sim.until(300, |sim| sim.ready(1, 1, next.end()));
    assert!(sim.acknowledge(&next));
    sim.links = [[true; 3]; 3];
    sim.reopen(0);
    sim.until(5000, |sim| {
        // Old primary last installed view zero. Rejoin may use the already
        // active view one; it need not disrupt that healthy quorum again.
        (0..3).all(|replica| sim.ready(replica, 1, next.end()))
    });
    let primary = sim.primary().unwrap();
    let after_rejoin = sim.propose(primary, 44).unwrap();
    sim.until(300, |sim| sim.ready(primary, 1, after_rejoin.end()));
    assert!(sim.acknowledge(&after_rejoin));
    sim.until(300, |sim| {
        (0..3).all(|replica| sim.ready(replica, 1, after_rejoin.end()))
    });
    for replica in &sim.replicas {
        assert_eq!(replica.accepted, sim.acknowledged);
    }
}

#[test]
fn physical_sync_without_callback_survives_full_cluster_power_cut() {
    let mut sim = Cluster::new(3);
    let request = sim.propose(0, 7).unwrap();
    for _ in 0..4 {
        exchange(&mut sim);
    }
    let mut old = Vec::new();
    for replica in 0..3 {
        sim.perform_disk(replica);
        sim.notify_disk(replica);
        sim.pump(replica);
        let Some(DiskAction::Sync(ticket)) = sim.replicas[replica].pending_disk() else {
            panic!("sync queued");
        };
        old.push(*ticket);
        sim.perform_disk(replica);
        assert_eq!(
            sim.replicas[replica].snapshot().unwrap().journal.durable,
            OpNumber(0)
        );
    }
    for replica in 0..3 {
        sim.cut(replica);
    }
    for replica in 0..3 {
        sim.reopen(replica);
    }
    for (replica, ticket) in old.into_iter().enumerate() {
        assert!(
            sim.replicas[replica]
                .driver
                .as_mut()
                .unwrap()
                .complete_sync(ticket, sim.now)
                .is_err()
        );
    }
    sim.until(3000, |sim| {
        (0..3).all(|replica| sim.ready(replica, 1, request.end()))
    });
    let primary = sim.primary().unwrap();
    let next = sim.propose(primary, 8).unwrap();
    sim.until(300, |sim| sim.ready(primary, 1, next.end()));
    assert!(sim.acknowledge(&next));
}

#[test]
fn isolated_primary_cannot_commit_and_its_divergent_suffix_is_replaced() {
    let mut sim = Cluster::new(4);
    let first = sim.propose(0, 1).unwrap();
    sim.until(100, |sim| {
        (0..3).all(|replica| sim.ready(replica, 0, first.end()))
    });
    assert!(sim.acknowledge(&first));
    for peer in 1..3 {
        sim.links[0][peer] = false;
        sim.links[peer][0] = false;
    }
    let uncertain = sim.propose(0, 999).unwrap();
    sim.until(100, |sim| {
        sim.replicas[0].stable.operations.len() == uncertain.end().op.0 as usize
    });
    assert!(!sim.acknowledge(&uncertain));
    sim.until(3000, |sim| {
        sim.ready(1, 1, first.end()) && sim.ready(2, 1, first.end())
    });
    assert_eq!(sim.replicas[0].snapshot().unwrap().committed, first.end());
    let replacement = sim.propose(1, 2).unwrap();
    assert_eq!(uncertain.end().op, replacement.end().op);
    assert_ne!(uncertain.end().digest, replacement.end().digest);
    sim.until(300, |sim| sim.ready(1, 1, replacement.end()));
    assert!(sim.acknowledge(&replacement));
    sim.cut(0);
    sim.links = [[true; 3]; 3];
    sim.reopen(0);
    sim.until(5000, |sim| {
        (0..3).all(|replica| sim.ready(replica, 1, replacement.end()))
    });
    assert_eq!(sim.replicas[0].accepted, sim.acknowledged);
}

#[test]
fn late_rejoin_activates_selected_tail_before_catching_up_to_newer_commits() {
    let mut sim = Cluster::new(82);
    let first = sim.propose(0, 1).unwrap();
    sim.until(100, |sim| {
        (0..3).all(|replica| sim.ready(replica, 0, first.end()))
    });
    assert!(sim.acknowledge(&first));
    // Normal commit was ACKed but not separately checkpointed in any manifest.
    // The new view selects this tail with an older committed floor, activates
    // it on two voters, then advances before the third voter installs that view.
    for replica in 0..3 {
        sim.cut(replica);
    }
    sim.reopen(1);
    sim.reopen(2);
    sim.until(3000, |sim| {
        sim.ready(1, 1, first.end()) && sim.ready(2, 1, first.end())
    });
    let next = sim.propose(1, 2).unwrap();
    sim.until(300, |sim| {
        sim.ready(1, 1, next.end()) && sim.ready(2, 1, next.end())
    });
    assert!(sim.acknowledge(&next));
    sim.reopen(0);
    sim.until(3000, |sim| sim.ready(0, 1, next.end()));
    assert_eq!(sim.replicas[0].accepted, sim.acknowledged);
}

#[test]
fn seeded_faults_restore_progress_with_one_voter_permanently_offline() {
    let seeds = std::env::var("OZZY_SIM_SEEDS").map_or(64, |value| value.parse::<u64>().unwrap());
    assert!(seeds > 0);
    for seed in 1..=seeds {
        let mut sim = Cluster::new(seed);
        let first = sim.propose(0, 1).unwrap();
        sim.until(100, |sim| {
            (0..3).all(|replica| sim.ready(replica, 0, first.end()))
        });
        assert!(sim.acknowledge(&first));
        availability::fault_burst(&mut sim);
        // Recovery must use only this healthy pair, not a rescue of every voter.
        sim.cut(2);
        sim.links = [[true; 3]; 3];
        for replica in 0..2 {
            if sim.replicas[replica].driver.is_none() {
                sim.reopen(replica);
            }
        }
        sim.until(10000, |sim| {
            sim.primary().is_some_and(|primary| {
                primary != 2 && sim.ready(0, 1, first.end()) && sim.ready(1, 1, first.end())
            })
        });
        let primary = sim.primary().unwrap();
        let resumed = sim.propose(primary, 10000 + u128::from(seed)).unwrap();
        sim.until(1000, |sim| sim.ready(primary, 1, resumed.end()));
        assert!(sim.acknowledge(&resumed));
        assert!(sim.replicas[2].driver.is_none());
        // Rejoin is a separate obligation after quorum liveness already passed.
        sim.reopen(2);
        sim.until(10000, |sim| {
            (0..3).all(|replica| sim.ready(replica, 1, resumed.end()))
        });
    }
}

#[test]
fn remote_selection_streams_history_larger_than_the_live_pipeline() {
    let mut sim = Cluster::new(9);
    let mut last = sim.propose(0, 1).unwrap();
    sim.until(100, |sim| {
        (0..3).all(|replica| sim.ready(replica, 0, last.end()))
    });
    assert!(sim.acknowledge(&last));
    sim.links[0][1] = false;
    for id in 2..=12 {
        last = sim.propose(0, id).unwrap();
        sim.until(100, |sim| {
            sim.ready(0, 0, last.end()) && sim.ready(2, 0, last.end())
        });
        assert!(sim.acknowledge(&last));
    }
    assert_eq!(sim.replicas[1].accepted.len(), 3);
    sim.cut(0);
    sim.until(3000, |sim| {
        sim.ready(1, 1, last.end()) && sim.ready(2, 1, last.end())
    });
    assert!(sim.replicas[1].transfers_completed > 0);
    assert!(sim.replicas[1].transfer_chunks_received >= 4);
    assert_eq!(sim.replicas[1].accepted, sim.acknowledged);
    let next = sim.propose(1, 13).unwrap();
    sim.until(100, |sim| sim.ready(1, 1, next.end()));
    assert!(sim.acknowledge(&next));
}

fn cut_metadata_action(sim: &mut Cluster, stage: usize, after_publication: bool) -> DiskAction {
    for _ in 0..3000 {
        sim.now += std::time::Duration::from_millis(1);
        for replica in 0..3 {
            sim.pump(replica);
            if replica == 1
                && let Some(action) = sim.replicas[replica].pending_disk()
                && matches!(
                    (stage, action),
                    (0, DiskAction::Promise(_))
                        | (1, DiskAction::Install { .. })
                        | (2, DiskAction::Activate(_))
                )
            {
                let action = action.clone();
                if after_publication {
                    assert!(sim.perform_disk(replica));
                }
                sim.cut(replica);
                return action;
            }
            sim.perform_disk(replica);
            sim.notify_disk(replica);
        }
        for _ in 0..128 {
            if sim.network.is_empty() {
                break;
            }
            sim.deliver(0);
        }
    }
    panic!("metadata crash boundary not reached");
}

#[test]
fn metadata_crash_cuts_preserve_promises_history_and_generation_fences() {
    for stage in 0..3 {
        for after_publication in [false, true] {
            let mut sim = Cluster::new(10 + stage as u64);
            let first = sim.propose(0, 1).unwrap();
            sim.until(100, |sim| {
                (0..3).all(|replica| sim.ready(replica, 0, first.end()))
            });
            assert!(sim.acknowledge(&first));
            sim.cut(0);
            let old = cut_metadata_action(&mut sim, stage, after_publication);
            let stable_view = sim.replicas[1].stable.promised.view;
            let last_normal = sim.replicas[1].stable.last_normal_view;
            sim.reopen(1);
            let driver = sim.replicas[1].driver.as_mut().unwrap();
            assert!(driver.scope().view > last_normal);
            assert!(driver.scope().view >= stable_view);
            match old {
                DiskAction::Promise(ticket) => assert!(driver.complete_promise(ticket).is_err()),
                DiskAction::Install { ticket, .. } => assert!(
                    driver
                        .complete_installation(ticket, ticket.committed(), sim.now)
                        .is_err()
                ),
                DiskAction::Activate(ticket) => {
                    assert!(driver.complete_activation(ticket).is_err());
                }
                _ => unreachable!(),
            }
            sim.until(5000, |sim| {
                sim.ready(1, 1, first.end()) && sim.ready(2, 1, first.end())
            });
            let primary = sim.primary().unwrap();
            let next = sim.propose(primary, 2).unwrap();
            sim.until(300, |sim| sim.ready(primary, 1, next.end()));
            assert!(sim.acknowledge(&next));
            assert!(sim.replicas[0].driver.is_none());
        }
    }
}
