//! Lost storage, interrupted replacement publication, and restored-quorum work.

use super::model::{Cluster, DiskAction, RecoveryDisk};

fn replacement(seed: u64, lost: usize) -> Cluster {
    let mut sim = Cluster::new(seed);
    for id in 1..=8 {
        let request = sim.propose(0, id).unwrap();
        sim.until(100, |sim| {
            (0..3).all(|replica| sim.ready(replica, 0, request.end()))
        });
        assert!(sim.acknowledge(&request));
    }
    sim.lose_store(lost);
    sim.reopen(lost);
    assert!(sim.replicas[lost].driver.is_none());
    assert!(!sim.replicas[lost].stable.admitted);
    sim
}

fn finish_with_rebuilt_voter(sim: &mut Cluster, lost: usize) {
    let protected = sim.acknowledged.last().unwrap().prefix();
    sim.links = [[true; 3]; 3];
    sim.disk_paused = [false; 3];
    sim.callbacks_paused = [false; 3];
    for replica in 0..3 {
        if !sim.replicas[replica].online() {
            sim.reopen(replica);
        }
    }
    sim.until(10_000, |sim| {
        (0..3).all(|replica| sim.ready(replica, 1, protected))
            && sim.primary().is_some_and(|primary| {
                let snapshot = sim.replicas[primary].snapshot().unwrap();
                snapshot.accepted == snapshot.applied
            })
    });
    assert_eq!(
        sim.replicas[lost].accepted.get(..sim.acknowledged.len()),
        Some(sim.acknowledged.as_slice())
    );
    let offline = (lost + 1) % 3;
    sim.cut(offline);
    sim.until(5000, |sim| {
        (0..3)
            .filter(|replica| *replica != offline)
            .all(|replica| sim.ready(replica, 1, protected))
            && sim.primary().is_some_and(|primary| primary != offline)
    });
    let primary = sim.primary().unwrap();
    let next = sim.propose(primary, 50_000).unwrap();
    sim.until(1000, |sim| {
        (0..3)
            .filter(|replica| *replica != offline)
            .all(|replica| sim.ready(replica, 1, next.end()))
    });
    assert!(sim.acknowledge(&next));
    assert_eq!(sim.replicas[lost].accepted, sim.acknowledged);
}

fn until_receiver_disk(sim: &mut Cluster, publication: bool) {
    for _ in 0..5000 {
        sim.now += std::time::Duration::from_millis(1);
        for replica in 0..3 {
            sim.pump(replica);
        }
        if matches!(sim.replicas[2].pending_disk(), Some(DiskAction::Recovery(action)) if matches!(action, RecoveryDisk::Publish(_)) == publication)
        {
            assert!(matches!(
                sim.replicas[2].pending_disk(),
                Some(DiskAction::Recovery(
                    RecoveryDisk::Stage { .. } | RecoveryDisk::Publish(_)
                ))
            ));
            return;
        }
        for replica in 0..3 {
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
    panic!("receiver did not reach publication={publication}");
}

#[test]
fn recovery_disk_cuts_before_and_after_physical_work_preserve_acknowledged_bytes() {
    for publication in [false, true] {
        for performed in [false, true] {
            let mut sim = replacement(102, 2);
            until_receiver_disk(&mut sim, publication);
            assert!(sim.replicas[2].driver.is_none());
            if performed {
                assert!(sim.perform_disk(2));
            }
            assert_eq!(sim.replicas[2].stable.admitted, publication && performed);
            // No callback was observed. A power cut discards staged buffered
            // bytes; a fully published replacement instead reopens fenced.
            sim.cut(2);
            if !(publication && performed) {
                assert_eq!(sim.replicas[2].stable.operations.len(), 0);
            }
            sim.reopen(2);
            assert!(sim.replicas[2].snapshot().is_none());
            assert_eq!(sim.replicas[2].driver.is_some(), publication && performed);
            finish_with_rebuilt_voter(&mut sim, 2);
        }
    }
}

#[test]
fn one_responder_and_duplicate_replies_cannot_admit_empty_storage() {
    use ozzy_replication::wire::{RecoveryMessage, ReplicaMessage};
    let mut sim = replacement(103, 2);
    sim.links[1][2] = false;
    let mut duplicates = 0;
    for _ in 0..1000 {
        sim.now += std::time::Duration::from_millis(1);
        for replica in 0..3 {
            sim.pump(replica);
            sim.perform_disk(replica);
            sim.notify_disk(replica);
        }
        if let Some(reply) = sim
            .network
            .iter()
            .find(|packet| {
                packet.from == 0
                    && packet.to == 2
                    && matches!(
                        packet.decode(),
                        ReplicaMessage::Recovery(RecoveryMessage::State(_))
                    )
            })
            .cloned()
        {
            assert!(sim.network.len() < 128);
            sim.network.push_back(reply);
            duplicates += 1;
        }
        for _ in 0..128 {
            if sim.network.is_empty() {
                break;
            }
            sim.deliver(0);
        }
        assert!(sim.replicas[2].driver.is_none());
        assert_eq!(sim.replicas[2].stable.operations.len(), 0);
        assert!(!sim.replicas[2].stable.admitted);
    }
    assert!(
        duplicates >= 2,
        "never delivered duplicate primary responses"
    );
    finish_with_rebuilt_voter(&mut sim, 2);
}

#[test]
fn delayed_pre_loss_ack_cannot_omit_the_primarys_unsynced_accepted_history() {
    use ozzy_replication::wire::{Control, RecoveryMessage, ReplicaMessage};
    use ozzy_replication::{OpNumber, Prefix};
    let mut sim = Cluster::new(104);
    sim.links[0][2] = false;
    sim.links[1][2] = false;
    let request = sim.propose(0, 1).unwrap();
    for _ in 0..4 {
        super::exchange(&mut sim);
    }
    sim.perform_disk(1);
    sim.notify_disk(1);
    sim.pump(1);
    assert!(matches!(
        sim.replicas[1].pending_disk(),
        Some(DiskAction::Sync(_))
    ));
    sim.perform_disk(1);
    sim.notify_disk(1);
    sim.now += std::time::Duration::from_millis(10);
    sim.pump(1);
    let position = sim.network.iter().position(|packet| {
        packet.from == 1 && packet.to == 0
            && matches!(packet.decode(), ReplicaMessage::Control(Control::PrepareOk { ack, .. }) if ack.durable == request.end())
    }).unwrap();
    let old_ack = sim.network.remove(position).unwrap();
    sim.lose_store(1);
    sim.reopen(1);
    sim.links[1][2] = true;
    sim.disk_paused[0] = true;
    // Capture a recovery snapshot while the primary's own write is still
    // unsynchronized. Its response cannot be sent until donor capture finishes.
    for _ in 0..20 {
        sim.tick();
    }
    assert_eq!(sim.replicas[0].stable.operations.len(), 0);
    sim.network.push_front(old_ack);
    sim.deliver(0);
    assert_eq!(
        sim.replicas[0].snapshot().unwrap().journal.durable,
        OpNumber(0)
    );
    assert!(!sim.acknowledge(&request));
    sim.disk_paused[0] = false;
    let mut full_snapshot = false;
    for _ in 0..100 {
        sim.now += std::time::Duration::from_millis(1);
        for replica in 0..3 {
            sim.pump(replica);
            sim.perform_disk(replica);
            sim.notify_disk(replica);
        }
        for packet in &sim.network {
            if packet.from == 0
                && packet.to == 1
                && let ReplicaMessage::Recovery(RecoveryMessage::State(state)) = packet.decode()
            {
                let log = state.response.primary.unwrap();
                assert_eq!(log.accepted, request.end());
                assert_eq!(log.committed, Prefix::GENESIS);
                full_snapshot = true;
            }
        }
        for _ in 0..128 {
            if sim.network.is_empty() {
                break;
            }
            sim.deliver(0);
        }
        if full_snapshot {
            break;
        }
    }
    assert!(full_snapshot);
    sim.until(100, |sim| sim.ready(0, 0, request.end()));
    assert!(sim.acknowledge(&request));
    assert_eq!(sim.replicas[2].stable.operations.len(), 0);
    finish_with_rebuilt_voter(&mut sim, 1);
}

#[test]
fn seeded_lost_store_disk_network_and_callback_faults_restore_quorum_progress() {
    let seeds =
        std::env::var("OZZY_SIM_RECOVERY_SEEDS").map_or(16, |value| value.parse::<u64>().unwrap());
    assert!(seeds > 0);
    for seed in 1..=seeds {
        for lost in 0..3 {
            let mut sim = replacement(seed, lost);
            super::availability::fault_burst(&mut sim);
            finish_with_rebuilt_voter(&mut sim, lost);
        }
    }
}
