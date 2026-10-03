//! One scheduler combines real byte persistence with the production replication core.

use super::model::Cluster;
use ozzy_journal_segment::simulation::Damage;

#[test]
fn publication_failure_cuts_preserve_receipts_across_leader_change() {
    for cut in 1..=7 {
        let mut sim = Cluster::with_storage(200 + cut as u64);
        let first = sim.propose(0, 1).unwrap();
        sim.until(100, |sim| (0..3).all(|id| sim.ready(id, 0, first.end())));
        assert!(sim.acknowledge(&first));
        let next = sim.propose(0, 2).unwrap();
        for _ in 0..4 {
            super::exchange(&mut sim);
        }
        for id in 0..3 {
            sim.perform_disk(id);
            sim.notify_disk(id);
            sim.pump(id);
        }
        sim.replicas[0].storage.as_mut().unwrap().fail_after(cut);
        sim.perform_disk(0);
        assert!(
            !sim.acknowledge(&next),
            "physical publication is not a group confirmation"
        );
        sim.cut(0);
        sim.reopen(0);
        sim.until(5000, |sim| (0..3).all(|id| sim.ready(id, 1, next.end())));
        let primary = sim.primary().unwrap();
        let fresh = sim.propose(primary, 3).unwrap();
        sim.until(1000, |sim| (0..3).all(|id| sim.ready(id, 1, fresh.end())));
        assert!(sim.acknowledge(&fresh));
    }
}

#[test]
fn unsynchronized_torn_bytes_do_not_erase_earlier_receipts() {
    for kept in [1, 7, 127, 1024, 4095] {
        let mut sim = Cluster::with_storage(kept as u64);
        let first = sim.propose(0, 1).unwrap();
        sim.until(100, |sim| (0..3).all(|id| sim.ready(id, 0, first.end())));
        assert!(sim.acknowledge(&first));
        let _uncertain = sim.propose(0, 2).unwrap();
        for _ in 0..4 {
            super::exchange(&mut sim);
        }
        for id in 0..3 {
            sim.perform_disk(id); // Append, without its callback or data barrier.
            sim.replicas[id]
                .storage
                .as_mut()
                .unwrap()
                .tear_unsynced(kept);
            sim.cut(id);
        }
        for id in 0..3 {
            sim.reopen(id);
        }
        sim.until(5000, |sim| (0..3).all(|id| sim.ready(id, 1, first.end())));
        for replica in &sim.replicas {
            assert_eq!(replica.accepted, sim.acknowledged);
        }
    }
}

#[test]
fn seal_persisting_before_body_discards_unconfirmed_group_and_rejoins() {
    let mut sim = Cluster::with_storage(514);
    let first = sim.propose(0, 1).unwrap();
    sim.until(100, |sim| (0..3).all(|id| sim.ready(id, 0, first.end())));
    assert!(sim.acknowledge(&first));
    let uncertain = sim.propose(0, 2).unwrap();
    for _ in 0..4 {
        super::exchange(&mut sim);
    }
    sim.perform_disk(1); // Append without a callback, data barrier, or vote.
    let storage = sim.replicas[1].storage.as_mut().unwrap();
    let unsynced = storage.unsynced_active_range();
    assert!(unsynced.len() >= 4096);
    storage
        .persist_range(&storage.active_name(), unsynced.end - 512..unsynced.end)
        .unwrap();
    assert_eq!(storage.unsynced_active_range(), unsynced);
    sim.cut(1);
    sim.reopen(1);
    assert!(sim.replicas[1].storage_error.is_none());
    assert!(sim.replicas[1].online());
    assert_eq!(sim.replicas[1].accepted, sim.acknowledged);
    sim.until(5000, |sim| {
        (0..3).all(|id| sim.ready(id, 0, uncertain.end()))
    });
    assert_eq!(
        sim.replicas[1].accepted.get(..sim.acknowledged.len()),
        Some(sim.acknowledged.as_slice())
    );

    // New confirmation must include the restarted broker.
    let primary = sim.primary().unwrap();
    let offline = (0..3).find(|id| *id != primary && *id != 1).unwrap();
    sim.cut(offline);
    let fresh = sim.propose(primary, 3).unwrap();
    sim.until(1000, |sim| {
        (0..3)
            .filter(|id| *id != offline)
            .all(|id| sim.ready(id, 0, fresh.end()))
    });
    assert!(sim.acknowledge(&fresh));
}

#[test]
#[should_panic(expected = "ready leader forgot confirmed history")]
fn missing_evidence_barrier_negative_control_reaches_independent_receipt_oracle() {
    let mut sim = Cluster::with_storage(19);
    // Pretend two publishers forgot their evidence barrier, not their data sync.
    // Third broker never receives this confirmed prefix.
    sim.links[0][2] = false;
    sim.links[1][2] = false;
    for id in 0..2 {
        sim.replicas[id]
            .storage
            .as_mut()
            .unwrap()
            .omit_in_place_sync(true);
    }
    let request = sim.propose(0, 1).unwrap();
    sim.until(100, |sim| sim.ready(0, 0, request.end()));
    assert!(sim.acknowledge(&request));
    sim.damage_store(1, Damage::ZeroSuffix(4096));
    sim.replicas[1]
        .storage
        .as_mut()
        .unwrap()
        .omit_in_place_sync(false);
    sim.reopen(1); // Broken publisher lets this broker forget its former copy.
    assert!(sim.replicas[1].storage_error.is_none());
    sim.cut(0); // Its intact surviving payload remains unavailable to elections.
    sim.links = [[true; 3]; 3];
    sim.until(5000, |sim| sim.primary().is_some());
}

#[test]
fn byte_storage_confirmed_damage_election_repair_and_fresh_confirmation() {
    for seed in 1..=4 {
        for damage in [
            Damage::Flip(4300),
            Damage::ZeroSuffix(4096),
            Damage::Truncate(4096),
        ] {
            let mut sim = Cluster::with_storage(seed);
            let first = sim.propose(0, 1).unwrap();
            sim.until(100, |sim| (0..3).all(|id| sim.ready(id, 0, first.end())));
            assert!(sim.acknowledge(&first));
            // Confirmed receipt survives independently of all later protocol state.
            let confirmed = sim.acknowledged.clone();
            sim.damage_store(1, damage);
            sim.reopen(1);
            assert!(sim.replicas[1].storage_error.is_some());
            assert!(!sim.replicas[1].online());
            sim.cut(0);
            sim.reopen(0);
            sim.until(5000, |sim| {
                sim.ready(0, 1, first.end()) && sim.ready(2, 1, first.end())
            });
            sim.quarantine(1);
            sim.until(5000, |sim| (0..3).all(|id| sim.ready(id, 1, first.end())));
            assert_eq!(sim.acknowledged, confirmed);
            assert_eq!(sim.replicas[1].accepted, confirmed);
            // Every following confirmation requires the rebuilt broker.
            sim.cut(0);
            sim.until(5000, |sim| sim.primary().is_some_and(|id| id != 0));
            let primary = sim.primary().unwrap();
            let next = sim.propose(primary, 2).unwrap();
            sim.until(1000, |sim| (1..3).all(|id| sim.ready(id, 1, next.end())));
            assert!(sim.acknowledge(&next));
        }
    }
}

#[test]
fn seeded_network_and_crash_schedules_use_byte_recovery() {
    for seed in 1..=4 {
        let mut sim = Cluster::with_storage(seed);
        let first = sim.propose(0, 1).unwrap();
        sim.until(100, |sim| (0..3).all(|id| sim.ready(id, 0, first.end())));
        assert!(sim.acknowledge(&first));
        super::availability::fault_burst(&mut sim);
        sim.links = [[true; 3]; 3];
        for id in 0..3 {
            if !sim.replicas[id].online() {
                sim.reopen(id);
            }
        }
        sim.until(10_000, |sim| (0..3).all(|id| sim.ready(id, 1, first.end())));
        let primary = sim.primary().unwrap();
        let next = sim.propose(primary, 10000).unwrap();
        sim.until(1000, |sim| (0..3).all(|id| sim.ready(id, 1, next.end())));
        assert!(sim.acknowledge(&next));
    }
}
