//! Every byte/publication cut through a multi-chunk replacement, plus refusal cases.

use super::model::{Cluster, DiskAction, RecoveryDisk};
use ozzy_journal_segment::simulation::Damage;

pub(super) fn replacement(policy: ozzy_replication::QuorumPolicy) -> Cluster {
    let mut sim = Cluster::with_storage_policy(3000, policy);
    for id in 1..=8 {
        let request = sim.propose(0, id).unwrap();
        sim.until(100, |sim| {
            (0..3).all(|id| {
                sim.ready(id, 0, request.end())
                    && sim.replicas[id].snapshot().unwrap().pending_operations == 0
            })
        });
        assert!(sim.acknowledge(&request));
    }
    sim.damage_store(2, Damage::ZeroSuffix(4096));
    sim.quarantine(2);
    sim
}

pub(super) fn until_action(sim: &mut Cluster, publication: bool) {
    for _ in 0..5000 {
        sim.now += std::time::Duration::from_millis(1);
        for id in 0..3 {
            sim.pump(id);
        }
        if matches!(sim.replicas[2].pending_disk(), Some(DiskAction::Recovery(action))
            if matches!(action, RecoveryDisk::Publish(_)) == publication)
        {
            return;
        }
        for id in 0..3 {
            sim.perform_disk(id);
            sim.notify_disk(id);
        }
        for _ in 0..128 {
            if sim.network.is_empty() {
                break;
            }
            sim.deliver(0);
        }
    }
    panic!("repair action not reached: publication={publication}");
}

fn finish(sim: &mut Cluster) {
    let protected = sim.acknowledged.last().unwrap().prefix();
    // Restart the old donor too: replacement must obtain current group authority.
    sim.cut(0);
    sim.reopen(0);
    sim.until(10_000, |sim| (0..3).all(|id| sim.ready(id, 1, protected)));
    assert_eq!(sim.replicas[2].accepted, sim.acknowledged);
    sim.cut(0);
    sim.until(5000, |sim| sim.primary().is_some_and(|id| id != 0));
    let request = sim.propose(sim.primary().unwrap(), 99).unwrap();
    sim.until(1000, |sim| (1..3).all(|id| sim.ready(id, 1, request.end())));
    assert!(sim.acknowledge(&request));
}

#[test]
fn every_repair_staging_and_publication_io_cut_retries_with_fresh_authority() {
    for publication in [false, true] {
        let mut baseline = replacement(ozzy_replication::QuorumPolicy::Durable);
        until_action(&mut baseline, publication);
        let before = baseline.replicas[2].storage.as_ref().unwrap().trace().len();
        baseline.perform_disk(2);
        let actions = baseline.replicas[2].storage.as_ref().unwrap().trace().len() - before;
        assert!(actions > 0);
        for cut in 1..=actions + 1 {
            let mut sim = replacement(ozzy_replication::QuorumPolicy::Durable);
            until_action(&mut sim, publication);
            sim.replicas[2].storage.as_mut().unwrap().fail_after(cut);
            sim.perform_disk(2);
            assert!(sim.replicas[2].snapshot().is_none());
            // Discard any callback, including after fully durable publication.
            sim.cut(2);
            sim.reopen(2);
            assert!(
                sim.replicas[2].storage_error.is_none(),
                "publication={publication} cut={cut}"
            );
            assert!(sim.replicas[2].snapshot().is_none());
            finish(&mut sim);
        }
    }
}

#[test]
fn two_damaged_payload_copies_remain_nonvoting_without_recovery_authority() {
    let mut sim = Cluster::with_storage(333);
    sim.links[0][2] = false;
    sim.links[1][2] = false;
    let request = sim.propose(0, 1).unwrap();
    sim.until(100, |sim| sim.ready(0, 0, request.end()));
    assert!(sim.acknowledge(&request));
    for id in 0..2 {
        sim.damage_store(id, Damage::Truncate(4096));
        sim.reopen(id);
        assert!(sim.replicas[id].storage_error.is_some());
        sim.quarantine(id);
    }
    sim.links = [[true; 3]; 3];
    for _ in 0..3000 {
        sim.tick();
        assert!(sim.primary().is_none());
        assert!((0..2).all(|id| sim.replicas[id].driver.is_none()));
    }
    assert_eq!(sim.acknowledged.last().unwrap().prefix(), request.end());
}

#[test]
fn correlated_authority_damage_refuses_reopen_and_quarantine() {
    for name in [
        "identity",
        "CONFIGURATION",
        "CURRENT",
        "MANIFEST.1",
        "DURABLE",
    ] {
        let mut sim = Cluster::with_storage(4000);
        let request = sim.propose(0, 1).unwrap();
        sim.until(100, |sim| (0..3).all(|id| sim.ready(id, 0, request.end())));
        assert!(sim.acknowledge(&request));
        for id in 0..2 {
            sim.cut(id);
            let storage = sim.replicas[id].storage.as_mut().unwrap();
            storage.damage(name, Damage::Flip(0));
            if name == "DURABLE" {
                // One damaged copy is tolerated; damage both.
                storage.damage(name, Damage::Flip(64 * 1024));
            }
            sim.reopen(id);
            assert!(sim.replicas[id].storage_error.is_some());
            assert!(
                sim.replicas[id]
                    .storage
                    .as_mut()
                    .unwrap()
                    .quarantine()
                    .is_err()
            );
        }
        for _ in 0..1000 {
            sim.tick();
        }
        assert!(sim.primary().is_none());
        assert_eq!(sim.acknowledged.last().unwrap().prefix(), request.end());
    }
}
