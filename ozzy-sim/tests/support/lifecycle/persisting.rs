//! RAM-copy confirmations with production byte storage and explicit restart fencing.

use super::model::Cluster;
use ozzy_journal_segment::BodyEncoding;
use ozzy_replication::wire::{Control, ReplicaMessage};
use ozzy_replication::{OpNumber, QuorumPolicy};

#[test]
fn ram_confirmation_precedes_storage_and_bounds_the_persistence_pipeline() {
    let mut sim = Cluster::with_storage_policy(71, QuorumPolicy::Replicated);
    sim.disk_paused = [true; 3];
    let first = sim.propose(0, 1).unwrap();
    sim.until(100, |sim| sim.ready(0, 0, first.end()));
    assert!(sim.acknowledge(&first));
    let next = sim.propose(0, 2).unwrap();
    sim.until(100, |sim| sim.ready(0, 0, next.end()));
    assert!(sim.acknowledge(&next));
    assert!(
        sim.propose(0, 3).is_none(),
        "RAM application must not release unwritten capacity"
    );
    for replica in &sim.replicas {
        assert_eq!(replica.stable.operations.len(), 0);
        assert_eq!(replica.snapshot().unwrap().journal.durable, OpNumber(0));
    }
    sim.disk_paused = [false; 3];
    sim.until(100, |sim| {
        sim.replicas
            .iter()
            .all(|r| r.snapshot().unwrap().pending_operations == 0)
    });
    for id in 3..20 {
        let request = sim.propose(0, id).unwrap();
        sim.until(100, |sim| {
            sim.replicas
                .iter()
                .all(|r| r.snapshot().unwrap().journal.durable >= request.end().op)
        });
        sim.until(100, |sim| (0..3).all(|i| sim.ready(i, 0, request.end())));
        assert!(sim.acknowledge(&request));
    }
}

#[test]
fn unannounced_ram_confirmation_survives_leader_loss_and_stale_disk_recovery() {
    let mut sim = Cluster::with_storage_policy(72, QuorumPolicy::Replicated);
    sim.disk_paused = [true; 3];
    sim.links[0][2] = false;
    sim.links[1][2] = false;
    let first = sim.propose(0, 1).unwrap();
    for _ in 0..100 {
        sim.now += std::time::Duration::from_millis(1);
        for i in 0..3 {
            sim.pump(i);
        }
        sim.network.retain(|packet| {
            !matches!(packet.decode(), ReplicaMessage::Control(Control::Commit(_)))
        });
        while !sim.network.is_empty() {
            sim.deliver(0);
        }
        if sim.ready(0, 0, first.end()) {
            break;
        }
    }
    assert!(sim.acknowledge(&first));
    assert_eq!(
        sim.replicas[1].snapshot().unwrap().committed.op,
        OpNumber(0)
    );
    assert!(sim.replicas.iter().all(|r| r.stable.operations.is_empty()));
    sim.cut(0);
    sim.reopen(0);
    assert!(
        !sim.replicas[0].online(),
        "stale but intact disk cannot vote"
    );
    assert!(
        sim.replicas[0]
            .storage_error
            .as_ref()
            .unwrap()
            .contains("memory")
    );
    sim.disk_paused = [false; 3];
    sim.links = [[true; 3]; 3];
    sim.until(5000, |sim| (1..3).all(|i| sim.ready(i, 1, first.end())));
    sim.quarantine(0);
    sim.until(5000, |sim| (0..3).all(|i| sim.ready(i, 1, first.end())));
    assert_eq!(sim.replicas[0].accepted, sim.acknowledged);
    // The recovered broker must help confirm new work with an original donor gone.
    sim.cut(2);
    sim.until(5000, |sim| {
        sim.primary().is_some_and(|primary| primary != 2)
            && (0..2).all(|i| sim.ready(i, 3, first.end()))
    });
    let primary = sim.primary().unwrap();
    let next = sim.propose(primary, 2).unwrap();
    sim.until(1000, |sim| (0..2).all(|i| sim.ready(i, 1, next.end())));
    assert!(sim.acknowledge(&next));
}

#[test]
fn failed_and_torn_background_writes_preserve_ram_confirmations_through_repair() {
    for encoding in [
        BodyEncoding::Raw,
        BodyEncoding::Lz4 {
            min_savings_bytes: 1,
        },
    ] {
        let mut failed = 0;
        let mut torn = 0;
        for cut in [1, 2, 3, 4, 8, 12, 16, 20, 24, 28, 32, 64] {
            let mut sim = Cluster::with_storage_policy(100 + cut as u64, QuorumPolicy::Replicated);
            for replica in &mut sim.replicas {
                replica
                    .storage
                    .as_mut()
                    .unwrap()
                    .set_body_encoding(encoding)
                    .unwrap();
            }
            sim.disk_paused = [true; 3];
            let first = sim.propose(0, 1).unwrap();
            sim.until(100, |sim| (0..3).all(|i| sim.ready(i, 0, first.end())));
            assert!(sim.acknowledge(&first));
            let expected = sim.acknowledged.clone();
            sim.disk_paused[0] = false;
            sim.replicas[0].storage.as_mut().unwrap().fail_after(cut);
            sim.perform_disk(0);
            if sim.replicas[0].online() {
                sim.replicas[0]
                    .storage
                    .as_mut()
                    .unwrap()
                    .tear_unsynced(1 + cut * 127 % 4095);
                torn += 1;
            } else {
                failed += 1;
            }
            sim.cut(0);
            sim.replicas[0].storage.as_mut().unwrap().clear_failure();
            sim.reopen(0);
            assert!(!sim.replicas[0].online());
            assert!(sim.replicas[0].storage_error.is_some());
            sim.disk_paused = [false; 3];
            sim.until(5000, |sim| (1..3).all(|i| sim.ready(i, 1, first.end())));
            sim.quarantine(0);
            sim.until(5000, |sim| (0..3).all(|i| sim.ready(i, 1, first.end())));
            assert_eq!(sim.replicas[0].accepted, expected);
            assert_eq!(sim.acknowledged, expected);
            // Keep the original failed leader available, remove another broker.
            // Every new confirmation now requires the repaired copy.
            sim.cut(2);
            sim.until(5000, |sim| sim.primary().is_some_and(|i| i != 2));
            let next = sim.propose(sim.primary().unwrap(), 2).unwrap();
            sim.until(1000, |sim| (0..2).all(|i| sim.ready(i, 1, next.end())));
            assert!(sim.acknowledge(&next));
        }
        assert!(
            failed > 0 && torn > 0,
            "fixture must exercise both write failures and torn bytes"
        );
    }
}

#[test]
fn seeded_ram_persistence_schedules_progress_with_a_permanently_stalled_outsider() {
    super::storage_schedule::sweep("OZZY_PERSISTING", run_schedule);
}

#[test]
fn ram_confirmed_leader_with_stalled_persistence_yields_to_healthy_pair() {
    // Minimized seed 15: no network fault burst is necessary. A RAM-confirmed
    // prefix must not hide either a stalled write or its undelivered callback.
    for seed in [6, 15] {
        run_schedule(seed, &[]);
    }
}

#[test]
fn interrupted_memory_voter_repair_never_reopens_with_unproven_authority() {
    use super::storage_repair::{replacement, until_action};
    for publication in [false, true] {
        let mut baseline = replacement(QuorumPolicy::Replicated);
        until_action(&mut baseline, publication);
        let before = baseline.replicas[2].storage.as_ref().unwrap().trace().len();
        baseline.perform_disk(2);
        let actions = baseline.replicas[2].storage.as_ref().unwrap().trace().len() - before;
        assert!(actions > 0);
        for cut in 1..=actions + 1 {
            let mut sim = replacement(QuorumPolicy::Replicated);
            let protected = sim.acknowledged.last().unwrap().prefix();
            until_action(&mut sim, publication);
            sim.replicas[2].storage.as_mut().unwrap().fail_after(cut);
            sim.perform_disk(2);
            assert!(sim.replicas[2].snapshot().is_none());
            sim.cut(2); // Also discard a successful but undelivered publication callback.
            sim.replicas[2].storage.as_mut().unwrap().clear_failure();
            sim.reopen(2);
            assert!(sim.replicas[2].snapshot().is_none());
            if let Some(error) = &sim.replicas[2].storage_error {
                assert!(
                    error.contains("memory"),
                    "unexpected restart refusal: {error}"
                );
                sim.quarantine(2);
            }
            sim.until(10_000, |sim| (0..3).all(|id| sim.ready(id, 0, protected)));
            assert_eq!(sim.replicas[2].accepted, sim.acknowledged);
            // Both original donors stay healthy throughout repair. Only after
            // exact recovery succeeds may the fresh record require the rebuilt broker.
            sim.cut(0);
            sim.until(5000, |sim| sim.primary().is_some_and(|id| id != 0));
            let request = sim.propose(sim.primary().unwrap(), 99).unwrap();
            sim.until(1000, |sim| (1..3).all(|id| sim.ready(id, 1, request.end())));
            assert!(sim.acknowledge_retry(&request));
        }
    }
}

fn run_schedule(seed: u64, events: &[u64]) {
    let mut sim = Cluster::with_storage_policy(seed, QuorumPolicy::Replicated);
    if seed.is_multiple_of(2) {
        for replica in &mut sim.replicas {
            replica
                .storage
                .as_mut()
                .unwrap()
                .set_body_encoding(BodyEncoding::Lz4 {
                    min_savings_bytes: 1,
                })
                .unwrap();
        }
    }
    sim.disk_paused = [true; 3];
    let first = sim.propose(0, 1).unwrap();
    sim.until(100, |sim| (0..3).all(|i| sim.ready(i, 0, first.end())));
    assert!(sim.acknowledge(&first));
    sim.disk_paused = [false; 3];
    let outsider = (seed % 3) as usize;
    if (seed / 3).is_multiple_of(2) {
        sim.disk_paused[outsider] = true;
    } else {
        sim.callbacks_paused[outsider] = true;
    }
    let mut requests = Vec::new();
    for (step, &event) in events.iter().enumerate() {
        sim.now += std::time::Duration::from_millis(1 + (event >> 8) % 5);
        let id = ((event >> 4) % 3) as usize;
        let argument = (event >> 16) as usize;
        match event % 9 {
            0 => sim.pump(id),
            1 => {
                sim.perform_disk(id);
            }
            2 => {
                sim.notify_disk(id);
            }
            3 if !sim.network.is_empty() => sim.deliver(argument % sim.network.len()),
            4 if !sim.network.is_empty() => {
                sim.network.remove(argument % sim.network.len());
            }
            5 if !sim.network.is_empty() && sim.network.len() < 128 => {
                sim.network
                    .push_back(sim.network[argument % sim.network.len()].clone());
            }
            6 => {
                let to = (id + 1 + argument % 2) % 3;
                sim.links[id][to] = !sim.links[id][to];
            }
            7 if requests.len() < 8 => {
                if let Some(request) = sim.propose(id, 1000 + step as u128) {
                    requests.push(request);
                }
            }
            _ => sim.tick(),
        }
        requests.retain(|request| !sim.acknowledge(request));
        sim.check();
    }
    sim.links = [[true; 3]; 3];
    let protected = sim.acknowledged.last().unwrap().prefix();
    sim.until(10_000, |sim| {
        sim.primary().is_some_and(|id| id != outsider)
            && (0..3).filter(|i| *i != outsider).all(|i| {
                sim.ready(i, 0, protected)
                    && sim.replicas[i].snapshot().unwrap().pending_operations == 0
            })
    });
    for id in 10_000..10_016 {
        let request = sim.propose(sim.primary().unwrap(), id).unwrap();
        let mut confirmed = false;
        for _ in 0..1000 {
            if sim.acknowledge_retry(&request) {
                confirmed = true;
                break;
            }
            sim.tick();
        }
        assert!(
            confirmed,
            "healthy pair did not confirm the exact record retry"
        );
        sim.until(1000, |sim| {
            (0..3).filter(|i| *i != outsider).all(|i| {
                sim.ready(i, 0, request.end())
                    && sim.replicas[i].snapshot().unwrap().pending_operations == 0
            })
        });
    }
    assert!(sim.disk_paused[outsider] || sim.callbacks_paused[outsider]);
}
