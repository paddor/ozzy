//! Repeat bounded seeded schedules with accelerated protocol time and real codecs.

use super::{model::Cluster, storage_schedule};
use ozzy_journal_segment::{BodyEncoding, simulation::Damage};
use ozzy_replication::QuorumPolicy;
use std::{
    fs::File,
    io::Write,
    time::{Duration, Instant},
};

#[test]
fn seeded_ram_recovery_combines_disk_faults_and_dropped_reordered_messages() {
    for seed in 1..=8 {
        ram_recovery(seed);
    }
}

#[test]
#[ignore = "long seeded fault job; OZZY_CHURN_SEED, SECONDS and RESULTS select the run"]
fn long_running_accelerated_fault_churn() {
    let seconds: u64 = std::env::var("OZZY_CHURN_SECONDS")
        .unwrap_or_else(|_| "3600".into())
        .parse()
        .unwrap();
    let mut seed: u64 = std::env::var("OZZY_CHURN_SEED")
        .unwrap_or_else(|_| "1".into())
        .parse()
        .unwrap();
    assert!(seconds > 0 && seed > 0);
    let mut ledger = File::create(std::env::var("OZZY_CHURN_RESULTS").unwrap()).unwrap();
    let start = Instant::now();
    let mut next_report = Duration::ZERO;
    let mut cases = 0;
    while start.elapsed() < Duration::from_secs(seconds) {
        let events = storage_schedule::sequence(seed);
        storage_schedule::run_seed("OZZY_STORAGE", storage_schedule::run, seed, &events);
        ram_recovery(seed);
        cases += 2;
        if start.elapsed() >= next_report {
            writeln!(
                ledger,
                "{{\"elapsed_secs\":{},\"last_seed\":{seed},\"cases\":{cases}}}",
                start.elapsed().as_secs()
            )
            .unwrap();
            ledger.flush().unwrap();
            next_report = start.elapsed() + Duration::from_secs(60);
        }
        seed = seed.checked_add(1).unwrap();
    }
    writeln!(
        ledger,
        "{{\"complete\":true,\"elapsed_secs\":{},\"next_seed\":{seed},\"cases\":{cases}}}",
        start.elapsed().as_secs()
    )
    .unwrap();
}

fn ram_recovery(seed: u64) {
    let mut sim = Cluster::with_storage_policy(seed, QuorumPolicy::Replicated);
    let encoding = if seed.is_multiple_of(2) {
        BodyEncoding::Raw
    } else {
        BodyEncoding::Lz4 {
            min_savings_bytes: 1,
        }
    };
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
    sim.until(100, |sim| (0..3).all(|id| sim.ready(id, 0, first.end())));
    assert!(sim.acknowledge(&first));
    sim.disk_paused[0] = false;
    sim.replicas[0]
        .storage
        .as_mut()
        .unwrap()
        .fail_after(1 + seed as usize % 32);
    sim.perform_disk(0);
    if sim.replicas[0].online() {
        sim.replicas[0]
            .storage
            .as_mut()
            .unwrap()
            .tear_unsynced(1 + seed as usize % 4095);
    }
    sim.cut(0);
    sim.replicas[0].storage.as_mut().unwrap().clear_failure();
    sim.reopen(0);
    assert!(
        !sim.replicas[0].online(),
        "seed {seed}: unproven RAM restart can vote"
    );
    sim.disk_paused = [false; 3];
    sim.until(5000, |sim| (1..3).all(|id| sim.ready(id, 1, first.end())));
    sim.quarantine(0);
    lose_and_reorder(&mut sim);
    sim.until(10_000, |sim| (0..3).all(|id| sim.ready(id, 1, first.end())));
    // A media fault follows recovery, and another recovery must preserve history.
    sim.damage_store(1, Damage::Flip(4300));
    sim.reopen(1);
    assert!(
        sim.replicas[1].storage_error.is_some(),
        "seed {seed}: corrupt store opened"
    );
    sim.quarantine(1);
    lose_and_reorder(&mut sim);
    sim.until(10_000, |sim| (0..3).all(|id| sim.ready(id, 1, first.end())));
    // Remove an original donor. Both recovered copies must supply the new quorum.
    sim.cut(2);
    sim.now += Duration::from_secs(30);
    sim.until(5000, |sim| sim.primary().is_some_and(|id| id != 2));
    let fresh = sim.propose(sim.primary().unwrap(), 2).unwrap();
    sim.until(1000, |sim| (0..2).all(|id| sim.ready(id, 1, fresh.end())));
    assert!(
        sim.acknowledge(&fresh),
        "seed {seed}: repaired pair failed to confirm"
    );
}

fn lose_and_reorder(sim: &mut Cluster) {
    for _ in 0..200 {
        let milliseconds = [1, 5, 20][sim.choose(3)];
        sim.now += Duration::from_millis(milliseconds);
        for id in 0..3 {
            sim.pump(id);
            sim.perform_disk(id);
            sim.notify_disk(id);
        }
        if !sim.network.is_empty() {
            let position = sim.choose(sim.network.len());
            match sim.choose(3) {
                0 => {
                    sim.network.remove(position);
                }
                1 if sim.network.len() < 128 => {
                    sim.network.push_back(sim.network[position].clone());
                }
                _ => sim.deliver(position),
            }
        }
        sim.check();
    }
}
