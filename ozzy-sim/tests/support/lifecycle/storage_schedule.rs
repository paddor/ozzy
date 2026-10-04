//! Raw event words preserve choices when unrelated events are deleted for reduction.

use super::model::Cluster;
use ozzy_journal_segment::simulation::Damage;
use std::time::Duration;

const EVENTS: usize = 400;

pub(super) fn sequence(seed: u64) -> Vec<u64> {
    let mut random = seed.max(1);
    (0..EVENTS)
        .map(|_| {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            random
        })
        .collect()
}

fn encode(events: &[u64]) -> String {
    events
        .iter()
        .map(|event| format!("{event:016x}"))
        .collect::<Vec<_>>()
        .join(",")
}

pub(super) fn run(seed: u64, events: &[u64]) {
    let mut sim = Cluster::with_storage(seed);
    let first = sim.propose(0, 1).unwrap();
    sim.until(100, |sim| (0..3).all(|id| sim.ready(id, 0, first.end())));
    assert!(sim.acknowledge(&first));
    sim.damage_store(1, Damage::ZeroSuffix(4096));
    sim.reopen(1);
    assert!(sim.replicas[1].storage_error.is_some());
    sim.quarantine(1);
    let mut requests = Vec::new();
    let mut proposals = 0;
    for &event in events {
        sim.now += Duration::from_millis(1 + ((event >> 8) % 5));
        let id = ((event >> 4) % 3) as usize;
        let argument = (event >> 16) as usize;
        match event % 14 {
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
                let packet = sim.network[argument % sim.network.len()].clone();
                sim.network.push_back(packet);
            }
            6 => {
                let to = (id + 1 + argument % 2) % 3;
                sim.links[id][to] = !sim.links[id][to];
            }
            7 if proposals < 16 => {
                if let Some(request) = sim.propose(id, 1000 + u128::from(event >> 8)) {
                    requests.push(request);
                    proposals += 1;
                }
            }
            8 => {
                if sim.replicas[id].online() {
                    sim.cut(id);
                } else {
                    sim.reopen(id);
                }
            }
            9 if sim.replicas[id].pending_disk().is_some() => {
                sim.replicas[id]
                    .storage
                    .as_mut()
                    .unwrap()
                    .fail_after(1 + argument % 16);
                sim.perform_disk(id);
            }
            10 if sim.replicas[id].online() => {
                sim.replicas[id]
                    .storage
                    .as_mut()
                    .unwrap()
                    .tear_unsynced(argument % 4096);
                sim.cut(id);
            }
            11 => sim.now += Duration::from_millis([0, 20, 99, 100, 800, 30_000][argument % 6]),
            12 => sim.tick(),
            _ => {}
        }
        requests.retain(|request| !sim.acknowledge(request));
        sim.check();
    }
    // Healthy phase is explicit and bounded; it never fabricates donor history.
    sim.links = [[true; 3]; 3];
    for id in 0..3 {
        sim.replicas[id].storage.as_mut().unwrap().clear_failure();
        if !sim.replicas[id].online() {
            sim.reopen(id);
        }
    }
    let protected = sim.acknowledged.last().unwrap().prefix();
    sim.until(10_000, |sim| {
        (0..3).all(|id| sim.ready(id, 0, protected))
            && sim.primary().is_some_and(|id| {
                let snapshot = sim.replicas[id].snapshot().unwrap();
                snapshot.accepted == snapshot.applied
            })
    });
    // Require new confirmation with the rebuilt broker and one surviving donor.
    sim.cut(0);
    sim.until(5000, |sim| sim.primary().is_some_and(|id| id != 0));
    let primary = sim.primary().unwrap();
    let fresh = sim.propose(primary, u128::MAX).unwrap();
    sim.until(1000, |sim| (1..3).all(|id| sim.ready(id, 0, fresh.end())));
    assert!(sim.acknowledge(&fresh));
}

fn failure(run: fn(u64, &[u64]), seed: u64, events: &[u64]) -> Option<String> {
    std::panic::catch_unwind(|| run(seed, events))
        .err()
        .map(|error| {
            let message = error
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| error.downcast_ref::<&str>().map(|s| (*s).to_owned()))
                .unwrap_or_else(|| "non-string failure".into());
            // Preserve the complete assertion. A shorter trace reproducing a
            // different failure is not a valid reduction of this counterexample.
            message
        })
}

#[test]
fn replayable_storage_and_network_faults_during_repair() {
    sweep("OZZY_STORAGE", run);
}

pub(super) fn sweep(prefix: &str, run: fn(u64, &[u64])) {
    let first = std::env::var(format!("{prefix}_SEED")).map_or(1, |s| s.parse::<u64>().unwrap());
    let seeds = std::env::var(format!("{prefix}_SEEDS")).map_or(16, |s| s.parse::<u64>().unwrap());
    assert!(seeds > 0);
    let replay = std::env::var(format!("{prefix}_TRACE")).ok().map(|s| {
        if s.is_empty() {
            return Vec::new();
        }
        let events: Vec<_> = s
            .split(',')
            .map(|word| u64::from_str_radix(word, 16).unwrap())
            .collect();
        assert!(events.len() <= EVENTS);
        events
    });
    for seed in first..first + if replay.is_some() { 1 } else { seeds } {
        let events = replay.clone().unwrap_or_else(|| sequence(seed));
        run_seed(prefix, run, seed, &events);
    }
}

pub(super) fn run_seed(prefix: &str, run: fn(u64, &[u64]), seed: u64, events: &[u64]) {
    if let Some(class) = failure(run, seed, events) {
        eprintln!("{prefix}_SEED={seed} {prefix}_TRACE={}", encode(events));
        let reduction = ozzy_sim::schedule::minimize(events, 64, |candidate| {
            failure(run, seed, candidate).as_ref() == Some(&class)
        });
        eprintln!(
            "reduced in {} attempts: {prefix}_SEED={seed} {prefix}_TRACE={}",
            reduction.attempts,
            encode(&reduction.events)
        );
        panic!("storage schedule failure: {class}");
    }
}
