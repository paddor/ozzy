//! Safety faults followed by a healthy quorum and a permanently faulty outsider.
//! Testing strategy, not protocol code, follows
//! [TigerBeetle's liveness mode](https://tigerbeetle.com/blog/2023-07-06-simulation-testing-for-liveness/).

use super::model::Cluster;

#[derive(Debug, Clone, Copy)]
enum Outsider {
    Offline,
    Deaf,
    Mute,
    DiskStalled,
    CallbackStalled,
}

impl Outsider {
    fn install(self, sim: &mut Cluster, outsider: usize) {
        sim.links = [[true; 3]; 3];
        for replica in 0..3 {
            if !sim.replicas[replica].online() {
                sim.reopen(replica);
            }
        }
        match self {
            Self::Offline => sim.cut(outsider),
            Self::Deaf => {
                for source in &mut sim.links {
                    source[outsider] = false;
                }
            }
            Self::Mute => sim.links[outsider] = [false; 3],
            Self::DiskStalled => sim.disk_paused[outsider] = true,
            Self::CallbackStalled => sim.callbacks_paused[outsider] = true,
        }
    }

    fn assert_persistent(self, sim: &Cluster, outsider: usize) {
        match self {
            Self::Offline => assert!(!sim.replicas[outsider].online()),
            Self::Deaf => assert!(sim.links.iter().all(|source| !source[outsider])),
            Self::Mute => assert_eq!(sim.links[outsider], [false; 3]),
            Self::DiskStalled => assert!(sim.disk_paused[outsider]),
            Self::CallbackStalled => assert!(sim.callbacks_paused[outsider]),
        }
    }
}

pub(super) fn fault_burst(sim: &mut Cluster) {
    let mut requests = Vec::new();
    for step in 0..600 {
        sim.now += std::time::Duration::from_millis(1);
        let replica = sim.choose(3);
        match sim.choose(10) {
            0 => sim.pump(replica),
            1 => {
                sim.perform_disk(replica);
            }
            2 => {
                sim.notify_disk(replica);
            }
            3 if !sim.network.is_empty() => {
                let position = sim.choose(sim.network.len());
                sim.deliver(position);
            }
            4 if !sim.network.is_empty() => {
                let position = sim.choose(sim.network.len());
                sim.network.remove(position);
            }
            5 if !sim.network.is_empty() && sim.network.len() < 128 => {
                let position = sim.choose(sim.network.len());
                sim.network.push_back(sim.network[position].clone());
            }
            6 => {
                let peer = (replica + 1 + sim.choose(2)) % 3;
                sim.links[replica][peer] = !sim.links[replica][peer];
            }
            7 if requests.len() < 16 => {
                if let Some(request) = sim.propose(replica, step + 2) {
                    requests.push(request);
                }
            }
            8 => {
                if sim.replicas[replica].online() {
                    sim.cut(replica);
                } else {
                    sim.reopen(replica);
                }
            }
            _ => {}
        }
        requests.retain(|request| !sim.acknowledge(request));
        sim.check();
    }
}

fn run(seed: u64, outsider: usize, fault: Outsider) {
    let mut sim = Cluster::new(seed);
    let first = sim.propose(0, 1).unwrap();
    sim.until(100, |sim| {
        (0..3).all(|replica| sim.ready(replica, 0, first.end()))
    });
    assert!(sim.acknowledge(&first));
    fault_burst(&mut sim);
    recover_and_append(&mut sim, outsider, fault);
}

fn recover_and_append(sim: &mut Cluster, outsider: usize, fault: Outsider) {
    let protected = sim.acknowledged.last().unwrap().prefix();
    fault.install(sim, outsider);
    // An idle primary with a stalled disk can still send valid heartbeats.
    // Supply actual work before expecting its progress timeout to fire.
    if let Some(primary) = sim.primary() {
        let _ = sim.propose(primary, 9000);
    }
    // Do not require the outsider to become normal, finish I/O, or receive data.
    sim.until(10_000, |sim| {
        sim.primary().is_some_and(|primary| {
            let snapshot = sim.replicas[primary].snapshot().unwrap();
            primary != outsider && snapshot.accepted == snapshot.applied
        }) && (0..3)
            .filter(|replica| *replica != outsider)
            .all(|replica| sim.ready(replica, 0, protected))
    });
    for burst in 0..4 {
        let primary = sim.primary().unwrap();
        assert_ne!(primary, outsider);
        let requests: Vec<_> = (0..4)
            .map(|slot| sim.propose(primary, 10_000 + burst * 4 + slot).unwrap())
            .collect();
        assert!(
            sim.propose(primary, 20_000).is_none(),
            "bounded pipeline full"
        );
        let end = requests.last().unwrap().end();
        sim.until(1000, |sim| {
            (0..3)
                .filter(|replica| *replica != outsider)
                .all(|replica| sim.ready(replica, 0, end))
        });
        for request in requests {
            assert!(sim.acknowledge(&request));
        }
        // Keep the fault present beyond the longest election timeout. One
        // successful request just after recovery would miss recurring livelock.
        for _ in 0..100 {
            // One protocol heartbeat per step. Advance virtual idle time, not
            // disk state; every step still services the healthy pair normally.
            sim.now += std::time::Duration::from_millis(9);
            sim.tick();
        }
        fault.assert_persistent(sim, outsider);
    }
}

#[test]
fn healthy_idle_primary_with_stalled_disk_relinquishes_after_new_work() {
    for fault in [Outsider::DiskStalled, Outsider::CallbackStalled] {
        let mut sim = Cluster::new(90);
        let request = sim.propose(0, 1).unwrap();
        sim.until(100, |sim| {
            (0..3).all(|replica| sim.ready(replica, 0, request.end()))
        });
        assert!(sim.acknowledge(&request));
        recover_and_append(&mut sim, 0, fault);
    }
}

#[test]
fn every_healthy_pair_progresses_after_seeded_faults_with_a_permanent_outsider() {
    let seeds = std::env::var("OZZY_SIM_AVAILABILITY_SEEDS")
        .map_or(4, |value| value.parse::<u64>().unwrap());
    assert!(seeds > 0);
    for seed in 1..=seeds {
        for outsider in 0..3 {
            for fault in [
                Outsider::Offline,
                Outsider::Deaf,
                Outsider::Mute,
                Outsider::DiskStalled,
                Outsider::CallbackStalled,
            ] {
                let result = std::panic::catch_unwind(|| run(seed, outsider, fault));
                if let Err(error) = result {
                    eprintln!("availability case: seed={seed} outsider={outsider} fault={fault:?}");
                    std::panic::resume_unwind(error);
                }
            }
        }
    }
}
