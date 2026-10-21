//! Seeded single-thread cluster over production partition schedulers.
//!
//! Three brokers each run a production `PartitionActors` shard with one
//! replicated group over production shard journals on a simulated device.
//! One seed drives the network, disks, clients, and broker order; virtual
//! time moves in 10 ms ticks. Safety checks run after every tick; a healing
//! drain then requires convergence. A failure prints its reproduce command.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable,
    clippy::too_many_lines,
    clippy::too_many_arguments,
    reason = "simulation test support"
)]

mod broker;
mod check;
mod clock;
mod disk;
mod network;
mod seeds;
mod sim;
mod workload;

use broker::{MAX_SEGMENTS, SEGMENT_BYTES};
use network::Profile;
use sim::{
    Fault::{self, IsolateLeader},
    Options, Report,
};

/// One scenario: ticks of proposals, scripted faults, and journal shape.
/// `random` has only the profile's random faults.
#[derive(Debug, Clone, Copy)]
struct Scenario {
    ticks: u64,
    script: &'static [Fault],
    batch: usize,
    max_segments: usize,
    segment_bytes: u64,
    sparse: &'static [usize],
    /// Caps the profile's floors for confirmed proposals and compared ops,
    /// where a full journal bounds them.
    max_floor: usize,
    min_applied: u64,
    min_refused: u64,
}

const RANDOM: Scenario = Scenario {
    ticks: 1500,
    script: &[],
    batch: 1,
    max_segments: MAX_SEGMENTS,
    segment_bytes: SEGMENT_BYTES,
    sparse: &[],
    max_floor: usize::MAX,
    min_applied: 0,
    min_refused: 0,
};

/// A lost leader stays cut off for 15 s: three view attempts under the
/// doubling 2 s to 8 s election timeouts, over the slowest profiles.
const LOST_LEADER: Fault = IsolateLeader {
    at: 2500,
    ticks: 1500,
};

fn scenario(name: &str) -> Option<Scenario> {
    Some(match name {
        "random" => RANDOM,
        "failover" => Scenario {
            script: &[IsolateLeader {
                at: 500,
                ticks: 1500,
            }],
            ..RANDOM
        },
        "long-history" => Scenario {
            ticks: 3000,
            script: &[LOST_LEADER],
            ..RANDOM
        },
        "very-long-history" => Scenario {
            ticks: 8000,
            script: &[IsolateLeader {
                at: 7500,
                ticks: 1500,
            }],
            ..RANDOM
        },
        "repeated-failover" => Scenario {
            ticks: 3500,
            script: &[
                IsolateLeader {
                    at: 500,
                    ticks: 800,
                },
                IsolateLeader {
                    at: 1500,
                    ticks: 800,
                },
                IsolateLeader {
                    at: 2500,
                    ticks: 800,
                },
            ],
            ..RANDOM
        },
        // Packing gives sparse 32 KiB segments' operations dense ones, and
        // four of the six stay free for followers that need more. Clients are
        // refused after about 100 operations, then the full leader is lost.
        "full" => Scenario {
            ticks: 3000,
            script: &[LOST_LEADER],
            max_segments: 6,
            segment_bytes: 32 * 1024,
            max_floor: 20,
            min_refused: 1,
            ..RANDOM
        },
        // Both followers write one operation per group while the first
        // leader packs each proposal's four into one. Six 64 KiB segments of
        // 15 groups hold fewer than 90 of their operations, so converging
        // past 120 needs packing. The lost leader hands over to a sparse one.
        "sparse-followers" => Scenario {
            ticks: 3000,
            script: &[IsolateLeader {
                at: 800,
                ticks: 1500,
            }],
            batch: 4,
            max_segments: 6,
            segment_bytes: 64 * 1024,
            sparse: &[1, 2],
            max_floor: 20,
            min_applied: 120,
            ..RANDOM
        },
        _ => return None,
    })
}

/// Proposals and faults for the scenario, then up to 30 s to heal and
/// converge. Vacuity floors fit each profile's fault rate.
fn options(profile: &str, scenario: &str) -> Options {
    let scenario = self::scenario(scenario).expect("known scenario");
    let floor = match profile {
        "none" | "slow" => 100,
        "light" => 20,
        _ => 5,
    }
    .min(scenario.max_floor);
    Options {
        ticks: scenario.ticks,
        drain: 3000,
        clients: 3,
        batch: scenario.batch,
        max_segments: scenario.max_segments,
        segment_bytes: scenario.segment_bytes,
        sparse: scenario.sparse,
        min_confirmed: floor,
        min_compared: floor as u64,
        min_applied: scenario.min_applied,
        min_refused: scenario.min_refused,
        script: scenario.script,
    }
}

/// The command that replays one run.
fn reproduce(seed: u64, profile: &str, scenario: &str) -> String {
    format!(
        "OZZY_ACTOR_SIM_SEED={seed} OZZY_ACTOR_SIM_PROFILE={profile} \
         OZZY_ACTOR_SIM_SCENARIO={scenario} \
         cargo nextest run -p ozzy-sim --test ozzy_actor_cluster --profile extended \
         seeded::stress::campaign"
    )
}

/// Run one seed or panic with everything needed to reproduce a failure. A
/// vacuous run is no failure here.
fn outcome(seed: u64, profile: &str, scenario: &str) -> Report {
    let swarm = seeds::Seeds::derive(seed).swarm;
    let parameters = Profile::named(profile, swarm).expect("known profile");
    sim::run(seed, parameters, options(profile, scenario)).unwrap_or_else(|failure| {
        panic!(
            "actor cluster seed {seed} profile {profile} scenario {scenario} failed at tick {}: {}\n\
             parameters {:?}\nhash {:#018x}\n{}\nreproduce: {}",
            failure.tick,
            failure.reason,
            failure.profile,
            failure.hash,
            failure.detail,
            reproduce(seed, profile, scenario)
        )
    })
}

/// Run one seed that must also clear its vacuity floors.
fn checked(seed: u64, profile: &str, scenario: &str) -> Report {
    let report = outcome(seed, profile, scenario);
    assert!(
        !report.vacuous,
        "actor cluster seed {seed} profile {profile} scenario {scenario} was vacuous: {report:?}\n\
         reproduce: {}",
        reproduce(seed, profile, scenario)
    );
    report
}

#[test]
fn quiet_network_confirms_and_converges() {
    for seed in [1, 2] {
        checked(seed, "none", "random");
    }
}

#[test]
fn light_faults_keep_history_and_converge() {
    for seed in [1, 2] {
        checked(seed, "light", "random");
    }
}

#[test]
fn heavy_faults_keep_history_and_converge() {
    for seed in [1, 2] {
        checked(seed, "heavy", "random");
    }
}

#[test]
fn swarm_faults_keep_history_and_converge() {
    for seed in [1, 2] {
        checked(seed, "swarm", "random");
    }
}

#[test]
fn slow_disks_keep_history_and_converge() {
    for seed in [1, 2] {
        checked(seed, "slow", "random");
    }
}

/// The other two elect a leader within the isolation, and the old leader
/// catches up after it rejoins.
#[test]
fn lost_leader_is_replaced() {
    checked(1, "none", "failover");
    checked(2, "light", "failover");
}

/// A view change after a long history still finishes within the election
/// timeouts.
#[test]
fn lost_leader_is_replaced_after_long_history() {
    checked(1, "none", "long-history");
}

/// Each new leader is lost in turn.
#[test]
fn repeatedly_lost_leaders_are_replaced() {
    checked(1, "light", "repeated-failover");
}

/// A full journal refuses proposals without faulting, and the full group
/// still replaces its lost leader and converges.
#[test]
fn full_journal_refuses_proposals_and_replaces_its_leader() {
    checked(1, "none", "full");
    // Light faults also change views before the journal fills.
    checked(4, "light", "full");
}

/// Followers that need several times the leader's space pack sparse
/// segments instead of filling, so the group keeps confirming and converges.
#[test]
fn sparse_followers_pack_instead_of_filling() {
    checked(1, "none", "sparse-followers");
}

/// Seeds that found bugs. Schedules move with the code, so each names what
/// it found.
#[test]
fn regression_seeds() {
    // A retention turn deferred to a waiting proposal ended the round, and
    // nothing started the next one: the safety sweep found the proposal.
    checked(5, "swarm", "random");
}

/// Process-wide state would leak from one run into the next one.
#[test]
fn same_seed_replays_in_one_process() {
    let first = checked(3, "heavy", "random");
    let other = checked(4, "heavy", "random");
    let again = checked(3, "heavy", "random");
    assert_eq!(first, again, "seed 3 did not replay");
    assert_ne!(first.hash, other.hash, "distinct seeds gave one schedule");
}

/// Seed campaigns. The `::stress::` path keeps them out of the fast suite.
mod seeded {
    mod stress {
        use super::super::{checked, outcome, reproduce};

        const PROFILES: [&str; 5] = ["none", "light", "heavy", "swarm", "slow"];
        /// Each scenario with its profiles. Heavy and swarm faults leave too
        /// little progress to fill a journal.
        const SCENARIOS: [(&str, &[&str]); 4] = [
            ("random", &PROFILES),
            ("failover", &PROFILES),
            ("full", &["none", "light", "slow"]),
            ("sparse-followers", &["none", "light", "slow"]),
        ];

        /// About 4800 operations in 75 segments before the leader is lost. A
        /// view change still installs well inside its election timeouts.
        #[test]
        fn lost_leader_is_replaced_after_very_long_history() {
            checked(1, "none", "very-long-history");
        }

        /// `OZZY_ACTOR_SIM_SEEDS` seeds (default 100) over every scenario and
        /// its profiles, or `OZZY_ACTOR_SIM_SEED`. `OZZY_ACTOR_SIM_PROFILE` and
        /// `OZZY_ACTOR_SIM_SCENARIO` pick one of each. Any failure fails at once;
        /// vacuous runs fail only above one in twenty.
        #[test]
        fn campaign() {
            let scenarios: Vec<(String, &[&str])> = std::env::var("OZZY_ACTOR_SIM_SCENARIO")
                .map_or_else(
                    |_| {
                        SCENARIOS
                            .iter()
                            .map(|(name, profiles)| ((*name).to_string(), *profiles))
                            .collect()
                    },
                    |one| {
                        let profiles = SCENARIOS
                            .iter()
                            .find(|(name, _)| *name == one)
                            .map_or(&PROFILES[..], |(_, profiles)| *profiles);
                        vec![(one, profiles)]
                    },
                );
            let profile = std::env::var("OZZY_ACTOR_SIM_PROFILE").ok();
            let seeds: Vec<u64> = if let Ok(seed) = std::env::var("OZZY_ACTOR_SIM_SEED") {
                vec![seed.parse().expect("OZZY_ACTOR_SIM_SEED")]
            } else {
                let count = std::env::var("OZZY_ACTOR_SIM_SEEDS")
                    .map_or(100, |count| count.parse().expect("OZZY_ACTOR_SIM_SEEDS"));
                (1..=count).collect()
            };
            let mut runs = 0;
            let mut vacuous = Vec::new();
            for seed in seeds {
                for (scenario, profiles) in &scenarios {
                    let profiles = profile
                        .as_deref()
                        .map_or(profiles.to_vec(), |one| vec![one]);
                    for profile in profiles {
                        runs += 1;
                        let report = outcome(seed, profile, scenario);
                        if report.vacuous {
                            eprintln!(
                                "vacuous: {report:?}\n  {}",
                                reproduce(seed, profile, scenario)
                            );
                            vacuous.push((seed, profile, scenario.clone()));
                        }
                    }
                }
            }
            // Faults can leave a run with too little progress to prove much;
            // more than one in twenty means progress itself regressed.
            assert!(
                vacuous.len() * 20 <= runs,
                "{} of {runs} runs were vacuous: {vacuous:?}",
                vacuous.len()
            );
        }
    }
}
