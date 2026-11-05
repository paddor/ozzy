//! Seeded single-thread cluster over production partition schedulers.
//!
//! Three brokers each run a production `PartitionActors` shard with one or
//! two replicated groups, each over a production shard journal on the
//! broker's simulated device.
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
mod chaos;
mod check;
mod clock;
mod consumer;
mod disk;
mod network;
mod records;
mod seeds;
mod sim;
mod workload;

use broker::{Control, GROUPS, MAX_SEGMENTS, SEGMENT_BYTES};
use network::Profile;
use sim::{
    Fault::{
        self, CrashAll, CrashAllMidWrite, CrashFollower, CrashLeader, DeafLeader, FailFollowerDisk,
        FailLeaderDisk, IsolateFollower, IsolateLeader, KillFollower, KillLeader, MuteLeader,
        PauseLeader, Recrash, Rewind, SkewLeader, StallLeader,
    },
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
    /// Brokers whose disk takes 16 ms per job.
    slow: &'static [usize],
    /// Producers append records beside the barrier clients.
    records: bool,
    /// Random overlapping faults.
    chaos: bool,
    /// The safeguard a negative control turns off, if any.
    control: Control,
    /// Both partitions retire records after `RETENTION_AGE`.
    retention: bool,
    /// Caps the profile's floors for confirmed proposals, records, and
    /// compared ops, where a full journal bounds them.
    max_floor: usize,
    /// Caps the records floor further.
    max_record_floor: usize,
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
    slow: &[],
    records: true,
    chaos: false,
    control: Control::None,
    retention: false,
    max_floor: usize::MAX,
    max_record_floor: usize::MAX,
    min_applied: 0,
    min_refused: 0,
};

/// A lost leader stays cut off for 15 s: three view attempts under the
/// doubling 2 s to 8 s election timeouts, over the slowest profiles.
const LOST_LEADER: Fault = IsolateLeader {
    at: 2500,
    ticks: 1500,
};

const SKEWED_LEADER: Fault = SkewLeader {
    at: 500,
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
        // A follower's disk takes 16 ms per job. Once the leader is lost,
        // the new one commits only through that disk.
        "slow-follower" => Scenario {
            ticks: 3000,
            script: &[LOST_LEADER],
            slow: &[2],
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
        // four of the six stay free for followers that need more. Clients and
        // producers share the space and are refused after about 80
        // operations, then the full leader is lost. A scheduled batch also
        // needs room to release each record alone, so it is refused early
        // and its producer stays on it: a partition may get no records.
        "full" => Scenario {
            ticks: 3000,
            script: &[LOST_LEADER],
            max_segments: 6,
            segment_bytes: 32 * 1024,
            max_floor: 10,
            max_record_floor: 0,
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
        // A sparse follower misses about 1300 operations, then rejoins just
        // as the leader is lost. The new view must fetch and install them in
        // 8-operation transfers, longer than any election timeout, and the
        // install passes the 24-segment limit unless it packs. Without
        // records: they fill the leader, and a sparse copy of a full leader
        // cannot fit, as a pack never merges segments over half full.
        "sparse-rejoin" => Scenario {
            ticks: 5100,
            script: &[
                IsolateFollower {
                    at: 600,
                    ticks: 1500,
                },
                IsolateLeader {
                    at: 2100,
                    ticks: 3000,
                },
            ],
            batch: 4,
            max_segments: 24,
            segment_bytes: 32 * 1024,
            sparse: &[1],
            records: false,
            max_floor: 20,
            ..RANDOM
        },
        // A follower loses power and restarts over what reached its disk,
        // then the leader does. Each comes back with an empty in-memory
        // state and must rejoin; the lost leader's peers elect without it.
        "crash" => Scenario {
            ticks: 3500,
            script: &[
                CrashFollower {
                    at: 500,
                    ticks: 300,
                },
                CrashLeader {
                    at: 1300,
                    ticks: 1500,
                },
            ],
            ..RANDOM
        },
        // A follower's disk, then the leader's, fails every write and sync.
        // Each broker stops on one as its process exits, then restarts over
        // what its disk kept.
        "disk-error" => Scenario {
            ticks: 3500,
            script: &[
                FailFollowerDisk {
                    at: 500,
                    ticks: 300,
                },
                FailLeaderDisk {
                    at: 1300,
                    ticks: 1500,
                },
            ],
            ..RANDOM
        },
        // As `crash`, but only the processes die: each restart sees what its
        // disk cached, and writes it had queued may still land.
        "kill" => Scenario {
            ticks: 3500,
            script: &[
                KillFollower {
                    at: 500,
                    ticks: 300,
                },
                KillLeader {
                    at: 1300,
                    ticks: 1500,
                },
            ],
            ..RANDOM
        },
        // Every broker loses power at once, mid-traffic, and restarts over
        // what reached its disk. Each committed operation must survive.
        "power-loss" => Scenario {
            ticks: 3000,
            script: &[CrashAll { at: 1500 }],
            ..RANDOM
        },
        // Random overlapping faults on any broker: power loss, kills, pauses,
        // disk stalls and failures, cuts, and connection resets. Two brokers
        // may be down at once.
        "chaos" => Scenario {
            ticks: 5000,
            chaos: true,
            max_floor: 20,
            ..RANDOM
        },
        // The leader's clock is 5 s ahead for 15 s. Its guard holds every
        // release meanwhile, as no peer agrees.
        "skew" => Scenario {
            script: &[SKEWED_LEADER],
            ..RANDOM
        },
        // Negative control: as `skew` without the guard. The leader releases
        // records before they are due, and the run must fail.
        "skew-unguarded" => Scenario {
            script: &[SKEWED_LEADER],
            control: Control::Unguarded,
            ..RANDOM
        },
        // Negative control: the dedup partition stores what producers send
        // again after a restart, and the run must fail.
        "dedup-off" => Scenario {
            control: Control::NoDedup,
            ..RANDOM
        },
        // Negative control: producer sessions retire after 0.5 s instead of
        // 3 s, and the run must fail.
        "idle-early" => Scenario {
            control: Control::EarlyIdle,
            ..RANDOM
        },
        // Both partitions retire records after 2 s in 32 KiB segments, below
        // the latest due time, so held records are carried. A follower cut
        // off for 6 s catches up past retired history.
        "retention" => Scenario {
            ticks: 2000,
            script: &[IsolateFollower {
                at: 600,
                ticks: 600,
            }],
            segment_bytes: 32 * 1024,
            retention: true,
            ..RANDOM
        },
        // Negative control: as `retention`, but records retire after 0.5 s.
        // The run must fail.
        "retention-young" => Scenario {
            ticks: 3000,
            segment_bytes: 32 * 1024,
            retention: true,
            control: Control::YoungRetention,
            ..RANDOM
        },
        // Negative control: every disk loses the last 5 s it reported done.
        // The run must fail.
        "rewind" => Scenario {
            ticks: 3000,
            script: &[Rewind {
                at: 1000,
                ticks: 500,
            }],
            ..RANDOM
        },
        // Power fails again while the brokers recover from the first loss, at a
        // seeded tick: mid election, installation, or the first new writes.
        "recovery-crash" => Scenario {
            ticks: 3000,
            script: &[CrashAll { at: 1500 }, Recrash { within: 250 }],
            ..RANDOM
        },
        // Every disk stops completing jobs, then power fails within 300 ms with
        // the queued writes in flight, so they tear.
        "torn-write" => Scenario {
            ticks: 3000,
            script: &[CrashAllMidWrite {
                at: 1500,
                within: 30,
            }],
            ..RANDOM
        },
        // The leader's disk completes nothing for 15 s while its links stay
        // up, as a host write stall does. Another broker must take over.
        "disk-stall" => Scenario {
            ticks: 3000,
            script: &[StallLeader {
                at: 1000,
                ticks: 1500,
            }],
            ..RANDOM
        },
        // The leader's process runs nothing for 15 s, then resumes into a
        // newer view with every timer due and a backlog of input.
        "pause" => Scenario {
            ticks: 3000,
            script: &[PauseLeader {
                at: 1000,
                ticks: 1500,
            }],
            ..RANDOM
        },
        // The leader first hears nothing, then a later leader can send
        // nothing. Each must be replaced while its other direction works.
        "one-way" => Scenario {
            ticks: 4500,
            script: &[
                DeafLeader {
                    at: 500,
                    ticks: 1500,
                },
                MuteLeader {
                    at: 2500,
                    ticks: 1500,
                },
            ],
            ..RANDOM
        },
        _ => return None,
    })
}

/// Proposals and faults for the scenario on `partitions`, then up to 30 s to
/// heal and converge. Vacuity floors fit each profile's fault rate.
fn options(profile: &str, scenario: &str, partitions: usize) -> Options {
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
        partitions,
        batch: scenario.batch,
        max_segments: scenario.max_segments,
        segment_bytes: scenario.segment_bytes,
        sparse: scenario.sparse,
        slow: scenario.slow,
        records: scenario.records,
        chaos: scenario.chaos,
        control: scenario.control,
        retention: scenario.retention,
        min_confirmed: floor,
        min_records: floor.min(scenario.max_record_floor),
        min_compared: floor as u64,
        min_applied: scenario.min_applied,
        min_refused: scenario.min_refused,
        script: scenario.script,
    }
}

/// The command that replays one run.
fn reproduce(seed: u64, profile: &str, scenario: &str, partitions: usize) -> String {
    format!(
        "OZZY_ACTOR_SIM_SEED={seed} OZZY_ACTOR_SIM_PROFILE={profile} \
         OZZY_ACTOR_SIM_SCENARIO={scenario} OZZY_ACTOR_SIM_PARTITIONS={partitions} \
         cargo nextest run -p ozzy-sim --test ozzy_actor_cluster --profile extended \
         seeded::stress::campaign"
    )
}

/// Run one seed or panic with everything needed to reproduce a failure. A
/// vacuous run is no failure here.
fn outcome(seed: u64, profile: &str, scenario: &str, partitions: usize) -> Report {
    let swarm = seeds::Seeds::derive(seed).swarm;
    let parameters = Profile::named(profile, swarm).expect("known profile");
    let options = options(profile, scenario, partitions);
    sim::run(seed, parameters, options).unwrap_or_else(|failure| {
        panic!(
            "actor cluster seed {seed} profile {profile} scenario {scenario} partitions \
             {partitions} failed at tick {}: {}\n\
             parameters {:?}\nhash {:#018x}\n{}\nreproduce: {}",
            failure.tick,
            failure.reason,
            failure.profile,
            failure.hash,
            failure.detail,
            reproduce(seed, profile, scenario, partitions)
        )
    })
}

/// Run one seed on one partition that must fail; returns why.
fn failure(seed: u64, profile: &str, scenario: &str) -> String {
    failure_on(1, seed, profile, scenario)
}

/// As `failure`, on `partitions` partitions.
fn failure_on(partitions: usize, seed: u64, profile: &str, scenario: &str) -> String {
    let swarm = seeds::Seeds::derive(seed).swarm;
    let parameters = Profile::named(profile, swarm).expect("known profile");
    match sim::run(seed, parameters, options(profile, scenario, partitions)) {
        Ok(report) => panic!(
            "actor cluster seed {seed} profile {profile} scenario {scenario} passed: {report:?}\n\
             reproduce: {}",
            reproduce(seed, profile, scenario, partitions)
        ),
        Err(failure) => failure.reason,
    }
}

/// Run one seed over every partition; it must also clear its vacuity floors.
fn checked(seed: u64, profile: &str, scenario: &str) -> Report {
    checked_on(GROUPS.len(), seed, profile, scenario)
}

/// As `checked`, on one partition. Scripted fast cases run one to stay near
/// ten seconds; campaigns run every scenario over both.
fn one_partition(seed: u64, profile: &str, scenario: &str) -> Report {
    checked_on(1, seed, profile, scenario)
}

fn checked_on(partitions: usize, seed: u64, profile: &str, scenario: &str) -> Report {
    let report = outcome(seed, profile, scenario, partitions);
    assert!(
        !report.vacuous,
        "actor cluster seed {seed} profile {profile} scenario {scenario} partitions {partitions} \
         was vacuous: {report:?}\nreproduce: {}",
        reproduce(seed, profile, scenario, partitions)
    );
    report
}

#[test]
fn quiet_network_confirms_and_converges() {
    let report = checked(1, "none", "random");
    // Consumers went live and read most records from the live topic.
    assert!(
        report.cancels >= 1 && report.live * 4 >= report.records as u64,
        "{report:?}"
    );
    // Dedup producers restarted with batches in flight and found them
    // stored, and some sessions retired while their producers paused.
    assert!(
        report.restarts >= 1 && report.duplicates >= 1 && report.retired >= 1,
        "{report:?}"
    );
    eprintln!("{report:?}");
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
    one_partition(1, "none", "failover");
    one_partition(2, "light", "failover");
}

/// A view change after a long history still finishes within the election
/// timeouts.
#[test]
fn lost_leader_is_replaced_after_long_history() {
    one_partition(1, "none", "long-history");
}

/// A new leader commits through a follower's slow disk.
#[test]
fn new_leader_commits_through_a_slow_follower() {
    one_partition(1, "none", "slow-follower");
}

/// Each new leader is lost in turn.
#[test]
fn repeatedly_lost_leaders_are_replaced() {
    one_partition(1, "light", "repeated-failover");
}

/// A full journal refuses proposals without faulting, and the full group
/// still replaces its lost leader and converges.
#[test]
fn full_journal_refuses_proposals_and_replaces_its_leader() {
    one_partition(1, "none", "full");
    // Light faults also change views before the journal fills.
    one_partition(4, "light", "full");
}

/// Followers that need several times the leader's space pack sparse
/// segments instead of filling, so the group keeps confirming and converges.
#[test]
fn sparse_followers_pack_instead_of_filling() {
    one_partition(1, "none", "sparse-followers");
}

/// A follower that missed a long history catches up within one view
/// change, packing at the segment limit, while the old leader is away.
#[test]
fn lagging_sparse_follower_installs_a_long_suffix_at_the_limit() {
    // The catch-up outlasts the 8 s longest election timeout, yet ends in
    // the first view after the old one.
    assert_eq!(one_partition(1, "none", "sparse-rejoin").view, 1);
}

/// A follower, then the leader, lose power and restart over their durable
/// disks. Each rejoins without losing a confirmed operation.
#[test]
fn crashed_brokers_restart_and_rejoin() {
    one_partition(1, "none", "crash");
}

/// A broker whose disk fails writes and syncs stops, restarts over what its
/// disk kept, and rejoins. The leader is replaced meanwhile.
#[test]
fn failed_disk_stops_and_restarts_its_broker() {
    one_partition(1, "none", "disk-error");
}

/// A follower, then the leader, are killed and restart over their cached
/// disks. Each rejoins without losing a confirmed operation.
#[test]
fn killed_brokers_restart_and_rejoin() {
    one_partition(1, "none", "kill");
}

/// All three brokers lose power at once and elect again over their durable
/// disks without losing a committed operation.
#[test]
fn whole_cluster_power_loss_keeps_every_commit() {
    let report = one_partition(1, "none", "power-loss");
    // Rolls publish the applied commit, and an installation replays only
    // after the state a restart recovered. Refetching or replaying all
    // history takes twice as long.
    assert!(
        report.outage.is_some_and(|ticks| ticks <= 300),
        "{report:?}"
    );
}

/// Power fails again while the brokers recover from a power loss. The
/// second recovery keeps every commit too.
#[test]
fn power_loss_during_recovery_keeps_every_commit() {
    one_partition(1, "none", "recovery-crash");
}

/// Power fails while every disk holds queued writes. Whatever part of them
/// reached media, every commit survives.
#[test]
fn power_loss_mid_write_keeps_every_commit() {
    one_partition(1, "none", "torn-write");
}

/// A leader whose disk stalls hands over to the two healthy brokers, then
/// rejoins once its disk recovers.
#[test]
fn stalled_leader_disk_is_replaced() {
    one_partition(1, "none", "disk-stall");
}

/// A leader whose process stops is replaced. Once it resumes, its due
/// timers and stale input do not disturb the newer view.
#[test]
fn paused_leader_is_replaced_and_resumes_as_follower() {
    one_partition(1, "none", "pause");
}

/// A leader that only hears, then one that only speaks, is replaced by the
/// two brokers that still talk both ways.
#[test]
fn one_way_cut_leaders_are_replaced() {
    one_partition(1, "none", "one-way");
}

/// A leader whose clock runs 5 s ahead releases nothing early, and releases
/// every held record once its clock agrees again.
#[test]
fn skewed_leader_holds_releases_until_its_clock_agrees() {
    let report = one_partition(1, "none", "skew");
    assert!(report.scheduled >= 20, "{report:?}");
}

/// Negative control: without the clock guard, the skewed leader releases
/// records early, and the checks must notice.
#[test]
fn unguarded_skewed_leader_releases_early() {
    let reason = failure(1, "none", "skew-unguarded");
    assert!(reason.contains("before its due time"), "{reason}");
}

/// Negative control: a partition without dedup stores a restarted
/// producer's resend again, and the checks must notice.
#[test]
fn partition_without_dedup_stores_resends_twice() {
    let reason = failure_on(GROUPS.len(), 1, "none", "dedup-off");
    assert!(reason.contains("seen at"), "{reason}");
}

/// Negative control: producer sessions retire after a sixth of their idle
/// time, and the checks must notice.
#[test]
fn early_idle_expiry_fails_the_run() {
    let reason = failure_on(GROUPS.len(), 1, "none", "idle-early");
    assert!(
        reason.contains("after the session last changed"),
        "{reason}"
    );
}

/// Both partitions retire records 2 s old. Consumers skip what retired
/// before they read it, held records survive by carry, and a lagging
/// follower catches up past retired history. Its leader once replayed the
/// retired history to it on every turn, a busy loop.
#[test]
fn age_retention_retires_old_records_and_carries_held_ones() {
    let report = checked(2, "none", "retention");
    assert!(
        report.retained_from > 0 && report.scheduled >= 20,
        "{report:?}"
    );
    eprintln!("{report:?}");
}

/// Negative control: records retire after a quarter of the age the checks
/// hold retention to, and they must notice.
#[test]
fn young_retention_fails_the_run() {
    let reason = failure_on(GROUPS.len(), 1, "none", "retention-young");
    assert!(reason.contains("retired at true time"), "{reason}");
}

/// Negative control: every disk loses what it reported done in the last 5 s.
/// The checks must notice the lost confirmed operations.
#[test]
fn rewound_disks_fail_the_run() {
    for seed in [1, 2] {
        let reason = failure(seed, "none", "rewind");
        // A history check, not a startup or liveness failure.
        assert!(
            reason.contains("Confirmed reports digest") || reason.contains("past the converged op"),
            "seed {seed}: {reason}"
        );
    }
}

/// Random overlapping faults, two brokers down at times, keep every
/// confirmed record and converge once they end. One partition: a quiet
/// network carries several thousand records.
#[test]
fn random_overlapping_faults_keep_history_and_converge() {
    let report = one_partition(1, "none", "chaos");
    assert!(report.faults >= 20, "{report:?}");
    eprintln!("{report:?}");
}

/// As above on both partitions, with the light profile's loss, delay, and
/// short cuts too.
#[test]
fn random_overlapping_and_network_faults_keep_history_and_converge() {
    let report = checked(2, "light", "chaos");
    assert!(report.faults >= 20, "{report:?}");
    eprintln!("{report:?}");
}

/// Seeds that found bugs. Schedules move with the code, so each names what
/// it found.
#[test]
fn regression_seeds() {
    // A retention turn deferred to a waiting proposal ended the round, and
    // nothing started the next one: the safety sweep found the proposal.
    checked(5, "swarm", "random");
    // A follower refused a proposal it still held, after a due retention
    // turn had deferred to it. Nothing asked for the next round.
    one_partition(74, "slow", "crash");
    // A due release waited for room in the leader's full pipeline, with a
    // record queued behind it. The leader kept asking for another round, so
    // its shard woke itself for a whole turn.
    checked(8, "none", "sparse-followers");
    // A dedup partition's retention turn left identity expiry owed, due at
    // once. Writers took the next round, and nothing asked for another one:
    // the safety sweep started the turn.
    checked(2, "none", "failover");
    // A stalled disk completed a job for one partition's visit. The other
    // partition's leader kept suspecting storage until the safety sweep.
    checked(6, "slow", "chaos");
    // A retirement's file work ended inside a visit, and nothing asked for
    // the next round: the safety sweep started the due retention turn.
    checked(23, "none", "retention");
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
        use super::super::{GROUPS, checked, outcome, reproduce};
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

        const PROFILES: [&str; 5] = ["none", "light", "heavy", "swarm", "slow"];
        /// Each scenario with its profiles. Heavy and swarm faults leave too
        /// little progress to fill a journal, or to retire a segment.
        const SCENARIOS: [(&str, &[&str]); 18] = [
            ("random", &PROFILES),
            ("failover", &PROFILES),
            ("slow-follower", &PROFILES),
            ("full", &["none", "light", "slow"]),
            ("sparse-followers", &["none", "light", "slow"]),
            ("sparse-rejoin", &["none", "light", "slow"]),
            ("crash", &PROFILES),
            ("disk-error", &PROFILES),
            ("kill", &PROFILES),
            ("power-loss", &PROFILES),
            ("recovery-crash", &PROFILES),
            ("torn-write", &PROFILES),
            ("disk-stall", &PROFILES),
            ("pause", &PROFILES),
            ("one-way", &PROFILES),
            ("chaos", &PROFILES),
            ("skew", &PROFILES),
            ("retention", &["none", "light", "slow"]),
        ];

        /// About 4800 operations in 75 segments before the leader is lost. A
        /// view change still installs well inside its election timeouts.
        #[test]
        fn lost_leader_is_replaced_after_very_long_history() {
            checked(1, "none", "very-long-history");
        }

        /// `OZZY_ACTOR_SIM_SEEDS` seeds (default 20) over every scenario and
        /// its profiles, or `OZZY_ACTOR_SIM_SEED`. `OZZY_ACTOR_SIM_PROFILE` and
        /// `OZZY_ACTOR_SIM_SCENARIO` pick one of each, and
        /// `OZZY_ACTOR_SIM_PARTITIONS` hosts one partition instead of two. Runs share nothing, so
        /// they spread over `OZZY_ACTOR_SIM_THREADS` threads (default: one per
        /// core). Any failure stops the campaign; vacuous runs fail only above
        /// one in twenty.
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
            let profile = profile.as_deref();
            let seeds: Vec<u64> = if let Ok(seed) = std::env::var("OZZY_ACTOR_SIM_SEED") {
                vec![seed.parse().expect("OZZY_ACTOR_SIM_SEED")]
            } else {
                let count = std::env::var("OZZY_ACTOR_SIM_SEEDS")
                    .map_or(20, |count| count.parse().expect("OZZY_ACTOR_SIM_SEEDS"));
                (1..=count).collect()
            };
            let partitions = std::env::var("OZZY_ACTOR_SIM_PARTITIONS")
                .map_or(GROUPS.len(), |count| {
                    count.parse().expect("OZZY_ACTOR_SIM_PARTITIONS")
                });
            assert!(
                (1..=GROUPS.len()).contains(&partitions),
                "OZZY_ACTOR_SIM_PARTITIONS is 1 or 2"
            );
            let threads = std::env::var("OZZY_ACTOR_SIM_THREADS").map_or_else(
                |_| std::thread::available_parallelism().map_or(1, usize::from),
                |threads| threads.parse().expect("OZZY_ACTOR_SIM_THREADS"),
            );
            let runs: Vec<(u64, &str, &str)> = seeds
                .iter()
                .flat_map(|seed| {
                    scenarios.iter().flat_map(move |(scenario, profiles)| {
                        profile
                            .map_or(profiles.to_vec(), |one| vec![one])
                            .into_iter()
                            .map(move |profile| (*seed, profile, scenario.as_str()))
                    })
                })
                .collect();
            let next = AtomicUsize::new(0);
            let failed = AtomicBool::new(false);
            let run = || {
                let _stop = StopOnPanic(&failed);
                let mut vacuous = Vec::new();
                while !failed.load(Ordering::Relaxed) {
                    let Some(&(seed, profile, scenario)) =
                        runs.get(next.fetch_add(1, Ordering::Relaxed))
                    else {
                        break;
                    };
                    let report = outcome(seed, profile, scenario, partitions);
                    if report.vacuous {
                        eprintln!(
                            "vacuous: {report:?}\n  {}",
                            reproduce(seed, profile, scenario, partitions)
                        );
                        vacuous.push((seed, profile, scenario));
                    }
                }
                vacuous
            };
            let workers: Vec<_> = std::thread::scope(|scope| {
                let workers: Vec<_> = (0..threads).map(|_| scope.spawn(run)).collect();
                workers
                    .into_iter()
                    .map(std::thread::ScopedJoinHandle::join)
                    .collect()
            });
            let mut vacuous = Vec::new();
            for worker in workers {
                // Each failure printed its own reproduction as it panicked.
                vacuous.extend(worker.unwrap_or_else(|panic| std::panic::resume_unwind(panic)));
            }
            vacuous.sort_unstable();
            // Faults can leave a run with too little progress to prove much;
            // more than one in twenty means progress itself regressed.
            assert!(
                vacuous.len() * 20 <= runs.len(),
                "{} of {} runs were vacuous: {vacuous:?}",
                vacuous.len(),
                runs.len()
            );
        }

        /// Stops the other campaign threads once one fails.
        struct StopOnPanic<'a>(&'a AtomicBool);

        impl Drop for StopOnPanic<'_> {
            fn drop(&mut self) {
                if std::thread::panicking() {
                    self.0.store(true, Ordering::Relaxed);
                }
            }
        }
    }
}
