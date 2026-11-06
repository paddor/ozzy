mod stress {
    use super::super::{GROUPS, checked, outcome, reproduce};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    const PROFILES: [&str; 5] = ["none", "light", "heavy", "swarm", "slow"];
    /// Each scenario with its profiles. Heavy and swarm faults leave too
    /// little progress to fill a journal, or to retire a segment. Under a
    /// slow link they also change views for real: a follower that hears
    /// late gets too few COMMITs through its full 3 s path and suspects
    /// the leader, and their PUB loss can stall the leader's commit for
    /// 1 s while a gap repairs. Two suspicions end the view.
    const SCENARIOS: [(&str, &[&str]); 21] = [
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
        ("slow-link", &["none", "light", "slow"]),
        ("outsider", &PROFILES),
        ("chaos-outsider", &PROFILES),
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
        let partitions = std::env::var("OZZY_ACTOR_SIM_PARTITIONS").map_or(GROUPS.len(), |count| {
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
