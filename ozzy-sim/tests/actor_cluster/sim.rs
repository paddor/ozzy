//! The tick loop. Each tick: advance time, start or heal network faults,
//! deliver due frames, then in each 1 ms disk step complete due jobs and run
//! every broker's shard turn in seeded order. Clients then observe replies
//! and submit. Checks run after every tick. `OZZY_ACTOR_SIM_TRACE` prints
//! each broker's view and positions when they change.

use crate::{
    broker::{Broker, Lane},
    check::{Checker, converged, leader},
    clock::Clock,
    disk::Disk,
    network::{BROKERS, Network, Profile, Stats},
    seeds::{Rng, Seeds, Trace},
    workload::{Counts, Workload},
};

/// Disk completion steps per 10 ms tick.
const DISK_STEPS: u64 = 10;

#[derive(Debug, Clone, Copy)]
pub(crate) struct Options {
    /// Ticks of faults and new proposals.
    pub ticks: u64,
    /// Further ticks to heal, settle outstanding proposals, and converge.
    pub drain: u64,
    pub clients: usize,
    /// Vacuity floors: confirmed proposals, and ops two brokers reported.
    pub min_confirmed: usize,
    pub min_compared: u64,
    /// Scripted faults in tick order, on top of the profile's random ones.
    pub script: &'static [Fault],
}

/// A scripted fault.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Fault {
    /// From tick `at`, once a ready leader exists, cut it off from both
    /// peers for `ticks`. The other two must elect a ready leader before it
    /// rejoins.
    IsolateLeader { at: u64, ticks: u64 },
}

#[derive(Debug, Clone, Copy)]
struct Isolation {
    broker: usize,
    view: u64,
    until: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Report {
    pub hash: u64,
    pub ticks: u64,
    pub confirmed: usize,
    pub compared: u64,
    pub applied: u64,
    /// Below a vacuity floor, or a scripted fault never started: the run
    /// converged but proved little.
    pub vacuous: bool,
}

#[derive(Debug)]
pub(crate) struct Failure {
    pub tick: u64,
    pub reason: String,
    pub hash: u64,
    pub profile: Profile,
    pub detail: String,
}

struct Sim {
    clock: Clock,
    network: Network,
    executor: Rng,
    brokers: Vec<Broker>,
    workload: Workload,
    checker: Checker,
    trace: Trace,
    options: Options,
    /// The next scripted fault and a running isolation.
    next_fault: usize,
    isolation: Option<Isolation>,
}

pub(crate) fn run(seed: u64, profile: Profile, options: Options) -> Result<Report, Box<Failure>> {
    let seeds = Seeds::derive(seed);
    let clock = Clock::default();
    let mut disks = Rng::new(seeds.disk);
    let mut brokers = Vec::new();
    let mut lanes: Vec<Vec<Lane>> = Vec::new();
    for index in 0..BROKERS {
        let disk = Disk::new(disks.next(), profile.disk_mean);
        let (broker, broker_lanes) =
            Broker::start(index, &clock, disk, options.clients).map_err(|reason| {
                Box::new(Failure {
                    tick: 0,
                    reason,
                    hash: 0,
                    profile,
                    detail: String::new(),
                })
            })?;
        brokers.push(broker);
        lanes.push(broker_lanes);
    }
    let mut sim = Sim {
        clock,
        network: Network::new(profile, seeds.network),
        executor: Rng::new(seeds.executor),
        brokers,
        workload: Workload::new(lanes, seeds.workload),
        checker: Checker::default(),
        trace: Trace::default(),
        options,
        next_fault: 0,
        isolation: None,
    };
    let result = sim.run();
    let hash = sim.trace.value();
    let tick = sim.clock.tick();
    let detail = sim.detail();
    let result = result.and_then(|applied| {
        for broker in sim.brokers {
            broker.shutdown()?;
        }
        Ok(applied)
    });
    result
        .map(|(applied, vacuous)| Report {
            hash,
            ticks: tick,
            confirmed: sim.workload.confirmed.len(),
            compared: sim.checker.compared,
            applied,
            vacuous,
        })
        .map_err(|reason| {
            Box::new(Failure {
                tick,
                reason,
                hash,
                profile,
                detail,
            })
        })
}

impl Sim {
    /// Run to convergence; returns the common applied operation and whether
    /// the run was vacuous.
    fn run(&mut self) -> Result<(u64, bool), String> {
        let end = self.options.ticks + self.options.drain;
        let mut last = [None; BROKERS];
        while self.clock.tick() < end {
            self.clock.advance();
            let tick = self.clock.tick();
            let draining = tick > self.options.ticks;
            if tick == self.options.ticks + 1 {
                self.network.heal();
            }
            self.script(tick)?;
            self.network.step(tick);
            let mut order: [usize; BROKERS] = std::array::from_fn(|index| index);
            self.executor.shuffle(&mut order);
            for index in order {
                let broker = &mut self.brokers[index];
                self.network
                    .deliver(index, tick, &mut broker.inbox, &mut self.trace);
            }
            // Disks finish jobs in 1 ms steps; each step lets every shard
            // observe them. Time and the network move per tick.
            for step in 0..DISK_STEPS {
                let at = tick * DISK_STEPS + step;
                for index in order {
                    let broker = &mut self.brokers[index];
                    broker.disk.step(at, index, &mut self.trace);
                    if let Some(lanes) = broker.turn(
                        &mut self.network,
                        &self.clock,
                        &mut self.trace,
                        self.options.clients,
                    )? {
                        self.workload.replace(index, lanes);
                    }
                }
            }
            self.workload
                .step(tick, !draining, &mut self.checker, &mut self.trace)?;
            for broker in &self.brokers {
                let status = broker.status();
                if std::env::var_os("OZZY_ACTOR_SIM_TRACE").is_some() {
                    let shown = Some((
                        status.scope.view,
                        status.application_ready,
                        status.normal.map(|normal| {
                            (
                                normal.accepted.op.0,
                                normal.committed.op.0,
                                normal.applied.op.0,
                            )
                        }),
                    ));
                    if last[broker.index] != shown {
                        eprintln!("tick {tick} broker {}: {shown:?}", broker.index);
                        last[broker.index] = shown;
                    }
                }
                self.checker.observe(broker.index, &status)?;
                let missed = broker.shard.sweep_progress();
                if missed != 0 {
                    return Err(format!(
                        "broker {}: lost wakeup: the safety sweep found work {missed} time(s)",
                        broker.index
                    ));
                }
            }
            if draining
                && self.workload.idle()
                && let Some(applied) = converged(&self.brokers)
            {
                return self.finish(applied);
            }
        }
        Err(format!(
            "no convergence after {} drain ticks",
            self.options.drain
        ))
    }

    /// End a due isolation, then start the next scripted fault once it is due
    /// and a leader exists.
    fn script(&mut self, tick: u64) -> Result<(), String> {
        if let Some(isolation) = self.isolation
            && tick >= isolation.until
        {
            match leader(&self.brokers) {
                Some((broker, view)) if broker != isolation.broker && view > isolation.view => {}
                _ => {
                    return Err(format!(
                        "no new leader while broker {} was cut off as leader of view {}",
                        isolation.broker, isolation.view
                    ));
                }
            }
            self.network.rejoin();
            self.isolation = None;
        }
        if self.isolation.is_none()
            && let Some(Fault::IsolateLeader { at, ticks }) =
                self.options.script.get(self.next_fault).copied()
            && tick >= at
            && let Some((broker, view)) = leader(&self.brokers)
        {
            self.network.isolate(broker);
            self.isolation = Some(Isolation {
                broker,
                view,
                until: tick + ticks,
            });
            self.next_fault += 1;
            self.trace.word(tick);
            self.trace.word(broker as u64);
        }
        Ok(())
    }

    /// The converged op, and whether the run was vacuous.
    fn finish(&self, applied: ozzy_replication::Prefix) -> Result<(u64, bool), String> {
        for through in &self.workload.confirmed {
            if through.op > applied.op {
                return Err(format!(
                    "confirmed op {} is past the converged op {}",
                    through.op.0, applied.op.0
                ));
            }
        }
        let vacuous = self.next_fault < self.options.script.len()
            || self.workload.confirmed.len() < self.options.min_confirmed
            || self.checker.compared < self.options.min_compared;
        Ok((applied.op.0, vacuous))
    }

    fn detail(&self) -> String {
        use std::fmt::Write as _;
        let network: Stats = self.network.stats;
        let counts: Counts = self.workload.counts;
        let mut detail = format!(
            "network {network:?}, {} queued\nworkload {counts:?}, {} confirmed, {} compared",
            self.network.queued(),
            self.workload.confirmed.len(),
            self.checker.compared,
        );
        if let Some(isolation) = self.isolation {
            let _ = write!(detail, "\nisolation {isolation:?}");
        }
        for broker in &self.brokers {
            let status = broker.status();
            let _ = write!(
                detail,
                "\nbroker {}: view {} ready {} normal {:?} disk {} unbound {} lanes {}/{}",
                broker.index,
                status.scope.view,
                status.application_ready,
                status.normal.map(|normal| (
                    normal.status,
                    normal.accepted.op.0,
                    normal.committed.op.0,
                    normal.applied.op.0
                )),
                status.disk_pending,
                broker.unbound,
                broker.inbox.control.len(),
                broker.inbox.data.len(),
            );
        }
        detail
    }
}
