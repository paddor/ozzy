//! The tick loop. Each tick: advance time, start or heal network faults,
//! deliver due frames, then in each 1 ms disk step complete due jobs and run
//! every broker's shard turn in seeded order. Clients then observe replies
//! and submit. Checks run after every tick. `OZZY_ACTOR_SIM_TRACE` prints
//! each broker's view and positions when they change.

use crate::{
    broker::{Broker, Lane, Shape, session},
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
    /// Barrier operations per proposal.
    pub batch: usize,
    /// Journal segment limit and size.
    pub max_segments: usize,
    pub segment_bytes: u64,
    /// Brokers that write one operation per physical group, so their
    /// journals need several times the space of a leader that packs a
    /// proposal into one group.
    pub sparse: &'static [usize],
    /// Vacuity floors: confirmed proposals, ops two brokers reported, the
    /// converged op, and proposals a full journal refused.
    pub min_confirmed: usize,
    pub min_compared: u64,
    pub min_applied: u64,
    pub min_refused: u64,
    /// Scripted faults in tick order, on top of the profile's random ones.
    pub script: &'static [Fault],
}

impl Options {
    fn shape(&self, broker: usize) -> Shape {
        Shape {
            max_segments: self.max_segments,
            segment_bytes: self.segment_bytes,
            sparse: self.sparse.contains(&broker),
        }
    }
}

/// A scripted fault.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Fault {
    /// From tick `at`, once a ready leader exists, cut it off from both
    /// peers for `ticks`. The other two must elect a ready leader before it
    /// rejoins.
    IsolateLeader { at: u64, ticks: u64 },
    /// From tick `at`, once a ready leader exists, cut off one of its
    /// followers for `ticks`, a sparse one if any. The other two keep going.
    IsolateFollower { at: u64, ticks: u64 },
    /// As `IsolateLeader`, but the leader also loses power and restarts
    /// over its durable disk image. Its links come back after `ticks`.
    CrashLeader { at: u64, ticks: u64 },
    /// As `IsolateFollower`, with a power loss and restart.
    CrashFollower { at: u64, ticks: u64 },
    /// From tick `at`, once a ready leader exists, every broker loses power
    /// at once and restarts over its durable disk image. Links come back at
    /// once. No committed operation may be lost.
    CrashAll { at: u64 },
}

impl Fault {
    /// Its start tick and length.
    const fn window(self) -> (u64, u64) {
        match self {
            Self::IsolateLeader { at, ticks }
            | Self::IsolateFollower { at, ticks }
            | Self::CrashLeader { at, ticks }
            | Self::CrashFollower { at, ticks } => (at, ticks),
            Self::CrashAll { at } => (at, 0),
        }
    }

    const fn leader(self) -> bool {
        matches!(self, Self::IsolateLeader { .. } | Self::CrashLeader { .. })
    }

    const fn crash(self) -> bool {
        matches!(
            self,
            Self::CrashLeader { .. } | Self::CrashFollower { .. } | Self::CrashAll { .. }
        )
    }
}

#[derive(Debug, Clone, Copy)]
struct Isolation {
    broker: usize,
    view: u64,
    until: u64,
    /// The cut-off broker led `view`, so another must take over.
    leader: bool,
    /// Another broker led a later view at some tick of the isolation. A
    /// fault that ends it may leave no leader at the last tick.
    replaced: bool,
    /// It restarted and has no links until the isolation ends.
    crashed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Report {
    pub hash: u64,
    pub ticks: u64,
    pub confirmed: usize,
    pub compared: u64,
    pub applied: u64,
    /// The view the group converged in.
    pub view: u64,
    /// Proposals a journal refused as too full.
    pub refused: u64,
    /// Ticks from the last whole-cluster power loss until a leader serves.
    pub outage: Option<u64>,
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
    /// The tick of the last whole-cluster power loss, and how long until a
    /// leader served again.
    power_loss: Option<(u64, Option<u64>)>,
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
            Broker::start(index, &clock, disk, options.clients, options.shape(index)).map_err(
                |reason| {
                    Box::new(Failure {
                        tick: 0,
                        reason,
                        hash: 0,
                        profile,
                        detail: String::new(),
                    })
                },
            )?;
        brokers.push(broker);
        lanes.push(broker_lanes);
    }
    let mut sim = Sim {
        clock,
        network: Network::new(profile, seeds.network),
        executor: Rng::new(seeds.executor),
        brokers,
        workload: Workload::new(lanes, seeds.workload, options.batch),
        checker: Checker::default(),
        trace: Trace::default(),
        options,
        next_fault: 0,
        isolation: None,
        power_loss: None,
    };
    let result = sim.run();
    let hash = sim.trace.value();
    let tick = sim.clock.tick();
    let detail = sim.detail();
    let view = leader(&sim.brokers).map_or(0, |(_, view)| view);
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
            view,
            refused: sim.workload.counts.full,
            outage: sim.power_loss.and_then(|(_, outage)| outage),
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
            if let Some((at, outage @ None)) = &mut self.power_loss
                && leader(&self.brokers).is_some()
            {
                *outage = Some(tick - *at);
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
        if let Some(isolation) = &mut self.isolation
            && leader(&self.brokers)
                .is_some_and(|(broker, view)| broker != isolation.broker && view > isolation.view)
        {
            isolation.replaced = true;
        }
        if let Some(isolation) = self.isolation
            && tick >= isolation.until
        {
            if isolation.leader && !isolation.replaced {
                return Err(format!(
                    "no new leader while broker {} was cut off as leader of view {}",
                    isolation.broker, isolation.view
                ));
            }
            self.network.rejoin();
            if isolation.crashed {
                self.reconnect(isolation.broker)?;
            }
            self.isolation = None;
        }
        if self.isolation.is_none()
            && let Some(fault) = self.options.script.get(self.next_fault).copied()
            && tick >= fault.window().0
            && let Some((leader, view)) = leader(&self.brokers)
        {
            self.next_fault += 1;
            self.trace.word(tick);
            if let Fault::CrashAll { .. } = fault {
                return self.crash_all(tick);
            }
            let leads = fault.leader();
            let crashed = fault.crash();
            let broker = if leads {
                leader
            } else {
                let followers = (0..BROKERS).filter(|&index| index != leader);
                followers
                    .clone()
                    .find(|index| self.options.sparse.contains(index))
                    .or_else(|| followers.clone().next())
                    .expect("two followers")
            };
            if crashed {
                self.crash(broker, tick)?;
            } else {
                self.network.isolate(broker);
            }
            self.isolation = Some(Isolation {
                broker,
                view,
                until: tick + fault.window().1,
                leader: leads,
                replaced: false,
                crashed,
            });
            self.trace.word(broker as u64);
        }
        Ok(())
    }

    /// `broker` loses power and restarts over its durable image, cut off
    /// from both peers, which drop their links to it.
    fn crash(&mut self, broker: usize, tick: u64) -> Result<(), String> {
        let now = self.clock.now();
        self.network.crash(broker);
        for peer in (0..BROKERS).filter(|peer| *peer != broker) {
            let session = session(peer, broker, self.epoch(peer, broker));
            self.brokers[peer].disconnect(broker, session, now)?;
        }
        let old = self.brokers.remove(broker);
        let (restarted, lanes) = old.restart(
            &self.clock,
            self.options.clients,
            self.options.shape(broker),
        )?;
        self.brokers.insert(broker, restarted);
        self.workload.restarted(broker, lanes, tick);
        self.checker.restarted(broker);
        Ok(())
    }

    /// Every broker loses power at the same instant and restarts. Their
    /// links come back at once.
    fn crash_all(&mut self, tick: u64) -> Result<(), String> {
        self.power_loss = Some((tick, None));
        for broker in 0..BROKERS {
            self.crash(broker, tick)?;
        }
        self.network.rejoin();
        let now = self.clock.now();
        for left in 0..BROKERS {
            for right in left + 1..BROKERS {
                let session = session(left, right, self.epoch(left, right));
                self.brokers[left].connect(right, session, now)?;
                self.brokers[right].connect(left, session, now)?;
            }
        }
        Ok(())
    }

    /// Establish fresh links between a restarted `broker` and its peers.
    fn reconnect(&mut self, broker: usize) -> Result<(), String> {
        let now = self.clock.now();
        for peer in (0..BROKERS).filter(|peer| *peer != broker) {
            let session = session(peer, broker, self.epoch(peer, broker));
            self.brokers[peer].connect(broker, session, now)?;
            self.brokers[broker].connect(peer, session, now)?;
        }
        Ok(())
    }

    /// The link between two brokers, counted from zero: each restart of
    /// either starts a new one.
    fn epoch(&self, left: usize, right: usize) -> u64 {
        self.brokers[left].incarnation + self.brokers[right].incarnation
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
            || self.workload.counts.full < self.options.min_refused
            || applied.op.0 < self.options.min_applied
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
                "\nbroker {}: restarts {} view {} ready {} normal {:?} disk {} unbound {} lanes {}/{}",
                broker.index,
                broker.incarnation,
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
