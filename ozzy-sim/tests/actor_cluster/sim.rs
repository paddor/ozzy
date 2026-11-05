//! The tick loop. Each tick: advance time, start or heal network faults,
//! deliver due frames, then in each 1 ms disk step complete due jobs and run
//! every broker's shard turn in seeded order. Clients then observe replies
//! and submit. Checks run after every tick. `OZZY_ACTOR_SIM_TRACE` prints
//! each broker's view and positions when they change.

mod script;

pub(crate) use script::Fault;

use ozzy_io::simulation::Image;

use crate::{
    broker::{Broker, Control, Lane, Shape, session},
    chaos::{self, Chaos, Kind},
    check::{Checker, converged, leader, log_end},
    clock::Clock,
    consumer::{self, Consumers},
    disk::Disk,
    network::{BROKERS, Network, Profile, Stats},
    records::{self, Ledger, Producers},
    seeds::{Rng, Seeds, Trace},
    workload::{Counts, Workload},
};

/// Disk completion steps per 10 ms tick.
const DISK_STEPS: u64 = 10;

/// A skewed leader's clock offset, 5 s ahead: past the tolerance, and past
/// every due time a producer picks.
const SKEW: i64 = 5000;

/// Mean disk completion delay of a slow broker, in 1 ms steps: 2 to 16 times
/// a profile's.
const SLOW_DISK_MEAN: f64 = 16.0;

#[derive(Debug, Clone, Copy)]
pub(crate) struct Options {
    /// Ticks of faults and new proposals.
    pub ticks: u64,
    /// Further ticks to heal, settle outstanding proposals, and converge.
    pub drain: u64,
    pub clients: usize,
    /// Partitions each broker hosts, one per group, on one device.
    pub partitions: usize,
    /// Barrier operations per proposal.
    pub batch: usize,
    /// Journal segment limit and size.
    pub max_segments: usize,
    pub segment_bytes: u64,
    /// Brokers that write one operation per physical group, so their
    /// journals need several times the space of a leader that packs a
    /// proposal into one group.
    pub sparse: &'static [usize],
    /// Brokers whose disk takes `SLOW_DISK_MEAN` per job.
    pub slow: &'static [usize],
    /// Producers append records beside the barrier clients.
    pub records: bool,
    /// Random overlapping faults beside the profile's and the script's.
    pub chaos: bool,
    /// The safeguard a negative control turns off, if any.
    pub control: Control,
    /// Vacuity floors: confirmed proposals, confirmed records per partition,
    /// ops two brokers reported, the converged op, and proposals a full
    /// journal refused.
    pub min_confirmed: usize,
    pub min_records: usize,
    pub min_compared: u64,
    pub min_applied: u64,
    pub min_refused: u64,
    /// Scripted faults in tick order, on top of the profile's random ones.
    pub script: &'static [Fault],
}

impl Options {
    fn shape(&self, broker: usize) -> Shape {
        Shape {
            partitions: self.partitions,
            control: self.control,
            max_segments: self.max_segments,
            segment_bytes: self.segment_bytes,
            sparse: self.sparse.contains(&broker),
        }
    }
}

/// What survives a broker restart.
#[derive(Debug)]
enum Loss {
    /// Only the process dies; the disk cache survives.
    Process,
    /// Power fails; what reached media survives.
    Power,
    /// What an earlier `Disk::snapshot` held survives.
    Rewind(Image),
}

#[derive(Debug, Clone, Copy)]
struct Isolation {
    broker: usize,
    view: u64,
    until: u64,
    /// After a leader fault another broker must take over. A crashed
    /// broker has no links until the isolation ends.
    fault: Fault,
    /// Another broker led a later view at some tick of the isolation. A
    /// fault that ends it may leave no leader at the last tick.
    replaced: bool,
    /// It restarted and has no links until the isolation ends.
    restarted: bool,
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
    /// Random overlapping faults started.
    pub faults: u64,
    /// Records confirmed and read, and how many of them were scheduled.
    pub records: usize,
    pub scheduled: usize,
    /// Live copies consumers read, and replay streams they canceled to go live.
    pub live: u64,
    pub cancels: u64,
    /// Dedup partition: producer restarts, sessions that retired, records a
    /// resend found stored, and records stored again after their window.
    pub restarts: u64,
    pub retired: u64,
    pub duplicates: u64,
    pub repeats: usize,
    /// Visits only the safety sweep found work for within 1 s of a clock
    /// step, before the guard's next look could see it.
    pub clock_wakeups: u64,
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
    producers: Producers,
    consumers: Consumers,
    /// Every confirmed and read record, by partition.
    ledgers: Vec<Ledger>,
    /// History checks, by group.
    checkers: Vec<Checker>,
    trace: Trace,
    options: Options,
    /// The next scripted fault and a running isolation.
    next_fault: usize,
    isolation: Option<Isolation>,
    /// The tick of the last whole-cluster power loss, and how long until a
    /// leader served again.
    power_loss: Option<(u64, Option<u64>)>,
    /// The seeded tick of a scheduled whole-cluster power loss. It holds
    /// later faults.
    crash_due: Option<u64>,
    /// Disk copies a scheduled power loss rewinds to.
    snapshots: Option<Vec<Image>>,
    chaos: Option<Chaos>,
    /// Connections between brokers so far, by pair; each new one after a
    /// drop gets a fresh session.
    links: [[u64; BROKERS]; BROKERS],
    /// Whether two brokers have a connection now.
    connected: [[bool; BROKERS]; BROKERS],
    /// Visits only the safety sweep found work for right after a clock step
    /// or the guard resuming.
    clock_wakeups: u64,
}

pub(crate) fn run(seed: u64, profile: Profile, options: Options) -> Result<Report, Box<Failure>> {
    let seeds = Seeds::derive(seed);
    let clock = Clock::default();
    let mut disks = Rng::new(seeds.disk);
    let mut brokers = Vec::new();
    let mut lanes: Vec<Vec<Lane>> = Vec::new();
    for index in 0..BROKERS {
        let mean = if options.slow.contains(&index) {
            SLOW_DISK_MEAN
        } else {
            profile.disk_mean
        };
        let disk = Disk::new(disks.next(), mean);
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
        producers: Producers::new(seeds.producers, options.partitions),
        consumers: Consumers::new(seeds.consumers, options.partitions, profile.pub_loss),
        ledgers: (0..options.partitions).map(|_| Ledger::default()).collect(),
        checkers: (0..options.partitions)
            .map(|_| Checker::default())
            .collect(),
        trace: Trace::default(),
        options,
        next_fault: 0,
        isolation: None,
        power_loss: None,
        crash_due: None,
        snapshots: None,
        chaos: options.chaos.then(|| Chaos::new(seeds.chaos)),
        links: [[0; BROKERS]; BROKERS],
        connected: [[true; BROKERS]; BROKERS],
        clock_wakeups: 0,
    };
    let result = sim.run();
    let hash = sim.trace.value();
    let tick = sim.clock.tick();
    let detail = sim.detail();
    if matches!(result, Ok((_, true))) && std::env::var_os("OZZY_ACTOR_SIM_TRACE").is_some() {
        eprintln!("vacuous run:\n{detail}");
    }
    let view = leader(&sim.brokers, 0).map_or(0, |(_, view)| view);
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
            compared: sim.checkers.iter().map(|checker| checker.compared).sum(),
            applied,
            view,
            refused: sim.workload.counts.full,
            outage: sim.power_loss.and_then(|(_, outage)| outage),
            faults: sim.chaos.as_ref().map_or(0, Chaos::total),
            records: sim.ledgers.iter().map(Ledger::len).sum(),
            scheduled: sim.ledgers.iter().map(Ledger::scheduled).sum(),
            live: sim.consumers.counts.published,
            cancels: sim.consumers.counts.cancels,
            restarts: sim.producers.counts.restarts,
            retired: sim.producers.counts.retired,
            duplicates: sim.producers.counts.duplicates,
            repeats: sim.ledgers.iter().map(Ledger::repeats).sum(),
            clock_wakeups: sim.clock_wakeups,
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
            self.nemesis(tick, draining)?;
            self.network.step(tick);
            let mut order: [usize; BROKERS] = std::array::from_fn(|index| index);
            self.executor.shuffle(&mut order);
            for index in order {
                let broker = &mut self.brokers[index];
                self.network
                    .deliver(index, tick, &mut broker.inbox, &mut self.trace);
            }
            // Disks finish jobs in 1 ms steps; each step lets every shard
            // observe them. Time and the network move per tick. A paused
            // process observes nothing.
            let paused = self
                .isolation
                .filter(|isolation| matches!(isolation.fault, Fault::PauseLeader { .. }))
                .map(|isolation| isolation.broker);
            // A broker its injected disk error stopped restarts at the end of
            // the tick.
            let mut exited = None;
            let held_paused: [bool; BROKERS] =
                std::array::from_fn(|index| self.held(index, Kind::Pause));
            // Each running dispatcher samples its clock against its peers.
            for index in order {
                if paused != Some(index) && !held_paused[index] {
                    self.brokers[index].sample(&mut self.network, tick, &mut self.trace);
                }
            }
            // A random disk failure may surface after its fault ended.
            let chaos = self.chaos.is_some();
            for step in 0..DISK_STEPS {
                let at = tick * DISK_STEPS + step;
                for index in order {
                    let broker = &mut self.brokers[index];
                    broker.disk.step(at, index, &mut self.trace);
                    if paused == Some(index) || exited == Some(index) || held_paused[index] {
                        continue;
                    }
                    let turn = broker.turn(
                        &mut self.network,
                        &self.clock,
                        &mut self.trace,
                        self.options.clients,
                    );
                    match turn {
                        Ok(Some(lanes)) => self.workload.replace(index, lanes),
                        Ok(None) => {}
                        Err(_)
                            if broker.disk.failed()
                                && (self.isolation.is_some_and(|isolation| {
                                    isolation.broker == index && !isolation.restarted
                                }) || chaos) =>
                        {
                            exited = Some(index);
                        }
                        Err(reason) => return Err(reason),
                    }
                }
            }
            if let Some(broker) = exited
                && let Some(chaos) = &mut self.chaos
            {
                // Its supervisor restarts it over the cache, healthy.
                chaos
                    .active
                    .retain(|fault| (fault.broker, fault.kind) != (broker, Kind::FailDisk));
                self.network.drop_links(broker);
                self.restart(broker, tick, Loss::Process)?;
                self.relink()?;
            } else if let Some(broker) = exited {
                // The host stays up, so the cache does too.
                self.crash(broker, tick, Loss::Process)?;
                if let Some(isolation) = &mut self.isolation {
                    isolation.restarted = true;
                }
            }
            self.workload
                .step(tick, !draining, &mut self.checkers[0], &mut self.trace)?;
            if self.options.records {
                self.clients(tick, !draining)?;
            }
            for broker in &mut self.brokers {
                let status = broker.status(0);
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
                for (group, checker) in self.checkers.iter_mut().enumerate() {
                    checker
                        .observe(broker.index, &broker.status(group))
                        .map_err(|reason| format!("group {group}: {reason}"))?;
                }
                let (after_clock, missed) = broker.missed_wakeups(tick);
                self.clock_wakeups += after_clock;
                if missed != 0 {
                    return Err(format!(
                        "broker {}: lost wakeup: the safety sweep found work {missed} time(s)",
                        broker.index
                    ));
                }
            }
            if let Some((at, outage @ None)) = &mut self.power_loss
                && (0..self.options.partitions).all(|group| leader(&self.brokers, group).is_some())
            {
                *outage = Some(tick - *at);
            }
            if draining
                && self.workload.idle()
                && self.producers.idle()
                && (!self.options.records
                    || ((0..self.options.partitions).all(|partition| {
                        log_end(&self.brokers, partition)
                            .is_some_and(|end| self.consumers.caught_up(partition, end))
                    }) && self.ledgers.iter().all(Ledger::released)
                        && self.facts_ready()))
                && (1..self.options.partitions)
                    .all(|group| converged(&self.brokers, group).is_some())
                && let Some(applied) = converged(&self.brokers, 0)
            {
                return self.finish(applied);
            }
        }
        Err(format!(
            "no convergence after {} drain ticks",
            self.options.drain
        ))
    }

    /// Hand every client its replies, then let producers and consumers send.
    fn clients(&mut self, tick: u64, submit: bool) -> Result<(), String> {
        let now = self.clock.now();
        for broker in 0..BROKERS {
            while let Some(message) = self.brokers[broker].replies.pop_front() {
                if let Some(index) = records::producer(&message) {
                    self.producers.reply(
                        index,
                        broker,
                        &message,
                        tick,
                        &mut self.ledgers,
                        &mut self.trace,
                    )?;
                } else if let Some(index) = consumer::consumer(&message) {
                    self.consumers.reply(
                        index,
                        broker,
                        &message,
                        tick,
                        now,
                        &mut self.ledgers,
                        &self.producers,
                    )?;
                }
            }
            while let Some(message) = self.brokers[broker].publications.pop_front() {
                self.consumers.publish(&self.brokers[broker], &message);
            }
        }
        for broker in &self.brokers {
            self.producers
                .clock_offset(broker.host.offset().unsigned_abs());
        }
        self.producers
            .step(tick, submit, &mut self.brokers, &mut self.trace)?;
        let ends: Vec<Option<u64>> = (0..self.options.partitions)
            .map(|partition| log_end(&self.brokers, partition))
            .collect();
        self.consumers.step(
            tick,
            now,
            &mut self.brokers,
            &ends,
            &mut self.ledgers,
            &self.producers,
            &mut self.trace,
        )
    }

    /// End a due isolation, then start the next scripted fault once it is due
    /// and a leader exists.
    fn script(&mut self, tick: u64) -> Result<(), String> {
        if let Some(isolation) = &mut self.isolation
            && leader(&self.brokers, 0)
                .is_some_and(|(broker, view)| broker != isolation.broker && view > isolation.view)
        {
            isolation.replaced = true;
        }
        if let Some(isolation) = self.isolation
            && tick >= isolation.until
        {
            if isolation.fault.leader() && !isolation.replaced {
                let how = match isolation.fault {
                    Fault::StallLeader { .. } => "stalled",
                    Fault::PauseLeader { .. } => "paused",
                    _ => "cut off",
                };
                return Err(format!(
                    "no new leader while broker {} was {how} as leader of view {}",
                    isolation.broker, isolation.view
                ));
            }
            if matches!(
                isolation.fault,
                Fault::FailLeaderDisk { .. } | Fault::FailFollowerDisk { .. }
            ) && !isolation.restarted
            {
                return Err(format!(
                    "broker {} never stopped on its injected disk error",
                    isolation.broker
                ));
            }
            if matches!(isolation.fault, Fault::SkewLeader { .. }) {
                self.brokers[isolation.broker].step_clock(0, tick);
            }
            self.network.rejoin();
            if isolation.restarted {
                self.reconnect(isolation.broker)?;
            }
            self.isolation = None;
        }
        if self.isolation.is_none()
            && let Some(&Fault::Recrash { within }) = self.options.script.get(self.next_fault)
        {
            let (at, _) = self
                .power_loss
                .ok_or("a recrash needs a power loss first")?;
            self.next_fault += 1;
            self.crash_due = Some(at + 1 + self.executor.below(within));
        }
        if let Some(due) = self.crash_due {
            if tick < due {
                return Ok(());
            }
            self.crash_due = None;
            self.trace.word(tick);
            return self.crash_all(tick);
        }
        if self.isolation.is_none()
            && let Some(fault) = self.options.script.get(self.next_fault).copied()
            && tick >= fault.window().0
            && let Some((leader, view)) = leader(&self.brokers, 0)
        {
            self.next_fault += 1;
            self.trace.word(tick);
            match fault {
                Fault::CrashAll { .. } => return self.crash_all(tick),
                Fault::Rewind { ticks, .. } => {
                    self.snapshots = Some(
                        self.brokers
                            .iter()
                            .map(|broker| broker.disk.snapshot())
                            .collect(),
                    );
                    self.crash_due = Some(tick + ticks);
                    return Ok(());
                }
                Fault::CrashAllMidWrite { within, .. } => {
                    for broker in &mut self.brokers {
                        broker.disk.stall(u64::MAX);
                    }
                    self.crash_due = Some(tick + 1 + self.executor.below(within));
                    return Ok(());
                }
                _ => {}
            }
            let broker = if fault.leader() || matches!(fault, Fault::SkewLeader { .. }) {
                leader
            } else {
                let followers = (0..BROKERS).filter(|&index| index != leader);
                followers
                    .clone()
                    .find(|index| self.options.sparse.contains(index))
                    .or_else(|| followers.clone().next())
                    .expect("two followers")
            };
            match fault {
                _ if fault.crash() => {
                    let loss =
                        if matches!(fault, Fault::KillLeader { .. } | Fault::KillFollower { .. }) {
                            Loss::Process
                        } else {
                            Loss::Power
                        };
                    self.crash(broker, tick, loss)?;
                }
                Fault::StallLeader { .. } => {
                    let until = (tick + fault.window().1) * DISK_STEPS;
                    self.brokers[broker].disk.stall(until);
                    self.network.hold_partitions(broker);
                }
                Fault::PauseLeader { .. } => self.network.hold_partitions(broker),
                Fault::FailLeaderDisk { .. } | Fault::FailFollowerDisk { .. } => {
                    self.brokers[broker].disk.fail_writes();
                    self.network.hold_partitions(broker);
                }
                Fault::SkewLeader { .. } => self.brokers[broker].step_clock(SKEW, tick),
                Fault::DeafLeader { .. } => self.network.cut_one_way(broker, false),
                Fault::MuteLeader { .. } => self.network.cut_one_way(broker, true),
                _ => self.network.isolate(broker),
            }
            self.isolation = Some(Isolation {
                broker,
                view,
                until: tick + fault.window().1,
                fault,
                replaced: false,
                restarted: fault.crash(),
            });
            self.trace.word(broker as u64);
        }
        Ok(())
    }

    /// `broker` stops and restarts over what `loss` left of its disk, cut off
    /// from both peers, which drop their links to it.
    fn crash(&mut self, broker: usize, tick: u64, loss: Loss) -> Result<(), String> {
        self.network.crash(broker);
        self.restart(broker, tick, loss)
    }

    /// `broker` stops and restarts over what `loss` left of its disk. Its
    /// peers drop their links to it; the new process has none.
    fn restart(&mut self, broker: usize, tick: u64, loss: Loss) -> Result<(), String> {
        let now = self.clock.now();
        for peer in (0..BROKERS).filter(|peer| *peer != broker) {
            let session = session(peer, broker, self.epoch(peer, broker));
            self.brokers[peer].disconnect(broker, session, now)?;
            self.connected[peer][broker] = false;
            self.connected[broker][peer] = false;
        }
        let old = self.brokers.remove(broker);
        let (restarted, lanes) = old.restart(
            |disk| match loss {
                Loss::Process => disk.process_crash(),
                Loss::Power => disk.power_loss(),
                Loss::Rewind(snapshot) => disk.rewind(snapshot),
            },
            self.options.clients,
            self.options.shape(broker),
        )?;
        self.brokers.insert(broker, restarted);
        self.workload.restarted(broker, lanes, tick);
        for checker in &mut self.checkers {
            checker.restarted(broker);
        }
        Ok(())
    }

    /// Every broker loses power at the same instant and restarts. Their
    /// links come back at once.
    fn crash_all(&mut self, tick: u64) -> Result<(), String> {
        self.power_loss = Some((tick, None));
        let mut snapshots = self.snapshots.take().map(Vec::into_iter);
        for broker in 0..BROKERS {
            let loss = snapshots
                .as_mut()
                .and_then(Iterator::next)
                .map_or(Loss::Power, Loss::Rewind);
            self.crash(broker, tick, loss)?;
        }
        self.network.rejoin();
        for left in 0..BROKERS {
            for right in left + 1..BROKERS {
                self.connect(left, right)?;
            }
        }
        Ok(())
    }

    /// Establish fresh links between a restarted `broker` and its peers.
    fn reconnect(&mut self, broker: usize) -> Result<(), String> {
        for peer in (0..BROKERS).filter(|peer| *peer != broker) {
            self.connect(peer, broker)?;
        }
        Ok(())
    }

    /// A new connection between two brokers, under a fresh session.
    fn connect(&mut self, left: usize, right: usize) -> Result<(), String> {
        let now = self.clock.now();
        self.links[left.min(right)][left.max(right)] += 1;
        let session = session(left, right, self.epoch(left, right));
        self.brokers[left].connect(right, session, now)?;
        self.brokers[right].connect(left, session, now)?;
        self.connected[left][right] = true;
        self.connected[right][left] = true;
        Ok(())
    }

    /// The current connection between two brokers, counted from zero.
    fn epoch(&self, left: usize, right: usize) -> u64 {
        self.links[left.min(right)][left.max(right)]
    }

    /// Whether a random fault of `kind` holds `broker`.
    fn held(&self, broker: usize, kind: Kind) -> bool {
        self.chaos
            .as_ref()
            .is_some_and(|chaos| chaos.holds(broker, kind))
    }

    /// End due random faults, all of them once draining, then maybe start a
    /// new one.
    fn nemesis(&mut self, tick: u64, draining: bool) -> Result<(), String> {
        let Some(chaos) = &mut self.chaos else {
            return Ok(());
        };
        let ended = if draining {
            chaos.end_all()
        } else {
            chaos.ended(tick)
        };
        let mut changed = !ended.is_empty();
        for fault in ended {
            match fault.kind {
                Kind::Stall => self.brokers[fault.broker].disk.stall(0),
                Kind::FailDisk => self.brokers[fault.broker].disk.heal_writes(),
                Kind::Skew => self.brokers[fault.broker].step_clock(0, tick),
                _ => {}
            }
        }
        let connected = self.connected;
        let drawn = if draining {
            None
        } else {
            self.chaos
                .as_mut()
                .and_then(|chaos| chaos.draw(tick, |left, right| connected[left][right]))
        };
        if let Some(fault) = drawn {
            changed = true;
            self.trace.word(tick);
            self.trace.word(fault.kind as u64);
            self.trace.word(fault.broker as u64);
            self.start(fault, tick)?;
        }
        if changed {
            self.apply_cuts();
            self.relink()?;
        }
        Ok(())
    }

    fn start(&mut self, fault: chaos::Fault, tick: u64) -> Result<(), String> {
        let broker = fault.broker;
        match fault.kind {
            Kind::PowerLoss | Kind::Kill => {
                self.network.drop_links(broker);
                let loss = if fault.kind == Kind::Kill {
                    Loss::Process
                } else {
                    Loss::Power
                };
                self.restart(broker, tick, loss)?;
            }
            Kind::Stall => self.brokers[broker].disk.stall(fault.until * DISK_STEPS),
            Kind::FailDisk => self.brokers[broker].disk.fail_writes(),
            Kind::LinkReset => {
                let now = self.clock.now();
                let session = session(broker, fault.peer, self.epoch(broker, fault.peer));
                self.network.drop_link(broker, fault.peer);
                self.brokers[broker].disconnect(fault.peer, session, now)?;
                self.brokers[fault.peer].disconnect(broker, session, now)?;
                self.connected[broker][fault.peer] = false;
                self.connected[fault.peer][broker] = false;
            }
            Kind::ClientReset => self.brokers[broker].reset_clients(),
            Kind::Skew => self.brokers[broker].step_clock(fault.offset, tick),
            Kind::Pause | Kind::Isolate | Kind::Deaf | Kind::Mute => {}
        }
        Ok(())
    }

    /// Cut each broker as its random faults say. A broker that restarted
    /// stays cut off until its fault ends.
    fn apply_cuts(&mut self) {
        let mut cuts = [None; BROKERS];
        if let Some(chaos) = &self.chaos {
            for fault in &chaos.active {
                let direction = match fault.kind {
                    Kind::PowerLoss | Kind::Kill | Kind::Isolate => 0,
                    Kind::Mute => 1,
                    Kind::Deaf => 2,
                    _ => continue,
                };
                let cut: &mut Option<u64> = &mut cuts[fault.broker];
                *cut =
                    Some(cut.map_or(direction, |old: u64| if old == direction { old } else { 0 }));
            }
        }
        self.network.set_cuts(cuts);
    }

    /// Connect every pair without a connection unless a random fault keeps
    /// it down: a restarted broker until its fault ends, a reset connection
    /// until it comes back.
    fn relink(&mut self) -> Result<(), String> {
        let Some(chaos) = &self.chaos else {
            return Ok(());
        };
        let down =
            |broker: usize| chaos.holds(broker, Kind::PowerLoss) || chaos.holds(broker, Kind::Kill);
        let reset = |left: usize, right: usize| {
            chaos.active.iter().any(|fault| {
                fault.kind == Kind::LinkReset
                    && ((fault.broker, fault.peer) == (left, right)
                        || (fault.broker, fault.peer) == (right, left))
            })
        };
        let connected = &self.connected;
        let pairs: Vec<(usize, usize)> = (0..BROKERS)
            .flat_map(|left| (left + 1..BROKERS).map(move |right| (left, right)))
            .filter(|&(left, right)| {
                !connected[left][right] && !down(left) && !down(right) && !reset(left, right)
            })
            .collect();
        for (left, right) in pairs {
            self.connect(left, right)?;
        }
        Ok(())
    }

    /// Every copy reports its partitions' facts: no journal is busy.
    fn facts_ready(&self) -> bool {
        self.brokers.iter().all(|broker| {
            (0..self.options.partitions).all(|partition| broker.facts(partition).is_some())
        })
    }

    /// The converged op, and whether the run was vacuous.
    fn finish(&self, applied: ozzy_replication::Prefix) -> Result<(u64, bool), String> {
        for broker in &self.brokers {
            for (partition, ledger) in self.ledgers.iter().enumerate() {
                let facts = broker.facts(partition).ok_or_else(|| {
                    format!("broker {}: no facts of partition {partition}", broker.index)
                })?;
                ledger
                    .check_log(
                        broker.index,
                        facts.next_offset.get(),
                        self.consumers.read_through(partition),
                        &self.producers,
                    )
                    .map_err(|reason| format!("partition {partition}: {reason}"))?;
            }
        }
        for through in &self.workload.confirmed {
            if through.op > applied.op {
                return Err(format!(
                    "confirmed op {} is past the converged op {}",
                    through.op.0, applied.op.0
                ));
            }
        }
        let vacuous = self.next_fault < self.options.script.len()
            || self.chaos.as_ref().is_some_and(|chaos| chaos.total() < 5)
            || self.workload.counts.full < self.options.min_refused
            || applied.op.0 < self.options.min_applied
            || self.workload.confirmed.len() < self.options.min_confirmed
            || (self.options.records
                && self
                    .ledgers
                    .iter()
                    .any(|ledger| ledger.len() < self.options.min_records))
            || self.checkers[0].compared < self.options.min_compared;
        Ok((applied.op.0, vacuous))
    }

    fn detail(&self) -> String {
        use std::fmt::Write as _;
        let network: Stats = self.network.stats;
        let counts: Counts = self.workload.counts;
        let mut detail = format!(
            "network {network:?}, {} queued\nworkload {counts:?}, {} confirmed, {} compared\n\
             producers {:?}, {:?} records confirmed\nconsumers {:?}, read through {:?}",
            self.network.queued(),
            self.workload.confirmed.len(),
            self.checkers[0].compared,
            self.producers.counts,
            self.ledgers.iter().map(Ledger::len).collect::<Vec<_>>(),
            self.consumers.counts,
            (0..self.options.partitions)
                .map(|partition| self.consumers.read_through(partition))
                .collect::<Vec<_>>(),
        );
        if let Some(isolation) = self.isolation {
            let _ = write!(detail, "\nisolation {isolation:?}");
        }
        if let Some(chaos) = &self.chaos {
            let _ = write!(
                detail,
                "\nchaos started {:?}\nchaos active {:?}",
                chaos.summary(),
                chaos.active
            );
        }
        // Process-wide, so a campaign may show other runs' entries too.
        for entry in ozzy_runtime::profiling::recent_logs().iter().rev().take(8) {
            let _ = write!(detail, "\nlog {entry:?}");
        }
        let partitions = self.options.partitions;
        for (broker, group) in self
            .brokers
            .iter()
            .flat_map(|broker| (0..partitions).map(move |group| (broker, group)))
        {
            let status = broker.status(group);
            let _ = write!(
                detail,
                "\nbroker {} group {group}: restarts {} clock {:+} ms paused {} view {} ready {} normal {:?} disk {} unbound {} lanes {}/{}",
                broker.index,
                broker.incarnation,
                broker.host.offset(),
                broker.trust.status().paused,
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
