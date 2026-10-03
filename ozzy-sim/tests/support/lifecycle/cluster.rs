use std::collections::VecDeque;

use super::*;

#[derive(Debug, Clone)]
pub(crate) struct Request {
    primary: usize,
    scope: Scope,
    pub operations: Vec<Operation>,
}

impl Request {
    pub(crate) fn end(&self) -> Prefix {
        tail(&self.operations)
    }
}

/// Independent schedule/oracle. Neither is available to replica decisions.
#[derive(Debug)]
pub(crate) struct Cluster {
    pub replicas: [Replica; 3],
    configuration: Configuration,
    pub network: VecDeque<Packet>,
    pub links: [[bool; 3]; 3],
    pub disk_paused: [bool; 3],
    pub callbacks_paused: [bool; 3],
    pub now: Duration,
    pub committed: Vec<Operation>,
    pub acknowledged: Vec<Operation>,
    acknowledged_view: u64,
    // Oracle-only evidence from explicitly damaged disks. No replica can read
    // this. It proves past durability, never current history availability.
    lost_disks: [Option<Vec<Operation>>; 3],
    // Historical RAM retention is oracle evidence only, never repair input.
    lost_ram: [Vec<Operation>; 3],
    seed: u64,
    random: u64,
    trace: VecDeque<String>,
}

impl Cluster {
    pub(crate) fn with_storage(seed: u64) -> Self {
        Self::with_storage_policy(seed, QuorumPolicy::Durable)
    }

    pub(crate) fn with_storage_policy(seed: u64, policy: QuorumPolicy) -> Self {
        let mut cluster = Self::with_policy(seed, policy);
        for replica in &mut cluster.replicas {
            replica.enable_storage(seed);
        }
        cluster
    }

    pub(crate) fn damage_store(
        &mut self,
        replica: usize,
        damage: ozzy_journal_segment::simulation::Damage,
    ) {
        assert!(
            self.lost_disks[replica].is_none(),
            "one permanent media fault per broker per schedule"
        );
        self.cut(replica);
        self.event(format!("damage {replica}: {damage:?}"));
        self.lost_disks[replica] = Some(self.replicas[replica].stable.operations.clone());
        let storage = self.replicas[replica].storage.as_mut().unwrap();
        storage.damage(&storage.active_name(), damage);
        self.replicas[replica].stable.operations.clear();
        self.replicas[replica].stable.committed = Prefix::GENESIS;
        self.check();
    }

    pub(crate) fn quarantine(&mut self, replica: usize) {
        assert!(!self.replicas[replica].online());
        self.event(format!("quarantine {replica}"));
        self.replicas[replica]
            .storage
            .as_mut()
            .unwrap()
            .quarantine()
            .unwrap();
        self.replicas[replica].stable.admitted = false;
        self.reopen(replica);
    }

    pub(crate) fn new(seed: u64) -> Self {
        Self::with_policy(seed, QuorumPolicy::Durable)
    }

    fn with_policy(seed: u64, policy: QuorumPolicy) -> Self {
        Self {
            configuration: configuration_record_with_policy(policy).configuration(),
            replicas: std::array::from_fn(|id| match policy {
                QuorumPolicy::Durable => Replica::new(id),
                QuorumPolicy::Replicated => Replica::with_policy(id, policy),
            }),
            network: VecDeque::new(),
            links: [[true; 3]; 3],
            disk_paused: [false; 3],
            callbacks_paused: [false; 3],
            now: Duration::ZERO,
            committed: Vec::new(),
            acknowledged: Vec::new(),
            acknowledged_view: 0,
            lost_disks: std::array::from_fn(|_| None),
            lost_ram: std::array::from_fn(|_| Vec::new()),
            seed,
            random: seed.max(1),
            trace: VecDeque::new(),
        }
    }

    fn event(&mut self, mut event: String) {
        if self.trace.len() == 256 {
            self.trace.pop_front();
        }
        event.insert_str(0, &format!("{}ms ", self.now.as_millis()));
        self.trace.push_back(event);
    }

    pub(crate) fn choose(&mut self, bound: usize) -> usize {
        self.random ^= self.random << 13;
        self.random ^= self.random >> 7;
        self.random ^= self.random << 17;
        (self.random % bound as u64) as usize
    }

    pub(crate) fn propose(&mut self, from: usize, id: u128) -> Option<Request> {
        let snapshot = self.replicas[from].snapshot()?;
        if !snapshot.ready_for_appends
            || self.replicas[from].candidate.is_some()
            || self.configuration.primary(snapshot.scope.view) != node(from)
        {
            return None;
        }
        let operations = self.replicas[from].record_operations(id);
        self.event(format!(
            "propose {from} id={id} view={} end={}",
            snapshot.scope.view,
            tail(&operations).op.0
        ));
        if !self.replicas[from].admit(from, snapshot.scope, &operations, self.now) {
            return None;
        }
        self.check();
        Some(Request {
            primary: from,
            scope: snapshot.scope,
            operations,
        })
    }

    pub(crate) fn acknowledge(&mut self, request: &Request) -> bool {
        let Some(snapshot) = self.replicas[request.primary].snapshot() else {
            return false;
        };
        let end = request.end();
        if snapshot.scope != request.scope || snapshot.applied.op < end.op {
            return false;
        }
        let through = end.op.0 as usize;
        let operations = &self.replicas[request.primary].accepted;
        let first = request.operations[0].number as usize - 1;
        assert_eq!(&operations[first..through], &request.operations);
        assert_eq!(prefix(operations, through), end);
        if self.configuration.policy() == QuorumPolicy::Durable {
            assert_eq!(
                self.replicas[request.primary]
                    .stable
                    .operations
                    .get(..through),
                Some(&operations[..through])
            );
        }
        assert!(
            self.replicas
                .iter()
                .enumerate()
                .filter(|(index, _)| self.held_confirming_copy(*index, &operations[..through]))
                .count()
                >= 2
        );
        let overlap = through.min(self.acknowledged.len());
        assert_eq!(&self.acknowledged[..overlap], &operations[..overlap]);
        if through > self.acknowledged.len() {
            self.acknowledged = operations[..through].to_vec();
            self.acknowledged_view = request.scope.view;
        }
        self.event(format!(
            "client ACK {} view={} op={}",
            request.primary, request.scope.view, end.op.0
        ));
        self.check();
        true
    }

    /// Retry lookup through the current ready leader. The original canonical
    /// bytes must already be applied; this never manufactures a replacement or
    /// uses the independent confirmation oracle as protocol input.
    pub(crate) fn acknowledge_retry(&mut self, request: &Request) -> bool {
        let Some(primary) = self.primary() else {
            return false;
        };
        let snapshot = self.replicas[primary].snapshot().unwrap();
        let first = request.operations[0].number as usize - 1;
        let through = request.end().op.0 as usize;
        if snapshot.applied.op < request.end().op
            || self.replicas[primary].accepted.get(first..through)
                != Some(request.operations.as_slice())
        {
            return false;
        }
        self.acknowledge(&Request {
            primary,
            scope: snapshot.scope,
            operations: request.operations.clone(),
        })
    }

    fn enqueue(&mut self, packets: Vec<Packet>) {
        for packet in packets {
            if self.network.len() == PACKET_LIMIT {
                self.event(format!("full network drops {}->{}", packet.from, packet.to));
            } else {
                self.network.push_back(packet);
            }
        }
    }

    pub(crate) fn pump(&mut self, replica: usize) {
        let packets = self.replicas[replica].pump(self.now);
        self.enqueue(packets);
        self.check();
    }

    pub(crate) fn perform_disk(&mut self, replica: usize) -> bool {
        if self.disk_paused[replica] {
            return false;
        }
        let action = self.replicas[replica]
            .pending_disk()
            .map(std::mem::discriminant);
        let performed = self.replicas[replica].perform_disk();
        if performed {
            self.event(format!("disk perform {replica}: {action:?}"));
        }
        self.check();
        performed
    }

    pub(crate) fn notify_disk(&mut self, replica: usize) -> Option<DiskAction> {
        if self.callbacks_paused[replica] {
            return None;
        }
        let action = self.replicas[replica].notify_disk(self.now);
        if let Some(action) = &action {
            self.event(format!(
                "disk notify {replica}: {:?}",
                std::mem::discriminant(action)
            ));
        }
        self.check();
        action
    }

    pub(crate) fn deliver(&mut self, position: usize) {
        let packet = self.network.remove(position).unwrap();
        self.event(format!(
            "deliver {}->{} {}",
            packet.from,
            packet.to,
            packet.summary()
        ));
        if self.links[packet.from][packet.to] {
            let replies = self.replicas[packet.to].receive(&packet, self.now);
            self.enqueue(replies);
        }
        self.check();
    }

    pub(crate) fn cut(&mut self, replica: usize) {
        self.event(format!("power cut {replica}"));
        if self.replicas[replica].accepted.len() > self.lost_ram[replica].len() {
            self.lost_ram[replica].clone_from(&self.replicas[replica].accepted);
        }
        self.replicas[replica].power_cut();
        self.check();
    }

    pub(crate) fn reopen(&mut self, replica: usize) {
        self.event(format!(
            "reopen {replica}, admitted={}",
            self.replicas[replica].stable.admitted
        ));
        self.replicas[replica].reopen(self.now);
        assert!(self.replicas[replica].snapshot().is_none());
        self.check();
    }

    pub(crate) fn lose_store(&mut self, replica: usize) {
        assert!(
            self.lost_disks.iter().all(Option::is_none),
            "model permits one permanent disk loss"
        );
        self.cut(replica);
        self.event(format!("erase store {replica}"));
        self.lost_disks[replica] = Some(self.replicas[replica].stable.operations.clone());
        self.replicas[replica].stable = disk::DiskImage::new(self.configuration.scope());
        self.replicas[replica].stable.admitted = false;
        self.replicas[replica].buffered.clear();
        self.check();
    }

    fn held_confirming_copy(&self, replica: usize, known: &[Operation]) -> bool {
        self.was_durable(replica, known)
            || (self.configuration.policy() == QuorumPolicy::Replicated
                && (self.replicas[replica].accepted.get(..known.len()) == Some(known)
                    || self.lost_ram[replica].get(..known.len()) == Some(known)))
    }

    fn was_durable(&self, replica: usize, known: &[Operation]) -> bool {
        self.replicas[replica].stable.operations.get(..known.len()) == Some(known)
            || self.lost_disks[replica]
                .as_ref()
                .is_some_and(|operations| operations.get(..known.len()) == Some(known))
    }

    pub(crate) fn tick(&mut self) {
        self.now += Duration::from_millis(1);
        for replica in 0..3 {
            self.pump(replica);
            self.perform_disk(replica);
            self.notify_disk(replica);
        }
        // Bound both packets and bytes: every encoded packet has fixed limits.
        for _ in 0..PACKET_LIMIT {
            if self.network.is_empty() {
                break;
            }
            self.deliver(0);
        }
    }

    pub(crate) fn until(&mut self, ticks: usize, mut done: impl FnMut(&Self) -> bool) {
        for _ in 0..ticks {
            if done(self) {
                return;
            }
            self.tick();
        }
        assert!(done(self), "liveness deadline expired");
    }

    pub(crate) fn ready(&self, replica: usize, minimum_view: u64, through: Prefix) -> bool {
        self.replicas[replica].snapshot().is_some_and(|snapshot| {
            snapshot.scope.view >= minimum_view
                && snapshot.ready_for_appends
                && snapshot.applied.op >= through.op
                && self.replicas[replica].candidate.is_none()
                && prefix(&self.replicas[replica].accepted, through.op.0 as usize) == through
        })
    }

    pub(crate) fn primary(&self) -> Option<usize> {
        (0..3)
            .filter(|&replica| {
                self.replicas[replica].snapshot().is_some_and(|snapshot| {
                    self.configuration.primary(snapshot.scope.view) == node(replica)
                        && snapshot.ready_for_appends
                        && self.replicas[replica].candidate.is_none()
                })
            })
            .max_by_key(|&replica| self.replicas[replica].snapshot().unwrap().scope.view)
    }

    pub(crate) fn check(&mut self) {
        assert!(self.network.len() <= PACKET_LIMIT);
        for (id, replica) in self.replicas.iter().enumerate() {
            if !replica.stable.admitted {
                assert!(replica.driver.is_none(), "unpublished replacement can vote");
            }
            assert!(replica.stable.operations.len() <= HISTORY_LIMIT);
            assert_eq!(
                prefix(
                    &replica.stable.operations,
                    replica.stable.committed.op.0 as usize
                ),
                replica.stable.committed
            );
            assert!(replica.stable.last_normal_view <= replica.stable.promised.view);
            let Some(snapshot) = replica.snapshot() else {
                continue;
            };
            let through = snapshot.committed.op.0 as usize;
            let known = &replica.accepted[..through];
            assert_eq!(tail(known), snapshot.committed);
            assert!(snapshot.applied.op <= snapshot.committed.op);
            if self.configuration.policy() == QuorumPolicy::Durable {
                assert!(snapshot.committed.op <= snapshot.journal.durable);
                assert_eq!(replica.stable.operations.get(..through), Some(known));
            }
            assert!(snapshot.journal.durable <= snapshot.journal.written);
            assert!(snapshot.journal.written <= snapshot.accepted.op);
            assert!(snapshot.pending_operations <= PIPELINE);
            assert!(snapshot.pending_body_bytes <= BODY_BYTES);
            if through != 0 {
                assert!(
                    self.replicas
                        .iter()
                        .enumerate()
                        .filter(|(index, _)| self.held_confirming_copy(*index, known))
                        .count()
                        >= 2,
                    "commit without historical copies for the configured policy"
                );
            }
            let overlap = through.min(self.committed.len());
            assert_eq!(
                &self.committed[..overlap],
                &known[..overlap],
                "committed history changed"
            );
            if through > self.committed.len() {
                self.committed = known.to_vec();
            }
            if snapshot.ready_for_appends && replica.candidate.is_none() {
                if self.configuration.primary(snapshot.scope.view) == node(id)
                    && snapshot.scope.view >= self.acknowledged_view
                {
                    assert_eq!(
                        replica.accepted.get(..self.acknowledged.len()),
                        Some(self.acknowledged.as_slice()),
                        "ready leader forgot confirmed history"
                    );
                }
                let records = replica.accepted[..snapshot.applied.op.0 as usize]
                    .iter()
                    .map(|operation| match operation.decoded() {
                        OperationBody::Append(append) => append
                            .batches
                            .iter()
                            .map(|batch| batch.records.len())
                            .sum::<usize>(),
                        _ => 0,
                    })
                    .sum::<usize>();
                let visible = replica
                    .images
                    .committed()
                    .partition(workload::partition())
                    .map_or(0, |state| state.next_offset.get());
                assert_eq!(
                    visible, records as u64,
                    "application visibility outran committed records"
                );
            }
        }
        assert_eq!(
            self.committed.get(..self.acknowledged.len()),
            Some(self.acknowledged.as_slice())
        );
        self.check_surviving_copies();
    }

    fn check_surviving_copies(&self) {
        // The RAM-policy schedules preserve at least one live or persisted copy.
        // Total volatile loss is outside their fault budget and tested separately
        // as refusal to restart from unproven state. Oracle memory is never a donor.
        let surviving = self
            .replicas
            .iter()
            .filter(|replica| {
                replica.stable.operations.get(..self.acknowledged.len())
                    == Some(self.acknowledged.as_slice())
                    || (self.configuration.policy() == QuorumPolicy::Replicated
                        && replica.accepted.get(..self.acknowledged.len())
                            == Some(self.acknowledged.as_slice()))
            })
            .count();
        let required = match self.configuration.policy() {
            QuorumPolicy::Replicated => 1,
            QuorumPolicy::Durable => {
                2_usize.saturating_sub(self.lost_disks.iter().filter(|loss| loss.is_some()).count())
            }
        };
        assert!(
            surviving >= required,
            "acknowledged history lost its required surviving copies"
        );
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!(
                "lifecycle seed={} lost_voter={:?} time={:?}",
                self.seed,
                self.lost_disks
                    .iter()
                    .enumerate()
                    .filter_map(|(id, loss)| loss.as_ref().map(|_| id))
                    .collect::<Vec<_>>(),
                self.now
            );
            for (index, replica) in self.replicas.iter().enumerate() {
                eprintln!(
                    "replica {index}: scope={:?} stable={} snapshot={:?} io={:?}",
                    replica.driver.as_ref().map(|driver| driver.scope()),
                    replica.stable.operations.len(),
                    replica.snapshot(),
                    replica.pending_disk().map(std::mem::discriminant)
                );
            }
            for event in &self.trace {
                eprintln!("{event}");
            }
        }
    }
}
