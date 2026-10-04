use std::collections::VecDeque;

use ozzy_proto::RequestId;
use ozzy_replication::driver::{Action, ReplicaDriver};
use ozzy_replication::wire::{Control, FetchOps, ReplicaMessage};
use ozzy_replication::{Admission, Commit, JournalGeneration, NormalReplica, StartView};

use super::disk::{DiskImage, PendingDisk};
use super::flow::Flow;
use super::*;

#[derive(Debug)]
struct Transfer {
    request: FetchOps,
    operations: Vec<Operation>,
    retry_at: Duration,
}

#[derive(Debug)]
pub(crate) struct Replica {
    pub driver: Option<Box<ReplicaDriver>>,
    pub(super) configuration: Configuration,
    pub(super) recovering: Option<super::recovery::LostState>,
    pub(super) donors: [Option<super::recovery::Donor>; 3],
    pub stable: DiskImage,
    pub storage: Option<ozzy_journal_segment::simulation::Journal>,
    pub storage_error: Option<String>,
    pub accepted: Vec<Operation>,
    pub(super) buffered: Vec<Operation>,
    pub(super) images: CanonicalImages,
    pub(super) candidate: Option<CanonicalImages>,
    pub(super) io: VecDeque<PendingDisk>,
    pub(super) generation: JournalGeneration,
    pub(super) observed_sync: usize,
    pub(super) id: usize,
    next_generation: u128,
    pub(super) next_request: u128,
    pin: Option<History>,
    cache: [Option<History>; 3],
    transfer: Option<Transfer>,
    start: Option<(usize, StartView)>,
    deferred_commit: Option<(usize, Commit)>,
    pub(super) cursors: [Prefix; 3],
    pub(super) flow: Flow,
    ack_at: Duration,
    cache_scope: Scope,
    pub transfers_completed: usize,
    pub transfer_chunks_received: usize,
}

impl Replica {
    pub(crate) fn new(id: usize) -> Self {
        Self::with_policy(id, QuorumPolicy::Durable)
    }

    pub(super) fn with_policy(id: usize, policy: QuorumPolicy) -> Self {
        let configuration = configuration_record_with_policy(policy).configuration();
        let generation = JournalGeneration(((id as u128 + 1) << 64) | 1);
        let driver = ReplicaDriver::from_normal(
            NormalReplica::bootstrap(configuration, node(id), generation, limits()).unwrap(),
            Duration::ZERO,
            timing(),
        )
        .unwrap();
        Self {
            driver: Some(Box::new(driver)),
            configuration,
            recovering: None,
            donors: std::array::from_fn(|_| None),
            stable: DiskImage::new(configuration.scope()),
            storage: None,
            storage_error: None,
            accepted: Vec::new(),
            buffered: Vec::new(),
            images: CanonicalImages::new(StateLimits::default(), HISTORY_LIMIT, PIPELINE),
            candidate: None,
            io: VecDeque::new(),
            generation,
            observed_sync: 0,
            id,
            next_generation: generation.0,
            next_request: (id as u128 + 1) << 64,
            pin: None,
            cache: std::array::from_fn(|_| None),
            transfer: None,
            start: None,
            deferred_commit: None,
            cursors: [Prefix::GENESIS; 3],
            flow: Flow::new(id, generation, configuration.scope(), Duration::ZERO, true),
            ack_at: Duration::ZERO,
            cache_scope: configuration.scope(),
            transfers_completed: 0,
            transfer_chunks_received: 0,
        }
    }

    pub(crate) fn snapshot(&self) -> Option<ozzy_replication::ReplicaSnapshot> {
        self.driver.as_ref()?.normal().map(NormalReplica::snapshot)
    }

    pub(crate) fn online(&self) -> bool {
        self.driver.is_some() || self.recovering.is_some()
    }

    pub(super) fn fresh_generation(&mut self) -> JournalGeneration {
        self.next_generation += 1;
        JournalGeneration(self.next_generation)
    }

    pub(crate) fn power_cut(&mut self) {
        if let Some(storage) = &mut self.storage {
            storage.crash();
        }
        self.driver = None;
        self.recovering = None;
        self.donors = std::array::from_fn(|_| None);
        self.io.clear();
        self.accepted.clear();
        self.buffered.clone_from(&self.stable.operations);
        self.candidate = None;
        self.pin = None;
        self.cache = std::array::from_fn(|_| None);
        self.transfer = None;
        self.start = None;
        self.deferred_commit = None;
        self.cursors = [Prefix::GENESIS; 3];
    }

    pub(crate) fn reopen(&mut self, now: Duration) {
        assert!(self.driver.is_none());
        self.generation = self.fresh_generation();
        if let Some(storage) = &mut self.storage {
            match storage.recover(self.generation) {
                Ok(recovered) => {
                    self.stable = super::storage::image(recovered, self.configuration);
                    self.storage_error = None;
                }
                Err(error) => {
                    self.storage_error = Some(error.to_string());
                    self.recovering = None;
                    return;
                }
            }
        }
        if !self.stable.admitted {
            self.begin_recovery(now);
            return;
        }
        self.recovering = None;
        self.accepted.clone_from(&self.stable.operations);
        self.buffered.clone_from(&self.stable.operations);
        self.observed_sync = self.stable.operations.len();
        self.candidate = Some(private_replay(&self.stable.operations));
        let recover = if self.configuration.policy() == QuorumPolicy::Replicated {
            ozzy_replication::ViewChange::recover_drained
        } else {
            ozzy_replication::ViewChange::recover_intact
        };
        let changing = recover(
            self.configuration,
            node(self.id),
            self.generation,
            ozzy_replication::RecoveredState {
                scope: self.stable.promised,
                log: ozzy_replication::FrozenLog {
                    last_normal_view: self.stable.last_normal_view,
                    accepted: tail(&self.stable.operations),
                    committed: self.stable.committed,
                },
            },
            limits(),
        )
        .unwrap();
        self.driver = Some(Box::new(
            ReplicaDriver::from_view_change(changing, now, timing()).unwrap(),
        ));
        self.pin_current();
        self.flow = Flow::new(
            self.id,
            self.generation,
            self.driver.as_ref().unwrap().scope(),
            now,
            false,
        );
        self.ack_at = now;
    }

    pub(super) fn pin_current(&mut self) {
        self.pin = Some(History {
            source: LogSource {
                voter: node(self.id),
                generation: self.generation,
                accepted: tail(&self.stable.operations),
            },
            operations: self.stable.operations.clone(),
        });
    }

    fn history(&self, source: LogSource) -> Option<&History> {
        self.pin
            .iter()
            .chain(self.cache.iter().flatten())
            .find(|history| history.source == source)
    }

    fn lookup(&self, source: LogSource, number: OpNumber) -> Option<Digest> {
        self.history(source)?.lookup(source, number)
    }

    pub(crate) fn admit(
        &mut self,
        from: usize,
        scope: Scope,
        operations: &[Operation],
        now: Duration,
    ) -> bool {
        if self.driver.is_some() {
            self.sync_flow(now);
        }
        let Some(driver) = &mut self.driver else {
            return false;
        };
        // Let the driver observe a newer prepare even when the old image cannot
        // admit it. Such a message fences; it cannot install authority.
        if scope != driver.scope() || driver.normal().is_none() {
            let metadata: Vec<_> = operations.iter().map(Operation::metadata).collect();
            if let Err(error) = driver.prepare(node(from), scope, &metadata, now) {
                assert!(retryable(error), "unexpected prepare rejection: {error:?}");
            }
            return false;
        }
        let snapshot = driver.normal().unwrap().snapshot();
        if !snapshot.ready_for_appends || self.candidate.is_some() {
            return false;
        }
        let first_new = operations.partition_point(|op| op.number <= snapshot.accepted.op.0);
        // Old descriptors may already have retired. Verify any overlap against
        // the local log, never compare with another replica's model memory.
        for op in &operations[..first_new] {
            assert_eq!(self.accepted.get(op.number as usize - 1), Some(op));
        }
        if first_new == operations.len() {
            return true;
        }
        let fresh = &operations[first_new..];
        if fresh[0].number != snapshot.accepted.op.0 + 1
            || fresh.len() + snapshot.pending_operations > PIPELINE
            || fresh.iter().map(|op| op.body.len()).sum::<usize>() + snapshot.pending_body_bytes
                > BODY_BYTES
            || self.accepted.len() + fresh.len() > HISTORY_LIMIT
        {
            return false;
        }
        let transitions: Vec<_> = fresh.iter().map(|op| (op.number, op.decoded())).collect();
        let plans = self.images.prepare_group(&transitions).unwrap();
        let metadata: Vec<_> = fresh.iter().map(Operation::metadata).collect();
        let validation = driver.begin_validation().unwrap();
        match driver.prepare_validated(node(from), validation, &metadata, now) {
            Ok(Admission::Write {
                ticket,
                first_new: 0,
            }) => {
                self.images.install_prepared_group(plans).unwrap();
                self.accepted.extend_from_slice(fresh);
                self.enqueue(DiskAction::Write(ticket));
                true
            }
            Err(error) if retryable(error) => false,
            other => panic!("unexpected fresh admission: {other:?}"),
        }
    }

    pub(crate) fn receive(&mut self, packet: &Packet, now: Duration) -> Vec<Packet> {
        if self.recovering.is_some() {
            self.receive_recovery(packet, now);
            return Vec::new();
        }
        if self.driver.is_none() {
            return Vec::new();
        }
        self.sync_flow(now);
        let mut output = Vec::new();
        let message = match packet.decode_for(self.configuration, self.flow.epoch()) {
            Ok(message) => message,
            Err(ozzy_replication::wire::WireError::Flow(
                ozzy_replication::flow::FlowError::Channel,
            )) => return output,
            Err(error) => panic!("unexpected wire rejection: {error:?}"),
        };
        match message {
            ReplicaMessage::Checkpoint(_) | ReplicaMessage::HistoryRetired(_) => {
                panic!("retained recovery is exercised by the real broker harness")
            }
            ReplicaMessage::Recovery(message) => {
                self.receive_recovery_request(packet.from, &message);
            }
            ReplicaMessage::Flow(message) => {
                self.receive_flow(packet.from, message, now, &mut output);
            }
            ReplicaMessage::Control(message) => {
                self.acknowledge_cursor(packet.from, &message);
                self.receive_control(packet.from, message, now);
            }
            ReplicaMessage::Prepare(_) => unreachable!("epoch binding rejects legacy data"),
            ReplicaMessage::FetchOps(request) => {
                if request.scope == self.driver.as_ref().unwrap().scope()
                    && let Some(history) = self
                        .donor_history(packet.from)
                        .filter(|history| history.source == request.source)
                        .or(self.pin.as_ref())
                    && history.source == request.source
                    && history.lookup(request.source, request.predecessor.op)
                        == Some(request.predecessor.digest)
                {
                    let first = request.predecessor.op.0 as usize;
                    let end =
                        (first + request.max_operations as usize).min(history.operations.len());
                    let mut bytes = 0;
                    let count = history.operations[first..end]
                        .iter()
                        .take_while(|op| {
                            bytes += op.body.len();
                            bytes <= request.max_body_bytes as usize
                        })
                        .count();
                    if count != 0 {
                        output.push(Packet::ops(
                            self.id,
                            packet.from,
                            request,
                            &history.operations[first..first + count],
                        ));
                    }
                }
            }
            ReplicaMessage::Ops(batch) => {
                if let Some(transfer) = &mut self.transfer
                    && transfer.request.scope == self.driver.as_ref().unwrap().scope()
                    && batch.validate_response(transfer.request).is_ok()
                {
                    let operations: Vec<_> = batch
                        .operations()
                        .map(|operation| wire::own(operation, self.configuration))
                        .collect();
                    assert!(transfer.operations.len() + operations.len() <= HISTORY_LIMIT);
                    transfer.operations.extend(operations);
                    self.transfer_chunks_received += 1;
                    if batch.end() == batch.source().accepted {
                        // Only a chain reaching the advertised tail enters the
                        // lookup cache. Intermediate responses are not evidence.
                        let transfer = self.transfer.take().unwrap();
                        self.cache[index(batch.source().voter)] = Some(History {
                            source: batch.source(),
                            operations: transfer.operations,
                        });
                        self.transfers_completed += 1;
                    } else {
                        self.next_request += 1;
                        transfer.request.request_id =
                            RequestId::from_bytes(self.next_request.to_be_bytes());
                        transfer.request.predecessor = batch.end();
                        transfer.retry_at = now;
                    }
                }
            }
        }
        output
    }

    fn acknowledge_cursor(&mut self, from: usize, message: &Control) {
        let acknowledgment = match message {
            Control::PrepareOk { ack, .. } => Some((ack.scope, ack.durable)),
            Control::PrepareRetained { ack, .. } => Some((ack.scope, ack.retained)),
            _ => None,
        };
        if let Some((scope, retained)) = acknowledgment
            && self.snapshot().is_some_and(|s| s.scope == scope)
            && retained.op.0 as usize <= self.accepted.len()
            && prefix(&self.accepted, retained.op.0 as usize) == retained
        {
            self.cursors[from] = retained;
        }
    }

    pub(super) fn receive_control(&mut self, from: usize, message: Control, now: Duration) {
        match self
            .driver
            .as_mut()
            .unwrap()
            .receive(node(from), message, now)
        {
            Ok(Some(start)) => self.start = Some((from, start)),
            Ok(None) => {}
            Err(DriverError::Replication(ReplicationError::HistoryGap)) => {
                if let Control::Commit(commit) = message {
                    self.deferred_commit = Some((from, commit));
                }
            }
            Err(error) => assert!(
                retryable(error),
                "unexpected control rejection: {error:?}; packet={message:?}"
            ),
        }
    }

    fn want_history(&mut self, source: LogSource, now: Duration) {
        if self.history(source).is_some() {
            return;
        }
        if source.accepted == Prefix::GENESIS {
            self.cache[index(source.voter)] = Some(History {
                source,
                operations: Vec::new(),
            });
        } else if self
            .transfer
            .as_ref()
            .is_none_or(|transfer| transfer.request.source != source)
        {
            self.next_request += 1;
            self.transfer = Some(Transfer {
                request: FetchOps {
                    scope: self.driver.as_ref().unwrap().scope(),
                    request_id: RequestId::from_bytes(self.next_request.to_be_bytes()),
                    source,
                    predecessor: Prefix::GENESIS,
                    max_operations: PIPELINE as u32,
                    max_body_bytes: BODY_BYTES as u32,
                },
                operations: Vec::new(),
                retry_at: now,
            });
        }
    }

    fn install(&mut self, now: Duration) {
        if self.snapshot().is_some()
            || !self.io.is_empty()
            || self
                .driver
                .as_ref()
                .unwrap()
                .installation_ticket()
                .is_some()
        {
            return;
        }
        let mut driver = self.driver.take().unwrap();
        let primary = self.configuration.primary(driver.scope().view) == node(self.id);
        let mut missing = None;
        let mut lookup = |source, number| {
            self.lookup(source, number).or_else(|| {
                missing = Some(source);
                None
            })
        };
        let selected = if primary {
            driver
                .select(&mut lookup)
                .map(ozzy_replication::SelectedView::source)
        } else if let Some((_, start)) = self
            .start
            .filter(|(_, start)| start.scope == driver.scope())
        {
            Ok(LogSource {
                voter: self.configuration.primary(start.scope.view),
                generation: start.generation,
                accepted: start.accepted,
            })
        } else {
            self.driver = Some(driver);
            return;
        };
        let source = match selected {
            Ok(source) => source,
            Err(error) => {
                assert!(retryable(error), "selection rejected: {error:?}");
                self.driver = Some(driver);
                if let Some(source) = missing {
                    self.want_history(source, now);
                }
                return;
            }
        };
        if self.history(source).is_none() {
            self.driver = Some(driver);
            self.want_history(source, now);
            return;
        }
        let generation = self.fresh_generation();
        let ticket = if primary {
            driver.begin_primary_install(generation)
        } else {
            let (from, start) = self.start.unwrap();
            driver.begin_backup_install(node(from), start, generation, |source, number| {
                self.lookup(source, number)
            })
        };
        self.driver = Some(driver);
        match ticket {
            Ok(ticket) => {
                let operations = self.history(source).unwrap().operations.clone();
                let floor = ticket.protected_committed().op.0 as usize;
                assert_eq!(&operations[..floor], &self.stable.operations[..floor]);
                let candidate = private_replay(&operations);
                for chunk in operations[ticket.committed().op.0 as usize..].chunks(PIPELINE) {
                    let metadata: Vec<_> = chunk.iter().map(Operation::metadata).collect();
                    self.driver
                        .as_mut()
                        .unwrap()
                        .validate_install_suffix(&metadata)
                        .unwrap();
                }
                self.candidate = Some(candidate);
                self.enqueue(DiskAction::Install { ticket, operations });
            }
            Err(error) => assert!(retryable(error), "installation rejected: {error:?}"),
        }
    }

    pub(crate) fn pump(&mut self, now: Duration) -> Vec<Packet> {
        if self.recovering.is_some() {
            return self.pump_recovery(now);
        }
        let Some(driver) = &self.driver else {
            return Vec::new();
        };
        if self.cache_scope != driver.scope() {
            self.cache_scope = driver.scope();
            self.cache = std::array::from_fn(|_| None);
            self.donors = std::array::from_fn(|_| None);
            self.transfer = None;
            self.cursors = [Prefix::GENESIS; 3];
            self.deferred_commit = self
                .deferred_commit
                .filter(|(_, commit)| commit.scope == self.cache_scope);
        }
        if let Some((from, commit)) = self.deferred_commit.take() {
            self.receive_control(from, Control::Commit(commit), now);
        }
        self.apply_normal();
        let mut output = Vec::new();
        for _ in 0..6 {
            match self.driver.as_mut().unwrap().poll(now).unwrap() {
                Some(Action::Broadcast(message)) => {
                    for to in 0..3 {
                        if to != self.id {
                            output.push(Packet::control(self.id, to, message));
                        }
                    }
                }
                Some(Action::Send { to, message }) => {
                    output.push(Packet::control(self.id, index(to), message));
                }
                Some(Action::PersistPromise(ticket)) => self.enqueue(DiskAction::Promise(ticket)),
                None => break,
            }
        }
        if self.io.is_empty() {
            if let Ok(ticket) = self.driver.as_ref().unwrap().begin_sync()
                && ticket.through().0 as usize > self.observed_sync
            {
                self.enqueue(DiskAction::Sync(ticket));
            } else if self.candidate.is_some()
                && let Ok(ticket) = self.driver.as_ref().unwrap().begin_activation()
            {
                self.enqueue(DiskAction::Activate(ticket));
            }
        }
        self.install(now);
        if let Some(transfer) = &mut self.transfer
            && now >= transfer.retry_at
        {
            transfer.retry_at = now + timing().retransmit;
            output.push(Packet::fetch(
                self.id,
                index(transfer.request.source.voter),
                transfer.request,
            ));
        }
        if now >= self.ack_at {
            self.ack_at = now + timing().retransmit;
            self.confirmation_ack(&mut output);
        }
        self.normal_packets(now, &mut output);
        self.pump_donors(&mut output);
        assert!(output.len() <= 23);
        output
    }

    pub(crate) fn apply_normal(&mut self) {
        if self.candidate.is_some() {
            return;
        }
        if let Some(snapshot) = self.snapshot()
            && snapshot.ready_for_appends
            && snapshot.committed.op > snapshot.applied.op
        {
            self.images.commit_through(snapshot.committed.op.0).unwrap();
            self.driver
                .as_mut()
                .unwrap()
                .apply_through(snapshot.committed)
                .unwrap();
        }
    }

    fn confirmation_ack(&self, output: &mut Vec<Packet>) {
        let Some(normal) = self.driver.as_ref().unwrap().normal() else {
            return;
        };
        let snapshot = normal.snapshot();
        if self.configuration.primary(snapshot.scope.view) != node(self.id) {
            let ack = match self.configuration.policy() {
                QuorumPolicy::Durable => Control::PrepareOk {
                    ack: normal.acknowledgment().unwrap(),
                },
                QuorumPolicy::Replicated => Control::PrepareRetained {
                    ack: normal.retained_acknowledgment().unwrap(),
                },
            };
            output.push(Packet::control(
                self.id,
                index(self.configuration.primary(snapshot.scope.view)),
                ack,
            ));
        }
    }
}
