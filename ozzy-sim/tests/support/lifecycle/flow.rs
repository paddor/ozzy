//! Production receipt/credit/repair policy over simulated message delivery.
//! Network entries are already submitted transmissions, not an actor outbox.
//! Local OMQ backpressure and asynchronous validation remain actor-test concerns.

use ozzy_proto::RequestId;
use ozzy_replication::flow::{
    Channel, ProbeTiming, ReceiveEpoch, Receiver, Report, StatusOutcome, Transmitter,
};
use ozzy_replication::wire::{Control, FlowMessage, FlowState};
use ozzy_replication::{Commit, JournalGeneration};

use super::*;

#[derive(Debug)]
pub(super) struct Flow {
    key: Option<(Scope, JournalGeneration)>,
    receive: Receiver,
    peers: [Option<Transmitter>; 3],
    published: Option<Report>,
    // Deterministic startup identities, disjoint across simulated boots. Each
    // boot reserves 2^32 epochs/request IDs; assert before crossing that range.
    boot: u128,
    next_epoch: u128,
}

impl Flow {
    pub(super) fn new(
        id: usize,
        generation: JournalGeneration,
        scope: Scope,
        now: Duration,
        ready: bool,
    ) -> Self {
        assert!(generation.0 < (1u128 << 96));
        let first = (generation.0 << 32) | 1;
        Self {
            key: ready.then_some((scope, generation)),
            receive: Receiver::new(
                Channel {
                    scope,
                    epoch: ReceiveEpoch::new(first).unwrap(),
                },
                Prefix::GENESIS,
                limits(),
            )
            .unwrap(),
            peers: std::array::from_fn(|peer| {
                (peer != id).then(|| {
                    Transmitter::new(
                        scope,
                        limits(),
                        RequestId::from_bytes(first.to_be_bytes()),
                        ProbeTiming {
                            initial: Duration::from_millis(20),
                            maximum: Duration::from_millis(200),
                        },
                        now,
                    )
                    .unwrap()
                })
            }),
            published: None,
            boot: generation.0,
            next_epoch: first,
        }
    }

    pub(super) fn epoch(&self) -> ReceiveEpoch {
        self.receive.report().channel.epoch
    }
}

impl Replica {
    fn flow_ready(&self) -> bool {
        self.snapshot()
            .is_some_and(|s| s.ready_for_appends && self.candidate.is_none())
    }

    pub(super) fn sync_flow(&mut self, now: Duration) {
        let scope = self.driver.as_ref().unwrap().scope();
        for peer in self.flow.peers.iter_mut().flatten() {
            peer.change_scope(scope, now).unwrap();
        }
        if !self.flow_ready() {
            return;
        }
        let snapshot = self.snapshot().unwrap();
        let key = (snapshot.scope, snapshot.journal.generation);
        if self.flow.key != Some(key) {
            assert_eq!(
                snapshot.pending_operations, 0,
                "activate before normal admission"
            );
            self.flow.next_epoch = self.flow.next_epoch.checked_add(1).unwrap();
            assert_eq!(self.flow.next_epoch >> 32, self.flow.boot);
            self.flow
                .receive
                .reinitialize(
                    Channel {
                        scope,
                        epoch: ReceiveEpoch::new(self.flow.next_epoch).unwrap(),
                    },
                    snapshot.applied,
                )
                .unwrap();
            self.flow.key = Some(key);
            self.flow.published = None;
        }
    }

    pub(super) fn receive_flow(
        &mut self,
        from: usize,
        message: FlowMessage<'_>,
        now: Duration,
        output: &mut Vec<Packet>,
    ) {
        if !self.flow_ready() {
            return;
        }
        let scope = self.driver.as_ref().unwrap().scope();
        let primary = index(self.configuration.primary(scope.view));
        match message {
            FlowMessage::Probe(probe) if from == primary && probe.scope == scope => {
                output.push(Packet::state(
                    self.id,
                    from,
                    FlowState {
                        handle: 1,
                        repair_limit: None,
                        request_id: Some(probe.request_id),
                        report: self.flow.receive.report(),
                    },
                ));
            }
            FlowMessage::State(state) if primary == self.id => {
                let outcome = self.flow.peers[from]
                    .as_mut()
                    .unwrap()
                    .observe(state.report, state.request_id, now)
                    .unwrap();
                let opened = outcome == StatusOutcome::Verify && self.open_flow(from, now);
                if opened || outcome == StatusOutcome::Observed {
                    self.received_commit(from, output);
                }
            }
            FlowMessage::Prepare { batch, .. } if from == primary && batch.scope() == scope => {
                let operations: Vec<_> = batch
                    .operations()
                    .map(|operation| wire::own(operation, self.configuration))
                    .collect();
                if self.admit(from, scope, &operations, now) {
                    let report = self.flow.receive.report();
                    let fresh: Vec<_> = operations
                        .iter()
                        .filter(|op| op.number > report.received.op.0)
                        .map(flow_operation)
                        .collect();
                    if !fresh.is_empty() {
                        self.flow.receive.retain(report.channel, &fresh).unwrap();
                    }
                    self.receive_control(
                        from,
                        Control::Commit(Commit {
                            scope,
                            committed: batch.committed(),
                        }),
                        now,
                    );
                    self.publish_receipt(output);
                }
            }
            _ => {}
        }
    }

    fn open_flow(&mut self, to: usize, now: Duration) -> bool {
        let Some(request) = self.flow.peers[to].as_ref().unwrap().candidate() else {
            return false;
        };
        let report = request.report();
        if report.received.op.0 as usize > self.accepted.len() {
            return false;
        }
        // Only this replica's canonical history is available to the adapter.
        // No other replica, stable-disk oracle, or election cache supplies it.
        let base = prefix(&self.accepted, report.base.op.0 as usize);
        let received = prefix(&self.accepted, report.received.op.0 as usize);
        self.flow.peers[to]
            .as_mut()
            .unwrap()
            .open_verified(request, base, received, now)
            .unwrap()
    }

    fn received_commit(&self, to: usize, output: &mut Vec<Packet>) {
        let received = self.flow.peers[to]
            .as_ref()
            .unwrap()
            .sender()
            .unwrap()
            .received();
        let snapshot = self.snapshot().unwrap();
        // Receipt is not a vote. Only the driver's independent quorum commit
        // permits a bounded prefix-specific COMMIT to release this peer's window.
        let committed = if received.op <= snapshot.committed.op {
            received
        } else {
            snapshot.committed
        };
        output.push(Packet::control(
            self.id,
            to,
            Control::Commit(Commit {
                scope: snapshot.scope,
                committed,
            }),
        ));
    }

    fn publish_receipt(&mut self, output: &mut Vec<Packet>) {
        let report = self.flow.receive.report();
        if self.flow.published != Some(report) {
            let primary = index(self.configuration.primary(report.channel.scope.view));
            output.push(Packet::state(
                self.id,
                primary,
                FlowState {
                    handle: 1,
                    repair_limit: None,
                    request_id: None,
                    report,
                },
            ));
            self.flow.published = Some(report);
        }
    }

    pub(super) fn normal_packets(&mut self, now: Duration, output: &mut Vec<Packet>) {
        self.sync_flow(now);
        if !self.flow_ready() {
            return;
        }
        let snapshot = self.snapshot().unwrap();
        if self.configuration.primary(snapshot.scope.view) != node(self.id) {
            let released = snapshot.applied.op.0.min(snapshot.journal.durable.0) as usize;
            self.flow
                .receive
                .release(prefix(&self.accepted, released))
                .unwrap();
            self.publish_receipt(output);
            return;
        }
        let floor = self
            .driver
            .as_ref()
            .unwrap()
            .normal()
            .unwrap()
            .start_view()
            .unwrap()
            .map_or(Prefix::GENESIS, |start| start.accepted);
        for to in 0..3 {
            if to == self.id {
                continue;
            }
            if self.open_flow(to, now) {
                self.received_commit(to, output);
            }
            let peer = self.flow.peers[to].as_mut().unwrap();
            if !(peer
                .sender()
                .is_some_and(|s| s.received() == snapshot.accepted)
                && self.cursors[to].op >= snapshot.accepted.op)
                && let Some(probe) = peer.poll_probe(snapshot.accepted, false, now).unwrap()
            {
                assert_eq!(
                    u128::from_be_bytes(*probe.request_id.as_bytes()) >> 32,
                    self.flow.boot
                );
                output.push(Packet::probe(self.id, to, probe));
            }
            let Some(sender) = peer.sender() else {
                continue;
            };
            let repair = peer.repair(false);
            let after = repair.map_or(sender.sent(), |repair| repair.after);
            if after.op < floor.op || after.op >= snapshot.accepted.op {
                continue;
            }
            let available = repair.map_or_else(|| sender.available(), |_| limits());
            let first = after.op.0 as usize;
            assert_eq!(prefix(&self.accepted, first), after);
            let through = repair.map_or(self.accepted.len(), |repair| repair.through.op.0 as usize);
            let mut bytes = 0;
            let count = self.accepted[first..through]
                .iter()
                .take(available.max_operations)
                .take_while(|op| {
                    bytes += op.body.len();
                    bytes <= available.max_body_bytes
                })
                .count();
            if count == 0 {
                continue;
            }
            let end = first + count;
            let committed = prefix(&self.accepted, end.min(snapshot.committed.op.0 as usize));
            let operations = &self.accepted[first..end];
            let packet = Packet::prepare(
                self.id,
                to,
                snapshot.scope,
                sender.channel().epoch,
                committed,
                operations,
            );
            if let Some(repair) = repair {
                peer.record_repair(repair, tail(operations)).unwrap();
            } else {
                let metadata: Vec<_> = operations.iter().map(flow_operation).collect();
                peer.record_send(&metadata).unwrap();
            }
            output.push(packet);
        }
    }
}

fn flow_operation(operation: &Operation) -> ozzy_replication::flow::Operation {
    ozzy_replication::flow::Operation {
        prefix: operation.prefix(),
        previous_digest: operation.previous,
        body_bytes: operation.body.len() as u64,
    }
}
