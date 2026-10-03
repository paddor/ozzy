//! Fresh application traffic after the timed election/installation fixture.
//! Actors retain their live driver and disk workers. Completion flags only stop
//! the workload; they never authorize an append, ACK, commit, or application.

use super::append_worker::{Command, Completion};
use super::election_driver::timing;
use super::*;
use ozzy_replication::SyncTicket;
use ozzy_replication::driver::{Action, ReplicaDriver};
use tokio::sync::watch;

const RECORDS: u8 = 4;

pub(super) async fn finish(
    sockets: [&Socket; 2],
    replicas: [(ReplicaDriver, Disk); 2],
    acknowledged: Prefix,
    origin: tokio::time::Instant,
) {
    let [(primary, primary_disk), (backup, backup_disk)] = replicas;
    let (done, _) = watch::channel(0u8);
    let (mut primary, mut backup) = tokio::join!(
        Actor::new(1, primary, primary_disk).run(sockets[0], origin, done.clone()),
        Actor::new(2, backup, backup_disk).run(sockets[1], origin, done),
    );
    assert!(primary.dropped_prepare && primary.backup_ack_before_local_sync);
    assert!(backup.dropped_ack && backup.duplicates > 0);
    let committed = primary.driver.normal().unwrap().snapshot().committed;
    assert_eq!(committed.op.0, u64::from(RECORDS) + 2);
    for actor in [&mut primary, &mut backup] {
        let snapshot = actor.driver.normal().unwrap().snapshot();
        assert_eq!(snapshot.scope.view, 1);
        assert_eq!(snapshot.committed, committed);
        assert_eq!(snapshot.applied, committed);
        assert_eq!(snapshot.pending_operations, 0);
        actor
            .disk
            .verify_installed(committed, acknowledged, usize::from(RECORDS))
            .await;
    }
}

struct Actor {
    index: u8,
    driver: ReplicaDriver,
    disk: Disk,
    pending: Option<oneshot::Receiver<Completion>>,
    sync: Option<SyncTicket>,
    flight: Option<(Frames, Prefix)>,
    retry_at: Duration,
    release_sync: Option<oneshot::Sender<()>>,
    next_record: u8,
    dropped_prepare: bool,
    dropped_ack: bool,
    duplicates: usize,
    backup_ack_before_local_sync: bool,
}

impl Drop for Actor {
    fn drop(&mut self) {
        // Disk's destructor joins the worker. Release this fault-injection gate
        // first, including when a test timeout cancels the protocol future.
        if let Some(release) = self.release_sync.take() {
            let _ = release.send(());
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn canceled_actor_releases_held_sync_before_joining_its_disk_worker() {
    let (mut cores, _) = replicas();
    let driver = ReplicaDriver::from_normal(cores.remove(1), Duration::ZERO, timing()).unwrap();
    let mut actor = Actor::new(1, driver, Disk::start(1).await);
    let bytes: Vec<_> = bodies()
        .iter()
        .map(|body| {
            (
                body.kind(),
                encode_operation_body(body, OperationLimits::default()).unwrap(),
            )
        })
        .collect();
    let operations = canonical_operations(&bytes);
    let (release, gate) = oneshot::channel();
    actor.release_sync = Some(release);
    let (written, mut durable) = actor.disk.submit(&operations, Some(gate));
    written.await.unwrap();
    assert_eq!(durable.try_recv(), Err(oneshot::error::TryRecvError::Empty));
    drop(actor);
    let position = durable.await.unwrap();
    assert_eq!(position.op_number, 3);
    assert_eq!(position.digest, operations.last().unwrap().prefix().digest);
}

impl Actor {
    fn new(index: u8, driver: ReplicaDriver, disk: Disk) -> Self {
        Self {
            index,
            driver,
            disk,
            pending: None,
            sync: None,
            flight: None,
            retry_at: Duration::ZERO,
            release_sync: None,
            next_record: 1,
            dropped_prepare: false,
            dropped_ack: false,
            duplicates: 0,
            backup_ack_before_local_sync: false,
        }
    }

    fn other(&self) -> u8 {
        3 - self.index
    }

    async fn run(
        mut self,
        socket: &Socket,
        origin: tokio::time::Instant,
        done: watch::Sender<u8>,
    ) -> Self {
        let mut completed = done.subscribe();
        let mut clock = tokio::time::interval(timing().retransmit);
        loop {
            if *completed.borrow() == 0b110 && self.pending.is_none() {
                return self;
            }
            let now = origin.elapsed();
            // Four fixed-size controls, one <=8 KiB PREPARE, one worker action.
            for _ in 0..4 {
                match self.driver.poll(now).unwrap() {
                    Some(Action::Broadcast(control)) => {
                        send_control(socket, self.index, self.other(), control).await;
                    }
                    Some(Action::Send { to, message }) if to == node(self.other()) => {
                        send_control(socket, self.index, self.other(), message).await;
                    }
                    Some(Action::Send { .. }) => {} // Original primary stays unavailable.
                    Some(Action::PersistPromise(_)) => panic!("healthy fresh append timed out"),
                    None => break,
                }
            }
            if let Some((frames, _)) = &self.flight
                && now >= self.retry_at
            {
                if self.dropped_prepare {
                    send(socket, self.other(), frames).await;
                } else {
                    self.dropped_prepare = true;
                }
                self.retry_at = now + timing().retransmit;
            }
            if self.pending.is_none() {
                self.schedule();
            }
            tokio::select! {
                frames = receive(socket, self.other()) => {
                    self.receive(socket, &frames, origin.elapsed()).await;
                }
                completion = async { self.pending.as_mut().unwrap().await.unwrap() }, if self.pending.is_some() => {
                    self.pending = None;
                    self.complete(socket, completion, origin.elapsed(), &done).await;
                }
                _ = clock.tick() => {}
                _ = completed.changed() => {}
            }
        }
    }

    fn schedule(&mut self) {
        let snapshot = self.driver.normal().unwrap().snapshot();
        if snapshot.committed != snapshot.applied {
            self.pending = Some(self.disk.append_work(|done| Command::Apply {
                scope: snapshot.scope,
                generation: snapshot.journal.generation,
                through: snapshot.committed,
                done,
            }));
        } else if self.index == 1 && self.flight.is_none() && self.next_record < RECORDS {
            let body = record_body(self.next_record);
            let operation = OwnedOperation {
                original_view: snapshot.scope.view,
                op_number: snapshot.accepted.op.0 + 1,
                previous: snapshot.accepted.digest,
                kind: body.kind(),
                body: encode_operation_body(&body, OperationLimits::default()).unwrap(),
            };
            self.validate(operation);
        }
    }

    fn validate(&mut self, operation: OwnedOperation) {
        assert!(self.pending.is_none());
        let validation = self.driver.begin_validation().unwrap();
        self.pending = Some(self.disk.append_work(|done| Command::Validate {
            validation,
            operation,
            done,
        }));
    }

    async fn receive(&mut self, socket: &Socket, frames: &Frames, now: Duration) {
        let message = decode(
            &borrowed(frames),
            binding(self.other()),
            WireLimits::default(),
        )
        .unwrap();
        match message {
            ReplicaMessage::Control(control) => {
                self.driver
                    .receive(node(self.other()), control, now)
                    .unwrap();
                if let Control::PrepareOk { ack, .. } = control
                    && self
                        .flight
                        .as_ref()
                        .is_some_and(|(_, target)| ack.durable == *target)
                    && let Some(release) = self.release_sync.take()
                {
                    let snapshot = self.driver.normal().unwrap().snapshot();
                    assert!(snapshot.committed.op.0 < ack.durable.op.0);
                    assert!(snapshot.journal.durable.0 < ack.durable.op.0);
                    self.backup_ack_before_local_sync = true;
                    release.send(()).unwrap();
                }
                if matches!(control, Control::StartView(_)) {
                    self.ack(socket).await;
                }
            }
            ReplicaMessage::Prepare(batch) => {
                assert_eq!(self.index, 2);
                assert_eq!(batch.scope(), self.driver.scope());
                let operations: Vec<_> = batch.operations().collect();
                assert_eq!(operations.len(), 1);
                let operation = &operations[0];
                let snapshot = self.driver.normal().unwrap().snapshot();
                if operation.prefix().op <= snapshot.accepted.op {
                    if operation.prefix().op == snapshot.accepted.op {
                        assert_eq!(operation.prefix(), snapshot.accepted);
                    }
                    self.duplicates += 1;
                    self.ack(socket).await;
                } else if self.pending.is_none() && snapshot.applied == snapshot.accepted {
                    let canonical = operation.canonical();
                    self.validate(OwnedOperation {
                        original_view: canonical.original_view,
                        op_number: canonical.op_number,
                        previous: canonical.previous_digest,
                        kind: canonical.kind,
                        body: canonical.body.to_vec(),
                    });
                } // One in-flight application slot. The sender retains and retries.
            }
            ReplicaMessage::Ops(_) => {} // Delayed duplicate of completed installation.
            ReplicaMessage::FetchOps(_) => panic!("installation already complete"),
            ReplicaMessage::Flow(_) => panic!("fixture has not opened a credited flow"),
            ReplicaMessage::Recovery(_) => panic!("fixture has no lost-state recovery"),
        }
    }

    async fn complete(
        &mut self,
        socket: &Socket,
        completion: Completion,
        now: Duration,
        done: &watch::Sender<u8>,
    ) {
        match completion {
            Completion::Validated(validated) => {
                let Admission::Write {
                    ticket,
                    first_new: 0,
                } = self
                    .driver
                    .prepare_validated(node(1), validated.validation, &[validated.prepared], now)
                    .unwrap()
                else {
                    panic!("fresh validated operation")
                };
                let mut release = None;
                if self.index == 1 {
                    self.flight = Some((encode(&validated), validated.prepared.prefix()));
                    self.retry_at = now;
                    if !self.backup_ack_before_local_sync {
                        let (sender, receiver) = oneshot::channel();
                        self.release_sync = Some(sender);
                        release = Some(receiver);
                    }
                }
                self.pending = Some(self.disk.append_work(|done| Command::Write {
                    validated,
                    ticket,
                    release,
                    done,
                }));
            }
            Completion::Written { ticket, durable } => {
                self.driver.complete_write(ticket).unwrap();
                self.sync = Some(self.driver.begin_sync().unwrap());
                self.pending = Some(durable);
            }
            Completion::Durable(position) => {
                let sync = self.sync.take().unwrap();
                let snapshot = self.driver.normal().unwrap().snapshot();
                assert_eq!(position.op_number, sync.through().0);
                assert_eq!(position.op_number, snapshot.accepted.op.0);
                assert_eq!(position.digest, snapshot.accepted.digest);
                self.driver.complete_sync(sync, now).unwrap();
                if self.index == 2 {
                    self.ack(socket).await;
                }
            }
            Completion::Applied {
                scope,
                generation,
                through,
            } => {
                let snapshot = self.driver.normal().unwrap().snapshot();
                assert_eq!(snapshot.scope, scope);
                assert_eq!(snapshot.journal.generation, generation);
                self.driver.apply_through(through).unwrap();
                if self.index == 1 {
                    assert_eq!(self.flight.take().unwrap().1, through);
                    self.next_record += 1;
                }
                if through.op.0 == u64::from(RECORDS) + 2 {
                    done.send_modify(|voters| *voters |= 1 << self.index);
                }
            }
        }
    }

    async fn ack(&mut self, socket: &Socket) {
        let ack = self.driver.normal().unwrap().acknowledgment().unwrap();
        if ack.durable.op.0 > 3 && !self.dropped_ack {
            self.dropped_ack = true;
            return;
        }
        send_control(socket, self.index, self.other(), Control::PrepareOk { ack }).await;
    }
}

fn encode(validated: &super::append_worker::Validated) -> Frames {
    let operation = Operation::from_verified(
        validated.operation.canonical(),
        canonical_body_digest(&validated.operation.body),
    );
    let mut metadata = [0; 512];
    let mut payload = [0; 8192];
    let encoded = encode_prepare(
        node(1),
        session(),
        Prepare {
            scope: validated.validation.scope(),
            committed: validated.validation.committed(),
            operations: &[operation],
        },
        &mut metadata,
        &mut payload,
        WireLimits::default(),
    )
    .unwrap();
    [
        Bytes::copy_from_slice(&encoded.header),
        Bytes::copy_from_slice(&metadata[..encoded.metadata_bytes]),
        Bytes::copy_from_slice(&payload[..encoded.payload_bytes]),
    ]
}
