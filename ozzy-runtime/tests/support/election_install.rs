//! Continue elected survivors through OMQ history transfer and real disk publication.
//! The shared completion flag only stops the fixture after both actors activate;
//! it supplies no election, quorum, log selection, or commit evidence.

use super::election_driver::timing;
use super::install_worker::{Command, Completion as DiskCompletion};
use super::*;
use ozzy_replication::driver::{Action, ReplicaDriver};
use tokio::sync::watch;

pub(super) async fn finish(
    sockets: [&Socket; 2],
    replicas: [(ReplicaDriver, Disk); 2],
    acknowledged: Prefix,
    origin: tokio::time::Instant,
) {
    let [(primary, primary_disk), (backup, backup_disk)] = replicas;
    let (done, _) = watch::channel(0u8);
    let primary = Actor::new(1, primary, primary_disk);
    let backup = Actor::new(2, backup, backup_disk);
    let (primary, backup) = tokio::join!(
        primary.run(sockets[0], origin, done.clone()),
        backup.run(sockets[1], origin, done),
    );
    assert!(primary.faults.start && primary.faults.history);
    assert!(backup.faults.ack && backup.faults.duplicate_history > 0);
    for actor in [&primary, &backup] {
        let snapshot = actor.driver.normal().unwrap().snapshot();
        assert_eq!(snapshot.scope.view, 1);
        assert_eq!(snapshot.committed, acknowledged);
        assert_eq!(snapshot.applied, acknowledged);
        assert!(snapshot.ready_for_appends);
    }
    Box::pin(super::election_append::finish(
        sockets,
        [(primary.driver, primary.disk), (backup.driver, backup.disk)],
        acknowledged,
        origin,
    ))
    .await;
}

struct Actor {
    index: u8,
    driver: ReplicaDriver,
    disk: Disk,
    pending: Option<oneshot::Receiver<DiskCompletion>>,
    fetch: Option<FetchOps>,
    fetched: Vec<OwnedOperation>,
    fetch_retry: Duration,
    installed: Option<JournalGeneration>,
    activated: bool,
    faults: Faults,
}

#[derive(Default)]
struct Faults {
    start: bool,
    history: bool,
    ack: bool,
    duplicate_history: usize,
}

impl Actor {
    fn new(index: u8, mut driver: ReplicaDriver, mut disk: Disk) -> Self {
        let pending = if index == 1 {
            let ticket = driver.begin_primary_install(JournalGeneration(11)).unwrap();
            Some(disk.installation(|done| Command::Install {
                ticket,
                operations: None,
                done,
            }))
        } else {
            None
        };
        Self {
            index,
            driver,
            disk,
            pending,
            fetch: None,
            fetched: Vec::with_capacity(8),
            fetch_retry: Duration::ZERO,
            installed: None,
            activated: false,
            faults: Faults::default(),
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
            for _ in 0..4 {
                match self.driver.poll(now).unwrap() {
                    Some(Action::Broadcast(message)) => {
                        if matches!(message, Control::StartView(_)) && !self.faults.start {
                            self.faults.start = true;
                            continue; // Drop first installed-view announcement.
                        }
                        send_control(socket, self.index, self.other(), message).await;
                    }
                    Some(Action::Send { to, message }) if to == node(self.other()) => {
                        send_control(socket, self.index, self.other(), message).await;
                    }
                    Some(Action::Send { .. }) => {} // Voter zero remains crashed.
                    Some(Action::PersistPromise(_)) => panic!(
                        "healthy installation unexpectedly changed view: node={} now={now:?} normal={:?} install={:?} pending={} fetch={:?}",
                        self.index,
                        self.driver.normal().map(NormalReplica::snapshot),
                        self.driver.installation_ticket(),
                        self.pending.is_some(),
                        self.fetch
                    ),
                    None => break,
                }
            }
            if let Some(fetch) = self.fetch
                && now >= self.fetch_retry
            {
                send_fetch(socket, self.index, self.other(), fetch).await;
                self.fetch_retry = now + timing().retransmit;
            }
            if !self.activated
                && self.pending.is_none()
                && let Some(normal) = self.driver.normal()
            {
                let snapshot = normal.snapshot();
                if snapshot.committed == snapshot.accepted {
                    let generation = self.installed.unwrap();
                    self.pending = Some(self.disk.installation(|done| Command::Activate {
                        scope: snapshot.scope,
                        generation,
                        through: snapshot.committed,
                        done,
                    }));
                }
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

    async fn receive(&mut self, socket: &Socket, frames: &Frames, now: Duration) {
        let message = decode(
            &borrowed(frames),
            binding(self.other()),
            WireLimits::default(),
        )
        .unwrap();
        match message {
            ReplicaMessage::Control(control) => {
                if let Some(start) = self
                    .driver
                    .receive(node(self.other()), control, now)
                    .unwrap()
                {
                    assert_eq!(self.index, 2);
                    let ticket = self
                        .driver
                        .begin_backup_install(node(1), start, JournalGeneration(12), |_, _| None)
                        .unwrap();
                    self.fetch = Some(FetchOps {
                        scope: ticket.scope(),
                        request_id: RequestId::new(),
                        source: ticket.source(),
                        predecessor: Prefix::GENESIS,
                        max_operations: 2,
                        max_body_bytes: 8192,
                    });
                    self.fetch_retry = now;
                } else if matches!(control, Control::StartView(_)) && self.driver.normal().is_some()
                {
                    self.ack(socket).await;
                }
            }
            ReplicaMessage::FetchOps(request) => {
                assert_eq!(self.index, 1);
                assert_eq!(request.scope, self.driver.scope());
                // One bounded worker request. Dropped duplicates retry at sender.
                if self.pending.is_none() {
                    self.pending = Some(
                        self.disk
                            .installation(|done| Command::Fetch { request, done }),
                    );
                }
            }
            ReplicaMessage::Ops(ops) => {
                let Some(request) = self.fetch else {
                    self.faults.duplicate_history += 1;
                    return;
                }; // Completed transfer's duplicate.
                if ops.validate_response(request).is_err() {
                    self.faults.duplicate_history += 1;
                    return;
                } // Old request/session cursor.
                for operation in ops.operations() {
                    let canonical = operation.canonical();
                    assert!(self.fetched.len() < 8);
                    self.fetched.push(OwnedOperation {
                        original_view: canonical.original_view,
                        op_number: canonical.op_number,
                        previous: canonical.previous_digest,
                        kind: canonical.kind,
                        body: canonical.body.to_vec(),
                    });
                }
                assert!(self.fetched.iter().map(|op| op.body.len()).sum::<usize>() <= 8192);
                if ops.end() == request.source.accepted {
                    self.fetch = None;
                    let operations = Some(std::mem::take(&mut self.fetched));
                    let ticket = self.driver.installation_ticket().unwrap();
                    assert!(self.pending.is_none());
                    self.pending = Some(self.disk.installation(|done| Command::Install {
                        ticket,
                        operations,
                        done,
                    }));
                } else {
                    self.fetch = Some(FetchOps {
                        request_id: RequestId::new(),
                        predecessor: ops.end(),
                        ..request
                    });
                    self.fetch_retry = now;
                }
            }
            ReplicaMessage::Prepare(_) => panic!("fresh prepares wait for activation"),
            ReplicaMessage::Flow(_) => panic!("fixture has not opened a credited flow"),
            ReplicaMessage::Recovery(_) => panic!("fixture has no lost-state recovery"),
        }
    }

    async fn complete(
        &mut self,
        socket: &Socket,
        completion: DiskCompletion,
        now: Duration,
        done: &watch::Sender<u8>,
    ) {
        match completion {
            DiskCompletion::Installed {
                ticket,
                prepared,
                applied,
            } => {
                for chunk in prepared.chunks(2) {
                    self.driver.validate_install_suffix(chunk).unwrap();
                }
                self.driver
                    .complete_installation(ticket, applied, now)
                    .unwrap();
                self.installed = Some(ticket.generation());
                if self.index == 2 {
                    self.ack(socket).await;
                }
            }
            DiskCompletion::History {
                request,
                operations,
            } => {
                if !self.faults.history {
                    self.faults.history = true;
                    return; // Requester must retransmit after this lost response.
                }
                let operations: Vec<_> = operations
                    .iter()
                    .map(|op| {
                        Operation::from_verified(op.canonical(), canonical_body_digest(&op.body))
                    })
                    .collect();
                let mut metadata = [0; 512];
                let mut payload = [0; 8192];
                let encoded = encode_ops(
                    node(self.index),
                    session(),
                    request,
                    &operations,
                    &mut metadata,
                    &mut payload,
                    WireLimits::default(),
                )
                .unwrap();
                let frames = [
                    Bytes::copy_from_slice(&encoded.header),
                    Bytes::copy_from_slice(&metadata[..encoded.metadata_bytes]),
                    Bytes::copy_from_slice(&payload[..encoded.payload_bytes]),
                ];
                send(socket, self.other(), &frames).await;
                send(socket, self.other(), &frames).await; // Duplicate old transfer cursor.
            }
            DiskCompletion::Activated {
                scope,
                generation,
                through,
            } => {
                assert_eq!(self.driver.scope(), scope);
                assert_eq!(self.installed, Some(generation));
                self.driver.apply_through(through).unwrap();
                self.activated = true;
                done.send_modify(|voters| *voters |= 1 << self.index);
            }
        }
    }

    async fn ack(&mut self, socket: &Socket) {
        if !self.faults.ack {
            self.faults.ack = true;
            return; // A repeated START_VIEW must elicit another durable ACK.
        }
        send_control(
            socket,
            self.index,
            self.other(),
            Control::PrepareOk {
                ack: self.driver.normal().unwrap().acknowledgment().unwrap(),
            },
        )
        .await;
    }
}

async fn send_fetch(socket: &Socket, from: u8, to: u8, request: FetchOps) {
    let mut metadata = [0; 200];
    let encoded = encode_fetch(
        node(from),
        session(),
        request,
        &mut metadata,
        WireLimits::default(),
    )
    .unwrap();
    send(
        socket,
        to,
        &[
            Bytes::copy_from_slice(&encoded.header),
            Bytes::copy_from_slice(&metadata[..encoded.metadata_bytes]),
            Bytes::new(),
        ],
    )
    .await;
}
