//! Timed election actions over OMQ, with real promises on the journal workers.
//! All configured destinations enter the bounded production outbox, including
//! the crashed voter. Installation and activation continue on the same sockets.

use super::*;
use ozzy_replication::SelectedView;
use ozzy_replication::driver::{Action, ReplicaDriver, Timing};
use ozzy_runtime::replica_transport::{EnqueueError, QueueLimits, ReplicaOutbox, SendClass};

pub(super) fn timing() -> Timing {
    Timing {
        heartbeat: Duration::from_millis(10),
        // The fault workload deliberately serializes two real disk barriers.
        // Deterministic driver tests exercise exact timeout boundaries separately.
        primary_timeout: Duration::from_millis(500),
        retransmit: Duration::from_millis(10),
        election_timeout: Duration::from_secs(2),
        max_election_timeout: Duration::from_secs(8),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn timed_omq_election_preserves_quorum_acked_history_after_primary_loss() {
    tokio::time::timeout(WAIT, Box::pin(election(Transport::Inproc)))
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn timed_tcp_failover_resumes_appends_and_reopens_exact_history() {
    tokio::time::timeout(WAIT, Box::pin(election(Transport::Tcp)))
        .await
        .unwrap();
}

async fn election(transport: Transport) {
    let (_context, sockets) = connected_sockets(transport).await;
    let (mut cores, mut images) = replicas();
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
    let mut disks = Vec::new();
    for index in 0..3u8 {
        disks.push(Disk::start(index).await);
    }
    for index in [0, 1] {
        let ticket = admit(&mut cores[index], &mut images[index], &operations);
        let (written, durable) = disks[index].submit(&operations, None);
        written.await.unwrap();
        cores[index].complete_write(ticket).unwrap();
        let sync = cores[index].begin_sync().unwrap();
        complete_sync(&mut cores[index], sync, durable).await;
    }
    send_ack(&sockets[1], 1, &cores[1]).await;
    receive_ack(&sockets[0], 1, &mut cores[0]).await;
    let acknowledged = cores[0].snapshot().committed;
    assert_eq!(acknowledged, operations.last().unwrap().prefix());
    images[0].commit_through(acknowledged.op.0).unwrap();
    cores[0].apply_through(acknowledged).unwrap();
    assert_eq!(cores[1].snapshot().committed, Prefix::GENESIS);
    let mut sockets = sockets.into_iter();
    drop(sockets.next().unwrap());
    drop(cores.remove(0)); // No COMMIT announcement survives this crash.
    drop(disks.remove(0));
    let (stop_tx, stop_rx) = oneshot::channel();
    let (_unused_stop, candidate_stop) = oneshot::channel();
    let candidate_socket = sockets.next().unwrap();
    let backup_socket = sockets.next().unwrap();
    let origin = tokio::time::Instant::now();
    let candidate = run(
        1,
        &candidate_socket,
        cores.remove(0),
        disks.remove(0),
        candidate_stop,
        origin,
    );
    let backup = run(
        2,
        &backup_socket,
        cores.remove(0),
        disks.remove(0),
        stop_rx,
        origin,
    );
    let (selected, other) = tokio::join!(
        async {
            let selected = candidate.await;
            // Exercise retransmission after the winning phase returns, before
            // its peer sees shutdown. Socket lifetime must span that handoff.
            tokio::time::sleep(timing().retransmit * 2).await;
            stop_tx.send(()).unwrap();
            selected
        },
        backup
    );
    assert!(other.2.is_none());
    let chosen = selected.2.unwrap();
    assert_eq!(chosen.source().accepted, acknowledged);
    assert_eq!(chosen.scope().view, 1);
    assert_eq!(chosen.voter_mask(), 0b110);
    assert_eq!(chosen.committed(), Prefix::GENESIS);
    Box::pin(super::election_install::finish(
        [&candidate_socket, &backup_socket],
        [(selected.0, selected.1), (other.0, other.1)],
        acknowledged,
        origin,
    ))
    .await;
}

async fn run(
    index: u8,
    socket: &Socket,
    normal: NormalReplica,
    mut disk: Disk,
    mut stop: oneshot::Receiver<()>,
    origin: tokio::time::Instant,
) -> (ReplicaDriver, Disk, Option<SelectedView>) {
    let mut driver = ReplicaDriver::from_normal(normal, Duration::ZERO, timing()).unwrap();
    let limits = QueueLimits {
        messages: 8,
        bytes: 8192,
        message_bytes: 1024,
    };
    let mut outbox = ReplicaOutbox::new(configuration(), node(index), limits, limits).unwrap();
    let identity = socket.identity_routing().unwrap();
    let mut clock = tokio::time::interval(timing().retransmit);
    let mut pending: Option<(PromiseTicket, oneshot::Receiver<()>)> = None;
    let other = if index == 1 { 2 } else { 1 };
    loop {
        // Four controls are at most 4 * 184 metadata bytes; no unbounded drain.
        for _ in 0..4 {
            match driver.poll(origin.elapsed()).unwrap() {
                Some(Action::PersistPromise(ticket)) => {
                    assert!(pending.is_none());
                    pending = Some((ticket, disk.promise(ticket)));
                }
                Some(Action::Broadcast(message)) => {
                    for &to in configuration().voters() {
                        if to != node(index) {
                            enqueue_control(&mut outbox, index, to, message);
                        }
                    }
                }
                Some(Action::Send { to, message }) => {
                    enqueue_control(&mut outbox, index, to, message);
                }
                None => break,
            }
        }
        if let Ok(selected) = driver.select(|_, _| None) {
            assert!(pending.is_none());
            return (driver, disk, Some(selected));
        }
        tokio::select! {
            result = outbox.flush_ready(&identity), if outbox.has_pending() => {
                // Unroutable transmissions do not change membership or evidence.
                // The driver regenerates controls on its bounded retry schedule.
                result.unwrap();
            }
            frames = receive(socket, other) => {
                let ReplicaMessage::Control(control) = decode(
                    &borrowed(&frames), binding(other), WireLimits::default(),
                ).unwrap() else { panic!("election control") };
                driver.receive(node(other), control, origin.elapsed()).unwrap();
            }
            () = async {
                (&mut pending.as_mut().unwrap().1).await.unwrap();
            }, if pending.is_some() => {
                driver.complete_promise(pending.take().unwrap().0).unwrap();
            }
            _ = clock.tick() => {}
            _ = &mut stop => {
                if let Some((ticket, completion)) = pending.take() {
                    completion.await.unwrap();
                    driver.complete_promise(ticket).unwrap();
                }
                return (driver, disk, None);
            }
        }
    }
}

fn enqueue_control(outbox: &mut ReplicaOutbox, from: u8, to: NodeId, control: Control) {
    let mut metadata = [0; 184];
    let encoded = encode_control(node(from), session(), control, &mut metadata).unwrap();
    let message = Message::multipart([
        Bytes::copy_from_slice(&encoded.header),
        Bytes::copy_from_slice(&metadata[..encoded.metadata_bytes]),
        Bytes::new(),
    ]);
    match outbox.try_enqueue(to, SendClass::Control, message) {
        Ok(()) | Err((EnqueueError::Full, _)) => {} // Driver retains/retransmits evidence.
        Err(error) => panic!("invalid election transmission: {error:?}"),
    }
}
