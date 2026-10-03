//! Production cores/codecs over OMQ with real journals on dedicated test workers.
//! Sessions are configured by the trusted fixture; HELLO/auth negotiation and
//! Node integration and process restart/rejoin remain separate gates.

use std::thread;
use std::time::Duration;

use bytes::Bytes;
use fanring::mpsc;
use omq_tokio::{Context, Endpoint, Message, Options, Socket, SocketType};
use ozzy_core::state::{CanonicalImages, StateLimits};
use ozzy_journal::operation::{
    Append, AppendBatch, AppendRecord, CanonicalOperation, CreatePartition, OpenProducer,
    OperationBody, OperationKind, OperationLimits, RetentionPolicy, canonical_body_digest,
    decode_operation_body, encode_operation_body,
};
use ozzy_journal_segment::{
    DecodeLimits, GroupDirectory, GroupIdentity, LogPosition, MetadataLimits, OpenGroupJournal,
    SegmentHeader,
};
use ozzy_proto::{
    GroupId, LinkSessionId, MessageId, NodeId, Offset, OperationId, OwnerEpoch, PartitionId,
    PartitionIncarnation, ProducerEpoch, ProducerId, ProducerSequence, RequestId, StoreId,
    VolumeId,
};
use ozzy_replication::wire::{
    Control, FetchOps, Grant, Operation, PeerBinding, Prepare, ReplicaMessage, WireLimits, decode,
    encode_control, encode_fetch, encode_ops, encode_prepare,
};
use ozzy_replication::{
    Admission, Configuration, ConfigurationRecord, ConfiguredVoter, Digest, JournalGeneration,
    LogSource, NormalReplica, PipelineLimits, Prefix, PreparedOperation, PromiseTicket,
    WriteTicket,
};
use tokio::sync::oneshot;

const WAIT: Duration = Duration::from_secs(30);
type Frames = [Bytes; 3];

#[path = "support/election_driver.rs"]
mod election_driver;

#[path = "support/election_install.rs"]
mod election_install;

#[path = "support/install_worker.rs"]
mod install_worker;

#[path = "support/append_worker.rs"]
mod append_worker;

#[path = "support/election_append.rs"]
mod election_append;

fn node(index: u8) -> NodeId {
    NodeId::from_bytes([index + 1; 16])
}
fn configuration() -> Configuration {
    configuration_record().configuration()
}
fn configuration_record() -> ConfigurationRecord {
    ConfigurationRecord::new(
        GroupId::from_bytes([7; 16]),
        1,
        std::array::from_fn(|index| ConfiguredVoter {
            node_id: node(index as u8),
            principal: Digest::from_bytes([index as u8 + 40; 32]),
        }),
    )
    .unwrap()
}
fn session() -> LinkSessionId {
    LinkSessionId::from_bytes([9; 16])
}
fn partition() -> PartitionIncarnation {
    PartitionIncarnation::from_bytes([10; 16])
}
fn binding(peer: u8) -> PeerBinding {
    PeerBinding::new(configuration(), node(peer), session()).unwrap()
}
fn borrowed(frames: &Frames) -> [&[u8]; 3] {
    frames.each_ref().map(AsRef::as_ref)
}

#[derive(Debug)]
struct OwnedOperation {
    original_view: u64,
    op_number: u64,
    previous: Digest,
    kind: OperationKind,
    body: Vec<u8>,
}

impl OwnedOperation {
    fn canonical(&self) -> CanonicalOperation<'_> {
        CanonicalOperation {
            group_id: configuration().scope().group_id,
            configuration_epoch: 1,
            original_view: self.original_view,
            op_number: self.op_number,
            previous_digest: self.previous,
            kind: self.kind,
            body: &self.body,
        }
    }
}

#[derive(Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "four preallocated worker slots move validation plans without boxing"
)]
enum DiskCommand {
    Write {
        operations: Vec<OwnedOperation>,
        written: oneshot::Sender<()>,
        durable: oneshot::Sender<LogPosition>,
        release: Option<oneshot::Receiver<()>>,
    },
    Reopen(oneshot::Sender<(LogPosition, LogPosition, usize)>),
    Promise(PromiseTicket, oneshot::Sender<()>),
    Installation(install_worker::Command),
    Append(append_worker::Command),
}

#[derive(Debug)]
struct Disk {
    sender: Option<mpsc::Sender<DiskCommand>>,
    join: Option<thread::JoinHandle<()>>,
}

impl Disk {
    async fn start(index: u8) -> Self {
        let (sender, mut receiver) = mpsc::channel(4);
        let (ready_tx, ready_rx) = oneshot::channel();
        let join = thread::spawn(move || {
            let temporary = tempfile::Builder::new()
                .prefix(".replica-wire-")
                .tempdir_in(env!("CARGO_MANIFEST_DIR"))
                .unwrap();
            let identity = GroupIdentity {
                group_id: configuration().scope().group_id,
                replica_node_id: node(index),
                volume_id: VolumeId::from_bytes([index + 20; 16]),
                store_id: StoreId::from_bytes([index + 30; 16]),
                store_generation: 1,
            };
            let root = temporary.path().join("group");
            let configuration_bytes = configuration_record().encode();
            let mut journal = format_journal(
                &root,
                identity,
                &configuration_bytes,
                JournalGeneration(u128::from(index) + 1),
            );
            ready_tx.send(()).unwrap();
            let mut recovery = install_worker::State::Empty;
            while let Ok(command) = receiver.recv() {
                match command {
                    DiskCommand::Write {
                        operations,
                        written,
                        durable,
                        release,
                    } => {
                        let operations: Vec<_> =
                            operations.iter().map(OwnedOperation::canonical).collect();
                        let position = journal.append(&operations).unwrap();
                        written.send(()).unwrap();
                        if let Some(release) = release {
                            release.blocking_recv().unwrap();
                        }
                        journal.sync_through(position).unwrap();
                        durable.send(journal.accepted_position().unwrap()).unwrap();
                    }
                    DiskCommand::Reopen(done) => {
                        if let install_worker::State::Active(images) = &recovery {
                            assert!(
                                images.committed().revision()
                                    >= journal.committed_position().unwrap().op_number
                            );
                            assert_eq!(
                                images.speculative().revision(),
                                journal.accepted_position().unwrap().op_number
                            );
                        }
                        recovery = install_worker::State::Empty;
                        drop(journal);
                        journal = GroupDirectory::open_with_configuration(
                            &root,
                            identity,
                            MetadataLimits::default(),
                            &configuration_bytes,
                        )
                        .unwrap()
                        .recover(
                            JournalGeneration(u128::from(index) + 101),
                            DecodeLimits::default(),
                            OperationLimits::default(),
                        )
                        .unwrap();
                        let records = verify_records(&journal);
                        done.send((
                            journal.accepted_position().unwrap(),
                            journal.committed_position().unwrap(),
                            records,
                        ))
                        .unwrap();
                    }
                    DiskCommand::Promise(ticket, done) => {
                        journal = publish_promise(journal, ticket);
                        done.send(()).unwrap();
                    }
                    DiskCommand::Installation(command) => {
                        (journal, recovery) = install_worker::handle(journal, recovery, command);
                    }
                    DiskCommand::Append(command) => {
                        append_worker::handle(&mut journal, &mut recovery, command);
                    }
                }
            }
        });
        ready_rx.await.unwrap();
        Self {
            sender: Some(sender),
            join: Some(join),
        }
    }

    fn submit(
        &mut self,
        operations: &[Operation<'_>],
        release: Option<oneshot::Receiver<()>>,
    ) -> (oneshot::Receiver<()>, oneshot::Receiver<LogPosition>) {
        assert!(operations.len() <= 8);
        assert!(
            operations
                .iter()
                .map(|operation| operation.canonical().body.len())
                .sum::<usize>()
                <= 8192
        );
        let operations = operations
            .iter()
            .map(|operation| {
                let canonical = operation.canonical();
                OwnedOperation {
                    original_view: canonical.original_view,
                    op_number: canonical.op_number,
                    previous: canonical.previous_digest,
                    kind: canonical.kind,
                    body: canonical.body.to_vec(),
                }
            })
            .collect();
        let (written, written_rx) = oneshot::channel();
        let (durable, durable_rx) = oneshot::channel();
        self.sender
            .as_mut()
            .unwrap()
            .try_send(DiskCommand::Write {
                operations,
                written,
                durable,
                release,
            })
            .unwrap();
        (written_rx, durable_rx)
    }

    async fn verify_reopen(&mut self, prefix: Prefix) {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .as_mut()
            .unwrap()
            .try_send(DiskCommand::Reopen(sender))
            .unwrap();
        let (accepted, committed, records) = receiver.await.unwrap();
        assert_eq!(
            accepted,
            LogPosition {
                op_number: prefix.op.0,
                digest: prefix.digest
            }
        );
        assert_eq!(committed, LogPosition::GENESIS); // No per-response commit marker.
        assert_eq!(records, 1);
    }

    fn promise(&mut self, ticket: PromiseTicket) -> oneshot::Receiver<()> {
        let (done, completed) = oneshot::channel();
        self.sender
            .as_mut()
            .unwrap()
            .try_send(DiskCommand::Promise(ticket, done))
            .unwrap();
        completed
    }
}

fn format_journal(
    root: &std::path::Path,
    identity: GroupIdentity,
    configuration: &[u8],
    generation: JournalGeneration,
) -> OpenGroupJournal {
    let header = SegmentHeader::new(identity.group_id, 1, None, Digest::ZERO, 32 * 1024).unwrap();
    GroupDirectory::format_new_with_configuration(root, identity, 1, &header, configuration)
        .unwrap()
        .recover(
            generation,
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap()
}

fn publish_promise(journal: OpenGroupJournal, ticket: PromiseTicket) -> OpenGroupJournal {
    let current = journal.directory().manifest();
    assert_eq!(ticket.scope().group_id, current.identity.group_id);
    assert_eq!(
        ticket.scope().configuration_epoch,
        current.configuration_epoch
    );
    assert_eq!(
        ticket.scope().configuration_digest,
        configuration().scope().configuration_digest
    );
    assert_eq!(
        ticket.generation(),
        journal.writer().durable_position().generation()
    );
    assert_eq!(ticket.log().last_normal_view, current.last_normal_view);
    assert_eq!(
        journal.accepted_position().unwrap(),
        LogPosition {
            op_number: ticket.log().accepted.op.0,
            digest: ticket.log().accepted.digest,
        }
    );
    let mut next = current.clone();
    next.generation += 1;
    next.parent_generation = current.generation;
    next.promised_view = ticket.scope().view;
    next.accepted = journal.accepted_position().unwrap();
    next.committed = LogPosition {
        op_number: ticket.log().committed.op.0,
        digest: ticket.log().committed.digest,
    };
    journal.install_metadata(next).unwrap()
}

impl Drop for Disk {
    fn drop(&mut self) {
        drop(self.sender.take());
        if let Some(join) = self.join.take() {
            let result = join.join();
            if !thread::panicking() {
                result.unwrap();
            }
        }
    }
}

fn verify_records(journal: &OpenGroupJournal) -> usize {
    let mut count = 0;
    journal
        .replay_accepted(|item| {
            let body = decode_operation_body(
                item.operation.kind,
                &item.operation.body,
                OperationLimits::default(),
            )
            .unwrap();
            if matches!(body, OperationBody::Append(_)) {
                assert_eq!(body, record_body(count as u8));
                assert_eq!(item.operation.original_view, u64::from(count != 0));
                count += 1;
            }
            Ok::<_, std::convert::Infallible>(())
        })
        .unwrap();
    count
}

fn bodies() -> Vec<OperationBody<'static>> {
    let producer = ProducerId::from_bytes([11; 16]);
    vec![
        OperationBody::CreatePartition(CreatePartition {
            partition: partition(),
            stream: "events",
            topic: "orders",
            partition_id: PartitionId::ZERO,
            owner_epoch: OwnerEpoch::INITIAL,
            retention: RetentionPolicy::default(),
        }),
        OperationBody::OpenProducer(OpenProducer {
            partition: partition(),
            producer_id: producer,
            expected_epoch: None,
            new_epoch: ProducerEpoch::INITIAL,
            operation_id: OperationId::from_bytes([12; 16]),
        }),
        record_body(0),
    ]
}

fn record_body<'a>(offset: u8) -> OperationBody<'a> {
    let payload: &[u8] = match offset {
        0 => b"{\"sku\":42,\"qty\":3}",
        1 => b"{\"sku\":73,\"qty\":1}",
        2 => b"{\"sku\":19,\"qty\":5}",
        3 => b"{\"sku\":92,\"qty\":2}",
        _ => panic!("bounded fixture workload"),
    };
    OperationBody::Append(Append {
        batches: vec![AppendBatch {
            partition: partition(),
            owner_epoch: OwnerEpoch::INITIAL,
            producer_id: ProducerId::from_bytes([11; 16]),
            producer_epoch: ProducerEpoch::INITIAL,
            first_sequence: ProducerSequence::new(u64::from(offset)),
            first_offset: Offset::new(u64::from(offset)),
            append_timestamp_millis: 123 + u64::from(offset),
            records: vec![AppendRecord {
                encoding: ozzy_proto::data::Encoding::Raw,
                message_id: MessageId::from_bytes([13 + offset; 16]),
                parts: vec![b"order.created".as_slice(), payload].into(),
            }]
            .into(),
        }],
    })
}

fn admit(
    core: &mut NormalReplica,
    images: &mut CanonicalImages,
    operations: &[Operation<'_>],
) -> WriteTicket {
    let bodies: Vec<_> = operations
        .iter()
        .map(|operation| {
            let canonical = operation.canonical();
            (
                canonical.op_number,
                decode_operation_body(canonical.kind, canonical.body, OperationLimits::default())
                    .unwrap(),
            )
        })
        .collect();
    let plans = images.prepare_group(&bodies).unwrap();
    let prepared: Vec<_> = operations
        .iter()
        .map(|operation| {
            PreparedOperation::from_verified(&operation.canonical(), operation.body_digest())
        })
        .collect();
    let Admission::Write { ticket, .. } = core
        .prepare(node(0), configuration().scope(), &prepared)
        .unwrap()
    else {
        panic!("fresh prepare");
    };
    images.install_prepared_group(plans).unwrap();
    ticket
}

fn encoded_prepare(operations: &[Operation<'_>], committed: Prefix) -> Frames {
    let mut metadata = vec![0; 1024];
    let mut payload = vec![0; 8192];
    let encoded = encode_prepare(
        node(0),
        session(),
        Prepare {
            scope: configuration().scope(),
            committed,
            operations,
        },
        &mut metadata,
        &mut payload,
        WireLimits::default(),
    )
    .unwrap();
    metadata.truncate(encoded.metadata_bytes);
    payload.truncate(encoded.payload_bytes);
    [
        Bytes::copy_from_slice(&encoded.header),
        metadata.into(),
        payload.into(),
    ]
}

async fn send(socket: &Socket, peer: u8, frames: &Frames) {
    socket
        .send(Message::multipart([
            Bytes::copy_from_slice(node(peer).as_bytes()),
            frames[0].clone(),
            frames[1].clone(),
            frames[2].clone(),
        ]))
        .await
        .unwrap();
}

async fn receive(socket: &Socket, sender: u8) -> Frames {
    let mut message = socket.recv().await.unwrap();
    assert_eq!(
        message.pop_front().unwrap().as_ref(),
        node(sender).as_bytes()
    );
    message.iter().collect::<Vec<_>>().try_into().unwrap()
}

async fn send_control(socket: &Socket, from: u8, to: u8, control: Control) {
    let mut metadata = [0; 184];
    let encoded = encode_control(node(from), session(), control, &mut metadata).unwrap();
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

async fn send_ack(socket: &Socket, index: u8, core: &NormalReplica) {
    send_control(
        socket,
        index,
        0,
        Control::PrepareOk {
            ack: core.acknowledgment().unwrap(),
            grant: Grant {
                revision: 1,
                record_limit: 8,
                byte_limit: 8192,
            },
        },
    )
    .await;
}

async fn receive_ack(socket: &Socket, from: u8, core: &mut NormalReplica) {
    let frames = receive(socket, from).await;
    let ReplicaMessage::Control(Control::PrepareOk { ack, .. }) =
        decode(&borrowed(&frames), binding(from), WireLimits::default()).unwrap()
    else {
        panic!("ACK");
    };
    core.receive_ack(node(from), ack).unwrap();
}

async fn receive_commit(socket: &Socket, from: u8, core: &mut NormalReplica) {
    let frames = receive(socket, from).await;
    let ReplicaMessage::Control(Control::Commit(commit)) =
        decode(&borrowed(&frames), binding(from), WireLimits::default()).unwrap()
    else {
        panic!("commit");
    };
    core.receive_commit(node(from), commit).unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn omq_quorum_uses_disk_workers_and_lagging_voter_catches_up() {
    tokio::time::timeout(WAIT, normal_path())
        .await
        .expect("normal replication stalled");
}

#[derive(Clone, Copy)]
enum Transport {
    Inproc,
    Tcp,
}

async fn connected_sockets(transport: Transport) -> (Context, Vec<Socket>) {
    let context = Context::current();
    let sockets: Vec<_> = (0..3)
        .map(|index| {
            context.socket(
                SocketType::Peer,
                Options::default()
                    .identity(Bytes::copy_from_slice(node(index).as_bytes()))
                    .router_mandatory(true)
                    .send_hwm(8)
                    .recv_hwm(8)
                    .linger(Duration::ZERO),
            )
        })
        .collect();
    let mut endpoints = Vec::new();
    for socket in &sockets {
        let endpoint = match transport {
            Transport::Inproc => Endpoint::Inproc {
                name: format!("ozzy-vsr-{}", NodeId::new()),
            },
            Transport::Tcp => "tcp://127.0.0.1:0".parse().unwrap(),
        };
        endpoints.push(socket.bind(endpoint).await.unwrap());
    }
    for (index, socket) in sockets.iter().enumerate() {
        for endpoint in endpoints.iter().skip(index + 1) {
            socket.connect(endpoint.clone()).await.unwrap();
        }
    }
    for socket in &sockets {
        socket.wait_connected(2, WAIT).await.unwrap();
    }
    (context, sockets)
}

fn replicas() -> (Vec<NormalReplica>, Vec<CanonicalImages>) {
    let cores = (0..3)
        .map(|index| {
            NormalReplica::bootstrap(
                configuration(),
                node(index),
                JournalGeneration(u128::from(index) + 1),
                PipelineLimits {
                    max_operations: 8,
                    max_body_bytes: 8192,
                },
            )
            .unwrap()
        })
        .collect();
    let images = (0..3)
        .map(|_| CanonicalImages::new(StateLimits::default(), 8, 8))
        .collect();
    (cores, images)
}

fn canonical_operations(bytes: &[(OperationKind, Vec<u8>)]) -> Vec<Operation<'_>> {
    let mut previous = Prefix::GENESIS;
    bytes
        .iter()
        .enumerate()
        .map(|(index, (kind, body))| {
            let operation = Operation::from_verified(
                CanonicalOperation {
                    group_id: configuration().scope().group_id,
                    configuration_epoch: 1,
                    original_view: 0,
                    op_number: index as u64 + 1,
                    previous_digest: previous.digest,
                    kind: *kind,
                    body,
                },
                canonical_body_digest(body),
            );
            previous = operation.prefix();
            operation
        })
        .collect()
}

async fn normal_path() {
    let (_context, sockets) = connected_sockets(Transport::Inproc).await;
    let mut disks = Vec::new();
    for index in 0..3 {
        disks.push(Disk::start(index).await);
    }
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
    let primary_ticket = admit(&mut cores[0], &mut images[0], &operations);
    let frames = encoded_prepare(&operations, Prefix::GENESIS);
    send(&sockets[0], 1, &frames).await; // Before even submitting the primary write.
    let received = receive(&sockets[1], 0).await;
    let ReplicaMessage::Prepare(batch) =
        decode(&borrowed(&received), binding(0), WireLimits::default()).unwrap()
    else {
        panic!("prepare");
    };
    let replica_operations: Vec<_> = batch.operations().collect();
    let ticket = admit(&mut cores[1], &mut images[1], &replica_operations);
    let (written, durable) = disks[1].submit(&replica_operations, None);
    written.await.unwrap();
    cores[1].complete_write(ticket).unwrap();
    let sync = cores[1].begin_sync().unwrap();
    complete_sync(&mut cores[1], sync, durable).await;
    send_ack(&sockets[1], 1, &cores[1]).await;
    receive_ack(&sockets[0], 1, &mut cores[0]).await;
    assert_eq!(cores[0].snapshot().committed, Prefix::GENESIS);

    let (release, gate) = oneshot::channel();
    let (written, durable) = disks[0].submit(&operations, Some(gate));
    written.await.unwrap();
    cores[0].complete_write(primary_ticket).unwrap();
    let sync = cores[0].begin_sync().unwrap();
    // The worker is blocked before sync. OMQ and this current-thread runtime
    // still deliver duplicate evidence, which cannot manufacture a local barrier.
    send_ack(&sockets[1], 1, &cores[1]).await;
    receive_ack(&sockets[0], 1, &mut cores[0]).await;
    assert_eq!(cores[0].snapshot().committed, Prefix::GENESIS);
    release.send(()).unwrap();
    complete_sync(&mut cores[0], sync, durable).await;
    let committed = cores[0].snapshot().committed;
    assert_eq!(committed, operations.last().unwrap().prefix());
    images[0].commit_through(committed.op.0).unwrap();
    cores[0].apply_through(committed).unwrap();
    send_control(
        &sockets[0],
        0,
        1,
        Control::Commit(cores[0].announcement().unwrap()),
    )
    .await;
    receive_commit(&sockets[1], 0, &mut cores[1]).await;
    images[1].commit_through(committed.op.0).unwrap();
    cores[1].apply_through(committed).unwrap();

    assert_eq!(cores[2].snapshot().accepted, Prefix::GENESIS);
    let (received, request) = catchup_exchange(&sockets, &cores[0], &operations).await;
    let ReplicaMessage::Ops(batch) =
        decode(&borrowed(&received), binding(0), WireLimits::default()).unwrap()
    else {
        panic!("catch-up ops");
    };
    batch.validate_response(request).unwrap();
    let operations: Vec<_> = batch.operations().collect();
    let ticket = admit(&mut cores[2], &mut images[2], &operations);
    send_control(
        &sockets[0],
        0,
        2,
        Control::Commit(cores[0].announcement().unwrap()),
    )
    .await;
    receive_commit(&sockets[2], 0, &mut cores[2]).await;
    assert_eq!(cores[2].snapshot().committed, Prefix::GENESIS);
    let (written, durable) = disks[2].submit(&operations, None);
    written.await.unwrap();
    cores[2].complete_write(ticket).unwrap();
    let sync = cores[2].begin_sync().unwrap();
    complete_sync(&mut cores[2], sync, durable).await;
    images[2].commit_through(committed.op.0).unwrap();
    cores[2].apply_through(committed).unwrap();
    send_ack(&sockets[2], 2, &cores[2]).await;
    receive_ack(&sockets[0], 2, &mut cores[0]).await;
    verify_cluster(&cores, &images, &mut disks, committed).await;
}

async fn catchup_exchange(
    sockets: &[Socket],
    primary: &NormalReplica,
    operations: &[Operation<'_>],
) -> (Frames, FetchOps) {
    let snapshot = primary.snapshot();
    let request = FetchOps {
        scope: snapshot.scope,
        request_id: RequestId::from_bytes([42; 16]),
        source: LogSource {
            voter: node(0),
            generation: snapshot.journal.generation,
            accepted: snapshot.accepted,
        },
        predecessor: Prefix::GENESIS,
        max_operations: 8,
        max_body_bytes: 8192,
    };
    let mut metadata = [0; 200];
    let encoded = encode_fetch(
        node(2),
        session(),
        request,
        &mut metadata,
        WireLimits::default(),
    )
    .unwrap();
    send(
        &sockets[2],
        0,
        &[
            Bytes::copy_from_slice(&encoded.header),
            Bytes::copy_from_slice(&metadata),
            Bytes::new(),
        ],
    )
    .await;
    let received = receive(&sockets[0], 2).await;
    let ReplicaMessage::FetchOps(decoded) =
        decode(&borrowed(&received), binding(2), WireLimits::default()).unwrap()
    else {
        panic!("fetch");
    };
    assert_eq!(decoded, request);
    assert_eq!(snapshot.journal.durable, request.source.accepted.op);
    // The fixture keeps this primary quiescent through the response. Real
    // serving must retain the corresponding journal pin across asynchronous I/O.
    let mut metadata = vec![0; 512];
    let mut payload = vec![0; 8192];
    let encoded = encode_ops(
        node(0),
        session(),
        decoded,
        operations,
        &mut metadata,
        &mut payload,
        WireLimits::default(),
    )
    .unwrap();
    metadata.truncate(encoded.metadata_bytes);
    payload.truncate(encoded.payload_bytes);
    send(
        &sockets[0],
        2,
        &[
            Bytes::copy_from_slice(&encoded.header),
            metadata.into(),
            payload.into(),
        ],
    )
    .await;
    (receive(&sockets[2], 0).await, request)
}

async fn complete_sync(
    core: &mut NormalReplica,
    ticket: ozzy_replication::SyncTicket,
    completion: oneshot::Receiver<LogPosition>,
) {
    let position = completion.await.unwrap();
    assert_eq!(position.op_number, ticket.through().0);
    assert_eq!(position.op_number, core.snapshot().accepted.op.0);
    assert_eq!(position.digest, core.snapshot().accepted.digest);
    core.complete_sync(ticket).unwrap();
}

async fn verify_cluster(
    cores: &[NormalReplica],
    images: &[CanonicalImages],
    disks: &mut [Disk],
    committed: Prefix,
) {
    for index in 0..3 {
        assert_eq!(cores[index].snapshot().applied, committed);
        assert_eq!(images[index].committed(), images[0].committed());
        assert_eq!(
            images[index]
                .committed()
                .partition(partition())
                .unwrap()
                .next_offset,
            Offset::new(1)
        );
        disks[index].verify_reopen(committed).await;
    }
}
