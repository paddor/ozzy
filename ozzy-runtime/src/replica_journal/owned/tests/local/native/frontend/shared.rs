use super::*;
use crate::{
    frontend::{ReceiveError, Rejection, TopicCatalog, WatchError},
    replicated::{
        BrokerAddress, BrokerLinkError, BrokerLinks, BrokerLinksConfig, SdkClock, WriterRuntime,
    },
};
use ozzy_proto::{TopicId, directory};
use std::{collections::BTreeSet, future::Future, pin::pin, task::Poll};

mod appends;
mod metadata;
mod multiple;
mod opening;
mod readers;
mod routes;
mod topic;

/// Waits are bounded by time. A count of turns is a time limit that shrinks
/// when SDK and transport threads compete with this thread for CPUs.
const SETTLE: Duration = Duration::from_secs(8);

#[derive(Default)]
struct ReaderTraffic {
    /// Answer this many SUBSCRIBE requests with a retryable admission refusal.
    refuse_subscribes: usize,
    hold_records: bool,
    unroutable_records: usize,
    records: std::collections::VecDeque<Message>,
    drop_subscribed: bool,
    drop_unsubscribed: usize,
    unsubscribed: usize,
    requests: Vec<(Opcode, ozzy_proto::reader::Subscription)>,
    block_failures: bool,
    failures: BTreeSet<RequestId>,
}

struct Harness {
    local: NodeId,
    service: Service,
    input: TestInput,
    waiting: Option<DataInput>,
    session: Option<LinkSessionId>,
    server: Socket,
    data_server: Socket,
    publisher: Socket,
    wire: DataLimits,
    readers: ReaderTraffic,
    publish: bool,
    publications: std::collections::VecDeque<Message>,
    compressed_appends: usize,
    controller: Controller,
    actors: PartitionActors,
    group: GroupId,
    groups: Vec<GroupId>,
    held_io: BTreeSet<ozzy_io::simulation::JobId>,
    watch: Option<RequestId>,
    drop_snapshots: bool,
    metadata: metadata::Traffic,
    snapshots: usize,
    drop_opened: Option<ProducerId>,
    defer_openings: bool,
    openings: Vec<(ProducerId, OperationId, RequestId)>,
    drop_confirmation: Option<ProducerId>,
    hold_confirmation: Option<ProducerId>,
    held_confirmation: Option<Message>,
    drop_append: Option<(ProducerId, u64)>,
    refuse_append: Option<(ProducerId, u64)>,
    capacity_replies: BTreeSet<RequestId>,
    appends: Vec<(
        ProducerId,
        u64,
        Vec<ozzy_proto::MessageId>,
        RequestId,
        LinkSessionId,
    )>,
}

impl Harness {
    fn dispatch_ready(&mut self) {
        while let Some(received) = self
            .waiting
            .take()
            .or_else(|| self.input.try_recv().unwrap())
        {
            let decoded = packet(&received.message);
            let group = match decoded.envelope.opcode {
                Opcode::OpenProducer => {
                    producer::decode_open(decoded, limits().envelope)
                        .unwrap()
                        .authority
                        .group_id
                }
                Opcode::Append => {
                    append::route(decoded, limits().envelope)
                        .unwrap()
                        .authority
                        .group_id
                }
                Opcode::Subscribe | Opcode::Ack | Opcode::Unsubscribe => {
                    let ozzy_proto::reader::Source::Group { authority, .. } =
                        ozzy_proto::reader::route(decoded, self.wire.envelope).unwrap()
                    else {
                        panic!("shared reader requires group source")
                    };
                    authority.group_id
                }
                opcode => panic!("unexpected actor command {opcode:?}"),
            };
            let delivered =
                if self.defer_openings && decoded.envelope.opcode == Opcode::OpenProducer {
                    // The production shard answers this way until its
                    // partition finished local initialization.
                    self.actors
                        .defer_client(group, &received.message, Duration::ZERO)
                } else {
                    self.actors
                        .receive_client(group, &received.message, Duration::ZERO)
                };
            match delivered.unwrap() {
                NativeReceive::Busy => {
                    self.waiting = Some(received);
                    break;
                }
                result => assert_eq!(result, NativeReceive::Accepted),
            }
            drop(received);
        }
    }

    fn pump(&mut self, settle: bool) {
        if let Some(peer) = &mut self.metadata.peer {
            peer.pump();
        }
        self.receive_ready();
        self.dispatch_ready();
        let _ = self.actors.poll_progress(
            &mut Context::from_waker(Waker::noop()),
            Duration::ZERO,
            |_, message| {
                if message
                    .part_slice(0)
                    .is_some_and(|prefix| prefix.len() == 32)
                {
                    if self.publish {
                        match self.publisher.try_send(message) {
                            Ok(()) | Err(TrySendError::Full(_)) => {}
                            Err(error) => panic!("reader publication: {error:?}"),
                        }
                    } else {
                        assert!(self.publications.len() < 16);
                        self.publications.push_back(message);
                    }
                    return Ok(());
                }
                let decoded = packet(&message);
                if decoded.envelope.opcode == Opcode::Nack {
                    let nack = ozzy_proto::nack::decode(decoded, limits().envelope).unwrap();
                    if nack.code == 16 {
                        self.readers
                            .failures
                            .insert(decoded.envelope.request_id.unwrap());
                        if self.readers.block_failures {
                            return Err(TrySendError::Full(message));
                        }
                    }
                    if nack.code == 10 && nack.retry == ozzy_proto::nack::RetryClass::AfterBackoff {
                        self.capacity_replies
                            .insert(decoded.envelope.request_id.unwrap());
                    }
                }
                self.service
                    .try_reply(
                        if decoded.envelope.opcode == Opcode::Records {
                            Class::Data
                        } else {
                            Class::Control
                        },
                        message,
                    )
                    .map_err(|(error, message)| match error {
                        ReplyError::Full => TrySendError::Full(message),
                        error => panic!("native reply: {error:?}"),
                    })
            },
        );
        if settle {
            for (id, _) in self.controller.jobs() {
                if self.held_io.contains(&id) {
                    continue;
                }
                self.controller.execute(id, Effect::Normal).unwrap();
                self.controller.deliver(id).unwrap();
            }
        }
        self.flush_replies();
    }

    fn flush_replies(&mut self) {
        self.service.poll_watch().unwrap();
        self.service
            .flush(|message| {
                let decoded = packet(&message);
                if decoded.envelope.opcode == Opcode::StateSnapshot
                    && decoded.metadata.first() == Some(&0)
                    && self.metadata.hold
                {
                    // Keep one late response; subsequent lost responses do
                    // not accumulate backing outside the fixture's bound.
                    if self.metadata.held.is_empty() {
                        self.metadata.held.push(message);
                    }
                    return Ok(());
                }
                if decoded.envelope.opcode == Opcode::Subscribed && self.readers.drop_subscribed {
                    return Ok(());
                }
                if decoded.envelope.opcode == Opcode::Unsubscribed {
                    self.readers.unsubscribed += 1;
                    if self.readers.drop_unsubscribed != 0 {
                        self.readers.drop_unsubscribed -= 1;
                        return Ok(());
                    }
                }
                if decoded.envelope.opcode == Opcode::Records && self.readers.unroutable_records > 0
                {
                    self.readers.unroutable_records -= 1;
                    return Err(TrySendError::Error(omq_tokio::Error::Unroutable));
                }
                if decoded.envelope.opcode == Opcode::Records && self.readers.hold_records {
                    assert!(self.readers.records.len() < 16);
                    self.readers.records.push_back(message);
                    return Ok(());
                }
                if decoded.envelope.opcode == Opcode::ProducerOpened
                    && self.drop_opened.is_some_and(|writer| {
                        ozzy_proto::producer::decode_opened(decoded, limits().envelope)
                            .unwrap()
                            .producer
                            == writer
                    })
                {
                    self.drop_opened = None;
                    return Ok(());
                }
                if decoded.envelope.opcode == Opcode::Appended
                    && self.drop_confirmation.is_some_and(|writer| {
                        append::stream::decode_confirmed(decoded, limits().envelope)
                            .unwrap()
                            .key
                            .producer_id
                            == writer
                    })
                {
                    self.drop_confirmation = None;
                    return Ok(());
                }
                if decoded.envelope.opcode == Opcode::Appended
                    && self.hold_confirmation.is_some_and(|writer| {
                        append::stream::decode_confirmed(decoded, limits().envelope)
                            .unwrap()
                            .key
                            .producer_id
                            == writer
                    })
                {
                    self.hold_confirmation = None;
                    self.held_confirmation = Some(message);
                    return Ok(());
                }
                if decoded.envelope.opcode == Opcode::StateSnapshot
                    && decoded.metadata.first() == Some(&1)
                {
                    self.snapshots += 1;
                    if self.drop_snapshots {
                        return Ok(());
                    }
                }
                (if decoded.envelope.opcode == Opcode::Records {
                    &self.data_server
                } else {
                    &self.server
                })
                .try_send(message)
            })
            .unwrap();
    }

    fn refuse_for_credit(&mut self, request: Envelope) {
        let mut metadata = Vec::with_capacity(256);
        let header = ozzy_proto::nack::encode(
            Envelope {
                opcode: Opcode::Nack,
                response: true,
                sender: self.local,
                ..request
            },
            ozzy_proto::nack::Nack {
                code: 10,
                retry: ozzy_proto::nack::RetryClass::AfterBackoff,
                detail: &[],
                diagnostic: "",
            },
            &mut metadata,
            limits().envelope,
        )
        .unwrap();
        self.server
            .try_send(crate::native_frames::message(
                request.sender.as_bytes(),
                header,
                &metadata,
                Bytes::new(),
            ))
            .unwrap();
    }

    #[expect(
        clippy::too_many_lines,
        reason = "fixture decodes and replies to each native command in one turn"
    )]
    fn receive_ready(&mut self) {
        for _ in 0..16 {
            let message = match self
                .server
                .try_recv()
                .or_else(|_| self.data_server.try_recv())
            {
                Ok(message) => message,
                Err(omq_tokio::Error::WouldBlock) => break,
                Err(error) => panic!("broker receive: {error:?}"),
            };
            let decoded = packet(&message);
            let stale_session = self
                .service
                .links()
                .get(decoded.envelope.sender)
                .is_none_or(|link| decoded.envelope.session != Some(link.binding.session));
            match decoded.envelope.opcode {
                Opcode::Subscribe => self.readers.requests.push((
                    Opcode::Subscribe,
                    ozzy_proto::reader::decode_subscribe(decoded, self.wire.envelope)
                        .unwrap()
                        .subscription,
                )),
                Opcode::Unsubscribe => self.readers.requests.push((
                    Opcode::Unsubscribe,
                    ozzy_proto::reader::decode_unsubscribe(decoded, self.wire.envelope)
                        .unwrap()
                        .subscription,
                )),
                _ => {}
            }
            if decoded.envelope.opcode == Opcode::Subscribe && self.readers.refuse_subscribes != 0 {
                self.readers.refuse_subscribes -= 1;
                self.refuse_for_credit(decoded.envelope);
                continue;
            }
            if decoded.envelope.opcode == Opcode::OpenProducer {
                let open = ozzy_proto::producer::decode_open(decoded, limits().envelope).unwrap();
                self.openings.push((
                    open.producer,
                    open.operation,
                    decoded.envelope.request_id.unwrap(),
                ));
            }
            if decoded.envelope.opcode == Opcode::Append {
                let mut scratch = Vec::new();
                let append =
                    append::decode_append_with_scratch(decoded, self.wire, &mut scratch).unwrap();
                self.compressed_appends +=
                    usize::from(append.payload_encoding == append::PayloadEncoding::Lz4);
                self.appends.push((
                    append.key.producer_id,
                    append.key.first_sequence,
                    append
                        .records
                        .iter()
                        .map(|record| record.message_id)
                        .collect(),
                    decoded.envelope.request_id.unwrap(),
                    decoded.envelope.session.unwrap(),
                ));
                if self.drop_append == Some((append.key.producer_id, append.key.first_sequence)) {
                    self.drop_append = None;
                    continue;
                }
                if self.refuse_append == Some((append.key.producer_id, append.key.first_sequence)) {
                    self.refuse_append = None;
                    self.refuse_for_credit(decoded.envelope);
                    continue;
                }
            }
            if decoded.envelope.opcode == Opcode::StateSnapshotRequest
                && decoded.metadata.first() == Some(&1)
                && !stale_session
            {
                self.watch = Some(
                    directory::decode_request(
                        decoded,
                        limits().envelope,
                        directory::Limits::default(),
                    )
                    .unwrap()
                    .watch,
                );
            }
            if let Err(error) = self.service.receive(message, 4096) {
                assert!(
                    matches!(
                        error,
                        ReceiveError::Dispatch(Rejection::Data(
                            crate::frontend::DataPressure::Full { .. }
                        ))
                    ) || (stale_session
                        && matches!(error, ReceiveError::Watch(WatchError::Session))),
                    "unexpected frontend refusal: {error:?}"
                );
            }
            if let Some(link) = self.service.links().get(link(70, 80).binding.peer)
                && self
                    .session
                    .as_ref()
                    .is_none_or(|session| *session != link.binding.session)
            {
                self.install_client(link);
            }
        }
    }

    fn install_client(&mut self, link: Link) {
        self.session = Some(link.binding.session);
    }

    async fn shutdown(mut self) {
        self.publications.clear();
        self.readers.records.clear();
        let mut closing = pin!(self.actors.shutdown());
        let deadline = std::time::Instant::now() + SETTLE;
        loop {
            if let Poll::Ready(result) = futures::poll!(closing.as_mut()) {
                result.unwrap();
                break;
            }
            for (id, _) in self.controller.jobs() {
                self.controller.execute(id, Effect::Normal).unwrap();
                self.controller.deliver(id).unwrap();
            }
            assert!(
                std::time::Instant::now() < deadline,
                "shared SDK shard did not drain"
            );
            // Socket and one-shot polling also spend the caller's Tokio budget.
            // A cooperative yield is not evidence of a missing file job.
            tokio::task::yield_now().await;
        }
        self.server.close().await.unwrap();
        self.data_server.close().await.unwrap();
        self.publisher.close().await.unwrap();
        if let Some(peer) = self.metadata.peer {
            peer.server.close().await.unwrap();
        }
    }

    async fn drive<F: Future>(&mut self, future: F, settle: bool) -> F::Output {
        let mut future = pin!(future);
        let deadline = std::time::Instant::now() + SETTLE;
        loop {
            self.pump(settle);
            if let Poll::Ready(value) = futures::poll!(future.as_mut()) {
                return value;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "shared broker control did not settle: {}",
                std::any::type_name::<F>()
            );
            tokio::task::yield_now().await;
        }
    }

    /// Pump until `done` holds for state that SDK threads change.
    async fn until(&mut self, settle: bool, what: &str, mut done: impl FnMut(&Self) -> bool) {
        let deadline = std::time::Instant::now() + SETTLE;
        loop {
            self.pump(settle);
            if done(self) {
                return;
            }
            assert!(std::time::Instant::now() < deadline, "{what}");
            tokio::task::yield_now().await;
        }
    }

    /// Let retry backoff expire on the manual clock while driving.
    async fn drive_advancing<F: Future>(
        &mut self,
        clock: &SdkClock,
        future: F,
        settle: bool,
    ) -> F::Output {
        let mut future = pin!(future);
        for _ in 0..10000 {
            self.pump(settle);
            if let Poll::Ready(value) = futures::poll!(future.as_mut()) {
                return value;
            }
            clock
                .advance(clock.now().saturating_add(Duration::from_millis(1)))
                .unwrap();
            tokio::task::yield_now().await;
        }
        panic!("shared broker control did not settle");
    }
}

fn config(local: NodeId, addresses: Vec<BrokerAddress>, clock: SdkClock) -> BrokerLinksConfig {
    let mut parameters = link(70, 80).remote;
    parameters.capabilities |= handshake::OWNER_ROUTING;
    BrokerLinksConfig {
        reader: None,
        local,
        brokers: addresses,
        parameters,
        requests: 12,
        control_bytes: 1024 * 1024,
        routing_bytes: 1024 * 1024,
        append: Some(crate::replicated::AppendLinkLimits {
            writers: 4,
            requests: 8,
            records: 16,
            bytes: 16 * 1024 * 1024,
        }),
        maximum_partitions: 16,
        request_timeout: Duration::from_secs(5),
        retry_interval: Duration::from_millis(100),
        clock,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn shared_broker_links_open_independent_writers_without_waiting_for_unavailable_brokers() {
    tokio::time::timeout(Duration::from_secs(10), Box::pin(scenario()))
        .await
        .unwrap();
}

async fn setup_shared() -> (
    Harness,
    BrokerLinks,
    SdkClock,
    ozzy_proto::nack::AuthorityHint,
) {
    setup_shared_with_writers(&[12, 30]).await
}

async fn setup_shared_with_writers(
    writers: &[u8],
) -> (
    Harness,
    BrokerLinks,
    SdkClock,
    ozzy_proto::nack::AuthorityHint,
) {
    setup_shared_partitions(writers, 1).await
}

#[allow(clippy::too_many_lines)]
async fn setup_shared_partitions(
    writers: &[u8],
    count: usize,
) -> (
    Harness,
    BrokerLinks,
    SdkClock,
    ozzy_proto::nack::AuthorityHint,
) {
    setup_shared_profile(writers, count, None).await
}

async fn setup_shared_profile(
    writers: &[u8],
    count: usize,
    reader_window: Option<(u64, u64, usize)>,
) -> (
    Harness,
    BrokerLinks,
    SdkClock,
    ozzy_proto::nack::AuthorityHint,
) {
    Box::pin(setup_shared_directory_profile(
        writers,
        count,
        reader_window,
        false,
    ))
    .await
}

#[allow(clippy::too_many_lines)]
async fn setup_shared_directory_profile(
    writers: &[u8],
    count: usize,
    reader_window: Option<(u64, u64, usize)>,
    alternate_metadata: bool,
) -> (
    Harness,
    BrokerLinks,
    SdkClock,
    ozzy_proto::nack::AuthorityHint,
) {
    assert!(matches!(count, 1 | 2));
    let runtime = WriterRuntime::new().unwrap();
    let mut wire = limits();
    if reader_window.is_some() {
        wire.envelope.max_payload_bytes = 4096;
    }
    let (mut controller, io) = setup();
    let capacity = multiple::budgets(count);
    let memory = |budget: crate::dispatch::Budget| {
        crate::memory::Domain::new(None, budget.bytes)
            .unwrap()
            .owner(crate::memory::Limits {
                bytes: budget.bytes,
                buffers: 64 * count,
                cache_bytes: budget.bytes,
            })
            .unwrap()
    };
    let data = memory(capacity.data);
    let control = memory(capacity.control);
    let mut native = Vec::new();
    let mut partitions = Vec::new();
    let mut groups = Vec::new();
    for number in 0..count {
        let incarnation = multiple::incarnation(number);
        let mut actor = actor_with_partition_memory(
            &mut controller,
            io.clone(),
            number as u8 + 1,
            writers,
            incarnation,
            PartitionId::new(number as u32),
            Some(&data),
        );
        groups.push(actor.group());
        native.push(super::super::intake_access_profile(
            &mut actor,
            NativeAccess::TrustedClients {
                clients: 2,
                writers: 2,
            },
            incarnation,
            None,
            wire,
        ));
        partitions.push(PartitionActor::Local(Box::new(actor)));
    }
    let authority = partitions[0].authority_hint();
    let group = groups[0];
    let (mut service, input) = multiple::service(authority.primary, &groups, &data, &control, wire);
    let server = runtime.context().socket(
        SocketType::Peer,
        Options::default()
            .identity(Bytes::copy_from_slice(authority.primary.as_bytes()))
            .router_mandatory(true)
            .send_hwm(16)
            .recv_hwm(16)
            .max_message_size(8192),
    );
    let data_server = runtime.context().socket(
        SocketType::Peer,
        Options::default()
            .identity(Bytes::copy_from_slice(authority.primary.as_bytes()))
            .router_mandatory(true)
            .send_hwm(16)
            .recv_hwm(16)
            .max_message_size(8192),
    );
    let endpoint = server
        .bind(
            format!("inproc://shared-native-{}", RequestId::new())
                .parse()
                .unwrap(),
        )
        .await
        .unwrap();
    let data_endpoint = data_server
        .bind(
            format!("inproc://shared-native-data-{}", RequestId::new())
                .parse()
                .unwrap(),
        )
        .await
        .unwrap();
    let publisher = runtime
        .context()
        .socket(SocketType::Pub, Options::default().send_hwm(4));
    let publications = publisher
        .bind(
            format!("inproc://shared-readers-{}", RequestId::new())
                .parse()
                .unwrap(),
        )
        .await
        .unwrap();
    let catalog = multiple::catalog(authority, &groups, &endpoint, &publications);
    service
        .install_catalog(if alternate_metadata {
            metadata::paginate(&catalog, count)
        } else {
            catalog
        })
        .unwrap();
    let mut actors = PartitionActors::new(partitions, count, 1).unwrap();
    for native in native {
        actors.install_native(native, service.links()).unwrap();
    }
    if reader_window.is_some() {
        for (number, &group) in groups.iter().enumerate() {
            actors
                .install_readers(
                    group,
                    crate::replica_actor::SharedReaderConfig {
                        partition: multiple::incarnation(number),
                        limits: wire,
                        subscriptions: 2,
                    },
                    service.links(),
                )
                .unwrap();
        }
    }
    multiple::watches(&mut service, &actors, &groups);
    // These addresses are never bound. Only the primary can complete a
    // transport or Ozzy handshake in this inproc harness.
    let mut addresses = vec![BrokerAddress {
        node: authority.primary,
        endpoint,
        data_endpoint,
    }];
    for index in 0..2 {
        addresses.push(BrokerAddress {
            node: NodeId::from_bytes([index as u8 + 2; 16]),
            data_endpoint: format!(
                "inproc://shared-unavailable-data-{}-{index}",
                RequestId::new()
            )
            .parse()
            .unwrap(),
            endpoint: format!("inproc://shared-unavailable-{}-{index}", RequestId::new())
                .parse()
                .unwrap(),
        });
    }
    let metadata_peer = if alternate_metadata {
        let address = &addresses[1];
        let (mut service, input) = multiple::service(address.node, &groups, &data, &control, wire);
        service
            .install_catalog(metadata::paginate(
                &multiple::catalog(authority, &groups, &addresses[0].endpoint, &publications),
                count,
            ))
            .unwrap();
        let server = runtime.context().socket(
            SocketType::Peer,
            Options::default()
                .identity(Bytes::copy_from_slice(address.node.as_bytes()))
                .router_mandatory(true)
                .send_hwm(16)
                .recv_hwm(16)
                .max_message_size(8192),
        );
        let peer = Box::new(metadata::Peer::new(service, input, server));
        peer.server.bind(address.endpoint.clone()).await.unwrap();
        Some(peer)
    } else {
        None
    };
    let clock = SdkClock::manual();
    let mut options = config(link(70, 80).binding.peer, addresses, clock.clone());
    // Four logical owners must fit even when each allows two caller handles.
    // Test the owner bound, independently of the queue representation's size.
    let writer = crate::replicated::SharedTopicWriterConfig {
        limits: limits(),
        compress_payloads: true,
        batch_target_bytes: 1024,
        max_producers: 2,
        inflight_appends: 1,
    };
    let reservation = writer
        .link_reservation(options.parameters.receive.envelope.max_metadata_bytes)
        .unwrap();
    let append = options.append.as_mut().unwrap();
    append.bytes =
        append.writers * reservation.idle_bytes + (append.requests + 1) * reservation.request_bytes;
    if let Some((_records, _bytes, subscriptions)) = reader_window {
        options.parameters =
            handshake::Parameters::streaming(wire, handshake::PRODUCER | handshake::CONSUMER)
                .unwrap();
        options.parameters.capabilities |= handshake::OWNER_ROUTING | handshake::OWNER_READ;
        options.reader = Some(crate::replicated::ReaderLinkLimits {
            subscriptions,
            bytes: 4 * 1024 * 1024,
            queue_messages: 1,
        });
    }
    let links = BrokerLinks::connect_with_ids(
        &runtime,
        options,
        LinkIds::deterministic(NonZeroU64::new(70).unwrap()),
        LinkIds::deterministic(NonZeroU64::new(71).unwrap()),
    )
    .await
    .unwrap();
    assert_eq!(
        links.socket_count(),
        if reader_window.is_some() { 5 } else { 2 }
    );
    let harness = Harness {
        local: authority.primary,
        service,
        input,
        waiting: None,
        session: None,
        server,
        data_server,
        publisher,
        wire,
        readers: ReaderTraffic::default(),
        publish: true,
        publications: std::collections::VecDeque::new(),
        compressed_appends: 0,
        controller,
        actors,
        group,
        groups,
        held_io: BTreeSet::new(),
        watch: None,
        drop_snapshots: false,
        metadata: metadata::Traffic {
            peer: metadata_peer,
            ..metadata::Traffic::default()
        },
        snapshots: 0,
        drop_opened: None,
        defer_openings: false,
        openings: Vec::new(),
        drop_confirmation: None,
        hold_confirmation: None,
        held_confirmation: None,
        drop_append: None,
        refuse_append: None,
        capacity_replies: BTreeSet::new(),
        appends: Vec::new(),
    };
    (harness, links, clock, authority)
}

fn packet(message: &Message) -> Packet<'_> {
    let frames = std::array::from_fn::<_, 3, _>(|index| message.part_slice(index + 1).unwrap());
    decode_packet(&frames, EnvelopeLimits::default()).unwrap()
}

async fn scenario() {
    let (mut harness, links, clock, authority) = setup_shared().await;
    let group = harness.group;
    let topic = harness.drive(links.topic("orders"), false).await.unwrap();
    assert_eq!(topic.partition_count(), 1);
    assert_eq!(topic.partition(0).unwrap().group, group);
    assert_eq!(links.session(NodeId::from_bytes([2; 16])), None);
    assert_eq!(links.session(NodeId::from_bytes([3; 16])), None);
    assert!(matches!(
        harness.drive(links.topic("missing"), false).await,
        Err(BrokerLinkError::Rejected {
            code: 18,
            retry: ozzy_proto::nack::RetryClass::Permanent,
            ..
        })
    ));
    let session = links.session(authority.primary).unwrap();
    let open = |writer, operation, mode, expected_epoch| Open {
        authority: authority.authority,
        partition: partition(),
        producer: ProducerId::from_bytes([writer; 16]),
        mode,
        expected_epoch,
        operation: OperationId::from_bytes([operation; 16]),
    };
    let first = links.open_producer(
        authority.primary,
        open(40, 41, Mode::Resume, None),
        Policy::LocalDurable,
    );
    for expected in [None, Some(u64::MAX)] {
        assert!(matches!(
            links
                .open_producer(
                    authority.primary,
                    open(40, 90, Mode::Fence, expected),
                    Policy::LocalDurable
                )
                .await,
            Err(BrokerLinkError::Configuration)
        ));
    }
    let second = links.open_producer(
        authority.primary,
        open(30, 31, Mode::Resume, None),
        Policy::LocalDurable,
    );
    let mut opening = pin!(futures::future::join(first, second));
    assert!(futures::poll!(opening.as_mut()).is_pending());
    for _ in 0..100 {
        harness.pump(false);
        assert!(
            futures::poll!(opening.as_mut()).is_pending(),
            "transport receipt confirmed unfinished disk work"
        );
        tokio::task::yield_now().await;
    }
    let (first, second) = harness.drive(opening, true).await;
    assert_eq!((first.unwrap().epoch, second.unwrap().epoch), (1, 1));
    let (fenced, unchanged) = harness
        .drive(
            futures::future::join(
                links.open_producer(
                    authority.primary,
                    open(40, 42, Mode::Fence, Some(1)),
                    Policy::LocalDurable,
                ),
                links.open_producer(
                    authority.primary,
                    open(30, 32, Mode::Resume, Some(1)),
                    Policy::LocalDurable,
                ),
            ),
            true,
        )
        .await;
    assert_eq!((fenced.unwrap().epoch, unchanged.unwrap().epoch), (2, 1));
    assert_eq!(links.session(authority.primary), Some(session));
    assert_eq!(links.socket_count(), 2);
    // Admission and negotiation deadlines use the same injected clock.
    let mut missing = pin!(links.open_producer(
        NodeId::from_bytes([2; 16]),
        open(40, 43, Mode::Resume, Some(2)),
        Policy::LocalDurable
    ));
    assert!(futures::poll!(missing.as_mut()).is_pending());
    clock.advance(Duration::from_secs(5)).unwrap();
    assert!(matches!(missing.await, Err(BrokerLinkError::Timeout)));
    links.shutdown().await.unwrap();
    harness.shutdown().await;
}
