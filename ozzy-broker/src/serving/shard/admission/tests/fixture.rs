use super::*;
use bytes::Bytes;
use omq_tokio::Message;
use ozzy_proto::{
    Envelope, MessageId, Opcode, ProducerId, RequestId, append, data::DataLimits, handshake,
};
use ozzy_runtime::{
    frontend::{
        Access, Dispatcher, DispatcherLimits, LinkIds, LinkSessions, ReceiveError, ReceiveStorage,
        Rejection, ReplyLimits, RoutingTable, Service,
    },
    memory::{self, Domain, Limits},
    replica_transport::QueueLimits,
};
use std::{collections::BTreeSet, num::NonZeroU64, task::Waker};

pub(super) struct FakeActors {
    pub receiver: Receiver,
    pub peer: ozzy_proto::NodeId,
    pub demand: bool,
    pub history: Option<ozzy_replication::wire::FetchOps>,
    pub busy: BTreeSet<ProducerId>,
    pub replica_busy: bool,
    pub minimum_body_bytes: usize,
    pub fail_revoke: bool,
    pub grants: usize,
}

impl AdmissionActors for FakeActors {
    fn receive_demand(&self, group: GroupId) -> Option<ReceiveDemand> {
        if !self.demand || group != self.receiver.report().channel.scope.group_id {
            return None;
        }
        self.history.map(ReceiveDemand::History).or_else(|| {
            Some(ReceiveDemand::Normal {
                peer: self.peer,
                report: self.receiver.report(),
                available: self.receiver.available(),
                minimum_body_bytes: self.minimum_body_bytes,
            })
        })
    }

    fn receive_is_current(&self, _: GroupId, purpose: ReceivePurpose) -> bool {
        match purpose {
            ReceivePurpose::Normal(channel) => self.receiver.report().channel == channel,
            ReceivePurpose::History(history) => self.history == Some(history),
        }
    }

    fn receive_window(&self, group: GroupId) -> Option<(Report, PipelineLimits)> {
        (group == self.receiver.report().channel.scope.group_id)
            .then(|| (self.receiver.report(), self.receiver.available()))
    }

    fn client_ready(&self, _: GroupId, _: ozzy_proto::PartitionIncarnation) -> bool {
        true
    }

    fn has_work(&self, request: GrantRequest) -> Result<bool, StartupError> {
        if request.binding.kind == Kind::Broker {
            return Ok(self.replica_busy);
        }
        Ok(request
            .route
            .writer
            .is_some_and(|writer| self.busy.contains(&writer)))
    }

    fn grant_receive(
        &mut self,
        channel: Channel,
        operations: u64,
        bytes: u64,
    ) -> Result<(), StartupError> {
        self.receiver
            .grant(channel, operations, bytes)
            .map_err(failure)?;
        self.grants += 1;
        Ok(())
    }

    fn revoke_receive(&mut self, channel: Channel) -> Result<(), IntakeError> {
        if self.fail_revoke {
            return Err(IntakeError::Invariant);
        }
        let next = Channel {
            epoch: ReceiveEpoch::new(channel.epoch.get() + 1).unwrap(),
            ..channel
        };
        self.receiver
            .revoke_unused(next.epoch)
            .map_err(|_| IntakeError::Invariant)?;
        Ok(())
    }
}

pub(super) struct Fixture {
    pub admission: ShardAdmission,
    pub service: Service,
    pub binding: Binding,
    pub actors: FakeActors,
    pub routes: RoutingTable,
    pub client: frontend::Binding,
    pub broker: frontend::Binding,
    pub data: memory::Owner,
    pub client_data: Destination,
}

fn node(index: u8) -> ozzy_proto::NodeId {
    ozzy_proto::NodeId::from_bytes([index; 16])
}

pub(super) fn placement(index: u8) -> frontend::Placement {
    frontend::Placement {
        group: GroupId::from_bytes([index + 10; 16]),
        partition: ozzy_proto::PartitionIncarnation::from_bytes([index + 20; 16]),
        shard: 0,
    }
}

fn budget(bytes: usize) -> dispatch::Budget {
    dispatch::Budget {
        queue_slots: 4,
        retained_messages: 4,
        bytes,
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "real bounded frontend setup without sockets or disk"
)]
pub(super) fn fixture() -> Fixture {
    let limits = dispatch::Budgets {
        data: budget(65_536),
        control: budget(32_768),
    };
    let envelope = ozzy_proto::EnvelopeLimits {
        max_metadata_bytes: 512,
        max_payload_bytes: 512,
    };
    let codec = DataLimits {
        envelope,
        max_records: 4,
        max_parts: 4,
        max_record_bytes: 512,
    };
    let buffers = ReceiveBuffers::new(
        envelope,
        ReceiveStorage::Inproc {
            payload_backing_bytes: 512,
        },
    )
    .unwrap();
    let domain = Domain::new(None, 196_608).unwrap();
    let data = domain
        .owner(Limits {
            bytes: 131_072,
            buffers: 128,
            cache_bytes: 131_072,
        })
        .unwrap();
    let control = domain
        .owner(Limits {
            bytes: 65_536,
            buffers: 128,
            cache_bytes: 65_536,
        })
        .unwrap();
    let (sender, mut intake) = ShardIntake::new(
        data.clone(),
        &control,
        dispatch::Limits {
            capacity: limits,
            clients: 2,
            grants: 8,
        },
        8,
        1,
    )
    .unwrap();
    let placements = [placement(0), placement(1)];
    let mut replicas = Vec::new();
    let mut client_data = None;
    for placement in placements {
        for (kind, class, quota) in [
            (
                Kind::Client,
                Class::Data,
                memory::Quota {
                    bytes: 2048,
                    buffers: 2,
                },
            ),
            (
                Kind::Client,
                Class::Control,
                memory::Quota {
                    bytes: 256,
                    buffers: 2,
                },
            ),
            (
                Kind::Broker,
                Class::Data,
                memory::Quota {
                    bytes: 2048,
                    buffers: 2,
                },
            ),
            (Kind::Broker, Class::Control, memory::Quota::default()),
        ] {
            let destination = intake
                .destination(placement.group, kind, class, quota)
                .unwrap();
            if kind == Kind::Broker && class == Class::Data {
                replicas.push((placement, destination.clone()));
            }
            if placement == placements[0] && kind == Kind::Client && class == Class::Data {
                client_data = Some(destination);
            }
        }
    }
    let make_routes = || RoutingTable::new(&[0], &placements, 2, envelope).unwrap();
    let queue = QueueLimits {
        messages: 4,
        bytes: 32_768,
        message_bytes: 8192,
    };
    let dispatcher = Dispatcher::new(
        node(9),
        make_routes(),
        vec![(0, sender)],
        DispatcherLimits {
            peers: 2,
            grants_per_class: 8,
            replies: ReplyLimits {
                data: queue,
                control: queue,
            },
        },
    )
    .unwrap();
    let parameters =
        handshake::Parameters::streaming(codec, handshake::OWNER | 8, 8, 65_536).unwrap();
    let ids = |id| LinkIds::deterministic(NonZeroU64::new(id).unwrap());
    let mut service = Service::new(
        dispatcher,
        parameters,
        &[
            Access {
                peer: node(1),
                kind: Kind::Broker,
            },
            Access {
                peer: node(2),
                kind: Kind::Client,
            },
        ],
        ids(99),
    )
    .unwrap();
    for (id, roles) in [(1, handshake::OWNER | 8), (2, handshake::PRODUCER)] {
        let parameters = handshake::Parameters::streaming(codec, roles, 8, 65_536).unwrap();
        let remote = LinkSessions::with_ids(
            node(id),
            parameters,
            handshake::OWNER,
            1,
            ids(u64::from(id)),
        )
        .unwrap();
        service
            .receive(
                Message::with_prefix(
                    Bytes::copy_from_slice(node(id).as_bytes()),
                    Message::multipart(remote.start(node(9)).unwrap()),
                ),
                4096,
            )
            .unwrap();
    }
    let broker = service.links().get(node(1)).unwrap().binding;
    let client = service.links().get(node(2)).unwrap().binding;
    let binding = Binding {
        links: service.links(),
        port: service.port(0, limits).unwrap(),
        requests: service.grant_requests(0, 8).unwrap(),
    };
    let channel = Channel {
        scope: Scope {
            group_id: placements[0].group,
            configuration_epoch: 1,
            configuration_digest: Digest::from_bytes([7; 32]),
            view: 1,
        },
        epoch: ReceiveEpoch::new(1).unwrap(),
    };
    let receiver = Receiver::new_reserved(
        channel,
        Prefix::GENESIS,
        PipelineLimits {
            max_operations: 64,
            max_body_bytes: 1024,
        },
    )
    .unwrap();
    Fixture {
        admission: ShardAdmission::new(intake, replicas, limits, buffers),
        service,
        binding,
        actors: FakeActors {
            receiver,
            peer: node(1),
            demand: true,
            history: None,
            busy: BTreeSet::new(),
            replica_busy: false,
            minimum_body_bytes: 1024,
            fail_revoke: false,
            grants: 0,
        },
        routes: make_routes(),
        client,
        broker,
        data,
        client_data: client_data.unwrap(),
    }
}

impl Fixture {
    fn operation(scope: Scope) -> ozzy_replication::wire::Operation<'static> {
        use ozzy_journal::operation::{CanonicalOperation, OperationKind, canonical_body_digest};
        let body = b"1234567890123456";
        ozzy_replication::wire::Operation::from_verified(
            CanonicalOperation {
                group_id: scope.group_id,
                configuration_epoch: scope.configuration_epoch,
                original_view: scope.view,
                op_number: 1,
                previous_digest: Digest::ZERO,
                kind: OperationKind::Barrier,
                body,
            },
            canonical_body_digest(body),
        )
    }

    pub(super) fn history_request(&self) -> ozzy_replication::wire::FetchOps {
        let scope = self.actors.receiver.report().channel.scope;
        ozzy_replication::wire::FetchOps {
            scope,
            request_id: RequestId::from_bytes([8; 16]),
            source: ozzy_replication::LogSource {
                voter: self.broker.peer,
                generation: ozzy_replication::JournalGeneration(1),
                accepted: Self::operation(scope).prefix(),
            },
            predecessor: Prefix::GENESIS,
            max_operations: 1,
            max_body_bytes: 512,
        }
    }

    pub(super) fn history(&self, request: ozzy_replication::wire::FetchOps) -> Message {
        let mut metadata = [0; 512];
        let mut payload = [0; 512];
        let encoded = ozzy_replication::wire::encode_ops(
            self.broker.peer,
            self.broker.session,
            request,
            &[Self::operation(request.scope)],
            &mut metadata,
            &mut payload,
            ozzy_replication::wire::WireLimits::default(),
        )
        .unwrap();
        Message::multipart([
            Bytes::copy_from_slice(self.broker.peer.as_bytes()),
            Bytes::copy_from_slice(&encoded.header),
            Bytes::copy_from_slice(&metadata[..encoded.metadata_bytes]),
            Bytes::copy_from_slice(&payload[..encoded.payload_bytes]),
        ])
    }

    pub(super) fn prepare(&self, channel: Channel) -> Message {
        use ozzy_replication::wire::{self, Prepare, WireLimits};
        let operations = [Self::operation(channel.scope)];
        let mut metadata = [0; 512];
        let mut payload = [0; 512];
        let encoded = wire::encode_flow_prepare(
            self.broker.peer,
            self.broker.session,
            channel.epoch,
            Prepare {
                scope: channel.scope,
                committed: Prefix::GENESIS,
                operations: &operations,
            },
            &mut metadata,
            &mut payload,
            WireLimits::default(),
        )
        .unwrap();
        Message::multipart([
            Bytes::copy_from_slice(self.broker.peer.as_bytes()),
            Bytes::copy_from_slice(&encoded.header),
            Bytes::copy_from_slice(&metadata[..encoded.metadata_bytes]),
            Bytes::copy_from_slice(&payload[..encoded.payload_bytes]),
        ])
    }

    pub(super) fn begin(&mut self) -> Result<(), StartupError> {
        self.admission.begin_turn(
            &mut Context::from_waker(Waker::noop()),
            &mut self.binding,
            &mut self.actors,
        )
    }

    pub(super) fn finish(&mut self) {
        self.admission
            .finish_turn(&mut self.binding, &mut self.actors)
            .unwrap();
    }

    pub(super) fn commands(&mut self) {
        for _ in 0..32 {
            if !self.service.poll_command().unwrap() {
                break;
            }
        }
    }

    pub(super) fn writer(&self, index: u8, writer: u8) -> Message {
        let target = placement(index);
        let envelope = Envelope {
            opcode: Opcode::Append,
            response: false,
            request_id: Some(RequestId::from_bytes([writer; 16])),
            sender: self.client.peer,
            session: Some(self.client.session),
        };
        let mut metadata = Vec::with_capacity(512);
        let mut payload = Vec::with_capacity(512);
        let header = append::encode_append(
            envelope,
            append::Append {
                authority: ozzy_proto::data::Authority {
                    group_id: target.group,
                    config_epoch: 1,
                    view: 1,
                },
                partition: target.partition,
                owner_epoch: 1,
                key: append::AppendKey {
                    producer_id: ProducerId::from_bytes([writer; 16]),
                    producer_epoch: 1,
                    first_sequence: 0,
                },
                policy: append::Policy::QuorumDurable,
                records: &[append::Record {
                    encoding: ozzy_proto::data::Encoding::Raw,
                    message_id: MessageId::from_bytes([writer; 16]),
                    parts: &[b"opaque"],
                }],
            },
            &mut metadata,
            &mut payload,
            DataLimits::default(),
        )
        .unwrap();
        Message::multipart([
            Bytes::copy_from_slice(self.client.peer.as_bytes()),
            Bytes::copy_from_slice(&header),
            Bytes::from(metadata),
            Bytes::from(payload),
        ])
    }

    pub(super) fn request_writer(&mut self, index: u8, writer: u8) {
        assert!(matches!(
            self.service.receive(self.writer(index, writer), 4096),
            Err(ReceiveError::Dispatch(Rejection::NoGrant))
        ));
    }

    pub(super) fn dequeue(&mut self) -> ozzy_runtime::frontend::IntakeMessage {
        self.admission
            .intake
            .receive(&self.binding.links, &self.routes)
            .unwrap()
            .unwrap()
    }
}
