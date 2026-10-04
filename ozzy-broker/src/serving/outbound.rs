//! Bounded observation and retry of shard-to-dispatcher replies.

use super::failure;
use crate::StartupError;
use omq_tokio::{Message, TrySendError};
use ozzy_proto::{EnvelopeLimits, NodeId, Opcode};
use ozzy_runtime::{
    dispatch::Class,
    frontend::{Pending, Port, PublicationError, PublicationResult, ReplyError, ReplyResult},
};
use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

enum State {
    Ready(Message),
    Pending(Pending<ReplyResult>),
}

enum Publication {
    Ready(Message),
    Pending(Pending<PublicationResult>),
}
struct Entry {
    peer: NodeId,
    class: Class,
    bytes: usize,
    state: State,
    retry: Duration,
    queued: Option<Instant>,
}

pub(super) struct Outbound {
    slots: Vec<Option<Entry>>,
    usage: BTreeMap<(NodeId, Class), (usize, usize)>,
    limits: [usize; 2],
    next: usize,
    publications: [Option<Publication>; 2],
    publication_bytes: usize,
    envelope: EnvelopeLimits,
}

impl Outbound {
    pub(super) fn new(peers: usize, message_bytes: usize, envelope: EnvelopeLimits) -> Self {
        Self {
            slots: (0..peers * 12).map(|_| None).collect(),
            usage: BTreeMap::new(),
            limits: [message_bytes * 4, 1024 * 1024],
            next: 0,
            publications: [None, None],
            publication_bytes: message_bytes + 2048,
            envelope,
        }
    }

    pub(super) fn send(&mut self, message: Message) -> Result<(), TrySendError> {
        let frames: [&[u8]; 3] =
            std::array::from_fn(|index| message.part_slice(index + 1).unwrap_or_default());
        let opcode = ozzy_proto::decode_packet(&frames, self.envelope)
            .ok()
            .map(|packet| packet.envelope.opcode);
        if opcode == Some(Opcode::PreparePub)
            || message
                .part_slice(0)
                .is_some_and(|prefix| prefix.len() == 32)
        {
            // Retain each partition's prepared frame until this shared slot
            // is available. Publication never consumes reply/control slots.
            let slot = usize::from(opcode != Some(Opcode::PreparePub));
            if self.publications[slot].is_some() {
                return Err(TrySendError::Full(message));
            }
            self.publications[slot] = Some(Publication::Ready(message));
            return Ok(());
        }
        let Some(peer) = message
            .part_slice(0)
            .and_then(|bytes| bytes.try_into().ok())
            .map(NodeId::from_bytes)
        else {
            return Err(TrySendError::Error(omq_tokio::Error::Config(
                "invalid shard reply routing".into(),
            )));
        };
        let frames: [&[u8]; 3] =
            std::array::from_fn(|index| message.part_slice(index + 1).unwrap_or_default());
        let class = match ozzy_proto::decode_packet(&frames, self.envelope)
            .ok()
            .map(|packet| packet.envelope.opcode)
        {
            Some(Opcode::PrepareFlow | Opcode::Ops | Opcode::SnapshotChunk | Opcode::Records) => {
                Class::Data
            }
            _ => Class::Control,
        };
        let index = usize::from(class != Class::Data);
        let bytes = message.max_message_size_len() + 2048;
        let usage = self.usage.get(&(peer, class)).copied().unwrap_or_default();
        if usage.0 >= if class == Class::Data { 4 } else { 8 }
            || bytes > self.limits[index].saturating_sub(usage.1)
        {
            return Err(TrySendError::Full(message));
        }
        let data_slots = self.slots.len() / 3;
        let slots = if class == Class::Data {
            &mut self.slots[..data_slots]
        } else {
            &mut self.slots[data_slots..]
        };
        let Some(slot) = slots.iter_mut().find(|slot| slot.is_none()) else {
            return Err(TrySendError::Full(message));
        };
        self.usage
            .insert((peer, class), (usage.0 + 1, usage.1 + bytes));
        *slot = Some(Entry {
            peer,
            class,
            bytes,
            state: State::Ready(message),
            retry: Duration::ZERO,
            queued: ozzy_runtime::profiling::start(),
        });
        Ok(())
    }

    pub(super) fn poll(
        &mut self,
        cx: &mut Context<'_>,
        port: &mut Port,
        now: Duration,
    ) -> Result<(), StartupError> {
        port.poll_progress(cx).map_err(failure)?;
        for slot in 0..2 {
            self.poll_publication(cx, port, slot)?;
        }
        // Bound the turn by occupied entries. New entries take the first free
        // slot of their class, so a window over all slots reaches them once
        // per complete pass and holds per-peer capacity for many turns.
        let mut occupied = self
            .usage
            .values()
            .map(|usage| usage.0)
            .sum::<usize>()
            .min(64);
        for _ in 0..self.slots.len() {
            if occupied == 0 {
                break;
            }
            let index = self.next;
            self.next = (index + 1) % self.slots.len();
            let Some(mut entry) = self.slots[index].take() else {
                continue;
            };
            occupied -= 1;
            if let State::Pending(pending) = &mut entry.state {
                match Pin::new(pending).poll(cx) {
                    Poll::Pending => {
                        self.slots[index] = Some(entry);
                        continue;
                    }
                    Poll::Ready(Ok(Ok(()) | Err((ReplyError::Peer | ReplyError::Session, _)))) => {
                        ozzy_runtime::profiling::finish(
                            ozzy_runtime::profiling::Stage::ShardReplyQueue,
                            entry.queued,
                        );
                        let usage = self
                            .usage
                            .get_mut(&(entry.peer, entry.class))
                            .expect("owned outbound entry");
                        usage.0 -= 1;
                        usage.1 -= entry.bytes;
                        if usage.0 == 0 {
                            self.usage.remove(&(entry.peer, entry.class));
                        }
                        cx.waker().wake_by_ref();
                        continue;
                    }
                    Poll::Ready(Ok(Err((ReplyError::Full, message)))) => {
                        ozzy_runtime::profiling::event(
                            ozzy_runtime::profiling::Event::ShardReplyRefusal,
                        );
                        entry.state = State::Ready(message);
                        entry.retry = now + Duration::from_millis(10);
                    }
                    Poll::Ready(Ok(Err((error, _)))) => return Err(failure(error)),
                    Poll::Ready(Err(error)) => return Err(failure(error)),
                }
            }
            if now >= entry.retry
                && let State::Ready(message) = entry.state
            {
                entry.state = match port.try_reply(entry.class, message, entry.bytes) {
                    Ok(pending) => State::Pending(pending),
                    Err((ozzy_runtime::frontend::PortError::Admission(_), message)) => {
                        ozzy_runtime::profiling::event(
                            ozzy_runtime::profiling::Event::ShardPortRefusal,
                        );
                        entry.retry = now + Duration::from_millis(10);
                        State::Ready(message)
                    }
                    Err((error, _)) => return Err(failure(error)),
                };
                if matches!(entry.state, State::Pending(_)) {
                    cx.waker().wake_by_ref();
                }
            }
            self.slots[index] = Some(entry);
        }
        Ok(())
    }

    fn poll_publication(
        &mut self,
        cx: &mut Context<'_>,
        port: &mut Port,
        slot: usize,
    ) -> Result<(), StartupError> {
        let Some(mut publication) = self.publications[slot].take() else {
            return Ok(());
        };
        if let Publication::Pending(pending) = &mut publication {
            match Pin::new(pending).poll(cx) {
                Poll::Pending => {
                    self.publications[slot] = Some(publication);
                    return Ok(());
                }
                Poll::Ready(Ok(Err((PublicationError::Full, message)))) => {
                    self.publications[slot] = Some(Publication::Ready(message));
                    cx.waker().wake_by_ref();
                    return Ok(());
                }
                Poll::Ready(Ok(_)) => {
                    cx.waker().wake_by_ref();
                    return Ok(());
                }
                Poll::Ready(Err(error)) => return Err(failure(error)),
            }
        }
        let Publication::Ready(message) = publication else {
            unreachable!("settled pending")
        };
        match port.try_publish(message, self.publication_bytes) {
            Ok(pending) => {
                self.publications[slot] = Some(Publication::Pending(pending));
                cx.waker().wake_by_ref();
            }
            Err((ozzy_runtime::frontend::PortError::Admission(_), message)) => {
                self.publications[slot] = Some(Publication::Ready(message));
            }
            Err((error, _)) => return Err(failure(error)),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use ozzy_proto::{Envelope, LinkSessionId, RequestId};

    fn limits() -> EnvelopeLimits {
        EnvelopeLimits {
            max_metadata_bytes: 64 * 1024,
            max_payload_bytes: 2 * 1024 * 1024,
        }
    }

    fn frame(opcode: Opcode, bytes: usize) -> Message {
        let envelope = Envelope {
            opcode,
            response: false,
            request_id: (opcode != Opcode::PreparePub).then_some(RequestId::from_bytes([1; 16])),
            sender: NodeId::from_bytes([2; 16]),
            session: (opcode != Opcode::PreparePub).then_some(LinkSessionId::from_bytes([3; 16])),
        };
        let header = envelope.encode_header(0, bytes, limits()).unwrap();
        Message::with_prefix(
            Bytes::copy_from_slice(NodeId::from_bytes([4; 16]).as_bytes()),
            Message::multipart([
                Bytes::copy_from_slice(&header),
                Bytes::new(),
                Bytes::from(vec![0; bytes]),
            ]),
        )
    }

    #[test]
    fn configured_large_replica_and_reader_frames_use_data_capacity() {
        for opcode in [Opcode::PrepareFlow, Opcode::Ops, Opcode::Records] {
            let mut outbound = Outbound::new(1, 2 * 1024 * 1024, limits());
            let message = frame(opcode, 1024 * 1024 + 4096);
            assert!(outbound.send(message).is_ok(), "{opcode:?}");
            assert_eq!(outbound.usage.len(), 1);
            assert_eq!(
                outbound.usage[&(NodeId::from_bytes([4; 16]), Class::Data)].0,
                1
            );
        }
    }

    #[test]
    fn queued_replies_settle_within_two_turns_of_a_responsive_dispatcher() {
        use ozzy_proto::{GroupId, PartitionIncarnation, data::DataLimits, handshake};
        use ozzy_runtime::{
            dispatch::{Budget, Budgets},
            frontend::{
                Access, Dispatcher, DispatcherLimits, Kind, LinkIds, Placement, ReplyLimits,
                RoutingTable, Service,
            },
            replica_transport::QueueLimits,
        };
        let budgets = Budgets {
            data: Budget {
                queue_slots: 8,
                retained_messages: 8,
                bytes: 1024 * 1024,
            },
            control: Budget {
                queue_slots: 16,
                retained_messages: 16,
                bytes: 1024 * 1024,
            },
        };
        let (sender, _shard) = ozzy_runtime::frontend::data_channel(
            &omq_tokio::Context::new(),
            0,
            Kind::Client,
            Class::Control,
            8,
            128 * 1024,
            1024 * 1024,
        )
        .unwrap();
        let routes = RoutingTable::new(
            &[0],
            &[Placement {
                shard: 0,
                group: GroupId::from_bytes([10; 16]),
                partition: PartitionIncarnation::from_bytes([20; 16]),
            }],
            1,
            limits(),
        )
        .unwrap();
        let queue = QueueLimits {
            messages: 8,
            bytes: 1024 * 1024,
            message_bytes: 128 * 1024,
        };
        let dispatcher = Dispatcher::new(
            NodeId::from_bytes([9; 16]),
            routes,
            vec![(0, sender)],
            DispatcherLimits {
                peers: 2,

                replies: ReplyLimits {
                    control: queue,
                    data: queue,
                },
            },
        )
        .unwrap();
        let mut parameters =
            handshake::Parameters::streaming(DataLimits::default(), handshake::OWNER | 8).unwrap();
        parameters.capabilities |= handshake::OWNER_READ | (1 << 3) | (1 << 8);
        parameters.required_capabilities = 0;
        let mut service = Service::new(
            dispatcher,
            parameters,
            &[Access {
                peer: NodeId::from_bytes([4; 16]),
                kind: Kind::Broker,
            }],
            LinkIds::random(),
        )
        .unwrap();
        let mut port = service
            .port(&omq_tokio::Context::new(), 0, budgets)
            .unwrap();
        // Production sizing: 32 clients and the two other brokers.
        let mut outbound = Outbound::new(34, 128 * 1024, limits());
        for _ in 0..8 {
            outbound.send(frame(Opcode::Commit, 0)).unwrap();
        }
        assert!(matches!(
            outbound.send(frame(Opcode::Commit, 0)),
            Err(TrySendError::Full(_))
        ));
        let mut cx = Context::from_waker(std::task::Waker::noop());
        let mut turns = 0;
        while !outbound.usage.is_empty() {
            turns += 1;
            assert!(
                turns <= 2,
                "queued replies still hold per-peer slots in turn {turns}"
            );
            outbound
                .poll(&mut cx, &mut port, Duration::from_millis(turns))
                .unwrap();
            while service.poll_command().unwrap() {}
        }
        assert!(outbound.send(frame(Opcode::Commit, 0)).is_ok());
    }

    #[test]
    fn a_pending_publication_preserves_other_partitions_without_using_reply_capacity() {
        let mut outbound = Outbound::new(1, 2 * 1024 * 1024, limits());
        let publication = |partition| {
            Message::multipart([
                Bytes::from(vec![partition; 32]),
                Bytes::new(),
                Bytes::new(),
                Bytes::from_static(b"records"),
            ])
        };
        outbound.send(publication(1)).unwrap();
        let Err(TrySendError::Full(second)) = outbound.send(publication(2)) else {
            panic!("another partition's publication must remain available for retry");
        };
        assert_eq!(second.part_slice(0), Some([2; 32].as_slice()));
        assert_eq!(second.part_slice(3), Some(b"records".as_slice()));
        assert!(outbound.usage.is_empty());
        outbound.send(frame(Opcode::Appended, 0)).unwrap();
        let Some(Publication::Ready(first)) = outbound.publications[1].take() else {
            panic!("first publication must remain queued");
        };
        assert_eq!(first.part_slice(0), Some([1; 32].as_slice()));
        outbound.send(second).unwrap();
        assert_eq!(outbound.usage.len(), 1);
    }

    #[test]
    fn follower_publication_pressure_preserves_reader_publication_capacity() {
        let mut outbound = Outbound::new(1, 2 * 1024 * 1024, limits());
        outbound.send(frame(Opcode::PreparePub, 32)).unwrap();
        let reader = Message::multipart([
            Bytes::from(vec![1; 32]),
            Bytes::new(),
            Bytes::new(),
            Bytes::from_static(b"records"),
        ]);
        outbound.send(reader).unwrap();
        assert!(outbound.publications.iter().all(Option::is_some));
        assert!(outbound.usage.is_empty());
        assert!(matches!(
            outbound.send(frame(Opcode::PreparePub, 32)),
            Err(TrySendError::Full(_))
        ));
    }

    #[test]
    fn large_control_frames_cannot_consume_data_capacity() {
        let mut outbound = Outbound::new(1, 2 * 1024 * 1024, limits());
        assert!(matches!(
            outbound.send(frame(Opcode::Commit, 1024 * 1024 + 4096)),
            Err(TrySendError::Full(_))
        ));
        assert!(outbound.usage.is_empty());
    }
}
