//! A consumer follows the partition through each leader's production
//! `SharedReaders`, as an SDK reader replays: SUBSCRIBE from its next offset,
//! then RECORDS in offset order. A newer leader, a refusal, or a silent
//! leader while records wait ends the subscription; it subscribes again from
//! its next offset at the newest leader. Every record goes to the `Ledger`.

use crate::{
    broker::{Broker, GROUP},
    check::leader,
    records::{LIMITS, Ledger, PARTITION, Producers},
    seeds::Trace,
};
use bytes::Bytes;
use omq_tokio::Message;
use ozzy_proto::{
    Envelope, NodeId, Opcode, RequestId, SubscriptionId,
    append::Authority,
    decode_packet, handshake,
    reader::{self, Start, Subscribe, Subscription, Target},
};
use ozzy_runtime::frontend::{Binding, Kind, Link};

/// Consumers, each with its own client link to every broker.
pub(crate) const CONSUMERS: usize = 1;
/// Ticks without a delivery, while records wait, before subscribing again.
const SILENCE: u64 = 200;
/// Ticks after a refusal before subscribing again.
const RETRY: u64 = 20;
/// The first byte of every consumer node ID.
const NODE: u8 = 0xe0;

#[derive(Debug, Clone, Copy)]
struct Active {
    broker: usize,
    view: u64,
    subscription: Subscription,
    request: RequestId,
    /// The last tick anything arrived for it, or it started.
    heard: u64,
}

#[derive(Debug, Default)]
struct Consumer {
    /// The offset the next delivery must start at.
    next: u64,
    active: Option<Active>,
    next_at: u64,
    attempts: u64,
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct Counts {
    pub subscribes: u64,
    pub deliveries: u64,
    pub records: u64,
    pub refusals: u64,
    pub stale: u64,
}

#[derive(Debug)]
pub(crate) struct Consumers {
    consumers: Vec<Consumer>,
    pub counts: Counts,
}

impl Consumers {
    pub(crate) fn new() -> Self {
        Self {
            consumers: (0..CONSUMERS).map(|_| Consumer::default()).collect(),
            counts: Counts::default(),
        }
    }

    /// Every consumer read through `end`.
    pub(crate) fn caught_up(&self, end: u64) -> bool {
        self.consumers.iter().all(|consumer| consumer.next == end)
    }

    /// The lowest offset a consumer has yet to read.
    pub(crate) fn read_through(&self) -> u64 {
        self.consumers
            .iter()
            .map(|consumer| consumer.next)
            .min()
            .unwrap_or(0)
    }

    /// Subscribe where needed. `end` is the newest leader's log end.
    pub(crate) fn step(
        &mut self,
        tick: u64,
        brokers: &mut [Broker],
        end: Option<u64>,
        trace: &mut Trace,
    ) -> Result<(), String> {
        let newest = leader(brokers);
        for index in 0..CONSUMERS {
            let consumer = &mut self.consumers[index];
            let waiting = end.is_some_and(|end| consumer.next < end);
            let stale = consumer.active.is_some_and(|active| {
                newest.is_some_and(|(broker, view)| (broker, view) != (active.broker, active.view))
                    || (waiting && tick >= active.heard + SILENCE)
            });
            if stale {
                consumer.active = None;
            }
            if consumer.active.is_some() || tick < consumer.next_at {
                continue;
            }
            let Some((broker, view)) = newest else {
                continue;
            };
            consumer.attempts += 1;
            let subscription = Subscription {
                id: SubscriptionId::from_bytes([NODE + index as u8; 16]),
                generation: u128::from(consumer.attempts),
            };
            let request = request_id(index, consumer.attempts);
            let broker = &mut brokers[broker];
            let mut metadata = Vec::with_capacity(LIMITS.envelope.max_metadata_bytes);
            let header = reader::encode_subscribe(
                Envelope {
                    opcode: Opcode::Subscribe,
                    response: false,
                    request_id: Some(request),
                    sender: client(index),
                    session: Some(broker.consumer_session(index)),
                },
                &Subscribe {
                    subscription,
                    target: Target::Group {
                        authority: Authority {
                            group_id: GROUP,
                            config_epoch: 1,
                            view,
                        },
                        partition: PARTITION,
                        owner_epoch: 1,
                    },
                    start: Start::Offset(consumer.next),
                },
                &mut metadata,
                LIMITS.envelope,
            )
            .map_err(|error| format!("consumer {index}: subscribe: {error}"))?;
            let mut control = Vec::with_capacity(header.len() + metadata.len());
            control.extend_from_slice(&header);
            control.extend_from_slice(&metadata);
            let mut control = Bytes::from(control);
            let header = control.split_to(header.len());
            broker.requests.push_back(Message::multipart([
                Bytes::copy_from_slice(client(index).as_bytes()),
                header,
                control,
                Bytes::new(),
            ]));
            self.counts.subscribes += 1;
            consumer.active = Some(Active {
                broker: broker.index,
                view,
                subscription,
                request,
                heard: tick,
            });
            trace.word(tick);
            trace.word(index as u64 + 200);
        }
        Ok(())
    }

    /// One reply to consumer `index`. Records of the current subscription
    /// must continue exactly at its next offset.
    pub(crate) fn reply(
        &mut self,
        index: usize,
        broker: usize,
        message: &Message,
        tick: u64,
        ledger: &mut Ledger,
        producers: &Producers,
    ) -> Result<(), String> {
        let frames: [&[u8]; 3] =
            std::array::from_fn(|frame| message.part_slice(frame + 1).unwrap_or_default());
        let packet = decode_packet(&frames, LIMITS.envelope)
            .map_err(|error| format!("consumer {index}: reply from broker {broker}: {error}"))?;
        let consumer = &mut self.consumers[index];
        let current = consumer.active.filter(|active| active.broker == broker);
        match packet.envelope.opcode {
            Opcode::Subscribed => {
                let subscribed = reader::decode_subscribed(packet, LIMITS.envelope)
                    .map_err(|error| format!("consumer {index}: subscribed: {error}"))?;
                let Some(active) =
                    current.filter(|active| active.subscription == subscribed.subscription)
                else {
                    self.counts.stale += 1;
                    return Ok(());
                };
                if subscribed.resolved_offset != consumer.next {
                    return Err(format!(
                        "consumer {index}: broker {broker} resolved offset {} for a subscription \
                         from {}",
                        subscribed.resolved_offset, consumer.next
                    ));
                }
                consumer.active = Some(Active {
                    heard: tick,
                    ..active
                });
            }
            Opcode::Records => {
                let delivery = reader::decode_records(packet, LIMITS)
                    .map_err(|error| format!("consumer {index}: records: {error}"))?;
                let Some(active) =
                    current.filter(|active| active.subscription == delivery.header.subscription)
                else {
                    self.counts.stale += 1;
                    return Ok(());
                };
                if delivery.header.first_offset != consumer.next {
                    return Err(format!(
                        "consumer {index}: broker {broker} delivered from offset {} while the \
                         subscription was at {}",
                        delivery.header.first_offset, consumer.next
                    ));
                }
                self.counts.deliveries += 1;
                for record in delivery.records.iter() {
                    let parts: Vec<&[u8]> = record.parts.collect();
                    let body = *record.message_id.as_bytes();
                    if parts != [&body[..]] {
                        return Err(format!(
                            "consumer {index}: offset {} holds {parts:?} under ID {body:?}",
                            consumer.next
                        ));
                    }
                    let (producer, sequence) =
                        crate::records::decode_body(body).ok_or_else(|| {
                            format!(
                                "consumer {index}: offset {} holds a record nobody sent: {body:?}",
                                consumer.next
                            )
                        })?;
                    if !producers.sent(producer, sequence) {
                        return Err(format!(
                            "consumer {index}: offset {} holds producer {producer} sequence \
                             {sequence}, which it never sent",
                            consumer.next
                        ));
                    }
                    ledger.read(producer, sequence, consumer.next)?;
                    consumer.next += 1;
                    self.counts.records += 1;
                }
                consumer.active = Some(Active {
                    heard: tick,
                    ..active
                });
            }
            Opcode::Nack => {
                if current.is_some_and(|active| packet.envelope.request_id == Some(active.request))
                {
                    self.counts.refusals += 1;
                    consumer.active = None;
                    consumer.next_at = tick + RETRY;
                } else {
                    self.counts.stale += 1;
                }
            }
            Opcode::Unsubscribed | Opcode::Ack => self.counts.stale += 1,
            opcode => {
                return Err(format!(
                    "consumer {index}: unexpected {opcode:?} from broker {broker}"
                ));
            }
        }
        Ok(())
    }
}

/// The consumer a client-bound message goes to.
pub(crate) fn consumer(message: &Message) -> Option<usize> {
    let route = message.part_slice(0)?;
    (0..CONSUMERS).find(|&index| route == client(index).as_bytes())
}

/// The link a broker publishes for consumer `index` under `session`.
pub(crate) fn link(index: usize, session: ozzy_proto::LinkSessionId) -> Link {
    Link {
        binding: Binding {
            peer: client(index),
            session,
            kind: Kind::Client,
        },
        send: LIMITS,
        remote: handshake::Parameters::reader(LIMITS, handshake::CONSUMER)
            .expect("consumer parameters"),
    }
}

/// Consumer `index`'s client node.
pub(crate) fn client(index: usize) -> NodeId {
    NodeId::from_bytes([NODE + index as u8; 16])
}

fn request_id(index: usize, attempt: u64) -> RequestId {
    let mut bytes = [NODE + index as u8; 16];
    bytes[8..].copy_from_slice(&attempt.to_be_bytes());
    RequestId::from_bytes(bytes)
}
