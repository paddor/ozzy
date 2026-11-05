//! A consumer reads the partition as an SDK reader does, through each
//! leader's production `SharedReaders` and live topic, merged by the
//! production `LiveCursor`: SUBSCRIBE from its next offset, which also joins
//! the leader's live topic once SUBSCRIBED admits it; RECORDS until the
//! cursor goes live, then only the replay stream is canceled; a gap holds a
//! publication and replays up to it; silence, a refusal, or a newer leader
//! replays from the next offset again.
//!
//! The live topic is a PUB: each joined consumer whose grant admits it gets
//! a copy, dropped when its queue is full or by the profile's PUB loss.
//! Every delivered record goes to the `Ledger`, and its offset must be the
//! consumer's own next one, apart from what the cursor decides.

use crate::{
    broker::{Broker, GROUPS, PARTITIONS},
    check::leader,
    records::{LIMITS, Ledger, Producers},
    seeds::{Rng, Trace},
};
use bytes::Bytes;
use omq_tokio::Message;
use ozzy_core::live::{CursorError, LiveCursor, Publication, Replayed};
use ozzy_proto::{
    Envelope, MessageId, NodeId, Opcode, Packet, RequestId, SubscriptionId,
    append::Authority,
    data::Records,
    decode_packet, handshake,
    reader::{
        self, Cancel, Source, Start, Subscribe, Subscribed, Subscription, Target, Unsubscribe,
    },
};
use ozzy_runtime::frontend::{Binding, Kind, Link};
use std::{collections::VecDeque, time::Duration};

/// Consumers per partition, each with its own client link to every broker;
/// consumer `i` reads partition `i % partitions`.
const PER_PARTITION: usize = 2;
/// Consumers when every group hosts a partition.
pub(crate) const CONSUMERS: usize = PER_PARTITION * GROUPS.len();
/// Ticks without a replay delivery, while records wait, before subscribing
/// again.
const SILENCE: u64 = 200;
/// Ticks after a refusal before subscribing again.
const RETRY: u64 = 20;
/// Live delivery silent this long returns to replay, as
/// `TopicReaderConfig::refresh` does.
const QUIET: Duration = Duration::from_secs(1);
/// Live copies a consumer holds unread; more are dropped, as a SUB's
/// high-water mark drops them.
const LIVE_QUEUE: usize = 8;
/// The first byte of every consumer node ID.
const NODE: u8 = 0xe0;

/// A replay subscription the consumer sent.
#[derive(Debug, Clone, Copy)]
struct Requested {
    broker: usize,
    view: u64,
    subscription: Subscription,
    request: RequestId,
    from: u64,
    /// The last tick anything arrived for it, or it started.
    heard: u64,
    /// SUBSCRIBED accepted it, with this source.
    accepted: Option<Source>,
}

/// The live topic a consumer's SUB joined after SUBSCRIBED.
#[derive(Debug, Clone, Copy)]
struct Joined {
    broker: usize,
    source: Source,
    topic: [u8; 32],
}

/// One record: its ID and single part.
type Held = Vec<(MessageId, Vec<u8>)>;

#[derive(Debug)]
struct Consumer {
    cursor: LiveCursor,
    /// The offset the next delivered record must have, kept apart from the
    /// cursor.
    next: u64,
    replay: Option<Requested>,
    joined: Option<Joined>,
    live: VecDeque<Message>,
    /// The publication the cursor holds across a gap, from offset `.0`.
    held: Option<(u64, Held)>,
    next_at: u64,
    attempts: u64,
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct Counts {
    pub subscribes: u64,
    pub cancels: u64,
    pub replayed: u64,
    pub published: u64,
    pub dropped: u64,
    pub held: u64,
    pub refusals: u64,
    pub stale: u64,
    /// Replay streams the cursor had moved past, reopened.
    pub reopened: u64,
}

#[derive(Debug)]
pub(crate) struct Consumers {
    readers: Vec<Consumer>,
    partitions: usize,
    rng: Rng,
    /// Chance that a live copy is lost.
    loss: f64,
    pub counts: Counts,
}

impl Consumers {
    pub(crate) fn new(seed: u64, partitions: usize, loss: f64) -> Self {
        Self {
            readers: (0..PER_PARTITION * partitions)
                .map(|_| Consumer {
                    cursor: LiveCursor::new(0, QUIET),
                    next: 0,
                    replay: None,
                    joined: None,
                    live: VecDeque::new(),
                    held: None,
                    next_at: 0,
                    attempts: 0,
                })
                .collect(),
            partitions,
            rng: Rng::new(seed),
            loss,
            counts: Counts::default(),
        }
    }

    /// Every consumer of `partition` read through `end`.
    pub(crate) fn caught_up(&self, partition: usize, end: u64) -> bool {
        self.of(partition).all(|consumer| consumer.next == end)
    }

    /// The lowest offset a consumer of `partition` has yet to read.
    pub(crate) fn read_through(&self, partition: usize) -> u64 {
        self.of(partition)
            .map(|consumer| consumer.next)
            .min()
            .unwrap_or(0)
    }

    fn of(&self, partition: usize) -> impl Iterator<Item = &Consumer> {
        self.readers.iter().skip(partition).step_by(self.partitions)
    }

    /// A broker's live publication: a copy for each consumer that joined
    /// its topic there and that the broker's grants admit.
    pub(crate) fn publish(&mut self, broker: &Broker, message: &Message) {
        let Some(topic) = message.part_slice(0) else {
            return;
        };
        for index in 0..self.readers.len() {
            let consumer = &mut self.readers[index];
            let joined = consumer
                .joined
                .is_some_and(|joined| joined.broker == broker.index && joined.topic == topic);
            if !joined || !broker.grants.admits(topic, client(index)) {
                continue;
            }
            if consumer.live.len() >= LIVE_QUEUE || self.rng.chance(self.loss) {
                self.counts.dropped += 1;
                continue;
            }
            consumer.live.push_back(message.clone());
        }
    }

    /// Follow the newest leader, read live copies, and subscribe or cancel
    /// replay as each cursor requires. `ends` are the newest leaders' log
    /// ends, by partition.
    pub(crate) fn step(
        &mut self,
        tick: u64,
        now: Duration,
        brokers: &mut [Broker],
        ends: &[Option<u64>],
        ledgers: &mut [Ledger],
        producers: &Producers,
        trace: &mut Trace,
    ) -> Result<(), String> {
        for index in 0..self.readers.len() {
            let partition = index % self.partitions;
            let newest = leader(brokers, partition);
            let end = ends[partition];
            let ledger = &mut ledgers[partition];
            let consumer = &mut self.readers[index];
            let moved =
                |broker: usize, view: u64| newest.is_some_and(|newest| newest != (broker, view));
            let replay_moved = consumer
                .replay
                .is_some_and(|replay| moved(replay.broker, replay.view));
            let live_moved = consumer.joined.is_some_and(|joined| {
                let Source::Group { authority, .. } = joined.source else {
                    return true;
                };
                moved(joined.broker, authority.view)
            });
            if replay_moved || live_moved {
                // The log's authority changed: replay from the cursor there.
                consumer.cursor.source_changed();
                consumer.replay = None;
                consumer.joined = None;
                consumer.live.clear();
                consumer.held = None;
            }
            consumer.cursor.tick(now);
            while !consumer.cursor.is_paused()
                && let Some(message) = consumer.live.pop_front()
            {
                self.counts.published += 1;
                publication(
                    index,
                    consumer,
                    &message,
                    now,
                    ledger,
                    producers,
                    &mut self.counts,
                )?;
            }
            let waiting = end.is_some_and(|end| consumer.next < end);
            if consumer
                .replay
                .is_some_and(|replay| waiting && tick >= replay.heard + SILENCE)
            {
                consumer.replay = None;
            }
            match (consumer.cursor.replay(), consumer.replay) {
                (None, Some(replay)) => {
                    // Live delivery took over; the live topic stays granted.
                    if let Some(source) = replay.accepted {
                        cancel(
                            index,
                            partition,
                            &mut brokers[replay.broker],
                            replay,
                            source,
                        )?;
                        self.counts.cancels += 1;
                    }
                    consumer.replay = None;
                }
                (Some(wanted), None) if tick >= consumer.next_at => {
                    let Some((broker, view)) = newest else {
                        continue;
                    };
                    consumer.attempts += 1;
                    let replay = subscribe(
                        index,
                        partition,
                        &mut brokers[broker],
                        view,
                        consumer.attempts,
                        wanted.from,
                        tick,
                    )?;
                    consumer.replay = Some(replay);
                    self.counts.subscribes += 1;
                    trace.word(tick);
                    trace.word(index as u64 + 200);
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// One reply to consumer `index`.
    pub(crate) fn reply(
        &mut self,
        index: usize,
        broker: usize,
        message: &Message,
        tick: u64,
        now: Duration,
        ledgers: &mut [Ledger],
        producers: &Producers,
    ) -> Result<(), String> {
        let ledger = &mut ledgers[index % self.partitions];
        let frames: [&[u8]; 3] =
            std::array::from_fn(|frame| message.part_slice(frame + 1).unwrap_or_default());
        let packet = decode_packet(&frames, LIMITS.envelope)
            .map_err(|error| format!("consumer {index}: reply from broker {broker}: {error}"))?;
        let consumer = &mut self.readers[index];
        let current = consumer.replay.filter(|replay| replay.broker == broker);
        match packet.envelope.opcode {
            Opcode::Subscribed => {
                let subscribed = reader::decode_subscribed(packet, LIMITS.envelope)
                    .map_err(|error| format!("consumer {index}: subscribed: {error}"))?;
                let Some(replay) =
                    current.filter(|replay| replay.subscription == subscribed.subscription)
                else {
                    self.counts.stale += 1;
                    return Ok(());
                };
                if subscribed.resolved_offset != replay.from {
                    return Err(format!(
                        "consumer {index}: broker {broker} resolved offset {} for a subscription \
                         from {}",
                        subscribed.resolved_offset, replay.from
                    ));
                }
                let topic = reader::publication_topic(subscribed.source)
                    .map_err(|error| format!("consumer {index}: live topic: {error}"))?;
                // The SUB joins the live topic once SUBSCRIBED admits it.
                if consumer
                    .joined
                    .is_none_or(|joined| (joined.broker, joined.topic) != (broker, topic))
                {
                    consumer.live.clear();
                }
                consumer.joined = Some(Joined {
                    broker,
                    source: subscribed.source,
                    topic,
                });
                consumer.replay = Some(Requested {
                    heard: tick,
                    accepted: Some(subscribed.source),
                    ..replay
                });
            }
            Opcode::Records => {
                let delivery = reader::decode_records(packet, LIMITS)
                    .map_err(|error| format!("consumer {index}: records: {error}"))?;
                let Some(replay) =
                    current.filter(|replay| replay.subscription == delivery.header.subscription)
                else {
                    self.counts.stale += 1;
                    return Ok(());
                };
                consumer.replay = Some(Requested {
                    heard: tick,
                    ..replay
                });
                let first = delivery.header.first_offset;
                let count = delivery.records.len() as u64;
                let decision = match consumer.cursor.replayed(first, count, now) {
                    Ok(decision) => decision,
                    // A held publication moved the cursor past this stream,
                    // which went live and gapped again before it was
                    // canceled. As the SDK reader: reopen exact history.
                    Err(CursorError::Replay) => {
                        self.counts.reopened += 1;
                        consumer.replay = None;
                        return Ok(());
                    }
                    Err(error) => {
                        return Err(format!(
                            "consumer {index}: replay from offset {first} at cursor {}: {error}",
                            consumer.cursor.next()
                        ));
                    }
                };
                self.counts.replayed += 1;
                match decision {
                    Replayed::Ignore => {}
                    Replayed::Deliver => {
                        read(
                            index,
                            consumer,
                            first,
                            delivery.records,
                            0,
                            ledger,
                            producers,
                        )?;
                    }
                    Replayed::DeliverThenHeld { skip } => {
                        read(
                            index,
                            consumer,
                            first,
                            delivery.records,
                            0,
                            ledger,
                            producers,
                        )?;
                        let held = consumer.held.take();
                        if let (Some(skip), Some((held_first, records))) = (skip, held) {
                            deliver(
                                index, consumer, held_first, records, skip, ledger, producers,
                            )?;
                        }
                    }
                }
            }
            Opcode::Nack => {
                if current.is_some_and(|replay| packet.envelope.request_id == Some(replay.request))
                {
                    self.counts.refusals += 1;
                    consumer.replay = None;
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

/// One live copy, as the cursor decides.
fn publication(
    index: usize,
    consumer: &mut Consumer,
    message: &Message,
    now: Duration,
    ledger: &mut Ledger,
    producers: &Producers,
    counts: &mut Counts,
) -> Result<(), String> {
    let frames: [&[u8]; 3] =
        std::array::from_fn(|frame| message.part_slice(frame + 1).unwrap_or_default());
    let packet: Packet<'_> = decode_packet(&frames, LIMITS.envelope)
        .map_err(|error| format!("consumer {index}: publication: {error}"))?;
    let delivery = reader::decode_publication(packet, LIMITS)
        .map_err(|error| format!("consumer {index}: publication: {error}"))?;
    // A copy of another source than the joined one is not this log's.
    if consumer
        .joined
        .is_none_or(|joined| joined.source != delivery.header.source)
    {
        counts.stale += 1;
        return Ok(());
    }
    let first = delivery.header.first_offset;
    let count = delivery.records.len() as u64;
    match consumer
        .cursor
        .publication(first, count, now)
        .map_err(|error| format!("consumer {index}: publication at {first}: {error}"))?
    {
        Publication::Drop => {}
        Publication::Deliver { skip } => {
            read(
                index,
                consumer,
                first,
                delivery.records,
                skip,
                ledger,
                producers,
            )?;
        }
        Publication::Hold => {
            counts.held += 1;
            consumer.held = Some((first, owned(delivery.records)));
        }
    }
    Ok(())
}

fn owned(records: Records<'_>) -> Held {
    records
        .iter()
        .map(|record| (record.message_id, record.parts.flatten().copied().collect()))
        .collect()
}

/// Deliver `records` from offset `first`, after the first `skip`.
fn read(
    index: usize,
    consumer: &mut Consumer,
    first: u64,
    records: Records<'_>,
    skip: u64,
    ledger: &mut Ledger,
    producers: &Producers,
) -> Result<(), String> {
    deliver(
        index,
        consumer,
        first,
        owned(records),
        skip,
        ledger,
        producers,
    )
}

fn deliver(
    index: usize,
    consumer: &mut Consumer,
    first: u64,
    records: Held,
    skip: u64,
    ledger: &mut Ledger,
    producers: &Producers,
) -> Result<(), String> {
    for (offset, (id, payload)) in (first..).zip(records).skip(skip as usize) {
        if offset != consumer.next {
            return Err(format!(
                "consumer {index}: delivered offset {offset} while expecting {}",
                consumer.next
            ));
        }
        let body = *id.as_bytes();
        if payload != body {
            return Err(format!(
                "consumer {index}: offset {offset} holds {payload:?} under ID {body:?}"
            ));
        }
        let (producer, sequence) = crate::records::decode_body(body).ok_or_else(|| {
            format!("consumer {index}: offset {offset} holds a record nobody sent: {body:?}")
        })?;
        if !producers.sent(producer, sequence) {
            return Err(format!(
                "consumer {index}: offset {offset} holds producer {producer} sequence \
                 {sequence}, which it never sent"
            ));
        }
        ledger.read(producer, sequence, offset)?;
        consumer.next += 1;
    }
    Ok(())
}

fn subscribe(
    index: usize,
    partition: usize,
    broker: &mut Broker,
    view: u64,
    attempt: u64,
    from: u64,
    tick: u64,
) -> Result<Requested, String> {
    let subscription = Subscription {
        id: SubscriptionId::from_bytes([NODE + index as u8; 16]),
        generation: u128::from(attempt),
    };
    let request = request_id(index, attempt);
    let mut metadata = Vec::with_capacity(LIMITS.envelope.max_metadata_bytes);
    let header = reader::encode_subscribe(
        envelope(index, broker, Opcode::Subscribe, request),
        &Subscribe {
            subscription,
            target: Target::Group {
                authority: Authority {
                    group_id: GROUPS[partition],
                    config_epoch: 1,
                    view,
                },
                partition: PARTITIONS[partition],
                owner_epoch: 1,
            },
            start: Start::Offset(from),
        },
        &mut metadata,
        LIMITS.envelope,
    )
    .map_err(|error| format!("consumer {index}: subscribe: {error}"))?;
    send(index, partition, broker, header, &metadata);
    Ok(Requested {
        broker: broker.index,
        view,
        subscription,
        request,
        from,
        heard: tick,
        accepted: None,
    })
}

/// End only the replay stream; the subscription and its grant stay.
fn cancel(
    index: usize,
    partition: usize,
    broker: &mut Broker,
    replay: Requested,
    source: Source,
) -> Result<(), String> {
    let mut metadata = Vec::with_capacity(LIMITS.envelope.max_metadata_bytes);
    let header = reader::encode_unsubscribe(
        envelope(
            index,
            broker,
            Opcode::Unsubscribe,
            request_id(index, u64::MAX - replay.subscription.generation as u64),
        ),
        Unsubscribe {
            selected: Subscribed {
                subscription: replay.subscription,
                source,
                resolved_offset: replay.from,
            },
            cancel: Cancel::Replay,
        },
        &mut metadata,
        LIMITS.envelope,
    )
    .map_err(|error| format!("consumer {index}: unsubscribe: {error}"))?;
    send(index, partition, broker, header, &metadata);
    Ok(())
}

fn envelope(index: usize, broker: &Broker, opcode: Opcode, request: RequestId) -> Envelope {
    Envelope {
        opcode,
        response: false,
        request_id: Some(request),
        sender: client(index),
        session: Some(broker.consumer_session(index)),
    }
}

fn send(index: usize, partition: usize, broker: &mut Broker, header: [u8; 64], metadata: &[u8]) {
    let mut control = Vec::with_capacity(header.len() + metadata.len());
    control.extend_from_slice(&header);
    control.extend_from_slice(metadata);
    let mut control = Bytes::from(control);
    let header = control.split_to(header.len());
    broker.requests.push_back((
        partition,
        Message::multipart([
            Bytes::copy_from_slice(client(index).as_bytes()),
            header,
            control,
            Bytes::new(),
        ]),
    ));
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
