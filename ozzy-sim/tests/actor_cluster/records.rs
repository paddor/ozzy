//! Producers append records through each broker's production native intake,
//! as an SDK writer does over its links: `OPEN_PRODUCER`, then one APPEND of
//! `RECORDS` records at a time. A NACK's authority hint redirects; an unknown
//! outcome or a silent broker resends the same identity elsewhere later.
//!
//! A third of the batches are scheduled, each record due between 0.5 s ago
//! and 3 s ahead by true time; their confirmations carry schedule sequences.
//!
//! The dedup partition takes no due times. Its producers sometimes restart
//! with a batch in flight, as the SDK takeover does: resume, fence a new
//! epoch, and send the batch again from sequence zero. Some pause around
//! the idle expiry; NACK 6 then makes them resume a fresh epoch. Each epoch
//! maps its sequences onto logical record numbers, which the message ID and
//! body carry.
//!
//! Every confirmation goes to the `Ledger`, stale ones too: two records at
//! one offset or schedule sequence, one record at two, or a producer's
//! unscheduled records out of order fail the run. A `Duplicate` names the
//! original's offset. Scheduled records release in order of due time, then
//! schedule sequence, and never before their due time minus the clock
//! tolerance. A session retires only after its idle time, less the clock
//! slack. At the end, every copy's log must hold exactly the confirmed
//! records, at offsets from zero without a gap.

mod ledger;
pub(crate) use ledger::Ledger;

use crate::{
    broker::{Broker, DEDUP_GROUP, DEDUP_WINDOW, GROUPS, PARTITIONS, PRODUCER_IDLE},
    clock,
    network::{BROKERS, node},
    seeds::{Rng, Trace},
};
use bytes::Bytes;
use omq_tokio::Message;
use ozzy_proto::{
    Envelope, EnvelopeLimits, LinkSessionId, MessageId, NodeId, Opcode, OperationId, ProducerId,
    RequestId,
    append::{self, AppendDisposition, AppendKey, Authority, Policy},
    data::{DataLimits, Encoding, Record},
    decode_packet, handshake,
    nack::{self, AuthorityHint, RetryClass},
    producer::{self, Mode, Open},
};
use ozzy_runtime::frontend::{Binding, Kind, Link};
use std::collections::BTreeMap;

/// Producers per partition, each with its own client link to every broker;
/// producer `i` writes partition `i % partitions`.
const PER_PARTITION: usize = 2;
/// Producers when every group hosts a partition.
pub(crate) const PRODUCERS: usize = PER_PARTITION * GROUPS.len();
/// Client codec limits in both directions.
pub(crate) const LIMITS: DataLimits = DataLimits {
    envelope: EnvelopeLimits {
        max_metadata_bytes: 1024,
        max_payload_bytes: 1024,
    },
    max_records: 4,
    max_parts: 4,
    max_record_bytes: 64,
};
/// Records per APPEND.
const RECORDS: u64 = 3;
/// The share of scheduled batches, and their records' due times from the
/// batch start: the earliest, in milliseconds, and the span.
const SCHEDULED: f64 = 1.0 / 3.0;
const EARLIEST: i64 = -500;
const SPAN: u64 = 3500;
/// Ticks without a reply before a request is resent elsewhere, and before a
/// retry after a refusal.
const TIMEOUT: u64 = 300;
const RESEND: u64 = 100;
const REDIRECT: u64 = 10;
/// Per tick with an APPEND out, the chance that a dedup partition's
/// producer restarts.
const RESTART: f64 = 1.0 / 150.0;
/// After a batch, the chance that a dedup partition's producer pauses, for
/// half to one and a half times the idle expiry.
const PAUSE: f64 = 1.0 / 50.0;
/// NACK code of a fenced or unknown session.
const FENCED: u16 = 6;
/// The first byte of every producer node ID.
const NODE: u8 = 0xc0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Resume the session, or open epoch one.
    Resume,
    /// Fence a new epoch over this one.
    Fence(u64),
    Append,
}

/// One batch's first send, and whether a later epoch sent it again.
#[derive(Debug, Clone, Copy)]
struct Batch {
    sent: u64,
    epoch: u64,
    resent: bool,
}

#[derive(Debug, Clone, Copy)]
struct Request {
    broker: usize,
    id: RequestId,
    sent: u64,
}

#[derive(Debug)]
struct Producer {
    phase: Phase,
    /// Writer fence once opened.
    epoch: u64,
    /// Logical record number of sequence zero in each epoch.
    bases: BTreeMap<u64, u64>,
    /// The current open, reused by its resends, and its first send.
    operation: u64,
    open_sent: Option<u64>,
    /// A restart: fence a new epoch once resumed.
    takeover: bool,
    /// The latest tick the session surely changed after: the first send of
    /// a batch it confirmed, or of an open that made its epoch.
    changed: u64,
    /// Every batch so far, by its first record.
    batches: BTreeMap<u64, Batch>,
    /// First record of the current batch.
    first: u64,
    /// Records of the current batch confirmed so far.
    done: [bool; RECORDS as usize],
    /// An attempt at the current batch has an unknown outcome, so the batch
    /// may be stored. A refusal says nothing about earlier attempts.
    uncertain: bool,
    /// Refusals of an unresolved batch while draining. A full journal
    /// refuses its resends for good, so the batch stays unknown.
    drain_refusals: u32,
    abandoned: bool,
    /// End of every record sent so far.
    sent_end: u64,
    /// Due times of the current batch, when it is scheduled.
    due: Option<[u64; RECORDS as usize]>,
    /// Due time of every scheduled record so far.
    scheduled: BTreeMap<u64, u64>,
    target: usize,
    /// The newest view any reply or hint named.
    view: u64,
    pending: Option<Request>,
    next_at: u64,
    attempts: u64,
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct Counts {
    pub appends: u64,
    pub opens: u64,
    pub timeouts: u64,
    pub unknown: u64,
    pub redirects: u64,
    pub backoffs: u64,
    pub ignored: u64,
    /// Dedup partition: restarts, sessions found retired, and records a
    /// confirmation found stored before.
    pub restarts: u64,
    pub retired: u64,
    pub duplicates: u64,
}

#[derive(Debug)]
pub(crate) struct Producers {
    writers: Vec<Producer>,
    partitions: usize,
    rng: Rng,
    /// No new batches: finish the current ones.
    draining: bool,
    /// The largest host clock offset so far, in milliseconds. Stamps of a
    /// stepped leader stay, so time windows may close this much early.
    slack: u64,
    pub counts: Counts,
}

impl Producers {
    pub(crate) fn new(seed: u64, partitions: usize) -> Self {
        let mut producers = Self {
            writers: (0..PER_PARTITION * partitions)
                .map(|index| Producer {
                    phase: Phase::Resume,
                    epoch: 0,
                    bases: BTreeMap::new(),
                    operation: 0,
                    open_sent: None,
                    takeover: false,
                    changed: 0,
                    batches: BTreeMap::new(),
                    first: 0,
                    done: [false; RECORDS as usize],
                    uncertain: false,
                    drain_refusals: 0,
                    abandoned: false,
                    sent_end: 0,
                    due: None,
                    scheduled: BTreeMap::new(),
                    target: index % BROKERS,
                    view: 0,
                    pending: None,
                    next_at: 0,
                    attempts: 0,
                })
                .collect(),
            partitions,
            rng: Rng::new(seed),
            draining: false,
            slack: 0,
            counts: Counts::default(),
        };
        for (index, producer) in producers.writers.iter_mut().enumerate() {
            plan(
                &mut producers.rng,
                producer,
                0,
                index % partitions != DEDUP_GROUP,
            );
        }
        producers
    }

    /// Whether producer `index` writes the dedup partition.
    fn dedup(&self, index: usize) -> bool {
        self.partitions > DEDUP_GROUP && index % self.partitions == DEDUP_GROUP
    }

    /// A host clock is `offset` milliseconds from true time.
    pub(crate) fn clock_offset(&mut self, offset: u64) {
        self.slack = self.slack.max(offset);
    }

    /// The true time from which producer `producer`'s record `record` may
    /// take a second offset: a later epoch sent its batch again on the dedup
    /// partition, and the window has passed since the first send.
    pub(crate) fn repeat_from(&self, producer: usize, record: u64) -> Option<u64> {
        let batch = self.writers[producer]
            .batches
            .get(&(record - record % RECORDS))?;
        (batch.resent && self.dedup(producer)).then(|| {
            wall(batch.sent)
                .saturating_add(DEDUP_WINDOW)
                .saturating_sub(2 * self.slack)
        })
    }

    /// The due time of producer `producer`'s record `sequence`, if scheduled.
    pub(crate) fn due(&self, producer: usize, sequence: u64) -> Option<u64> {
        self.writers[producer].scheduled.get(&sequence).copied()
    }

    /// No request is outstanding, and no batch that may be stored awaits its
    /// confirmation, unless a full journal keeps refusing it.
    pub(crate) fn idle(&self) -> bool {
        self.writers.iter().all(|producer| {
            producer.pending.is_none() && (!producer.unresolved() || producer.abandoned)
        })
    }

    /// Producer `producer` sent its record `sequence`.
    pub(crate) fn sent(&self, producer: usize, sequence: u64) -> bool {
        sequence < self.writers[producer].sent_end
    }

    /// Record `sequence` of `producer` may be stored without a confirmation:
    /// its batch's outcome stayed unknown.
    pub(crate) fn may_hold(&self, producer: usize, sequence: u64) -> bool {
        let producer = &self.writers[producer];
        producer.unresolved() && (producer.first..producer.first + RECORDS).contains(&sequence)
    }

    /// Send due requests. `submit` false only finishes the current batches.
    pub(crate) fn step(
        &mut self,
        tick: u64,
        submit: bool,
        brokers: &mut [Broker],
        trace: &mut Trace,
    ) -> Result<(), String> {
        self.draining = !submit;
        for index in 0..self.writers.len() {
            let producer = &mut self.writers[index];
            if let Some(request) = producer.pending
                && tick >= request.sent + TIMEOUT
            {
                // No reply: the broker may have stopped, or taken it and
                // gone silent. Resend the same identity elsewhere.
                self.counts.timeouts += 1;
                producer.pending = None;
                producer.uncertain |= producer.phase == Phase::Append;
                producer.target = other(&mut self.rng, request.broker);
                producer.next_at = tick;
            }
            let dedup = self.dedup(index);
            let producer = &mut self.writers[index];
            // As the SDK takeover after a process restart: the batch in
            // flight may be stored, so it goes again under a new epoch.
            if submit
                && dedup
                && producer.phase == Phase::Append
                && producer.pending.is_some()
                && self.rng.chance(RESTART)
            {
                self.counts.restarts += 1;
                producer.pending = None;
                producer.uncertain = true;
                producer.done = [false; RECORDS as usize];
                producer.takeover = true;
                producer.reopen(Phase::Resume);
                producer.next_at = tick;
            }
            if producer.pending.is_some()
                || producer.abandoned
                || tick < producer.next_at
                || (!submit && !producer.unresolved())
            {
                continue;
            }
            self.send(index, tick, brokers, trace)?;
        }
        Ok(())
    }

    fn send(
        &mut self,
        index: usize,
        tick: u64,
        brokers: &mut [Broker],
        trace: &mut Trace,
    ) -> Result<(), String> {
        let producer = &mut self.writers[index];
        producer.attempts += 1;
        let partition = index % self.partitions;
        let broker = &mut brokers[producer.target];
        let id = request_id(index, producer.attempts);
        let envelope = |opcode| Envelope {
            opcode,
            response: false,
            request_id: Some(id),
            sender: client(index),
            session: Some(broker.client_session(index)),
        };
        let authority = Authority {
            group_id: GROUPS[partition],
            config_epoch: 1,
            view: producer.view,
        };
        let mut metadata = Vec::with_capacity(LIMITS.envelope.max_metadata_bytes);
        let mut payload = Vec::with_capacity(LIMITS.envelope.max_payload_bytes);
        let header = match producer.phase {
            phase @ (Phase::Resume | Phase::Fence(_)) => {
                self.counts.opens += 1;
                producer.open_sent.get_or_insert(tick);
                let (mode, expected_epoch) = match phase {
                    Phase::Fence(epoch) => (Mode::Fence, Some(epoch)),
                    _ => (Mode::Resume, None),
                };
                producer::encode_open(
                    envelope(Opcode::OpenProducer),
                    Open {
                        authority,
                        partition: PARTITIONS[partition],
                        producer: producer_id(index),
                        mode,
                        expected_epoch,
                        operation: operation_id(index, producer.operation),
                    },
                    &mut metadata,
                    LIMITS.envelope,
                )
                .map_err(|error| format!("producer {index}: open: {error}"))?
            }
            Phase::Append => {
                self.counts.appends += 1;
                producer.sent_end = producer.sent_end.max(producer.first + RECORDS);
                let epoch = producer.epoch;
                let batch = producer.batches.entry(producer.first).or_insert(Batch {
                    sent: tick,
                    epoch,
                    resent: false,
                });
                batch.resent |= batch.epoch != epoch;
                let first_sequence = producer.first - producer.bases[&epoch];
                let bodies: Vec<[u8; 16]> = (producer.first..producer.first + RECORDS)
                    .map(|sequence| body(index, sequence))
                    .collect();
                let parts: Vec<[&[u8]; 1]> = bodies.iter().map(|body| [&body[..]]).collect();
                let records: Vec<Record<'_>> = bodies
                    .iter()
                    .zip(&parts)
                    .map(|(body, parts)| Record {
                        encoding: Encoding::Raw,
                        message_id: MessageId::from_bytes(*body),
                        parts,
                    })
                    .collect();
                append::encode_append(
                    envelope(Opcode::Append),
                    append::Append {
                        authority,
                        partition: PARTITIONS[partition],
                        owner_epoch: 1,
                        key: AppendKey {
                            producer_id: producer_id(index),
                            producer_epoch: producer.epoch,
                            first_sequence,
                        },
                        policy: Policy::QuorumDurable,
                        due_millis: producer.due.as_ref().map(|due| &due[..]),
                        records: &records,
                    },
                    &mut metadata,
                    &mut payload,
                    LIMITS,
                )
                .map_err(|error| format!("producer {index}: append: {error}"))?
            }
        };
        let mut control = Vec::with_capacity(header.len() + metadata.len());
        control.extend_from_slice(&header);
        control.extend_from_slice(&metadata);
        let mut control = Bytes::from(control);
        let header = control.split_to(header.len());
        broker.requests.push_back((
            partition,
            Message::multipart([
                Bytes::copy_from_slice(client(index).as_bytes()),
                header,
                control,
                Bytes::from(payload),
            ]),
        ));
        producer.pending = Some(Request {
            broker: broker.index,
            id,
            sent: tick,
        });
        trace.word(tick);
        trace.word(index as u64 + 100);
        Ok(())
    }

    /// One reply to producer `index`. Every confirmation goes to its
    /// partition's ledger, a late one too.
    pub(crate) fn reply(
        &mut self,
        index: usize,
        broker: usize,
        message: &Message,
        tick: u64,
        ledgers: &mut [Ledger],
        trace: &mut Trace,
    ) -> Result<(), String> {
        let ledger = &mut ledgers[index % self.partitions];
        let dedup = self.dedup(index);
        let frames: [&[u8]; 3] =
            std::array::from_fn(|frame| message.part_slice(frame + 1).unwrap_or_default());
        let packet = decode_packet(&frames, LIMITS.envelope)
            .map_err(|error| format!("producer {index}: reply from broker {broker}: {error}"))?;
        let current = self.writers[index]
            .pending
            .is_some_and(|request| packet.envelope.request_id == Some(request.id));
        trace.word(tick);
        trace.word(packet.envelope.opcode as u64);
        match packet.envelope.opcode {
            Opcode::Appended => {
                let confirmed = append::stream::decode_confirmed(packet, LIMITS.envelope)
                    .map_err(|error| format!("producer {index}: appended: {error}"))?;
                let key = confirmed.key;
                if key.producer_id != producer_id(index) {
                    return Err(format!("producer {index}: confirmation for {key:?}"));
                }
                let base = *self.writers[index]
                    .bases
                    .get(&key.producer_epoch)
                    .ok_or_else(|| format!("producer {index}: confirmation for {key:?}"))?;
                let records = base + key.first_sequence..base + confirmed.end_sequence;
                let scheduled = confirmed.disposition == AppendDisposition::Scheduled;
                if let Some(record) = records
                    .clone()
                    .find(|&record| self.due(index, record).is_some() != scheduled)
                {
                    return Err(format!(
                        "producer {index}: record {record} confirmed as {:?}",
                        confirmed.disposition
                    ));
                }
                match confirmed.disposition {
                    AppendDisposition::Scheduled => {
                        ledger.schedule(index, records.clone(), confirmed.first_offset)?;
                    }
                    AppendDisposition::Duplicate if !dedup => {
                        return Err(format!(
                            "producer {index}: {key:?} found stored on a partition without dedup"
                        ));
                    }
                    disposition => {
                        if disposition == AppendDisposition::Duplicate {
                            self.counts.duplicates += records.end - records.start;
                        }
                        ledger.confirm(
                            index,
                            records.clone(),
                            confirmed.first_offset,
                            |record| self.repeat_from(index, record),
                            wall(tick),
                        )?;
                    }
                }
                let producer = &mut self.writers[index];
                producer.view = producer.view.max(confirmed.authority.view);
                // A restarted process never sees its predecessor's replies.
                if key.producer_epoch == producer.epoch && !producer.takeover {
                    // It committed after the batch's first send.
                    let batch = records.start - records.start % RECORDS;
                    if let Some(batch) = producer.batches.get(&batch) {
                        producer.changed = producer.changed.max(batch.sent);
                    }
                    for record in records {
                        if let Some(done) = record
                            .checked_sub(producer.first)
                            .and_then(|at| producer.done.get_mut(at as usize))
                        {
                            *done = true;
                        }
                    }
                }
                if producer.done.iter().all(|done| *done) {
                    producer.first += RECORDS;
                    plan(&mut self.rng, producer, tick, !dedup);
                    producer.done = [false; RECORDS as usize];
                    producer.uncertain = false;
                    producer.drain_refusals = 0;
                    producer.pending = None;
                    producer.next_at = tick + self.rng.below(3);
                    if dedup && self.rng.chance(PAUSE) {
                        let idle = ticks(PRODUCER_IDLE);
                        producer.next_at += idle / 2 + self.rng.below(idle);
                    }
                }
            }
            Opcode::ProducerOpened if current => {
                let opened = producer::decode_opened(packet, LIMITS.envelope)
                    .map_err(|error| format!("producer {index}: opened: {error}"))?;
                let slack = self.slack;
                let producer = &mut self.writers[index];
                producer.view = producer.view.max(opened.authority.view);
                let old = producer.epoch;
                let new = opened.epoch != old;
                match producer.phase {
                    Phase::Fence(over) if opened.epoch <= over || opened.next_sequence != 0 => {
                        return Err(format!(
                            "producer {index}: fenced epoch {over} into epoch {} at sequence {}",
                            opened.epoch, opened.next_sequence
                        ));
                    }
                    Phase::Fence(_) => producer.takeover = false,
                    // Only a session that retired resumes under a new epoch.
                    _ if old != 0 && new => {
                        let idle = (tick - producer.changed) * tick_millis();
                        if opened.epoch < old
                            || opened.next_sequence != 0
                            || idle + 2 * slack < PRODUCER_IDLE
                        {
                            return Err(format!(
                                "producer {index}: resumed epoch {} at sequence {} after epoch \
                                 {old}, {idle} ms after the session last changed",
                                opened.epoch, opened.next_sequence
                            ));
                        }
                        self.counts.retired += 1;
                    }
                    _ if new && opened.next_sequence != 0 => {
                        return Err(format!(
                            "producer {index}: opened epoch {} at sequence {}",
                            opened.epoch, opened.next_sequence
                        ));
                    }
                    _ => {}
                }
                if new {
                    producer.bases.insert(opened.epoch, producer.first);
                    producer.changed = producer.changed.max(producer.open_sent.unwrap_or(tick));
                } else if opened.next_sequence > producer.first - producer.bases[&old] + RECORDS {
                    // A resend of the current batch would reuse its results.
                    // Nothing else may have advanced this writer.
                    return Err(format!(
                        "producer {index}: opened at sequence {}, current batch starts at \
                         record {}",
                        opened.next_sequence, producer.first
                    ));
                }
                producer.epoch = opened.epoch;
                producer.pending = None;
                producer.next_at = tick;
                if producer.takeover {
                    producer.reopen(Phase::Fence(opened.epoch));
                } else {
                    producer.phase = Phase::Append;
                }
            }
            Opcode::Nack if current => {
                let refusal = nack::decode(packet, LIMITS.envelope)
                    .map_err(|error| format!("producer {index}: nack: {error:?}"))?;
                let hint = AuthorityHint::decode(refusal.detail).ok();
                let producer = &mut self.writers[index];
                producer.pending = None;
                match refusal.retry {
                    RetryClass::AfterAuthorityRefresh => {
                        self.counts.redirects += 1;
                        match hint
                            .and_then(|hint| Some((hint.authority.view, broker_of(hint.primary)?)))
                        {
                            Some((view, primary)) if view >= producer.view => {
                                producer.view = view;
                                producer.target = primary;
                            }
                            _ => producer.target = other(&mut self.rng, broker),
                        }
                        producer.next_at = tick + REDIRECT;
                    }
                    RetryClass::AfterBackoff => {
                        self.counts.backoffs += 1;
                        if self.draining && producer.unresolved() {
                            producer.drain_refusals += 1;
                            producer.abandoned = producer.drain_refusals >= 5;
                        }
                        producer.next_at = tick + RESEND;
                    }
                    RetryClass::UnknownOutcome | RetryClass::AfterReconnect => {
                        self.counts.unknown += 1;
                        producer.uncertain |= producer.phase == Phase::Append;
                        producer.target = other(&mut self.rng, broker);
                        producer.next_at = tick + RESEND;
                    }
                    // The session retired, or retired between a restart's
                    // resume and its fence: resume a fresh one.
                    RetryClass::Permanent if dedup && refusal.code == FENCED => {
                        producer.reopen(Phase::Resume);
                        producer.next_at = tick + REDIRECT;
                    }
                    RetryClass::Permanent => {
                        return Err(format!(
                            "producer {index}: {:?} refused by broker {broker}: code {} {}",
                            producer.phase, refusal.code, refusal.diagnostic
                        ));
                    }
                }
            }
            // A late reply to an earlier attempt.
            Opcode::ProducerOpened | Opcode::Nack => self.counts.ignored += 1,
            opcode => {
                return Err(format!(
                    "producer {index}: unexpected {opcode:?} from broker {broker}"
                ));
            }
        }
        Ok(())
    }
}

impl Producer {
    /// The current batch may be stored but is not fully confirmed yet.
    fn unresolved(&self) -> bool {
        self.uncertain || self.done.iter().any(|done| *done)
    }

    /// Start a new open, under a new operation ID.
    fn reopen(&mut self, phase: Phase) {
        self.phase = phase;
        self.operation += 1;
        self.open_sent = None;
    }
}

/// The producer a client-bound message goes to.
pub(crate) fn producer(message: &Message) -> Option<usize> {
    let route = message.part_slice(0)?;
    (0..PRODUCERS).find(|&index| route == client(index).as_bytes())
}

/// The link a broker publishes for producer `index` under `session`.
pub(crate) fn link(index: usize, session: LinkSessionId) -> Link {
    Link {
        binding: Binding {
            peer: client(index),
            session,
            kind: Kind::Client,
        },
        send: LIMITS,
        remote: handshake::Parameters::streaming(LIMITS, handshake::PRODUCER)
            .expect("producer parameters"),
    }
}

/// Producer `index`'s client node.
pub(crate) fn client(index: usize) -> NodeId {
    NodeId::from_bytes([NODE + index as u8; 16])
}

fn producer_id(index: usize) -> ProducerId {
    ProducerId::from_bytes([NODE + 0x10 + index as u8; 16])
}

/// The producer and sequence a record's bytes name.
pub(crate) fn decode_body(body: [u8; 16]) -> Option<(usize, u64)> {
    let producer = (0..PRODUCERS).find(|&index| body[0] == NODE + index as u8)?;
    (body[1..8] == [0; 7]).then(|| (producer, u64::from_be_bytes(body[8..].try_into().unwrap())))
}

/// Record bytes and message ID: producer and sequence.
fn body(index: usize, sequence: u64) -> [u8; 16] {
    let mut body = [0; 16];
    body[0] = NODE + index as u8;
    body[8..].copy_from_slice(&sequence.to_be_bytes());
    body
}

/// Producer `index`'s open number `open`.
fn operation_id(index: usize, open: u64) -> OperationId {
    let mut bytes = [NODE + index as u8; 16];
    if open != 0 {
        bytes[8..].copy_from_slice(&open.to_be_bytes());
    }
    OperationId::from_bytes(bytes)
}

/// True Unix time at `tick`.
fn wall(tick: u64) -> u64 {
    clock::wall_millis(clock::TICK * u32::try_from(tick).expect("tick fits u32"))
}

fn tick_millis() -> u64 {
    u64::try_from(clock::TICK.as_millis()).expect("tick fits u64")
}

/// Whole ticks in `millis`.
fn ticks(millis: u64) -> u64 {
    millis / tick_millis()
}

fn request_id(index: usize, attempt: u64) -> RequestId {
    let mut bytes = [NODE + index as u8; 16];
    bytes[8..].copy_from_slice(&attempt.to_be_bytes());
    RequestId::from_bytes(bytes)
}

fn broker_of(primary: NodeId) -> Option<usize> {
    (0..BROKERS).find(|&broker| node(broker) == primary)
}

/// A random broker other than `broker`.
/// Decide whether the batch starting now at `tick` is scheduled, and when
/// each of its records is due.
fn plan(rng: &mut Rng, producer: &mut Producer, tick: u64, scheduling: bool) {
    let start = wall(tick);
    producer.due = (scheduling && rng.chance(SCHEDULED)).then(|| {
        std::array::from_fn(|_| {
            start
                .checked_add_signed(EARLIEST)
                .expect("due time in range")
                + rng.below(SPAN)
        })
    });
    if let Some(due) = producer.due {
        producer.scheduled.extend((producer.first..).zip(due));
    }
}

fn other(rng: &mut Rng, broker: usize) -> usize {
    (broker + 1 + rng.below(BROKERS as u64 - 1) as usize) % BROKERS
}
