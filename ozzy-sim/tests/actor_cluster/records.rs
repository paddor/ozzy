//! Producers append records through each broker's production native intake,
//! as an SDK writer does over its links: `OPEN_PRODUCER`, then one APPEND of
//! `RECORDS` records at a time. A NACK's authority hint redirects; an unknown
//! outcome or a silent broker resends the same identity elsewhere later.
//!
//! Every confirmation goes to the `Ledger`, stale ones too: two records at
//! one offset, one record at two offsets, or a producer's records out of
//! order fail the run. At the end, every copy's log must hold exactly the
//! confirmed records, at offsets from zero without a gap.

use crate::{
    broker::{Broker, GROUP},
    network::{BROKERS, node},
    seeds::{Rng, Trace},
};
use bytes::Bytes;
use omq_tokio::Message;
use ozzy_proto::{
    Envelope, EnvelopeLimits, LinkSessionId, MessageId, NodeId, Opcode, OperationId,
    PartitionIncarnation, ProducerId, RequestId,
    append::{self, AppendKey, Authority, Policy},
    data::{DataLimits, Encoding, Record},
    decode_packet, handshake,
    nack::{self, AuthorityHint, RetryClass},
    producer::{self, Mode, Open},
};
use ozzy_runtime::frontend::{Binding, Kind, Link};
use std::collections::BTreeMap;

/// Producers, each with its own client link to every broker.
pub(crate) const PRODUCERS: usize = 2;
/// The one partition every broker provisions.
pub(crate) const PARTITION: PartitionIncarnation = PartitionIncarnation::from_bytes([8; 16]);
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
/// Ticks without a reply before a request is resent elsewhere, and before a
/// retry after a refusal.
const TIMEOUT: u64 = 300;
const RESEND: u64 = 100;
const REDIRECT: u64 = 10;
/// The first byte of every producer node ID.
const NODE: u8 = 0xc0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Open,
    Append,
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
    /// First sequence of the current batch.
    first: u64,
    /// Sequences of the current batch confirmed so far.
    done: [bool; RECORDS as usize],
    /// An attempt at the current batch has an unknown outcome, so the batch
    /// may be stored. A refusal says nothing about earlier attempts.
    uncertain: bool,
    /// Refusals of an unresolved batch while draining. A full journal
    /// refuses its resends for good, so the batch stays unknown.
    drain_refusals: u32,
    abandoned: bool,
    /// End of every sequence sent so far.
    sent_end: u64,
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
}

/// What one offset holds, as confirmations and reads saw it.
#[derive(Debug, Clone, Copy)]
struct Entry {
    record: (usize, u64),
    confirmed: bool,
    read: bool,
}

/// Every confirmed and every read record, checked as it arrives: one
/// offset per record, one record per offset, and each producer's records in
/// sequence order.
#[derive(Debug, Default)]
pub(crate) struct Ledger {
    /// Offset of each record, by producer and sequence.
    records: BTreeMap<(usize, u64), u64>,
    offsets: BTreeMap<u64, Entry>,
    confirmed: usize,
}

impl Ledger {
    /// Distinct confirmed records.
    pub(crate) fn len(&self) -> usize {
        self.confirmed
    }

    fn confirm(
        &mut self,
        producer: usize,
        sequences: std::ops::Range<u64>,
        first_offset: u64,
    ) -> Result<(), String> {
        for sequence in sequences.clone() {
            let offset = first_offset + (sequence - sequences.start);
            self.note(producer, sequence, offset, true)?;
        }
        Ok(())
    }

    /// A consumer read producer `producer`'s record `sequence` at `offset`.
    pub(crate) fn read(
        &mut self,
        producer: usize,
        sequence: u64,
        offset: u64,
    ) -> Result<(), String> {
        self.note(producer, sequence, offset, false)
    }

    fn note(
        &mut self,
        producer: usize,
        sequence: u64,
        offset: u64,
        confirmed: bool,
    ) -> Result<(), String> {
        let how = if confirmed { "confirmed" } else { "read" };
        if let Some(&old) = self.records.get(&(producer, sequence))
            && old != offset
        {
            return Err(format!(
                "producer {producer} sequence {sequence} {how} at offset {offset}, seen at {old} \
                 before"
            ));
        }
        if let Some(entry) = self.offsets.get(&offset)
            && entry.record != (producer, sequence)
        {
            return Err(format!(
                "offset {offset} {how} for producer {producer} sequence {sequence}, seen with {:?} \
                 before",
                entry.record
            ));
        }
        // One producer's records keep their sequence order.
        let before = self
            .records
            .range((producer, 0)..(producer, sequence))
            .next_back();
        let after = self
            .records
            .range((producer, sequence + 1)..(producer + 1, 0))
            .next();
        if before.is_some_and(|(_, &earlier)| earlier >= offset)
            || after.is_some_and(|(_, &later)| later <= offset)
        {
            return Err(format!(
                "producer {producer} sequence {sequence} {how} at offset {offset}, out of order \
                 with {before:?} and {after:?}"
            ));
        }
        self.records.insert((producer, sequence), offset);
        let entry = self.offsets.entry(offset).or_insert(Entry {
            record: (producer, sequence),
            confirmed: false,
            read: false,
        });
        if confirmed && !entry.confirmed {
            self.confirmed += 1;
        }
        entry.confirmed |= confirmed;
        entry.read |= !confirmed;
        Ok(())
    }

    /// At the end, on a copy whose log ends at `end`: consumers read every
    /// offset below it, each confirmed record is among them, and a record
    /// nobody confirmed comes from a batch whose outcome stayed unknown.
    pub(crate) fn check_log(
        &self,
        broker: usize,
        end: u64,
        read_through: u64,
        producers: &Producers,
    ) -> Result<(), String> {
        if let Some((&offset, entry)) = self.offsets.range(end..).next() {
            return Err(format!(
                "broker {broker}: log ends at offset {end}, but offset {offset} holds {:?} \
                 (confirmed {}, read {})",
                entry.record, entry.confirmed, entry.read
            ));
        }
        if read_through != end {
            return Err(format!(
                "broker {broker}: log ends at offset {end}, consumers read through {read_through}"
            ));
        }
        if self.offsets.len() as u64 != end {
            return Err(format!(
                "broker {broker}: log ends at offset {end}, but {} offsets were seen",
                self.offsets.len()
            ));
        }
        for (&offset, entry) in &self.offsets {
            let (producer, sequence) = entry.record;
            if !entry.read {
                return Err(format!(
                    "broker {broker}: confirmed offset {offset} was never read"
                ));
            }
            if !entry.confirmed && !producers.may_hold(producer, sequence) {
                return Err(format!(
                    "broker {broker}: offset {offset} holds producer {producer} sequence \
                     {sequence}, never confirmed though its batch was settled"
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct Producers {
    writers: Vec<Producer>,
    rng: Rng,
    /// No new batches: finish the current ones.
    draining: bool,
    pub counts: Counts,
}

impl Producers {
    pub(crate) fn new(seed: u64) -> Self {
        Self {
            writers: (0..PRODUCERS)
                .map(|index| Producer {
                    phase: Phase::Open,
                    epoch: 0,
                    first: 0,
                    done: [false; RECORDS as usize],
                    uncertain: false,
                    drain_refusals: 0,
                    abandoned: false,
                    sent_end: 0,
                    target: index % BROKERS,
                    view: 0,
                    pending: None,
                    next_at: 0,
                    attempts: 0,
                })
                .collect(),
            rng: Rng::new(seed),
            draining: false,
            counts: Counts::default(),
        }
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
        for index in 0..PRODUCERS {
            let producer = &mut self.writers[index];
            if let Some(request) = producer.pending
                && tick >= request.sent + TIMEOUT
            {
                // No reply: the broker may have stopped, or taken it and
                // gone silent. Resend the same identity elsewhere.
                self.counts.timeouts += 1;
                producer.pending = None;
                producer.uncertain = producer.phase == Phase::Append;
                producer.target = other(&mut self.rng, request.broker);
                producer.next_at = tick;
            }
            let producer = &mut self.writers[index];
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
            group_id: GROUP,
            config_epoch: 1,
            view: producer.view,
        };
        let mut metadata = Vec::with_capacity(LIMITS.envelope.max_metadata_bytes);
        let mut payload = Vec::with_capacity(LIMITS.envelope.max_payload_bytes);
        let header = match producer.phase {
            Phase::Open => {
                self.counts.opens += 1;
                producer::encode_open(
                    envelope(Opcode::OpenProducer),
                    Open {
                        authority,
                        partition: PARTITION,
                        producer: producer_id(index),
                        mode: Mode::Resume,
                        expected_epoch: None,
                        operation: OperationId::from_bytes([NODE + index as u8; 16]),
                    },
                    &mut metadata,
                    LIMITS.envelope,
                )
                .map_err(|error| format!("producer {index}: open: {error}"))?
            }
            Phase::Append => {
                self.counts.appends += 1;
                producer.sent_end = producer.sent_end.max(producer.first + RECORDS);
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
                        partition: PARTITION,
                        owner_epoch: 1,
                        key: AppendKey {
                            producer_id: producer_id(index),
                            producer_epoch: producer.epoch,
                            first_sequence: producer.first,
                        },
                        policy: Policy::QuorumDurable,
                        due_millis: None,
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
        broker.requests.push_back(Message::multipart([
            Bytes::copy_from_slice(client(index).as_bytes()),
            header,
            control,
            Bytes::from(payload),
        ]));
        producer.pending = Some(Request {
            broker: broker.index,
            id,
            sent: tick,
        });
        trace.word(tick);
        trace.word(index as u64 + 100);
        Ok(())
    }

    /// One reply to producer `index`. Every confirmation goes to `ledger`,
    /// a late one too.
    pub(crate) fn reply(
        &mut self,
        index: usize,
        broker: usize,
        message: &Message,
        tick: u64,
        ledger: &mut Ledger,
        trace: &mut Trace,
    ) -> Result<(), String> {
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
                ledger.confirm(
                    index,
                    key.first_sequence..confirmed.end_sequence,
                    confirmed.first_offset,
                )?;
                let producer = &mut self.writers[index];
                producer.view = producer.view.max(confirmed.authority.view);
                if key.producer_epoch == producer.epoch {
                    for sequence in key.first_sequence..confirmed.end_sequence {
                        if let Some(done) = sequence
                            .checked_sub(producer.first)
                            .and_then(|at| producer.done.get_mut(at as usize))
                        {
                            *done = true;
                        }
                    }
                }
                if producer.done.iter().all(|done| *done) {
                    producer.first += RECORDS;
                    producer.done = [false; RECORDS as usize];
                    producer.uncertain = false;
                    producer.drain_refusals = 0;
                    producer.pending = None;
                    producer.next_at = tick + self.rng.below(3);
                }
            }
            Opcode::ProducerOpened if current => {
                let opened = producer::decode_opened(packet, LIMITS.envelope)
                    .map_err(|error| format!("producer {index}: opened: {error}"))?;
                let producer = &mut self.writers[index];
                producer.view = producer.view.max(opened.authority.view);
                if producer.epoch != 0 && opened.epoch != producer.epoch {
                    return Err(format!(
                        "producer {index}: resumed epoch {} after {}",
                        opened.epoch, producer.epoch
                    ));
                }
                if opened.next_sequence > producer.first {
                    // A resend of the current batch would reuse its results.
                    // Nothing else may have advanced this writer.
                    if opened.next_sequence > producer.first + RECORDS {
                        return Err(format!(
                            "producer {index}: opened at sequence {}, current batch starts at {}",
                            opened.next_sequence, producer.first
                        ));
                    }
                }
                producer.epoch = opened.epoch;
                producer.phase = Phase::Append;
                producer.pending = None;
                producer.next_at = tick;
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
                        producer.uncertain = producer.phase == Phase::Append;
                        producer.target = other(&mut self.rng, broker);
                        producer.next_at = tick + RESEND;
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
        self.phase == Phase::Append && (self.uncertain || self.done.iter().any(|done| *done))
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

fn request_id(index: usize, attempt: u64) -> RequestId {
    let mut bytes = [NODE + index as u8; 16];
    bytes[8..].copy_from_slice(&attempt.to_be_bytes());
    RequestId::from_bytes(bytes)
}

fn broker_of(primary: NodeId) -> Option<usize> {
    (0..BROKERS).find(|&broker| node(broker) == primary)
}

/// A random broker other than `broker`.
fn other(rng: &mut Rng, broker: usize) -> usize {
    (broker + 1 + rng.below(BROKERS as u64 - 1) as usize) % BROKERS
}
