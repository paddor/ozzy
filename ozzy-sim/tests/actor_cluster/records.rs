//! Producers append records through each broker's production native intake,
//! as an SDK writer does over its links: `OPEN_PRODUCER`, then one APPEND of
//! `RECORDS` records at a time. A NACK's authority hint redirects; an unknown
//! outcome or a silent broker resends the same identity elsewhere later.
//!
//! A third of the batches are scheduled, each record due between 0.5 s ago
//! and 3 s ahead by true time; their confirmations carry schedule sequences.
//!
//! Every confirmation goes to the `Ledger`, stale ones too: two records at
//! one offset or schedule sequence, one record at two, or a producer's
//! unscheduled records out of order fail the run. Scheduled records release
//! in order of due time, then schedule sequence, and never before their due
//! time minus the clock tolerance. At the end, every copy's log must hold
//! exactly the confirmed records, at offsets from zero without a gap.

use crate::{
    broker::{Broker, CLOCK_TOLERANCE, GROUPS, PARTITIONS},
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
    /// Due times of the current batch, when it is scheduled.
    due: Option<[u64; RECORDS as usize]>,
    /// Due time of every scheduled sequence so far.
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
}

/// What one offset holds, as confirmations and reads saw it.
#[derive(Debug, Clone, Copy)]
struct Entry {
    record: (usize, u64),
    confirmed: bool,
    read: bool,
}

/// Every confirmed and every read record, checked as it arrives: one
/// offset and schedule sequence per record, one record per offset and
/// schedule sequence, and each producer's unscheduled records in sequence
/// order.
#[derive(Debug, Default)]
pub(crate) struct Ledger {
    /// Offset of each record, by producer and sequence, and whether it was
    /// scheduled.
    records: BTreeMap<(usize, u64), (u64, bool)>,
    offsets: BTreeMap<u64, Entry>,
    confirmed: usize,
    /// Schedule sequence of each confirmed scheduled record, and the reverse.
    schedules: BTreeMap<(usize, u64), u64>,
    scheduled: BTreeMap<u64, (usize, u64)>,
}

impl Ledger {
    /// Distinct confirmed records.
    pub(crate) fn len(&self) -> usize {
        self.confirmed + self.schedules.len()
    }

    /// Confirmed scheduled records.
    pub(crate) fn scheduled(&self) -> usize {
        self.schedules.len()
    }

    /// Every confirmed scheduled record was read.
    pub(crate) fn released(&self) -> bool {
        self.schedules
            .keys()
            .all(|record| self.records.contains_key(record))
    }

    fn confirm(
        &mut self,
        producer: usize,
        sequences: std::ops::Range<u64>,
        first_offset: u64,
    ) -> Result<(), String> {
        for sequence in sequences.clone() {
            let offset = first_offset + (sequence - sequences.start);
            self.note(producer, sequence, offset, true, false)?;
        }
        Ok(())
    }

    /// Records held until due, confirmed with schedule sequences from
    /// `first_schedule`.
    fn schedule(
        &mut self,
        producer: usize,
        sequences: std::ops::Range<u64>,
        first_schedule: u64,
    ) -> Result<(), String> {
        for sequence in sequences.clone() {
            let schedule = first_schedule + (sequence - sequences.start);
            let record = (producer, sequence);
            if let Some(&old) = self.schedules.get(&record)
                && old != schedule
            {
                return Err(format!(
                    "producer {producer} sequence {sequence} scheduled as {schedule}, as {old} \
                     before"
                ));
            }
            if let Some(&other) = self.scheduled.get(&schedule)
                && other != record
            {
                return Err(format!(
                    "schedule sequence {schedule} confirmed for producer {producer} sequence \
                     {sequence}, for {other:?} before"
                ));
            }
            self.schedules.insert(record, schedule);
            self.scheduled.insert(schedule, record);
        }
        Ok(())
    }

    /// A consumer read producer `producer`'s record `sequence` at `offset`
    /// at true time `wall`. A scheduled record, due at `due`, released at
    /// most the clock tolerance early.
    pub(crate) fn read(
        &mut self,
        producer: usize,
        sequence: u64,
        offset: u64,
        due: Option<u64>,
        wall: u64,
    ) -> Result<(), String> {
        if let Some(due) = due
            && due > wall + CLOCK_TOLERANCE
        {
            return Err(format!(
                "producer {producer} sequence {sequence} read at offset {offset} {} ms before \
                 its due time",
                due - wall
            ));
        }
        self.note(producer, sequence, offset, false, due.is_some())
    }

    fn note(
        &mut self,
        producer: usize,
        sequence: u64,
        offset: u64,
        confirmed: bool,
        scheduled: bool,
    ) -> Result<(), String> {
        let how = if confirmed { "confirmed" } else { "read" };
        if let Some(&(old, _)) = self.records.get(&(producer, sequence))
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
        // One producer's unscheduled records keep their sequence order.
        let unscheduled = |(_, (_, scheduled)): &(&(usize, u64), &(u64, bool))| !*scheduled;
        let before = self
            .records
            .range((producer, 0)..(producer, sequence))
            .rev()
            .find(unscheduled);
        let after = self
            .records
            .range((producer, sequence + 1)..(producer + 1, 0))
            .find(unscheduled);
        if !scheduled
            && (before.is_some_and(|(_, &(earlier, _))| earlier >= offset)
                || after.is_some_and(|(_, &(later, _))| later <= offset))
        {
            return Err(format!(
                "producer {producer} sequence {sequence} {how} at offset {offset}, out of order \
                 with {before:?} and {after:?}"
            ));
        }
        self.records
            .insert((producer, sequence), (offset, scheduled));
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
    /// offset below it, each confirmed record is among them, scheduled ones
    /// in release order, and a record nobody confirmed comes from a batch
    /// whose outcome stayed unknown.
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
            let confirmed = entry.confirmed || self.schedules.contains_key(&entry.record);
            if !confirmed && !producers.may_hold(producer, sequence) {
                return Err(format!(
                    "broker {broker}: offset {offset} holds producer {producer} sequence \
                     {sequence}, never confirmed though its batch was settled"
                ));
            }
        }
        self.check_releases(broker, producers)
    }

    /// Every confirmed scheduled record was released, in order of due time,
    /// then schedule sequence: a later schedule sequence that is due no
    /// later never releases first. Acceptance times only grow with the
    /// schedule sequence, so they cannot reverse that.
    fn check_releases(&self, broker: usize, producers: &Producers) -> Result<(), String> {
        let mut released = Vec::new();
        for (&(producer, sequence), &schedule) in &self.schedules {
            let Some(&(offset, _)) = self.records.get(&(producer, sequence)) else {
                return Err(format!(
                    "broker {broker}: producer {producer} sequence {sequence}, confirmed as \
                     schedule {schedule}, was never released"
                ));
            };
            let due = producers.due(producer, sequence).ok_or_else(|| {
                format!("producer {producer} sequence {sequence} has no due time")
            })?;
            released.push((offset, due, schedule));
        }
        released.sort_unstable();
        for (at, &(offset, due, schedule)) in released.iter().enumerate() {
            if let Some(&(later, later_due, later_schedule)) =
                released[at + 1..]
                    .iter()
                    .find(|(_, later_due, later_schedule)| {
                        *later_schedule < schedule && *later_due <= due
                    })
            {
                return Err(format!(
                    "broker {broker}: schedule {later_schedule} due {later_due} released at offset \
                     {later}, after schedule {schedule} due {due} at offset {offset}"
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct Producers {
    writers: Vec<Producer>,
    partitions: usize,
    rng: Rng,
    /// No new batches: finish the current ones.
    draining: bool,
    pub counts: Counts,
}

impl Producers {
    pub(crate) fn new(seed: u64, partitions: usize) -> Self {
        let mut producers = Self {
            writers: (0..PER_PARTITION * partitions)
                .map(|index| Producer {
                    phase: Phase::Open,
                    epoch: 0,
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
            counts: Counts::default(),
        };
        for producer in &mut producers.writers {
            plan(&mut producers.rng, producer, 0);
        }
        producers
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
            Phase::Open => {
                self.counts.opens += 1;
                producer::encode_open(
                    envelope(Opcode::OpenProducer),
                    Open {
                        authority,
                        partition: PARTITIONS[partition],
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
                        partition: PARTITIONS[partition],
                        owner_epoch: 1,
                        key: AppendKey {
                            producer_id: producer_id(index),
                            producer_epoch: producer.epoch,
                            first_sequence: producer.first,
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
                let sequences = key.first_sequence..confirmed.end_sequence;
                let scheduled = confirmed.disposition == AppendDisposition::Scheduled;
                if let Some(sequence) = sequences
                    .clone()
                    .find(|&sequence| self.due(index, sequence).is_some() != scheduled)
                {
                    return Err(format!(
                        "producer {index}: sequence {sequence} confirmed as {:?}",
                        confirmed.disposition
                    ));
                }
                if scheduled {
                    ledger.schedule(index, sequences, confirmed.first_offset)?;
                } else {
                    ledger.confirm(index, sequences, confirmed.first_offset)?;
                }
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
                    plan(&mut self.rng, producer, tick);
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
/// Decide whether the batch starting now at `tick` is scheduled, and when
/// each of its records is due.
fn plan(rng: &mut Rng, producer: &mut Producer, tick: u64) {
    let start = clock::wall_millis(clock::TICK * u32::try_from(tick).expect("tick fits u32"));
    producer.due = rng.chance(SCHEDULED).then(|| {
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
