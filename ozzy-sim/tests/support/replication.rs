//! Test-only adapters. Consensus and application transitions are production code.
//! Model allocations are bounded, but are not a runtime allocation benchmark.

use std::collections::VecDeque;

use ozzy_core::state::{CanonicalImages, CanonicalImagesError, StateLimits};
use ozzy_journal::operation::{
    CanonicalOperation, OperationBody, OperationCodecError, OperationKind, OperationLimits,
    canonical_body_digest, decode_operation_body, encode_operation_body, logical_operation_digest,
};
use ozzy_proto::{GroupId, NodeId};
use ozzy_replication::{
    Admission, Commit, Configuration, Digest, JournalGeneration, NormalReplica, OpNumber,
    PipelineLimits, Prefix, PrepareOk, PreparedOperation, ReplicationError, Scope, SyncTicket,
    WriteTicket,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Operation {
    scope: Scope,
    number: u64,
    previous: Digest,
    kind: OperationKind,
    pub(super) body: Vec<u8>,
}

impl Operation {
    fn canonical(&self) -> CanonicalOperation<'_> {
        CanonicalOperation {
            group_id: self.scope.group_id,
            configuration_epoch: self.scope.configuration_epoch,
            original_view: self.scope.view,
            op_number: self.number,
            previous_digest: self.previous,
            kind: self.kind,
            body: &self.body,
        }
    }

    fn metadata(&self) -> PreparedOperation {
        PreparedOperation::from_verified(&self.canonical(), canonical_body_digest(&self.body))
    }

    fn prefix(&self) -> Prefix {
        Prefix {
            op: OpNumber(self.number),
            digest: logical_operation_digest(&self.canonical()),
        }
    }
}

#[derive(Debug)]
struct PendingWrite {
    ticket: WriteTicket,
    copied: usize,
}

#[derive(Debug)]
pub(super) struct Replica {
    pub(super) core: NormalReplica,
    pub(super) images: CanonicalImages,
    pub(super) accepted: Vec<Operation>,
    pub(super) buffered: Vec<Operation>,
    pub(super) stable: Vec<Operation>,
    writes: VecDeque<PendingWrite>,
    pub(super) alive: bool,
    history_limit: usize,
    limits: PipelineLimits,
}

impl Replica {
    fn new(index: usize, history_limit: usize, limits: PipelineLimits) -> Self {
        Self {
            core: NormalReplica::bootstrap(
                configuration(),
                voter(index),
                JournalGeneration(index as u128 + 1),
                limits,
            )
            .unwrap(),
            images: CanonicalImages::new(
                StateLimits::default(),
                history_limit,
                limits.max_operations,
            ),
            accepted: Vec::new(),
            buffered: Vec::new(),
            stable: Vec::new(),
            writes: VecDeque::new(),
            alive: true,
            history_limit,
            limits,
        }
    }

    pub(super) fn admit(&mut self, operations: &[Operation]) -> Result<(), Error> {
        self.require_alive()?;
        if operations.len() > self.limits.max_operations
            || operations
                .iter()
                .try_fold(0usize, |sum, op| sum.checked_add(op.body.len()))
                .is_none_or(|sum| sum > self.limits.max_body_bytes)
        {
            return Err(Error::Capacity);
        }
        // Decode every body, including duplicates. Validate only new transitions
        // against speculative state; the replication core checks overlap digests.
        let mut transitions = Vec::new();
        for operation in operations {
            let body = decode_operation_body(operation.kind, &operation.body, self.codec_limits())?;
            if operation.number > self.core.snapshot().accepted.op.0 {
                transitions.push((operation.number, body));
            }
        }
        if self.accepted.len() + transitions.len() > self.history_limit {
            return Err(Error::Capacity);
        }
        let mut candidate = self.images.clone();
        let plans = candidate.prepare_group(&transitions)?;
        candidate.install_prepared_group(plans)?;
        let metadata: Vec<_> = operations.iter().map(Operation::metadata).collect();
        match self
            .core
            .prepare(voter(0), configuration().scope(), &metadata)?
        {
            Admission::Duplicate => {}
            Admission::Write { ticket, first_new } => {
                self.accepted.extend_from_slice(&operations[first_new..]);
                self.writes.push_back(PendingWrite { ticket, copied: 0 });
                self.images = candidate;
            }
        }
        Ok(())
    }

    fn codec_limits(&self) -> OperationLimits {
        OperationLimits {
            max_body_bytes: self.limits.max_body_bytes,
            max_payload_bytes: self.limits.max_body_bytes,
            ..OperationLimits::default()
        }
    }

    /// Physical write and completion delivery are separate events.
    pub(super) fn write(&mut self, count: usize) -> Result<(), Error> {
        self.require_alive()?;
        let write = self.writes.front_mut().ok_or(Error::NotWritten)?;
        let start = write.ticket.first().0 as usize - 1 + write.copied;
        let end = start
            .saturating_add(count)
            .min(write.ticket.through().0 as usize);
        self.buffered.extend_from_slice(&self.accepted[start..end]);
        write.copied += end - start;
        Ok(())
    }

    pub(super) fn notify_write(&mut self) -> Result<(), Error> {
        self.require_alive()?;
        let write = self.writes.front().ok_or(Error::NotWritten)?;
        let count = (write.ticket.through().0 - write.ticket.first().0 + 1) as usize;
        if write.copied != count {
            return Err(Error::NotWritten);
        }
        self.core.complete_write(write.ticket)?;
        self.writes.pop_front();
        Ok(())
    }

    pub(super) fn persist(&mut self, ticket: SyncTicket) -> Result<(), Error> {
        self.require_alive()?;
        if ticket.generation() != self.core.snapshot().journal.generation
            || ticket.through() > self.core.snapshot().journal.written
        {
            return Err(Error::NotWritten);
        }
        let through = ticket.through().0 as usize;
        if through > self.stable.len() {
            self.stable
                .extend_from_slice(&self.buffered[self.stable.len()..through]);
        }
        Ok(())
    }

    pub(super) fn notify_sync(&mut self, ticket: SyncTicket) -> Result<(), Error> {
        self.require_alive()?;
        if ticket.generation() != self.core.snapshot().journal.generation
            || ticket.through().0 as usize > self.stable.len()
        {
            return Err(Error::NotPersisted);
        }
        self.core.complete_sync(ticket)?;
        Ok(())
    }

    pub(super) fn drain_disk(&mut self) {
        // Bounded by the admitted operation and body-byte limits.
        for _ in 0..self.limits.max_operations {
            if self.writes.is_empty() {
                break;
            }
            self.write(self.limits.max_operations).unwrap();
            self.notify_write().unwrap();
        }
        assert!(self.writes.is_empty());
        let ticket = self.core.begin_sync().unwrap();
        self.persist(ticket).unwrap();
        self.notify_sync(ticket).unwrap();
    }

    pub(super) fn apply(&mut self) -> Result<(), Error> {
        self.require_alive()?;
        let committed = self.core.snapshot().committed;
        // Stage application before releasing consensus metadata. No mutation if
        // the normal generation was fenced while application was outstanding.
        let mut candidate = self.images.clone();
        candidate.commit_through(committed.op.0)?;
        self.core.apply_through(committed)?;
        self.images = candidate;
        Ok(())
    }

    pub(super) fn power_cut(&mut self) {
        self.core.fence();
        self.alive = false;
        self.buffered.clone_from(&self.stable);
        self.writes.clear();
        // No fake restart constructor: a real recovery protocol must authorize
        // voting again. The old core/images remain diagnostic snapshots only.
    }

    fn require_alive(&self) -> Result<(), Error> {
        if !self.alive {
            return Err(Error::Offline);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
enum Message {
    Prepare(Vec<Operation>),
    Ack(PrepareOk),
    Commit(Commit),
}

#[derive(Debug, Clone)]
struct Packet {
    from: usize,
    to: usize,
    message: Message,
}

#[derive(Debug)]
pub(super) struct Cluster {
    pub(super) replicas: [Replica; 3],
    pub(super) links: [[bool; 3]; 3],
    network: Vec<Packet>,
    packet_limit: usize,
    limits: PipelineLimits,
}

impl Cluster {
    pub(super) fn new(history: usize, operations: usize, bytes: usize, packets: usize) -> Self {
        let limits = PipelineLimits {
            max_operations: operations,
            max_body_bytes: bytes,
        };
        Self {
            replicas: std::array::from_fn(|index| Replica::new(index, history, limits)),
            links: [[true; 3]; 3],
            network: Vec::new(),
            packet_limit: packets,
            limits,
        }
    }

    pub(super) fn propose(&mut self, bodies: &[OperationBody<'_>]) -> Result<(), Error> {
        self.replicas[0].require_alive()?;
        self.replicas[0].core.announcement()?;
        if bodies.len() > self.limits.max_operations {
            return Err(Error::Capacity);
        }
        let mut previous = self.replicas[0].core.snapshot().accepted;
        let mut operations = Vec::new();
        let mut bytes = 0;
        for body in bodies {
            let operation = Operation {
                scope: configuration().scope(),
                number: previous.op.0 + 1,
                previous: previous.digest,
                kind: body.kind(),
                body: encode_operation_body(body, self.replicas[0].codec_limits())?,
            };
            bytes += operation.body.len();
            if bytes > self.limits.max_body_bytes {
                return Err(Error::Capacity);
            }
            previous = operation.prefix();
            operations.push(operation);
        }
        self.replicas[0].admit(&operations)
    }

    pub(super) fn send_prepare(
        &mut self,
        to: usize,
        first: usize,
        count: usize,
    ) -> Result<(), Error> {
        self.replicas[0].require_alive()?;
        self.replicas[0].core.announcement()?;
        if count > self.limits.max_operations {
            return Err(Error::Capacity);
        }
        let operations = self.replicas[0]
            .accepted
            .get(first..first + count)
            .ok_or(Error::Capacity)?;
        if operations.iter().map(|op| op.body.len()).sum::<usize>() > self.limits.max_body_bytes {
            return Err(Error::Capacity);
        }
        self.enqueue(Packet {
            from: 0,
            to,
            message: Message::Prepare(operations.to_vec()),
        })
    }

    pub(super) fn send_ack(&mut self, from: usize) -> Result<(), Error> {
        self.replicas[from].require_alive()?;
        let ack = self.replicas[from].core.acknowledgment()?;
        self.enqueue(Packet {
            from,
            to: 0,
            message: Message::Ack(ack),
        })
    }

    pub(super) fn send_commit(&mut self, to: usize) -> Result<(), Error> {
        self.replicas[0].require_alive()?;
        let commit = self.replicas[0].core.announcement()?;
        self.enqueue(Packet {
            from: 0,
            to,
            message: Message::Commit(commit),
        })
    }

    /// A catch-up response may announce an earlier committed prefix. The core
    /// supplies authority and the upper bound; verified local journal bytes
    /// supply the digest after that metadata has left the in-memory pipeline.
    fn send_commit_through(&mut self, to: usize, through: usize) -> Result<(), Error> {
        self.replicas[0].require_alive()?;
        let mut commit = self.replicas[0].core.announcement()?;
        assert!(through <= commit.committed.op.0 as usize);
        commit.committed = if through == 0 {
            Prefix::GENESIS
        } else {
            self.replicas[0].stable[through - 1].prefix()
        };
        self.enqueue(Packet {
            from: 0,
            to,
            message: Message::Commit(commit),
        })
    }

    fn enqueue(&mut self, packet: Packet) -> Result<(), Error> {
        if self.network.len() >= self.packet_limit {
            return Err(Error::Capacity);
        }
        self.network.push(packet);
        Ok(())
    }

    pub(super) fn queued(&self) -> usize {
        self.network.len()
    }

    pub(super) fn duplicate(&mut self, index: usize) -> Result<(), Error> {
        self.enqueue(self.network[index].clone())
    }

    pub(super) fn drop_packet(&mut self, index: usize) {
        self.network.swap_remove(index);
    }

    pub(super) fn deliver(&mut self, index: usize) -> Result<(), Error> {
        let packet = self.network.swap_remove(index);
        if !self.links[packet.from][packet.to] || !self.replicas[packet.to].alive {
            return Ok(());
        }
        let replica = &mut self.replicas[packet.to];
        match packet.message {
            Message::Prepare(operations) => replica.admit(&operations)?,
            Message::Ack(ack) => replica.core.receive_ack(voter(packet.from), ack)?,
            Message::Commit(commit) => replica.core.receive_commit(voter(packet.from), commit)?,
        }
        Ok(())
    }

    /// Eventually deliver missing bytes and cumulative evidence after fault burst.
    pub(super) fn heal(&mut self) {
        self.links = [[true; 3]; 3];
        self.network.clear();
        self.replicas[0].drain_disk();
        let target = self.replicas[0].accepted.len();
        // Each round admits at most one missing operation per backup. Commit
        // and application free pipeline credits before the next round.
        for _ in 0..=target {
            for backup in 1..3 {
                if !self.replicas[backup].alive {
                    continue;
                }
                let start = self.replicas[backup].accepted.len();
                if start < target {
                    self.send_prepare(backup, start, 1).unwrap();
                    self.deliver(0).unwrap();
                }
                self.replicas[backup].drain_disk();
                self.send_ack(backup).unwrap();
                self.deliver(0).unwrap();
            }
            self.replicas[0].apply().unwrap();
            for backup in 1..3 {
                if self.replicas[backup].alive {
                    let through = self.replicas[backup]
                        .accepted
                        .len()
                        .min(self.replicas[0].core.snapshot().committed.op.0 as usize);
                    self.send_commit_through(backup, through).unwrap();
                    self.deliver(0).unwrap();
                    self.replicas[backup].apply().unwrap();
                }
            }
            self.check();
            if self.replicas.iter().all(|replica| {
                !replica.alive || replica.core.snapshot().applied.op.0 as usize == target
            }) {
                break;
            }
        }
        assert_eq!(
            self.replicas[0].core.snapshot().applied.op.0 as usize,
            target
        );
        for replica in &self.replicas {
            if replica.alive {
                assert_eq!(
                    replica.images.committed(),
                    self.replicas[0].images.committed()
                );
                assert_eq!(
                    replica.images.committed_identities(),
                    self.replicas[0].images.committed_identities()
                );
            }
        }
    }

    /// Independent oracle checks physical copies and application visibility.
    pub(super) fn check(&self) {
        assert!(self.network.len() <= self.packet_limit);
        let primary = &self.replicas[0];
        let committed = primary.core.snapshot().committed.op.0 as usize;
        for (index, replica) in self.replicas.iter().enumerate() {
            assert_eq!(replica.stable, replica.buffered[..replica.stable.len()]);
            assert_eq!(replica.stable, primary.accepted[..replica.stable.len()]);
            if !replica.alive {
                continue;
            }
            let snapshot = replica.core.snapshot();
            assert_eq!(
                snapshot.accepted.op.0,
                replica.images.speculative().revision()
            );
            assert_eq!(snapshot.applied.op.0, replica.images.committed().revision());
            assert!(snapshot.applied.op <= snapshot.committed.op);
            assert!(snapshot.committed.op <= snapshot.journal.durable);
            assert!(snapshot.journal.durable <= snapshot.journal.written);
            assert!(snapshot.journal.written <= snapshot.accepted.op);
            assert!(snapshot.journal.durable.0 as usize <= replica.stable.len());
            assert!(snapshot.journal.written.0 as usize <= replica.buffered.len());
            assert!(replica.buffered.len() <= replica.accepted.len());
            assert_eq!(replica.buffered, replica.accepted[..replica.buffered.len()]);
            assert_eq!(replica.accepted, primary.accepted[..replica.accepted.len()]);
            assert!(snapshot.pending_operations <= self.limits.max_operations);
            assert!(snapshot.pending_body_bytes <= self.limits.max_body_bytes);
            if index != 0 {
                assert!(snapshot.committed.op.0 as usize <= committed);
            }
        }
        if committed > 0 {
            assert!(primary.stable.len() >= committed);
            let matching = self
                .replicas
                .iter()
                .filter(|replica| {
                    replica.stable.len() >= committed
                        && replica.stable[..committed] == primary.accepted[..committed]
                })
                .count();
            assert!(
                matching >= 2,
                "producer commit lacks two physical stable copies"
            );
        }
    }
}

pub(super) fn voter(index: usize) -> NodeId {
    NodeId::from_bytes([index as u8 + 1; 16])
}

fn configuration() -> Configuration {
    Configuration::new(
        GroupId::from_bytes([7; 16]),
        1,
        Digest::from_bytes([8; 32]),
        [voter(0), voter(1), voter(2)],
    )
    .unwrap()
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub(super) enum Error {
    #[error(transparent)]
    Replication(#[from] ReplicationError),
    #[error(transparent)]
    Application(#[from] CanonicalImagesError),
    #[error(transparent)]
    Codec(#[from] OperationCodecError),
    #[error("model capacity exceeded")]
    Capacity,
    #[error("physical write not completed")]
    NotWritten,
    #[error("physical sync not completed")]
    NotPersisted,
    #[error("replica crashed; recovery not implemented")]
    Offline,
}
