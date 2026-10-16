//! Producer entries: typed and wire APPEND batches, producer opens, and the
//! in-place edits a streaming retry makes to them.

use super::{
    AppendBuffer, CanonicalOperation, Descriptor, Digest, JournalError, ProducerSession,
    RequestKind,
};

impl AppendBuffer {
    pub(in crate::replica_journal) fn prepare_producer_append(
        &mut self,
        request: crate::replica_journal::ProducerAppend<'_>,
    ) -> Result<(), JournalError> {
        self.prepare_producer_batch(ozzy_journal::operation::AppendBatch {
            partition: request.partition,
            owner_epoch: request.owner_epoch,
            producer_id: request.producer_id,
            producer_epoch: request.producer_epoch,
            first_sequence: request.first_sequence,
            first_offset: ozzy_proto::Offset::ZERO,
            append_timestamp_millis: 0,
            records: request.records.into(),
        })
    }

    pub(in crate::replica_journal) fn prepare_producer_batch(
        &mut self,
        batch: ozzy_journal::operation::AppendBatch<'_>,
    ) -> Result<(), JournalError> {
        use ozzy_journal::operation::{Append, OperationBody};
        self.check_producer_entry()?;
        let producer = batch.producer_id;
        let body = OperationBody::Append(Append {
            batches: vec![batch],
        });
        self.push_typed_body(&body)?;
        self.request_kind = RequestKind::Producer;
        self.note_producer(producer);
        Ok(())
    }

    /// Key the buffer by producer after pushing one APPEND entry.
    fn note_producer(&mut self, producer: ozzy_proto::ProducerId) {
        self.producer =
            (self.entries.len() == 1 || self.producer == Some(producer)).then_some(producer);
    }

    /// The one producer whose open or APPENDs this buffer holds.
    pub(in crate::replica_journal) fn producer(&self) -> Option<ozzy_proto::ProducerId> {
        self.producer_session
            .as_ref()
            .map(|session| session.request.producer)
            .or(self.producer)
    }

    pub(in crate::replica_journal) fn prepare_producer_open(
        &mut self,
        request: ozzy_proto::producer::Open,
        policy: ozzy_proto::append::Policy,
    ) -> Result<(), JournalError> {
        if !self.is_empty() {
            return Err(JournalError::AppendMismatch);
        }
        let new_epoch = match (request.mode, request.expected_epoch) {
            (ozzy_proto::producer::Mode::Resume | ozzy_proto::producer::Mode::Create, None) => 1,
            (ozzy_proto::producer::Mode::Resume, Some(epoch)) => epoch,
            (ozzy_proto::producer::Mode::Fence, Some(epoch)) => {
                epoch.checked_add(1).ok_or(JournalError::AppendMismatch)?
            }
            (ozzy_proto::producer::Mode::Fence, None)
            | (ozzy_proto::producer::Mode::Create, Some(_)) => {
                return Err(JournalError::AppendMismatch);
            }
        };
        if new_epoch == 0
            || request.expected_epoch == Some(0)
            || request.authority.group_id.as_bytes() == &[0; 16]
            || request.authority.config_epoch == 0
            || request.partition.as_bytes() == &[0; 16]
            || request.operation.as_bytes() == &[0; 16]
            || request.producer.as_bytes() == &[0; 16]
        {
            return Err(JournalError::AppendMismatch);
        }
        self.push_typed_body(&ozzy_journal::operation::OperationBody::OpenProducer(
            ozzy_journal::operation::OpenProducer {
                partition: request.partition,
                producer_id: request.producer,
                expected_epoch: request.expected_epoch.map(ozzy_proto::ProducerEpoch::new),
                new_epoch: ozzy_proto::ProducerEpoch::new(new_epoch),
                operation_id: request.operation,
            },
        ))?;
        self.producer_session = Some(Box::new(ProducerSession {
            request,
            opened: None,
        }));
        self.proposal_authority = Some(request.authority);
        self.proposal_policy = Some(policy);
        Ok(())
    }

    pub(in crate::replica_journal) fn prepare_wire_producer_batch(
        &mut self,
        request: ozzy_proto::append::ValidatedAppend<'_>,
    ) -> Result<(), JournalError> {
        use ozzy_journal::operation::{
            AppendHeader, OperationKind, OperationLimits, append_wire_record_batch,
        };
        self.check_producer_entry()?;
        let room = self.limits.max_body_bytes - self.bodies.len();
        let limits = OperationLimits {
            max_body_bytes: room,
            max_payload_bytes: room,
            max_records: room / 20,
            max_parts: room / 4,
            ..OperationLimits::default()
        };
        let bytes = 80_usize
            .checked_add(request.records.encoded().len())
            .and_then(|bytes| bytes.checked_add(request.encoded_payload.len()))
            .and_then(|bytes| {
                bytes.checked_add(
                    usize::from(
                        request.payload_encoding == ozzy_proto::append::PayloadEncoding::Lz4,
                    ) * 9,
                )
            })
            .ok_or(JournalError::AppendCapacity)?;
        self.reserve_body_bytes(bytes)?;
        let batch = ozzy_journal::operation::WireBatch {
            header: AppendHeader {
                partition: request.partition,
                owner_epoch: ozzy_proto::OwnerEpoch::new(request.owner_epoch),
                producer_id: request.key.producer_id,
                producer_epoch: ozzy_proto::ProducerEpoch::new(request.key.producer_epoch),
                first_sequence: ozzy_proto::ProducerSequence::new(request.key.first_sequence),
                first_offset: ozzy_proto::Offset::ZERO,
                append_timestamp_millis: 0,
            },
            records: request.records,
            encoding: request.payload_encoding,
            payload: request.encoded_payload,
        };
        let (range, proof) = append_wire_record_batch(self.bodies.mutable()?, batch, limits)?;
        self.entries.push(Descriptor {
            envelope: CanonicalOperation {
                group_id: ozzy_proto::GroupId::from_bytes([0; 16]),
                configuration_epoch: 0,
                original_view: 0,
                op_number: 0,
                previous_digest: Digest::ZERO,
                kind: OperationKind::Append,
                body: &[],
            },
            body: range,
            digest: None,
            primary_payload_validated: false,
            proof: Some(proof),
        });
        self.request_kind = RequestKind::Producer;
        self.note_producer(request.key.producer_id);
        Ok(())
    }

    /// Remove a verified streaming retry prefix in place. Only the newly
    /// accepted suffix becomes a canonical operation. No replacement arena.
    pub(in crate::replica_journal) fn trim_producer_prefix(
        &mut self,
        count: usize,
        limits: ozzy_journal::operation::OperationLimits,
    ) -> Result<(), JournalError> {
        use ozzy_journal::operation::decode_append_view;
        self.validated = false;
        if self.entries.len() != 1 {
            return Err(JournalError::AppendMismatch);
        }
        let mut batches = decode_append_view(&self.bodies, limits)?.batches();
        let batch = batches.next().ok_or(JournalError::AppendMismatch)?;
        if batches.next().is_some()
            || count == 0
            || count >= batch.summary.record_count
            || !self.producer_stream
        {
            return Err(JournalError::AppendMismatch);
        }
        if batch.prepared_payload().is_some() {
            return self.trim_prepared_prefix(count, limits);
        }
        let remaining = (batch.summary.record_count - count) as u32;
        let sequence = batch
            .summary
            .first_sequence
            .get()
            .checked_add(count as u64)
            .ok_or(JournalError::AppendMismatch)?;
        let mut records = batch.raw_records().ok_or(JournalError::AppendMismatch)?;
        let (descriptors, payload) = records.remaining_bytes();
        let descriptor_bytes = descriptors.len();
        let length_bytes = records.tiny_lengths().map_or(0, <[u8]>::len);
        let payload_bytes = payload.len();
        records.nth(count - 1).ok_or(JournalError::AppendMismatch)?;
        let (descriptors, payload) = records.remaining_bytes();
        let kept_descriptors = descriptors.len();
        let kept_lengths = records.tiny_lengths().map_or(0, <[u8]>::len);
        let kept_payload = payload.len();
        let descriptor_start = 80 + descriptor_bytes - kept_descriptors;
        let length_start = 80 + descriptor_bytes + length_bytes - kept_lengths;
        let payload_start = 80 + descriptor_bytes + length_bytes + payload_bytes - kept_payload;
        let end = 80 + kept_descriptors + kept_lengths + kept_payload;
        self.bodies.reserve(0)?;
        // Move IDs/descriptors, optional compact lengths, and payload separately.
        self.bodies
            .mutable()?
            .copy_within(descriptor_start..80 + descriptor_bytes, 80);
        self.bodies.mutable()?.copy_within(
            length_start..80 + descriptor_bytes + length_bytes,
            80 + kept_descriptors,
        );
        self.bodies
            .mutable()?
            .copy_within(payload_start.., 80 + kept_descriptors + kept_lengths);
        self.bodies.truncate(end)?;
        self.bodies.mutable()?[52..60].copy_from_slice(&sequence.to_be_bytes());
        let remaining = remaining | if length_bytes != 0 { 1 << 31 } else { 0 };
        self.bodies.mutable()?[76..80].copy_from_slice(&remaining.to_be_bytes());
        self.entries[0].body.end = end;
        self.entries[0].proof = None;
        Ok(())
    }

    // One-batch canonical header: count, partition, owner, producer, epoch,
    // sequence, then offset and timestamp. Called only after full typed decoding.
    pub(in crate::replica_journal) fn replace_append_position(
        &mut self,
        index: usize,
        position: [u8; 16],
    ) -> Result<[u8; 16], JournalError> {
        const START: usize = 4 + 16 + 8 + 16 + 8 + 8;
        self.validated = false;
        assert!(self.is_producer() && index < self.len());
        let start = self.entries[index].body.start + START;
        let target: &mut [u8; 16] = self
            .bodies
            .mutable()?
            .get_mut(start..start + 16)
            .and_then(|field| field.try_into().ok())
            .ok_or(JournalError::AppendMismatch)?;
        Ok(std::mem::replace(target, position))
    }

    pub(in crate::replica_journal) fn validated_producer_summary(
        &self,
        index: usize,
    ) -> Option<ozzy_journal::operation::AppendBatchSummary> {
        let entry = self.entries.get(index)?;
        entry.proof?.summary(&self.bodies[entry.body.clone()]).ok()
    }
}
