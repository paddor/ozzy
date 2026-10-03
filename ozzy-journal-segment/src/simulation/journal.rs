use std::borrow::Cow;
use std::collections::BTreeMap;
use std::ops::Range;

use super::Damage;
use super::io::{Memory, Segment};
use crate::directory::publication::{self, MetadataIo, SyncedOverwrite};
use crate::directory::{NoopObserver, evidence, memory_voting, recovery::recovery_marker};
use crate::{
    CanonicalOperation, CanonicalRecoveryRequirements, ChainPosition, CommitMode, CurrentReference,
    DecodeLimits, DecodedOperation, Digest, DirectoryError, GroupIdentity, LogPosition, Manifest,
    MetadataError, MetadataLimits, OperationLimits, SegmentHeader, SegmentReference, SegmentWriter,
    WriterError, WriterPosition, decode_current, decode_group_identity, decode_manifest,
    decode_segment_header, encode_current, encode_group_identity, encode_manifest, manifest_digest,
    scan_segment,
};
use memory_voting::VoteState;
use ozzy_journal::progress::JournalGeneration;

#[cfg(test)]
mod tests;

const CAPACITY: u64 = 1024 * 1024;

/// Decoded storage state. The scheduler may observe it; it supplies no vote.
#[derive(Debug)]
pub struct Recovered {
    pub manifest: Manifest,
    pub admitted: bool,
    pub operations: Vec<DecodedOperation<'static>>,
}

/// Single-segment adapter for seeded storage/protocol schedules.
#[derive(Debug)]
pub struct Journal {
    memory: Memory,
    identity: GroupIdentity,
    configuration: Vec<u8>,
    manifest: Manifest,
    writer: Option<SegmentWriter<Segment>>,
    positions: BTreeMap<u64, WriterPosition>,
    memory_voting: bool,
    body_encoding: crate::BodyEncoding,
}

impl Journal {
    /// Explicit fresh bootstrap. No disk image is silently replaced.
    pub fn format(
        identity: GroupIdentity,
        configuration: Vec<u8>,
        epoch: u64,
        generation: JournalGeneration,
        max_write: usize,
    ) -> Result<Self, DirectoryError> {
        let memory = Memory::default();
        memory.0.lock().unwrap().max_write = max_write.max(1);
        let mut journal = Self {
            memory,
            identity,
            configuration,
            manifest: Manifest {
                generation: 1,
                parent_generation: 0,
                identity,
                configuration_epoch: epoch,
                commit_mode: CommitMode::External,
                durable_evidence: true,
                promised_view: 0,
                last_normal_view: 0,
                accepted: LogPosition::GENESIS,
                committed: LogPosition::GENESIS,
                checkpoint: None,
                segments: vec![reference(1)],
            },
            writer: None,
            positions: BTreeMap::new(),
            memory_voting: false,
            body_encoding: crate::BodyEncoding::Raw,
        };
        journal
            .memory
            .write_new("identity", &encode_group_identity(identity)?)?;
        journal.memory.sync_file("identity")?;
        journal
            .memory
            .write_new("CONFIGURATION", &journal.configuration)?;
        journal.memory.sync_file("CONFIGURATION")?;
        journal.create_segment(1, generation)?;
        journal.publish_evidence()?;
        journal.publish_manifest(journal.manifest.clone(), false)?;
        Ok(journal)
    }

    /// Fresh bootstrap only: publish the production running marker before RAM votes.
    /// Subsequent opens require drained evidence or explicit nonvoting recovery.
    pub fn enable_memory_voting(&mut self) -> Result<(), DirectoryError> {
        if self.memory_voting
            || position(self.writer.as_ref().unwrap().written_position()) != LogPosition::GENESIS
        {
            return Err(DirectoryError::MemoryHistoryUnproven);
        }
        self.memory_voting = true;
        self.publish_memory_evidence(VoteState::Running)
    }

    /// Shutdown after the adapter has stopped voting and drained all accepted work.
    pub fn drain_memory_voting(&mut self) -> Result<(), DirectoryError> {
        if !self.memory_voting {
            return Err(DirectoryError::ConfigurationMismatch);
        }
        self.publish_view(
            self.manifest.promised_view,
            self.manifest.last_normal_view,
            self.manifest.committed,
        )?;
        self.publish_memory_evidence(VoteState::Drained)
    }

    /// Select the production per-operation encoder, including raw fallback.
    pub fn set_body_encoding(
        &mut self,
        encoding: crate::BodyEncoding,
    ) -> Result<(), DirectoryError> {
        if !encoding.is_supported() {
            return Err(DirectoryError::ConfigurationMismatch);
        }
        self.body_encoding = encoding;
        Ok(())
    }

    /// Physical append only; the caller observes its completion separately.
    pub fn append(&mut self, operations: &[CanonicalOperation<'_>]) -> Result<(), DirectoryError> {
        let position = self
            .writer
            .as_mut()
            .expect("opened journal")
            .append_with_body_encoding(operations, self.body_encoding)?;
        self.positions
            .insert(position.next_chain().next_op_number() - 1, position);
        Ok(())
    }

    /// Exact captured data barrier followed by independent accepted evidence.
    pub fn sync(&mut self, through: u64) -> Result<(), DirectoryError> {
        let writer = self.writer.as_mut().expect("opened journal");
        let position = if writer.durable_position().next_chain().next_op_number() - 1 == through {
            writer.durable_position()
        } else {
            *self
                .positions
                .get(&through)
                .expect("complete append boundary")
        };
        writer.sync_through(position)?;
        self.publish_evidence()
    }

    /// Publish a promise, selected view, or activation floor authorized by the core.
    pub fn publish_view(
        &mut self,
        promised: u64,
        normal: u64,
        committed: LogPosition,
    ) -> Result<(), DirectoryError> {
        let writer = self.writer.as_mut().expect("opened journal");
        writer.sync_through(writer.written_position())?;
        let accepted = position(writer.durable_position());
        let mut next = self.successor();
        next.promised_view = promised;
        next.last_normal_view = normal;
        next.accepted = accepted;
        next.committed = committed;
        self.publish_manifest(next, true)
    }

    /// Install externally selected complete history on a fresh segment.
    pub fn replace(
        &mut self,
        generation: JournalGeneration,
        view: u64,
        committed: LogPosition,
        operations: &[CanonicalOperation<'_>],
    ) -> Result<(), DirectoryError> {
        let segment = self.fresh_segment(generation)?;
        if !operations.is_empty() {
            self.append(operations)?;
        }
        let writer = self.writer.as_mut().unwrap();
        writer.sync_through(writer.written_position())?;
        let accepted = position(writer.durable_position());
        let mut next = self.successor();
        next.segments = vec![reference(segment)];
        next.accepted = accepted;
        next.committed = committed;
        next.promised_view = view;
        next.last_normal_view = view;
        self.publish_manifest(next, true)
    }

    /// Remove voting eligibility without reading or altering damaged payloads.
    pub fn quarantine(&mut self) -> Result<(), DirectoryError> {
        let (manifest, admitted) = self.load_authority()?;
        if !admitted {
            return Err(DirectoryError::ConfigurationMismatch);
        }
        self.manifest = manifest;
        publication::replace_reusable(
            &mut self.memory,
            "CONFIGURATION",
            ".CONFIGURATION.quarantine.tmp",
            &recovery_marker(&self.configuration)?,
            &mut NoopObserver,
        )
    }

    /// Start fresh staging after durable nonvoting admission and a fresh nonce.
    pub fn begin_staging(&mut self, generation: JournalGeneration) -> Result<(), DirectoryError> {
        let (manifest, admitted) = self.load_authority()?;
        if admitted {
            return Err(DirectoryError::ConfigurationMismatch);
        }
        self.manifest = manifest;
        let segment = self.fresh_segment(generation)?;
        let mut next = self.successor();
        next.segments = vec![reference(segment)];
        next.accepted = LogPosition::GENESIS;
        next.committed = LogPosition::GENESIS;
        self.publish_manifest(next, true)
    }

    /// Publish a complete core-authorized replacement after private semantic replay.
    pub fn publish_recovery(
        &mut self,
        view: u64,
        committed: LogPosition,
    ) -> Result<(), DirectoryError> {
        if self.memory.read("CONFIGURATION")? != recovery_marker(&self.configuration)? {
            return Err(DirectoryError::ConfigurationMismatch);
        }
        self.publish_view(view, view, committed)?;
        let operations = self.read_operations()?;
        let mut replay = ozzy_core::state::CanonicalRecovery::new(
            ozzy_core::state::StateLimits::default(),
            128,
            4,
        );
        for operation in operations {
            let body = ozzy_journal::operation::decode_operation_body(
                operation.kind,
                &operation.body,
                OperationLimits::default(),
            )
            .map_err(WriterError::from)?;
            replay
                .apply(
                    operation.op_number,
                    &body,
                    operation.op_number <= committed.op_number,
                )
                .map_err(|error| std::io::Error::other(error.to_string()))?;
        }
        replay
            .finish()
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        publication::replace_reusable(
            &mut self.memory,
            "CONFIGURATION",
            ".CONFIGURATION.recovered.tmp",
            &self.configuration,
            &mut NoopObserver,
        )?;
        if self.memory_voting {
            self.publish_memory_evidence(VoteState::Drained)?;
        }
        Ok(())
    }

    /// Lose volatile bytes and directory changes without completing pending callbacks.
    pub fn crash(&mut self) {
        self.writer = None;
        self.positions.clear();
        self.memory.0.lock().unwrap().crash();
    }

    /// Decode selected bytes and execute the production protected-tail recovery.
    pub fn recover(&mut self, generation: JournalGeneration) -> Result<Recovered, DirectoryError> {
        let (manifest, admitted) = self.load_authority()?;
        self.manifest = manifest;
        if !admitted {
            return self.snapshot();
        }
        let copies = self.evidence(&self.manifest)?;
        let protected = copies.protected(&self.manifest)?;
        if !copies.mirrored() {
            let (record, copy) = copies.repair();
            self.memory
                .write_synced(evidence::NAME, &[copy * evidence::COPY_STRIDE], &record)?;
        }
        let segment = self.manifest.segments[0];
        let writer = SegmentWriter::recover_canonical(
            Segment(self.memory.clone(), segment_name(segment.segment_id)),
            generation,
            segment.first_group_number,
            segment.first_chain,
            DecodeLimits::default(),
            CAPACITY,
            CanonicalRecoveryRequirements {
                operation_limits: OperationLimits::default(),
                protected: [
                    Some(following(protected)?),
                    Some(following(self.manifest.committed)?),
                ],
                configuration_epoch: Some(self.manifest.configuration_epoch),
                promised_view: Some(self.manifest.promised_view),
                discard_damaged_tail: true,
            },
        )?;
        let accepted = position(writer.durable_position());
        self.writer = Some(writer);
        if self.manifest.accepted != accepted {
            let mut next = self.successor();
            next.accepted = accepted;
            self.publish_manifest(next, true)?;
        }
        if self.memory_voting {
            let expected = memory_voting::encode(
                &self.manifest,
                &self.configuration,
                accepted,
                VoteState::Drained,
            )?;
            if self.memory.read(memory_voting::NAME).ok().as_deref() != Some(expected.as_slice()) {
                return Err(DirectoryError::MemoryHistoryUnproven);
            }
            self.publish_memory_evidence(VoteState::Running)?;
        }
        self.snapshot()
    }

    /// Decode a projection after successful physical work. Never used as an oracle.
    pub fn snapshot(&self) -> Result<Recovered, DirectoryError> {
        let admitted = self.memory.inspect("CONFIGURATION")? == self.configuration;
        Ok(Recovered {
            manifest: self.manifest.clone(),
            admitted,
            operations: if admitted {
                self.read_operations()?
            } else {
                Vec::new()
            },
        })
    }

    /// Corrupt an offline file, preserving the result through future crashes.
    pub fn damage(&mut self, name: &str, damage: Damage) {
        assert!(self.writer.is_none(), "damage requires an offline broker");
        let mut disk = self.memory.0.lock().unwrap();
        let id = disk.inode(name).unwrap();
        match damage {
            Damage::Flip(offset) => disk.files[id].stable[offset] ^= 1,
            Damage::ZeroSuffix(offset) => disk.files[id].stable[offset..].fill(0),
            Damage::Truncate(offset) => disk.files[id].stable.truncate(offset),
            Damage::Missing => {
                disk.stable_names.remove(name);
            }
        }
        disk.crash();
    }

    /// Selected payload file for a scoped fault schedule.
    pub fn active_name(&self) -> String {
        segment_name(self.manifest.segments[0].segment_id)
    }

    /// Inject an I/O error before the nth next byte/directory operation.
    pub fn fail_after(&mut self, operations: usize) {
        assert!(operations > 0);
        let mut disk = self.memory.0.lock().unwrap();
        disk.fail_at = Some(disk.trace.len() + operations);
    }

    /// End the injected I/O-failure period when a schedule restores healthy storage.
    pub fn clear_failure(&mut self) {
        self.memory.0.lock().unwrap().fail_at = None;
    }

    /// Negative control: simulate a publisher omitting the directory barrier.
    pub fn omit_directory_sync(&mut self, omit: bool) {
        self.memory.0.lock().unwrap().omit_directory_sync = omit;
    }

    /// Negative control: simulate `DURABLE` writes returning before they are durable.
    pub fn omit_in_place_sync(&mut self, omit: bool) {
        self.memory.0.lock().unwrap().omit_in_place_sync = omit;
    }

    /// Persist only a prefix of an unsynchronized append before the next crash.
    /// Never changes bytes already covered by a successful file barrier.
    pub fn tear_unsynced(&mut self, kept_bytes: usize) {
        let name = self.active_name();
        let mut disk = self.memory.0.lock().unwrap();
        let index = disk.inode(&name).unwrap();
        let file = &mut disk.files[index];
        let kept = file
            .stable
            .len()
            .saturating_add(kept_bytes)
            .min(file.pending.len());
        file.stable = file.pending[..kept].to_vec();
    }

    /// Let selected pending bytes and their required file length survive a crash.
    /// Ranges are absolute file offsets. Does not publish directory entries or
    /// complete a journal barrier; the scheduler may persist ranges in any order.
    /// This chooses a possible crash image, not a writeback syscall's guarantee.
    pub fn persist_range(&mut self, name: &str, range: Range<usize>) -> Result<(), DirectoryError> {
        self.memory.0.lock().unwrap().persist_range(name, range)?;
        Ok(())
    }

    /// Complete active-segment writes not yet covered by a journal barrier.
    /// Range writeback can preserve some bytes without advancing this boundary.
    pub fn unsynced_active_range(&self) -> Range<usize> {
        let writer = self.writer.as_ref().expect("opened journal");
        writer.durable_position().end_offset() as usize
            ..writer.written_position().end_offset() as usize
    }

    /// Trace belongs to the scheduler; it is never protocol evidence.
    pub fn trace(&self) -> Vec<String> {
        self.memory.0.lock().unwrap().trace.clone()
    }

    fn successor(&self) -> Manifest {
        let mut next = self.manifest.clone();
        next.parent_generation = next.generation;
        next.generation += 1;
        next
    }

    fn create_segment(
        &mut self,
        id: u64,
        generation: JournalGeneration,
    ) -> Result<(), DirectoryError> {
        let name = segment_name(id);
        self.memory.write_new(&name, &[])?;
        self.writer = Some(SegmentWriter::initialize_at(
            Segment(self.memory.clone(), name),
            SegmentHeader::new(self.identity.group_id, id, None, Digest::ZERO, CAPACITY)?,
            generation,
            1,
            ChainPosition::GENESIS,
        )?);
        self.memory.sync_parent("segments")?;
        self.positions.clear();
        Ok(())
    }

    fn fresh_segment(&mut self, generation: JournalGeneration) -> Result<u64, DirectoryError> {
        let mut id = self.manifest.segments[0].segment_id;
        for _ in 0..128 {
            id += 1;
            if !self.memory.exists(&segment_name(id))? {
                self.create_segment(id, generation)?;
                return Ok(id);
            }
        }
        Err(DirectoryError::RollProbeLimit { limit: 128 })
    }

    fn publish_memory_evidence(&mut self, state: VoteState) -> Result<(), DirectoryError> {
        let accepted = position(self.writer.as_ref().unwrap().durable_position());
        let bytes = memory_voting::encode(&self.manifest, &self.configuration, accepted, state)?;
        publication::replace_reusable(
            &mut self.memory,
            memory_voting::NAME,
            memory_voting::TEMPORARY,
            &bytes,
            &mut NoopObserver,
        )
    }

    fn publish_evidence(&mut self) -> Result<(), DirectoryError> {
        let accepted = position(self.writer.as_ref().unwrap().durable_position());
        if !self.memory.exists(evidence::NAME)? {
            return publication::replace_evidence(
                &mut self.memory,
                &evidence::image(&self.manifest, accepted)?,
                &mut NoopObserver,
            );
        }
        let copies = self.evidence(&self.manifest)?;
        let (bytes, first) = copies.next(&self.manifest, accepted)?;
        publication::overwrite_evidence(&mut self.memory, first, &bytes, &mut NoopObserver)
    }

    fn evidence(&self, manifest: &Manifest) -> Result<evidence::Copies, DirectoryError> {
        evidence::Copies::decode_file(&self.memory.read(evidence::NAME)?, manifest)
    }

    fn publish_manifest(&mut self, next: Manifest, successor: bool) -> Result<(), DirectoryError> {
        let (next, bytes, digest) = if successor {
            publication::prepare_manifest(
                &mut self.memory,
                &self.manifest,
                next,
                MetadataLimits::default(),
            )?
        } else {
            let bytes = encode_manifest(&next)?;
            let digest = manifest_digest(&bytes, MetadataLimits::default())?;
            (next, bytes, digest)
        };
        publication::install_immutable(
            &mut self.memory,
            &format!("MANIFEST.{}", next.generation),
            &bytes,
            &mut NoopObserver,
        )?;
        let current = encode_current(CurrentReference {
            group_id: self.identity.group_id,
            store_id: self.identity.store_id,
            generation: next.generation,
            manifest_digest: digest,
        })?;
        publication::replace_current(
            &mut self.memory,
            next.generation,
            &current,
            &mut NoopObserver,
        )?;
        self.manifest = next;
        Ok(())
    }

    fn load_authority(&self) -> Result<(Manifest, bool), DirectoryError> {
        if decode_group_identity(&self.memory.read("identity")?)? != self.identity {
            return Err(DirectoryError::IdentityMismatch);
        }
        let current = decode_current(&self.memory.read("CURRENT")?)?;
        let bytes = self
            .memory
            .read(&format!("MANIFEST.{}", current.generation))?;
        let manifest = decode_manifest(&bytes, MetadataLimits::default())?;
        if current.group_id != self.identity.group_id
            || current.store_id != self.identity.store_id
            || manifest.identity != self.identity
            || manifest.generation != current.generation
            || manifest_digest(&bytes, MetadataLimits::default())? != current.manifest_digest
            || !manifest.durable_evidence
            || manifest.segments.len() != 1
            || manifest.checkpoint.is_some()
        {
            return Err(DirectoryError::CurrentMismatch);
        }
        self.evidence(&manifest)?.protected(&manifest)?;
        let header = decode_segment_header(
            &self
                .memory
                .read(&segment_name(manifest.segments[0].segment_id))?,
        )?;
        if header.group_id() != self.identity.group_id
            || header.segment_id() != manifest.segments[0].segment_id
        {
            return Err(DirectoryError::IdentityMismatch);
        }
        let configuration = self.memory.read("CONFIGURATION")?;
        let admitted = configuration == self.configuration;
        if !admitted && configuration != recovery_marker(&self.configuration)? {
            return Err(DirectoryError::ConfigurationMismatch);
        }
        Ok((manifest, admitted))
    }

    fn read_operations(&self) -> Result<Vec<DecodedOperation<'static>>, DirectoryError> {
        let mut bytes = self.memory.inspect(&self.active_name())?;
        if let Some(writer) = &self.writer {
            bytes.truncate(writer.durable_position().end_offset() as usize);
        }
        let scan = scan_segment(&bytes, 1, ChainPosition::GENESIS, DecodeLimits::default())?;
        Ok(scan
            .groups
            .into_iter()
            .flat_map(|group| group.operations)
            .map(|op| DecodedOperation {
                body: Cow::Owned(op.body.into_owned()),
                ..op
            })
            .collect())
    }
}

fn position(position: WriterPosition) -> LogPosition {
    LogPosition {
        op_number: position.next_chain().next_op_number() - 1,
        digest: position.next_chain().previous_digest(),
    }
}

fn segment_name(id: u64) -> String {
    format!("segments/{id}.log")
}

fn reference(id: u64) -> SegmentReference {
    SegmentReference {
        segment_id: id,
        file_generation: 0,
        first_group_number: 1,
        first_chain: ChainPosition::GENESIS,
        capacity: CAPACITY,
        sealed: None,
    }
}

fn following(position: LogPosition) -> Result<ChainPosition, MetadataError> {
    position.validate()?;
    Ok(ChainPosition::new(
        position
            .op_number
            .checked_add(1)
            .ok_or(MetadataError::InvalidLogPosition)?,
        position.digest,
    ))
}
