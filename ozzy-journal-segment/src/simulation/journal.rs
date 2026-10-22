use std::borrow::Cow;
use std::collections::BTreeMap;
use std::future::Future;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::task::{Context, Poll, Waker};

use super::Damage;
use crate::async_files::Access;
use crate::directory::{CONFIGURATION_FILE, evidence, segment_reference_name};
use crate::{
    BodyEncoding, CanonicalOperation, CanonicalRecoveryLimits, CheckpointLimits, CommitMode,
    DecodeLimits, DecodedOperation, Digest, DirectoryError, GroupIdentity, GroupJournal,
    JournalFormat, JournalLimits, LogPosition, Manifest, MetadataLimits, OperationLimits,
    RecoveryDirectory, RecoveryPublication, SegmentHeader, SegmentOptions, SegmentWriteMode,
    SuffixReplacement, SuffixStreamLimits, WriterPosition, logical_operation_digest, scan_segment,
};
use ozzy_io::simulation::{self, Controller, Effect, Image, ImageLimits};
use ozzy_io::{Local, OpenMode, Operation, Quota, SyncMode};
use ozzy_journal::progress::JournalGeneration;

#[cfg(test)]
mod tests;

const CAPACITY: u64 = 1024 * 1024;
const ROOT: &str = "/group";
const CHUNK_OPERATIONS: usize = 16;
const ORPHAN_PROBES: usize = 128;

/// Decoded storage state. The scheduler may observe it; it supplies no vote.
#[derive(Debug)]
pub struct Recovered {
    /// Metadata generation selected after simulated reopen.
    pub manifest: Manifest,
    /// Whether recovered evidence permits this replica to vote.
    pub admitted: bool,
    /// Validated canonical operations recovered from the selected segments.
    pub operations: Vec<DecodedOperation<'static>>,
}

/// The async journal over one simulated device for seeded storage schedules.
/// Every call runs its physical jobs to completion before returning.
pub struct Journal {
    device: Device,
    store: Option<GroupJournal>,
    identity: GroupIdentity,
    configuration: Vec<u8>,
    manifest: Manifest,
    positions: BTreeMap<u64, WriterPosition>,
    body_encoding: BodyEncoding,
}

impl std::fmt::Debug for Journal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Journal")
            .field("identity", &self.identity)
            .field("manifest", &self.manifest)
            .field("open", &self.store.is_some())
            .finish_non_exhaustive()
    }
}

impl Journal {
    /// Explicit fresh bootstrap. No disk image is silently replaced.
    pub fn format(
        identity: GroupIdentity,
        configuration: Vec<u8>,
        epoch: u64,
        generation: JournalGeneration,
    ) -> Result<Self, DirectoryError> {
        let mut device = Device::new(Image::default())?;
        let format = JournalFormat {
            identity,
            configuration_epoch: epoch,
            commit_mode: CommitMode::External,
            first_segment: SegmentHeader::new(identity.group_id, 1, None, Digest::ZERO, CAPACITY)?,
            configuration: configuration.clone(),
        };
        let io = device.io.clone();
        let journal = device.run(GroupJournal::format(
            ROOT.into(),
            io,
            format,
            generation,
            limits(),
        ))?;
        Ok(Self {
            device,
            manifest: journal.manifest().clone(),
            store: Some(journal),
            identity,
            configuration,
            positions: BTreeMap::new(),
            body_encoding: BodyEncoding::Raw,
        })
    }

    /// Select the production per-operation encoder, including raw fallback.
    pub fn set_body_encoding(&mut self, encoding: BodyEncoding) -> Result<(), DirectoryError> {
        if !encoding.is_supported() {
            return Err(DirectoryError::ConfigurationMismatch);
        }
        self.body_encoding = encoding;
        Ok(())
    }

    /// Physical append only; the caller observes its completion separately.
    pub fn append(&mut self, operations: &[CanonicalOperation<'_>]) -> Result<(), DirectoryError> {
        let journal = self.store.as_mut().expect("opened journal");
        let position = self
            .device
            .run(journal.append(operations, self.body_encoding))?;
        self.positions.insert(last_operation(position), position);
        Ok(())
    }

    /// Exact captured data barrier followed by independent accepted evidence.
    pub fn sync(&mut self, through: u64) -> Result<(), DirectoryError> {
        let journal = self.store.as_mut().expect("opened journal");
        let durable = journal.writer().durable_position();
        let position = if last_operation(durable) == through {
            durable
        } else {
            *self
                .positions
                .get(&through)
                .expect("complete append boundary")
        };
        self.device.run(journal.sync_through(position))?;
        self.device.run(journal.publish_durable_progress())
    }

    /// Publish a promise, selected view, or activation floor authorized by the core.
    pub fn publish_view(
        &mut self,
        promised: u64,
        normal: u64,
        committed: LogPosition,
    ) -> Result<(), DirectoryError> {
        let journal = self.store.as_mut().expect("opened journal");
        let written = journal.writer().written_position();
        self.device.run(journal.sync_through(written))?;
        let mut next = successor(journal.manifest());
        next.promised_view = promised;
        next.last_normal_view = normal;
        next.accepted = journal.accepted_position()?;
        next.committed = committed;
        let result = self.device.run(journal.install_metadata(next));
        self.manifest = journal.manifest().clone();
        result
    }

    /// Install externally selected complete history. The protected committed
    /// prefix stays in place; later operations go to a fresh segment.
    pub fn replace(
        &mut self,
        generation: JournalGeneration,
        view: u64,
        committed: LogPosition,
        operations: &[CanonicalOperation<'_>],
    ) -> Result<(), DirectoryError> {
        let journal = self.store.take().expect("opened journal");
        self.positions.clear();
        let protected = journal.committed_position()?;
        let accepted = operations
            .last()
            .map_or(LogPosition::GENESIS, |operation| LogPosition {
                op_number: operation.op_number,
                digest: logical_operation_digest(operation),
            });
        let replacement = SuffixReplacement {
            expected_current: journal.current(),
            protected_committed: protected,
            retained: protected,
            promised_view: view,
            last_normal_view: view,
            committed,
            writer_generation: generation,
            segment_capacity: CAPACITY,
            body_encoding: self.body_encoding,
        };
        let mut installer = self
            .device
            .run(journal.begin_suffix_replacement(crate::SuffixRequest {
                replacement,
                accepted,
                limits: stream_limits(),
            }))
            .map_err(other)?;
        let suffix: Vec<_> = operations
            .iter()
            .filter(|operation| operation.op_number > protected.op_number)
            .copied()
            .collect();
        for chunk in suffix.chunks(CHUNK_OPERATIONS) {
            self.device
                .run(installer.append_chunk(chunk))
                .map_err(other)?;
        }
        let journal = self.device.run(installer.finish()).map_err(other)?;
        self.manifest = journal.manifest().clone();
        self.store = Some(journal);
        Ok(())
    }

    /// Remove voting eligibility without reading or altering damaged payloads.
    pub fn quarantine(&mut self) -> Result<(), DirectoryError> {
        self.close();
        let io = self.device.io.clone();
        let recovery = self.device.run(RecoveryDirectory::open_for_repair(
            ROOT.into(),
            io,
            self.identity,
            &self.configuration,
            limits(),
        ))?;
        self.manifest = recovery.manifest().clone();
        let recovery = self
            .device
            .run(recovery.quarantine_for_recovery(&self.configuration))?;
        self.device.run(recovery.close())
    }

    /// Start fresh staging after durable nonvoting admission and a fresh nonce.
    pub fn begin_staging(&mut self, generation: JournalGeneration) -> Result<(), DirectoryError> {
        self.close();
        let io = self.device.io.clone();
        let recovery = self.device.run(RecoveryDirectory::open_recovering(
            ROOT.into(),
            io,
            self.identity,
            &self.configuration,
            limits(),
        ))?;
        let journal = self.device.run(recovery.recover_nonvoting(
            &self.configuration,
            generation,
            ORPHAN_PROBES,
        ))?;
        self.manifest = journal.manifest().clone();
        self.store = Some(journal);
        Ok(())
    }

    /// Publish a complete core-authorized replacement after private semantic replay.
    pub fn publish_recovery(
        &mut self,
        view: u64,
        committed: LogPosition,
    ) -> Result<(), DirectoryError> {
        self.publish_view(view, view, committed)?;
        let journal = self.store.as_mut().expect("opened journal");
        let publication = RecoveryPublication {
            current: journal.current(),
            generation: journal.writer().written_position().generation(),
            view,
            accepted: journal.manifest().accepted,
            committed: journal.manifest().committed,
        };
        let candidate = self
            .device
            .run(journal.publish_recovered_configuration(
                &self.configuration,
                publication,
                recovery_limits(),
            ))
            .map_err(other)?;
        drop(candidate);
        self.manifest = journal.manifest().clone();
        Ok(())
    }

    /// Lose volatile bytes and directory changes without completing pending callbacks.
    pub fn crash(&mut self) {
        self.store = None;
        self.positions.clear();
        self.device.crash();
    }

    /// Reopen through the production recovery path. A nonvoting store is only
    /// inspected; its operations remain private until recovery publication.
    pub fn recover(&mut self, generation: JournalGeneration) -> Result<Recovered, DirectoryError> {
        self.close();
        let result = self.open(generation);
        if result.is_err() {
            self.close();
        }
        result
    }

    fn open(&mut self, generation: JournalGeneration) -> Result<Recovered, DirectoryError> {
        let io = self.device.io.clone();
        let opened = self.device.run(GroupJournal::open(
            ROOT.into(),
            io.clone(),
            crate::ExpectedStore {
                identity: self.identity,
                configuration: Some(&self.configuration),
            },
            generation,
            limits(),
        ));
        let journal = match opened {
            Ok(journal) => journal,
            Err(DirectoryError::ConfigurationMismatch) => {
                self.device.settle();
                let recovery = self.device.run(RecoveryDirectory::open_recovering(
                    ROOT.into(),
                    io,
                    self.identity,
                    &self.configuration,
                    limits(),
                ))?;
                self.manifest = recovery.manifest().clone();
                self.device.run(recovery.close())?;
                return self.snapshot();
            }
            Err(error) => return Err(error),
        };
        self.manifest = journal.manifest().clone();
        self.store = Some(journal);
        self.snapshot()
    }

    /// Decode a projection after successful physical work. Never used as an oracle.
    pub fn snapshot(&self) -> Result<Recovered, DirectoryError> {
        let image = self.device.image();
        let admitted = image
            .bytes(&root().join(CONFIGURATION_FILE), false)
            .is_ok_and(|bytes| bytes == self.configuration);
        let manifest = self.store.as_ref().map_or_else(
            || self.manifest.clone(),
            |journal| journal.manifest().clone(),
        );
        let operations = if admitted {
            self.read_operations(image, &manifest)?
        } else {
            Vec::new()
        };
        Ok(Recovered {
            manifest,
            admitted,
            operations,
        })
    }

    /// Corrupt an offline file, preserving the result through future crashes.
    pub fn damage(&mut self, name: &str, damage: Damage) {
        assert!(self.store.is_none(), "damage requires an offline broker");
        let path = root().join(name);
        let access = Access::new(self.device.io.clone(), None);
        if let Damage::Missing = damage {
            let parent = path.parent().unwrap().to_path_buf();
            self.device
                .run(access.done(Operation::RemoveFile { path }))
                .unwrap();
            self.device.run(access.sync_directory(parent)).unwrap();
        } else {
            let mut bytes = self.device.image().bytes(&path, true).unwrap().to_vec();
            match damage {
                Damage::Flip(offset) => bytes[offset] ^= 1,
                Damage::ZeroSuffix(offset) => bytes[offset..].fill(0),
                Damage::Truncate(offset) => bytes.truncate(offset),
                Damage::Missing => unreachable!(),
            }
            self.device.run(overwrite(&access, path, &bytes)).unwrap();
        }
        drop(access);
        self.device.crash();
    }

    /// Selected payload file for a scoped fault schedule.
    pub fn active_name(&self) -> String {
        let manifest = self
            .store
            .as_ref()
            .map_or(&self.manifest, GroupJournal::manifest);
        let active = *manifest.segments.last().expect("active segment");
        segment_reference_name(active)
            .to_string_lossy()
            .into_owned()
    }

    /// Inject an I/O error into the nth next physical operation.
    pub fn fail_after(&mut self, operations: usize) {
        assert!(operations > 0);
        self.device.fail_at = Some(self.device.executed + operations);
    }

    /// End the injected I/O-failure period when a schedule restores healthy storage.
    pub fn clear_failure(&mut self) {
        self.device.fail_at = None;
    }

    /// Negative control: directory and metadata-file barriers report success
    /// without persisting anything.
    pub fn omit_metadata_barriers(&mut self, omit: bool) {
        self.device.omit_metadata_barriers = omit;
    }

    /// Negative control: `DURABLE` evidence writes return before they are durable.
    pub fn omit_evidence_barrier(&mut self, omit: bool) {
        self.device.omit_evidence_barrier = omit;
    }

    /// Persist only a prefix of unsynchronized active bytes before the next crash.
    /// Never changes bytes already covered by a successful file barrier.
    pub fn tear_unsynced(&mut self, kept_bytes: usize) {
        let path = root().join(self.active_name());
        let image = self.device.image();
        let durable = image.bytes(&path, true).unwrap().len();
        let dirty = image.bytes(&path, false).unwrap().len();
        let end = durable.saturating_add(kept_bytes).min(dirty);
        if durable < end {
            self.device
                .controller()
                .persist_range(&path, durable..end)
                .unwrap();
        }
    }

    /// Let selected pending bytes and their required file length survive a crash.
    /// Ranges are absolute file offsets. Does not publish directory entries or
    /// complete a journal barrier; the scheduler may persist ranges in any order.
    /// This chooses a possible crash image, not a writeback syscall's guarantee.
    pub fn persist_range(&mut self, name: &str, range: Range<usize>) -> Result<(), DirectoryError> {
        let path = root().join(name);
        self.device.controller().persist_range(&path, range)?;
        Ok(())
    }

    /// Complete active-segment writes not yet covered by a journal barrier.
    /// Range writeback can preserve some bytes without advancing this boundary.
    pub fn unsynced_active_range(&self) -> Range<usize> {
        let writer = self.store.as_ref().expect("opened journal").writer();
        writer.durable_position().end_offset() as usize
            ..writer.written_position().end_offset() as usize
    }

    /// Physical operations executed so far. Schedules use it to bound fault cuts.
    pub fn executed_operations(&self) -> usize {
        self.device.executed
    }

    fn close(&mut self) {
        self.store = None;
        self.positions.clear();
        self.device.settle();
    }

    fn read_operations(
        &self,
        image: &Image,
        manifest: &Manifest,
    ) -> Result<Vec<DecodedOperation<'static>>, DirectoryError> {
        let durable = self
            .store
            .as_ref()
            .map(|journal| journal.writer().durable_position().end_offset() as usize);
        let mut operations = Vec::new();
        for reference in &manifest.segments {
            let mut bytes = image
                .bytes(&root().join(segment_reference_name(*reference)), false)?
                .to_vec();
            let end = match reference.sealed {
                Some(sealed) => Some(sealed.valid_bytes as usize),
                None => durable,
            };
            if let Some(end) = end {
                bytes.truncate(end);
            }
            let scan = scan_segment(
                &bytes,
                reference.first_group_number,
                reference.first_chain,
                DecodeLimits::default(),
            )?;
            operations.extend(
                scan.groups
                    .into_iter()
                    .flat_map(|group| group.operations)
                    .map(|operation| DecodedOperation {
                        body: Cow::Owned(operation.body.into_owned()),
                        ..operation
                    }),
            );
        }
        Ok(operations)
    }
}

/// One simulated device whose jobs complete in submission order.
struct Device {
    controller: Option<Controller>,
    io: Local,
    executed: usize,
    fail_at: Option<usize>,
    omit_metadata_barriers: bool,
    omit_evidence_barrier: bool,
    evidence_written: bool,
}

impl Device {
    fn new(image: Image) -> Result<Self, DirectoryError> {
        let (controller, mut clients) = Controller::new(config(), image)?;
        Ok(Self {
            controller: Some(controller),
            io: Local::new(clients.remove(0)),
            executed: 0,
            fail_at: None,
            omit_metadata_barriers: false,
            omit_evidence_barrier: false,
            evidence_written: false,
        })
    }

    fn controller(&mut self) -> &mut Controller {
        self.controller.as_mut().expect("running device")
    }

    fn image(&self) -> &Image {
        self.controller.as_ref().expect("running device").image()
    }

    /// Process death with power loss; injected failures end with it.
    fn crash(&mut self) {
        let controller = self.controller.take().expect("running device");
        let (image, _) = controller.crash(true).expect("simulated crash");
        let (controller, mut clients) = Controller::new(config(), image).expect("restart");
        self.controller = Some(controller);
        self.io = Local::new(clients.remove(0));
        self.fail_at = None;
    }

    fn run<T>(&mut self, future: impl Future<Output = T>) -> T {
        let mut future = std::pin::pin!(future);
        let mut context = Context::from_waker(Waker::noop());
        for _ in 0..1_000_000 {
            if let Poll::Ready(result) = future.as_mut().poll(&mut context) {
                return result;
            }
            self.execute_queued();
        }
        panic!("simulated storage exceeded its step bound")
    }

    /// Complete deferred work, such as closes from dropped handles.
    fn settle(&mut self) {
        while !self.controller().jobs().is_empty() {
            self.execute_queued();
        }
    }

    fn execute_queued(&mut self) {
        let controller = self.controller.as_mut().expect("running device");
        for (id, _) in controller.jobs() {
            self.executed += 1;
            let effect = match controller.operation(id).unwrap().unprotected() {
                _ if self.fail_at == Some(self.executed) => {
                    Effect::FailBefore(std::io::ErrorKind::Other)
                }
                Operation::Sync {
                    mode: SyncMode::All,
                    ..
                } if self.omit_metadata_barriers => Effect::SkipBarrier,
                Operation::Sync {
                    mode: SyncMode::Data,
                    ..
                } if std::mem::take(&mut self.evidence_written) => Effect::SkipBarrier,
                Operation::Write { offset, data, .. } => {
                    // Evidence overwrites each copy in place, then barriers it.
                    self.evidence_written |= self.omit_evidence_barrier
                        && data.len() == evidence::RECORD_BYTES
                        && matches!(*offset as usize, 0 | evidence::COPY_STRIDE);
                    Effect::Normal
                }
                _ => Effect::Normal,
            };
            controller.execute(id, effect).unwrap();
            controller.deliver(id).unwrap();
        }
        drop(controller.take_trace());
    }
}

async fn overwrite(access: &Access, path: PathBuf, bytes: &[u8]) -> std::io::Result<()> {
    let handle = access.open(path, OpenMode::ReadWrite, false, false).await?;
    access
        .done(Operation::SetLength {
            handle: handle.clone(),
            length: bytes.len() as u64,
        })
        .await?;
    access.write_all(&handle, 0, bytes).await?;
    access.sync(&handle).await
}

fn root() -> &'static Path {
    Path::new(ROOT)
}

fn config() -> simulation::Config {
    let quota = Quota {
        operations: 8,
        bytes: 4 * 1024 * 1024,
    };
    simulation::Config {
        limits: ozzy_io::Limits {
            shards: 1,
            data: quota,
            progress: quota,
        },
        handles: 64,
        image: ImageLimits {
            nodes: 1024,
            directory_entries: 4096,
            file_bytes: 2 * CAPACITY as usize,
            total_bytes: 256 * 1024 * 1024,
        },
        trace_events: 100_000,
    }
}

fn limits() -> JournalLimits {
    JournalLimits {
        metadata: MetadataLimits::default(),
        decode: DecodeLimits::default(),
        operations: OperationLimits::default(),
        checkpoint: CheckpointLimits::default(),
        io: SegmentOptions {
            max_segment_bytes: CAPACITY,
            chunk_bytes: 64 * 1024,
            direct: false,
            write_mode: SegmentWriteMode::Buffered,
        },
        directory_entries: 1024,
        directory_name_bytes: 64 * 1024,
    }
}

fn stream_limits() -> SuffixStreamLimits {
    SuffixStreamLimits {
        max_group_operations: CHUNK_OPERATIONS,
        max_group_body_bytes: 1024 * 1024,
        max_segments: 16,
        max_staged_bytes: 16 * 1024 * 1024,
        max_source_segment_bytes: CAPACITY,
        max_orphan_probes: ORPHAN_PROBES,
    }
}

fn recovery_limits() -> CanonicalRecoveryLimits {
    CanonicalRecoveryLimits {
        retained_identities: 128,
        ..CanonicalRecoveryLimits::default()
    }
}

fn successor(manifest: &Manifest) -> Manifest {
    let mut next = manifest.clone();
    next.parent_generation = next.generation;
    next.generation += 1;
    next
}

fn last_operation(position: WriterPosition) -> u64 {
    position.next_chain().next_op_number() - 1
}

fn other(error: impl std::fmt::Display) -> DirectoryError {
    std::io::Error::other(error.to_string()).into()
}
