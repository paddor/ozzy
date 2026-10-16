//! Confirmed floors precede checkpoint selection and physical retirement.

use super::{JournalError, OwnedJournal};
use crate::replica_journal::shard::yield_turn;
use crate::replica_journal::{ProposalBuffer, authority::position};
use ozzy_core::{
    checkpoint::Layout,
    retention::{Plan, Segment},
    state::{CanonicalState, StateLimits, StateSnapshotLimits, canonical_state_schema_digest},
};
use ozzy_journal::operation::{OperationBody, ProducerResultFloor, Trim, encode_operation_body};
use ozzy_journal_segment::{
    AsyncCheckpointFiles, AsyncCheckpointSource, AsyncRetirementBudget, AsyncRetirementProof,
    AsyncRetirementScan, AsyncScannedPrefix, CanonicalCheckpointError, RetentionFloors,
    RetiredPrefix,
};
use ozzy_proto::{CheckpointId, Offset, OperationId, PartitionIncarnation};
use ozzy_replication::driver::ValidationTicket;
use std::collections::BTreeMap;

/// Unselected objects one settled turn removes per cleanup class.
const CLEANUP_BUDGET: usize = 8;

#[derive(Debug, Default)]
pub(super) struct Retention {
    /// Summaries of selected sealed segments, from a roll or a scan.
    pub(super) sealed: BTreeMap<u64, Segment>,
    /// Record ends of segments this owner rolled, so retirement need not
    /// scan them.
    pub(super) proofs: Vec<AsyncRetirementProof>,
    /// The last settled turn spent its one proposal on an idle-time
    /// watermark; the next one proposes record retention first.
    records_owed: bool,
    /// Manifest generation whose unselected objects are all reclaimed or
    /// pinned, with the pinned ones.
    cleaned: Option<(u64, Vec<u64>, Vec<CheckpointId>)>,
    /// Retirement whose checkpoint write and prefix scan run beside writes.
    retiring: Retiring,
    /// Retirement attempts started, so a stale completion is recognized.
    attempts: u64,
}

impl OwnedJournal {
    fn charge_retention(&self) -> Result<Option<crate::memory::Charge>, JournalError> {
        if self.append_memory.is_none() {
            return Ok(None);
        }
        let bytes = self.journal.ready()?.retention_scratch_bytes()?;
        charge(self.append_memory.as_ref(), bytes)
    }

    /// Read at most `maximum_segments` immutable summaries; cache each exact
    /// generation once. Selected capacities include unexamined and active space.
    pub async fn plan_retention(
        &mut self,
        ticket: ValidationTicket,
        now_millis: u64,
        maximum_segments: usize,
    ) -> Result<Option<(PartitionIncarnation, Plan)>, JournalError> {
        self.healthy()?;
        self.validate_image(ticket)?;
        let mut partitions = self.images()?.committed().partitions();
        let Some((partition, state)) = partitions.next() else {
            return Ok(None);
        };
        if partitions.next().is_some() {
            return Err(JournalError::Configuration);
        }
        let (policy, floor, end) = (state.retention, state.retained_from, state.next_offset);
        drop(partitions);
        if policy.max_age_millis.is_none() && policy.max_bytes.is_none() {
            return Ok(None);
        }
        let _scratch = self.charge_retention()?;
        let journal = self.journal.ready()?;
        let references = journal.manifest().segments.clone();
        self.retention
            .sealed
            .retain(|id, _| references.iter().any(|entry| entry.segment_id == *id));
        self.retention.proofs.retain(|proof| {
            references
                .iter()
                .any(|entry| entry.segment_id == proof.segment_id())
        });
        let active_time = self
            .reader
            .as_ref()
            .and_then(|reader| reader.newest_active_append_millis(partition));
        let mut segments = Vec::with_capacity(references.len());
        let mut record_end = floor;
        for (index, reference) in references.iter().enumerate() {
            if reference.sealed.is_some()
                && index < maximum_segments
                && !self.retention.sealed.contains_key(&reference.segment_id)
            {
                let summary = journal
                    .retention_segment(reference.segment_id, partition)
                    .await?;
                self.retention.sealed.insert(reference.segment_id, summary);
            }
            let last_operation = references
                .get(index + 1)
                .map_or(ticket.accepted().op.0, |next| {
                    next.first_chain.next_op_number() - 1
                });
            let mut segment = self
                .retention
                .sealed
                .get(&reference.segment_id)
                .copied()
                .unwrap_or(Segment {
                    id: reference.segment_id,
                    capacity: reference.capacity,
                    sealed: reference.sealed.is_some(),
                    last_operation,
                    record_end,
                    newest_append_millis: if reference.sealed.is_none() {
                        active_time
                    } else {
                        None
                    },
                });
            record_end = record_end.max(segment.record_end);
            segment.record_end = if segment.sealed { record_end } else { end };
            segments.push(segment);
        }
        Ok(Some((
            partition,
            Plan::select(
                &segments,
                policy,
                now_millis,
                ticket.applied().op.0,
                maximum_segments,
            )?,
        )))
    }

    /// Expire bounded producer retry ranges before advancing the record floor.
    /// The caller submits this through the ordinary mode's proposal/commit path.
    pub fn retention_proposal(
        &self,
        partition: PartitionIncarnation,
        floor: Offset,
        operation_floor: u64,
        seed: OperationId,
    ) -> Result<Option<ProposalBuffer>, JournalError> {
        let state = self
            .images()?
            .committed()
            .partition(partition)
            .ok_or(JournalError::Configuration)?;
        let floor = floor.max(state.retained_from);
        let mut buffer = self
            .lease_proposal_buffer()
            .map_err(|_| JournalError::AppendCapacity)?;
        let mut producers: Vec<_> = state
            .producers()
            .filter(|(_, producer)| {
                producer.result_floor_for_retention(floor, operation_floor)
                    > producer.producer_result_floor
            })
            .take(self.append_limits.max_operations.saturating_add(1))
            .collect();
        producers.sort_unstable_by_key(|(id, _)| *id);
        let mut complete = true;
        for (producer_id, producer) in producers {
            let new_floor = producer.result_floor_for_retention(floor, operation_floor);
            if new_floor <= producer.producer_result_floor {
                continue;
            }
            if buffer.len() == self.append_limits.max_operations {
                complete = false;
                break;
            }
            let id = operation_id(seed, buffer.len());
            push(
                &mut buffer,
                &OperationBody::ProducerResultFloor(ProducerResultFloor {
                    partition,
                    producer_id,
                    producer_epoch: producer.producer_epoch,
                    expected_floor: producer.producer_result_floor,
                    new_floor,
                    operation_id: id,
                }),
                self.limits.operations,
            )?;
        }
        if complete
            && floor > state.retained_from
            && buffer.len() < self.append_limits.max_operations
        {
            let id = operation_id(seed, buffer.len());
            push(
                &mut buffer,
                &OperationBody::Trim(Trim {
                    partition,
                    expected_floor: state.retained_from,
                    new_floor: floor,
                    operation_id: id,
                }),
                self.limits.operations,
            )?;
        }
        Ok((!buffer.is_empty()).then_some(buffer))
    }

    /// Select a durable checkpoint of exactly the settled confirmed image, then
    /// unreference only a validated sealed prefix below those committed floors.
    /// Runs every retirement step inline. Cancellation fences this owner.
    /// Captured readers keep their file pins.
    pub async fn retire_confirmed_history(
        &mut self,
        ticket: ValidationTicket,
        id: CheckpointId,
        budget: AsyncRetirementBudget,
    ) -> Result<RetiredPrefix, JournalError> {
        self.begin_retirement(ticket, id, budget).await?;
        let work = self
            .take_retirement_work()
            .ok_or(JournalError::CompletionMismatch)?;
        let done = Box::pin(work.run()).await;
        self.complete_retirement(done);
        self.finish_retirement()
            .await?
            .ok_or(JournalError::CompletionMismatch)
    }

    /// Settled step of a retirement: publish the exact confirmed position and
    /// capture its state, identity rows, and the sealed prefix to retire. A
    /// job then encodes and writes the checkpoint and scans the prefix beside
    /// later writes; `finish_retirement` installs both between writes.
    pub(in crate::replica_journal) async fn begin_retirement(
        &mut self,
        ticket: ValidationTicket,
        id: CheckpointId,
        budget: AsyncRetirementBudget,
    ) -> Result<(), JournalError> {
        self.healthy()?;
        self.validate_image(ticket)?;
        self.require_settled(ticket)?;
        if !matches!(self.retention.retiring, Retiring::Idle) {
            return Err(JournalError::CompletionMismatch);
        }
        let scratch = self.charge_retention()?;
        let state = self.images()?.committed().clone();
        let floors = RetentionFloors::from_canonical_state(&state)?;
        let applied = position(self.applied);
        // Repeated bounded passes reuse only the exact selected checkpoint.
        // The scan still revalidates its files and checks the floors.
        let build = self
            .journal
            .ready()?
            .manifest()
            .checkpoint
            .is_none_or(|selected| selected.position != applied);
        let rows = if build && self.dedup.limits.is_some() {
            let memory = &self.append_memory;
            Some(
                self.dedup
                    .checkpoint_rows(|bytes| charge(memory.as_ref(), bytes))?,
            )
        } else {
            None
        };
        self.faulted = true;
        let journal = self.journal.ready_mut()?;
        journal
            .sync_through(journal.writer().written_position())
            .await?;
        let checkpoint = if build {
            let mut next = journal.manifest().clone();
            next.parent_generation = next.generation;
            next.generation = next
                .generation
                .checked_add(1)
                .ok_or(JournalError::Configuration)?;
            next.accepted = applied;
            next.committed = applied;
            journal.install_metadata(next).await?;
            if state.revision() != journal.committed_position()?.op_number {
                return Err(JournalError::CompletionMismatch);
            }
            Some(CheckpointImage {
                source: journal.prepare_checkpoint_source(
                    id,
                    canonical_state_schema_digest(),
                    self.limits.checkpoint.max_chunk_bytes,
                )?,
                state,
                rows,
            })
        } else {
            None
        };
        let scan =
            journal.prepare_retirement_scan(floors, applied, budget, &self.retention.proofs)?;
        self.retention.attempts += 1;
        self.retention.retiring = Retiring::Captured(Box::new(RetirementWork {
            attempt: self.retention.attempts,
            checkpoint,
            scan,
            limits: (self.recovery.state, self.recovery.snapshot),
            _scratch: scratch,
        }));
        self.faulted = false;
        Ok(())
    }

    /// Captured retirement work for the shard to run as a job.
    pub(in crate::replica_journal) fn take_retirement_work(&mut self) -> Option<RetirementWork> {
        match std::mem::take(&mut self.retention.retiring) {
            Retiring::Captured(work) => {
                self.retention.retiring = Retiring::Running(work.attempt);
                Some(*work)
            }
            other => {
                self.retention.retiring = other;
                None
            }
        }
    }

    /// Keep finished work only for the attempt still running; an abandoned
    /// attempt's files become unselected leftovers.
    pub(in crate::replica_journal) fn complete_retirement(&mut self, done: RetirementDone) {
        if matches!(self.retention.retiring, Retiring::Running(attempt) if attempt == done.attempt)
        {
            self.retention.retiring = Retiring::Done(Box::new(done));
        }
    }

    /// Whether a retirement is captured, running, or awaiting installation.
    pub(in crate::replica_journal) fn retiring(&self) -> bool {
        !matches!(self.retention.retiring, Retiring::Idle)
    }

    /// Whether finished retirement work waits for an owner step between writes.
    pub(in crate::replica_journal) fn retirement_ready(&self) -> bool {
        matches!(self.retention.retiring, Retiring::Done(_)) && self.journal.ready().is_ok()
    }

    /// Drop retirement work captured before the journal's selection changed.
    pub(in crate::replica_journal) fn abandon_retirement(&mut self) {
        self.retention.retiring = Retiring::Idle;
    }

    /// Owner step between writes: select the new checkpoint and unreference
    /// the scanned prefix in one manifest publication, then narrow identity
    /// coverage. Later accepted operations keep their claims. A failed write,
    /// validation, or scan fences this owner.
    pub(in crate::replica_journal) async fn finish_retirement(
        &mut self,
    ) -> Result<Option<RetiredPrefix>, JournalError> {
        self.healthy()?;
        let Retiring::Done(done) = std::mem::take(&mut self.retention.retiring) else {
            return Ok(None);
        };
        self.faulted = true;
        let (files, prefix) = done.result?;
        let journal = self.journal.ready_mut()?;
        let retired = journal.install_retirement(files.as_ref(), prefix).await?;
        drop(files);
        if let Some(reader) = &mut self.reader {
            reader.retired(journal, &retired.unreferenced_segment_ids)?;
        }
        // Retirement changes selected storage, never application state.
        // Narrow identity coverage to the retained history. Rebuilding it
        // would index every surviving sealed segment while nothing is admitted.
        let first = journal.manifest().segments[0];
        self.images
            .as_mut()
            .ok_or(JournalError::CompletionMismatch)?
            .map_identity_indexes(|identities| identities.retire_prefix(first))?;
        self.replay.clear();
        self.storage_validation.clear();
        self.retention
            .sealed
            .retain(|id, _| !retired.unreferenced_segment_ids.contains(id));
        self.retention.proofs.retain(|proof| {
            !retired
                .unreferenced_segment_ids
                .contains(&proof.segment_id())
        });
        self.faulted = false;
        Ok(Some(retired))
    }
}

/// Confirmed state and identity rows captured by the owner; a job encodes
/// and writes them as one checkpoint image.
#[derive(Debug)]
struct CheckpointImage {
    source: AsyncCheckpointSource,
    state: CanonicalState,
    rows: Option<super::dedup::checkpoint::CheckpointRows>,
}

impl CheckpointImage {
    /// Encode the layout and state, then write them with the identity rows.
    async fn write(
        self,
        snapshot: StateSnapshotLimits,
    ) -> Result<AsyncCheckpointFiles, JournalError> {
        let mut steps = 0_usize;
        let bytes = self
            .state
            .encode_snapshot_cooperative(snapshot, async |_| {
                steps += 1;
                if steps.is_multiple_of(STATE_ENCODE_STEPS) {
                    yield_turn().await;
                }
            })
            .await
            .map_err(CanonicalCheckpointError::from)?;
        let layout = Layout::new(
            bytes.len() as u64,
            self.rows.as_ref().map(|rows| rows.identities),
        )
        .map_err(CanonicalCheckpointError::from)?;
        layout
            .validate_state(&self.state)
            .map_err(CanonicalCheckpointError::from)?;
        let plan = self.source.plan(
            layout
                .encoded_bytes()
                .map_err(CanonicalCheckpointError::from)?,
        )?;
        let mut writer = plan.start().await?;
        let fragment = writer.fragment_bytes();
        let rows = self.rows.as_ref().map_or(&[][..], |rows| &rows.bytes[..]);
        for part in [&layout.encode()[..], &bytes, rows] {
            for chunk in part.chunks(fragment) {
                writer.push(chunk).await?;
            }
        }
        Ok(writer.finish().await?)
    }
}

/// State snapshot entries encoded between yields to other shard work.
const STATE_ENCODE_STEPS: usize = 1024;

/// Retirement file work that borrows no journal owner.
#[derive(Debug)]
pub(in crate::replica_journal) struct RetirementWork {
    attempt: u64,
    checkpoint: Option<CheckpointImage>,
    scan: AsyncRetirementScan,
    limits: (StateLimits, StateSnapshotLimits),
    _scratch: Option<crate::memory::Charge>,
}

impl RetirementWork {
    /// Write and read back the checkpoint, then scan the prefix.
    pub(in crate::replica_journal) async fn run(self) -> RetirementDone {
        let Self {
            attempt,
            checkpoint,
            scan,
            limits: (state, snapshot),
            _scratch,
        } = self;
        let result = async {
            let files = match checkpoint {
                Some(image) => {
                    let files = image.write(snapshot).await?;
                    files.decode_canonical_state(state, snapshot).await?;
                    Some(files)
                }
                None => None,
            };
            Ok((files, scan.scan().await?))
        }
        .await;
        RetirementDone { attempt, result }
    }
}

/// Finished retirement work awaiting its owner step.
#[derive(Debug)]
pub(in crate::replica_journal) struct RetirementDone {
    attempt: u64,
    result: Result<(Option<AsyncCheckpointFiles>, AsyncScannedPrefix), JournalError>,
}

/// Where a retirement stands between its settled turn and its installation.
#[derive(Debug, Default)]
enum Retiring {
    #[default]
    Idle,
    /// Captured by a settled turn; the shard starts it as a job.
    Captured(Box<RetirementWork>),
    /// Detached work in flight for this attempt.
    Running(u64),
    /// Detached work finished; an owner step between writes installs it.
    Done(Box<RetirementDone>),
}

fn charge(
    memory: Option<&crate::memory::Owner>,
    bytes: usize,
) -> Result<Option<crate::memory::Charge>, JournalError> {
    let Some(owner) = memory else {
        return Ok(None);
    };
    owner.try_charge(bytes).map(Some).map_err(|error| {
        if error.kind() == std::io::ErrorKind::WouldBlock {
            JournalError::AppendCapacity
        } else {
            error.into()
        }
    })
}

fn push(
    buffer: &mut ProposalBuffer,
    body: &OperationBody<'_>,
    limits: ozzy_journal::operation::OperationLimits,
) -> Result<(), JournalError> {
    buffer.push(body.kind(), &encode_operation_body(body, limits)?)
}

fn operation_id(seed: OperationId, ordinal: usize) -> OperationId {
    let mut bytes = *seed.as_bytes();
    for (target, value) in bytes[8..].iter_mut().zip((ordinal as u64).to_be_bytes()) {
        *target ^= value;
    }
    OperationId::from_bytes(bytes)
}

impl OwnedJournal {
    pub(in crate::replica_journal) async fn retention_turn(
        &mut self,
        ticket: ValidationTicket,
        now_millis: u64,
        seed: OperationId,
        leader: bool,
    ) -> Result<super::super::RetentionTurn, JournalError> {
        self.healthy()?;
        self.validate_image(ticket)?;
        if ticket.accepted() != ticket.applied() || ticket.committed() != ticket.applied() {
            return Ok(deferred());
        }
        // Bootstrap may activate the empty log before CreatePartition commits.
        // No partition state has selected an unlimited retention policy yet.
        if self.images()?.committed().partitions().next().is_none() {
            return Ok(deferred());
        }
        // One bounded expiry step, then record retention in the same turn.
        // Under steady load expiry always has more work, so waiting for it
        // would starve record retention. A turn carries one proposal.
        let owed = std::mem::take(&mut self.retention.records_owed);
        let identities = self.retain_identities(ticket, leader && !owed)?;
        if identities.proposal.is_some() {
            self.retention.records_owed = true;
            return Ok(super::super::RetentionTurn {
                enabled: true,
                more_work: identities.more_work,
                expired_identities: identities.expired,
                proposal: identities.proposal,
                released: None,
            });
        }
        let mut turn = self
            .record_retention_turn(ticket, now_millis, seed, leader)
            .await?;
        turn.enabled |= self.dedup.limits.is_some();
        turn.more_work |= identities.more_work;
        turn.expired_identities = identities.expired;
        Ok(turn)
    }

    /// Recovery can leave unselected generations even when the selected log
    /// fits its retention limit. Each class has a fixed removal budget, and a
    /// pass waits behind the device's data writes while this partition admits
    /// nothing. Selection changes only when the manifest does, so a pass
    /// repeats after a publication, while objects remain, or once a pinned
    /// object is released.
    async fn reclaim_unselected(&mut self, ticket: ValidationTicket) -> Result<(), JournalError> {
        let journal = self.journal.ready()?;
        let generation = journal.manifest().generation;
        let owed = match &self.retention.cleaned {
            Some((cleaned, segments, checkpoints)) => {
                *cleaned != generation || journal.pins_released(segments, checkpoints)?
            }
            None => true,
        };
        if !owed {
            return Ok(());
        }
        self.retention.cleaned = None;
        let (mut clean, mut segments, mut checkpoints) = (true, Vec::new(), Vec::new());
        for kind in [
            super::StorageCleanup::Segments,
            super::StorageCleanup::Checkpoints,
            super::StorageCleanup::Indexes,
            super::StorageCleanup::Metadata,
        ] {
            let cleaned = self.cleanup_storage(ticket, kind, CLEANUP_BUDGET).await?;
            clean &= cleaned.removed_objects < CLEANUP_BUDGET;
            segments.extend(cleaned.pinned_segment_ids);
            checkpoints.extend(cleaned.pinned_checkpoint_ids);
        }
        if clean {
            self.retention.cleaned = Some((generation, segments, checkpoints));
        }
        Ok(())
    }

    async fn record_retention_turn(
        &mut self,
        ticket: ValidationTicket,
        now_millis: u64,
        seed: OperationId,
        leader: bool,
    ) -> Result<super::super::RetentionTurn, JournalError> {
        // One retirement at a time; its job and installation run between
        // writes, and planning resumes once it is installed.
        if !matches!(self.retention.retiring, Retiring::Idle) {
            return Ok(deferred());
        }
        let planned = match self.plan_retention(ticket, now_millis, 1).await {
            Err(JournalError::AppendCapacity) => return Ok(deferred()),
            result => result?,
        };
        let Some((partition, plan)) = planned else {
            return Ok(super::super::RetentionTurn {
                enabled: false,
                expired_identities: 0,
                more_work: false,
                proposal: None,
                released: None,
            });
        };
        self.reclaim_unselected(ticket).await?;
        let state = self
            .images()?
            .committed()
            .partition(partition)
            .ok_or(JournalError::Configuration)?;
        let retry_floors_pending = state.producers().any(|(_, producer)| {
            producer.result_floor_for_retention(
                plan.record_floor.max(state.retained_from),
                plan.operation_floor,
            ) > producer.producer_result_floor
        });
        if plan.record_floor > state.retained_from || retry_floors_pending {
            let proposal = if leader {
                match self.retention_proposal(
                    partition,
                    plan.record_floor,
                    plan.operation_floor,
                    seed,
                ) {
                    Err(JournalError::AppendCapacity) => return Ok(deferred()),
                    result => result?,
                }
            } else {
                None
            };
            return Ok(super::super::RetentionTurn {
                enabled: true,
                expired_identities: 0,
                more_work: proposal.is_some(),
                proposal,
                released: None,
            });
        }
        let mut released = None;
        let mut more_work = false;
        if !plan.retire.is_empty() {
            let previous = self.history.take();
            released = previous.as_ref().map(super::history::source);
            let max_read_bytes = self.limits.io.max_segment_bytes as usize;
            let retirement = self
                .begin_retirement(
                    ticket,
                    CheckpointId::from_bytes(*seed.as_bytes()),
                    AsyncRetirementBudget {
                        max_segments: 1,
                        max_read_bytes,
                    },
                )
                .await;
            if matches!(retirement, Err(JournalError::AppendCapacity)) {
                self.history = previous;
                return Ok(deferred());
            }
            retirement?;
            // Eligible debt continues once this retirement is installed.
            more_work = true;
        } else if leader && plan.roll_active {
            let work = self.begin_roll(16)?;
            let done = work.publish().await;
            self.complete_roll(done)?;
            more_work = true;
        }
        Ok(super::super::RetentionTurn {
            enabled: true,
            expired_identities: 0,
            more_work,
            proposal: None,
            released,
        })
    }
}

fn deferred() -> super::super::RetentionTurn {
    super::super::RetentionTurn {
        enabled: true,
        expired_identities: 0,
        more_work: false,
        proposal: None,
        released: None,
    }
}
