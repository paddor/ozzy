//! One caller owns the bounded completion holes; confirmations are read-only.

use super::{
    PendingProgress, ProcessingIdentity, ProcessingToken, ProgressError,
    progress::{Observer, Publisher, RESERVATION_BYTES, Submission},
};
use ozzy_proto::{Offset, append::Policy};
use std::{collections::VecDeque, sync::Arc};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Debug)]
pub(crate) struct Window {
    progress: Observer,
    submission: Submission,
    retired: u64,
    delivered: u64,
    processed: u64,
    completed: VecDeque<bool>,
    records: usize,
    _permit: OwnedSemaphorePermit,
}

impl Window {
    pub(crate) fn reservation_bytes(records: usize) -> Option<usize> {
        records
            .checked_add(size_of::<Self>())?
            .checked_add(RESERVATION_BYTES)
    }

    pub(crate) fn observer(&self) -> Observer {
        self.progress.clone()
    }
    /// Reserve metadata before allocation. Queue storage is released with this
    /// owner; shared identity storage follows the final token/confirmation alias.
    pub(crate) fn new(
        identity: ProcessingIdentity,
        cursor: Offset,
        records: usize,
        budget: &Arc<Semaphore>,
    ) -> Result<(Self, Publisher), ProgressError> {
        if identity.group.as_bytes() == &[0; 16]
            || identity.partition.as_bytes() == &[0; 16]
            || identity.member.as_bytes() == &[0; 16]
            || identity.assignment_epoch == 0
            || !matches!(
                identity.policy,
                Policy::LocalDurable | Policy::QuorumDurable | Policy::QuorumReplicatedPersisting
            )
            || records == 0
            || records > u32::MAX as usize
        {
            return Err(ProgressError::Configuration);
        }
        let queue_bytes = records
            .checked_add(size_of::<Self>())
            .and_then(|bytes| u32::try_from(bytes).ok())
            .ok_or(ProgressError::Configuration)?;
        let permit = budget
            .clone()
            .try_acquire_many_owned(queue_bytes)
            .map_err(|_| ProgressError::Capacity)?;
        let shared = budget
            .clone()
            .try_acquire_many_owned(RESERVATION_BYTES as u32)
            .map_err(|_| ProgressError::Capacity)?;
        let mut completed = VecDeque::new();
        completed
            .try_reserve_exact(records)
            .map_err(|_| ProgressError::Capacity)?;
        if completed.capacity() > records {
            return Err(ProgressError::Capacity);
        }
        let mut publisher = Publisher::new(identity, cursor.get(), Some(shared));
        let submission = publisher
            .take_submission()
            .expect("new processing publisher");
        Ok((
            Self {
                progress: publisher.observer(),
                submission,
                retired: cursor.get(),
                delivered: cursor.get(),
                processed: cursor.get(),
                completed,
                records,
                _permit: permit,
            },
            publisher,
        ))
    }

    pub(crate) fn has_capacity(&self) -> bool {
        self.completed.len() < self.records && self.progress.stopping().is_none()
    }

    pub(crate) fn deliver(&mut self, offset: Offset) -> Result<ProcessingToken, ProgressError> {
        if let Some(error) = self.progress.stopping() {
            return Err(error);
        }
        if offset.get() != self.delivered {
            return Err(ProgressError::Response);
        }
        let next = self
            .delivered
            .checked_add(1)
            .ok_or(ProgressError::Response)?;
        if !self.has_capacity() {
            return Err(ProgressError::Capacity);
        }
        self.completed.push_back(false);
        self.delivered = next;
        self.submission.record_delivery(next);
        Ok(ProcessingToken {
            progress: self.progress.clone(),
            cursor: Offset::new(next),
        })
    }

    pub(super) fn acknowledge(
        &mut self,
        token: ProcessingToken,
    ) -> Result<PendingProgress, ProgressError> {
        if !self.progress.same_window(&token.progress)
            || token.cursor.get() == 0
            || token.cursor.get() > self.delivered
        {
            return Err(ProgressError::Token);
        }
        if token.cursor.get() > self.progress.confirmed() {
            if let Some(error) = self.progress.stopping() {
                return Err(error);
            }
            let index = usize::try_from(token.cursor.get() - 1 - self.retired)
                .map_err(|_| ProgressError::Token)?;
            *self.completed.get_mut(index).ok_or(ProgressError::Token)? = true;
        }
        Ok(PendingProgress {
            progress: token.progress,
            cursor: token.cursor,
        })
    }

    /// Mark one completed record and publish every newly contiguous position.
    /// The scan is bounded by the reserved window's count/metadata capacity.
    /// No further caller poll is needed to submit the end of a closed hole.
    pub(crate) fn submit(
        &mut self,
        token: ProcessingToken,
    ) -> Result<PendingProgress, ProgressError> {
        let pending = self.acknowledge(token)?;
        let more = self.advance(self.records);
        debug_assert!(!more, "processing scan exceeds its reserved window");
        self.submission.publish(self.processed)?;
        Ok(pending)
    }

    pub(crate) fn close(&mut self, error: ProgressError) {
        self.submission.close(error);
    }

    /// Bound each scan by ready records. True means another contiguous record
    /// remains ready and the owner should schedule another turn without waiting.
    pub(super) fn advance(&mut self, maximum: usize) -> bool {
        for _ in 0..maximum {
            let index = (self.processed - self.retired) as usize;
            if self.completed.get(index) != Some(&true) {
                return false;
            }
            self.processed += 1;
        }
        let index = (self.processed - self.retired) as usize;
        self.completed.get(index) == Some(&true)
    }

    /// Terminal failure or requested closure, if any.
    pub(crate) fn stopped(&self) -> Option<ProgressError> {
        self.progress.stopping()
    }

    /// Every delivered record is confirmed, or this window stopped.
    pub(crate) fn drained(&self) -> bool {
        self.progress.confirmed() >= self.delivered || self.progress.stopping().is_some()
    }

    #[cfg(test)]
    pub(crate) fn processed(&self) -> Offset {
        Offset::new(self.processed)
    }

    /// Confirmation releases tracked holes, with a count bound independent of
    /// the cumulative reply's span. Payload aliases belong to the existing reader.
    pub(crate) fn retire(&mut self, maximum: usize) -> bool {
        let confirmed = self.progress.confirmed();
        for _ in 0..maximum {
            if self.retired == confirmed {
                return false;
            }
            self.completed
                .pop_front()
                .expect("confirmed delivered record");
            self.retired += 1;
        }
        self.retired < confirmed
    }
}
