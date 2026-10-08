//! Confirmation publication and terminal failure shared by pending handles.
use super::{AppendKey, Policy, ProducerId, RecordReceipt, WriterConfig, WriterError};
use crate::signal::StateSignal;
use ozzy_proto::{MessageId, PartitionIncarnation};

#[cfg(all(test, ozzy_loom))]
use loom::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};
#[cfg(not(all(test, ozzy_loom)))]
use std::sync::{
    Mutex, OnceLock,
    atomic::{AtomicU64, Ordering},
};
#[cfg(all(test, ozzy_loom))]
#[path = "progress/once_lock.rs"]
mod once_lock;
#[cfg(all(test, ozzy_loom))]
use once_lock::OnceLock;

/// Completion state and immutable writer identity shared by pending handles.
/// Contains no payloads or admission queues.
#[derive(Debug)]
pub(super) struct Progress {
    confirmed: AtomicU64,
    pub(super) failure: OnceLock<WriterError>,
    // Failure seals the confirmed prefix. Serialize that terminal transition
    // with confirmation publication so observations never change afterward.
    transition: Mutex<Option<u64>>,
    pub(super) changed: StateSignal,
    partition: PartitionIncarnation,
    owner_epoch: u64,
    producer_id: ProducerId,
    producer_epoch: u64,
    policy: Policy,
}

impl Progress {
    pub(super) fn new(config: &WriterConfig) -> Self {
        Self {
            confirmed: AtomicU64::new(config.next_sequence),
            failure: OnceLock::new(),
            transition: Mutex::new(None),
            changed: StateSignal::default(),
            partition: config.partition,
            owner_epoch: config.owner_epoch,
            producer_id: config.producer_id,
            producer_epoch: config.producer_epoch,
            policy: config.policy,
        }
    }

    pub(super) fn receipt(
        &self,
        sequence: u64,
        message_id: MessageId,
        offset: u64,
    ) -> RecordReceipt {
        RecordReceipt {
            partition: self.partition,
            owner_epoch: self.owner_epoch,
            key: AppendKey {
                producer_id: self.producer_id,
                producer_epoch: self.producer_epoch,
                first_sequence: sequence,
            },
            message_id,
            offset,
            policy: self.policy,
        }
    }

    pub(super) async fn wait(&self, target: u64) -> Result<(), WriterError> {
        self.changed.wait_for(|| self.observe(target)).await
    }

    pub(super) fn observe(&self, target: u64) -> Option<Result<(), WriterError>> {
        if target <= self.confirmed() {
            return Some(Ok(()));
        }
        let failure = self.failure.get()?;
        // Progress serializes confirmation and terminal failure publication.
        // Reload after acquiring failure so a racing earlier confirmation wins.
        if target <= self.confirmed() {
            Some(Ok(()))
        } else {
            Some(Err(failure.clone()))
        }
    }

    pub(super) fn confirmed(&self) -> u64 {
        self.confirmed.load(Ordering::Acquire)
    }

    pub(super) fn fail(&self, error: WriterError) {
        let _transition = self
            .transition
            .lock()
            .expect("writer progress transition poisoned");
        self.failure.get_or_init(|| error);
    }

    pub(super) fn confirm<T>(
        &self,
        first: u64,
        end: u64,
        first_offset: u64,
        ready_next: u64,
        retire: impl FnOnce(usize) -> T,
    ) -> Result<T, WriterError> {
        let mut transition = self
            .transition
            .lock()
            .expect("writer progress transition poisoned");
        if let Some(error) = self.failure.get() {
            return Err(error.clone());
        }
        let confirmed = self.confirmed();
        if first > confirmed || first >= end || end > ready_next || end < confirmed {
            return Err(super::Error::Response.into());
        }
        let count = usize::try_from(end - confirmed).map_err(|_| super::Error::Response)?;
        let last_offset = first_offset
            .checked_add(end - first - 1)
            .ok_or(super::Error::Response)?;
        if transition.is_some_and(|last| {
            if first < confirmed {
                first_offset.checked_add(confirmed - first - 1) != Some(last)
            } else {
                first_offset <= last
            }
        }) {
            return Err(super::Error::Response.into());
        }
        let retired = retire(count);
        *transition = Some(last_offset);
        self.confirmed.store(end, Ordering::Release);
        drop(transition);
        self.changed.notify_changed();
        Ok(retired)
    }
}
