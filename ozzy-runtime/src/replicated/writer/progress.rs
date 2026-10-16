//! Confirmation publication and terminal failure shared by pending handles.
use super::{AppendKey, Policy, ProducerId, RecordReceipt, WriterConfig, WriterError};
use crate::signal::StateSignal;
use ozzy_proto::{MessageId, PartitionIncarnation};

#[cfg(all(test, ozzy_loom))]
use loom::sync::atomic::{AtomicU8, AtomicU64, Ordering};
#[cfg(not(all(test, ozzy_loom)))]
use std::sync::{
    OnceLock,
    atomic::{AtomicU8, AtomicU64, Ordering},
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
    // One owner publishes confirmations. Failure may seal from another thread;
    // an active publisher finishes before observers can see the terminal seal.
    transition: AtomicU8,
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
            transition: AtomicU8::new(0),
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
        disposition: ozzy_proto::append::AppendDisposition,
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
            disposition,
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
        if self.transition.load(Ordering::Acquire) != SEALED {
            return None;
        }
        let failure = self.failure.get().expect("failure precedes terminal seal");
        // The terminal seal follows any publisher that won admission before it.
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
        self.failure.get_or_init(|| error);
        self.transition.fetch_or(SEALED, Ordering::AcqRel);
        self.changed.notify_changed();
    }

    pub(super) fn confirm<T>(
        &self,
        first: u64,
        end: u64,
        ready_next: u64,
        retire: impl FnOnce(usize) -> T,
    ) -> Result<T, WriterError> {
        let confirmed = self.confirmed();
        if first > confirmed || first >= end || end > ready_next || end < confirmed {
            return Err(super::Error::Response.into());
        }
        let count = usize::try_from(end - confirmed).map_err(|_| super::Error::Response)?;
        self.transition
            .compare_exchange(0, PUBLISHING, Ordering::Acquire, Ordering::Acquire)
            .map_err(|state| {
                assert_ne!(state & SEALED, 0, "one confirmation publisher");
                self.failure.get().expect("failure precedes seal").clone()
            })?;
        let publication = Publication(self);
        let retired = retire(count);
        self.confirmed.store(end, Ordering::Release);
        drop(publication);
        Ok(retired)
    }
}

const PUBLISHING: u8 = 1;
const SEALED: u8 = 2;

struct Publication<'a>(&'a Progress);

impl Drop for Publication<'_> {
    fn drop(&mut self) {
        self.0.transition.fetch_and(!PUBLISHING, Ordering::Release);
        self.0.changed.notify_changed();
    }
}
