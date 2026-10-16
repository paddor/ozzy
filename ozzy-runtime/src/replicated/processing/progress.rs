//! Only the unique owner publishes progress or seals terminal failure.

use super::{ProcessingIdentity, ProgressError};
use crate::signal::StateSignal;
use std::{mem::size_of, sync::Arc};
use tokio::sync::OwnedSemaphorePermit;

#[cfg(all(test, ozzy_loom))]
use loom::sync::atomic::{AtomicU64, Ordering};
#[cfg(not(all(test, ozzy_loom)))]
use std::sync::{
    OnceLock,
    atomic::{AtomicU64, Ordering},
};
#[cfg(all(test, ozzy_loom))]
#[path = "../writer/progress/once_lock.rs"]
mod once_lock;
#[cfg(all(test, ozzy_loom))]
use once_lock::OnceLock;

#[derive(Debug)]
struct Shared {
    identity: ProcessingIdentity,
    confirmed: AtomicU64,
    submitted: AtomicU64,
    delivered: AtomicU64,
    closing: OnceLock<ProgressError>,
    failure: OnceLock<ProgressError>,
    changed: StateSignal,
    // A token or pending confirmation keeps this reservation until its last alias.
    _permit: Option<OwnedSemaphorePermit>,
}

pub(super) const RESERVATION_BYTES: usize = size_of::<Shared>() + 2 * size_of::<usize>();

/// Mutable publisher is never cloned or shared. Confirmation and failure are
/// ordered by its owner; observers cannot race another writer into the prefix.
#[derive(Debug)]
pub(crate) struct Publisher {
    shared: Arc<Shared>,
    submission_taken: bool,
    activity: Option<Arc<StateSignal>>,
}

/// The caller owns this single cumulative intent writer. Tokens and futures
/// hold only observers, so neither can submit or close another window.
#[derive(Debug)]
pub(super) struct Submission {
    shared: Arc<Shared>,
    cursor: u64,
}

/// Immutable identity plus independently readable published state.
#[derive(Debug, Clone)]
pub(crate) struct Observer(Arc<Shared>);

impl Publisher {
    pub(super) fn new(
        identity: ProcessingIdentity,
        cursor: u64,
        permit: Option<OwnedSemaphorePermit>,
    ) -> Self {
        Self {
            submission_taken: false,
            activity: None,
            shared: Arc::new(Shared {
                identity,
                confirmed: AtomicU64::new(cursor),
                submitted: AtomicU64::new(cursor),
                delivered: AtomicU64::new(cursor),
                closing: OnceLock::new(),
                failure: OnceLock::new(),
                changed: StateSignal::default(),
                _permit: permit,
            }),
        }
    }

    /// Fan confirmations and closure into one reader/membership wakeup.
    pub(crate) fn notify_on(&mut self, activity: Arc<StateSignal>) {
        self.activity = Some(activity);
    }

    fn notify(&self) {
        self.shared.changed.notify_changed();
        if let Some(activity) = &self.activity {
            activity.notify_changed();
        }
    }

    pub(super) fn observer(&self) -> Observer {
        Observer(self.shared.clone())
    }

    pub(super) fn take_submission(&mut self) -> Option<Submission> {
        if self.submission_taken {
            return None;
        }
        self.submission_taken = true;
        Some(Submission {
            shared: self.shared.clone(),
            cursor: self.shared.submitted.load(Ordering::Relaxed),
        })
    }

    pub(super) fn submitted(&self) -> u64 {
        self.shared.submitted.load(Ordering::Acquire)
    }

    /// A ready prefix stays visible until its broker confirmation. Capture the
    /// generation before reading intent, including intent published before poll.
    pub(super) async fn ready(&self) -> Result<u64, ProgressError> {
        self.shared
            .changed
            .wait_for(|| self.observe_submission())
            .await
    }

    pub(super) fn observe_submission(&self) -> Option<Result<u64, ProgressError>> {
        if let Some(error) = self
            .shared
            .closing
            .get()
            .or_else(|| self.shared.failure.get())
        {
            return Some(Err(*error));
        }
        let cursor = self.submitted();
        (cursor > self.shared.confirmed.load(Ordering::Relaxed)).then_some(Ok(cursor))
    }

    /// The owner validates the authenticated response and supplies its greatest
    /// submitted contiguous cursor. Neither delivery nor submission is proof.
    pub(crate) fn confirm(&mut self, cursor: u64, submitted: u64) -> Result<(), ProgressError> {
        if let Some(error) = self.shared.failure.get() {
            return Err(*error);
        }
        let prior = self.shared.confirmed.load(Ordering::Relaxed);
        if cursor > submitted {
            return Err(ProgressError::Response);
        }
        if cursor > prior {
            self.shared.confirmed.store(cursor, Ordering::Release);
            self.notify();
        }
        Ok(())
    }

    pub(crate) fn fail(&mut self, error: ProgressError) {
        if self.shared.failure.get().is_none() {
            self.shared.failure.get_or_init(|| error);
            self.notify();
        }
    }
}

impl Drop for Publisher {
    fn drop(&mut self) {
        self.fail(ProgressError::Closed);
    }
}

impl Submission {
    pub(super) fn record_delivery(&mut self, cursor: u64) {
        self.shared.delivered.store(cursor, Ordering::Release);
    }
    pub(super) fn publish(&mut self, cursor: u64) -> Result<(), ProgressError> {
        if let Some(error) = self
            .shared
            .closing
            .get()
            .or_else(|| self.shared.failure.get())
        {
            return Err(*error);
        }
        if cursor < self.cursor {
            return Err(ProgressError::Response);
        }
        if cursor > self.cursor {
            self.shared.submitted.store(cursor, Ordering::Release);
            self.cursor = cursor;
            self.shared.changed.notify_changed();
        }
        Ok(())
    }

    /// The SDK owner orders this request against its last confirmation, then
    /// seals failure. Requesting closure does not create another publisher.
    pub(super) fn close(&mut self, error: ProgressError) {
        if self.shared.closing.get().is_none() {
            self.shared.closing.get_or_init(|| error);
            self.shared.changed.notify_changed();
        }
    }
}

impl Drop for Submission {
    fn drop(&mut self) {
        self.close(ProgressError::Closed);
    }
}

impl Observer {
    pub(super) fn identity(&self) -> ProcessingIdentity {
        self.0.identity
    }

    pub(super) fn same_window(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    pub(crate) fn confirmed(&self) -> u64 {
        self.0.confirmed.load(Ordering::Acquire)
    }

    pub(crate) fn delivered(&self) -> u64 {
        self.0.delivered.load(Ordering::Acquire)
    }

    pub(super) fn failure(&self) -> Option<ProgressError> {
        self.0.failure.get().copied()
    }

    pub(crate) fn stopping(&self) -> Option<ProgressError> {
        self.0
            .closing
            .get()
            .or_else(|| self.0.failure.get())
            .copied()
    }

    pub(super) fn observe(&self, cursor: u64) -> Option<Result<(), ProgressError>> {
        if cursor <= self.confirmed() {
            return Some(Ok(()));
        }
        let error = self.failure()?;
        // Failure follows the owner's last confirmation. Reacquire that prefix
        // before returning an error so a racing earlier confirmation stays valid.
        Some(if cursor <= self.confirmed() {
            Ok(())
        } else {
            Err(error)
        })
    }

    pub(super) async fn wait(&self, cursor: u64) -> Result<(), ProgressError> {
        self.0.changed.wait_for(|| self.observe(cursor)).await
    }

    pub(super) async fn closing(&self) -> ProgressError {
        self.0
            .changed
            .wait_for(|| {
                self.0
                    .closing
                    .get()
                    .or_else(|| self.0.failure.get())
                    .copied()
            })
            .await
    }
}
