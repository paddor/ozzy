use ozzy_replication::driver::ActivationTicket;
use ozzy_replication::{InstallTicket, PromiseTicket, SyncTicket, WriteTicket};

use super::*;

/// Atomic stable metadata plus complete synchronized operations. Corrupt
/// sectors and lying devices are not modeled here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DiskImage {
    pub admitted: bool,
    pub promised: Scope,
    pub last_normal_view: u64,
    pub committed: Prefix,
    pub operations: Vec<Operation>,
}

impl DiskImage {
    pub(super) fn new(scope: Scope) -> Self {
        Self {
            admitted: true,
            promised: scope,
            last_normal_view: 0,
            committed: Prefix::GENESIS,
            operations: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum DiskAction {
    Write(WriteTicket),
    Sync(SyncTicket),
    Promise(PromiseTicket),
    Install {
        ticket: InstallTicket,
        operations: Vec<Operation>,
    },
    Activate(ActivationTicket),
    Recovery(super::recovery::RecoveryDisk),
}

#[derive(Debug)]
pub(super) struct PendingDisk {
    action: DiskAction,
    performed: bool,
    application: Option<Prefix>,
}

impl Replica {
    pub(super) fn enqueue(&mut self, action: DiskAction) {
        assert!(self.io.len() < PIPELINE + 2);
        self.io.push_back(PendingDisk {
            action,
            performed: false,
            application: None,
        });
    }

    pub(crate) fn pending_disk(&self) -> Option<&DiskAction> {
        self.io.front().map(|pending| &pending.action)
    }

    /// Successful physical work, deliberately independent of its callback.
    pub(crate) fn perform_disk(&mut self) -> bool {
        let Some(pending) = self.io.front().filter(|pending| !pending.performed) else {
            return false;
        };
        let action = pending.action.clone();
        if let Err(error) = self.perform_storage(&action) {
            self.storage_error = Some(error.to_string());
            self.power_cut();
            return true;
        }
        let Some(pending) = self.io.front_mut() else {
            return false;
        };
        if pending.performed {
            return false;
        }
        match &pending.action {
            DiskAction::Write(ticket) => {
                assert_eq!(ticket.generation(), self.generation);
                assert_eq!(self.buffered.len() + 1, ticket.first().0 as usize);
                self.buffered.extend_from_slice(
                    &self.accepted[ticket.first().0 as usize - 1..ticket.through().0 as usize],
                );
            }
            DiskAction::Sync(ticket) => {
                assert_eq!(ticket.generation(), self.generation);
                let through = ticket.through().0 as usize;
                assert!(through <= self.buffered.len());
                self.stable.operations = self.buffered[..through].to_vec();
            }
            DiskAction::Promise(ticket) => {
                assert_eq!(ticket.generation(), self.generation);
                assert_eq!(ticket.log().accepted, tail(&self.stable.operations));
                assert_eq!(self.buffered, self.stable.operations);
                assert!(ticket.scope().view >= self.stable.promised.view);
                assert_eq!(ticket.log().last_normal_view, self.stable.last_normal_view);
                assert!(ticket.log().committed.op >= self.stable.committed.op);
                self.stable.promised = ticket.scope();
                self.stable.committed = ticket.log().committed;
            }
            DiskAction::Install { ticket, operations } => {
                assert_eq!(ticket.previous_generation(), self.generation);
                assert!(ticket.scope().view >= self.stable.promised.view);
                assert_eq!(tail(operations), ticket.accepted());
                assert_eq!(
                    prefix(operations, ticket.committed().op.0 as usize),
                    ticket.committed()
                );
                let protected = self.stable.committed.op.0 as usize;
                assert_eq!(
                    &self.stable.operations[..protected],
                    &operations[..protected]
                );
                self.stable = DiskImage {
                    admitted: true,
                    promised: ticket.scope(),
                    last_normal_view: ticket.scope().view,
                    committed: ticket.committed(),
                    operations: operations.clone(),
                };
            }
            DiskAction::Activate(ticket) => {
                assert_eq!(ticket.generation(), self.generation);
                assert_eq!(ticket.scope(), self.stable.promised);
                assert_eq!(ticket.through(), tail(&self.stable.operations));
                self.stable.committed = ticket.through();
            }
            DiskAction::Recovery(action) => {
                pending.application =
                    super::recovery::perform(action, &mut self.stable, &mut self.buffered);
            }
        }
        pending.performed = true;
        if let Some(storage) = &self.storage {
            let decoded = super::storage::image(storage.snapshot().unwrap(), self.configuration);
            assert_eq!(
                decoded, self.stable,
                "byte recovery differs from action contract"
            );
            self.stable = decoded;
        }
        true
    }

    pub(crate) fn notify_disk(&mut self, now: Duration) -> Option<DiskAction> {
        if !self.io.front().is_some_and(|pending| pending.performed) {
            return None;
        }
        let pending = self.io.pop_front().unwrap();
        let action = pending.action;
        if let DiskAction::Recovery(recovery) = &action {
            self.complete_recovery_disk(recovery, pending.application, now);
            return Some(action);
        }
        let driver = self.driver.as_mut().unwrap();
        match &action {
            DiskAction::Write(ticket) => driver.complete_write(*ticket).unwrap(),
            DiskAction::Sync(ticket) => {
                assert!(ticket.through().0 as usize <= self.stable.operations.len());
                driver.complete_sync(*ticket, now).unwrap();
                self.observed_sync = ticket.through().0 as usize;
            }
            DiskAction::Promise(ticket) => {
                driver.complete_promise(*ticket).unwrap();
                self.pin_current();
            }
            DiskAction::Install { ticket, operations } => {
                self.generation = ticket.generation();
                self.observed_sync = operations.len();
                self.accepted.clone_from(operations);
                self.buffered.clone_from(operations);
                driver
                    .complete_installation(*ticket, ticket.committed(), now)
                    .unwrap();
                self.pin_current();
            }
            DiskAction::Activate(ticket) => {
                match driver.complete_activation(*ticket) {
                    Ok(()) => self.images = self.candidate.take().expect("private selected image"),
                    Err(DriverError::StaleValidation) => {} // Later view fenced the callback.
                    Err(error) => panic!("activation callback failed: {error:?}"),
                }
            }
            DiskAction::Recovery(_) => unreachable!("handled before normal driver access"),
        }
        Some(action)
    }
}
