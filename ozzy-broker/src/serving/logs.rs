//! The broker handle's side of runtime logs.

use super::Broker;
use crate::logs::{Command, CreateError, DeleteError, LogSetup, LogSpec, LogState, LogTable};
use ozzy_proto::GroupId;
use std::{collections::BTreeMap, future::Future, sync::Arc};
use tokio::sync::{mpsc, oneshot};

/// What the broker handle keeps to reach its shards' runtime logs.
#[derive(Debug)]
pub(super) struct LogHandles {
    pub(super) setup: Option<Arc<LogSetup>>,
    pub(super) lanes: BTreeMap<u32, mpsc::Sender<Command>>,
    pub(super) table: LogTable,
}

/// Why a command did not reach its shard.
enum Unsent {
    Disabled,
    Unplaced(String),
    Busy,
    Stopped,
}

impl LogHandles {
    /// Queue the command `build` makes for log `id` on the shard that owns
    /// it, and return where its answer arrives.
    fn send<T>(
        &self,
        id: GroupId,
        build: impl FnOnce(oneshot::Sender<T>) -> Command,
    ) -> Result<oneshot::Receiver<T>, Unsent> {
        let setup = self.setup.as_ref().ok_or(Unsent::Disabled)?;
        let shard = setup
            .shard(id)
            .map_err(|error| Unsent::Unplaced(error.to_string()))?;
        let lane = self.lanes.get(&shard).ok_or(Unsent::Stopped)?;
        let (reply, answer) = oneshot::channel();
        lane.try_send(build(reply)).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => Unsent::Busy,
            mpsc::error::TrySendError::Closed(_) => Unsent::Stopped,
        })?;
        Ok(answer)
    }
}

impl Broker {
    /// Create this broker's copy of a log. The embedder gives every member
    /// the same spec; a replicated log elects once two copies exist. A log
    /// that is ready with the same spec answers `Ok` again. Dropping the
    /// future does not cancel a creation that was queued.
    ///
    /// # Errors
    ///
    /// See [`CreateError`]. After `Busy` the same call may be repeated.
    pub fn create_log(
        &self,
        spec: LogSpec,
    ) -> impl Future<Output = Result<(), CreateError>> + Send + 'static + use<> {
        let id = spec.id;
        let sent = self.logs.send(id, |reply| Command::Create { spec, reply });
        async move {
            match sent {
                Ok(answer) => answer.await.unwrap_or(Err(CreateError::Stopped)),
                Err(Unsent::Disabled) => Err(CreateError::Disabled),
                Err(Unsent::Unplaced(reason)) => Err(CreateError::Failed(reason)),
                Err(Unsent::Busy) => Err(CreateError::Busy),
                Err(Unsent::Stopped) => Err(CreateError::Stopped),
            }
        }
    }

    /// Delete this broker's copy of a log: its routes go first, open
    /// requests end, the store is removed. The ID stays recorded as deleted
    /// and is never created here again. Deleting a deleted log answers `Ok`.
    ///
    /// # Errors
    ///
    /// See [`DeleteError`]. After `Busy` the same call may be repeated.
    pub fn delete_log(
        &self,
        id: GroupId,
    ) -> impl Future<Output = Result<(), DeleteError>> + Send + 'static + use<> {
        let sent = self.logs.send(id, |reply| Command::Delete { id, reply });
        async move {
            match sent {
                Ok(answer) => answer.await.unwrap_or(Err(DeleteError::Stopped)),
                Err(Unsent::Disabled) => Err(DeleteError::Unknown),
                Err(Unsent::Unplaced(reason)) => Err(DeleteError::Failed(reason)),
                Err(Unsent::Busy) => Err(DeleteError::Busy),
                Err(Unsent::Stopped) => Err(DeleteError::Stopped),
            }
        }
    }

    /// Every runtime log this broker records, deleted ones included, by ID.
    pub fn logs(&self) -> Vec<(GroupId, LogState)> {
        self.logs.table.states()
    }

    /// The handle of a ready runtime log. Its status tells whether this
    /// broker leads it.
    pub fn runtime_log(&self, id: GroupId) -> Option<ozzy_runtime::log::Log> {
        self.logs.table.log(id)
    }
}
