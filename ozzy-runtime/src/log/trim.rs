//! Trimming: the writer gives up the records below a floor, and every copy
//! frees the segments that hold nothing else.

use super::{Command, Log, Pending, SubmitError};
use ozzy_proto::Offset;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::oneshot;

/// Why a trim did not commit. `Unknown` ends the writer's term; after any
/// other one it goes on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum TrimError {
    /// The log's floor is not the expected one.
    #[error("the log's floor is {current:?}")]
    Floor {
        /// The log's floor.
        current: Offset,
    },
    /// The new floor is not above the expected one, or lies past the log's
    /// end.
    #[error("floor does not move forward within the log")]
    Invalid,
    /// The journal's own bookkeeping moved while the trim waited. Nothing
    /// changed; send it again.
    #[error("journal moved under the trim")]
    Retry,
    /// The trim names another view than the one this broker leads.
    #[error("trim names view {named}, the log is in view {current}")]
    View {
        /// View the trim named.
        named: u64,
        /// View this broker leads.
        current: u64,
    },
    /// This broker does not lead the log now, or it leads in doubt.
    #[error("this broker does not lead the log")]
    NotLeader,
    /// The view changed after admission. The floor may or may not have
    /// moved; the next leader's status tells.
    #[error("trim outcome unknown after a view change")]
    Unknown,
    /// The service or its partition stopped.
    #[error("log stopped")]
    Stopped,
}

#[derive(Debug)]
pub(crate) struct TrimRequest {
    pub(crate) view: u64,
    pub(crate) expected: Offset,
    pub(crate) floor: Offset,
    pub(crate) reply: oneshot::Sender<Result<(), TrimError>>,
}

impl Log {
    /// Move the log's floor from `expected_floor` to `floor` in `view`:
    /// records below `floor` are gone for every reader. Each copy then
    /// frees its sealed segments that hold no record at or above the
    /// floor; the segment being written stays. One trim runs at a time.
    ///
    /// # Errors
    ///
    /// `Busy` when the queue is full, `Stopped` after the service ends.
    pub fn trim(
        &self,
        view: u64,
        expected_floor: Offset,
        floor: Offset,
    ) -> Result<Pending<(), TrimError>, SubmitError> {
        let (reply, pending) = oneshot::channel();
        self.submit(
            Command::Trim(TrimRequest {
                view,
                expected: expected_floor,
                floor,
                reply,
            }),
            0,
        )?;
        Ok(Pending(pending))
    }
}

impl Future for Pending<(), TrimError> {
    type Output = Result<(), TrimError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.poll_reply(cx, TrimError::Stopped)
    }
}
