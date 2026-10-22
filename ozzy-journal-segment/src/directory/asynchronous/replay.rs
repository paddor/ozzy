//! Replay of accepted operations after the checkpoint or a given position.

use super::{DirectoryError, Journal, ReplayError, ReplayedOperation};
use crate::LogPosition;

impl Journal {
    /// Replay after the selected checkpoint. Each callback sees one validated
    /// operation borrowed from one physical group, never a filesystem handle.
    pub async fn replay_accepted<E>(
        &self,
        mut visit: impl FnMut(ReplayedOperation<'_>) -> Result<(), E>,
    ) -> Result<(), ReplayError<E>>
    where
        E: std::error::Error + 'static,
    {
        self.replay_accepted_async(async |item| visit(item)).await
    }

    /// Replay with asynchronous visitors, for example to resolve cold control
    /// identities before synchronous canonical-state validation. One bounded
    /// physical group owns the borrowed operation across the visitor's awaits.
    /// The caller must discard partial results if final segment authority fails.
    pub async fn replay_accepted_async<E>(
        &self,
        visit: impl AsyncFnMut(ReplayedOperation<'_>) -> Result<(), E>,
    ) -> Result<(), ReplayError<E>>
    where
        E: std::error::Error + 'static,
    {
        let start = self
            .manifest
            .checkpoint
            .map_or(LogPosition::GENESIS, |value| value.position);
        self.replay_accepted_after_async(start, visit).await
    }

    /// Replay the accepted operations after `start` instead of the checkpoint.
    /// Segments wholly before it are not read. `start` must be at or after the
    /// checkpoint and in this history: its op, or the chain a segment starts
    /// from, must carry its digest.
    pub(crate) async fn replay_accepted_after_async<E>(
        &self,
        start: LogPosition,
        mut visit: impl AsyncFnMut(ReplayedOperation<'_>) -> Result<(), E>,
    ) -> Result<(), ReplayError<E>>
    where
        E: std::error::Error + 'static,
    {
        self.healthy().map_err(ReplayError::Journal)?;
        let accepted = self.accepted_position().map_err(ReplayError::Journal)?;
        let committed = self.committed_position().map_err(ReplayError::Journal)?;
        let mut replayed = start;
        let mut anchored = start.op_number == 0;
        let mut budget = crate::cooperative::Budget::default();
        let segments = &self.manifest.segments;
        for (at, reference) in segments.iter().enumerate() {
            if segments
                .get(at + 1)
                .is_some_and(|next| next.first_chain.next_op_number() <= start.op_number)
            {
                continue;
            }
            if reference.first_chain.next_op_number() == start.op_number + 1 {
                anchored = reference.first_chain.previous_digest() == start.digest;
            }
            let mut groups = self
                .segment_groups(*reference)
                .await
                .map_err(ReplayError::Journal)?;
            while let Some(group) = groups.next().await.map_err(ReplayError::Journal)? {
                for operation in &group.operations {
                    budget.charge(operation.body.len()).await;
                    if operation.op_number == start.op_number {
                        anchored = operation.digest == start.digest;
                    }
                    if operation.op_number <= start.op_number
                        || operation.op_number > accepted.op_number
                    {
                        continue;
                    }
                    visit(ReplayedOperation {
                        operation,
                        committed: operation.op_number <= committed.op_number,
                    })
                    .await
                    .map_err(ReplayError::Visitor)?;
                    replayed = LogPosition {
                        op_number: operation.op_number,
                        digest: operation.digest,
                    };
                }
            }
            groups.finish().await.map_err(ReplayError::Journal)?;
        }
        if !anchored {
            return Err(ReplayError::Journal(DirectoryError::PositionMismatch(
                start.op_number,
            )));
        }
        if replayed != accepted {
            return Err(ReplayError::Journal(DirectoryError::PositionMismatch(
                accepted.op_number,
            )));
        }
        Ok(())
    }
}
