//! History a source retired: boundary notices, and the requests that met one.

use super::{
    ActorError, FetchOps, FetchPurpose, HistoryReason, NodeId, PendingIo, Prefix, ReplicaActor,
    Scope, SendClass, TransferPurpose, wire,
};
use crate::replica_actor::control_message;

impl ReplicaActor {
    pub(in crate::replica_actor) fn complete_retired_lookup(
        &mut self,
        positions: &crate::replica_journal::ReplicationPositions,
        to: NodeId,
        request: FetchOps,
    ) -> Result<(), ActorError> {
        if !self.application_ready()
            || positions.ticket.scope() != self.driver.scope()
            || request.scope != self.driver.scope()
            || self.configuration.primary(request.scope.view) != self.local
        {
            return Ok(());
        }
        let retired = request.predecessor.op < positions.retained_predecessor.op;
        match positions.positions[0] {
            // A position not read is no evidence either way.
            None if !retired => return Ok(()),
            Some(held) if !retired && held == request.predecessor => {}
            // A retired predecessor, or another prefix at its op: name the
            // boundary to rescan from.
            _ => {
                return self.notify_retired(
                    to,
                    request.scope,
                    wire::HistoryFence::Fetch(request.request_id),
                    positions.retained_predecessor,
                );
            }
        }
        let Some(buffer) = self.buffer.take() else {
            return Ok(());
        };
        self.pending = Some(PendingIo::Fetch(
            self.journal
                .fetch_history(request, buffer)
                .map_err(|rejected| rejected.reason)?,
            FetchPurpose::Serve(to),
        ));
        Ok(())
    }

    pub(in crate::replica_actor) fn notify_retired(
        &mut self,
        to: NodeId,
        scope: Scope,
        fence: wire::HistoryFence,
        before: Prefix,
    ) -> Result<(), ActorError> {
        let Some(session) = self.session(to) else {
            return Ok(());
        };
        let encoded = wire::encode_history_retired(
            self.local,
            session,
            wire::HistoryRetired {
                scope,
                fence,
                before,
            },
            &mut self.metadata,
            self.wire_limits,
        )?;
        self.enqueue(
            to,
            SendClass::Control,
            control_message(encoded, &self.metadata),
        )
    }

    pub(in crate::replica_actor) fn receive_retired(
        &mut self,
        from: NodeId,
        notice: wire::HistoryRetired,
    ) -> Result<(), ActorError> {
        if notice.scope != self.driver.scope() {
            return Ok(());
        }
        let behind = match notice.fence {
            wire::HistoryFence::Receive(epoch) => {
                from == self.configuration.primary(notice.scope.view)
                    && self.work.receive.ledger.report().channel.epoch == epoch
                    && self
                        .driver
                        .normal()
                        .is_some_and(|normal| normal.snapshot().accepted.op < notice.before.op)
            }
            wire::HistoryFence::Fetch(id) => self.retired_transfer(from, id, notice)?,
        };
        if behind {
            self.recovery_required = true;
            self.ingress.close();
        }
        Ok(())
    }

    /// The donor retired history that transfer `id` asked for. A lookup scans
    /// on from the boundary or bridges it; `true` when this copy fell behind.
    fn retired_transfer(
        &mut self,
        from: NodeId,
        id: ozzy_proto::RequestId,
        notice: wire::HistoryRetired,
    ) -> Result<bool, ActorError> {
        let Some(transfer) = self.transfer.filter(|transfer| {
            transfer.request.request_id == id
                && transfer.request.scope == notice.scope
                && transfer.request.source.voter == from
                && notice.before.op <= transfer.request.source.accepted.op
        }) else {
            return Ok(false);
        };
        if let TransferPurpose::Install = transfer.purpose {
            return self.retired_install(transfer.request, notice);
        }
        if notice.before.op <= transfer.request.predecessor.op {
            // The source does not hold the expected prefix a lookup started
            // from. Rescan from its boundary; the digest found there is
            // evidence once the scan reaches the tail.
            if notice.before != transfer.request.predecessor
                && let TransferPurpose::Lookup { op, .. } = transfer.purpose
            {
                self.lookup.scan.restart();
                self.start_transfer(
                    notice.scope,
                    transfer.request.source,
                    notice.before,
                    TransferPurpose::Lookup {
                        op,
                        found: (op == notice.before.op).then_some(notice.before),
                    },
                )?;
            }
            return Ok(false);
        }
        if let TransferPurpose::Lookup { op, .. } = transfer.purpose
            && op >= notice.before.op
        {
            if notice.before.op == transfer.request.source.accepted.op {
                if notice.before == transfer.request.source.accepted {
                    self.lookup
                        .verified(transfer.request.source, notice.before)?;
                    self.transfer = None;
                }
                return Ok(false);
            }
            // The boundary is only a scan starting point. No digest
            // enters election evidence until the exact tail verifies.
            self.lookup.scan.restart();
            self.start_transfer(
                notice.scope,
                transfer.request.source,
                notice.before,
                TransferPurpose::Lookup {
                    op,
                    found: (op == notice.before.op).then_some(notice.before),
                },
            )?;
        } else if let TransferPurpose::Lookup { op, .. } = transfer.purpose
            && self.pinned.is_some_and(|local| {
                local.voter == self.local && local.accepted.op >= notice.before.op
            })
        {
            self.bridge_or_retire(transfer.request.source, op, notice.before);
            self.transfer = None;
        } else {
            return Ok(true);
        }
        Ok(false)
    }

    /// The source retired history that installation `request` asked for. A
    /// backup stages from its own commit, which no lookup checked, so other
    /// history there contradicts that commit. A retired start is proved
    /// through the local copy, as a lookup would; staging then reads that
    /// prefix locally. `true` when this copy fell behind.
    fn retired_install(
        &mut self,
        request: FetchOps,
        notice: wire::HistoryRetired,
    ) -> Result<bool, ActorError> {
        if notice.before.op <= request.predecessor.op {
            if notice.before != request.predecessor {
                return Err(ActorError::history(HistoryReason::Transfer));
            }
            return Ok(false);
        }
        let covered = self.pinned.is_some_and(|local| {
            local.voter == self.local && local.accepted.op >= notice.before.op
        });
        if !covered
            || self
                .lookup
                .unavailable(request.source, request.predecessor.op)
        {
            return Ok(true);
        }
        self.bridge_or_retire(request.source, request.predecessor.op, notice.before);
        self.transfer = None;
        Ok(false)
    }
}
