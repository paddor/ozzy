//! Watch snapshot replies and route notices from one broker.

use super::{
    BrokerLinkError, Duration, GroupId, LinkSessionId, Opcode, Packet, Rejection, RequestId,
    Watcher, directory, nack,
};

impl Watcher {
    /// Consume a watch reply or notice on the live `session`. `false` when
    /// `packet` is neither.
    pub(in crate::replicated::broker_links) fn receive(
        &mut self,
        packet: Packet<'_>,
        session: Option<LinkSessionId>,
        now: Duration,
    ) -> Result<bool, BrokerLinkError> {
        if packet.envelope.session != session || session.is_none() {
            return Ok(false);
        }
        let reply = self
            .pending
            .as_ref()
            .filter(|pending| {
                pending.sent
                    && packet.envelope.response
                    && packet.envelope.request_id == Some(pending.request)
            })
            .map(|pending| (pending.group, pending.revision));
        if let Some((group, revision)) = reply {
            return self.receive_reply(packet, now, group, revision);
        }
        let resync = match (packet.envelope.opcode, packet.envelope.response) {
            (Opcode::StateUpdate, false) => false,
            (Opcode::StateResync, false) => true,
            _ => return Ok(false),
        };
        let Some(watch) = packet
            .metadata
            .get(1..17)
            .and_then(|bytes| bytes.try_into().ok())
            .map(RequestId::from_bytes)
        else {
            return Ok(true);
        };
        let Some(&group) = self.ids.get(&watch) else {
            return Ok(true);
        };
        let Some(entry) = self.entries.get_mut(&group) else {
            return Ok(true);
        };
        let session = entry.session;
        if packet.envelope.session != Some(session) {
            return Ok(true);
        }
        let limits = self.limits;
        if resync {
            let notice = directory::decode_resync(packet, limits)?;
            entry.update(self.remote, group, |cache| {
                cache.resync(self.remote, session, notice)
            })?;
        } else {
            let update = directory::decode_update(packet, limits, directory::Limits::default())?;
            entry.update(self.remote, group, |cache| {
                cache.update(self.remote, session, update)
            })?;
        }
        entry.state.changed.notify_changed();
        self.rescan();
        Ok(true)
    }

    /// Answer to the outstanding snapshot request for `group` at `revision`.
    fn receive_reply(
        &mut self,
        packet: Packet<'_>,
        now: Duration,
        group: GroupId,
        revision: u64,
    ) -> Result<bool, BrokerLinkError> {
        if !matches!(packet.envelope.opcode, Opcode::StateSnapshot | Opcode::Nack) {
            return Ok(true);
        }
        let limits = self.limits;
        if packet.envelope.opcode == Opcode::Nack {
            let reply = nack::decode(packet, limits)?;
            self.retry_pending(now)?;
            if reply.retry == nack::RetryClass::Permanent
                && let Some(entry) = self.entries.get_mut(&group)
            {
                entry.rejected = Some(revision);
                entry.state.reject(
                    group,
                    self.remote,
                    Rejection {
                        session: entry.session,
                        code: reply.code,
                        retry: reply.retry,
                    },
                );
                entry.state.changed.notify_changed();
            }
            return Ok(true);
        }
        let snapshot = directory::decode_snapshot(packet, limits, directory::Limits::default())?;
        let Some(entry) = self.entries.get_mut(&group) else {
            self.pending = None;
            return Ok(true);
        };
        if snapshot.watch != entry.watch {
            return Ok(true);
        }
        let session = entry.session;
        entry.update(self.remote, group, |cache| {
            cache.snapshot(self.remote, session, snapshot)
        })?;
        entry.state.clear_rejection(group, self.remote);
        entry.revision = revision;
        self.timers.remove(&(entry.retry_at, group));
        entry.retry_at = Duration::ZERO;
        entry.rejected = None;
        entry.state.changed.notify_changed();
        self.pending = None;
        self.rescan();
        Ok(true)
    }
}
