//! Directory snapshots and route watches: client topic and watch requests,
//! and the watch hints shards publish to them.

use super::{Link, ReceiveError, Service, ServiceError};
use crate::dispatch::Class;
use crate::frontend::{
    CatalogError, ReplyError, RouteState, WatchError, WatchNotice, WatchRegistry,
};
use bytes::Bytes;
use ozzy_proto::{ENVELOPE_BYTES, Envelope, EnvelopeLimits, NodeId, Opcode, directory, nack};

impl Service {
    pub(super) fn receive_topic(
        &mut self,
        peer: NodeId,
        packet: ozzy_proto::Packet<'_>,
    ) -> Result<(), ReceiveError> {
        let link = self.links.get(peer).ok_or(CatalogError::Unknown)?;
        if !link.client_session(peer, &packet.envelope) {
            return Err(ReceiveError::Role);
        }
        let Ok(request) = directory::decode_topic_request(
            packet,
            self.parameters.receive.envelope,
            directory::Limits::default(),
        ) else {
            return self.reject_directory(peer, packet.envelope, 1, nack::RetryClass::Permanent);
        };
        let max_metadata = link.send.envelope.max_metadata_bytes.min(
            self.dispatcher
                .limits
                .replies
                .control
                .message_bytes
                .saturating_sub(80),
        );
        let Some(catalog) = self.catalog.as_ref() else {
            return self.reject_directory(peer, packet.envelope, 2, nack::RetryClass::Permanent);
        };
        let page = match catalog.page(&request, max_metadata) {
            Ok(page) => page,
            Err(error) => {
                let code = if error == CatalogError::Unknown {
                    18
                } else {
                    1
                };
                return self.reject_directory(
                    peer,
                    packet.envelope,
                    code,
                    nack::RetryClass::Permanent,
                );
            }
        };
        let mut metadata = Vec::with_capacity(page.metadata_bytes()?);
        let header = directory::encode_topic_page(
            self.directory_response(packet.envelope, &link),
            &page,
            &mut metadata,
            link.send.envelope,
            directory::Limits::default(),
        )?;
        self.send_directory_reply(peer, header, &metadata)
    }

    fn reject_directory(
        &mut self,
        peer: NodeId,
        request: Envelope,
        code: u16,
        retry: nack::RetryClass,
    ) -> Result<(), ReceiveError> {
        let link = self.links.get(peer).ok_or(ReceiveError::Role)?;
        if !link.client_session(peer, &request) || request.request_id.is_none() {
            return Err(ReceiveError::Role);
        }
        let mut metadata = Vec::with_capacity(11);
        let header = nack::encode(
            Envelope {
                opcode: Opcode::Nack,
                response: true,
                sender: self.local(),
                ..request
            },
            nack::Nack {
                code,
                retry,
                detail: &[],
                diagnostic: "",
            },
            &mut metadata,
            link.send.envelope,
        )?;
        self.send_directory_reply(peer, header, &metadata)
    }

    pub(super) fn receive_watch(
        &mut self,
        peer: NodeId,
        packet: ozzy_proto::Packet<'_>,
    ) -> Result<(), ReceiveError> {
        let link = self.links.get(peer).ok_or(WatchError::Session)?;
        if !link.client_session(peer, &packet.envelope) {
            return Err(ReceiveError::Role);
        }
        let Ok(request) = directory::decode_request(
            packet,
            self.parameters.receive.envelope,
            directory::Limits::default(),
        ) else {
            return self.reject_directory(peer, packet.envelope, 1, nack::RetryClass::Permanent);
        };
        let Some(watches) = self.watches.as_mut() else {
            return self.reject_directory(peer, packet.envelope, 2, nack::RetryClass::Permanent);
        };
        let routes = match watches.register_bounded(
            peer,
            link.binding.session,
            &request,
            link.send.envelope.max_metadata_bytes,
        ) {
            Ok(routes) => routes,
            Err(error) => {
                let Some((code, retry)) = watch_rejection(error) else {
                    return Err(ReceiveError::Role);
                };
                return self.reject_directory(peer, packet.envelope, code, retry);
            }
        };
        let snapshot = directory::Snapshot {
            watch: request.watch,
            routes,
        };
        let mut metadata = Vec::with_capacity(snapshot.metadata_bytes()?);
        let header = directory::encode_snapshot(
            self.directory_response(packet.envelope, &link),
            &snapshot,
            &mut metadata,
            link.send.envelope,
            directory::Limits::default(),
        )?;
        self.send_directory_reply(peer, header, &metadata)
    }

    /// Snapshot reply to a client directory request on its current session.
    fn directory_response(&self, request: Envelope, link: &Link) -> Envelope {
        Envelope {
            opcode: Opcode::StateSnapshot,
            response: true,
            request_id: request.request_id,
            sender: self.local(),
            session: Some(link.binding.session),
        }
    }

    /// A full control queue drops the reply; the client retries on timeout.
    fn send_directory_reply(
        &mut self,
        peer: NodeId,
        header: [u8; ENVELOPE_BYTES],
        metadata: &[u8],
    ) -> Result<(), ReceiveError> {
        let reply = crate::native_frames::message(peer.as_bytes(), header, metadata, Bytes::new());
        match self.try_reply(Class::Control, reply) {
            Ok(()) | Err((ReplyError::Full, _)) => Ok(()),
            Err((error, _)) => Err(ReceiveError::DirectoryReply(error)),
        }
    }

    /// Install one trusted partition actor's newer observation. This changes
    /// only routing hints, never election or confirmation authority. A bounded
    /// shard port command supplies the cross-thread path in broker assembly.
    pub fn publish_route(&mut self, update: &RouteState) -> Result<bool, ServiceError> {
        self.watches
            .as_mut()
            .ok_or(ServiceError::Watch)?
            .publish(update)
            .map_err(|_| ServiceError::Watch)
    }

    /// Queue at most one pending watch hint. Full peer queues preserve it for a
    /// later attempt. A queued hint may still be lost on disconnect, so clients
    /// also refresh after timeout or a wrong-leader response.
    pub fn poll_watch(&mut self) -> Result<bool, ServiceError> {
        let Some(notice) = self.watches.as_mut().and_then(WatchRegistry::next_notice) else {
            return Ok(false);
        };
        let link = self.links.get(notice.peer).ok_or(ServiceError::Watch)?;
        if link.binding.session != notice.session {
            return Err(ServiceError::Watch);
        }
        let (header, metadata) = self.encode_notice(&notice, link.send.envelope)?;
        let message =
            crate::native_frames::message(notice.peer.as_bytes(), header, &metadata, Bytes::new());
        match self.try_reply(Class::Control, message) {
            Ok(()) => {
                self.watches
                    .as_mut()
                    .ok_or(ServiceError::Watch)?
                    .queued(&notice)
                    .map_err(|_| ServiceError::Watch)?;
                Ok(true)
            }
            Err((ReplyError::Full, _)) => Ok(false),
            Err(_) => Err(ServiceError::Watch),
        }
    }

    /// Encode a route update, or a resync when the notice carries none.
    fn encode_notice(
        &self,
        notice: &WatchNotice,
        limits: EnvelopeLimits,
    ) -> Result<([u8; ENVELOPE_BYTES], Vec<u8>), ServiceError> {
        let envelope = |opcode| Envelope {
            opcode,
            response: false,
            request_id: None,
            sender: self.local(),
            session: Some(notice.session),
        };
        let Some(route) = &notice.update else {
            let mut metadata = Vec::with_capacity(17);
            let header = directory::encode_resync(
                envelope(Opcode::StateResync),
                directory::Resync {
                    watch: notice.watch,
                },
                &mut metadata,
                limits,
            )
            .map_err(|_| ServiceError::Watch)?;
            return Ok((header, metadata));
        };
        let update = directory::Update {
            watch: notice.watch,
            route: route.clone(),
        };
        let mut metadata =
            Vec::with_capacity(update.metadata_bytes().map_err(|_| ServiceError::Watch)?);
        let header = directory::encode_update(
            envelope(Opcode::StateUpdate),
            &update,
            &mut metadata,
            limits,
            directory::Limits::default(),
        )
        .map_err(|_| ServiceError::Watch)?;
        Ok((header, metadata))
    }
}

/// NACK code and retry class of a refused watch registration. A foreign
/// session is a role error without a reply.
fn watch_rejection(error: WatchError) -> Option<(u16, nack::RetryClass)> {
    match error {
        WatchError::Full => Some((10, nack::RetryClass::AfterBackoff)),
        WatchError::Unknown => Some((18, nack::RetryClass::Permanent)),
        WatchError::Invalid | WatchError::Conflict | WatchError::Stale => {
            Some((1, nack::RetryClass::Permanent))
        }
        WatchError::Session => None,
    }
}
