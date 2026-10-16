use super::{
    ActorError, AuthorityHint, Bytes, Client, Cursor, Delivery, Envelope, Failure, Message,
    NativeReceive, NodeId, Opcode, Payload, SharedReaders, Slot, decode_packet, reader,
};
use crate::transport::Frames;
use ozzy_proto::Packet;

impl SharedReaders {
    pub(in crate::replica_actor) fn receive(
        &mut self,
        message: &Message,
        hint: AuthorityHint,
        ready: bool,
    ) -> Result<NativeReceive, ActorError> {
        self.discard_stale();
        let Some((route, frames)) = message.routed_packet() else {
            return Ok(NativeReceive::Ignored);
        };
        let Ok(peer) = <[u8; 16]>::try_from(route).map(NodeId::from_bytes) else {
            return Ok(NativeReceive::Ignored);
        };
        let Ok(packet) = decode_packet(&frames, self.config.limits.envelope) else {
            return Ok(NativeReceive::Ignored);
        };
        let Some(client) = self.current(peer, packet.envelope) else {
            return Ok(NativeReceive::Ignored);
        };
        // Acknowledgments, unsubscribe replies, and failures share one bounded
        // reply slot. Keep the incoming command in its shard reservation until
        // the previous reply leaves; accepting it here could discard its reply.
        if self.rejection.is_some() {
            return Ok(NativeReceive::Busy);
        }
        let result = if ready {
            self.command(client, packet, hint)
        } else {
            Err(Failure::new(12))
        };
        if let Err(error) = result {
            self.reject(client, packet.envelope, error, hint)?;
        }
        self.wake.wake();
        Ok(NativeReceive::Accepted)
    }

    fn command(
        &mut self,
        client: Client,
        packet: Packet<'_>,
        hint: AuthorityHint,
    ) -> Result<(), Failure> {
        let limits = self.config.limits.envelope;
        let source = reader::route(packet, limits).map_err(|_| Failure::new(1))?;
        self.check_source(source, hint)?;
        match packet.envelope.opcode {
            Opcode::Subscribe => {
                let subscribe =
                    reader::decode_subscribe(packet, limits).map_err(|_| Failure::new(1))?;
                let subscribe = reader::Subscribe {
                    subscription: subscribe.subscription,
                    target: subscribe.target,
                    start: subscribe.start,
                };
                self.subscribe(client.peer, packet.envelope, &subscribe, source)
            }
            Opcode::Ack => self.ack(client, packet),
            Opcode::Unsubscribe => self.unsubscribe(client, packet),
            _ => Err(Failure::new(2)),
        }
    }

    /// Serve only this partition's group source while this broker is its primary.
    fn check_source(&self, source: reader::Source, hint: AuthorityHint) -> Result<(), Failure> {
        let reader::Source::Group {
            authority,
            partition,
            ..
        } = source
        else {
            return Err(Failure::new(2));
        };
        if partition != self.config.partition
            || authority.group_id != hint.authority.group_id
            || authority.config_epoch != hint.authority.config_epoch
        {
            return Err(Failure::new(3));
        }
        if authority.view != hint.authority.view || hint.primary != self.local {
            return Err(Failure::new(5));
        }
        Ok(())
    }

    fn ack(&mut self, client: Client, packet: Packet<'_>) -> Result<(), Failure> {
        let limits = self.config.limits.envelope;
        if packet.envelope.request_id.is_some() && self.rejection.is_some() {
            return Err(Failure::new(10));
        }
        let ack = reader::decode_ack(packet, limits).map_err(|_| Failure::new(1))?;
        let slot = self
            .find(client.peer, ack.subscription)
            .filter(|slot| slot.delivery.source == ack.source)
            .ok_or_else(|| Failure::new(12))?;
        slot.delivery.observe(ack)?;
        if packet.envelope.request_id.is_some() {
            let header = reader::encode_ack(
                Envelope {
                    response: true,
                    sender: self.local,
                    ..packet.envelope
                },
                ack,
                &mut self.metadata,
                client.link.send.envelope,
            )
            .map_err(|_| Failure::new(1))?;
            self.reply(client, header);
        }
        Ok(())
    }

    fn unsubscribe(&mut self, client: Client, packet: Packet<'_>) -> Result<(), Failure> {
        let selected = reader::decode_unsubscribe(packet, self.config.limits.envelope)
            .map_err(|_| Failure::new(1))?;
        if self.rejection.is_some() {
            return Err(Failure::new(10));
        }
        self.slots.retain(|slot| {
            slot.peer != client.peer
                || slot.delivery.subscribe.subscription != selected.subscription
                || slot.delivery.source != selected.source
        });
        let header = reader::encode_unsubscribed(
            Envelope {
                opcode: Opcode::Unsubscribed,
                response: true,
                sender: self.local,
                ..packet.envelope
            },
            selected,
            &mut self.metadata,
            client.link.send.envelope,
        )
        .map_err(|_| Failure::new(1))?;
        self.reply(client, header);
        Ok(())
    }

    /// Hold one encoded reply in the shared reply slot.
    fn reply(&mut self, client: Client, header: [u8; 64]) {
        self.rejection = Some(crate::native_frames::message(
            client.peer.as_bytes(),
            header,
            &self.metadata,
            Bytes::new(),
        ));
    }

    fn find(&mut self, peer: NodeId, subscription: reader::Subscription) -> Option<&mut Slot> {
        self.slots
            .iter_mut()
            .find(|slot| slot.peer == peer && slot.delivery.subscribe.subscription == subscription)
    }

    fn subscribe(
        &mut self,
        peer: NodeId,
        envelope: Envelope,
        subscribe: &reader::Subscribe,
        source: reader::Source,
    ) -> Result<(), Failure> {
        let old = self.slots.iter().position(|slot| {
            slot.peer == peer
                && slot.delivery.subscribe.subscription.id == subscribe.subscription.id
        });
        let index = if let Some(index) = old
            && self.slots[index].delivery.subscribe.subscription == subscribe.subscription
        {
            let slot = &self.slots[index];
            if &slot.delivery.subscribe != subscribe || slot.delivery.source != source {
                return Err(Failure::new(15));
            }
            if slot.pending.is_some() {
                return Ok(());
            }
            index
        } else {
            if old.is_none() && self.slots.len() == self.config.subscriptions {
                return Err(Failure::new(10));
            }
            let slot = Slot {
                peer,
                payload: Payload::waking(
                    self.config.limits.envelope.max_payload_bytes,
                    self.wake.clone(),
                ),
                delivery: Delivery::new(
                    envelope,
                    subscribe.clone(),
                    source,
                    Cursor::select(self.pool.clone(), subscribe.start),
                ),
                pending: None,
                resolved_offset: None,
                reply_due: true,
            };
            if let Some(index) = old {
                self.slots[index] = slot;
                index
            } else {
                self.slots.push(slot);
                self.slots.len() - 1
            }
        };
        let slot = &mut self.slots[index];
        slot.delivery.request = envelope;
        slot.reply_due = true;
        Ok(())
    }

    pub(super) fn reject(
        &mut self,
        client: Client,
        envelope: Envelope,
        error: Failure,
        hint: AuthorityHint,
    ) -> Result<(), ActorError> {
        if self.rejection.is_some() || envelope.request_id.is_none() {
            return Ok(());
        }
        let authority = hint.encode().map_err(|_| ActorError::Limits)?;
        let detail = if matches!(error.code, 5 | 12) {
            authority.as_slice()
        } else {
            error.detail()
        };
        let header = ozzy_proto::nack::encode(
            Envelope {
                opcode: Opcode::Nack,
                response: true,
                sender: self.local,
                ..envelope
            },
            ozzy_proto::nack::Nack {
                code: error.code,
                retry: error.retry,
                detail,
                diagnostic: "",
            },
            &mut self.metadata,
            client.link.send.envelope,
        )
        .map_err(|_| ActorError::Limits)?;
        self.reply(client, header);
        Ok(())
    }
}
