use super::{
    ActorError, AuthorityHint, Bytes, Cursor, Delivery, Envelope, Failure, Link, Message,
    NativeReceive, NodeId, Opcode, Payload, SharedReaders, Slot, decode_packet, reader,
};
use ozzy_proto::{Offset, Packet};

impl SharedReaders {
    pub(in crate::replica_actor) fn receive(
        &mut self,
        message: &Message,
        hint: AuthorityHint,
        ready: bool,
    ) -> Result<NativeReceive, ActorError> {
        self.discard_stale();
        let Some(peer) = message
            .part_slice(0)
            .and_then(|bytes| bytes.try_into().ok())
            .map(NodeId::from_bytes)
        else {
            return Ok(NativeReceive::Ignored);
        };
        if message.len() != 4 {
            return Ok(NativeReceive::Ignored);
        }
        let frames: [&[u8]; 3] =
            std::array::from_fn(|i| message.part_slice(i + 1).expect("four frames"));
        let Ok(packet) = decode_packet(&frames, self.config.limits.envelope) else {
            return Ok(NativeReceive::Ignored);
        };
        let Some(link) = self.current(peer, packet.envelope) else {
            return Ok(NativeReceive::Ignored);
        };
        // Acknowledgments, unsubscribe replies, and failures share one bounded
        // reply slot. Keep the incoming command in its shard reservation until
        // the previous reply leaves; accepting it here could discard its reply.
        if self.rejection.is_some() {
            return Ok(NativeReceive::Busy);
        }
        let result = if ready {
            self.command(peer, packet, link, hint)
        } else {
            Err(Failure::new(12))
        };
        if let Err(error) = result {
            self.reject(peer, packet.envelope, link, error, hint)?;
        }
        self.work.mark();
        Ok(NativeReceive::Accepted)
    }

    fn command(
        &mut self,
        peer: NodeId,
        packet: Packet<'_>,
        link: Link,
        hint: AuthorityHint,
    ) -> Result<(), Failure> {
        let limits = self.config.limits.envelope;
        let source = reader::route(packet, limits).map_err(|_| Failure::new(1))?;
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
        match packet.envelope.opcode {
            Opcode::Subscribe => {
                let subscribe =
                    reader::decode_subscribe(packet, limits).map_err(|_| Failure::new(1))?;
                let subscribe = reader::Subscribe {
                    subscription: subscribe.subscription,
                    target: subscribe.target,
                    start: subscribe.start,
                };
                self.subscribe(peer, packet.envelope, link, &subscribe, source)
            }
            Opcode::Credit => self.credit(peer, packet, link),
            Opcode::Ack => self.ack(peer, packet, link),
            Opcode::Unsubscribe => {
                let selected =
                    reader::decode_unsubscribe(packet, limits).map_err(|_| Failure::new(1))?;
                if self.rejection.is_some() {
                    return Err(Failure::new(10));
                }
                for slot in &mut self.slots {
                    if slot.as_ref().is_some_and(|slot| {
                        slot.peer == peer
                            && slot.delivery.subscribe.subscription == selected.subscription
                            && slot.delivery.source == selected.source
                    }) {
                        *slot = None;
                    }
                }
                let header = reader::encode_unsubscribed(
                    Envelope {
                        opcode: Opcode::Unsubscribed,
                        response: true,
                        sender: self.local,
                        ..packet.envelope
                    },
                    selected,
                    &mut self.metadata,
                    link.send.envelope,
                )
                .map_err(|_| Failure::new(1))?;
                self.rejection = Some(crate::native_frames::message(
                    peer.as_bytes(),
                    header,
                    &self.metadata,
                    Bytes::new(),
                ));
                Ok(())
            }
            _ => Err(Failure::new(2)),
        }
    }

    fn credit(&mut self, peer: NodeId, packet: Packet<'_>, link: Link) -> Result<(), Failure> {
        let limits = self.config.limits.envelope;
        if self.rejection.is_some() {
            return Err(Failure::new(10));
        }
        let credit = reader::decode_credit(packet, limits).map_err(|_| Failure::new(1))?;
        let slot = self
            .find(peer, credit.subscription)
            .filter(|slot| slot.delivery.source == credit.source)
            .ok_or_else(|| Failure::new(12))?;
        slot.delivery.credit(
            credit,
            (link.remote.inflight_records, link.remote.inflight_bytes),
        )?;
        let header = reader::encode_credit(
            Envelope {
                response: true,
                sender: self.local,
                ..packet.envelope
            },
            credit,
            &mut self.metadata,
            link.send.envelope,
        )
        .map_err(|_| Failure::new(1))?;
        self.rejection = Some(crate::native_frames::message(
            peer.as_bytes(),
            header,
            &self.metadata,
            Bytes::new(),
        ));
        Ok(())
    }

    fn ack(&mut self, peer: NodeId, packet: Packet<'_>, link: Link) -> Result<(), Failure> {
        let limits = self.config.limits.envelope;
        if packet.envelope.request_id.is_some() && self.rejection.is_some() {
            return Err(Failure::new(10));
        }
        let ack = reader::decode_ack(packet, limits).map_err(|_| Failure::new(1))?;
        let slot = self
            .find(peer, ack.subscription)
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
                link.send.envelope,
            )
            .map_err(|_| Failure::new(1))?;
            self.rejection = Some(crate::native_frames::message(
                peer.as_bytes(),
                header,
                &self.metadata,
                Bytes::new(),
            ));
        }
        Ok(())
    }

    fn find(&mut self, peer: NodeId, subscription: reader::Subscription) -> Option<&mut Slot> {
        self.slots
            .iter_mut()
            .flatten()
            .find(|slot| slot.peer == peer && slot.delivery.subscribe.subscription == subscription)
    }

    fn subscribe(
        &mut self,
        peer: NodeId,
        envelope: Envelope,
        link: Link,
        subscribe: &reader::Subscribe,
        source: reader::Source,
    ) -> Result<(), Failure> {
        let old = self.slots.iter().position(|slot| {
            slot.as_ref().is_some_and(|slot| {
                slot.peer == peer
                    && slot.delivery.subscribe.subscription.id == subscribe.subscription.id
            })
        });
        let index = old
            .or_else(|| self.slots.iter().position(Option::is_none))
            .ok_or_else(|| Failure::new(10))?;
        if let Some(slot) = &self.slots[index]
            && slot.delivery.subscribe.subscription == subscribe.subscription
        {
            if &slot.delivery.subscribe != subscribe || slot.delivery.source != source {
                return Err(Failure::new(15));
            }
            if slot.pending.is_some() {
                return Ok(());
            }
        } else {
            self.slots[index] = Some(Slot {
                peer,
                payload: Payload::notifying(
                    self.config.limits.envelope.max_payload_bytes,
                    self.work.clone(),
                ),
                delivery: Delivery::new(
                    envelope,
                    subscribe.clone(),
                    source,
                    Cursor::new(self.pool.clone(), Some(Offset::new(subscribe.start))),
                ),
                pending: None,
            });
        }
        let header = reader::encode_subscribed(
            Envelope {
                opcode: Opcode::Subscribed,
                response: true,
                sender: self.local,
                ..envelope
            },
            reader::Subscribed {
                subscription: subscribe.subscription,
                source,
            },
            &mut self.metadata,
            link.send.envelope,
        )
        .map_err(|_| Failure::new(1))?;
        self.slots[index]
            .as_mut()
            .expect("installed subscription")
            .pending = Some(crate::native_frames::message(
            peer.as_bytes(),
            header,
            &self.metadata,
            Bytes::new(),
        ));
        Ok(())
    }

    pub(super) fn reject(
        &mut self,
        peer: NodeId,
        envelope: Envelope,
        link: Link,
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
            link.send.envelope,
        )
        .map_err(|_| ActorError::Limits)?;
        self.rejection = Some(crate::native_frames::message(
            peer.as_bytes(),
            header,
            &self.metadata,
            Bytes::new(),
        ));
        Ok(())
    }
}
