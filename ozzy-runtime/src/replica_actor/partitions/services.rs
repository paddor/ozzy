//! Native client intake, group coordinator and reader services attached to
//! partitions, and the client commands delivered to them.

use super::{NativeCoordinator, NativeService, PartitionActor, PartitionActors, PartitionError};
use crate::frontend::Links;
use crate::replica_actor::shared_readers::{SharedReaderConfig, SharedReaders};
use crate::replica_actor::{GroupCoordinator, NativeIntake, NativeReceive};
use omq_tokio::Message;
use ozzy_proto::{GroupId, NodeId};
use std::time::Duration;

impl PartitionActors {
    /// Configure native services after recovery publication, before the next
    /// normal scheduling turn. No proposal resources exist during recovery.
    /// A partition with installed client or reader services cannot be reopened.
    pub fn startup_actor(
        &mut self,
        group: GroupId,
    ) -> Result<Option<&mut PartitionActor>, PartitionError> {
        let slot = *self.index.get(&group).ok_or(PartitionError::Unknown)?;
        if self.stopped
            || self.slots[slot].native.is_some()
            || self.slots[slot].coordinator.is_some()
            || self.slots[slot].readers.is_some()
        {
            return Err(PartitionError::NativeConfiguration);
        }
        if matches!(self.slots[slot].actor, PartitionActor::Recovering(_)) {
            return Ok(None);
        }
        Ok(Some(&mut self.slots[slot].actor))
    }

    /// Attach the consumer-group coordinator of this partition. It uses the
    /// actor's proposal lane and the frontend's shared client links.
    pub fn install_coordinator(
        &mut self,
        coordinator: GroupCoordinator,
        links: Links,
    ) -> Result<(), PartitionError> {
        let (group, local) = coordinator.identity();
        let slot = *self.index.get(&group).ok_or(PartitionError::Unknown)?;
        if self.stopped
            || self.slots[slot].coordinator.is_some()
            || self.slots[slot].actor.native_identity().1 != local
        {
            return Err(PartitionError::NativeConfiguration);
        }
        self.slots[slot].coordinator = Some(NativeCoordinator { coordinator, links });
        self.touch(slot);
        Ok(())
    }

    /// Attach a socket-free client adapter on the same partition shard. Take its
    /// proposal lane and leases before transferring the actor. Links are the
    /// frontend's independently established observations, shared across groups.
    pub fn install_native(
        &mut self,
        intake: NativeIntake,
        links: Links,
    ) -> Result<(), PartitionError> {
        let identity = intake.identity();
        let slot = *self.index.get(&identity.0).ok_or(PartitionError::Unknown)?;
        if self.stopped
            || self.slots[slot].native.is_some()
            || self.slots[slot].actor.native_identity() != identity
        {
            return Err(PartitionError::NativeConfiguration);
        }
        self.slots[slot].native = Some(NativeService { intake, links });
        self.touch(slot);
        Ok(())
    }

    /// Attach bounded reader subscriptions to this partition's existing journal
    /// and frontend sessions. It creates no physical connection or file worker.
    /// With `grants`, its subscribers' live topic admits them on the reader PUB.
    pub fn install_readers(
        &mut self,
        group: GroupId,
        config: SharedReaderConfig,
        links: Links,
        grants: Option<crate::frontend::ShardGrants>,
    ) -> Result<(), PartitionError> {
        let slot = *self.index.get(&group).ok_or(PartitionError::Unknown)?;
        if self.stopped || self.slots[slot].readers.is_some() {
            return Err(PartitionError::NativeConfiguration);
        }
        let readers = SharedReaders::new(&mut self.slots[slot].actor, config, links, grants)
            .map_err(|source| PartitionError::Actor { group, source })?;
        self.slots[slot].readers = Some(readers);
        self.touch(slot);
        Ok(())
    }

    /// Attach an embedder's log to this partition. Without `lane`, as while
    /// the partition recovers, it answers until `attach_log_lane` lends one.
    pub fn install_log(
        &mut self,
        group: GroupId,
        port: crate::log::LogPort,
        lane: Option<crate::replica_actor::LogLane>,
    ) -> Result<(), PartitionError> {
        let slot = *self.index.get(&group).ok_or(PartitionError::Unknown)?;
        if self.stopped || self.slots[slot].log.is_some() {
            return Err(PartitionError::NativeConfiguration);
        }
        self.slots[slot].log = Some(crate::replica_actor::log_service::LogService::new(
            port, lane,
        ));
        self.touch(slot);
        Ok(())
    }

    /// Lend a recovered partition's log the new actor's lane.
    pub fn attach_log_lane(
        &mut self,
        group: GroupId,
        lane: crate::replica_actor::LogLane,
    ) -> Result<(), PartitionError> {
        let slot = *self.index.get(&group).ok_or(PartitionError::Unknown)?;
        let log = self.slots[slot]
            .log
            .as_mut()
            .filter(|_| !self.stopped)
            .ok_or(PartitionError::NativeConfiguration)?;
        log.attach(lane);
        self.touch(slot);
        Ok(())
    }

    /// Whether this partition still owns native proposals or pending replies.
    /// This is a scheduling observation, independent of leadership and physical
    /// buffer release. Unknown groups or missing native adapters fail closed.
    pub fn native_has_work(&self, group: GroupId) -> Result<bool, PartitionError> {
        let slot = *self.index.get(&group).ok_or(PartitionError::Unknown)?;
        if matches!(self.slots[slot].actor, PartitionActor::Recovering(_)) {
            return Ok(false);
        }
        self.slots[slot]
            .native
            .as_ref()
            .map(|native| native.intake.has_work())
            .ok_or(PartitionError::NativeConfiguration)
    }

    /// Commands of one client other than APPENDs that still own a native slot
    /// or the shared reader reply slot on this partition. Other clients do
    /// not count. Unknown groups or missing native adapters fail closed.
    pub fn native_client_work(
        &self,
        group: GroupId,
        node: NodeId,
    ) -> Result<usize, PartitionError> {
        let slot = *self.index.get(&group).ok_or(PartitionError::Unknown)?;
        if matches!(self.slots[slot].actor, PartitionActor::Recovering(_)) {
            return Ok(0);
        }
        let native = self.slots[slot]
            .native
            .as_ref()
            .ok_or(PartitionError::NativeConfiguration)?
            .intake
            .client_work(node);
        let readers = self.slots[slot]
            .readers
            .as_ref()
            .map_or(0, |readers| readers.client_work(node));
        let coordinator = self.slots[slot]
            .coordinator
            .as_ref()
            .map_or(0, |service| service.coordinator.client_work(node));
        Ok(native + readers + coordinator)
    }

    /// Requests of one client's writer that still own native proposals or
    /// replies on this partition. Other writers do not count. Unknown groups
    /// or missing native adapters fail closed.
    pub fn native_writer_work(
        &self,
        group: GroupId,
        node: NodeId,
        producer: ozzy_proto::ProducerId,
    ) -> Result<usize, PartitionError> {
        let slot = *self.index.get(&group).ok_or(PartitionError::Unknown)?;
        if matches!(self.slots[slot].actor, PartitionActor::Recovering(_)) {
            return Ok(0);
        }
        self.slots[slot]
            .native
            .as_ref()
            .map(|native| native.intake.writer_work(node, producer))
            .ok_or(PartitionError::NativeConfiguration)
    }

    /// Deliver a shard-admitted client command through the current shared link.
    /// Session replacement rejects old queued input before proposal admission.
    /// Supports explicit local and fixed-three actors through the same codec.
    pub fn receive_client(
        &mut self,
        group: GroupId,
        message: &Message,
        now: Duration,
    ) -> Result<NativeReceive, PartitionError> {
        self.deliver_client(group, message, now, false)
    }

    /// Return a bounded retry reply for a current client while local partition
    /// initialization is pending. No producer or append proposal is admitted.
    pub fn defer_client(
        &mut self,
        group: GroupId,
        message: &Message,
        now: Duration,
    ) -> Result<NativeReceive, PartitionError> {
        self.deliver_client(group, message, now, true)
    }

    fn deliver_client(
        &mut self,
        group: GroupId,
        message: &Message,
        now: Duration,
        deferred: bool,
    ) -> Result<NativeReceive, PartitionError> {
        self.observe(now)?;
        let slot = *self.index.get(&group).ok_or(PartitionError::Unknown)?;
        if matches!(self.slots[slot].actor, PartitionActor::Recovering(_)) {
            return Ok(NativeReceive::Ignored);
        }
        self.touch(slot);
        let count = self.slots.len();
        let held = &mut self.slots[slot];
        let native = held
            .native
            .as_mut()
            .ok_or(PartitionError::NativeConfiguration)?;
        let Some(peer) = message
            .part_slice(0)
            .and_then(|bytes| bytes.try_into().ok())
            .map(NodeId::from_bytes)
        else {
            return Ok(NativeReceive::Ignored);
        };
        let Some(link) = native.links.get(peer) else {
            return Ok(NativeReceive::Ignored);
        };
        // Admission does not carry a wakeup itself. Poll every actor again even
        // when the new request targets an earlier point in the current scan.
        self.remaining = count;
        let hint = held.actor.authority_hint();
        if GroupCoordinator::supports(message)
            && let Some(service) = &mut held.coordinator
        {
            if deferred {
                return Ok(NativeReceive::Busy);
            }
            let Some(link) = service.links.get(peer) else {
                return Ok(NativeReceive::Ignored);
            };
            return service
                .coordinator
                .receive(message, link, &mut held.actor, now)
                .map_err(|source| PartitionError::Native { group, source });
        }
        if SharedReaders::supports(message)
            && let Some(readers) = &mut held.readers
        {
            let ready = !deferred
                && held
                    .actor
                    .read_access()
                    .is_some_and(|access| access.0.is_some());
            return readers
                .receive(message, hint, ready)
                .map_err(|source| PartitionError::Actor { group, source });
        }
        if !deferred && publishing(held.readers.as_ref(), &mut held.actor) {
            return Ok(NativeReceive::Busy);
        }
        let received = if deferred {
            native.intake.defer(message, link, hint)
        } else {
            native.intake.receive(message, link, hint)
        };
        received.map_err(|source| PartitionError::Native { group, source })
    }
}

/// A leader hands its framed publication to the dispatcher before it admits
/// more writes, so intake cannot starve the reader path. Slow SUBs retain
/// only sent frames and never hold writers back.
fn publishing(readers: Option<&SharedReaders>, actor: &mut PartitionActor) -> bool {
    readers.is_some_and(SharedReaders::publishing)
        && actor.read_access().is_some_and(|access| access.0.is_some())
}
