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
            || self.native[slot].is_some()
            || self.coordinators[slot].is_some()
            || self.readers[slot].is_some()
        {
            return Err(PartitionError::NativeConfiguration);
        }
        if matches!(self.actors[slot], PartitionActor::Recovering(_)) {
            return Ok(None);
        }
        Ok(Some(&mut self.actors[slot]))
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
            || self.coordinators[slot].is_some()
            || self.actors[slot].native_identity().1 != local
        {
            return Err(PartitionError::NativeConfiguration);
        }
        self.coordinators[slot] = Some(NativeCoordinator { coordinator, links });
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
            || self.native[slot].is_some()
            || self.actors[slot].native_identity() != identity
        {
            return Err(PartitionError::NativeConfiguration);
        }
        self.native[slot] = Some(NativeService { intake, links });
        self.touch(slot);
        Ok(())
    }

    /// Attach bounded reader subscriptions to this partition's existing journal
    /// and frontend sessions. It creates no physical connection or file worker.
    pub fn install_readers(
        &mut self,
        group: GroupId,
        config: SharedReaderConfig,
        links: Links,
    ) -> Result<(), PartitionError> {
        let slot = *self.index.get(&group).ok_or(PartitionError::Unknown)?;
        if self.stopped || self.readers[slot].is_some() {
            return Err(PartitionError::NativeConfiguration);
        }
        let readers = SharedReaders::new(&mut self.actors[slot], config, links)
            .map_err(|source| PartitionError::Actor { group, source })?;
        self.readers[slot] = Some(readers);
        self.touch(slot);
        Ok(())
    }

    /// Whether this partition still owns native proposals or pending replies.
    /// This is a scheduling observation, independent of leadership and physical
    /// buffer release. Unknown groups or missing native adapters fail closed.
    pub fn native_has_work(&self, group: GroupId) -> Result<bool, PartitionError> {
        let slot = *self.index.get(&group).ok_or(PartitionError::Unknown)?;
        if matches!(self.actors[slot], PartitionActor::Recovering(_)) {
            return Ok(false);
        }
        self.native[slot]
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
        if matches!(self.actors[slot], PartitionActor::Recovering(_)) {
            return Ok(0);
        }
        let native = self.native[slot]
            .as_ref()
            .ok_or(PartitionError::NativeConfiguration)?
            .intake
            .client_work(node);
        let readers = self.readers[slot]
            .as_ref()
            .map_or(0, |readers| readers.client_work(node));
        let coordinator = self.coordinators[slot]
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
        if matches!(self.actors[slot], PartitionActor::Recovering(_)) {
            return Ok(0);
        }
        self.native[slot]
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
        if matches!(self.actors[slot], PartitionActor::Recovering(_)) {
            return Ok(NativeReceive::Ignored);
        }
        self.touch(slot);
        let native = self.native[slot]
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
        self.remaining = self.actors.len();
        let hint = self.actors[slot].authority_hint();
        if GroupCoordinator::supports(message)
            && let Some(service) = &mut self.coordinators[slot]
        {
            if deferred {
                return Ok(NativeReceive::Busy);
            }
            let Some(link) = service.links.get(peer) else {
                return Ok(NativeReceive::Ignored);
            };
            return service
                .coordinator
                .receive(message, link, &mut self.actors[slot], now)
                .map_err(|source| PartitionError::Native { group, source });
        }
        if SharedReaders::supports(message)
            && let Some(readers) = &mut self.readers[slot]
        {
            let ready = !deferred
                && self.actors[slot]
                    .read_access()
                    .is_some_and(|access| access.0.is_some());
            return readers
                .receive(message, hint, ready)
                .map_err(|source| PartitionError::Actor { group, source });
        }
        let received = if deferred {
            native.intake.defer(message, link, hint)
        } else {
            native.intake.receive(message, link, hint)
        };
        received.map_err(|source| PartitionError::Native { group, source })
    }
}
