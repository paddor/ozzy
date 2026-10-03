use super::{
    Class, Client, Destination, GrantRequest, IntakeError, Kind, Links, Port, ReceiveSize,
    Reservation, ShardIntake, control_matches, dispatch, memory, refill_control,
};
use crate::frontend::{GrantSpec, GrantTarget, Subject};
use ozzy_replication::wire::ReceiveFence;

// Do not park a second large canonical body in every follower partition.
const DOUBLE_REPLICA_WINDOW_MAX_CANONICAL_BYTES: usize = 1024 * 1024;
const TRIPLE_CLIENT_WINDOW_MAX_CANONICAL_BYTES: usize = 1024 * 1024;

impl ShardIntake {
    /// Reserve canonical capacity, then foreign receive capacity, then submit
    /// installation. No credit reaches dispatch if either reservation fails.
    /// Failure of the bounded command lane returns both unused reservations.
    /// Returns whether this starts a new installation. Metadata-only broker
    /// controls share a bounded four-message window across this shard.
    pub fn install(
        &mut self,
        port: &mut Port,
        links: &Links,
        request: GrantRequest,
        receive_fence: Option<ReceiveFence>,
        client: &Client,
        size: impl Into<ReceiveSize>,
    ) -> Result<bool, IntakeError> {
        let size = size.into();
        let wire_bytes = size.retained_bytes;
        if links.get(request.binding.peer).map(|link| link.binding) != Some(request.binding) {
            return Err(IntakeError::Session);
        }
        if !matches_fence(request, receive_fence) {
            return Err(IntakeError::Configuration);
        }
        let destination = self
            .destinations
            .get(&(
                request.route.placement.group,
                request.binding.kind,
                request.route.class,
            ))
            .ok_or(IntakeError::Configuration)?
            .clone();
        if self.refresh_existing(request, receive_fence, size)? {
            return Ok(false);
        }
        // Replication and history installation share one actor's receive arena.
        // Retire its old session/purpose before backing a replacement promise.
        // Independent SDK writers keep separate concurrent reservations.
        if destination.kind == Kind::Broker
            && destination.class == Class::Data
            && self.reservations.iter().flatten().any(|old| {
                old.destination
                    .capacity
                    .same_allowance(&destination.capacity)
            })
        {
            return Err(dispatch::Error::Full.into());
        }
        let first = match destination.class {
            Class::Data => 0,
            Class::Control => self.limits.grants,
        };
        let slot = self.reservations[first..first + self.limits.grants]
            .iter()
            .position(Option::is_none)
            .map(|offset| first + offset)
            .ok_or(dispatch::Error::Full)?;
        let per_message = canonical_quota(&destination, size);
        let control = request.binding.kind == Kind::Broker
            && request.route.class == Class::Control
            && per_message == memory::Quota::default();
        if control
            && self.destinations.values().any(|old| {
                old.kind == Kind::Broker
                    && old.class == Class::Control
                    && old.per_message != memory::Quota::default()
            })
        {
            return Err(IntakeError::Configuration);
        }
        let preferred = window_messages(
            control,
            receive_fence,
            &destination,
            per_message.bytes,
            self.client_window,
        );
        let (messages, quota, grant) = self.reserve_window(
            &destination,
            client,
            per_message,
            wire_bytes,
            preferred,
            !control,
        )?;
        let key = grant.key();
        // Partition-scoped control avoids promising one open buffer for every
        // partition when only one partition requested control capacity.
        let spec = grant_spec(request, receive_fence, control);
        let pending = match port.try_install(request.binding.peer, spec, grant) {
            Ok(pending) => pending,
            Err((error, grant)) => {
                drop(grant);
                let unused = self.input.credits().revoke_unused(&key)?;
                if unused.messages != messages {
                    return Err(IntakeError::Invariant);
                }
                if quota != memory::Quota::default() {
                    destination.capacity.release(quota)?;
                }
                return Err(error.into());
            }
        };
        self.reservations[slot] = Some(Reservation {
            request,
            receive_fence,
            destination,
            key,
            wire_bytes,
            canonical: quota,
            window: messages,
            control_window: control.then_some(4),
            pending: Some(pending),
            fenced: false,
            release_pending: false,
            dequeued: 0,
            admitted_at_fence: None,
            waiting: std::collections::VecDeque::new(),
        });
        Ok(true)
    }

    fn reserve_receive(
        &mut self,
        destination: &Destination,
        client: &Client,
        foreign: dispatch::Quota,
        quota: memory::Quota,
    ) -> Result<dispatch::Grant, IntakeError> {
        if quota != memory::Quota::default() {
            match destination.class {
                Class::Data => self.data.reserve(&destination.capacity, quota)?,
                Class::Control => self.control.reserve(&destination.capacity, quota)?,
            }
        }
        match self
            .input
            .credits()
            .grant(client, destination.class, foreign)
        {
            Ok(grant) => Ok(grant),
            Err(error) => {
                if quota != memory::Quota::default() {
                    destination.capacity.release(quota)?;
                }
                Err(error.into())
            }
        }
    }

    fn reserve_window(
        &mut self,
        destination: &Destination,
        client: &Client,
        per_message: memory::Quota,
        wire_bytes: usize,
        preferred: usize,
        fallback: bool,
    ) -> Result<(usize, memory::Quota, dispatch::Grant), IntakeError> {
        for messages in [preferred, 1] {
            let quota = scale_quota(per_message, messages)?;
            let foreign = dispatch::Quota {
                messages,
                bytes: wire_bytes
                    .checked_mul(messages)
                    .ok_or(IntakeError::Configuration)?,
            };
            match self.reserve_receive(destination, client, foreign, quota) {
                Ok(grant) => return Ok((messages, quota, grant)),
                Err(IntakeError::Admission(dispatch::Error::Full)) if messages > 1 && fallback => {}
                Err(IntakeError::Memory(error))
                    if messages > 1
                        && fallback
                        && error.kind() == std::io::ErrorKind::WouldBlock => {}
                Err(error) => return Err(error),
            }
        }
        Err(IntakeError::Invariant)
    }

    /// Replenish a consumed normal-replication or client dispatch slot after
    /// its actor work settles. Keep the installed grant and canonical backing.
    /// The caller must verify current authority and session.
    /// Backpressure leaves the spent reservation intact for a later retry.
    pub fn replenish_data_slot(&mut self, slot: usize, links: &Links) -> Result<bool, IntakeError> {
        let Some(old) = self.reservations.get(slot).and_then(Option::as_ref) else {
            return Ok(false);
        };
        if old.dequeued == 0
            || old.fenced
            || old.release_pending
            || !old.waiting.is_empty()
            || !(matches!(old.receive_fence, Some(ReceiveFence::Normal(_)))
                || (old.destination.kind == Kind::Client && old.destination.class == Class::Data))
        {
            return Ok(false);
        }
        let request = old.request;
        if links.get(request.binding.peer).map(|link| link.binding) != Some(request.binding) {
            return Err(IntakeError::Session);
        }
        let remaining = self.input.credits().remaining(&old.key)?;
        let restored_messages = if old.destination.kind == Kind::Client {
            // Keep the SDK's backed window full as each completed APPEND leaves
            // the actor. Queued messages still own their slots and must dequeue
            // before this reservation can be replenished.
            let admitted = old
                .window
                .checked_sub(remaining.messages)
                .ok_or(IntakeError::Invariant)?;
            if old.dequeued != admitted {
                return Ok(true);
            }
            old.dequeued
        } else {
            if old.dequeued < old.window {
                return Ok(true);
            }
            if remaining.messages != 0 {
                return Err(IntakeError::Invariant);
            }
            old.window
        };
        if restored_messages == 0 {
            return Err(IntakeError::Invariant);
        }
        // Allocating a canonical body spends its allowance permanently. A
        // released body returns physical capacity, not another allocation grant.
        // Back the next body before making its dispatch slot available again.
        let canonical = old.destination.capacity.remaining();
        let added = memory::Quota {
            bytes: old.canonical.bytes.saturating_sub(canonical.bytes),
            buffers: old.canonical.buffers.saturating_sub(canonical.buffers),
        };
        if added != memory::Quota::default() {
            self.data.reserve(&old.destination.capacity, added)?;
        }
        let full_wire_bytes = old
            .wire_bytes
            .checked_mul(old.window)
            .ok_or(IntakeError::Invariant)?;
        if let Err(error) = self.input.credits().extend(
            &old.key,
            dispatch::Quota {
                messages: restored_messages,
                bytes: full_wire_bytes.saturating_sub(remaining.bytes),
            },
        ) {
            if added != memory::Quota::default() {
                old.destination.capacity.release(added)?;
            }
            return Err(error.into());
        }
        let old = self.reservations[slot].as_mut().expect("spent reservation");
        old.dequeued = 0;
        self.spent -= 1;
        Ok(true)
    }

    fn refresh_existing(
        &mut self,
        request: GrantRequest,
        receive_fence: Option<ReceiveFence>,
        size: ReceiveSize,
    ) -> Result<bool, IntakeError> {
        let wire_bytes = size.retained_bytes;
        // Re-observing asynchronous demand must not revoke its own pending
        // installation or reserve the same allowance twice.
        if let Some(old) = self
            .reservations
            .iter_mut()
            .flatten()
            .find(|old| old.request == request || control_matches(old, request))
        {
            if old.receive_fence != receive_fence {
                return Err(dispatch::Error::Full.into());
            }
            if old.control_window.is_some() {
                old.wire_bytes = old.wire_bytes.max(wire_bytes);
                match refill_control(self.input.credits(), old) {
                    Ok(()) | Err(IntakeError::Admission(dispatch::Error::Revoked)) => {}
                    Err(error) => return Err(error),
                }
                return Ok(true);
            }
            let canonical = scale_quota(canonical_quota(&old.destination, size), old.window)?;
            if !old.fenced && (wire_bytes > old.wire_bytes || canonical.bytes > old.canonical.bytes)
            {
                match self.input.credits().remaining(&old.key) {
                    Ok(remaining) if remaining.messages != 0 => {
                        let canonical = memory::Quota {
                            bytes: canonical.bytes.max(old.canonical.bytes),
                            ..canonical
                        };
                        let added = memory::Quota {
                            bytes: canonical.bytes - old.canonical.bytes,
                            buffers: 0,
                        };
                        if added != memory::Quota::default() {
                            self.data.reserve(&old.destination.capacity, added)?;
                        }
                        if wire_bytes > old.wire_bytes
                            && let Err(error) = self.input.credits().extend(
                                &old.key,
                                dispatch::Quota {
                                    messages: 0,
                                    bytes: wire_bytes
                                        .saturating_sub(old.wire_bytes)
                                        .checked_mul(remaining.messages)
                                        .ok_or(IntakeError::Configuration)?,
                                },
                            )
                        {
                            if added != memory::Quota::default() {
                                old.destination.capacity.release(added)?;
                            }
                            return Err(error.into());
                        }
                        old.wire_bytes = old.wire_bytes.max(wire_bytes);
                        old.canonical = canonical;
                    }
                    Ok(_) | Err(dispatch::Error::Revoked) => {}
                    Err(error) => return Err(error.into()),
                }
            }
            return Ok(true);
        }
        Ok(false)
    }
}

fn canonical_quota(destination: &Destination, size: ReceiveSize) -> memory::Quota {
    let mut quota = destination.per_message;
    if destination.class == Class::Data {
        // Canonical APPEND preserves the wire payload and uses smaller record
        // descriptors. Its complete body fits within the conservative wire
        // backing charge. Reserve a second body for a private retry rewrite.
        quota.bytes = quota.bytes.min(
            size.canonical_body_bytes
                .unwrap_or(size.retained_bytes)
                .saturating_mul(2),
        );
    }
    quota
}

fn window_messages(
    control: bool,
    receive_fence: Option<ReceiveFence>,
    destination: &Destination,
    canonical_bytes: usize,
    client_window: usize,
) -> usize {
    if control {
        4
    } else if matches!(receive_fence, Some(ReceiveFence::Normal(_)))
        && destination.per_message.bytes <= DOUBLE_REPLICA_WINDOW_MAX_CANONICAL_BYTES
    {
        2
    } else if destination.kind == Kind::Client
        && destination.class == Class::Data
        && canonical_bytes <= TRIPLE_CLIENT_WINDOW_MAX_CANONICAL_BYTES
    {
        client_window
    } else {
        1
    }
}

fn scale_quota(quota: memory::Quota, messages: usize) -> Result<memory::Quota, IntakeError> {
    Ok(memory::Quota {
        bytes: quota
            .bytes
            .checked_mul(messages)
            .ok_or(IntakeError::Configuration)?,
        buffers: quota
            .buffers
            .checked_mul(messages)
            .ok_or(IntakeError::Configuration)?,
    })
}

fn matches_fence(request: GrantRequest, fence: Option<ReceiveFence>) -> bool {
    (request.binding.kind == Kind::Broker && request.route.class == Class::Data) == fence.is_some()
        && fence.is_none_or(|fence| fence.scope().group_id == request.route.placement.group)
}

fn grant_spec(request: GrantRequest, fence: Option<ReceiveFence>, control: bool) -> GrantSpec {
    if let Some(fence) = fence {
        GrantSpec::replica(fence)
    } else if control {
        GrantTarget::Control(request.route.placement.shard).into()
    } else {
        GrantTarget::Partition(Subject {
            group: request.route.placement.group,
            writer: request.route.writer,
        })
        .into()
    }
}
