//! Consumer control intake shared by durable progress and group activation.

use super::{
    Command, NativeIntake, NativeIntakeError, NativeReceive, PrepareFailure, Request,
    prepare_failure,
};
use crate::{
    frontend::Link,
    profiling::Event,
    replica_actor::response,
    replica_journal::{GroupAssignRequest, JournalError, ProposalBuffer},
};
use ozzy_proto::{
    OperationId, Packet, PartitionIncarnation,
    append::{Authority, Policy},
    group, handshake,
    nack::{AuthorityHint, RetryClass},
    progress,
};

/// What one kind of consumer control request requires of its client.
pub(super) struct ControlKind {
    command: Command,
    /// Reply metadata the client's link must carry.
    reply_bytes: usize,
    /// HELLO capability the consumer must have offered.
    capability: u16,
    slot_refusal: Event,
    arena_refusal: Event,
}

pub(super) const PROGRESS: ControlKind = ControlKind {
    command: Command::Progress,
    reply_bytes: progress::CONFIRMED_BYTES,
    capability: handshake::OWNER_PROGRESS,
    slot_refusal: Event::ProcessingSlotRefusal,
    arena_refusal: Event::ProcessingArenaRefusal,
};

pub(super) const ASSIGN: ControlKind = ControlKind {
    command: Command::Assign,
    reply_bytes: group::ASSIGNED_BYTES,
    capability: handshake::OWNER_GROUPS,
    slot_refusal: Event::AssignmentSlotRefusal,
    arena_refusal: Event::AssignmentArenaRefusal,
};

/// A control request that holds a free slot and passed the client checks.
#[derive(Clone, Copy)]
pub(super) struct Control {
    pub(super) slot: usize,
    pub(super) request: Request,
    pub(super) hint: AuthorityHint,
}

/// The proposal operation a correlated control request names.
pub(super) fn operation(packet: &Packet<'_>) -> Option<OperationId> {
    let request = packet.envelope.request_id?;
    Some(OperationId::from_bytes(*request.as_bytes()))
}

impl NativeIntake {
    /// Claim a control slot for a request from a consumer allowed to send it,
    /// or say how the request was answered instead.
    pub(super) fn admit_control(
        &mut self,
        packet: &Packet<'_>,
        link: Link,
        hint: AuthorityHint,
        kind: &ControlKind,
    ) -> Result<Result<Control, NativeReceive>, NativeIntakeError> {
        let Some(slot) =
            (self.progress_start()..self.slots.len()).find(|&index| self.slot_free(index))
        else {
            if self.config.progress_requests == 0 {
                return Ok(Err(NativeReceive::Ignored));
            }
            crate::profiling::event(kind.slot_refusal);
            return Ok(Err(NativeReceive::Busy));
        };
        if link.send.envelope.max_metadata_bytes < kind.reply_bytes {
            return Ok(Err(NativeReceive::Ignored));
        }
        let control = Control {
            slot,
            request: Request {
                envelope: packet.envelope,
                send: link.send,
                command: kind.command,
            },
            hint,
        };
        self.remaining = self.slots.len();
        if link.remote.roles & handshake::CONSUMER == 0
            || link.remote.capabilities & kind.capability == 0
        {
            return self.refuse(&control, (3, RetryClass::Permanent)).map(Err);
        }
        Ok(Ok(control))
    }

    /// Answer a claimed control request with a rejection.
    pub(super) fn refuse(
        &mut self,
        control: &Control,
        rejection: (u16, RetryClass),
    ) -> Result<NativeReceive, NativeIntakeError> {
        self.reject(control.slot, control.request, rejection, control.hint)?;
        Ok(NativeReceive::Accepted)
    }

    /// Partition, leadership, and policy checks of a decoded control body.
    pub(super) fn control_target(
        &self,
        partition: PartitionIncarnation,
        authority: Authority,
        policy: Policy,
        hint: AuthorityHint,
    ) -> Option<(u16, RetryClass)> {
        if partition != self.config.partition
            || !response::leads(self.config.group, self.config.local, authority, hint)
        {
            return Some((5, RetryClass::AfterAuthorityRefresh));
        }
        (policy != self.config.policy).then_some((2, RetryClass::Permanent))
    }

    /// Prepare a control body in the claimed slot's arena.
    pub(super) fn prepare_control(
        &mut self,
        control: &Control,
        prepare: impl FnOnce(&mut ProposalBuffer) -> Result<(), JournalError>,
    ) -> Result<(), PrepareFailure> {
        let buffer = self.slots[control.slot]
            .buffer
            .as_mut()
            .ok_or(PrepareFailure::Busy)?;
        buffer.clear();
        let prepared = prepare(buffer).map_err(|error| prepare_failure(&error));
        if prepared.is_err() {
            buffer.clear();
        }
        prepared
    }

    /// A body that did not prepare waits for arena memory or is rejected.
    pub(super) fn control_failure(
        &mut self,
        control: &Control,
        kind: &ControlKind,
        failure: PrepareFailure,
    ) -> Result<NativeReceive, NativeIntakeError> {
        match failure {
            PrepareFailure::Busy => {
                crate::profiling::event(kind.arena_refusal);
                Ok(NativeReceive::Busy)
            }
            PrepareFailure::Reject(code, retry) => self.refuse(control, (code, retry)),
        }
    }

    /// Relay one committed coordinator target to this source. The source
    /// allocates the epoch and captures the cursor; preparation proves nothing.
    pub(super) fn receive_assign(
        &mut self,
        packet: Packet<'_>,
        link: Link,
        hint: AuthorityHint,
    ) -> Result<NativeReceive, NativeIntakeError> {
        let control = match self.admit_control(&packet, link, hint, &ASSIGN)? {
            Ok(control) => control,
            Err(received) => return Ok(received),
        };
        let Ok(assign) = group::decode_assign(packet, self.config.limits.envelope) else {
            return self.refuse(&control, (1, RetryClass::Permanent));
        };
        if let Some(rejection) =
            self.control_target(assign.partition, assign.authority, assign.policy, hint)
        {
            return self.refuse(&control, rejection);
        }
        let Some(operation) = operation(&packet) else {
            return self.refuse(&control, (1, RetryClass::Permanent));
        };
        let request = GroupAssignRequest {
            authority: assign.authority,
            group: assign.group,
            partition: assign.partition,
            plan_revision: assign.plan_revision,
            member: assign.member,
            initial: assign.initial,
            operation,
        };
        let prepared = self.prepare_control(&control, |buffer| {
            buffer.prepare_group_assignment(request, assign.policy)
        });
        match prepared {
            Ok(()) => self.submit_prepared(control.slot, control.request, hint),
            Err(failure) => self.control_failure(&control, &ASSIGN, failure),
        }
    }
}
