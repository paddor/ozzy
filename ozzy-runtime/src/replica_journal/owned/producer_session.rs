//! Confirmed writer sessions through ordinary canonical proposal authority.

use super::{AppendBuffer, JournalError, OwnedJournal};
use crate::replica_journal::AppendAdmissionError;
use ozzy_core::state::{CanonicalImagesError, IdentityIndex, IdentityKey, StateError};
use ozzy_journal::operation::{OpenProducer, OperationBody, decode_operation_body};
use ozzy_journal_segment::AsyncIdentityResolveError;
use ozzy_proto::{
    append::Authority,
    producer::{Mode, Opened},
};
use ozzy_replication::{Prefix, driver::ValidationTicket};

impl OwnedJournal {
    /// Returns a conservative accepted prefix for a resume or exact retry.
    /// The actor must wait for confirmation and application before using the
    /// captured session coordinates as a reply. Fresh transitions use the
    /// ordinary validation, admission, and writeback path below this step.
    pub(super) async fn prepare_producer_open(
        &mut self,
        ticket: ValidationTicket,
        buffer: &mut AppendBuffer,
    ) -> Result<Option<Prefix>, JournalError> {
        let session = buffer
            .producer_session
            .as_mut()
            .ok_or(JournalError::AppendMismatch)?;
        let request = session.request;
        session.opened = None;
        if buffer.len() != 1 || buffer.is_producer() || buffer.is_partition_creation() {
            return Err(JournalError::AppendMismatch);
        }
        self.validate_image(ticket)?;
        let operation = buffer.operations().next().expect("one open");
        let OperationBody::OpenProducer(expected) =
            decode_operation_body(operation.kind, operation.body, self.limits.operations)?
        else {
            return Err(JournalError::AppendMismatch);
        };
        let current_state = self
            .images()?
            .speculative()
            .partition(request.partition)
            .ok_or(AppendAdmissionError::UnknownPartition)?
            .producer(request.producer);
        let current = current_state.map(|state| {
            (
                state.producer_epoch.get(),
                state.next_producer_sequence.get(),
                state.producer_result_floor.get(),
            )
        });
        // The latest transition belongs to canonical state, not a historical
        // index. Retiring its segment must not turn an exact fence retry into
        // another transition or force a read of deleted operation bytes.
        if let Some(state) = current_state
            && let Some(transition) = state.transition()
            && transition.operation_id == request.operation
        {
            if transition.expected_epoch != expected.expected_epoch
                || state.producer_epoch != expected.new_epoch
            {
                return Err(JournalError::AppendMismatch);
            }
            buffer
                .producer_session
                .as_mut()
                .expect("open request")
                .opened = Some(opened(
                ticket,
                request,
                (
                    state.producer_epoch.get(),
                    state.next_producer_sequence.get(),
                    state.producer_result_floor.get(),
                ),
                self.configuration.append_policy(),
            ));
            return Ok(Some(ticket.accepted()));
        }
        let (coordinates, resolved) = self
            .resolve_producer_open(request, expected, current)
            .await?;
        buffer
            .producer_session
            .as_mut()
            .expect("open request")
            .opened = Some(opened(
            ticket,
            request,
            coordinates,
            self.configuration.append_policy(),
        ));
        Ok(resolved.then_some(ticket.accepted()))
    }

    async fn resolve_producer_open(
        &mut self,
        request: ozzy_proto::producer::Open,
        expected: OpenProducer,
        current: Option<(u64, u64, u64)>,
    ) -> Result<((u64, u64, u64), bool), JournalError> {
        let key = IdentityKey::operation(request.operation);
        let claim = {
            let identities = self.images()?.speculative_identities();
            identities
                .resolve(&[key])
                .await
                .map_err(|error| match error {
                    AsyncIdentityResolveError::Lookup(error) => JournalError::Index(error),
                    AsyncIdentityResolveError::Identity(error) => identity_error(error),
                })?;
            identities.lookup(key).map_err(identity_error)?
        };
        if let Some(claim) = claim {
            let original = if let Some(open) = self.writeback.producer_open(
                request.operation,
                claim.op_number,
                self.limits.operations,
            )? {
                open
            } else {
                self.read_producer_open(request.operation, claim.op_number)
                    .await?
            };
            if original != expected {
                return Err(JournalError::AppendMismatch);
            }
            let (epoch, sequence, floor) = current.ok_or(AppendAdmissionError::Fenced)?;
            if epoch != original.new_epoch.get() {
                return Err(AppendAdmissionError::Fenced.into());
            }
            Ok(((epoch, sequence, floor), true))
        } else {
            Ok(match (request.mode, request.expected_epoch, current) {
                (Mode::Resume, Some(expected_epoch), Some((epoch, sequence, floor)))
                    if expected_epoch == epoch =>
                {
                    ((epoch, sequence, floor), true)
                }
                (Mode::Resume, None, Some(coordinates)) => (coordinates, true),
                (Mode::Resume | Mode::Create, None, None) => ((1, 0, 0), false),
                (Mode::Fence, Some(expected_epoch), Some((epoch, _, _)))
                    if expected_epoch == epoch =>
                {
                    ((expected.new_epoch.get(), 0, 0), false)
                }
                _ => return Err(AppendAdmissionError::Fenced.into()),
            })
        }
    }

    async fn read_producer_open(
        &mut self,
        id: ozzy_proto::OperationId,
        number: u64,
    ) -> Result<OpenProducer, JournalError> {
        let snapshot = self.journal.retry_snapshot(self.recovery.index).await?;
        let operation = snapshot
            .read_operation(id)
            .await?
            .ok_or(JournalError::AppendMismatch)?;
        if operation.op_number != number {
            return Err(JournalError::AppendMismatch);
        }
        let OperationBody::OpenProducer(open) = decode_operation_body(
            operation.kind,
            operation.body.as_ref(),
            self.limits.operations,
        )?
        else {
            return Err(JournalError::AppendMismatch);
        };
        Ok(open)
    }
}

fn opened(
    ticket: ValidationTicket,
    request: ozzy_proto::producer::Open,
    (epoch, next_sequence, retry_floor): (u64, u64, u64),
    policy: ozzy_proto::append::Policy,
) -> Opened {
    Opened {
        authority: Authority {
            group_id: ticket.scope().group_id,
            config_epoch: ticket.scope().configuration_epoch,
            view: ticket.scope().view,
        },
        partition: request.partition,
        producer: request.producer,
        epoch,
        next_sequence,
        retry_floor,
        policy,
    }
}

fn identity_error(error: ozzy_core::state::IdentityIndexError) -> JournalError {
    JournalError::Images(CanonicalImagesError::State(StateError::IdentityIndex(
        error,
    )))
}
