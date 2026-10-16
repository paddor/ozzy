use super::{JournalError, OwnedJournal, producer::Clock};
use crate::replica_journal::ProposalBuffer;
use ozzy_journal::operation::{DedupWatermark, OperationBody, encode_operation_body};
use ozzy_replication::driver::ValidationTicket;

/// One bounded identity-expiry step. It needs no settled journal: expiry
/// touches only confirmed rows, and the owner serializes it with validation.
#[derive(Debug, Default)]
pub struct IdentityRetention {
    /// Confirmed identities physically released by this step.
    pub expired: usize,
    /// Expirable identities remain; continue after a foreground turn.
    pub more_work: bool,
    /// Watermark proposal advancing idle logical time, awaiting confirmation.
    pub proposal: Option<ProposalBuffer>,
}

impl OwnedJournal {
    /// Expire confirmed identities under the current image without waiting
    /// for accepted or committed work to settle.
    pub(in crate::replica_journal) fn retain_identities_turn(
        &mut self,
        ticket: ValidationTicket,
        leader: bool,
    ) -> Result<IdentityRetention, JournalError> {
        self.healthy()?;
        self.validate_image(ticket)?;
        self.retain_identities(ticket, leader)
    }

    pub(in crate::replica_journal::owned) fn retain_identities(
        &mut self,
        ticket: ValidationTicket,
        leader: bool,
    ) -> Result<IdentityRetention, JournalError> {
        let Some(_) = self.dedup.limits else {
            return Ok(IdentityRetention::default());
        };
        let image = self
            .dedup
            .image
            .as_mut()
            .ok_or(JournalError::AppendMismatch)?;
        let expired = image.index.expire(256, 16 * 1024);
        let deadline = image.index.next_expiration_time();
        let more_work =
            expired != 0 && deadline.is_some_and(|at| image.index.committed_time() >= at);
        let mut turn = IdentityRetention {
            expired,
            more_work,
            proposal: None,
        };
        if !leader || deadline.is_none_or(|at| image.time >= at) {
            return Ok(turn);
        }
        let time = Clock::time(&mut self.dedup.clock, ticket, image.time)?;
        if deadline.is_none_or(|at| time < at) {
            return Ok(turn);
        }
        let partition = image.partition.ok_or(JournalError::AppendMismatch)?;
        let owner_epoch = self
            .images()?
            .speculative()
            .partition(partition)
            .ok_or(JournalError::AppendMismatch)?
            .owner_epoch;
        let mut proposal = match self.lease_proposal_buffer() {
            Ok(buffer) => buffer,
            Err(crate::replica_journal::SubmitError::Full) => return Ok(turn),
            Err(_) => return Err(JournalError::Stopped),
        };
        let body = OperationBody::DedupWatermark(DedupWatermark {
            partition,
            owner_epoch,
            logical_time: time,
        });
        match proposal.push(
            body.kind(),
            &encode_operation_body(&body, self.limits.operations)?,
        ) {
            Ok(()) => {}
            Err(JournalError::Io(error)) if error.kind() == std::io::ErrorKind::WouldBlock => {
                return Ok(turn);
            }
            Err(error) => return Err(error),
        }
        turn.proposal = Some(proposal);
        Ok(turn)
    }
}
