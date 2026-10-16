use super::{JournalError, OwnedJournal};
use ozzy_core::state::CoordinatorGroup;
use ozzy_proto::PartitionIncarnation;
use ozzy_replication::driver::ValidationTicket;
use std::sync::Arc;

impl OwnedJournal {
    /// Read one committed coordinator image without I/O. The image is shared
    /// with the canonical state; it never reflects accepted-only plans.
    pub fn coordinator_group(
        &self,
        ticket: ValidationTicket,
        coordinator: PartitionIncarnation,
        name: &str,
    ) -> Result<Option<Arc<CoordinatorGroup>>, JournalError> {
        self.active(ticket)?;
        if ticket.applied().op > self.applied.op {
            return Err(JournalError::AppendMismatch);
        }
        Ok(self
            .images()?
            .committed()
            .coordinator_group_by_name(coordinator, name)
            .cloned())
    }
}
