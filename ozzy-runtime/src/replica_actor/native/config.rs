//! Fixed per-partition native intake resources and their startup checks.

use super::{NativeAccess, NativeIntakeError};
use crate::replica_journal::ProposalBuffer;
use ozzy_proto::{GroupId, NodeId, PartitionIncarnation, append, data::DataLimits};

/// Fixed per-partition resources. The broker link and its HELLO are shared;
/// journal proposals and logical writer authorization stay on this shard.
#[derive(Clone, Debug)]
pub struct NativeIntakeConfig {
    /// Broker process identity.
    pub local: NodeId,
    /// Immutable partition group.
    pub group: GroupId,
    /// Immutable partition incarnation.
    pub partition: PartitionIncarnation,
    /// Exact configured confirmation boundary.
    pub policy: append::Policy,
    /// Trusted physical clients or explicitly provisioned logical writers.
    pub access: NativeAccess,
    /// Full validation limits, independently bounded by journal arenas.
    pub limits: DataLimits,
    /// Reserve canonical outcome/fingerprint space for a dedup-enabled owner.
    /// This sizes arenas; the journal's persisted topic policy owns semantics.
    pub reserve_dedup_results: bool,
    /// Outstanding APPENDs per writer. Each also reserves one separate open slot.
    pub requests_per_writer: usize,
    /// Independent consumer control slots shared by durable progress and group
    /// activation requests; zero disables both services.
    pub progress_requests: usize,
    /// Progress updates one proposal may carry, at most the actor's live
    /// window. Updates that arrive while one is in flight join the next.
    /// One disables batching.
    pub progress_batch: usize,
    /// Slots polled per turn, including blocked destinations.
    pub turn_slots: usize,
}

impl NativeIntakeConfig {
    /// All startup arenas, including independent processing-progress requests.
    pub fn required_buffers(&self) -> Option<usize> {
        self.access
            .required_buffers(self.requests_per_writer)?
            .checked_add(self.progress_requests)
    }

    /// Exact arena profile for a startup slot. APPEND slots cover the complete
    /// canonical request. Writer-open slots need at most 65 bytes; independent
    /// rejection slots never prepare a body; consumer control slots hold one
    /// assignment body or a batch of progress bodies. Narrow allocation limits
    /// prevent a small control request from retaining a large cached APPEND
    /// allocation.
    pub fn buffer_limits(&self, index: usize) -> Option<ozzy_replication::PipelineLimits> {
        let count = self.required_buffers()?;
        let stride = self.requests_per_writer.checked_add(1)?;
        if self.requests_per_writer == 0 || index >= count {
            return None;
        }
        let writer_slots = self.access.writer_count().checked_mul(stride)?;
        let progress_start = self.access.required_buffers(self.requests_per_writer)?;
        if index >= progress_start {
            // Any consumer control arena may carry a batch of progress updates.
            return Some(ozzy_replication::PipelineLimits {
                max_operations: self.progress_batch,
                max_body_bytes: self.progress_batch.checked_mul(98)?,
            });
        }
        let max_body_bytes = if index >= writer_slots {
            1
        } else if index.is_multiple_of(stride) {
            65
        } else {
            crate::replicated::ClientConfig {
                peers: Vec::new(),
                limits: self.limits,
            }
            .prepared_canonical_body_bytes(self.reserve_dedup_results)?
        };
        Some(ozzy_replication::PipelineLimits {
            max_operations: 1,
            max_body_bytes,
        })
    }
}

pub(super) fn validate(
    config: &NativeIntakeConfig,
    buffers: &[ProposalBuffer],
) -> Result<(), NativeIntakeError> {
    config.access.validate(config.local)?;
    let count = config.required_buffers();
    if config.local.as_bytes() == &[0; 16]
        || config.group.as_bytes() == &[0; 16]
        || config.partition.as_bytes() == &[0; 16]
        || config.requests_per_writer == 0
        || config.progress_batch == 0
        || count != Some(buffers.len())
        || buffers.len() > 1024
        || !(1..=1024).contains(&config.turn_slots)
        || config.limits.max_records == 0
        || config.limits.max_parts == 0
        || config.limits.envelope.max_metadata_bytes < 107
        || buffers.iter().enumerate().any(|(index, buffer)| {
            !buffer.is_empty()
                || buffer.limits().max_operations == 0
                || config
                    .buffer_limits(index)
                    .is_none_or(|limits| limits.max_body_bytes > buffer.limits().max_body_bytes)
        })
    {
        return Err(NativeIntakeError::Configuration);
    }
    Ok(())
}
