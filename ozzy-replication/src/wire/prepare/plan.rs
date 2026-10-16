//! Prepare encoding planned and checked before any byte is written.

use super::{
    BATCH_BYTES, Chain, EncodeContext, Envelope, NodeId, OpNumber, Opcode, Prefix, Prepare,
    PrepareEncoding, PrepareMetadata, ReceiveEpoch, WireError, WireLimits, WriteFields, Writer,
    metadata_length, validate_batch, validate_prefix, validate_scope, write_batch,
};

/// A checked prepare whose sizes are known before any byte is written.
pub(super) struct Planned<'a> {
    sender: NodeId,
    epoch: Option<ReceiveEpoch>,
    prepare: Prepare<'a>,
    first: u64,
    predecessor: Prefix,
    sizes: PrepareMetadata,
}

impl<'a> Planned<'a> {
    pub(super) fn new(
        sender: NodeId,
        epoch: Option<ReceiveEpoch>,
        prepare: Prepare<'a>,
        limits: WireLimits,
    ) -> Result<Self, WireError> {
        if sender.as_bytes() == &[0; 16] {
            return Err(ozzy_proto::EnvelopeError::ZeroIdentity.into());
        }
        validate_scope(prepare.scope)?;
        validate_prefix(prepare.committed)?;
        let count = prepare.operations.len();
        let batch_bytes = if epoch.is_some() {
            BATCH_BYTES + 16
        } else {
            BATCH_BYTES
        };
        let metadata_bytes = metadata_length(count, batch_bytes, limits)?;
        let Some(first) = prepare.operations.first().map(|op| op.canonical) else {
            return Err(WireError::Limit);
        };
        let predecessor = Prefix {
            op: OpNumber(first.op_number.checked_sub(1).ok_or(WireError::Chain)?),
            digest: first.previous_digest,
        };
        let chain = Chain {
            scope: prepare.scope,
            predecessor,
            committed: Some(prepare.committed),
        };
        let payload_bytes = validate_batch(chain, prepare.operations, limits)?;
        u32::try_from(metadata_bytes).map_err(|_| WireError::Limit)?;
        u32::try_from(payload_bytes).map_err(|_| WireError::Limit)?;
        Ok(Self {
            sender,
            epoch,
            prepare,
            first: first.op_number,
            predecessor,
            sizes: PrepareMetadata {
                metadata_bytes,
                payload_bytes,
            },
        })
    }

    /// Envelope the plan for one link session, then write it.
    pub(super) fn bind(
        &self,
        context: EncodeContext,
        metadata: &mut [u8],
        payload: Option<&mut [u8]>,
    ) -> Result<PrepareEncoding, WireError> {
        let header = Envelope {
            opcode: if self.epoch.is_some() {
                Opcode::PrepareFlow
            } else {
                Opcode::Prepare
            },
            response: false,
            request_id: None,
            sender: self.sender,
            session: Some(context.session),
        }
        .encode_header(
            self.sizes.metadata_bytes,
            self.sizes.payload_bytes,
            context.limits.envelope,
        )?;
        let sizes = self.write(metadata, payload)?;
        Ok(PrepareEncoding {
            header,
            metadata_bytes: sizes.metadata_bytes,
            payload_bytes: sizes.payload_bytes,
        })
    }

    pub(super) fn write(
        &self,
        metadata: &mut [u8],
        payload: Option<&mut [u8]>,
    ) -> Result<PrepareMetadata, WireError> {
        let sizes = self.sizes;
        if metadata.len() < sizes.metadata_bytes
            || payload
                .as_ref()
                .is_some_and(|payload| payload.len() < sizes.payload_bytes)
        {
            return Err(WireError::Capacity);
        }
        let mut writer = Writer::with_len(metadata, sizes.metadata_bytes)?;
        writer.scope(self.prepare.scope, self.sender)?;
        if let Some(epoch) = self.epoch {
            writer.bytes(&epoch.get().to_be_bytes())?;
        }
        writer.u64(self.first)?;
        writer.bytes(self.predecessor.digest.as_bytes())?;
        writer.prefix(self.prepare.committed)?;
        write_batch(&mut writer, payload, self.prepare.operations)?;
        Ok(sizes)
    }
}
