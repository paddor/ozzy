//! Scope, prefix, and digest fields shared by every replica message, with
//! their structural validation.

use super::{PeerBinding, WireError};
use crate::{Digest, OpNumber, Prefix, Scope};
use ozzy_proto::layout::{Reader, Writer};
use ozzy_proto::{GroupId, NodeId};

pub(super) fn validate_scope(scope: Scope) -> Result<(), WireError> {
    if scope.group_id.as_bytes() == &[0; 16] || scope.configuration_digest == Digest::ZERO {
        return Err(WireError::Scope);
    }
    Ok(())
}

pub(super) fn validate_election_view(scope: Scope) -> Result<(), WireError> {
    if scope.view == 0 {
        return Err(WireError::History);
    }
    Ok(())
}

pub(super) fn validate_prefix(prefix: Prefix) -> Result<(), WireError> {
    if (prefix.op.0 == 0) != (prefix.digest == Digest::ZERO) || prefix.op.0 == u64::MAX {
        return Err(WireError::Prefix);
    }
    Ok(())
}

/// Replica fields over the shared layout reader.
pub(super) trait ReadFields {
    fn digest(&mut self) -> Result<Digest, WireError>;
    fn prefix(&mut self) -> Result<Prefix, WireError>;
    fn routing_scope(&mut self, sender: NodeId) -> Result<Scope, WireError>;
    fn scope(&mut self, binding: PeerBinding) -> Result<Scope, WireError>;
}

impl ReadFields for Reader<'_> {
    fn digest(&mut self) -> Result<Digest, WireError> {
        Ok(Digest::from_bytes(self.array()?))
    }
    fn prefix(&mut self) -> Result<Prefix, WireError> {
        let value = Prefix {
            op: OpNumber(self.u64()?),
            digest: self.digest()?,
        };
        validate_prefix(value)?;
        Ok(value)
    }
    fn routing_scope(&mut self, sender: NodeId) -> Result<Scope, WireError> {
        let group_id = GroupId::from_bytes(self.array()?);
        let configuration_epoch = self.u64()?;
        let view = self.u64()?;
        let voter = NodeId::from_bytes(self.array()?);
        let configuration_digest = self.digest()?;
        if voter != sender {
            return Err(WireError::Peer);
        }
        let scope = Scope {
            group_id,
            configuration_epoch,
            configuration_digest,
            view,
        };
        validate_scope(scope)?;
        if configuration_epoch == 0 {
            return Err(WireError::Scope);
        }
        Ok(scope)
    }

    fn scope(&mut self, binding: PeerBinding) -> Result<Scope, WireError> {
        let scope = self.routing_scope(binding.peer)?;
        if scope
            != (Scope {
                view: scope.view,
                ..binding.configuration.scope()
            })
        {
            return Err(WireError::Scope);
        }
        Ok(scope)
    }
}

/// Replica fields over the shared layout writer.
pub(super) trait WriteFields {
    fn prefix(&mut self, prefix: Prefix) -> Result<(), WireError>;
    fn scope(&mut self, scope: Scope, voter: NodeId) -> Result<(), WireError>;
}

impl WriteFields for Writer<'_> {
    fn prefix(&mut self, prefix: Prefix) -> Result<(), WireError> {
        self.u64(prefix.op.0)?;
        self.bytes(prefix.digest.as_bytes())?;
        Ok(())
    }
    fn scope(&mut self, scope: Scope, voter: NodeId) -> Result<(), WireError> {
        self.bytes(scope.group_id.as_bytes())?;
        self.u64(scope.configuration_epoch)?;
        self.u64(scope.view)?;
        self.bytes(voter.as_bytes())?;
        self.bytes(scope.configuration_digest.as_bytes())?;
        Ok(())
    }
}
