//! Consumer group control requests: durable progress, source assignment,
//! membership, inspection, and deletion.

use super::{BrokerLinkError, BrokerLinks, driver};
use ozzy_proto::{NodeId, group, handshake, progress};
use std::time::Duration;

impl BrokerLinks {
    /// Query or advance one assigned source's durable processing cursor. The
    /// source validates the member/epoch and its configured confirmation policy.
    /// Retry uncertain updates cumulatively; transport receipt confirms nothing.
    pub async fn group_progress(
        &self,
        broker: NodeId,
        request: progress::Request,
    ) -> Result<progress::Confirmed, BrokerLinkError> {
        let deadline = self
            .clock()
            .now()
            .saturating_add(self.0.config.request_timeout);
        self.group_progress_until(broker, request, deadline).await
    }

    pub(in crate::replicated) fn require_progress(&self) -> Result<(), BrokerLinkError> {
        if self.0.config.parameters.roles & handshake::CONSUMER == 0
            || self.0.config.parameters.required_capabilities & handshake::OWNER_PROGRESS == 0
        {
            return Err(BrokerLinkError::Configuration);
        }
        Ok(())
    }

    pub(in crate::replicated) async fn group_progress_until(
        &self,
        broker: NodeId,
        request: progress::Request,
        deadline: Duration,
    ) -> Result<progress::Confirmed, BrokerLinkError> {
        self.require_progress()?;
        match self
            .request_until(broker, driver::Body::Progress(request), deadline)
            .await?
        {
            driver::Reply::Progress(confirmed) => Ok(confirmed),
            _ => Err(BrokerLinkError::Response),
        }
    }

    pub(in crate::replicated) fn require_groups(&self) -> Result<(), BrokerLinkError> {
        self.require_progress()?;
        if self.0.config.parameters.required_capabilities & handshake::OWNER_GROUPS == 0 {
            return Err(BrokerLinkError::Configuration);
        }
        Ok(())
    }

    /// Relay one committed coordinator target to its source leader. The source
    /// allocates the epoch and captures the resume cursor; a repeated plan
    /// revision returns the original activation.
    pub async fn group_assign(
        &self,
        broker: NodeId,
        request: group::AssignRequest,
    ) -> Result<group::AssignReply, BrokerLinkError> {
        let deadline = self
            .clock()
            .now()
            .saturating_add(self.0.config.request_timeout);
        self.group_assign_until(broker, request, deadline).await
    }

    pub(in crate::replicated) async fn group_assign_until(
        &self,
        broker: NodeId,
        request: group::AssignRequest,
        deadline: Duration,
    ) -> Result<group::AssignReply, BrokerLinkError> {
        self.require_groups()?;
        match self
            .request_until(broker, driver::Body::Assign(request), deadline)
            .await?
        {
            driver::Reply::Assigned(reply) => Ok(reply),
            _ => Err(BrokerLinkError::Response),
        }
    }

    /// Join, heartbeat, or leave on the group's coordinator leader. The reply
    /// describes committed intent; sources confirm activation separately.
    pub async fn group_join(
        &self,
        broker: NodeId,
        request: group::JoinRequest,
    ) -> Result<group::JoinReply, BrokerLinkError> {
        let deadline = self
            .clock()
            .now()
            .saturating_add(self.0.config.request_timeout);
        self.group_join_until(broker, request, deadline).await
    }

    pub(in crate::replicated) async fn group_join_until(
        &self,
        broker: NodeId,
        request: group::JoinRequest,
        deadline: Duration,
    ) -> Result<group::JoinReply, BrokerLinkError> {
        self.require_groups()?;
        match self
            .request_until(broker, driver::Body::Join(Box::new(request)), deadline)
            .await?
        {
            driver::Reply::Joined(reply) => Ok(reply),
            _ => Err(BrokerLinkError::Response),
        }
    }

    pub(in crate::replicated) async fn group_inspect_until(
        &self,
        broker: NodeId,
        request: group::InspectRequest,
        deadline: Duration,
    ) -> Result<group::Inspection, BrokerLinkError> {
        self.require_groups()?;
        match self
            .request_until(broker, driver::Body::Inspect(Box::new(request)), deadline)
            .await?
        {
            driver::Reply::Inspected(reply) => Ok(*reply),
            _ => Err(BrokerLinkError::Response),
        }
    }

    pub(in crate::replicated) async fn group_delete_until(
        &self,
        broker: NodeId,
        request: group::DeleteRequest,
        deadline: Duration,
    ) -> Result<group::DeleteReply, BrokerLinkError> {
        self.require_groups()?;
        match self
            .request_until(broker, driver::Body::Delete(Box::new(request)), deadline)
            .await?
        {
            driver::Reply::Deleted(reply) => Ok(reply),
            _ => Err(BrokerLinkError::Response),
        }
    }
}
