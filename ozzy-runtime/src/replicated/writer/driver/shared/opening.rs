//! Stable first-use producer transition, separate from physical link sessions.

use super::{Authority, Error, Failure, TopicRoutes};
use crate::replicated::{BrokerLinkError, WriterConfig};
use ozzy_proto::{OperationId, directory::RouteState, nack::RetryClass, producer};

pub(super) struct Opening {
    operation: Option<OperationId>,
    confirmed: bool,
}

impl Opening {
    pub(super) fn new(operation: Option<OperationId>) -> Self {
        Self {
            operation,
            confirmed: operation.is_none(),
        }
    }

    pub(super) async fn confirm(
        &mut self,
        routes: &TopicRoutes,
        route: &RouteState,
        config: &WriterConfig,
    ) -> Result<(), Failure> {
        if self.confirmed {
            return Ok(());
        }
        let opened = routes
            .links()
            .open_producer(
                route.leader.expect("selected leader"),
                producer::Open {
                    authority: Authority {
                        group_id: route.group,
                        config_epoch: route.config_epoch,
                        view: route.view,
                    },
                    partition: route.partition,
                    producer: config.producer_id,
                    mode: producer::Mode::Create,
                    expected_epoch: None,
                    operation: self.operation.expect("fresh producer operation"),
                },
                config.policy,
            )
            .await
            .map_err(failure)?;
        if opened.epoch != config.producer_epoch
            || opened.next_sequence != config.next_sequence
            || opened.retry_floor != 0
        {
            return Err(Failure::Fatal(Error::Response));
        }
        self.confirmed = true;
        Ok(())
    }
}

fn failure(error: BrokerLinkError) -> Failure {
    match error {
        BrokerLinkError::Timeout | BrokerLinkError::Session | BrokerLinkError::Transport(_) => {
            Failure::Retry(None)
        }
        BrokerLinkError::Rejected {
            retry: RetryClass::AfterBackoff,
            ..
        } => Failure::Later,
        BrokerLinkError::Rejected { retry, hint, .. } if retry != RetryClass::Permanent => {
            Failure::Retry(hint)
        }
        error => Failure::Fatal(error.into()),
    }
}
