//! Stable first-use producer transition, separate from physical link sessions.

use super::{Error, Failure, TopicRoutes};
use crate::replicated::{BrokerLinkError, WriterConfig, broker_links::LeaderRoute};
use ozzy_proto::{OperationId, nack::RetryClass, producer};

pub(super) struct Opening {
    /// Create operation of a fresh producer, until a broker confirms it.
    pending: Option<OperationId>,
}

impl Opening {
    pub(super) fn new(pending: Option<OperationId>) -> Self {
        Self { pending }
    }

    pub(super) async fn confirm(
        &mut self,
        routes: &TopicRoutes,
        selected: &LeaderRoute,
        config: &WriterConfig,
    ) -> Result<(), Failure> {
        let Some(operation) = self.pending else {
            return Ok(());
        };
        let opened = routes
            .links()
            .open_producer(
                selected.leader,
                producer::Open {
                    authority: selected.route.authority(),
                    partition: selected.route.partition,
                    producer: config.producer_id,
                    mode: producer::Mode::Create,
                    expected_epoch: None,
                    operation,
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
        self.pending = None;
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
