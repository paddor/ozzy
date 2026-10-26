//! A cursor's broker subscription: open, acknowledge, and return it.

use super::{
    BrokerLinkError, BrokerLinks, Context, Cursor, Duration, Opening, Poll, Returning,
    TopicReaderError, TopicRoutes, reader,
};
use ozzy_core::reader::seek::SeekError;
use ozzy_proto::Offset;

impl Cursor {
    pub(super) fn open(
        &mut self,
        links: &BrokerLinks,
        routes: &TopicRoutes,
        route: Option<ozzy_proto::directory::RouteState>,
    ) -> Result<(), BrokerLinkError> {
        let Some(route) = route else { return Ok(()) };
        let Some(broker) = route.leader else {
            return Ok(());
        };
        let Some(session) = links.session(broker) else {
            return Ok(());
        };
        let partition = routes
            .metadata()
            .partition(self.number)
            .ok_or(BrokerLinkError::Configuration)?;
        let subscribe = reader::Subscribe {
            subscription: reader::Subscription {
                id: self.inbox.id,
                generation: u128::from_be_bytes(*links.next_request()?.as_bytes()),
            },
            target: reader::Target::Group {
                authority: ozzy_proto::data::Authority {
                    group_id: partition.group,
                    config_epoch: partition.config_epoch,
                    view: route.view,
                },
                partition: partition.incarnation,
                owner_epoch: 1,
            },
            start: self.selector.unwrap_or(reader::Start::Offset(self.next)),
        };
        let links = links.clone();
        let inbox = std::sync::Arc::downgrade(&self.inbox);
        let renew = self.renew;
        self.opening = Some(Opening {
            broker,
            session,
            subscription: subscribe.subscription,
            source: subscribe
                .target
                .group_source()
                .ok_or(BrokerLinkError::Configuration)?,
            request: Box::pin(async move {
                let selected = links.subscribe(broker, inbox, subscribe, renew).await?;
                Ok((broker, session, selected))
            }),
        });
        Ok(())
    }

    pub(in crate::replicated::topic_reader) async fn acknowledge(
        &self,
        links: &BrokerLinks,
        processed: Option<Offset>,
    ) -> Result<(), TopicReaderError> {
        if processed.map(Offset::get) > self.next.checked_sub(1) {
            return Err(BrokerLinkError::Response.into());
        }
        if let Some((broker, _, selected)) = self.selected {
            links
                .reader_ack(
                    broker,
                    reader::Ack {
                        subscription: selected.subscription,
                        source: selected.source,
                        received: self.next.checked_sub(1),
                        processed: processed.map(Offset::get),
                    },
                )
                .await?;
        }
        Ok(())
    }

    /// End the subscription, including a live reader's topic grant, after
    /// any replay cancellation still in flight.
    pub(in crate::replicated::topic_reader) fn detach(
        &mut self,
        links: &BrokerLinks,
    ) -> Option<Returning> {
        let selected = (self.selected.take())
            .or_else(|| self.inbox.selected())
            .or(self.accepted);
        self.opening = None;
        self.pending = None;
        self.held = None;
        self.ready = None;
        self.clear_replay();
        let canceling = self.canceling.take();
        if canceling.is_none() && selected.is_none() {
            return None;
        }
        let links = links.clone();
        Some(Box::pin(async move {
            if let Some(canceling) = canceling {
                canceling.await?;
            }
            let Some((broker, session, selected)) = selected else {
                return Ok(());
            };
            let unsubscribe = reader::Unsubscribe {
                selected,
                cancel: reader::Cancel::Subscription,
            };
            links.unsubscribe(broker, session, unsubscribe).await
        }))
    }

    pub(super) fn poll_opening(
        &mut self,
        links: &BrokerLinks,
        routes: &TopicRoutes,
        now: Duration,
        cx: &mut Context<'_>,
    ) -> Result<(), TopicReaderError> {
        let Some(opening) = &mut self.opening else {
            return Ok(());
        };
        let Poll::Ready(result) = opening.request.as_mut().poll(cx) else {
            return Ok(());
        };
        self.opening = None;
        match result {
            Ok(selected) => {
                if self.selector.take().is_some() {
                    self.next = selected.2.resolved_offset;
                    self.live = ozzy_core::live::LiveCursor::new(self.next, self.refresh);
                }
                self.selected = Some(selected);
                self.accepted = Some(selected);
                self.renew = false;
                Ok(())
            }
            Err(error) => {
                self.early = None;
                self.opening_failed(links, routes, now, error)
            }
        }
    }

    /// A refused subscription: a reader error, or the time to ask again.
    fn opening_failed(
        &mut self,
        links: &BrokerLinks,
        routes: &TopicRoutes,
        now: Duration,
        error: BrokerLinkError,
    ) -> Result<(), TopicReaderError> {
        let partition = self.number;
        let error = match error {
            BrokerLinkError::RetentionGap { earliest } => {
                return Err(TopicReaderError::RetentionGap {
                    partition,
                    earliest,
                });
            }
            BrokerLinkError::Seek(SeekError::NotFound { earliest }) => {
                return Err(TopicReaderError::RecordNotFound {
                    partition,
                    earliest,
                });
            }
            BrokerLinkError::Seek(SeekError::Ambiguous { first, last }) => {
                return Err(TopicReaderError::AmbiguousRecordId {
                    partition,
                    first,
                    last,
                });
            }
            error => error,
        };
        if !super::retryable(&error) {
            return Err(error.into());
        }
        routes.refresh(self.number)?;
        // A temporary refusal names no other source. Ask the same broker
        // again as soon as any other refused request.
        let backoff = matches!(
            error,
            BrokerLinkError::Rejected {
                retry: ozzy_proto::nack::RetryClass::AfterBackoff,
                ..
            }
        );
        self.due = now
            + if backoff {
                links.retry_interval().min(self.refresh)
            } else {
                self.refresh
            };
        Ok(())
    }

    pub(super) fn poll_returns(
        &mut self,
        routes: &TopicRoutes,
        cx: &mut Context<'_>,
    ) -> Result<(), TopicReaderError> {
        let Some(canceling) = &mut self.canceling else {
            return Ok(());
        };
        match canceling.as_mut().poll(cx) {
            Poll::Ready(Ok(())) => {
                self.canceling = None;
                if self.live.is_paused() {
                    // A held publication may have found its gap while the
                    // old replay subscription was still closing.
                    self.due = Duration::ZERO;
                }
                cx.waker().wake_by_ref();
            }
            Poll::Ready(Err(error)) => {
                if !super::retryable(&error) {
                    return Err(error.into());
                }
                self.reset();
                routes.refresh(self.number)?;
            }
            Poll::Pending => {}
        }
        Ok(())
    }

    pub(super) fn retire(
        &mut self,
        links: &BrokerLinks,
        routes: &TopicRoutes,
        cx: &mut Context<'_>,
    ) -> Result<(), TopicReaderError> {
        if self.live.replay().is_none()
            && let Some((broker, session, selected)) = self.selected.take()
        {
            let links = links.clone();
            self.clear_replay();
            // Live delivery took over; the live topic stays granted.
            let unsubscribe = reader::Unsubscribe {
                selected,
                cancel: reader::Cancel::Replay,
            };
            self.canceling = Some(Box::pin(async move {
                links.unsubscribe(broker, session, unsubscribe).await
            }));
            // Queue the UNSUBSCRIBE now. Until the driver sends it, frames
            // of this generation still enter the inbox.
            self.poll_returns(routes, cx)?;
        }
        Ok(())
    }
}
