use super::{
    BrokerLinkError, BrokerLinks, Context, Cursor, Duration, Opening, Poll, Returning,
    TopicReaderError, TopicRoutes, reader,
};
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
        self.opening = Some(Opening {
            broker,
            session,
            source: subscribe.target.group_source().expect("group target"),
            request: Box::pin(async move {
                let selected = links.subscribe(broker, subscribe).await?;
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

    pub(in crate::replicated::topic_reader) fn detach(
        &mut self,
        links: &BrokerLinks,
    ) -> Option<Returning> {
        let selected = self.selected.take().or_else(|| self.inbox.selected());
        self.opening = None;
        self.pending = None;
        self.held = None;
        self.ready = None;
        self.inbox.clear();
        self.canceling.take().or_else(|| {
            selected.map(|(broker, session, selected)| {
                let links = links.clone();
                Box::pin(async move { links.unsubscribe(broker, session, selected).await })
                    as Returning
            })
        })
    }

    pub(super) fn poll_opening(
        &mut self,
        links: &BrokerLinks,
        routes: &TopicRoutes,
        now: Duration,
        cx: &mut Context<'_>,
    ) -> Result<(), TopicReaderError> {
        if let Some(opening) = &mut self.opening {
            match opening.request.as_mut().poll(cx) {
                Poll::Ready(Ok(selected)) => {
                    if self.selector.take().is_some() {
                        self.next = selected.2.resolved_offset;
                        self.live = ozzy_core::live::LiveCursor::new(self.next, self.refresh);
                    }
                    self.opening = None;
                    self.selected = Some(selected);
                    self.accepted = Some(selected);
                }
                Poll::Ready(Err(error)) => {
                    self.opening = None;
                    if let BrokerLinkError::RetentionGap { earliest } = error {
                        return Err(TopicReaderError::RetentionGap {
                            partition: self.number,
                            earliest,
                        });
                    }
                    if let BrokerLinkError::Seek(error) = error {
                        return Err(match error {
                            ozzy_core::reader::seek::SeekError::NotFound { earliest } => {
                                TopicReaderError::RecordNotFound {
                                    partition: self.number,
                                    earliest,
                                }
                            }
                            ozzy_core::reader::seek::SeekError::Ambiguous { first, last } => {
                                TopicReaderError::AmbiguousRecordId {
                                    partition: self.number,
                                    first,
                                    last,
                                }
                            }
                        });
                    }
                    if !super::retryable(&error) {
                        return Err(error.into());
                    }
                    routes.refresh(self.number)?;
                    // A temporary refusal names no other source. Ask the same
                    // broker again as soon as any other refused request.
                    self.due = now
                        + if matches!(
                            error,
                            BrokerLinkError::Rejected {
                                retry: ozzy_proto::nack::RetryClass::AfterBackoff,
                                ..
                            }
                        ) {
                            links.retry_interval().min(self.refresh)
                        } else {
                            self.refresh
                        };
                }
                Poll::Pending => {}
            }
        }
        Ok(())
    }

    pub(super) fn poll_returns(
        &mut self,
        routes: &TopicRoutes,
        cx: &mut Context<'_>,
    ) -> Result<(), TopicReaderError> {
        {
            let pending = &mut self.canceling;
            let result = pending.as_mut().map(|pending| pending.as_mut().poll(cx));
            match result {
                Some(Poll::Ready(Ok(()))) => {
                    *pending = None;
                    if self.live.is_paused() {
                        // A held publication may have found its gap while the
                        // old replay subscription was still closing.
                        self.due = Duration::ZERO;
                    }
                    cx.waker().wake_by_ref();
                }
                Some(Poll::Ready(Err(error))) => {
                    if !super::retryable(&error) {
                        return Err(error.into());
                    }
                    self.reset();
                    routes.refresh(self.number)?;
                }
                Some(Poll::Pending) | None => {}
            }
        }
        Ok(())
    }

    pub(super) fn retire(&mut self, links: &BrokerLinks) {
        if self.live.replay().is_none()
            && let Some((broker, session, selected)) = self.selected.take()
        {
            let links = links.clone();
            self.inbox.clear();
            self.canceling = Some(Box::pin(async move {
                links.unsubscribe(broker, session, selected).await
            }));
        }
    }
}
