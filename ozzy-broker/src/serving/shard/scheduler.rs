use super::{
    Binding, Context, Duration, Future, GroupId, IntakeMessage, Kind, LinkSessionId, NativeReceive,
    PartitionStatus, Poll, ProposalOutcome, StartupError, State, admission, failure,
};

impl State {
    pub(super) fn poll(
        &mut self,
        cx: &mut Context<'_>,
        binding: &mut Binding,
        now: Duration,
    ) -> Poll<Result<(), StartupError>> {
        let result = self.turn(cx, binding, now);
        match result {
            Ok(()) => Poll::Pending,
            Err(error) => Poll::Ready(Err(error)),
        }
    }

    fn turn(
        &mut self,
        cx: &mut Context<'_>,
        binding: &mut Binding,
        now: Duration,
    ) -> Result<(), StartupError> {
        self.sync_sessions(cx, binding, now)?;
        self.outbound.poll(cx, &mut binding.port, now)?;
        let _ = self
            .actors
            .poll_progress(cx, now, |_, message| self.outbound.send(message))
            .map_err(failure)?;
        self.install_recovered(binding)?;
        self.poll_bootstrap(cx)?;
        let _ = self
            .publisher
            .as_mut()
            .expect("installed publisher")
            .poll_progress(cx, &self.actors, &mut binding.port)
            .map_err(failure)?;
        self.admission.begin_turn(
            cx,
            binding,
            &mut admission::ActorAdapter {
                actors: &mut self.actors,
                bootstrap: &self.bootstrap,
                indices: &self.indices,
                local: self.config.local,
            },
        )?;
        self.receive_inputs(cx, binding, now)?;
        self.admission.finish_turn(
            binding,
            &mut admission::ActorAdapter {
                actors: &mut self.actors,
                bootstrap: &self.bootstrap,
                indices: &self.indices,
                local: self.config.local,
            },
        )?;
        self.group_cursor = (self.group_cursor + 16) % self.identities.len().max(1);
        Ok(())
    }

    /// Compare every partition's broker sessions with the frontend after a
    /// link changed or a partition finished recovery, sixteen per turn.
    fn sync_sessions(
        &mut self,
        cx: &mut Context<'_>,
        binding: &Binding,
        now: Duration,
    ) -> Result<(), StartupError> {
        let links = binding.links.generation();
        if self.synced_links != Some(links) {
            self.synced_links = Some(links);
            self.unsynced = self.identities.len();
        }
        for _ in 0..self.unsynced.min(16) {
            self.unsynced -= 1;
            self.sync_group(self.identities[self.unsynced].0, binding, now)?;
        }
        if self.unsynced != 0 {
            cx.waker().wake_by_ref();
        }
        Ok(())
    }

    fn group_indices(&self) -> impl Iterator<Item = usize> + use<> {
        let count = self.identities.len();
        let first = self.group_cursor;
        (0..count.min(16)).map(move |offset| (first + offset) % count)
    }

    fn sync_group(
        &mut self,
        group: GroupId,
        binding: &Binding,
        now: Duration,
    ) -> Result<(), StartupError> {
        if !matches!(
            self.actors.status(group),
            Some(PartitionStatus::Replicated(_))
        ) {
            return Ok(());
        }
        for &peer in self.config.brokers.keys() {
            let key = (group, peer);
            let old = self.sessions.get(&key).copied();
            let new = binding.links.get(peer).map(|link| link.binding.session);
            if old == new {
                continue;
            }
            if let Some(new) = new {
                self.actors
                    .replace_session(
                        group,
                        peer,
                        old.unwrap_or(LinkSessionId::from_bytes([0; 16])),
                        new,
                        now,
                    )
                    .map_err(failure)?;
                self.sessions.insert(key, new);
            } else {
                self.actors
                    .disconnect_session(group, peer, old.expect("observed old session"), now)
                    .map_err(failure)?;
                self.sessions.remove(&key);
            }
        }
        Ok(())
    }

    fn poll_bootstrap(&mut self, cx: &mut Context<'_>) -> Result<(), StartupError> {
        for index in self.group_indices() {
            let Some(bootstrap) = &mut self.bootstrap[index] else {
                continue;
            };
            if bootstrap.done {
                continue;
            }
            if let Some(pending) = &mut bootstrap.pending {
                let Poll::Ready(result) = pending.as_mut().poll(cx) else {
                    continue;
                };
                let reply = result.map_err(failure)?;
                bootstrap.pending = None;
                match reply.outcome {
                    ProposalOutcome::Committed { .. } => bootstrap.done = true,
                    ProposalOutcome::Invalid(error) => return Err(failure(error)),
                    ProposalOutcome::NotAdmitted | ProposalOutcome::Unknown => {
                        bootstrap.buffer = Some(reply.buffer);
                    }
                }
                cx.waker().wake_by_ref();
            }
            let ready = match self.actors.status(bootstrap.group) {
                Some(PartitionStatus::Local(_)) => true,
                Some(PartitionStatus::Replicated(status)) => {
                    status.application_ready
                        && self
                            .actors
                            .route_state(bootstrap.group, self.identities[index].1)
                            .is_some_and(|route| route.leader == Some(self.config.local))
                }
                None => false,
            };
            if !bootstrap.done
                && ready
                && let Some(buffer) = bootstrap.buffer.take()
            {
                match bootstrap.submitter.try_submit(buffer) {
                    Ok(pending) => {
                        bootstrap.pending = Some(Box::pin(pending));
                        cx.waker().wake_by_ref();
                    }
                    Err(unsubmitted) => bootstrap.buffer = Some(unsubmitted.buffer),
                }
            }
        }
        Ok(())
    }

    fn install_recovered(&mut self, binding: &Binding) -> Result<(), StartupError> {
        while let Some(group) = self.actors.take_recovered() {
            // Its broker sessions were not applied while it recovered.
            self.synced_links = None;
            let index = self.indices[&group];
            let plan = self
                .recoveries
                .get(&group)
                .ok_or_else(|| failure("unexpected recovery handoff"))?;
            let actor = self
                .actors
                .startup_actor(group)
                .map_err(failure)?
                .ok_or_else(|| failure("unpublished recovery handoff"))?;
            let (bootstrap, native) = super::build::recovered_services(
                actor,
                plan,
                &self.config,
                &self.destinations[index * 4],
                &self.destinations[index * 4 + 1],
            )?;
            self.actors
                .install_native(native, binding.links.clone())
                .map_err(failure)?;
            self.actors
                .install_readers(
                    group,
                    ozzy_runtime::replica_actor::SharedReaderConfig {
                        partition: plan.incarnation,
                        limits: self.config.limits,
                        subscriptions: super::CLIENTS,
                    },
                    binding.links.clone(),
                )
                .map_err(failure)?;
            self.bootstrap[index] = Some(bootstrap);
            self.recoveries.remove(&group);
        }
        Ok(())
    }

    fn client_ready(&self, group: GroupId, partition: ozzy_proto::PartitionIncarnation) -> bool {
        admission::client_ready(
            &self.actors,
            &self.bootstrap,
            &self.indices,
            self.config.local,
            group,
            partition,
        )
    }

    fn receive_inputs(
        &mut self,
        cx: &mut Context<'_>,
        binding: &Binding,
        now: Duration,
    ) -> Result<(), StartupError> {
        let slots = self.admission.intake.reservation_slots();
        for offset in 0..slots.min(16) {
            if let Some(received) = self
                .admission
                .intake
                .retry_deferred(
                    (self.admission.cursor + offset) % slots,
                    &binding.links,
                    &self.routes,
                )
                .map_err(failure)?
                && self.deliver_input(binding, received, now)?
            {
                cx.waker().wake_by_ref();
            }
        }
        for _ in 0..16 {
            let Some(received) = self
                .admission
                .intake
                .receive(&binding.links, &self.routes)
                .map_err(failure)?
            else {
                break;
            };
            if self.deliver_input(binding, received, now)? {
                cx.waker().wake_by_ref();
            }
        }
        Ok(())
    }

    fn deliver_input(
        &mut self,
        binding: &Binding,
        received: IntakeMessage,
        now: Duration,
    ) -> Result<bool, StartupError> {
        if !received.current {
            return Ok(true);
        }
        let group = received.request.route.placement.group;
        let result = if received.request.binding.kind == Kind::Broker {
            self.sync_group(group, binding, now)?;
            self.actors
                .receive(group, &received.message, now)
                .map_err(failure)?;
            NativeReceive::Accepted
        } else if self.client_ready(group, received.request.route.placement.partition) {
            self.actors
                .receive_client(group, &received.message, now)
                .map_err(failure)?
        } else {
            self.actors
                .defer_client(group, &received.message, now)
                .map_err(failure)?
        };
        if result == NativeReceive::Busy {
            self.admission.intake.defer(received).map_err(failure)?;
            return Ok(false);
        }
        Ok(true)
    }
}
