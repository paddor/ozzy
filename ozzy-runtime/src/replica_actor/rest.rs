//! Rest: an idle group stops its own heartbeats and votes.
//!
//! A settled primary's heartbeat offers a rest and names its beat source,
//! the part of its shard the group belongs to and that part's epoch. A
//! settled backup accepts with its own. Each side then trusts the other
//! while that broker's beats still name the source it agreed on, and sends
//! nothing for the group. A primary rests once every backup agreed or has
//! no link.
//!
//! Either side ends the rest by speaking: any other message from the peer
//! drops the agreement, and so does a beat that no longer covers it. A voter
//! that leaves its view asks its shard to change the epoch of its part,
//! which ends whatever its peers still believe.

use super::{ActorError, Control, ReplicaActor};
use crate::frontend::Beats;
use ozzy_proto::LinkSessionId;
use ozzy_replication::{BeatSource, Scope};
use std::time::Duration;

/// The broker's beats and where this actor reports in them.
#[derive(Debug)]
struct Installed {
    beats: Beats,
    /// This actor's shard.
    shard: u32,
    /// The part of the shard this actor's group belongs to.
    part: u32,
}

/// One agreement with a peer voter.
#[derive(Debug, Clone, Copy)]
struct Held {
    /// This shard's epoch when both agreed.
    local: u64,
    /// The peer's shard and epoch when both agreed.
    peer: BeatSource,
    session: LinkSessionId,
}

#[derive(Debug, Default)]
pub(super) struct Rest {
    /// The broker's beats, once this actor's shard has them.
    beats: Option<Installed>,
    held: [Option<Held>; 3],
    /// The view and epoch this voter last offered or accepted a rest in.
    spoke: Option<(Scope, u64)>,
    /// The role left that view under the same epoch: a peer may still
    /// trust this voter.
    withdraw: bool,
}

impl Rest {
    fn source(&self) -> Option<BeatSource> {
        let installed = self.beats.as_ref()?;
        installed.beats.source(installed.shard, installed.part)
    }

    /// This shard's epoch for the actor's part.
    fn epoch(&self) -> Option<u64> {
        self.source().map(|source| source.epoch)
    }

    /// Whether `held` still stands: the same local epoch, the same link,
    /// and a peer whose beats still name its source.
    fn trusted(&self, held: Held, peer: ozzy_proto::NodeId, session: LinkSessionId) -> bool {
        self.beats.as_ref().is_some_and(|installed| {
            self.epoch() == Some(held.local)
                && held.session == session
                && installed.beats.covers(peer, session, held.peer)
        })
    }

    /// When `peer`'s beats last proved it alive, on the actor's clock.
    fn heard(&self, peer: ozzy_proto::NodeId, now: Duration) -> Duration {
        let age = self.beats.as_ref().and_then(|installed| {
            let beats = &installed.beats;
            beats.heard(peer).map(|age| age.min(beats.timing().timeout))
        });
        age.map_or(now, |age| now.saturating_sub(age))
    }
}

impl ReplicaActor {
    /// Let this actor rest on `beats`, which report `shard`.
    pub(super) fn install_beats(&mut self, beats: Beats, shard: u32) {
        let part = Beats::part(self.driver.scope().group_id);
        self.rest.beats = Some(Installed { beats, shard, part });
    }

    /// Whether an agreement this voter holds no longer stands on the
    /// beats. The voter must then look at it.
    pub(super) fn rest_broken(&self) -> bool {
        (0..3).any(|voter| {
            self.rest.held[voter].is_some_and(|held| {
                let peer = self.peer(voter);
                !self.rest.trusted(held, peer, self.config.sessions[voter])
            })
        })
    }

    /// Whether the role left a view in which a peer may still trust this
    /// voter. The shard then changes its epoch.
    pub(super) fn take_withdrawal(&mut self) -> bool {
        std::mem::take(&mut self.rest.withdraw)
    }

    /// Whether this voter holds a rest agreement with any peer.
    pub(super) fn resting(&self) -> bool {
        self.rest.held.iter().any(Option::is_some)
    }

    /// Whether `voter` rests on an agreement with this voter. A resting
    /// backup installed this view and needs no repeat of it.
    pub(super) fn rests(&self, voter: usize) -> bool {
        self.rest.held[voter].is_some()
    }

    /// Whether the backup's agreement with its primary stands, so it sends
    /// no vote.
    pub(super) fn resting_backup(&self) -> bool {
        let primary = self.primary();
        self.voter_slot(primary)
            .is_some_and(|voter| primary != self.local && self.rest.held[voter].is_some())
    }

    /// Check every agreement before the round acts on it.
    pub(super) fn rest_round(&mut self, now: Duration) {
        let scope = self.driver.scope();
        if let Some((spoken, epoch)) = self.rest.spoke
            && spoken != scope
        {
            self.rest.spoke = None;
            self.rest.withdraw |= self.rest.epoch() == Some(epoch);
        }
        let settled = self.driver.settled();
        for voter in 0..3 {
            let Some(held) = self.rest.held[voter] else {
                continue;
            };
            let peer = self.peer(voter);
            let trusted = self.driver.covered(peer)
                && self.rest.trusted(held, peer, self.config.sessions[voter]);
            if trusted && settled {
                continue;
            }
            // An unsettled role speaks for itself from now on. A failed
            // proof counts from the peer's last beat.
            let heard = if trusted {
                now
            } else {
                self.rest.heard(peer, now)
            };
            self.end_rest(voter, heard, now);
        }
        self.rest_primary();
    }

    /// Drop the agreement with `voter`, whose proof last held at `heard`.
    fn end_rest(&mut self, voter: usize, heard: Duration, now: Duration) {
        if self.rest.held[voter].take().is_none() {
            return;
        }
        crate::profiling::event(crate::profiling::Event::ReplicaRestEnd);
        let peer = self.peer(voter);
        // A view change already uncovered every voter.
        let _ = self.driver.uncover(peer, heard);
        if self.primary() == peer {
            // The backup votes again.
            self.ack_at = now;
        }
        self.more_work = true;
    }

    /// A primary rests while every backup agreed or has no link.
    fn rest_primary(&mut self) {
        let primary = self.primary() == self.local;
        let resting = primary
            && self.resting()
            && self.driver.normal().is_some()
            && (0..3).all(|voter| {
                let peer = self.peer(voter);
                peer == self.local
                    || self.rest.held[voter].is_some()
                    || self.session(peer).is_none()
            });
        if self.driver.resting() != resting {
            self.driver.rest(resting);
        }
    }

    /// The primary's heartbeat to one backup: nothing while the backup
    /// rests, an offer to rest while this role is settled, and the plain
    /// commit otherwise.
    pub(super) fn heartbeat(
        &mut self,
        voter: usize,
        commit: ozzy_replication::Commit,
    ) -> Result<(), ActorError> {
        if self.rest.held[voter].is_some() {
            return Ok(());
        }
        let Some(source) = self.rest.source().filter(|_| self.driver.settled()) else {
            return self.announce(voter, commit, true);
        };
        let to = self.peer(voter);
        self.rest.spoke = Some((commit.scope, source.epoch));
        self.control(to, Control::Rest { commit, source })
    }

    /// Settle what a control from `voter` means for the rest. An offer or
    /// an acceptance that fits makes or renews the agreement. Anything else
    /// the peer says ends it: the peer speaks for itself again.
    pub(super) fn receive_rest(
        &mut self,
        voter: usize,
        control: Control,
        now: Duration,
    ) -> Result<(), ActorError> {
        let agreed = match control {
            Control::Rest { commit, source } => {
                self.agree(voter, commit.scope, commit.committed, source)
            }
            Control::RestOk { ack, source } => self.agree(voter, ack.scope, ack.durable, source),
            _ => None,
        };
        match (control, agreed) {
            (Control::Rest { .. }, Some(source)) => {
                let normal = super::require_normal(&self.driver)?;
                let ack = normal.acknowledgment().map_err(|error| {
                    ActorError::from(ozzy_replication::driver::DriverError::from(error))
                })?;
                let to = self.peer(voter);
                self.control(to, Control::RestOk { ack, source })?;
            }
            (Control::RestOk { .. }, Some(_)) => {}
            (Control::Rest { .. }, None) => {
                self.end_rest(voter, now, now);
                // Answer the heartbeat with the plain vote.
                self.ack_at = now;
            }
            _ => self.end_rest(voter, now, now),
        }
        self.rest_primary();
        Ok(())
    }

    /// Hold an agreement with `voter` if this role is settled at `prefix`
    /// in `scope` and the peer's beats cover `source`. Returns this voter's
    /// own source.
    fn agree(
        &mut self,
        voter: usize,
        scope: Scope,
        prefix: ozzy_replication::Prefix,
        source: BeatSource,
    ) -> Option<BeatSource> {
        let local = self.rest.source()?;
        let beats = &self.rest.beats.as_ref()?.beats;
        let peer = self.peer(voter);
        let session = self.session(peer)?;
        let snapshot = self.driver.normal()?.snapshot();
        if scope != snapshot.scope
            || prefix != snapshot.committed
            || !self.driver.settled()
            || !beats.covers(peer, session, source)
        {
            return None;
        }
        self.driver.cover(peer).ok()?;
        if self.rest.held[voter].is_none() {
            crate::profiling::event(crate::profiling::Event::ReplicaRest);
        }
        self.rest.held[voter] = Some(Held {
            local: local.epoch,
            peer: source,
            session,
        });
        self.rest.spoke = Some((scope, local.epoch));
        Some(local)
    }
}
