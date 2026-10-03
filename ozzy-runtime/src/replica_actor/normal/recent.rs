//! Bounded applied-packet history. Eviction never waits for a peer or pins arenas.

use super::{ActorError, JournalGeneration, Message, Prefix, ReplicaActor, Scope, VecDeque};
use crate::profiling::{Event, event};
use ozzy_replication::PipelineLimits;

#[cfg(test)]
mod tests;

#[derive(Debug)]
pub(super) struct Entry {
    pub scope: Scope,
    pub generation: JournalGeneration,
    pub predecessor: Prefix,
    pub end: Prefix,
    pub operations: usize,
    pub body_bytes: usize,
    pub packets: [Option<Message>; 3],
}

#[derive(Debug)]
pub(super) struct Recent {
    entries: VecDeque<Entry>,
    limits: PipelineLimits,
    operations: usize,
    body_bytes: usize,
}

impl Recent {
    pub(super) fn new(limits: PipelineLimits) -> Self {
        Self {
            entries: VecDeque::with_capacity(limits.max_operations),
            limits,
            operations: 0,
            body_bytes: 0,
        }
    }

    pub(super) fn clear(&mut self) {
        self.entries.clear();
        self.operations = 0;
        self.body_bytes = 0;
    }

    /// Only the actor's same-image, applied live retirement may populate this cache.
    pub(super) fn retain(&mut self, entry: Entry) {
        assert!(entry.operations > 0 && entry.operations <= self.limits.max_operations);
        assert!(entry.body_bytes <= self.limits.max_body_bytes);
        if self.entries.back().is_some_and(|last| {
            last.scope != entry.scope
                || last.generation != entry.generation
                || last.end != entry.predecessor
        }) {
            event(Event::ReplayCacheReset);
            self.clear();
        }
        while entry.operations > self.limits.max_operations - self.operations
            || entry.body_bytes > self.limits.max_body_bytes - self.body_bytes
        {
            let oldest = self.entries.pop_front().expect("nonempty overfull cache");
            self.operations -= oldest.operations;
            self.body_bytes -= oldest.body_bytes;
        }
        self.operations += entry.operations;
        self.body_bytes += entry.body_bytes;
        self.entries.push_back(entry);
    }

    fn after(
        &self,
        scope: Scope,
        generation: JournalGeneration,
        cursor: Prefix,
    ) -> Result<Option<usize>, ActorError> {
        if self
            .entries
            .back()
            .is_none_or(|last| last.scope != scope || last.generation != generation)
        {
            event(Event::ReplayCacheEmpty);
            return Ok(None);
        }
        // An old ACK is only a hint. Verify its exact boundary before releasing
        // that backup's window. Interior multi-op boundaries fall back to disk.
        for (index, entry) in self.entries.iter().enumerate() {
            if entry.predecessor.op == cursor.op {
                return if entry.predecessor == cursor {
                    Ok(Some(index))
                } else {
                    Err(ActorError::History)
                };
            }
            if entry.end.op == cursor.op && entry.end != cursor {
                return Err(ActorError::History);
            }
        }
        event(
            if cursor.op < self.entries.front().expect("nonempty cache").predecessor.op {
                Event::ReplayCacheEvicted
            } else if cursor.op < self.entries.back().expect("nonempty cache").end.op {
                Event::ReplayCacheInterior
            } else {
                Event::ReplayCacheAhead
            },
        );
        Ok(None)
    }
}

impl<E: crate::replica_journal::JournalExecution> ReplicaActor<E> {
    pub(super) fn replay_recent(
        &mut self,
        voter: usize,
        cursor: Prefix,
    ) -> Result<bool, ActorError> {
        let snapshot = self.driver.normal().expect("normal replay").snapshot();
        let Some(first) =
            self.work
                .recent
                .after(snapshot.scope, snapshot.journal.generation, cursor)?
        else {
            return Ok(false);
        };
        // The cache bounds count/body work. New credit and repairs share
        // the same sender; refreshed piggyback commits release each bounded chunk.
        for index in first..self.work.recent.entries.len() {
            let entry = &mut self.work.recent.entries[index];
            let packet = entry.packets[voter].take().expect("remote voter packet");
            let (predecessor, end) = (entry.predecessor, entry.end);
            let (sent, packet) = self.send_cached_packet(voter, packet, predecessor, end)?;
            self.work.recent.entries[index].packets[voter] = Some(packet);
            match sent {
                super::flow::PacketSend::Sent => {}
                super::flow::PacketSend::Wait => break,
                super::flow::PacketSend::Miss => {
                    event(Event::ReplayPacketMiss);
                    return Ok(false);
                }
            }
        }
        event(Event::ReplayCacheHit);
        Ok(true)
    }
}
