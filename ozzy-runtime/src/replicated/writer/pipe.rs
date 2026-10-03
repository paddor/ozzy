//! Local raw-ring exception: caller records contain owned Rust payload and receipt
//! values. A byte-wire replacement needs explicit ownership, not pointer tokens.
//!
//! Typed bounded intake from application handles to the SDK owner thread.
//!
//! Lanes use fanring's default deferred teardown: a push is a ring write plus
//! one work-signal mark, with no lane readiness and no per-record handoff
//! guard; the driver scans every lane on each turn. Records still queued when
//! the driver stops stay in the lane until its application handle drops; a
//! failed writer rejects every later send, so nothing waits on them.

use std::sync::Arc;

use crate::command_channel::TryRecvError;
use crate::signal::DataSignal;

use super::{WriterError, completion, state::Queued};

#[derive(Debug)]
pub(super) struct Sender {
    queue: fanring::mpsc::Sender<Queued>,
    work: Arc<DataSignal>,
    completions: completion::Pool,
}

#[derive(Debug)]
pub(super) struct Receiver {
    queue: fanring::mpsc::Receiver<Queued>,
}

pub(super) fn channel(capacity: usize, work: Arc<DataSignal>) -> (Sender, Receiver) {
    let (queue, incoming) = fanring::mpsc::channel(capacity);
    (
        Sender {
            queue,
            work,
            completions: completion::Pool::default(),
        },
        Receiver { queue: incoming },
    )
}

impl Sender {
    pub(super) fn try_clone(&self) -> Option<Self> {
        Some(Self {
            queue: self.queue.try_clone()?,
            work: self.work.clone(),
            completions: completion::Pool::default(),
        })
    }

    pub(super) fn completion(
        &mut self,
        message_id: ozzy_proto::MessageId,
    ) -> completion::Completion {
        self.completions.allocate(message_id)
    }

    /// The lane is not marked ready: the driver scans every lane on each
    /// turn, and the work signal's fences order this push against its drain.
    pub(super) fn try_send(&mut self, record: Queued) -> Result<(), WriterError> {
        self.queue
            .try_send_unsignaled(record)
            .map_err(|_| WriterError::Pipeline)?;
        self.work.mark();
        Ok(())
    }
}

impl Receiver {
    pub(super) fn try_recv_batch_into(
        &mut self,
        output: &mut Vec<Queued>,
        limit: usize,
    ) -> Result<usize, TryRecvError> {
        self.queue.try_recv_scan_into_while(output, limit, |_| true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replicated::writer::payload::Body;
    use ozzy_proto::{MessageId, data::Encoding};

    fn record(sequence: u64, body: Vec<u8>) -> Queued {
        Queued {
            encoding: Encoding::Raw,
            admitted_at: None,
            sequence,
            completion: crate::replicated::writer::state::Completion::new(MessageId::from_bytes(
                [3; 16],
            )),
            lengths: None,
            body: Body::from_vec(body),
        }
    }

    #[test]
    fn typed_lanes_preserve_payload_ownership_and_per_sender_order() {
        let work = Arc::new(DataSignal::default());
        let (mut first, mut receiver) = channel(4, work);
        let mut second = first.try_clone().unwrap();
        let large = vec![7; 4096];
        let pointer = large.as_ptr();
        first.try_send(record(0, large)).unwrap();
        first.try_send(record(2, vec![2; 16])).unwrap();
        second.try_send(record(1, vec![1; 16])).unwrap();

        let mut records = Vec::new();
        assert_eq!(receiver.try_recv_batch_into(&mut records, 4).unwrap(), 3);
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].body.bytes().as_ptr(), pointer);
        let first_lane: Vec<_> = records
            .iter()
            .filter(|record| record.sequence != 1)
            .map(|record| record.sequence)
            .collect();
        assert_eq!(first_lane, [0, 2]);
    }
}
