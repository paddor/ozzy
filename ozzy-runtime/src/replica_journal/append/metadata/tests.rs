use super::*;
use ozzy_journal::operation::OperationKind;
use ozzy_proto::{GroupId, NodeId};
use ozzy_replication::{JournalGeneration, PipelineLimits, Prefix, local};
use std::{
    future::Future,
    pin::pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};
use tokio::sync::Semaphore;

fn limits() -> PipelineLimits {
    PipelineLimits {
        max_operations: 32,
        max_body_bytes: 16 * HASH_TURN_BYTES,
    }
}

fn ticket() -> ValidationTicket {
    let configuration = local::Configuration::new(
        GroupId::from_bytes([1; 16]),
        1,
        NodeId::from_bytes([2; 16]),
        Digest::from_bytes([3; 32]),
    )
    .unwrap();
    local::Driver::recover(
        configuration,
        JournalGeneration(1),
        Prefix::GENESIS,
        limits(),
    )
    .unwrap()
    .begin_validation()
    .unwrap()
}

// Metadata hashing treats bodies as opaque. Canonical decoding happens later.
fn buffer(sizes: &[usize]) -> AppendBuffer {
    let lease = Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap();
    let mut buffer = AppendBuffer::new(JournalGeneration(1), limits(), lease);
    for &size in sizes {
        let body: Vec<_> = (0..size).map(|n| (n % 251) as u8).collect();
        buffer
            .push(CanonicalOperation {
                group_id: GroupId::from_bytes([0; 16]),
                configuration_epoch: 0,
                original_view: 0,
                op_number: 0,
                previous_digest: Digest::ZERO,
                kind: OperationKind::Barrier,
                body: &body,
            })
            .unwrap();
    }
    buffer
}

#[derive(Default)]
struct Wakes(AtomicUsize);

impl Wake for Wakes {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

fn finish(future: impl Future<Output = Result<(), JournalError>>) -> usize {
    let mut future = pin!(future);
    let counter = Arc::new(Wakes::default());
    let waker = Waker::from(counter.clone());
    for turns in 1..100 {
        match future.as_mut().poll(&mut Context::from_waker(&waker)) {
            Poll::Ready(result) => {
                result.unwrap();
                assert_eq!(counter.0.load(Ordering::Relaxed), turns - 1);
                return turns;
            }
            Poll::Pending => assert_eq!(counter.0.load(Ordering::Relaxed), turns),
        }
    }
    panic!("metadata hashing made no bounded progress");
}

#[test]
fn cooperative_metadata_hashes_exact_bodies_across_boundaries() {
    for sizes in [
        vec![0, 1, 239, 240, 241],
        vec![HASH_TURN_BYTES - 1],
        vec![HASH_TURN_BYTES],
        vec![HASH_TURN_BYTES + 1],
        vec![HASH_TURN_BYTES * 2 + 17],
        vec![HASH_TURN_BYTES / 2; 9],
    ] {
        let mut asynchronous = buffer(&sizes);
        let expected = asynchronous
            .entries
            .iter()
            .map(|entry| canonical_body_digest(&asynchronous.bodies[entry.body.clone()]))
            .collect::<Vec<_>>();
        let ticket = ticket();
        let turns = finish(asynchronous.prepare_metadata_async(ticket, true));
        assert_eq!(
            turns,
            sizes.iter().sum::<usize>().div_ceil(HASH_TURN_BYTES).max(1)
        );
        assert_eq!(asynchronous.prepared.len(), sizes.len());
        assert_eq!(asynchronous.body_digests, expected);
        assert!(
            asynchronous
                .entries
                .iter()
                .all(|entry| entry.digest.is_none())
        );
        let prepared = asynchronous.prepared.clone();
        finish(asynchronous.prepare_metadata_async(ticket, false));
        assert_eq!(asynchronous.prepared, prepared);
    }
}

#[test]
fn cancelled_metadata_hash_installs_nothing_and_retry_rehashes_changed_bytes() {
    let mut buffer = buffer(&[HASH_TURN_BYTES * 2 + 17]);
    let ticket = ticket();
    {
        let mut hashing = pin!(buffer.prepare_metadata_async(ticket, true));
        assert!(
            hashing
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }
    assert!(buffer.prepared.is_empty());
    assert!(buffer.body_digests.is_empty());
    assert_eq!(buffer.entries[0].envelope.op_number, 0);
    let before = canonical_body_digest(&buffer.bodies);
    buffer.bodies.mutable().unwrap()[0] ^= 1;
    finish(buffer.prepare_metadata_async(ticket, true));
    let after = canonical_body_digest(&buffer.bodies);
    assert_ne!(before, after);
    assert_eq!(buffer.body_digests, vec![after]);
    assert!(buffer.entries[0].digest.is_none());
    // A later proposal may assign another offset. Never cache this mutable
    // body's checksum as if wire decoding had verified an immutable payload.
    buffer.bodies.mutable().unwrap()[1] ^= 1;
    finish(buffer.prepare_metadata_async(ticket, true));
    assert_ne!(buffer.body_digests[0], after);
    assert_eq!(
        buffer.body_digests[0],
        canonical_body_digest(&buffer.bodies)
    );
}

#[test]
fn cooperative_metadata_keeps_envelope_validation() {
    let mut buffer = buffer(&[HASH_TURN_BYTES + 1]);
    let ticket = ticket();
    finish(buffer.prepare_metadata_async(ticket, true));
    buffer.entries[0].envelope.configuration_epoch += 1;
    let mut checking = pin!(buffer.prepare_metadata_async(ticket, false));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(checking.as_mut().poll(&mut cx).is_pending());
    assert!(matches!(
        checking.as_mut().poll(&mut cx),
        Poll::Ready(Err(JournalError::AppendMismatch))
    ));
}
