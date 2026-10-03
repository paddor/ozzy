use super::*;
use tokio::sync::Semaphore;

#[tokio::test]
async fn control_frame_aliases_retain_capacity_after_observer_cancellation() {
    let capacity = Arc::new(Semaphore::new(1));
    let lease = Arc::new(Lease {
        _permit: capacity.clone().try_acquire_owned().unwrap(),
    });
    // Received metadata can share a much larger transport allocation. Only
    // compact control fields may leave this socket owner's bounded scratch.
    let backing = Bytes::from(vec![7; 128 * 1024]);
    let source = Message::multipart([
        backing.slice(..16),
        backing.slice(16..80),
        backing.slice(80..208),
        Bytes::new(),
    ]);
    let reply = track(&source, &lease, true);
    assert_ne!(
        reply.part_bytes(2).unwrap().as_ptr(),
        source.part_bytes(2).unwrap().as_ptr()
    );
    assert_eq!(reply.part_slice(2), source.part_slice(2));
    let (send, receive) = oneshot::channel();
    let retained = reply.part_bytes(1).unwrap();
    send.send(Ok::<_, BrokerLinkError>(reply)).unwrap();
    drop((source, backing, lease, receive));
    assert_eq!(capacity.available_permits(), 0);
    let mut next = Box::pin(capacity.clone().acquire_owned());
    assert!(futures::poll!(next.as_mut()).is_pending());
    drop(retained);
    drop(next.await.unwrap());
    assert_eq!(capacity.available_permits(), 1);
}
