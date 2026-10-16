use super::*;
use tokio::sync::Semaphore;

fn progress_request() -> progress::Request {
    progress::Request {
        authority: ozzy_proto::append::Authority {
            group_id: ozzy_proto::GroupId::from_bytes([1; 16]),
            config_epoch: 1,
            view: 2,
        },
        assignment: progress::Assignment {
            group: ozzy_proto::ConsumerGroupId::from_bytes([3; 16]),
            partition: ozzy_proto::PartitionIncarnation::from_bytes([4; 16]),
            member: ozzy_proto::ConsumerMemberId::from_bytes([5; 16]),
            epoch: 6,
        },
        policy: ozzy_proto::append::Policy::QuorumDurable,
        cursor: Some(ozzy_proto::Offset::new(2)),
    }
}

fn progress_reply(confirmed: progress::Confirmed) -> Message {
    let mut metadata = Vec::with_capacity(progress::CONFIRMED_BYTES);
    let header = progress::encode_confirmed(
        Envelope {
            opcode: Opcode::ProgressCommitted,
            response: true,
            sender: NodeId::from_bytes([7; 16]),
            session: Some(LinkSessionId::from_bytes([8; 16])),
            request_id: Some(RequestId::from_bytes([9; 16])),
        },
        confirmed,
        &mut metadata,
        EnvelopeLimits::default(),
    )
    .unwrap();
    Message::multipart([
        Bytes::from(vec![7; 16]),
        Bytes::copy_from_slice(&header),
        Bytes::from(metadata),
        Bytes::new(),
    ])
}

#[tokio::test]
async fn decoded_progress_releases_control_capacity_before_its_observer_resumes() {
    let request = progress_request();
    let confirmed = progress::Confirmed {
        authority: request.authority,
        assignment: request.assignment,
        policy: request.policy,
        cursor: ozzy_proto::Offset::new(3),
        retained_from: ozzy_proto::Offset::ZERO,
        end: ozzy_proto::Offset::new(4),
    };
    let capacity = Arc::new(Semaphore::new(1));
    let lease = Arc::new(Lease {
        _permit: capacity.clone().try_acquire_owned().unwrap(),
    });
    let message = progress_reply(confirmed);
    let packet = packet(
        &message,
        NodeId::from_bytes([7; 16]),
        EnvelopeLimits::default(),
    )
    .unwrap();
    let reply = Body::Progress(request)
        .reply(packet, &message, &lease, EnvelopeLimits::default())
        .unwrap();
    let (send, receive) = oneshot::channel();
    send.send(reply).unwrap();
    drop((message, lease));
    assert_eq!(capacity.available_permits(), 1);
    let Reply::Progress(reply) = receive.await.unwrap() else {
        panic!("typed progress completion")
    };
    assert_eq!(reply, confirmed);
}

#[test]
fn progress_reply_checks_every_source_fence_policy_and_cumulative_cursor() {
    let request = progress_request();
    let confirmed = progress::Confirmed {
        authority: request.authority,
        assignment: request.assignment,
        policy: request.policy,
        cursor: ozzy_proto::Offset::new(2),
        retained_from: ozzy_proto::Offset::new(3),
        end: ozzy_proto::Offset::new(4),
    };
    let capacity = Arc::new(Semaphore::new(1));
    let lease = Arc::new(Lease {
        _permit: capacity.try_acquire_owned().unwrap(),
    });
    let decode = |request, confirmed| {
        let message = progress_reply(confirmed);
        let packet = packet(
            &message,
            NodeId::from_bytes([7; 16]),
            EnvelopeLimits::default(),
        )
        .unwrap();
        Body::Progress(request).reply(packet, &message, &lease, EnvelopeLimits::default())
    };
    assert!(
        decode(request, confirmed).is_ok(),
        "covered update can precede retention"
    );
    assert!(matches!(
        decode(
            progress::Request {
                cursor: None,
                ..request
            },
            confirmed
        ),
        Err(BrokerLinkError::Response)
    ));
    let mut wrong = confirmed;
    wrong.authority.view += 1;
    assert!(decode(request, wrong).is_err());
    wrong = confirmed;
    wrong.authority.config_epoch += 1;
    assert!(decode(request, wrong).is_err());
    wrong = confirmed;
    wrong.authority.group_id = ozzy_proto::GroupId::from_bytes([99; 16]);
    assert!(decode(request, wrong).is_err());
    for field in 0..4 {
        wrong = confirmed;
        match field {
            0 => wrong.assignment.epoch += 1,
            1 => wrong.assignment.group = ozzy_proto::ConsumerGroupId::from_bytes([99; 16]),
            2 => wrong.assignment.member = ozzy_proto::ConsumerMemberId::from_bytes([99; 16]),
            _ => {
                wrong.assignment.partition = ozzy_proto::PartitionIncarnation::from_bytes([99; 16]);
            }
        }
        assert!(decode(request, wrong).is_err());
    }
    wrong = confirmed;
    wrong.policy = ozzy_proto::append::Policy::QuorumReplicatedPersisting;
    assert!(decode(request, wrong).is_err());
    wrong = confirmed;
    wrong.cursor = ozzy_proto::Offset::new(1);
    assert!(decode(request, wrong).is_err());
}

#[test]
fn progress_rejections_preserve_expired_history_and_fail_closed_on_malformed_detail() {
    use crate::replicated::ProgressError;
    let request = progress::Request {
        cursor: None,
        ..progress_request()
    };
    let expired = progress::HistoryExpired {
        cursor: ozzy_proto::Offset::new(1),
        retained_from: ozzy_proto::Offset::new(3),
    };
    let detail = expired.encode().unwrap();
    let nack = nack::Nack {
        code: 9,
        retry: nack::RetryClass::Permanent,
        detail: &detail,
        diagnostic: "",
    };
    assert!(
        matches!(progress_rejection(request, nack, None), BrokerLinkError::Progress(ProgressError::HistoryExpired { cursor, retained_from }) if cursor == expired.cursor && retained_from == expired.retained_from)
    );
    assert!(matches!(
        progress_rejection(progress_request(), nack, None),
        BrokerLinkError::Response
    ));
    assert!(matches!(
        progress_rejection(
            request,
            nack::Nack {
                retry: nack::RetryClass::AfterBackoff,
                ..nack
            },
            None
        ),
        BrokerLinkError::Response
    ));
    for invalid in [&detail[..15], &[0; 16][..], &[]] {
        assert!(matches!(
            progress_rejection(
                request,
                nack::Nack {
                    detail: invalid,
                    ..nack
                },
                None
            ),
            BrokerLinkError::Response
        ));
    }
    let fenced = nack::Nack {
        code: 6,
        detail: &[],
        ..nack
    };
    assert!(matches!(
        progress_rejection(request, fenced, None),
        BrokerLinkError::Progress(ProgressError::Fenced)
    ));
    assert!(matches!(
        progress_rejection(
            request,
            nack::Nack {
                detail: b"unexpected",
                ..fenced
            },
            None
        ),
        BrokerLinkError::Response
    ));
    // Producer retry history uses the same code with a different command scope.
    assert!(matches!(
        reader_rejection(
            nack::Nack {
                detail: &[],
                ..nack
            },
            false,
            None
        ),
        BrokerLinkError::Rejected { code: 9, .. }
    ));
}

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
