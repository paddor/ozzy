use super::*;
use crate::frontend::{Binding, dispatch::tests::fixture};
use bytes::Bytes;
use ozzy_proto::{Envelope, LinkSessionId};

fn message(to: Binding, opcode: Opcode, payload: &'static [u8]) -> Message {
    let envelope = Envelope {
        opcode,
        response: false,
        request_id: None,
        sender: NodeId::from_bytes([9; 16]),
        session: Some(to.session),
    };
    let header = envelope
        .encode_header(0, payload.len(), EnvelopeLimits::default())
        .unwrap();
    Message::multipart([
        Bytes::copy_from_slice(to.peer.as_bytes()),
        Bytes::copy_from_slice(&header),
        Bytes::new(),
        Bytes::from_static(payload),
    ])
}

#[test]
fn recurring_control_replies_do_not_starve_data_with_one_send_slot() {
    let (mut dispatcher, _lanes, _, peer) = fixture();
    let mut sent = [0; 2];
    for round in 0..8 {
        for (class, opcode, payload) in [
            (Class::Control, Opcode::Commit, b"".as_slice()),
            (Class::Data, Opcode::Ops, b"body".as_slice()),
        ] {
            if dispatcher.queued_replies(peer.peer, class).unwrap().0 == 0 {
                dispatcher
                    .try_reply(class, message(peer, opcode, payload))
                    .unwrap();
            }
        }
        for _ in 0..round % 3 {
            dispatcher
                .flush_replies(|message| Err(TrySendError::Full(message)))
                .unwrap();
        }
        let mut available = true;
        dispatcher
            .flush_replies(|message| {
                if !std::mem::take(&mut available) {
                    return Err(TrySendError::Full(message));
                }
                let data = message.part_slice(1).unwrap()[5] == Opcode::Ops as u8;
                sent[usize::from(data)] += 1;
                Ok(())
            })
            .unwrap();
    }
    assert_eq!(sent, [4, 4]);
}

#[test]
fn identical_control_replies_leave_room_for_distinct_announcements() {
    let (mut dispatcher, _lanes, _, peer) = fixture();
    for _ in 0..32 {
        dispatcher
            .try_reply(Class::Control, message(peer, Opcode::Commit, b""))
            .unwrap();
    }
    dispatcher
        .try_reply(Class::Control, message(peer, Opcode::StartView, b""))
        .unwrap();
    assert_eq!(
        dispatcher
            .queued_replies(peer.peer, Class::Control)
            .unwrap()
            .0,
        2
    );
    let mut sent = Vec::new();
    while dispatcher.has_replies() {
        dispatcher
            .flush_replies(|message| {
                sent.push(message.part_slice(1).unwrap()[5]);
                Ok(())
            })
            .unwrap();
    }
    assert_eq!(sent, [Opcode::Commit as u8, Opcode::StartView as u8]);
    dispatcher
        .try_reply(Class::Control, message(peer, Opcode::Commit, b""))
        .unwrap();
    assert!(dispatcher.has_replies());
}

#[test]
fn full_peer_does_not_block_other_peers_or_consume_their_control_capacity() {
    let (mut dispatcher, _lanes, _, first) = fixture();
    let second = Binding {
        peer: NodeId::from_bytes([7; 16]),
        ..first
    };
    dispatcher.bind(second).unwrap();
    for peer in [first, second] {
        for _ in 0..2 {
            dispatcher
                .try_reply(Class::Data, message(peer, Opcode::Ops, b"body"))
                .unwrap();
        }
        assert_eq!(
            dispatcher
                .try_reply(Class::Data, message(peer, Opcode::Ops, b"extra"))
                .unwrap_err()
                .0,
            ReplyError::Full
        );
        dispatcher
            .try_reply(Class::Control, message(peer, Opcode::Commit, b""))
            .unwrap();
    }
    let mut submitted = Vec::new();
    for _ in 0..4 {
        dispatcher
            .flush_replies(|message| {
                if message.part_slice(0) == Some(first.peer.as_bytes().as_slice()) {
                    Err(TrySendError::Full(message))
                } else {
                    submitted.push(message);
                    Ok(())
                }
            })
            .unwrap();
    }
    assert_eq!(submitted.len(), 3);
    assert_eq!(
        dispatcher
            .queued_replies(first.peer, Class::Data)
            .unwrap()
            .0,
        2
    );
    assert_eq!(
        dispatcher
            .queued_replies(first.peer, Class::Control)
            .unwrap()
            .0,
        1
    );
    assert_eq!(
        dispatcher
            .queued_replies(second.peer, Class::Data)
            .unwrap()
            .0,
        0
    );
    assert_eq!(
        dispatcher
            .queued_replies(second.peer, Class::Control)
            .unwrap()
            .0,
        0
    );
    assert_eq!(submitted[0].part_slice(1).unwrap()[5], Opcode::Commit as u8);
}

#[test]
fn replacement_fences_old_queued_replies_and_preserves_duplicate_binding() {
    let (mut dispatcher, _lanes, _, old) = fixture();
    dispatcher
        .try_reply(Class::Control, message(old, Opcode::Commit, b""))
        .unwrap();
    assert!(!dispatcher.bind(old).unwrap());
    assert!(dispatcher.has_replies());
    let new = Binding {
        session: LinkSessionId::from_bytes([77; 16]),
        ..old
    };
    dispatcher.bind(new).unwrap();
    assert!(!dispatcher.has_replies());
    assert_eq!(
        dispatcher
            .try_reply(Class::Control, message(old, Opcode::Commit, b""))
            .unwrap_err()
            .0,
        ReplyError::Session
    );
    dispatcher
        .try_reply(Class::Control, message(new, Opcode::Commit, b""))
        .unwrap();
    let sent = dispatcher
        .flush_replies(|message| {
            assert_eq!(
                &message.part_slice(1).unwrap()[40..56],
                new.session.as_bytes()
            );
            Ok(())
        })
        .unwrap();
    assert_eq!(sent.control, SendAttempt::Submitted);
    assert!(!dispatcher.has_replies());
}

#[test]
fn data_cannot_enter_control_queue_and_socket_failure_is_not_confirmation() {
    let (mut dispatcher, _lanes, _, peer) = fixture();
    let data = message(peer, Opcode::Ops, b"canonical");
    let (error, returned) = dispatcher
        .try_reply(Class::Control, data.clone())
        .unwrap_err();
    assert_eq!(error, ReplyError::Class);
    assert_eq!(returned.part_slice(3), data.part_slice(3));
    dispatcher.try_reply(Class::Data, data).unwrap();
    let progress = dispatcher
        .flush_replies(|_| Err(TrySendError::Error(omq_tokio::Error::Unroutable)))
        .unwrap();
    assert_eq!(progress.data, SendAttempt::Unroutable);
    assert!(!dispatcher.has_replies());
    dispatcher
        .try_reply(Class::Control, message(peer, Opcode::Commit, b""))
        .unwrap();
    assert!(matches!(
        dispatcher.flush_replies(|_| Err(TrySendError::Closed)),
        Err(omq_tokio::Error::Closed)
    ));
}
